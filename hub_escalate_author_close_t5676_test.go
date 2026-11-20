package panewire

import (
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// The hub half of the ladder close operation (hk 5676): the step closes the
// author's pane through a durable node operation, reads the node's stored
// result, keeps an ambiguous outcome staged with one owner decision, fences an
// abandoned request before its stage is released, and leaves a node without the
// capability on the marker path.

// ladderNodeFake plays the holder node's ladder close operation: one stored
// record per request id, as nodeLadderClose keeps.
type ladderNodeFake struct {
	mu      sync.Mutex
	records map[string]nodeLadderCloseResult
	closes  int
	ops     []string
	// state is what a new close records and answers ("" is closed).
	state string
	// dropReply: the first close records its result and its reply is lost.
	dropReply bool
	// fenceDown: the node does not answer a fence.
	fenceDown bool
}

func (n *ladderNodeFake) reply(f *escalateFixture, op string, raw json.RawMessage) map[string]any {
	var args struct {
		RequestID string `json:"request_id"`
	}
	_ = json.Unmarshal(raw, &args)
	n.mu.Lock()
	defer n.mu.Unlock()
	n.ops = append(n.ops, op)
	if n.records == nil {
		n.records = map[string]nodeLadderCloseResult{}
	}
	stored, found := n.records[args.RequestID]
	answer := func(result nodeLadderCloseResult, replayed bool) map[string]any {
		result.Replayed = replayed
		return map[string]any{"result": result}
	}
	switch op {
	case nodeCommandLadderClose:
		if found {
			return answer(stored, true)
		}
		n.closes++
		result := nodeLadderCloseResult{State: firstNonempty(n.state, nodeLadderClosed)}
		if result.State == nodeLadderRefused {
			result.Reason = nodePaneChanged
		}
		n.records[args.RequestID] = result
		if result.State == nodeLadderClosed || result.State == nodeLadderUnknown {
			f.paneAbsent = true
		}
		if n.dropReply {
			n.dropReply = false
			return map[string]any{"error": "close_failed"}
		}
		return answer(result, false)
	case nodeCommandLadderCloseStatus:
		if !found {
			return answer(nodeLadderCloseResult{State: nodeLadderNone}, false)
		}
		return answer(stored, false)
	case nodeCommandLadderCloseFence:
		if n.fenceDown {
			return map[string]any{"error": "journal_failed"}
		}
		if !found {
			stored = nodeLadderCloseResult{State: nodeLadderFenced}
			n.records[args.RequestID] = stored
		}
		return answer(stored, false)
	}
	return map[string]any{"error": "invalid_args"}
}

func (n *ladderNodeFake) sent() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.ops)
}

func (n *ladderNodeFake) closeCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.closes
}

// withLadderNode makes the holder's node advertise the capability and answer
// the ladder ops.
func (f *escalateFixture) withLadderNode(t *testing.T) *ladderNodeFake {
	t.Helper()
	f.ladder = &ladderNodeFake{}
	f.hub.mu.Lock()
	f.hub.nodes["machine-a"].capabilities[hubCapabilityLadderClose] = true
	f.hub.mu.Unlock()
	return f.ladder
}

// stageOpBound stages the successor as a step that marked its close for a node
// operation and then stopped: the hub restarted before it sent the request.
func (f *escalateFixture) stageOpBound(t *testing.T, createdAt time.Time) hubSpawnTask {
	t.Helper()
	return f.stageOpBoundReplacing(t, createdAt, "")
}

// stageOpBoundReplacing is stageOpBound for a live author, whose reservation
// the stage takes over (previousID is its request id).
func (f *escalateFixture) stageOpBoundReplacing(t *testing.T, createdAt time.Time, previousID string) hubSpawnTask {
	t.Helper()
	next, err := escalateAuthorSubmission(escalateHolderRequest(f.job), f.request(), f.job+"-l1")
	if err != nil {
		t.Fatal(err)
	}
	setEscalateAuthorHost(&next, "machine-a")
	next.Repository.Checkout.Sha = strings.Repeat("b", 40)
	next.Repository.Checkout.VerifyReuse = true
	next.LadderStep = &fleet.LadderStepIdentity{Parent: f.job, Round: 5, PR: f.request().PR, Profile: "sonnet", Effort: "xhigh"}
	staged, _, err := f.hub.inbox.store.claimSpawnReplacing(t.Context(), next, f.job+"-l1", "parent", createdAt, previousID, "42:implementer", "staged", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.hub.inbox.store.markStagedCloseOp(t.Context(), staged.Request.RequestID, createdAt); err != nil {
		t.Fatal(err)
	}
	return staged
}

func (f *escalateFixture) decisionCount() int {
	count := 0
	for _, text := range f.infoTexts() {
		if strings.Contains(text, "authorless?") {
			count++
		}
	}
	return count
}

func (f *escalateFixture) stageStatus(t *testing.T) (string, bool) {
	t.Helper()
	stage, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1")
	if err != nil {
		return "", false
	}
	return stage.Status, true
}

// Acceptance 1: the hub stops between asking for the close and reading the
// node's answer. The node stored a close; the replay reads it back, never asks
// the node to close again, and activates the successor once.
func TestEscalateAuthorLadderCloseReplayReadsTheStoredResultAndActivatesOnce(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-80")
	f.completeAuthorRecord(t)
	node := f.withLadderNode(t)
	node.dropReply = true
	result, commands := f.escalate(t, nil)
	if result.Code != "pending" {
		t.Fatalf("a close whose answer was lost must stay pending: %+v", result)
	}
	if ops := commandOps(commands); slices.Contains(ops, "pane.close") || !slices.Contains(ops, "pane.ladder_close") {
		t.Fatalf("the step must close through the node operation: %v", ops)
	}
	if status, ok := f.stageStatus(t); !ok || status != "staged" {
		t.Fatalf("the stage must be kept while the outcome is unread: %q %v", status, ok)
	}
	// The hub restarts: nothing but the staged row and the node's record remain.
	placed := placedAnywhere
	result, commands = f.escalate(t, &placed)
	if result.Code != "" || result.Job != "pw-task-80-l1" || result.AuthorEnded {
		t.Fatalf("the replay must activate the successor: %+v", result)
	}
	ops := commandOps(commands)
	if slices.Contains(ops, "pane.close") || slices.Contains(ops, "pane.ladder_close") || !slices.Contains(ops, "pane.ladder_close_fence") {
		t.Fatalf("the replay must read the stored result, not close again: %v", ops)
	}
	if closes := node.closeCount(); closes != 1 {
		t.Fatalf("the pane was closed %d times", closes)
	}
	if status, _ := f.stageStatus(t); status == "staged" {
		t.Fatal("the successor was not activated")
	}
	f.awaitInfo(t, "ladder step pw-task-80 -> pw-task-80-l1 on sonnet@xhigh")
}

// Acceptance 1, the other side of the restart: the marker is written and the
// hub stops before the close request is sent. The pane is still there, so the
// replay sends the request, and the node closes once.
func TestEscalateAuthorLadderCloseRestartAfterTheMarkerBeforeTheRequest(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-81")
	f.completeAuthorRecord(t)
	node := f.withLadderNode(t)
	staged := f.stageOpBound(t, time.Now())
	placed := placedAnywhere
	result, commands := f.escalate(t, &placed)
	if result.Code != "" || result.Job != "pw-task-81-l1" {
		t.Fatalf("result=%+v", result)
	}
	if ops := commandOps(commands); !slices.Contains(ops, "pane.ladder_close") || slices.Contains(ops, "pane.close") {
		t.Fatalf("ops=%v", ops)
	}
	if closes := node.closeCount(); closes != 1 {
		t.Fatalf("closes=%d", closes)
	}
	if again, err := f.hub.inbox.store.spawnTask(t.Context(), staged.Request.RequestID); err != nil || again.Status == "staged" {
		t.Fatalf("the staged request must be activated: %+v %v", again.Status, err)
	}
}

// Acceptance 3: a close the node cannot vouch for keeps the stage and asks the
// owner once, however often the step is replayed.
func TestEscalateAuthorAmbiguousLadderCloseKeepsTheStageAndAsksOnce(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-82")
	f.completeAuthorRecord(t)
	node := f.withLadderNode(t)
	node.state = nodeLadderUnknown
	result, _ := f.escalate(t, nil)
	if result.Code != "pending" || result.Job != "" || result.AuthorEnded {
		t.Fatalf("result=%+v", result)
	}
	f.awaitInfo(t, "authorless?", "pw-task-82")
	if status, ok := f.stageStatus(t); !ok || status != "staged" {
		t.Fatalf("the stage must stay staged: %q %v", status, ok)
	}
	for range 2 {
		again, _ := f.escalate(t, nil)
		if again.Code != "pending" {
			t.Fatalf("a replay of an ambiguous close must stay pending: %+v", again)
		}
	}
	if status, ok := f.stageStatus(t); !ok || status != "staged" {
		t.Fatalf("the stage must still be staged after replays: %q %v", status, ok)
	}
	// The first decision was read by awaitInfo; the replays must add none.
	if extra := f.decisionCount(); extra != 0 {
		t.Fatalf("replays raised %d more decisions", extra)
	}
	if closes := node.closeCount(); closes != 1 {
		t.Fatalf("closes=%d", closes)
	}
}

// Acceptance 4: a node that did not advertise the capability gets the marker
// path unchanged. The ladder ops are never sent to it, even though a node that
// answered them is on the other end.
func TestEscalateAuthorNodeWithoutLadderCapabilityKeepsTheMarkerPath(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-83")
	f.completeAuthorRecord(t)
	f.ladder = &ladderNodeFake{}
	placed := placedAnywhere
	result, commands := f.escalate(t, &placed)
	if result.Code != "" || result.Job != "pw-task-83-l1" {
		t.Fatalf("result=%+v", result)
	}
	if ops := commandOps(commands); !slices.Equal(ops, []string{"agents.list", "worktree.inspect", "worktree.inspect", "pane.capture", "pane.close"}) {
		t.Fatalf("node commands %v", ops)
	}
	if !f.markerAtClose || f.markerAtCapture {
		t.Fatalf("the legacy marker must precede the close: capture=%v close=%v", f.markerAtCapture, f.markerAtClose)
	}
	if sent := f.ladder.sent(); len(sent) != 0 {
		t.Fatalf("the ladder ops went to a node without the capability: %v", sent)
	}
}

// A node's refusal (the pane went back to work, or a question is open) cancels
// the stage and leaves the author as it was.
func TestEscalateAuthorLadderCloseRefusalCancelsTheStage(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-84")
	f.completeAuthorRecord(t)
	node := f.withLadderNode(t)
	node.state = nodeLadderRefused
	result, _ := f.escalate(t, nil)
	if result.Code != "pane_not_closed" || result.AuthorEnded {
		t.Fatalf("result=%+v", result)
	}
	if _, ok := f.stageStatus(t); ok {
		t.Fatal("the cancelled stage remains")
	}
}

// An abandoned request is fenced before its stage is released: the node learns
// the request is over, and a close delayed in transit finds it fenced.
func TestExpiredLadderStageFencesTheNodeOperationBeforeRelease(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-85")
	f.completeAuthorRecord(t)
	node := f.withLadderNode(t)
	staged := f.stageOpBound(t, time.Now().Add(-10*time.Minute))
	f.hub.now = func() time.Time { return time.Now().Add(10 * time.Minute) }
	f.hub.expireStagedLadderSteps(t.Context())
	if _, ok := f.stageStatus(t); ok {
		t.Fatal("the expired stage remains")
	}
	if sent := node.sent(); !slices.Equal(sent, []string{nodeCommandLadderCloseFence}) {
		t.Fatalf("the stage must be fenced on the node before it is released: %v", sent)
	}
	late := node.reply(f, nodeCommandLadderClose, json.RawMessage(`{"request_id":"`+staged.Request.RequestID+`"}`))
	if result, _ := late["result"].(nodeLadderCloseResult); result.State != nodeLadderFenced || node.closeCount() != 0 {
		t.Fatalf("a delayed close must find the request fenced: %v closes=%d", late, node.closeCount())
	}
}

// A fence the node does not answer keeps the stage: releasing it would let a
// delayed close act on a pane nothing is waiting for.
func TestExpiredLadderStageStaysWhileTheFenceIsUnanswered(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-86")
	f.completeAuthorRecord(t)
	node := f.withLadderNode(t)
	node.fenceDown = true
	f.stageOpBound(t, time.Now().Add(-10*time.Minute))
	f.hub.now = func() time.Time { return time.Now().Add(10 * time.Minute) }
	f.hub.expireStagedLadderSteps(t.Context())
	if status, ok := f.stageStatus(t); !ok || status != "staged" {
		t.Fatalf("the stage must stay until the node is fenced: %q %v", status, ok)
	}
	node.mu.Lock()
	node.fenceDown = false
	node.mu.Unlock()
	f.hub.expireStagedLadderSteps(t.Context())
	if _, ok := f.stageStatus(t); ok {
		t.Fatal("the fenced stage must be released")
	}
}

// preseed makes the node hold a stored result for the stage's request.
func (n *ladderNodeFake) preseed(id, state string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.records == nil {
		n.records = map[string]nodeLadderCloseResult{}
	}
	n.records[id] = nodeLadderCloseResult{State: state}
}

func (f *escalateFixture) stageOpBoundKeepsTheStage(t *testing.T, what string) {
	t.Helper()
	if status, ok := f.stageStatus(t); !ok || status != "staged" {
		t.Fatalf("%s: the stage must stay staged: %q %v", what, status, ok)
	}
}

// A live record's author whose pane the node closed, with the node's result
// unknown and its reply lost: the replay reads the journal before it marks the
// job lost and activates. Unknown stays staged and asks the owner once.
func TestEscalateAuthorLiveAuthorWithUnknownCloseStaysStagedOnReplay(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-87")
	node := f.withLadderNode(t)
	node.state, node.dropReply = nodeLadderUnknown, true
	if result, _ := f.escalate(t, nil); result.Code != "pending" {
		t.Fatalf("result=%+v", result)
	}
	for range 2 {
		result, commands := f.escalate(t, nil)
		if result.Code != "pending" || result.Job != "" {
			t.Fatalf("a replay with an unknown close must not activate: %+v", result)
		}
		if slices.Contains(commandOps(commands), "job.mark_lost") {
			t.Fatalf("the author was marked lost over an unknown close: %v", commandOps(commands))
		}
		f.stageOpBoundKeepsTheStage(t, "live replay")
	}
	f.awaitInfo(t, "authorless?")
	if extra := f.decisionCount(); extra != 0 {
		t.Fatalf("replays raised %d more decisions", extra)
	}
}

// The expiry pass probed the pane present, and the close finished before the
// fence: the fence's verdict, not the probe, decides. A close that ran keeps
// the successor; the next pass, which finds the pane gone, activates it.
func TestExpiredLadderStageKeepsTheSuccessorWhenTheFenceFindsAClose(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-88")
	f.completeAuthorRecord(t)
	node := f.withLadderNode(t)
	staged := f.stageOpBound(t, time.Now().Add(-10*time.Minute))
	node.preseed(staged.Request.RequestID, nodeLadderClosed)
	f.hub.now = func() time.Time { return time.Now().Add(10 * time.Minute) }
	f.hub.expireStagedLadderSteps(t.Context())
	f.stageOpBoundKeepsTheStage(t, "pane still listed")
	if slices.ContainsFunc(f.infoTexts(), func(text string) bool { return strings.Contains(text, "staged_expired") }) {
		t.Fatal("a closed author was reported as remaining present")
	}
	f.paneAbsent = true
	f.hub.expireStagedLadderSteps(t.Context())
	if status, ok := f.stageStatus(t); !ok || status == "staged" {
		t.Fatalf("the next pass must activate the successor: %q %v", status, ok)
	}
}

// A node that stopped after writing its intent leaves the pane listed and the
// record unknown: expiry keeps the stage and asks the owner, and does not report
// an author that remained.
func TestExpiredLadderStageKeepsAnUnknownCloseForTheOwner(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-89")
	f.completeAuthorRecord(t)
	node := f.withLadderNode(t)
	staged := f.stageOpBound(t, time.Now().Add(-10*time.Minute))
	node.preseed(staged.Request.RequestID, nodeLadderUnknown)
	f.hub.now = func() time.Time { return time.Now().Add(10 * time.Minute) }
	f.hub.expireStagedLadderSteps(t.Context())
	f.stageOpBoundKeepsTheStage(t, "unknown close")
	f.awaitInfo(t, "authorless?")
}

// A live author's expired stage reads the same verdict.
func TestExpiredLadderStageOfALiveAuthorKeepsAnUnknownClose(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-90")
	node := f.withLadderNode(t)
	holder, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job)
	if err != nil {
		t.Fatal(err)
	}
	staged := f.stageOpBoundReplacing(t, time.Now().Add(-10*time.Minute), holder.Request.RequestID)
	node.preseed(staged.Request.RequestID, nodeLadderUnknown)
	f.hub.now = func() time.Time { return time.Now().Add(10 * time.Minute) }
	f.hub.expireStagedLadderSteps(t.Context())
	f.stageOpBoundKeepsTheStage(t, "live unknown close")
	f.awaitInfo(t, "authorless?")
}

// An operation-bound stage whose node cannot be asked about the journal (the
// pane list is unreadable and the node holds nothing) raises no decision: no
// close is known to have been sent.
func TestExpiredLadderStageWithUnreadablePaneAndNoRecordRaisesNoDecision(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-91")
	f.completeAuthorRecord(t)
	f.withLadderNode(t)
	f.stageOpBound(t, time.Now().Add(-10*time.Minute))
	f.panesUnknown = true
	f.hub.now = func() time.Time { return time.Now().Add(10 * time.Minute) }
	f.hub.expireStagedLadderSteps(t.Context())
	f.stageOpBoundKeepsTheStage(t, "unreadable panes")
	if n := f.decisionCount(); n != 0 {
		t.Fatalf("a decision was raised for a close nobody sent: %d", n)
	}
}

// The close protocol is the stage's: an operation-bound stage is never closed
// with a plain pane.close because the node's hello lost the capability.
func TestEscalateAuthorOperationBoundStageDoesNotFallBackToAPlainClose(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-92")
	f.completeAuthorRecord(t)
	staged := f.stageOpBound(t, time.Now())
	result, commands := f.escalate(t, nil)
	if result.Code != "pending" {
		t.Fatalf("result=%+v", result)
	}
	if slices.Contains(commandOps(commands), "pane.close") {
		t.Fatalf("a plain close was sent for an operation-bound stage: %v", commandOps(commands))
	}
	kind, err := f.hub.inbox.store.stagedCloseKind(t.Context(), staged.Request.RequestID)
	if err != nil || kind != stagedCloseNodeOp {
		t.Fatalf("the stage's binding was overwritten: %v %v", kind, err)
	}
}

package panewire

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// ladderCloseFixture is a chain whose holder node advertises ladder_close_v1
// and whose hub knows the job's grant (epoch 3). answer is what the node says
// to pane.ladder_close ("" or "none": no readable answer); fenceAnswer is its
// answer to the fence. stageAtSend is the stage's binding when the close was
// sent.
type ladderCloseFixture struct {
	*escalateFixture
	answer, fenceAnswer string
	stageAtSend         *fleet.LadderCloseBinding
	closeArgs           nodeLadderCloseArgs
}

func newLadderCloseFixture(t *testing.T, job string, capable bool) *ladderCloseFixture {
	t.Helper()
	f := &ladderCloseFixture{escalateFixture: newEscalateFixture(t, job), fenceAnswer: nodeLadderFenced}
	f.hub.mu.Lock()
	f.hub.nodes["machine-a"].capabilities = map[string]bool{hubCapabilityCloseQuestion: true, hubCapabilityLadderClose: capable}
	f.hub.jobs[job] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: job, Epoch: 3}, Node: "machine-a"}
	f.hub.mu.Unlock()
	f.extraReply = func(op string, command map[string]json.RawMessage) map[string]any {
		switch op {
		case nodeCommandPaneCloseStatus:
			return map[string]any{"result": nodeCloseOpStatusResult{State: nodeCloseOpNone, Attempt: "attempt-1"}}
		case nodeCommandLadderClose:
			f.mu.Lock()
			_ = json.Unmarshal(command["args"], &f.closeArgs)
			f.mu.Unlock()
			if stage, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), job+"-l1"); err == nil {
				f.stageAtSend = stagedLadderClose(stage)
			}
			if f.answer == "" || f.answer == "none" {
				return map[string]any{"error": "journal_failed"}
			}
			result := nodeLadderCloseResult{State: f.answer}
			if f.answer == nodeLadderRefused {
				result.Reason = nodePaneChanged
			}
			return map[string]any{"result": result}
		case nodeCommandLadderFence:
			if f.fenceAnswer == "none" {
				return map[string]any{"error": "journal_failed"}
			}
			return map[string]any{"result": nodeLadderCloseResult{State: f.fenceAnswer}}
		}
		return nil
	}
	return f
}

// A capable node is asked with pane.ladder_close under the K the stage froze
// before the send, never pane.close, and its closed verdict activates the
// successor.
func TestEscalateAuthorLadderCloseOnACapableNode(t *testing.T) {
	f := newLadderCloseFixture(t, "pw-task-5865", true)
	f.answer = nodeLadderClosed
	placed := placedAnywhere
	result, commands := f.escalate(t, &placed)
	ops := commandOps(commands)
	if result.Job != f.job+"-l1" || !slices.Contains(ops, nodeCommandLadderClose) || slices.Contains(ops, nodeCommandPaneClose) {
		t.Fatalf("result=%+v ops=%v", result, ops)
	}
	bound := f.stageAtSend
	if bound == nil || bound.Protocol != fleet.LadderCloseProtocolV1 || bound.Node != "machine-a" || bound.Epoch != 3 || bound.Attempt != "attempt-1" || bound.Pane != roleHeldPane {
		t.Fatalf("binding frozen before the send=%+v", bound)
	}
	args := f.closeArgs
	if args.Job != f.job || args.RequestID != bound.RequestID || args.Node != bound.Node || args.Epoch != bound.Epoch || args.Attempt != bound.Attempt || args.PaneID != bound.Pane || args.Name != f.job {
		t.Fatalf("pane.ladder_close args=%+v stored=%+v", args, bound)
	}
}

// A node without the capability keeps pane.close and the close marker; the
// stage records the marker protocol.
func TestEscalateAuthorOlderNodeKeepsPaneClose(t *testing.T) {
	f := newLadderCloseFixture(t, "pw-task-5865-old", false)
	placed := placedAnywhere
	result, commands := f.escalate(t, &placed)
	ops := commandOps(commands)
	if result.Job != f.job+"-l1" || !slices.Contains(ops, nodeCommandPaneClose) || slices.Contains(ops, nodeCommandLadderClose) || slices.Contains(ops, nodeCommandLadderFence) {
		t.Fatalf("result=%+v ops=%v", result, ops)
	}
	stage, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1")
	if bound := stagedLadderClose(stage); err != nil || bound == nil || bound.Protocol != fleet.LadderCloseProtocolMarker {
		t.Fatalf("stage binding=%+v err=%v", bound, err)
	}
}

// An unknown or unanswered verdict keeps the stage, activates nothing and
// files one decision event, however often the step is asked again.
func TestEscalateAuthorLadderCloseUnknownKeepsTheStage(t *testing.T) {
	for _, answer := range []string{nodeLadderUnknown, "none"} {
		t.Run(answer, func(t *testing.T) {
			f := newLadderCloseFixture(t, "pw-task-5865-unknown", true)
			f.answer = answer
			for range 2 {
				result, _ := f.escalate(t, nil)
				if result.Code != "pending" {
					t.Fatalf("result=%+v", result)
				}
			}
			stage, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1")
			if err != nil || stage.Status != "staged" {
				t.Fatalf("stage=%+v err=%v", stage, err)
			}
			f.awaitInfo(t, "unresolved")
			// A window that proves nothing more is filed: the repeat shares the
			// first step's event id. awaitInfo drained the first event.
			time.Sleep(300 * time.Millisecond)
			for _, text := range f.infoTexts() {
				if strings.Contains(text, "unresolved") {
					t.Fatalf("a second decision event: %q", text)
				}
			}
		})
	}
}

// A refusal the node answered closes nothing: the stage is released with
// today's refusal, and no fence is sent for a K the node answered.
func TestEscalateAuthorLadderCloseRefusedReleasesTheStage(t *testing.T) {
	for _, answer := range []string{nodeLadderRefused, nodeLadderFenced} {
		t.Run(answer, func(t *testing.T) {
			f := newLadderCloseFixture(t, "pw-task-5865-refused", true)
			f.answer = answer
			result, commands := f.escalate(t, nil)
			if result.Code != "pane_not_closed" || result.AuthorEnded || slices.Contains(commandOps(commands), nodeCommandLadderFence) {
				t.Fatalf("result=%+v ops=%v", result, commandOps(commands))
			}
			if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1"); err == nil {
				t.Fatal("the stage was kept")
			}
			if _, job, _, err := f.hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer"); err != nil || job != f.job {
				t.Fatalf("holder role job=%q err=%v", job, err)
			}
		})
	}
}

// Withdrawing a stage whose close was sent fences its request id first; a
// fence the node does not settle keeps the stage and answers pending.
func TestEscalateAuthorWithdrawingASentStageFencesFirst(t *testing.T) {
	f := newLadderCloseFixture(t, "pw-task-5865-fence", true)
	f.answer = nodeLadderUnknown
	if result, _ := f.escalate(t, nil); result.Code != "pending" {
		t.Fatalf("result=%+v", result)
	}
	// The task moves to another job: the replay withdraws the stage.
	f.mu.Lock()
	f.taskBound = "another-job"
	f.mu.Unlock()
	f.fenceAnswer = nodeLadderUnknown
	if result, _ := f.escalate(t, nil); result.Code != "pending" {
		t.Fatalf("a stage whose fence was not confirmed answered %+v", result)
	}
	if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1"); err != nil {
		t.Fatalf("stage lost: %v", err)
	}
	f.fenceAnswer = nodeLadderFenced
	result, commands := f.escalate(t, nil)
	ops := commandOps(commands)
	if result.Code != "job_mismatch" || !slices.Contains(ops, nodeCommandLadderFence) {
		t.Fatalf("result=%+v ops=%v", result, ops)
	}
	if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1"); err == nil {
		t.Fatal("the fenced stage was kept")
	}
}

// stageBoundCompletedSuccessor stages a completed author's successor whose
// close binding is already frozen, as a step that sent its close and stopped.
func (f *ladderCloseFixture) stageBoundCompletedSuccessor(t *testing.T) {
	t.Helper()
	f.completeAuthorRecord(t)
	f.stageCompletedSuccessor(t, false)
	requestID, err := fleet.NewRequestID()
	if err != nil {
		t.Fatal(err)
	}
	stage, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.hub.inbox.store.bindStagedClose(t.Context(), stage.Request.RequestID, fleet.LadderCloseBinding{Protocol: fleet.LadderCloseProtocolV1,
		RequestID: requestID, Node: "machine-a", Epoch: 3, Attempt: "attempt-1", Pane: roleHeldPane, Name: f.job, AgentStatus: "idle", Cwd: "/work/" + f.job + "-checkout"}); err != nil {
		t.Fatal(err)
	}
}

// Reading what a stage's close did never sends a close: a node that has no
// record of the K would run it. The expiry pass of a completed author whose
// pane is present must leave the pane, and release the stage through the fence.
func TestExpiredLadderStageReadsTheNodeRecordWithoutSendingAClose(t *testing.T) {
	f := newLadderCloseFixture(t, "pw-task-5865-expiry", true)
	f.answer = nodeLadderClosed
	f.stageBoundCompletedSuccessor(t)
	f.take()
	f.hub.now = func() time.Time { return time.Now().Add(10 * time.Minute) }
	f.hub.expireStagedLadderSteps(t.Context())
	ops := commandOps(f.take())
	if slices.Contains(ops, nodeCommandLadderClose) || slices.Contains(ops, nodeCommandPaneClose) || !slices.Contains(ops, nodeCommandLadderFence) {
		t.Fatalf("ops=%v", ops)
	}
	if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1"); err == nil {
		t.Fatal("the expired stage remains")
	}
	f.awaitInfo(t, "staged_expired")
}

// A replay whose pane is gone reads the K through the fence: a fenced K (the
// node closed nothing) is an author that ended independently, an unknown one
// keeps the stage with the decision, and no close is sent either way.
func TestEscalateAuthorReplayReadsTheKWithoutSendingAClose(t *testing.T) {
	for _, tc := range []struct{ fence, want, wantStage string }{
		{nodeLadderFenced, "parent_ended", ""},
		{nodeLadderUnknown, "pending", "staged"},
	} {
		t.Run(tc.fence, func(t *testing.T) {
			f := newLadderCloseFixture(t, "pw-task-5865-replay", true)
			f.fenceAnswer = tc.fence
			f.stageBoundCompletedSuccessor(t)
			f.paneAbsent = true
			result, commands := f.escalate(t, nil)
			ops := commandOps(commands)
			if result.Code != tc.want || slices.Contains(ops, nodeCommandLadderClose) || !slices.Contains(ops, nodeCommandLadderFence) {
				t.Fatalf("result=%+v ops=%v", result, ops)
			}
			stage, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1")
			if (tc.wantStage == "") != (err != nil) || err == nil && stage.Status != tc.wantStage {
				t.Fatalf("stage=%+v err=%v", stage, err)
			}
		})
	}
}

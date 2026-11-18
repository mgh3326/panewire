package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/mgh3326/panewire/internal/fleet"
)

// supersedeFixture is a hub with an implementer parent on machine-a (a real
// websocket node that answers the close-op commands the test scripts) and the
// parent's owner lane routed to machine-b, where the info line lands. The hk
// job store is a quotaJobs fake, so job.superseded and the node's lost record
// are observable.
type supersedeFixture struct {
	*quotaReplaceFixture
	successor string
}

func newSupersedeFixture(t *testing.T, mutate func(*fleet.Submission)) *supersedeFixture {
	t.Helper()
	f := newQuotaReplaceFixtureWith(t, "implementer", "parent-job", mutate)
	f.hub.supersedeClose = true
	f.hub.mu.Lock()
	f.hub.nodes["machine-a"].capabilities[hubCapabilityCloseOps] = true
	f.hub.jobs[f.job] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: f.job, OwnerLane: "parent", Epoch: 1, Pane: roleHeldPane}, Node: "machine-a", LastSeen: time.Now().UTC()}
	f.hub.mu.Unlock()
	return &supersedeFixture{quotaReplaceFixture: f, successor: "successor-job"}
}

// successorTask is the landed successor's task as the hub's landing hook sees
// it.
func (f *supersedeFixture) successorTask(role, owner string, explicit bool) hubSpawnTask {
	return hubSpawnTask{
		Job:   f.successor,
		Owner: owner,
		Request: fleet.Submission{
			RequestID:         "d676220b-e763-4af1-aefd-0000000000a1",
			Args:              []string{"--job", f.successor, "--parent-job", f.job, "--owner", owner, "--role", role},
			ParentJobExplicit: explicit,
		},
	}
}

// land stores the landing intent of task as the spawn completion would and
// runs one landing pass, as the wake does.
func (f *supersedeFixture) land(t *testing.T, task hubSpawnTask) {
	t.Helper()
	if landing, ok := f.hub.supersedeLandingOf(task); ok {
		if err := f.hub.inbox.store.recordSupersedeLanding(t.Context(), landing); err != nil {
			t.Fatal(err)
		}
	}
	f.hub.sweepSupersedeLandings(t.Context())
}

func (f *supersedeFixture) rows(t *testing.T) []poolAction {
	t.Helper()
	rows, err := f.hub.inbox.store.supersedeActionsOf(t.Context(), f.job)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func (f *supersedeFixture) onlyRow(t *testing.T) poolAction {
	t.Helper()
	rows := f.rows(t)
	if len(rows) != 1 {
		t.Fatalf("want one supersede row, got %+v", rows)
	}
	return rows[0]
}

// step runs one reconcile pass over the one live row against the scripted
// node replies.
func (f *supersedeFixture) step(t *testing.T, replies ...poolActionReply) error {
	t.Helper()
	return runPoolActionStep(t, f.hub, f.node, readOnlyPoolAction(t, f.hub), replies...)
}

// noMoreCommands ends a test: the read that proves silence also closes the
// node's connection, so it is the last use of the node.
func (f *supersedeFixture) noMoreCommands(t *testing.T) {
	t.Helper()
	if extra, ok := tryNodeCommand(f.node); ok {
		t.Fatalf("unexpected node command: %s", extra["op"])
	}
}

func (f *supersedeFixture) superseded() string {
	f.jobs.mu.Lock()
	defer f.jobs.mu.Unlock()
	v, _ := f.jobs.records[f.job]["superseded"].(string)
	return v
}

// nodeEnds plays the node's settlement: the parent's hk record ends lost with
// the action's end_reason.
func (f *supersedeFixture) nodeEnds(reason string) {
	f.jobs.mu.Lock()
	defer f.jobs.mu.Unlock()
	f.jobs.records[f.job]["state"] = "lost"
	f.jobs.records[f.job]["end_reason"] = reason
}

func tryNodeCommand(conn *websocket.Conn) (map[string]json.RawMessage, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	for {
		_, payload, err := conn.Read(ctx)
		if err != nil {
			return nil, false
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(payload, &fields) != nil {
			continue
		}
		if string(fields["type"]) == `"node.command"` {
			return fields, true
		}
	}
}

func supersedeInfos(destination *hubAgent) []hubRelayInjectEvent {
	var out []hubRelayInjectEvent
	for {
		select {
		case relay := <-destination.relays:
			if relay.Kind == "lane.event" && strings.Contains(relay.Text, "superseded by") {
				out = append(out, relay)
			}
		default:
			return out
		}
	}
}

func supersedeProbe(status string) poolActionReply {
	return poolActionReply{op: nodeCommandAgentsList, result: map[string]any{"agents": []nodeAgentRow{}, "panes": []nodeAgentRow{{PaneID: roleHeldPane, Name: "parent-job", AgentStatus: status, Cwd: "/work/project"}}, "panes_ok": true}}
}

var (
	supersedeNoneStatus = poolStatus(nodeCloseOpNone, false, false)
	supersedeClosed     = poolActionReply{op: nodeCommandPaneCloseOp, result: map[string]any{"outcome": fleet.CloseOpClosed, "replayed": false}}
)

// closeIdleParent drives a freshly queued row through the whole protocol: the
// hub reads the pane, sends the close, sees the node settle, and finishes.
func (f *supersedeFixture) closeIdleParent(t *testing.T) {
	t.Helper()
	if err := f.step(t, supersedeNoneStatus, supersedeProbe("idle"), supersedeClosed); err != nil {
		t.Fatal(err)
	}
	if err := f.step(t, poolStatus(fleet.CloseOpClosed, false, true)); err != nil {
		t.Fatal(err)
	}
	f.nodeEnds(supersedeEndReason(f.successor))
	if err := f.hub.reconcilePoolAction(t.Context(), readOnlyPoolAction(t, f.hub)); err != nil {
		t.Fatal(err)
	}
}

// TestSupersedeExplicitParentClosesIdleParentOnce is acceptance: an explicit
// --parent-job implementer lands while the parent is idle: one row, the
// parent closed through the close-op path, the worktree kept, one info line.
func TestSupersedeExplicitParentClosesIdleParentOnce(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	worktree := filepath.Join(t.TempDir(), "worktree")
	if err := os.MkdirAll(worktree, 0755); err != nil {
		t.Fatal(err)
	}
	f.land(t, f.successorTask("implementer", "parent", true))
	f.land(t, f.successorTask("implementer", "parent", true))
	row := f.onlyRow(t)
	if row.Data.Kind != poolActionKindSupersedeClose || row.Job != f.job || row.Node != "machine-a" || row.Epoch != 1 || row.Data.Supersede.Successor != f.successor || row.Data.Pane != roleHeldPane || row.Data.Owner != "parent" {
		t.Fatalf("row does not bind successor, node and epoch: %+v", row)
	}
	// The close request the node gets names the parent, its epoch and pane,
	// the idle status the hub read, and the end reason.
	done := make(chan error, 1)
	go func() { done <- f.hub.reconcilePoolAction(t.Context(), row) }()
	for _, reply := range []poolActionReply{supersedeNoneStatus, supersedeProbe("idle")} {
		command := nodeCommandRead(t, f.node)
		if string(command["op"]) != `"`+reply.op+`"` {
			t.Fatalf("command %s, want %s", command["op"], reply.op)
		}
		nodeCommandReplyTo(t, f.node, command, map[string]any{"result": reply.result})
	}
	command := nodeCommandRead(t, f.node)
	if string(command["op"]) != `"`+nodeCommandPaneCloseOp+`"` {
		t.Fatalf("command %s, want the close op", command["op"])
	}
	var args nodePaneCloseOpArgs
	if err := json.Unmarshal(command["args"], &args); err != nil {
		t.Fatal(err)
	}
	if args.RequestID != row.ID || args.Job != f.job || args.Epoch != 1 || args.PaneID != roleHeldPane || args.AgentStatus != "idle" || args.EndReason != "superseded_by="+f.successor {
		t.Fatalf("close args=%+v", args)
	}
	nodeCommandReplyTo(t, f.node, command, map[string]any{"result": supersedeClosed.result})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := f.step(t, poolStatus(fleet.CloseOpClosed, false, true)); err != nil {
		t.Fatal(err)
	}
	f.nodeEnds("superseded_by=" + f.successor)
	if err := f.hub.reconcilePoolAction(t.Context(), readOnlyPoolAction(t, f.hub)); err != nil {
		t.Fatal(err)
	}
	f.noMoreCommands(t)
	if got := f.superseded(); got != "" {
		t.Fatalf("the hub writes no hk marker on a parent it does not host (the node's lost record carries superseded_by): %q", got)
	}
	infos := supersedeInfos(f.destination)
	if len(infos) != 1 || !strings.Contains(infos[0].Text, "[panewire] closed parent-job on machine-a: superseded by successor-job") {
		t.Fatalf("info events=%+v", infos)
	}
	if _, err := os.Stat(worktree); err != nil {
		t.Fatalf("the close must never remove the worktree: %v", err)
	}
	if active, err := f.hub.inbox.store.activePoolActions(t.Context()); err != nil || len(active) != 0 {
		t.Fatalf("a finished close leaves no live row: %+v %v", active, err)
	}
	if rows := f.rows(t); len(rows) != 1 || rows[0].Phase != poolActionDone {
		t.Fatalf("row=%+v", rows)
	}
	// A later landing for the same parent adds no second row.
	f.land(t, f.successorTask("implementer", "parent", true))
	if rows := f.rows(t); len(rows) != 1 {
		t.Fatalf("one row per parent, got %+v", rows)
	}
}

// TestSupersedeImplicitParentCreatesNoRow: a parent filled from ARBITER_JOB is
// not a request to replace it. Older clients send no field, which reads the
// same.
func TestSupersedeImplicitParentCreatesNoRow(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	f.land(t, f.successorTask("implementer", "parent", false))
	if rows := f.rows(t); len(rows) != 0 {
		t.Fatalf("an implicit parent must create no row: %+v", rows)
	}
	var legacy fleet.Submission
	if err := json.Unmarshal([]byte(`{"request_id":"d676220b-e763-4af1-aefd-0000000000a1","args":["--job","successor-job","--parent-job","parent-job","--owner","parent","--role","implementer"]}`), &legacy); err != nil {
		t.Fatal(err)
	}
	task := f.successorTask("implementer", "parent", false)
	task.Request = legacy
	f.land(t, task)
	f.noMoreCommands(t)
	if rows := f.rows(t); len(rows) != 0 {
		t.Fatalf("a request with no field must create no row: %+v", rows)
	}
}

// TestSupersedeNeverQueuesForTheseParentsOrSuccessors: a reviewer or consult
// child, a --keep parent, a parent this hub has no spawn record for, a
// successor of another lane, the switch off, and an ended parent all create no
// row.
func TestSupersedeNeverQueuesForTheseParentsOrSuccessors(t *testing.T) {
	t.Run("reviewer and consult children", func(t *testing.T) {
		f := newSupersedeFixture(t, nil)
		for _, role := range []string{"reviewer", "consult"} {
			f.land(t, f.successorTask(role, "parent", true))
		}
		if rows := f.rows(t); len(rows) != 0 {
			t.Fatalf("a %s child must create no row: %+v", "reviewer or consult", rows)
		}
	})
	t.Run("keep parent", func(t *testing.T) {
		f := newSupersedeFixture(t, func(request *fleet.Submission) { request.Args = append(request.Args, "--keep") })
		f.land(t, f.successorTask("implementer", "parent", true))
		if rows := f.rows(t); len(rows) != 0 {
			t.Fatalf("a --keep parent must create no row: %+v", rows)
		}
	})
	t.Run("parent without a spawn record", func(t *testing.T) {
		f := newSupersedeFixture(t, nil)
		f.hub.mu.Lock()
		f.hub.jobs["local-job"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "local-job", OwnerLane: "parent", Epoch: 1, Pane: "w9:p9"}, Node: "machine-a"}
		f.hub.mu.Unlock()
		task := f.successorTask("implementer", "parent", true)
		task.Request.Args = []string{"--job", f.successor, "--parent-job", "local-job", "--owner", "parent", "--role", "implementer"}
		f.land(t, task)
		rows, err := f.hub.inbox.store.supersedeActionsOf(t.Context(), "local-job")
		if err != nil || len(rows) != 0 {
			t.Fatalf("a parent with no spawn record must create no row: %+v %v", rows, err)
		}
	})
	t.Run("another lane", func(t *testing.T) {
		f := newSupersedeFixture(t, nil)
		f.land(t, f.successorTask("implementer", "other-lane", true))
		if rows := f.rows(t); len(rows) != 0 {
			t.Fatalf("a foreign successor must create no row: %+v", rows)
		}
	})
	t.Run("switch off", func(t *testing.T) {
		f := newSupersedeFixture(t, nil)
		f.hub.supersedeClose = false
		f.land(t, f.successorTask("implementer", "parent", true))
		if rows := f.rows(t); len(rows) != 0 {
			t.Fatalf("with the switch off no row exists: %+v", rows)
		}
	})
	t.Run("ended parent", func(t *testing.T) {
		f := newSupersedeFixture(t, nil)
		f.hub.mu.Lock()
		f.hub.jobs[f.job].Completed = true
		f.hub.mu.Unlock()
		f.land(t, f.successorTask("implementer", "parent", true))
		if rows := f.rows(t); len(rows) != 0 {
			t.Fatalf("an ended parent must create no row: %+v", rows)
		}
	})
}

// TestSupersedeWorkingParentStaysPendingUntilItStops is acceptance: a working
// parent gets no close request; the row stays pending and a later pass, once
// the pane has stopped, closes it. A blocked pane waits too.
func TestSupersedeWorkingParentStaysPendingUntilItStops(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	f.land(t, f.successorTask("implementer", "parent", true))
	for _, status := range []string{"working", "working", "blocked"} {
		if err := f.step(t, supersedeNoneStatus, supersedeProbe(status)); err != nil {
			t.Fatal(err)
		}
		// A stray close request would sit unread on the node's connection
		// and fail the next scripted read.
		if row := f.onlyRow(t); row.Phase != poolActionPending || row.Data.Args != nil {
			t.Fatalf("a %s parent must leave the row pending with no request: %+v", status, row)
		}
	}
	if infos := supersedeInfos(f.destination); len(infos) != 0 {
		t.Fatalf("no info before the close: %+v", infos)
	}
	f.closeIdleParent(t)
	if infos := supersedeInfos(f.destination); len(infos) != 1 {
		t.Fatalf("the first pass after it stopped closes it with one info: %+v", infos)
	}
}

// TestSupersedeRowSurvivesHubRestart is acceptance: a pending row is finished
// by the reconciler of a hub started afterwards, which holds no in-memory
// intent.
func TestSupersedeRowSurvivesHubRestart(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	f.land(t, f.successorTask("implementer", "parent", true))
	if err := f.step(t, supersedeNoneStatus, supersedeProbe("working")); err != nil {
		t.Fatal(err)
	}
	restartPoolActionHub(t, f.quotaReplaceFixture)
	f.hub.supersedeClose = true
	if rows := f.rows(t); len(rows) != 1 || rows[0].Phase != poolActionPending {
		t.Fatalf("the row must outlive the hub: %+v", rows)
	}
	f.closeIdleParent(t)
	if infos := supersedeInfos(f.destination); len(infos) != 1 {
		t.Fatalf("the restarted hub owes one info: %+v", infos)
	}
	if rows := f.rows(t); len(rows) != 1 || rows[0].Phase != poolActionDone {
		t.Fatalf("row=%+v", rows)
	}
}

// TestSupersedeRestartAfterCloseRequestResumesIt: a hub restarted after the
// close request was sent replays the stored request instead of building a new
// one, then finishes.
func TestSupersedeRestartAfterCloseRequestResumesIt(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	f.land(t, f.successorTask("implementer", "parent", true))
	if err := f.step(t, supersedeNoneStatus, supersedeProbe("idle"), supersedeClosed); err != nil {
		t.Fatal(err)
	}
	sent := f.onlyRow(t).Data.Args
	if sent == nil {
		t.Fatal("the request is stored before it is sent")
	}
	restartPoolActionHub(t, f.quotaReplaceFixture)
	f.hub.supersedeClose = true
	if err := f.step(t, poolStatus(fleet.CloseOpClosing, false, false), supersedeClosed); err != nil {
		t.Fatal(err)
	}
	if again := f.onlyRow(t).Data.Args; again == nil || *again != *sent {
		t.Fatalf("restart must replay the same request: %+v vs %+v", again, sent)
	}
}

// flakyJobReads fails hk reads of job records while told to.
type flakyJobReads struct {
	fleet.JobStore
	mu   sync.Mutex
	fail bool
}

func (s *flakyJobReads) Get(ctx context.Context, id string) (map[string]any, error) {
	s.mu.Lock()
	fail := s.fail
	s.mu.Unlock()
	if fail {
		return nil, errors.New("handoffkeep unavailable")
	}
	return s.JobStore.Get(ctx, id)
}

func (s *flakyJobReads) setFail(fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail = fail
}

// TestSupersedeFailedHKAccessIsRetriedNotConsumed is acceptance: the hub
// writes no parent record itself (the node's settlement writes the lost
// record, which the hub reads back), so a settlement not yet proved and an
// unreadable hk both leave the row pending, and the info line is owed once.
func TestSupersedeFailedHKAccessIsRetriedNotConsumed(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	f.land(t, f.successorTask("implementer", "parent", true))
	if err := f.step(t, supersedeNoneStatus, supersedeProbe("idle"), supersedeClosed); err != nil {
		t.Fatal(err)
	}
	// The node closed the pane but has not settled the hk record yet.
	if err := f.step(t, poolStatus(fleet.CloseOpClosed, false, false)); err != nil {
		t.Fatal(err)
	}
	if row := f.onlyRow(t); row.Phase != poolActionPending {
		t.Fatalf("a closed but unsettled request stays pending: %+v", row)
	}
	if err := f.step(t, poolStatus(fleet.CloseOpClosed, false, true)); err != nil {
		t.Fatal(err)
	}
	// Settled, but hk cannot be read: the finish is retried, not consumed.
	flaky := &flakyJobReads{JobStore: f.jobs, fail: true}
	f.hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: flaky, Out: out, Err: errout}, nil
	}
	f.nodeEnds(supersedeEndReason(f.successor))
	if err := f.hub.reconcilePoolAction(t.Context(), readOnlyPoolAction(t, f.hub)); err == nil {
		t.Fatal("an unreadable hk must not finish the row")
	}
	if infos := supersedeInfos(f.destination); len(infos) != 0 {
		t.Fatalf("no info before the record is read: %+v", infos)
	}
	flaky.setFail(false)
	if err := f.hub.reconcilePoolAction(t.Context(), readOnlyPoolAction(t, f.hub)); err != nil {
		t.Fatal(err)
	}
	if infos := supersedeInfos(f.destination); len(infos) != 1 {
		t.Fatalf("one info once hk is readable: %+v", infos)
	}
	if rows := f.rows(t); len(rows) != 1 || rows[0].Phase != poolActionDone {
		t.Fatalf("rows=%+v", rows)
	}
}

// TestSupersedePaneThatChangedHandsIsRefusedWithoutClosing is acceptance: a
// pane that is no longer the parent's is never closed, by the hub's read or
// by the node's own check inside the close operation.
func TestSupersedePaneThatChangedHandsIsRefusedWithoutClosing(t *testing.T) {
	t.Run("hub read finds another pane under the label", func(t *testing.T) {
		f := newSupersedeFixture(t, nil)
		f.land(t, f.successorTask("implementer", "parent", true))
		other := poolActionReply{op: nodeCommandAgentsList, result: map[string]any{"agents": []nodeAgentRow{}, "panes": []nodeAgentRow{{PaneID: "w9:p9", Name: "parent-job", AgentStatus: "idle", Cwd: "/work/project"}}, "panes_ok": true}}
		release := poolActionReply{op: nodeCommandPaneCloseRelease, result: map[string]any{"state": fleet.CloseOpCancelled, "released": true, "settled": false, "attempt": "00002-job.spawned.json"}}
		if err := f.step(t, supersedeNoneStatus, other, release); err != nil {
			t.Fatal(err)
		}
		f.noMoreCommands(t)
		if rows := f.rows(t); len(rows) != 1 || rows[0].Phase != poolActionRefused || rows[0].Data.Args != nil {
			t.Fatalf("a pane that changed hands ends the row with no request: %+v", rows)
		}
	})
	t.Run("node refuses the binding", func(t *testing.T) {
		f := newSupersedeFixture(t, nil)
		f.land(t, f.successorTask("implementer", "parent", true))
		mismatch := poolActionReply{op: nodeCommandPaneCloseOp, result: map[string]any{"outcome": nodeCloseOpMismatch, "replayed": false}}
		release := poolActionReply{op: nodeCommandPaneCloseRelease, result: map[string]any{"state": fleet.CloseOpCancelled, "released": true, "settled": false, "attempt": "00002-job.spawned.json"}}
		if err := f.step(t, supersedeNoneStatus, supersedeProbe("idle"), mismatch, release); err != nil {
			t.Fatal(err)
		}
		f.noMoreCommands(t)
		if rows := f.rows(t); len(rows) != 1 || rows[0].Phase != poolActionRefused {
			t.Fatalf("row=%+v", rows)
		}
		if infos := supersedeInfos(f.destination); len(infos) != 0 {
			t.Fatalf("a refused close sends no info: %+v", infos)
		}
	})
}

// TestSupersedeRefusalWhilePaneRemainsIsAskedAgain: the node refused the close
// because the pane changed state between the hub's read and the close; the
// pane is still the parent's, so the row moves to a new request instead of
// ending, and the parent closes once it has stopped. The retry is bounded.
func TestSupersedeRefusalWhilePaneRemainsIsAskedAgain(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	f.land(t, f.successorTask("implementer", "parent", true))
	first := f.onlyRow(t)
	refused := poolActionReply{op: nodeCommandPaneCloseOp, result: map[string]any{"outcome": fleet.CloseOpRefused, "replayed": false}}
	if err := f.step(t, supersedeNoneStatus, supersedeProbe("idle"), refused, supersedeProbe("working")); err != nil {
		t.Fatal(err)
	}
	rows := f.rows(t)
	if len(rows) != 2 {
		t.Fatalf("a refusal that races a state change must queue the next request: %+v", rows)
	}
	var live poolAction
	for _, row := range rows {
		switch row.ID {
		case first.ID:
			if row.Phase != poolActionRefused {
				t.Fatalf("the refused request must end: %+v", row)
			}
		default:
			live = row
		}
	}
	if live.Phase != poolActionPending || live.Data.Args != nil || live.Data.Supersede.Generation != 1 || live.Data.Supersede.Successor != f.successor {
		t.Fatalf("next request=%+v", live)
	}
	f.closeIdleParent(t)

	// Bounded: every refusal with the pane still there asks again, up to the
	// cap, then the row ends for good.
	g := newSupersedeFixture(t, nil)
	g.land(t, g.successorTask("implementer", "parent", true))
	for i := 0; i < supersedeMaxGenerations; i++ {
		replies := []poolActionReply{supersedeNoneStatus, supersedeProbe("idle"), refused}
		if i < supersedeMaxGenerations-1 {
			replies = append(replies, supersedeProbe("working"))
		}
		if err := g.step(t, replies...); err != nil {
			t.Fatal(err)
		}
	}
	active, err := g.hub.inbox.store.activePoolActions(t.Context())
	if err != nil || len(active) != 0 {
		t.Fatalf("the cap must end the row: %+v %v", active, err)
	}
	if rows := g.rows(t); len(rows) != supersedeMaxGenerations {
		t.Fatalf("rows=%d, want %d generations", len(rows), supersedeMaxGenerations)
	}
}

// TestSupersedeIntentIsCommittedWithTheSucceededSpawn (consult-5195-002ed32a):
// the landing's intent is written in the transaction that makes the successor
// succeeded, only for an explicit implementer parent and only while the switch
// is on; when the intent cannot be stored the outcome is rolled back, so a
// landing is never committed without it.
func TestSupersedeIntentIsCommittedWithTheSucceededSpawn(t *testing.T) {
	t.Run("explicit", func(t *testing.T) {
		f := newSupersedeFixture(t, nil)
		f.hub.inbox.store.landingIntent = f.hub.supersedeLandingOf
		f.landSuccessor(t, "d676220b-e763-4af1-aefd-000000000002", f.successor, true)
		if pending := f.pendingLandings(t); len(pending) != 1 || pending[0].RequestID != "d676220b-e763-4af1-aefd-000000000002" {
			t.Fatalf("the spawn's own transaction must store the intent: %+v", pending)
		}
		f.hub.sweepSupersedeLandings(t.Context())
		f.onlyRow(t)
	})
	t.Run("implicit or switch off", func(t *testing.T) {
		f := newSupersedeFixture(t, nil)
		f.hub.inbox.store.landingIntent = f.hub.supersedeLandingOf
		f.landSuccessor(t, "d676220b-e763-4af1-aefd-000000000002", f.successor, false)
		f.hub.supersedeClose = false
		f.landSuccessor(t, "d676220b-e763-4af1-aefd-000000000003", "successor-2", true)
		f.hub.supersedeClose = true
		if pending := f.pendingLandings(t); len(pending) != 0 {
			t.Fatalf("no intent for an implicit parent or while off: %+v", pending)
		}
	})
	t.Run("a failed intent write rolls the outcome back", func(t *testing.T) {
		f := newSupersedeFixture(t, nil)
		task := f.claimSuccessor(t, "d676220b-e763-4af1-aefd-000000000002", f.successor, true)
		if _, err := f.hub.inbox.store.db.ExecContext(t.Context(), `DROP TABLE supersede_landings`); err != nil {
			t.Fatal(err)
		}
		if changed, err := f.hub.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "w9:p9"}, time.Now()); err == nil || changed {
			t.Fatalf("the outcome must not commit without its intent: changed=%v err=%v", changed, err)
		}
		saved, err := f.hub.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
		if err != nil || saved.Status == "succeeded" {
			t.Fatalf("the landing must stay uncommitted: %+v %v", saved, err)
		}
	})
}

// TestSupersedeLandingAcceptedWhileOffNeverBecomesAClose (review round 4): the
// switch decision is taken when the request is accepted and fenced by the
// generation, so replaying an uncommitted outcome under an enabled hub cannot
// give an off-period landing a fresh intent.
func TestSupersedeLandingAcceptedWhileOffNeverBecomesAClose(t *testing.T) {
	t.Run("accepted while off", func(t *testing.T) {
		f := newSupersedeFixture(t, nil)
		f.hub.supersedeClose = false
		task := f.claimSuccessor(t, "d676220b-e763-4af1-aefd-000000000002", f.successor, true)
		f.hub.supersedeClose = true // enabled before the outcome is replayed
		f.completeSuccessor(t, task)
		f.hub.sweepSupersedeLandings(t.Context())
		if len(f.pendingLandings(t)) != 0 || len(f.rows(t)) != 0 {
			t.Fatalf("an off-period acceptance must never close: %+v %+v", f.pendingLandings(t), f.rows(t))
		}
	})
	t.Run("fence moved before the landing", func(t *testing.T) {
		f := newSupersedeFixture(t, nil)
		task := f.claimSuccessor(t, "d676220b-e763-4af1-aefd-000000000002", f.successor, true)
		f.restartHubWith(t, false) // off start: the fence moves
		f.restartHubWith(t, true)  // enabled start replays the node's result
		f.completeSuccessor(t, task)
		// The landing must not even become pending: the sweep's generation
		// check is a second line of defense, not the first.
		if pending := f.pendingLandings(t); len(pending) != 0 {
			t.Fatalf("a landing armed before an off start must not become an intent: %+v", pending)
		}
		f.hub.sweepSupersedeLandings(t.Context())
		if len(f.rows(t)) != 0 {
			t.Fatalf("a landing armed before an off start must never close: %+v", f.rows(t))
		}
	})
}

// TestHubCLISupersedeOffWithoutTheInboxIsRefused (review rounds 4 and 5): a
// start that runs without the inbox database cannot record the revocation
// --supersede-close=false promises, whether the kill switch or an empty
// store path caused it, so it does not run.
func TestHubCLISupersedeOffWithoutTheInboxIsRefused(t *testing.T) {
	t.Setenv(hubDatabaseURLEnv, "")
	dir := t.TempDir()
	authPath := filepath.Join(dir, "hub-auth.env")
	if err := os.WriteFile(authPath, []byte("HUB_TOKEN_operator="+r6OperatorToken+"\nHUB_TOKEN_node-a="+r6NodeAToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	store := filepath.Join(dir, "inbox.sqlite3")
	for name, args := range map[string][]string{
		"kill switch":            {"--delivery-policy", "off", "--inbox-store", store},
		"kill switch, no store":  {"--delivery-policy", "off", "--inbox-store", ""},
		"no store and no db url": {"--inbox-store", ""},
	} {
		if _, _, code, err := newHubServerForCLI(append([]string{"--hub-auth", authPath, "--supersede-close=false"}, args...), nil); err == nil || code != ExitUsage {
			t.Fatalf("%s: code=%d err=%v, want a usage refusal", name, code, err)
		}
		// The same configuration with the switch left on still starts.
		hub, _, code, err := newHubServerForCLI(append([]string{"--hub-auth", authPath}, args...), nil)
		if err != nil || code != ExitOK || hub == nil {
			t.Fatalf("%s: without the off switch it starts: code=%d err=%v", name, code, err)
		}
		_ = hub.Close()
	}
	// With the inbox, the off start is accepted and records the fence.
	hub, _, code, err := newHubServerForCLI([]string{"--hub-auth", authPath, "--supersede-close=false", "--inbox-store", store}, nil)
	if err != nil || code != ExitOK || hub == nil {
		t.Fatalf("an off start with the inbox must run: code=%d err=%v", code, err)
	}
	_ = hub.Close()
}

// TestSupersedeNeverSendsReapOrWorktreeCommands: the hub no longer closes a
// parent through job.reap, and nothing in the whole close, settlement and
// notice touches the parent's worktree: only the close-op commands and the
// pane read go to the node.
func TestSupersedeNeverSendsReapOrWorktreeCommands(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	f.land(t, f.successorTask("implementer", "parent", true))
	allowed := map[string]bool{nodeCommandPaneCloseStatus: true, nodeCommandAgentsList: true, nodeCommandPaneCloseOp: true}
	pass := func(replies ...poolActionReply) {
		done := make(chan error, 1)
		go func() { done <- f.hub.reconcilePoolAction(t.Context(), readOnlyPoolAction(t, f.hub)) }()
		for _, reply := range replies {
			command := nodeCommandRead(t, f.node)
			var op string
			_ = json.Unmarshal(command["op"], &op)
			if !allowed[op] {
				t.Fatalf("the close must go through the close op only, got %s", op)
			}
			nodeCommandReplyTo(t, f.node, command, map[string]any{"result": reply.result})
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			command, _ := tryNodeCommand(f.node)
			t.Fatalf("the pass sent a command the test did not script: %s", command["op"])
		}
	}
	pass(supersedeNoneStatus, supersedeProbe("idle"), supersedeClosed)
	pass(poolStatus(fleet.CloseOpClosed, false, true))
	f.nodeEnds(supersedeEndReason(f.successor))
	pass() // settlement, the hk read and the info line need no node
	f.noMoreCommands(t)
}

// TestNodeJobReapForcedExpectRecheck: a forced close carrying the proven
// status closes a still-blocked or finished pane, refuses a resumed working
// pane without closing it, and refuses an invalid expectation.
func TestNodeJobReapForcedExpectRecheck(t *testing.T) {
	events := []map[string]any{nodeJobReapClaim("lane-a"), nodeJobReapSpawn("lane-a", false)}
	serve := func(t *testing.T, listed string, expect string, extra ...map[string]json.RawMessage) (map[string]any, string, int) {
		t.Helper()
		inbox := t.TempDir()
		markLostJobEvents(t, inbox, "job-a", events...)
		fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", listed, "/work/a"))}, read: nodeReapScreen("❯ ")}
		client := &HubClient{jobsInboxRoot: inbox, nodeHerdr: fake.run, nodeCloseSettle: time.Millisecond}
		result, code := nodeRun(t, client, nodeCommandJobReap, nodeJobReapArgs{Job: "job-a", Lane: "lane-a", Force: true, Expect: expect})
		_ = extra
		return result, code, fake.called("pane", "close")
	}
	if result, code, closes := serve(t, "blocked", "blocked"); code != "" || result["outcome"] != nodeJobReapClosed || closes != 1 {
		t.Fatalf("still-blocked forced close: result=%v code=%q closes=%d", result, code, closes)
	}
	if result, code, closes := serve(t, "working", "blocked"); code != "" || result["outcome"] != nodeJobReapJobWorking || closes != 0 {
		t.Fatalf("resumed working pane must be refused: result=%v code=%q closes=%d", result, code, closes)
	}
	if result, code, closes := serve(t, "idle", "blocked"); code != "" || result["outcome"] != nodeJobReapClosed || closes != 1 {
		t.Fatalf("finished pane stays closable: result=%v code=%q closes=%d", result, code, closes)
	}
	if _, code, _ := serve(t, "blocked", "working"); code == "" {
		t.Fatal("an invalid expectation must be refused")
	}
}

// TestSupersedeCloseIsOnByDefaultOnTheHubCommandLine: the flag is only an off
// switch (hk 5195).
func TestSupersedeCloseIsOnByDefaultOnTheHubCommandLine(t *testing.T) {
	authPath := filepath.Join(t.TempDir(), "hub-auth.env")
	if err := os.WriteFile(authPath, []byte("HUB_TOKEN_operator="+r6OperatorToken+"\nHUB_TOKEN_node-a="+r6NodeAToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		args []string
		want bool
	}{{nil, true}, {[]string{"--supersede-close=false"}, false}} {
		hub, _, code, err := newHubServerForCLI(append([]string{"--hub-auth", authPath}, tc.args...), nil)
		if err != nil || code != ExitOK || hub == nil {
			t.Fatalf("hub: code=%d err=%v", code, err)
		}
		if hub.supersedeClose != tc.want {
			t.Fatalf("args %v: supersedeClose=%v, want %v", tc.args, hub.supersedeClose, tc.want)
		}
		_ = hub.Close()
	}
}

// TestSupersedeOffSwitchCancelsRowsThatSentNothing: turning --supersede-close
// off stops a queued row that is still waiting on a working pane, and never
// touches a request already dispatched.
func TestSupersedeOffSwitchCancelsRowsThatSentNothing(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	f.land(t, f.successorTask("implementer", "parent", true))
	if err := f.step(t, supersedeNoneStatus, supersedeProbe("working")); err != nil {
		t.Fatal(err)
	}
	restartPoolActionHub(t, f.quotaReplaceFixture)
	f.hub.supersedeClose = false
	// The parent has stopped, but the switch is off: no node command at all.
	if err := f.hub.reconcilePoolAction(t.Context(), readOnlyPoolAction(t, f.hub)); err != nil {
		t.Fatal(err)
	}
	if rows := f.rows(t); len(rows) != 1 || rows[0].Phase != poolActionRefused {
		t.Fatalf("an unsent row must end when the switch is off: %+v", rows)
	}
	f.noMoreCommands(t)

	// A dispatched request is finished, not abandoned.
	g := newSupersedeFixture(t, nil)
	g.land(t, g.successorTask("implementer", "parent", true))
	if err := g.step(t, supersedeNoneStatus, supersedeProbe("idle"), supersedeClosed); err != nil {
		t.Fatal(err)
	}
	g.hub.supersedeClose = false
	if err := g.step(t, poolStatus(fleet.CloseOpClosed, false, true)); err != nil {
		t.Fatal(err)
	}
	if row := g.onlyRow(t); row.Phase != poolActionSettled {
		t.Fatalf("a dispatched close must still settle with the switch off: %+v", row)
	}
}

// claimSuccessor accepts an explicit (or implicit) implementer successor of the
// fixture's parent through the real spawn store.
func (f *supersedeFixture) claimSuccessor(t *testing.T, id, job string, explicit bool) hubSpawnTask {
	t.Helper()
	request := durableSpawnRequest()
	request.RequestID = id
	request.Args = []string{"-c", "/work/project", "-p", "/work/brief.md", "-m", "auto", "-l", job, "--job", job, "--t", "T1", "--parent-job", f.job, "--owner", "parent", "--role", "implementer", "--host", "auto", "--report-relative", job + "/report.md"}
	request.ParentJobExplicit = explicit
	request.Repository.Worktree = job + "-checkout"
	request.Repository.Checkout.Dir = job + "-checkout"
	task, _, err := f.hub.inbox.store.claimSpawn(t.Context(), request, job, "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func (f *supersedeFixture) completeSuccessor(t *testing.T, task hubSpawnTask) {
	t.Helper()
	if changed, err := f.hub.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "w9:p9"}, time.Now()); err != nil || !changed {
		t.Fatalf("land: changed=%v err=%v", changed, err)
	}
}

// landSuccessor accepts and lands a successor.
func (f *supersedeFixture) landSuccessor(t *testing.T, id, job string, explicit bool) hubSpawnTask {
	t.Helper()
	task := f.claimSuccessor(t, id, job, explicit)
	f.completeSuccessor(t, task)
	return task
}

func (f *supersedeFixture) pendingLandings(t *testing.T) []supersedeLanding {
	t.Helper()
	pending, err := f.hub.inbox.store.pendingSupersedeLandings(t.Context(), 1000)
	if err != nil {
		t.Fatal(err)
	}
	return pending
}

// TestSupersedeLandingThatCouldNotDecideIsRetried: the successor landed while
// the parent's node was away or hk failed; the stored intent queues the row
// on a later pass, once, with no new landing.
func TestSupersedeLandingThatCouldNotDecideIsRetried(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	// The parent is not known on any node when the successor lands.
	f.hub.mu.Lock()
	parentRecord := f.hub.jobs[f.job]
	delete(f.hub.jobs, f.job)
	f.hub.mu.Unlock()
	task := f.landSuccessor(t, "d676220b-e763-4af1-aefd-000000000002", f.successor, true)
	f.land(t, task)
	if rows := f.rows(t); len(rows) != 0 {
		t.Fatalf("no row while the parent is unknown: %+v", rows)
	}
	if pending := f.pendingLandings(t); len(pending) != 1 || pending[0].Parent != f.job || pending[0].Successor != f.successor {
		t.Fatalf("the intent must be stored before anything can fail: %+v", pending)
	}
	f.hub.sweepSupersedeLandings(t.Context())
	if len(f.rows(t)) != 0 || len(f.pendingLandings(t)) != 1 {
		t.Fatal("still unknown: the intent stays")
	}
	// The node reports the parent; the next pass queues the row.
	f.hub.mu.Lock()
	f.hub.jobs[f.job] = parentRecord
	f.hub.mu.Unlock()
	f.hub.sweepSupersedeLandings(t.Context())
	f.onlyRow(t)
	if pending := f.pendingLandings(t); len(pending) != 0 {
		t.Fatalf("a queued landing is decided: %+v", pending)
	}
	f.hub.sweepSupersedeLandings(t.Context())
	f.onlyRow(t) // no second row
}

// TestSupersedeIntentSurvivesHubRestartBeforeTheRowExists: the hub restarts
// between the landing and the row; the restarted hub's pass queues it.
func TestSupersedeIntentSurvivesHubRestartBeforeTheRowExists(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	f.hub.mu.Lock()
	delete(f.hub.jobs, f.job)
	f.hub.mu.Unlock()
	f.land(t, f.landSuccessor(t, "d676220b-e763-4af1-aefd-000000000002", f.successor, true))
	restartPoolActionHub(t, f.quotaReplaceFixture)
	f.hub.supersedeClose = true
	f.hub.mu.Lock()
	f.hub.nodes["machine-a"].capabilities[hubCapabilityCloseOps] = true
	f.hub.jobs[f.job] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: f.job, OwnerLane: "parent", Epoch: 1, Pane: roleHeldPane}, Node: "machine-a", LastSeen: time.Now().UTC()}
	f.hub.mu.Unlock()
	f.hub.sweepSupersedeLandings(t.Context())
	f.onlyRow(t)
}

// TestSupersedeLandingsFromADisabledPeriodCloseNothing: a landing while the
// switch is off stores no intent, and intents stored before it was turned off
// end rather than resume when it is turned on again.
func TestSupersedeLandingsFromADisabledPeriodCloseNothing(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	f.hub.supersedeClose = false
	f.land(t, f.landSuccessor(t, "d676220b-e763-4af1-aefd-000000000002", f.successor, true))
	f.hub.supersedeClose = true
	f.hub.sweepSupersedeLandings(t.Context())
	if len(f.pendingLandings(t)) != 0 || len(f.rows(t)) != 0 {
		t.Fatalf("a disabled landing must never become a close: %+v %+v", f.pendingLandings(t), f.rows(t))
	}
	// An intent stored while on and undecided, then the switch goes off.
	f.hub.mu.Lock()
	delete(f.hub.jobs, f.job)
	f.hub.mu.Unlock()
	f.land(t, f.landSuccessor(t, "d676220b-e763-4af1-aefd-000000000003", "successor-2", true))
	if len(f.pendingLandings(t)) != 1 {
		t.Fatal("the undecided intent is stored")
	}
	f.hub.supersedeClose = false
	f.hub.sweepSupersedeLandings(t.Context())
	f.hub.supersedeClose = true
	if len(f.pendingLandings(t)) != 0 {
		t.Fatal("the off switch ends every intent")
	}
}

// TestSupersedeRetryPassIsBoundedAndFair: undecided landings never starve one
// another, and decided ones never occupy the pass.
func TestSupersedeRetryPassIsBoundedAndFair(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	f.hub.mu.Lock()
	delete(f.hub.jobs, f.job)
	f.hub.mu.Unlock()
	total := supersedeLandingBatch + 10
	for i := 0; i < total; i++ {
		l := supersedeLanding{RequestID: fmt.Sprintf("req-%03d", i), Parent: f.job, Successor: fmt.Sprintf("succ-%03d", i), Owner: "parent", Created: time.Now().UnixMilli()}
		if err := f.hub.inbox.store.recordSupersedeLanding(t.Context(), l); err != nil {
			t.Fatal(err)
		}
	}
	f.hub.sweepSupersedeLandings(t.Context())
	pending, err := f.hub.inbox.store.pendingSupersedeLandings(t.Context(), 1000)
	if err != nil || len(pending) != total {
		t.Fatalf("pending=%d err=%v", len(pending), err)
	}
	// After one pass the 50 tried landings sit behind the 10 not yet tried.
	next, err := f.hub.inbox.store.pendingSupersedeLandings(t.Context(), 10)
	if err != nil || len(next) != 10 || next[0].RequestID < fmt.Sprintf("req-%03d", supersedeLandingBatch) {
		t.Fatalf("untried landings must go first: %+v err=%v", next, err)
	}
}

// TestSupersedeOffSwitchFencesAStoredButUnsentRequest: the row saved its
// request, then the hub restarted with the switch off. If the node has not
// started closing, the request is released with a tombstone and never sent;
// if it has, it is finished.
func TestSupersedeOffSwitchFencesAStoredButUnsentRequest(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state string
		sent  bool
	}{{"none", nodeCloseOpNone, false}, {"registered", fleet.CloseOpRegistered, false}, {"closing", fleet.CloseOpClosing, true}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSupersedeFixture(t, nil)
			f.land(t, f.successorTask("implementer", "parent", true))
			// Store the request without sending it, as a stop between the
			// save and the send leaves it.
			row := f.onlyRow(t)
			row.Data.Args = &nodePaneCloseOpArgs{RequestID: row.ID, Job: f.job, Epoch: 1, Attempt: "00002-job.spawned.json", PaneID: roleHeldPane, Name: f.job, AgentStatus: "idle", Cwd: "/work/project", EndReason: row.Data.EndReason}
			if err := f.hub.inbox.store.savePoolAction(t.Context(), row, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			f.hub.supersedeClose = false
			if tc.sent {
				if err := f.step(t, poolStatus(tc.state, false, false), supersedeClosed); err != nil {
					t.Fatal(err)
				}
				if got := f.onlyRow(t); got.Phase != poolActionPending {
					t.Fatalf("a close the node began is finished, not abandoned: %+v", got)
				}
				return
			}
			release := poolActionReply{op: nodeCommandPaneCloseRelease, result: map[string]any{"state": fleet.CloseOpCancelled, "released": true, "settled": false, "attempt": "00002-job.spawned.json"}}
			if err := f.step(t, poolStatus(tc.state, false, false), release); err != nil {
				t.Fatal(err)
			}
			f.noMoreCommands(t)
			if rows := f.rows(t); len(rows) != 1 || rows[0].Phase != poolActionRefused {
				t.Fatalf("the unsent request must be fenced: %+v", rows)
			}
		})
	}
}

// TestSupersedeRetryStaysBoundToTheRefusedAttempt: a refused close is asked
// again only against the attempt it named; a job re-prompted into a new
// attempt on the same pane is released, never closed.
func TestSupersedeRetryStaysBoundToTheRefusedAttempt(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	f.land(t, f.successorTask("implementer", "parent", true))
	refused := poolActionReply{op: nodeCommandPaneCloseOp, result: map[string]any{"outcome": fleet.CloseOpPreempted, "replayed": false}}
	if err := f.step(t, supersedeNoneStatus, supersedeProbe("idle"), refused, supersedeProbe("idle")); err != nil {
		t.Fatal(err)
	}
	live := readOnlyPoolAction(t, f.hub)
	if live.Data.TargetAttempt != "00002-job.spawned.json" || live.Data.Supersede.Generation != 1 {
		t.Fatalf("the retry must name the original attempt: %+v", live.Data)
	}
	// The job was re-prompted: close_status now reports another attempt.
	reprompted := poolActionReply{op: nodeCommandPaneCloseStatus, result: map[string]any{"state": nodeCloseOpNone, "released": false, "settled": false, "attempt": "00005-job.reprompted.json"}}
	release := poolActionReply{op: nodeCommandPaneCloseRelease, result: map[string]any{"state": fleet.CloseOpCancelled, "released": true, "settled": false, "attempt": "00005-job.reprompted.json"}}
	if err := f.step(t, reprompted, release); err != nil {
		t.Fatal(err)
	}
	f.noMoreCommands(t)
	if rows := f.rows(t); rows[len(rows)-1].Phase != poolActionRefused {
		t.Fatalf("a new attempt must never be closed by the old request: %+v", rows)
	}
}

// TestSupersedeDeferredCloseIsBoundToTheAttemptFirstSeen: a close deferred
// because the parent was working binds the parent's attempt at the first
// look, so a parent re-prompted into a new attempt before it goes idle is
// never closed by the old intent.
func TestSupersedeDeferredCloseIsBoundToTheAttemptFirstSeen(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	f.land(t, f.successorTask("implementer", "parent", true))
	if err := f.step(t, supersedeNoneStatus, supersedeProbe("working")); err != nil {
		t.Fatal(err)
	}
	if row := f.onlyRow(t); row.Phase != poolActionPending || row.Data.Args != nil || row.Data.TargetAttempt != "00002-job.spawned.json" {
		t.Fatalf("a deferred row must name the attempt it saw: %+v", row)
	}
	reprompted := poolActionReply{op: nodeCommandPaneCloseStatus, result: map[string]any{"state": nodeCloseOpNone, "released": false, "settled": false, "attempt": "00005-job.reprompted.json"}}
	release := poolActionReply{op: nodeCommandPaneCloseRelease, result: map[string]any{"state": fleet.CloseOpCancelled, "released": true, "settled": false, "attempt": "00005-job.reprompted.json"}}
	if err := f.step(t, reprompted, release); err != nil {
		t.Fatal(err)
	}
	f.noMoreCommands(t)
	if row := f.onlyRow(t); row.Phase != poolActionRefused || row.Data.Args != nil {
		t.Fatalf("a re-prompted parent must never be closed by the old intent: %+v", row)
	}
}

// restartHubWith restarts the fixture's hub with the switch set as given, as an
// operator restarting it with or without --supersede-close.
func (f *supersedeFixture) restartHubWith(t *testing.T, on bool) {
	t.Helper()
	f.hub.supersedeClose = on
	restartPoolActionHub(t, f.quotaReplaceFixture)
	f.hub.mu.Lock()
	f.hub.nodes["machine-a"].capabilities[hubCapabilityCloseOps] = true
	f.hub.jobs[f.job] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: f.job, OwnerLane: "parent", Epoch: 1, Pane: roleHeldPane}, Node: "machine-a", LastSeen: time.Now().UTC()}
	f.hub.mu.Unlock()
}

// storeUnsentRequest saves a close request on the row without sending it, as a
// stop between the save and the send leaves it.
func (f *supersedeFixture) storeUnsentRequest(t *testing.T) {
	t.Helper()
	row := f.onlyRow(t)
	row.Data.Args = &nodePaneCloseOpArgs{RequestID: row.ID, Job: f.job, Epoch: 1, Attempt: "00002-job.spawned.json", PaneID: roleHeldPane, Name: f.job, AgentStatus: "idle", Cwd: "/work/project", EndReason: row.Data.EndReason}
	if err := f.hub.inbox.store.savePoolAction(t.Context(), row, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
}

// TestSupersedeRevocationSurvivesAnOfflineNodeAndAnEnabledRestart is
// consult-5195-002ed32a (PRRT_kwDOUGxnCM642fyp): a hub started with the switch
// off revokes a stored request; a later enabled hub, started before the node
// reconnects and so unable to release it, must not send it. It stays
// cancellation-only until the release tombstone is proved.
func TestSupersedeRevocationSurvivesAnOfflineNodeAndAnEnabledRestart(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	f.land(t, f.successorTask("implementer", "parent", true))
	f.storeUnsentRequest(t)
	f.restartHubWith(t, false) // off start: advances the fence, node never asked
	f.restartHubWith(t, true)  // enabled start, node still unreachable for release
	row := f.onlyRow(t)
	// The node does not answer the status call: the row must stay pending and
	// nothing may be sent.
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- f.hub.reconcilePoolAction(ctx, row) }()
	command := nodeCommandRead(t, f.node)
	if string(command["op"]) != `"`+nodeCommandPaneCloseStatus+`"` {
		t.Fatalf("a revoked request must first ask the node's status, got %s", command["op"])
	}
	if err := <-done; err == nil {
		t.Fatal("an unanswered node must leave the revoked row pending")
	}
	if got := f.onlyRow(t); got.Phase != poolActionPending {
		t.Fatalf("row=%+v", got)
	}
	// The node answers later: the request is released, never sent.
	release := poolActionReply{op: nodeCommandPaneCloseRelease, result: map[string]any{"state": fleet.CloseOpCancelled, "released": true, "settled": false, "attempt": "00002-job.spawned.json"}}
	if err := f.step(t, supersedeNoneStatus, release); err != nil {
		t.Fatal(err)
	}
	f.noMoreCommands(t)
	if rows := f.rows(t); len(rows) != 1 || rows[0].Phase != poolActionRefused {
		t.Fatalf("rows=%+v", rows)
	}
}

// TestSupersedeOffStartRevokesIntentsAndRowsBeforeAnyWork: an off start moves
// the fence synchronously; a later enabled hub cancels a landing intent and a
// row that sent nothing without any sweep having run in between.
func TestSupersedeOffStartRevokesIntentsAndRowsBeforeAnyWork(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	f.land(t, f.successorTask("implementer", "parent", true)) // a row queued
	f.hub.mu.Lock()
	delete(f.hub.jobs, f.job)
	f.hub.mu.Unlock()
	if err := f.hub.inbox.store.recordSupersedeLanding(t.Context(), supersedeLanding{RequestID: "req-old", Parent: "other-parent", Successor: "succ-old", Owner: "parent", Created: time.Now().UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	f.restartHubWith(t, false)
	// The off start itself ends the unsent row, in a final phase every binary
	// reads (a rollback must not dispatch it); no reconcile has run.
	if rows := f.rows(t); len(rows) != 1 || rows[0].Phase != poolActionRefused {
		t.Fatalf("an off start must end a row that sent nothing: %+v", rows)
	}
	f.restartHubWith(t, true)
	f.hub.sweepSupersedeLandings(t.Context())
	if pending := f.pendingLandings(t); len(pending) != 0 {
		t.Fatalf("an intent of an earlier generation is cancelled: %+v", pending)
	}
	f.noMoreCommands(t)
}

// TestSupersedeOffStartFailsWhenTheFenceCannotBeRecorded: an off start that
// cannot persist the revocation does not run.
func TestSupersedeOffStartFailsWhenTheFenceCannotBeRecorded(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	if _, err := f.hub.inbox.store.db.ExecContext(t.Context(), `DROP TABLE supersede_fence`); err != nil {
		t.Fatal(err)
	}
	f.hub.supersedeClose = false
	if err := f.hub.initSupersedeFence(); err == nil {
		t.Fatal("an off start must fail when the fence is not recorded")
	}
	// An enabled start that cannot read the fence keeps the configured switch:
	// work is withheld and left unchanged, not ended.
	f.hub.supersedeClose = true
	if err := f.hub.initSupersedeFence(); err != nil || !f.hub.supersedeClose {
		t.Fatalf("an enabled start must not clear the switch: err=%v on=%v", err, f.hub.supersedeClose)
	}
	if fenced, err := f.hub.supersedeFenced(t.Context(), poolAction{Data: poolActionData{Supersede: &poolSupersede{Fence: 1}}}); err == nil || !fenced {
		t.Fatalf("an unreadable fence withholds dispatch with an error: fenced=%v err=%v", fenced, err)
	}
}

// TestSupersedeUnreadableFenceLeavesPendingWorkUntouched: while the fence is
// unreadable the landing pass ends nothing, and once it reads again the work
// resumes (review round 6).
func TestSupersedeUnreadableFenceLeavesPendingWorkUntouched(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	f.hub.mu.Lock()
	delete(f.hub.jobs, f.job)
	f.hub.mu.Unlock()
	f.land(t, f.successorTask("implementer", "parent", true)) // pending, parent unknown
	if len(f.pendingLandings(t)) != 1 {
		t.Fatal("the intent is pending")
	}
	if _, err := f.hub.inbox.store.db.ExecContext(t.Context(), `ALTER TABLE supersede_fence RENAME TO supersede_fence_away`); err != nil {
		t.Fatal(err)
	}
	if err := f.hub.initSupersedeFence(); err != nil || !f.hub.supersedeClose {
		t.Fatalf("enabled start: err=%v on=%v", err, f.hub.supersedeClose)
	}
	f.hub.sweepSupersedeLandings(t.Context())
	if len(f.pendingLandings(t)) != 1 {
		t.Fatal("an unreadable fence must not end a pending intent")
	}
	if _, err := f.hub.inbox.store.db.ExecContext(t.Context(), `ALTER TABLE supersede_fence_away RENAME TO supersede_fence`); err != nil {
		t.Fatal(err)
	}
	f.hub.mu.Lock()
	f.hub.jobs[f.job] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: f.job, OwnerLane: "parent", Epoch: 1, Pane: roleHeldPane}, Node: "machine-a", LastSeen: time.Now().UTC()}
	f.hub.mu.Unlock()
	f.hub.sweepSupersedeLandings(t.Context())
	f.onlyRow(t)
}

// TestSupersedeCancelledRowCreatesNoReplacement: a revoked request the node
// refuses or loses is never asked again under a new id.
func TestSupersedeCancelledRowCreatesNoReplacement(t *testing.T) {
	for _, state := range []string{fleet.CloseOpRefused, fleet.CloseOpPreempted} {
		f := newSupersedeFixture(t, nil)
		f.land(t, f.successorTask("implementer", "parent", true))
		f.storeUnsentRequest(t)
		f.hub.supersedeClose = false
		if err := f.step(t, poolStatus(state, false, false)); err != nil {
			t.Fatal(err)
		}
		if rows := f.rows(t); len(rows) != 1 || rows[0].Phase != poolActionRefused {
			t.Fatalf("%s: a cancelled row must end without a replacement: %+v", state, rows)
		}
	}
}

// TestSupersedeRevokedRequestWithNoAttemptOnTheNodeIsRetired (review round 6):
// when the node holds no attempt the stored request could bind to (its job
// directory expired), a close op for it cannot close anything and no
// tombstone can be written, so the revoked row retires on that proof alone.
func TestSupersedeRevokedRequestWithNoAttemptOnTheNodeIsRetired(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	f.land(t, f.successorTask("implementer", "parent", true))
	f.storeUnsentRequest(t)
	f.hub.supersedeClose = false
	gone := poolActionReply{op: nodeCommandPaneCloseStatus, result: map[string]any{"state": nodeCloseOpNone, "released": false, "settled": false, "attempt": ""}}
	if err := f.step(t, gone); err != nil {
		t.Fatal(err)
	}
	f.noMoreCommands(t)
	if rows := f.rows(t); len(rows) != 1 || rows[0].Phase != poolActionRefused {
		t.Fatalf("rows=%+v", rows)
	}
	// An ordinary none (the node still has the job) is not a tombstone.
	g := newSupersedeFixture(t, nil)
	g.land(t, g.successorTask("implementer", "parent", true))
	g.storeUnsentRequest(t)
	g.hub.supersedeClose = false
	noRelease := poolActionReply{op: nodeCommandPaneCloseRelease, result: map[string]any{"state": nodeCloseOpNone, "released": false, "settled": false, "attempt": "00002-job.spawned.json"}}
	if err := g.step(t, supersedeNoneStatus, noRelease); err == nil {
		t.Fatal("an ordinary none answer must not retire a revoked request")
	}
	if got := g.onlyRow(t); got.Phase != poolActionPending {
		t.Fatalf("row=%+v", got)
	}
}

// TestSupersedeRetriesShareOneChainAndBudget (review round 6): an unknown
// request that already has a successor, or whose chain spent the budget,
// mints no further request.
func TestSupersedeRetriesShareOneChainAndBudget(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	f.land(t, f.successorTask("implementer", "parent", true))
	f.storeUnsentRequest(t)
	unknown := poolStatus(fleet.CloseOpUnknown, false, false)
	if err := f.step(t, unknown, supersedeProbe("idle")); err != nil {
		t.Fatal(err)
	}
	rows := f.rows(t)
	if len(rows) != 2 {
		t.Fatalf("an unknown request mints one successor: %+v", rows)
	}
	var first, second poolAction
	for _, row := range rows {
		if row.Data.Args != nil {
			first = row
		} else {
			second = row
		}
	}
	if second.Data.Supersede.Generation != first.Data.Supersede.Generation+1 {
		t.Fatalf("the unknown retry must spend the shared budget: %+v %+v", first.Data.Supersede, second.Data.Supersede)
	}
	if may, err := f.hub.supersedeMayRetry(t.Context(), first); err != nil || may {
		t.Fatalf("a request whose chain already has a live successor must not mint another: may=%v err=%v", may, err)
	}
	// The successor and its descendants are all refused: the original, which
	// already named its successor, still mints nothing, and a chain that
	// reached the cap in a final row mints nothing either.
	second.Phase = poolActionRefused
	if err := f.hub.inbox.store.savePoolAction(t.Context(), second, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	rows = f.rows(t)
	for _, row := range rows {
		if row.ID == first.ID {
			first = row
		}
	}
	if first.Data.RetryID == "" {
		t.Fatalf("the unknown request must name its successor: %+v", first.Data)
	}
	if may, err := f.hub.supersedeMayRetry(t.Context(), first); err != nil || may {
		t.Fatalf("an ancestor that already minted must never mint again, even after its successor was refused: may=%v err=%v", may, err)
	}
	// A fresh unknown request (no successor yet) is stopped by the budget of
	// the whole chain, final rows included.
	deep := second
	deep.ID = "deep-final-row"
	deep.Data.Supersede = &poolSupersede{Generation: supersedeMaxGenerations - 1, Fence: first.Data.Supersede.Fence, Successor: f.successor}
	deep.Data.Args = nil
	if err := f.hub.inbox.store.insertPoolAction(t.Context(), deep, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	fresh := first
	fresh.Data.RetryID = ""
	if may, err := f.hub.supersedeMayRetry(t.Context(), fresh); err != nil || may {
		t.Fatalf("a chain whose final row spent the budget must not retry: may=%v err=%v", may, err)
	}
}

// TestHubBuiltSuccessorsNeverArmTheirParentsClose (review round 6): a quota
// replacement and an author ladder step copy the replaced request; the copy is
// no submitter's typed --parent-job.
func TestHubBuiltSuccessorsNeverArmTheirParentsClose(t *testing.T) {
	old := quotaHolderRequest("implementer", "mwr-9-p1")
	old.ParentJobExplicit = true
	old.Args = append(old.Args, "--parent-job", "grand-parent")
	replaced, err := quotaReplacementRequest(old, "mwr-9-p1", "mwr-9-p1-q1", "claude", "claude", "implementer")
	if err != nil {
		t.Fatal(err)
	}
	if replaced.ParentJobExplicit {
		t.Fatal("a quota replacement must not inherit the explicit parent flag")
	}
	f := newEscalateFixture(t, "author-explicit")
	holder := escalateHolderRequest(f.job)
	holder.ParentJobExplicit = true
	next, err := escalateAuthorSubmission(holder, f.request(), f.job+"-l1")
	if err != nil {
		t.Fatal(err)
	}
	if next.ParentJobExplicit {
		t.Fatal("an author ladder step must not inherit the explicit parent flag")
	}
}

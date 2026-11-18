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
	f.hub.maybeSupersedeParent(f.successorTask("implementer", "parent", true))
	f.hub.maybeSupersedeParent(f.successorTask("implementer", "parent", true))
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
	if got := f.superseded(); got != f.successor {
		t.Fatalf("job.superseded=%q, want %q", got, f.successor)
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
	f.hub.maybeSupersedeParent(f.successorTask("implementer", "parent", true))
	if rows := f.rows(t); len(rows) != 1 {
		t.Fatalf("one row per parent, got %+v", rows)
	}
}

// TestSupersedeImplicitParentCreatesNoRow: a parent filled from ARBITER_JOB is
// not a request to replace it. Older clients send no field, which reads the
// same.
func TestSupersedeImplicitParentCreatesNoRow(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	f.hub.maybeSupersedeParent(f.successorTask("implementer", "parent", false))
	if rows := f.rows(t); len(rows) != 0 {
		t.Fatalf("an implicit parent must create no row: %+v", rows)
	}
	var legacy fleet.Submission
	if err := json.Unmarshal([]byte(`{"request_id":"d676220b-e763-4af1-aefd-0000000000a1","args":["--job","successor-job","--parent-job","parent-job","--owner","parent","--role","implementer"]}`), &legacy); err != nil {
		t.Fatal(err)
	}
	task := f.successorTask("implementer", "parent", false)
	task.Request = legacy
	f.hub.maybeSupersedeParent(task)
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
			f.hub.maybeSupersedeParent(f.successorTask(role, "parent", true))
		}
		if rows := f.rows(t); len(rows) != 0 {
			t.Fatalf("a %s child must create no row: %+v", "reviewer or consult", rows)
		}
	})
	t.Run("keep parent", func(t *testing.T) {
		f := newSupersedeFixture(t, func(request *fleet.Submission) { request.Args = append(request.Args, "--keep") })
		f.hub.maybeSupersedeParent(f.successorTask("implementer", "parent", true))
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
		f.hub.maybeSupersedeParent(task)
		rows, err := f.hub.inbox.store.supersedeActionsOf(t.Context(), "local-job")
		if err != nil || len(rows) != 0 {
			t.Fatalf("a parent with no spawn record must create no row: %+v %v", rows, err)
		}
	})
	t.Run("another lane", func(t *testing.T) {
		f := newSupersedeFixture(t, nil)
		f.hub.maybeSupersedeParent(f.successorTask("implementer", "other-lane", true))
		if rows := f.rows(t); len(rows) != 0 {
			t.Fatalf("a foreign successor must create no row: %+v", rows)
		}
	})
	t.Run("switch off", func(t *testing.T) {
		f := newSupersedeFixture(t, nil)
		f.hub.supersedeClose = false
		f.hub.maybeSupersedeParent(f.successorTask("implementer", "parent", true))
		if rows := f.rows(t); len(rows) != 0 {
			t.Fatalf("with the switch off no row exists: %+v", rows)
		}
	})
	t.Run("ended parent", func(t *testing.T) {
		f := newSupersedeFixture(t, nil)
		f.hub.mu.Lock()
		f.hub.jobs[f.job].Completed = true
		f.hub.mu.Unlock()
		f.hub.maybeSupersedeParent(f.successorTask("implementer", "parent", true))
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
	f.hub.maybeSupersedeParent(f.successorTask("implementer", "parent", true))
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
	if got := f.superseded(); got != f.successor {
		t.Fatalf("a waiting parent is still recorded superseded, got %q", got)
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
	f.hub.maybeSupersedeParent(f.successorTask("implementer", "parent", true))
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
	f.hub.maybeSupersedeParent(f.successorTask("implementer", "parent", true))
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

// flakySupersededStore fails hk writes of the superseded field while told to.
type flakySupersededStore struct {
	fleet.JobStore
	mu    sync.Mutex
	fail  bool
	tries int
}

func (s *flakySupersededStore) Put(ctx context.Context, id string, fields map[string]any) error {
	if _, ok := fields["superseded"]; ok {
		s.mu.Lock()
		s.tries++
		fail := s.fail
		s.mu.Unlock()
		if fail {
			return errors.New("handoffkeep unavailable")
		}
	}
	return s.JobStore.Put(ctx, id, fields)
}

func (s *flakySupersededStore) setFail(fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail = fail
}

func (s *flakySupersededStore) attempts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tries
}

// TestSupersedeFailedHKWriteIsRetriedNotConsumed is acceptance: a failed
// job.superseded write leaves the row pending with no close request; the next
// pass writes it and goes on, and later passes do not write again.
func TestSupersedeFailedHKWriteIsRetriedNotConsumed(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	flaky := &flakySupersededStore{JobStore: f.jobs, fail: true}
	f.hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: flaky, Out: out, Err: errout}, nil
	}
	f.hub.maybeSupersedeParent(f.successorTask("implementer", "parent", true))
	if err := f.step(t); err == nil {
		t.Fatal("a failed hk write must not be consumed")
	}
	if row := f.onlyRow(t); row.Phase != poolActionPending || row.Data.Supersede.Recorded {
		t.Fatalf("row=%+v", row)
	}
	flaky.setFail(false)
	f.closeIdleParent(t)
	if got := f.superseded(); got != f.successor {
		t.Fatalf("retry must confirm the record: %q", got)
	}
	if got := flaky.attempts(); got != 2 {
		t.Fatalf("want one failed and one confirming write, got %d", got)
	}
	if infos := supersedeInfos(f.destination); len(infos) != 1 {
		t.Fatalf("one info: %+v", infos)
	}
}

// TestSupersedePaneThatChangedHandsIsRefusedWithoutClosing is acceptance: a
// pane that is no longer the parent's is never closed, by the hub's read or
// by the node's own check inside the close operation.
func TestSupersedePaneThatChangedHandsIsRefusedWithoutClosing(t *testing.T) {
	t.Run("hub read finds another pane under the label", func(t *testing.T) {
		f := newSupersedeFixture(t, nil)
		f.hub.maybeSupersedeParent(f.successorTask("implementer", "parent", true))
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
		f.hub.maybeSupersedeParent(f.successorTask("implementer", "parent", true))
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
	f.hub.maybeSupersedeParent(f.successorTask("implementer", "parent", true))
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
	g.hub.maybeSupersedeParent(g.successorTask("implementer", "parent", true))
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

// TestSupersedeLandingHookQueuesRowFromTheSucceededSpawn: the succeeded spawn
// operation itself creates the row, but only for an explicit parent.
func TestSupersedeLandingHookQueuesRowFromTheSucceededSpawn(t *testing.T) {
	for _, explicit := range []bool{true, false} {
		f := newSupersedeFixture(t, nil)
		request := durableSpawnRequest()
		request.RequestID = "d676220b-e763-4af1-aefd-000000000002"
		request.Args = []string{"-c", "/work/project", "-p", "/work/brief.md", "-m", "auto", "-l", f.successor, "--job", f.successor, "--t", "T1", "--parent-job", f.job, "--owner", "parent", "--role", "implementer", "--host", "auto", "--report-relative", f.successor + "/report.md"}
		request.ParentJobExplicit = explicit
		request.Repository.Worktree = f.successor + "-checkout"
		request.Repository.Checkout.Dir = f.successor + "-checkout"
		task, _, err := f.hub.inbox.store.claimSpawn(t.Context(), request, f.successor, "parent", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		changed, err := f.hub.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "w9:p9"})
		if err != nil || !changed {
			t.Fatalf("successor spawn must land: changed=%v err=%v", changed, err)
		}
		if explicit {
			eventually(t, 3*time.Second, "the landing to queue the close row", func() bool {
				rows, err := f.hub.inbox.store.supersedeActionsOf(t.Context(), f.job)
				return err == nil && len(rows) == 1
			})
			continue
		}
		// An implicit parent queues nothing; the landing hook has run once
		// the next explicit landing's row exists, so check after a real
		// explicit landing for another parent in this same hub.
		f.hub.maybeSupersedeParent(task)
		if rows := f.rows(t); len(rows) != 0 {
			t.Fatalf("an implicit landing must queue nothing: %+v", rows)
		}
	}
}

// TestSupersedeNeverSendsReapOrWorktreeCommands: the hub no longer closes a
// parent through job.reap, and nothing in the whole close, settlement and
// notice touches the parent's worktree: only the close-op commands and the
// pane read go to the node.
func TestSupersedeNeverSendsReapOrWorktreeCommands(t *testing.T) {
	f := newSupersedeFixture(t, nil)
	f.hub.maybeSupersedeParent(f.successorTask("implementer", "parent", true))
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
	f.hub.maybeSupersedeParent(f.successorTask("implementer", "parent", true))
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
	g.hub.maybeSupersedeParent(g.successorTask("implementer", "parent", true))
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

// landSuccessor lands an explicit (or implicit) implementer successor of the
// fixture's parent through the real spawn store, with the landing hook run by
// the test, and returns its task.
func (f *supersedeFixture) landSuccessor(t *testing.T, id, job string, explicit bool) hubSpawnTask {
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
	if changed, err := f.hub.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "w9:p9"}, time.Now()); err != nil || !changed {
		t.Fatalf("land: changed=%v err=%v", changed, err)
	}
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
	f.hub.maybeSupersedeParent(task)
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
	f.hub.maybeSupersedeParent(f.landSuccessor(t, "d676220b-e763-4af1-aefd-000000000002", f.successor, true))
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
	f.hub.maybeSupersedeParent(f.landSuccessor(t, "d676220b-e763-4af1-aefd-000000000002", f.successor, true))
	f.hub.supersedeClose = true
	f.hub.sweepSupersedeLandings(t.Context())
	if len(f.pendingLandings(t)) != 0 || len(f.rows(t)) != 0 {
		t.Fatalf("a disabled landing must never become a close: %+v %+v", f.pendingLandings(t), f.rows(t))
	}
	// An intent stored while on and undecided, then the switch goes off.
	f.hub.mu.Lock()
	delete(f.hub.jobs, f.job)
	f.hub.mu.Unlock()
	f.hub.maybeSupersedeParent(f.landSuccessor(t, "d676220b-e763-4af1-aefd-000000000003", "successor-2", true))
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
			f.hub.maybeSupersedeParent(f.successorTask("implementer", "parent", true))
			// Store the request without sending it, as a stop between the
			// save and the send leaves it.
			row := f.onlyRow(t)
			row.Data.Supersede.Recorded = true
			row.Data.Args = &nodePaneCloseOpArgs{RequestID: row.ID, Job: f.job, Epoch: 1, Attempt: "00002-job.spawned.json", PaneID: roleHeldPane, Name: f.job, AgentStatus: "idle", Cwd: "/work/project", EndReason: row.Data.EndReason}
			if err := f.hub.inbox.store.savePoolAction(t.Context(), row); err != nil {
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
	f.hub.maybeSupersedeParent(f.successorTask("implementer", "parent", true))
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

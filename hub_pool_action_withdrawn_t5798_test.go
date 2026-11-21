package panewire

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 5798: a fenced hand-over the hub gave up after a node downgrade is a durable
// withdrawn row. It is never offered a close or a retry, it keeps the job's fence,
// and it ends only when the node proves the close intent settled or cancelled.

var closingStatus = poolActionReply{op: nodeCommandPaneCloseStatus, result: map[string]any{"state": fleet.CloseOpClosing, "released": false, "settled": false, "attempt": "00002-job.spawned.json"}}

func statusOf(state string, released, settled bool) poolActionReply {
	return poolActionReply{op: nodeCommandPaneCloseStatus, result: map[string]any{"state": state, "released": released, "settled": settled, "attempt": "00002-job.spawned.json"}}
}

// closingOnDowngradedNode queues a fenced hand-over, lets the node begin its close,
// and withdraws the close fence capability: the row the next pass meets is closing
// on a node that can no longer fence it.
func closingOnDowngradedNode(t *testing.T, job string) (*quotaReplaceFixture, poolAction) {
	t.Helper()
	f := announcedFixture(t, job)
	queueAnnouncedRow(t, f, "ev-1")
	action := readOnlyPoolAction(t, f.hub)
	if err := runPoolActionStep(t, f.hub, f.node, action,
		poolStatus(nodeCloseOpNone, false, false),
		idlePane(job, "/work/project.checkout", "idle"),
		idlePane(job, "/work/project.checkout", "idle"),
		poolActionReply{op: nodeCommandPaneCloseOp, result: map[string]any{"outcome": fleet.CloseOpClosed, "replayed": false}},
	); err != nil {
		t.Fatal(err)
	}
	f.hub.mu.Lock()
	delete(f.hub.nodes["machine-a"].capabilities, hubCapabilityCloseOpStopSeq)
	f.hub.mu.Unlock()
	return f, readOnlyPoolAction(t, f.hub)
}

// stepRecording is runPoolActionStep that also returns each command's arguments.
func stepRecording(t *testing.T, f *quotaReplaceFixture, action poolAction, replies ...poolActionReply) ([]map[string]json.RawMessage, error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- f.hub.reconcilePoolAction(t.Context(), action) }()
	var commands []map[string]json.RawMessage
	for _, reply := range replies {
		command := nodeCommandRead(t, f.node)
		var op string
		if json.Unmarshal(command["op"], &op) != nil || op != reply.op {
			t.Fatalf("command=%s, want %s", command["op"], reply.op)
		}
		commands = append(commands, command)
		nodeCommandReplyTo(t, f.node, command, map[string]any{"result": reply.result})
	}
	return commands, <-done
}

func assertWithdrawnAndFenced(t *testing.T, f *quotaReplaceFixture, action poolAction) {
	t.Helper()
	rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), action.Job)
	if len(rows) != 1 || rows[0].ID != action.ID || rows[0].Phase != poolActionPending || !rows[0].Data.Withdrawn {
		t.Fatalf("rows=%+v, want the one original row, still withdrawn", rows)
	}
	if owns, _ := f.hub.inbox.store.poolActionOwns(t.Context(), action.Job); owns != action.ID {
		t.Fatalf("the job's fence is %q, want %s", owns, action.ID)
	}
}

// Release answers released=false for a closing request, so the downgrade give-up
// must not retire the row: it stays withdrawn and is reconciled each pass, and no
// pass closes. Only the node's proof of settlement retires it.
func TestWithdrawnHandoverStaysReconciledUntilTheNodeProvesSettled_t5798(t *testing.T) {
	const job = "pw-withdrawn-5798"
	f, action := closingOnDowngradedNode(t, job)
	if err := runPoolActionStep(t, f.hub, f.node, action, closingStatus); err != nil {
		t.Fatal(err)
	}
	assertWithdrawnAndFenced(t, f, action)

	// The next pass settles the journal without a pane close: the repeat carries no
	// stop_seq (a downgraded node rejects it) and the node answers unknown.
	commands, err := stepRecording(t, f, readOnlyPoolAction(t, f.hub), closingStatus,
		poolActionReply{op: nodeCommandPaneCloseOp, result: map[string]any{"outcome": fleet.CloseOpUnknown, "replayed": false}})
	if err != nil {
		t.Fatal(err)
	}
	var args map[string]json.RawMessage
	if err := json.Unmarshal(commands[1]["args"], &args); err != nil {
		t.Fatal(err)
	}
	if _, has := args["stop_seq"]; has {
		t.Fatalf("the journal-only recovery carries stop_seq: %s", commands[1]["args"])
	}
	assertWithdrawnAndFenced(t, f, action)

	// Unknown and not settled: the pane may still be open. No probe, no retry row,
	// and the row waits.
	for range 2 {
		if err := runPoolActionStep(t, f.hub, f.node, readOnlyPoolAction(t, f.hub), statusOf(fleet.CloseOpUnknown, false, false)); err != nil {
			t.Fatal(err)
		}
		assertWithdrawnAndFenced(t, f, action)
	}
	if lines := ownerLines(t, f.hub, "pool-handover:"); len(lines) != 1 {
		t.Fatalf("owner notices=%q, want exactly one", lines)
	}
	if ids := spawnOperations(t, f.hub, job+"-q1"); len(ids) != 0 {
		t.Fatalf("a successor was started: %v", ids)
	}

	// The node's proof of settlement retires it and frees the fence.
	if err := runPoolActionStep(t, f.hub, f.node, readOnlyPoolAction(t, f.hub), statusOf(fleet.CloseOpUnknown, false, true)); err != nil {
		t.Fatal(err)
	}
	rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), job)
	if len(rows) != 1 || rows[0].Phase != poolActionRefused {
		t.Fatalf("rows=%+v, want the settled withdrawal retired", rows)
	}
	if owns, _ := f.hub.inbox.store.poolActionOwns(t.Context(), job); owns != "" {
		t.Fatalf("the job is still fenced by %s", owns)
	}
}

// A cancelled intent hands the row to the guarded phase (its fence outlives the
// intent until the old job cannot write) rather than retiring it.
func TestWithdrawnHandoverWithACancelledIntentIsGuarded_t5798(t *testing.T) {
	f, action := closingOnDowngradedNode(t, "pw-cancelled-5798")
	if err := runPoolActionStep(t, f.hub, f.node, action, closingStatus); err != nil {
		t.Fatal(err)
	}
	if err := runPoolActionStep(t, f.hub, f.node, readOnlyPoolAction(t, f.hub), statusOf(nodeCloseIntentCancelled, false, false)); err != nil {
		t.Fatal(err)
	}
	rows, _ := f.hub.inbox.store.poolActionsOf(t.Context(), action.Job)
	if len(rows) != 1 || rows[0].Phase != poolActionGuarded {
		t.Fatalf("rows=%+v, want the row guarded", rows)
	}
}

// The hub crashes between the give-up's two writes (handover_failed recorded, the
// final phase not), and a capable
// node reconnects with the journal still closing (then unknown). The row is
// already withdrawn: no close is dispatched, no retry row is created, and the
// owner's notice is delivered once.
func TestWithdrawnHandoverSurvivesACrashAfterItsFirstWriteAndACapableReconnect_t5798(t *testing.T) {
	const job = "pw-crash-5798"
	f, action := closingOnDowngradedNode(t, job)
	// The crash: the hub dies after the first row write of the give-up. That write
	// must already carry handover_failed and the withdrawal together; any later
	// write (the notice's marker, the final phase) is lost.
	db := f.hub.inbox.store.db
	for _, ddl := range []string{
		`CREATE TABLE crash_budget(n INTEGER NOT NULL)`,
		`INSERT INTO crash_budget VALUES(1)`,
		`CREATE TRIGGER crash_after_first BEFORE UPDATE ON pool_actions WHEN (SELECT n FROM crash_budget)<=0 BEGIN SELECT RAISE(ABORT,'crash'); END`,
		`CREATE TRIGGER crash_count AFTER UPDATE ON pool_actions BEGIN UPDATE crash_budget SET n=n-1; END`,
	} {
		if _, err := db.ExecContext(t.Context(), ddl); err != nil {
			t.Fatal(err)
		}
	}
	_ = runPoolActionStep(t, f.hub, f.node, action, closingStatus)
	for _, ddl := range []string{`DROP TRIGGER crash_after_first`, `DROP TRIGGER crash_count`, `DROP TABLE crash_budget`} {
		if _, err := db.ExecContext(t.Context(), ddl); err != nil {
			t.Fatal(err)
		}
	}
	assertWithdrawnAndFenced(t, f, action)

	// A capable node reconnects: the journal answers closing, then unknown.
	f.hub.mu.Lock()
	f.hub.nodes["machine-a"].capabilities[hubCapabilityCloseOpStopSeq] = true
	f.hub.mu.Unlock()
	commands, err := stepRecording(t, f, readOnlyPoolAction(t, f.hub), closingStatus,
		poolActionReply{op: nodeCommandPaneCloseOp, result: map[string]any{"outcome": fleet.CloseOpUnknown, "replayed": false}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(commands[1]["args"]), "stop_seq") {
		t.Fatalf("the recovery sent a fenced close: %s", commands[1]["args"])
	}
	// Unknown is what replays into a retry on a row that is not withdrawn: the
	// script has no probe, and no row may appear.
	if err := runPoolActionStep(t, f.hub, f.node, readOnlyPoolAction(t, f.hub), statusOf(fleet.CloseOpUnknown, false, false)); err != nil {
		t.Fatal(err)
	}
	assertWithdrawnAndFenced(t, f, action)
	if lines := ownerLines(t, f.hub, "pool-handover:"); len(lines) != 1 {
		t.Fatalf("owner notices=%q, want the one notice", lines)
	}
}

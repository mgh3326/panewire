package panewire

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// guardedPoolAckStep runs one pass over a guarded action and answers the
// node's close_status, returning its arguments.
func guardedPoolAckStep(t *testing.T, f *quotaReplaceFixture, action poolAction, result map[string]any) map[string]any {
	t.Helper()
	args, err := guardedPoolAckReply(t, f, action, map[string]any{"result": result})
	if err != nil {
		t.Fatalf("guarded pass: %v", err)
	}
	return args
}

// guardedPoolAckReply runs one pass over a guarded action, answers the node's
// close_status with reply (a result or an error code) and returns the command's
// arguments and the pass's error.
func guardedPoolAckReply(t *testing.T, f *quotaReplaceFixture, action poolAction, reply map[string]any) (map[string]any, error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- f.hub.reconcilePoolAction(t.Context(), action) }()
	command := nodeCommandRead(t, f.node)
	var op string
	if json.Unmarshal(command["op"], &op) != nil || op != nodeCommandPaneCloseStatus {
		t.Fatalf("command=%s, want %s", command["op"], nodeCommandPaneCloseStatus)
	}
	var args map[string]any
	if err := json.Unmarshal(command["args"], &args); err != nil {
		t.Fatal(err)
	}
	nodeCommandReplyTo(t, f.node, command, reply)
	select {
	case err := <-done:
		return args, err
	case <-time.After(5 * time.Second):
		t.Fatal("guarded pass did not finish")
	}
	return args, nil
}

// hk 5073: once the hub has stored the guarded action it tells the node, on
// close_status, that the intent_cancelled answer is durable. The first read
// that learns intent_cancelled does not ack: nothing is stored yet.
func TestPoolActionAcksTheStoredGuardOnCloseStatus_t5073(t *testing.T) {
	f, action := poolActionFixture(t)
	args := guardedPoolAckStep(t, f, action, map[string]any{"state": "intent_cancelled", "released": false, "settled": false, "attempt": action.Data.Args.Attempt})
	if _, acked := args["ack"]; acked {
		t.Fatalf("the read that stores the guard carried an ack: %v", args)
	}
	guard := readOnlyPoolAction(t, f.hub)
	if guard.Phase != "guarded" {
		t.Fatalf("phase=%q, want guarded", guard.Phase)
	}
	args = guardedPoolAckStep(t, f, guard, map[string]any{"state": "intent_cancelled", "released": false, "settled": false, "attempt": action.Data.Args.Attempt})
	if args["ack"] != true || args["request_id"] != action.ID || args["job"] != action.Job {
		t.Fatalf("guarded pass sent %v, want ack for %s", args, action.ID)
	}
}

// The pass that finishes a guarded action (its hk record is terminal) still
// delivers the ack: no later pass will.
func TestPoolActionAcksBeforeFinishingAGuard_t5073(t *testing.T) {
	f, action := poolActionFixture(t)
	action.Phase = "guarded"
	if err := f.hub.inbox.store.savePoolAction(t.Context(), action); err != nil {
		t.Fatal(err)
	}
	f.jobs.mu.Lock()
	f.jobs.records[f.job]["state"] = "completed"
	f.jobs.mu.Unlock()
	args := guardedPoolAckStep(t, f, action, map[string]any{"state": "intent_cancelled", "released": false, "settled": false, "attempt": action.Data.Args.Attempt})
	if args["ack"] != true {
		t.Fatalf("the finishing pass sent %v, want an ack", args)
	}
	if _, err := f.hub.reassignJob(action.Job, "machine-b"); err != nil {
		t.Fatalf("terminal hk record did not release the guard: %v", err)
	}
}

func intentCancelledReply(action poolAction) map[string]any {
	return map[string]any{"result": map[string]any{"state": "intent_cancelled", "released": false, "settled": false, "attempt": action.Data.Args.Attempt}}
}

func terminalGuardedAction(t *testing.T) (*quotaReplaceFixture, poolAction) {
	t.Helper()
	f, action := poolActionFixture(t)
	action.Phase = "guarded"
	if err := f.hub.inbox.store.savePoolAction(t.Context(), action); err != nil {
		t.Fatal(err)
	}
	f.jobs.mu.Lock()
	f.jobs.records[f.job]["state"] = "completed"
	f.jobs.mu.Unlock()
	return f, action
}

// A terminal hk record does not finish the guard while the node has not
// confirmed the ack: a node error (journal_failed) or silence leaves the action
// guarded, and the next pass delivers the ack and finishes it. Finishing at the
// first failure would drop the ack for good, and the node's drain would wait
// forever.
func TestPoolActionKeepsATerminalGuardUntilTheAckLands_t5073(t *testing.T) {
	f, action := terminalGuardedAction(t)
	if _, err := guardedPoolAckReply(t, f, action, map[string]any{"error": "journal_failed"}); err == nil {
		t.Fatal("a failed ack finished the guarded action")
	}
	if _, err := f.hub.reassignJob(action.Job, "machine-b"); err == nil {
		t.Fatal("a failed ack released the reassignment guard")
	}
	// A node that does not answer within the pass is retried too.
	silent := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		silent <- f.hub.reconcilePoolAction(ctx, action)
	}()
	nodeCommandRead(t, f.node)
	if err := <-silent; err == nil {
		t.Fatal("a silent node finished the guarded action")
	}
	if _, err := f.hub.reassignJob(action.Job, "machine-b"); err == nil {
		t.Fatal("a silent node released the reassignment guard")
	}
	if args, err := guardedPoolAckReply(t, f, action, intentCancelledReply(action)); err != nil || args["ack"] != true {
		t.Fatalf("retry sent %v err=%v, want the ack and a finished action", args, err)
	}
	if _, err := f.hub.reassignJob(action.Job, "machine-b"); err != nil {
		t.Fatalf("the confirmed ack did not release the guard: %v", err)
	}
}

// A node of an older build rejects the ack argument. It never owed one, so it
// must not hold the guard.
func TestPoolActionReleasesATerminalGuardFromANodeThatRejectsTheAck_t5073(t *testing.T) {
	f, action := terminalGuardedAction(t)
	if _, err := guardedPoolAckReply(t, f, action, map[string]any{"error": "invalid_args"}); err != nil {
		t.Fatalf("guarded pass: %v", err)
	}
	if _, err := f.hub.reassignJob(action.Job, "machine-b"); err != nil {
		t.Fatalf("an older node held the guard: %v", err)
	}
}

// Once the node confirmed the ack the hub stores it and asks no more: a later
// terminal record finishes the guard without the node, which a rolled-back
// binary could no longer answer.
func TestPoolActionStopsAskingOnceTheAckLanded_t5073(t *testing.T) {
	f, action := poolActionFixture(t)
	guardedPoolAckStep(t, f, action, intentCancelledReply(action)["result"].(map[string]any))
	guard := readOnlyPoolAction(t, f.hub)
	if args := guardedPoolAckStep(t, f, guard, intentCancelledReply(action)["result"].(map[string]any)); args["ack"] != true {
		t.Fatalf("first guarded pass sent %v, want the ack", args)
	}
	guard = readOnlyPoolAction(t, f.hub)
	if !guard.Data.Acked {
		t.Fatal("the confirmed ack was not stored")
	}
	f.jobs.mu.Lock()
	f.jobs.records[f.job]["state"] = "completed"
	f.jobs.mu.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	// No node answers: an acked guard finishes on the terminal record alone.
	if err := f.hub.reconcilePoolAction(ctx, guard); err != nil {
		t.Fatal(err)
	}
	if _, err := f.hub.reassignJob(action.Job, "machine-b"); err != nil {
		t.Fatalf("terminal record did not release the acked guard: %v", err)
	}
}

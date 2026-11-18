package panewire

import (
	"encoding/json"
	"testing"
	"time"
)

// guardedPoolAckStep runs one pass over a guarded action and answers the
// node's close_status, returning its arguments.
func guardedPoolAckStep(t *testing.T, f *quotaReplaceFixture, action poolAction, result map[string]any) map[string]any {
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
	nodeCommandReplyTo(t, f.node, command, map[string]any{"result": result})
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("guarded pass: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("guarded pass did not finish")
	}
	return args
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

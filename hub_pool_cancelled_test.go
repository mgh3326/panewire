package panewire

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestPoolActionIntentCancelledRetainsReassignmentFence(t *testing.T) {
	f, action := poolActionFixture(t)
	if _, err := f.hub.inbox.store.db.ExecContext(t.Context(), `UPDATE spawn_reservations SET task_role='legacy-reviewer' WHERE request_id=?`, f.holder.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	done := make(chan error, 1)
	go func() { done <- f.hub.reconcilePoolAction(ctx, action) }()
	command := nodeCommandRead(t, f.node)
	// Journal release is bookkeeping; it does not prove the old pane stopped.
	nodeCommandReplyTo(t, f.node, command, map[string]any{"result": map[string]any{"state": "intent_cancelled", "released": true, "attempt": action.Data.Args.Attempt}})
	err := <-done
	cancel()
	if err != nil {
		t.Fatalf("cancel proof: %v", err)
	}
	guard := readOnlyPoolAction(t, f.hub)
	if guard.Phase != "guarded" {
		t.Fatalf("action phase=%q, want final action with retained guard", guard.Phase)
	}
	if _, err := f.hub.reassignJob(action.Job, "machine-b"); err == nil || !strings.Contains(err.Error(), "terminal") || !strings.Contains(err.Error(), "release") {
		t.Fatalf("reassignment did not explain its retained guard: %v", err)
	}
	if events := spawnFailedInjections(f.destination); len(events) != 0 {
		t.Fatalf("cancelled intent sent a notice: %+v", events)
	}
	if _, _, _, err := f.hub.inbox.store.spawnRoleOwner(t.Context(), "legacy-reviewer"); err != nil {
		t.Fatalf("cancelled intent released the role: %v", err)
	}
	restartPoolActionHub(t, f)
	f.hub.mu.Lock()
	f.hub.jobs[action.Job] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: action.Job, Epoch: 1}, Node: action.Node, Orphaned: true}
	f.hub.mu.Unlock()
	guard = readOnlyPoolAction(t, f.hub)
	if _, err := f.hub.reassignJob(action.Job, "machine-b"); err == nil {
		t.Fatal("restart lost the reassignment guard")
	}
	ctx, cancel = context.WithTimeout(t.Context(), 500*time.Millisecond)
	err = f.hub.reconcilePoolAction(ctx, guard)
	cancel()
	if err != nil {
		t.Fatalf("final action retried while the job was live: %v", err)
	}
	f.jobs.mu.Lock()
	f.jobs.records[f.job]["state"] = "completed"
	f.jobs.mu.Unlock()
	if err := f.hub.reconcilePoolAction(t.Context(), guard); err != nil {
		t.Fatal(err)
	}
	if _, err := f.hub.reassignJob(action.Job, "machine-b"); err != nil {
		t.Fatalf("terminal hk record did not release the guard: %v", err)
	}
}

func TestPoolActionGuardStopsOtherUnknownRetries(t *testing.T) {
	f, guard := poolActionFixture(t)
	guard.Phase = "guarded"
	if err := f.hub.inbox.store.savePoolAction(t.Context(), guard); err != nil {
		t.Fatal(err)
	}
	other := guard
	other.ID, other.Phase = "other-op", poolActionPending
	args := *other.Data.Args
	args.RequestID = other.ID
	other.Data.Args = &args
	if err := f.hub.inbox.store.insertPoolAction(t.Context(), other); err != nil {
		t.Fatal(err)
	}
	runUnknownRetry(t, f, other, other.Data.Args.Attempt)
	if actions, err := f.hub.inbox.store.activePoolActions(t.Context()); err != nil || len(actions) != 2 {
		t.Fatalf("guarded job has %d active actions, want guard and original request: %v", len(actions), err)
	}
}

func TestPoolActionGuardReleasedByNodeSettlement(t *testing.T) {
	f, action := poolActionFixture(t)
	action.Phase = "guarded"
	if err := f.hub.inbox.store.savePoolAction(t.Context(), action); err != nil {
		t.Fatal(err)
	}
	f.jobs.mu.Lock()
	delete(f.jobs.records, f.job)
	f.jobs.mu.Unlock()
	if err := runPoolActionStep(t, f.hub, f.node, action, poolStatus("closed", false, true)); err != nil {
		t.Fatal(err)
	}
	if events := spawnFailedInjections(f.destination); len(events) != 0 {
		t.Fatalf("guarded settlement sent a notice: %+v", events)
	}
	if _, err := f.hub.reassignJob(action.Job, "machine-b"); err != nil {
		t.Fatalf("settled node did not release the guard: %v", err)
	}
}

func TestPoolActionFinalizationLogsNodeAndAction(t *testing.T) {
	for _, tc := range []struct {
		name, state, outcome string
		settled              bool
	}{
		{"refused", "preempted", "refused", false},
		{"settled", "closed", "settled", true},
		{"guarded", "intent_cancelled", "guarded", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, action := poolActionFixture(t)
			logs := captureHubLog(f.hub)
			response := poolActionReply{op: nodeCommandPaneCloseStatus, result: map[string]any{"state": tc.state, "settled": tc.settled, "attempt": action.Data.Args.Attempt}}
			if err := runPoolActionStep(t, f.hub, f.node, action, response); err != nil {
				t.Fatal(err)
			}
			text := logs.String()
			for _, part := range []string{"pool close action finalized", action.ID, action.Node, action.Job, tc.outcome} {
				if !strings.Contains(text, part) {
					t.Fatalf("finalization log lacks %q: %s", part, text)
				}
			}
		})
	}
}

func TestPoolActionFinalizationDoesNotLogFailedSave(t *testing.T) {
	f, action := poolActionFixture(t)
	logs := captureHubLog(f.hub)
	if err := f.hub.inbox.store.close(); err != nil {
		t.Fatal(err)
	}
	if err := f.hub.refusePoolAction(t.Context(), action); err == nil {
		t.Fatal("finalization succeeded after the store closed")
	}
	if strings.Contains(logs.String(), "pool close action finalized") {
		t.Fatalf("failed save logged a final action: %s", logs.String())
	}
}

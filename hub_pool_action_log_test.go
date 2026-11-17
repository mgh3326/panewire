package panewire

import (
	"io"
	"strings"
	"testing"
)

func TestPoolActionCreationLogsNodeAndAction(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "action-job")
	f.hub.mu.Lock()
	f.hub.nodes["machine-a"].capabilities[hubCapabilityCloseOps] = true
	f.hub.jobs[f.job] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: f.job, Epoch: 1}, Node: "machine-a"}
	f.hub.mu.Unlock()
	logs := captureHubLog(f.hub)
	app, err := f.hub.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.hub.createPoolAction(t.Context(), poolExhaustion{Pool: "claude", Cause: poolCauseRetired}, f.holder, app); err != nil {
		t.Fatal(err)
	}
	action := readOnlyPoolAction(t, f.hub)
	text := logs.String()
	for _, part := range []string{"pool close action created", action.ID, action.Node, action.Job} {
		if !strings.Contains(text, part) {
			t.Fatalf("creation log lacks %q: %s", part, text)
		}
	}
}

func TestPoolActionCreationDoesNotLogStoreFailure(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "action-job")
	f.hub.mu.Lock()
	f.hub.nodes["machine-a"].capabilities[hubCapabilityCloseOps] = true
	f.hub.jobs[f.job] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: f.job, Epoch: 1}, Node: "machine-a"}
	f.hub.mu.Unlock()
	logs := captureHubLog(f.hub)
	app, err := f.hub.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.hub.inbox.store.close(); err != nil {
		t.Fatal(err)
	}
	if err := f.hub.createPoolAction(t.Context(), poolExhaustion{Pool: "claude", Cause: poolCauseRetired}, f.holder, app); err == nil {
		t.Fatal("creation succeeded after the store closed")
	}
	if strings.Contains(logs.String(), "pool close action created") {
		t.Fatalf("failed creation was logged: %s", logs.String())
	}
}

func TestPoolActionRetryLogsNodeAndBothIDs(t *testing.T) {
	f, original := poolActionFixture(t)
	logs := captureHubLog(f.hub)
	runUnknownRetry(t, f, original, original.Data.Args.Attempt)
	actions, err := f.hub.inbox.store.activePoolActions(t.Context())
	if err != nil || len(actions) != 2 {
		t.Fatalf("retry actions=%+v err=%v", actions, err)
	}
	var retry poolAction
	for _, action := range actions {
		if action.ID != original.ID {
			retry = action
		}
	}
	text := logs.String()
	for _, part := range []string{"pool close action retry", original.ID, retry.ID, original.Node, original.Job} {
		if !strings.Contains(text, part) {
			t.Fatalf("retry log lacks %q: %s", part, text)
		}
	}
}

func TestPoolActionPendingObservedOnceAfterHubRestart(t *testing.T) {
	f, action := poolActionFixture(t)
	restartPoolActionHub(t, f)
	logs := captureHubLog(f.hub)
	for range 2 {
		done := make(chan struct{})
		go func() { defer close(done); f.hub.reconcilePoolActions(t.Context()) }()
		command := nodeCommandRead(t, f.node)
		nodeCommandReplyTo(t, f.node, command, map[string]any{"result": poolStatus("closed", false, false).result})
		<-done
	}
	text := logs.String()
	if n := strings.Count(text, "pool close action pending"); n != 1 {
		t.Fatalf("got %d pending inventory lines, want one: %s", n, text)
	}
	for _, part := range []string{action.ID, action.Node, action.Job} {
		if !strings.Contains(text, part) {
			t.Fatalf("pending inventory lacks %q: %s", part, text)
		}
	}
}

func TestPoolActionGuardedFinalizationObservedAfterHubRestart(t *testing.T) {
	f, action := poolActionFixture(t)
	action.Phase = poolActionGuarded
	if err := f.hub.inbox.store.savePoolAction(t.Context(), action); err != nil {
		t.Fatal(err)
	}
	restartPoolActionHub(t, f)
	logs := captureHubLog(f.hub)
	for range 2 {
		done := make(chan struct{})
		go func() { defer close(done); f.hub.reconcilePoolActions(t.Context()) }()
		command := nodeCommandRead(t, f.node)
		nodeCommandReplyTo(t, f.node, command, map[string]any{"result": map[string]any{"state": "intent_cancelled", "attempt": action.Data.Args.Attempt}})
		<-done
	}
	text := logs.String()
	if n := strings.Count(text, "pool close action finalized"); n != 1 {
		t.Fatalf("got %d recovered finalization lines, want one: %s", n, text)
	}
	for _, part := range []string{action.ID, action.Node, action.Job, "guarded"} {
		if !strings.Contains(text, part) {
			t.Fatalf("recovered finalization lacks %q: %s", part, text)
		}
	}
}

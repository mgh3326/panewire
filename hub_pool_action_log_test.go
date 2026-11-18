package panewire

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"
)

func TestPoolActionInventoryCompletesBeforeSlowNode(t *testing.T) {
	f, first := poolActionFixture(t)
	second := first
	second.ID, second.Job, second.Node = "action-2", "later-job", "machine-b"
	if err := f.hub.inbox.store.insertPoolAction(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	started := time.Date(2026, time.November, 18, 1, 2, 3, 0, time.UTC)
	f.hub.now = func() time.Time { return started }
	logs := captureHubLog(f.hub)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); f.hub.reconcilePoolActions(ctx) }()
	defer func() { cancel(); <-done }()

	// Hold the first node command open while checking the inventory of both rows.
	_ = nodeCommandRead(t, f.node)
	var inventory string
	entries := make(map[string]map[string]any)
	for _, raw := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var line map[string]any
		if err := json.Unmarshal([]byte(raw), &line); err != nil {
			t.Fatal(err)
		}
		switch line["msg"] {
		case "pool close action inventory entry":
			entries[line["action"].(string)] = line
		case "pool close action inventory complete":
			inventory, _ = line["inventory"].(string)
			if line["count"] != float64(2) {
				t.Fatalf("inventory count = %v, want 2", line["count"])
			}
			when, err := time.Parse(time.RFC3339Nano, line["started_at"].(string))
			if err != nil || !when.Equal(started) {
				t.Fatalf("inventory start = %v, error = %v, want %v", when, err, started)
			}
		}
	}
	for _, action := range []poolAction{first, second} {
		entry := entries[action.ID]
		if inventory == "" || entry["inventory"] != inventory || entry["node"] != action.Node || entry["job"] != action.Job || entry["phase"] != action.Phase {
			t.Fatalf("incomplete inventory before first node responds: %s", logs.String())
		}
	}
}

func TestPoolActionInventoryDoesNotCompleteWhenStoreUnavailable(t *testing.T) {
	f, _ := poolActionFixture(t)
	logs := captureHubLog(f.hub)
	if err := f.hub.inbox.store.close(); err != nil {
		t.Fatal(err)
	}
	f.hub.reconcilePoolActions(t.Context())
	if strings.Contains(logs.String(), "pool close action inventory complete") {
		t.Fatalf("failed scan was reported complete: %s", logs.String())
	}
}

func TestPoolActionInventoryReportsEmptySnapshot(t *testing.T) {
	f, action := poolActionFixture(t)
	action.Phase = poolActionRefused
	if err := f.hub.inbox.store.savePoolAction(t.Context(), action); err != nil {
		t.Fatal(err)
	}
	logs := captureHubLog(f.hub)
	f.hub.reconcilePoolActions(t.Context())
	var line map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(logs.String())), &line); err != nil {
		t.Fatal(err)
	}
	if line["msg"] != "pool close action inventory complete" || line["count"] != float64(0) || line["inventory"] == "" {
		t.Fatalf("empty inventory was not reported: %s", logs.String())
	}
}

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
	if err := f.hub.createPoolAction(t.Context(), poolExhaustion{Pool: "claude", Cause: poolCauseRetired}, f.holder, app, nil); err != nil {
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
	if err := f.hub.createPoolAction(t.Context(), poolExhaustion{Pool: "claude", Cause: poolCauseRetired}, f.holder, app, nil); err == nil {
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

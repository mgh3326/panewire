package panewire

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// timeoutRefreshHub opens (or reopens) the hub on path for the hk 4600
// timeout tests: one node, machine-a, whose only claude reading is stale.
func timeoutRefreshHub(t *testing.T, path string, logs *controlPlaneLogCapture) *HubServer {
	t.Helper()
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv(crashReportDSNEnv, "")
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"}, InboxStorePath: path, Logger: slog.New(logs)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.Quota = staleRefreshQuota{}
		app.Source = staleRefreshSource{now: app.Now(), hosts: []string{"machine-a"}}
		return app, nil
	}
	return h
}

// hk 4600: a request that times out while parked on a stale-quota refresh
// names the machine and pool it waited on and the last refresh failure
// recorded there, instead of the queue-starved cause (PR #536 review
// 5895888980). The link and the failure are durable, so the hub restart
// between the pending pass and the deadline keeps both.
func TestHubSpawnTimeoutNamesQuotaRefreshFailure_t4600(t *testing.T) {
	retryHK(t)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	logs := &controlPlaneLogCapture{}
	hub := timeoutRefreshHub(t, path, logs)
	server := httptest.NewServer(hub.Handler())
	t.Cleanup(server.Close)
	node := t14NamedNode(t, hub, server, "machine-a", true, true)
	request := durableSpawnRequest()
	task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); hub.executeSpawnTask(t.Context(), task) }()
	command := nodeCommandRead(t, node)
	if string(command["op"]) != `"quota.refresh"` {
		t.Fatalf("unexpected node command: %v", command)
	}
	nodeCommandReplyTo(t, node, command, map[string]any{"error": "refresh_failed"})
	<-done
	// The deadline below is evaluated against the restarted hub, so let the
	// scheduler finish its attempt rather than assuming its order.
	if !waitUntil(2*time.Second, func() bool {
		_, found := logs.find("hub quota refresh failed")
		return found
	}) {
		t.Fatal("the failed quota refresh was not reported")
	}
	parked, err := hub.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || parked.Status != "accepted" || parked.Node != nil {
		t.Fatalf("the pending pass must leave the request accepted: status=%s node=%v err=%v", parked.Status, parked.Node, err)
	}
	hub.Close()

	restarted := timeoutRefreshHub(t, path, logs)
	expired, err := restarted.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	expired.Deadline = time.Now().Add(-time.Second)
	restarted.executeSpawnTask(t.Context(), expired)
	final, err := restarted.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Outcome.Reason != "timeout" || final.Outcome.RC != 124 {
		t.Fatalf("expired request settled with %+v, want a timeout", final.Outcome)
	}
	for _, want := range []string{"machine-a", "claude", "refresh_failed"} {
		if !strings.Contains(final.Outcome.Fix, want) {
			t.Fatalf("timeout fix %q does not name %q", final.Outcome.Fix, want)
		}
	}
	if strings.Contains(final.Outcome.Fix, "hub spawn worker occupancy") {
		t.Fatalf("timeout fix %q keeps the queue-starved cause", final.Outcome.Fix)
	}
	raw, err := json.Marshal(final.Outcome)
	if err != nil {
		t.Fatal(err)
	}
	var outcome map[string]any
	if err := json.Unmarshal(raw, &outcome); err != nil {
		t.Fatal(err)
	}
	refresh, _ := outcome["quota_refresh"].(map[string]any)
	if refresh["machine"] != "machine-a" || refresh["pool"] != "claude" || refresh["reason"] != "refresh_failed" {
		t.Fatalf("outcome %s does not name the failed refresh", raw)
	}
}

// A refresh that has not finished yet has no failure to name: the timeout
// still reports what it waited on instead of the spawn queue.
func TestHubSpawnTimeoutNamesUnfinishedQuotaRefresh_t4600(t *testing.T) {
	retryHK(t)
	logs := &controlPlaneLogCapture{}
	hub := timeoutRefreshHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), logs)
	server := httptest.NewServer(hub.Handler())
	t.Cleanup(server.Close)
	t14NamedNode(t, hub, server, "machine-a", true, true)
	request := durableSpawnRequest()
	task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	hub.executeSpawnTask(t.Context(), task)
	parked, err := hub.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || parked.Status != "accepted" || parked.Node != nil {
		t.Fatalf("the pending pass must leave the request accepted: status=%s node=%v err=%v", parked.Status, parked.Node, err)
	}
	parked.Deadline = time.Now().Add(-time.Second)
	hub.executeSpawnTask(t.Context(), parked)
	final, err := hub.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"machine-a", "claude", "had not finished a refresh attempt"} {
		if !strings.Contains(final.Outcome.Fix, want) {
			t.Fatalf("timeout fix %q does not say %q", final.Outcome.Fix, want)
		}
	}
	if strings.Contains(final.Outcome.Fix, "hub spawn worker occupancy") {
		t.Fatalf("timeout fix %q keeps the queue-starved cause", final.Outcome.Fix)
	}
}

// A finished measurement must not be reported as a failure: the timeout names
// that attempt as finished, not a failure the hub never saw.
func TestHubSpawnTimeoutNamesFinishedQuotaRefresh_t4600(t *testing.T) {
	retryHK(t)
	logs := &controlPlaneLogCapture{}
	hub := timeoutRefreshHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), logs)
	server := httptest.NewServer(hub.Handler())
	t.Cleanup(server.Close)
	t14NamedNode(t, hub, server, "machine-a", true, true)
	request := durableSpawnRequest()
	task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	hub.executeSpawnTask(t.Context(), task)
	parked, err := hub.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || parked.Status != "accepted" || parked.Node != nil {
		t.Fatalf("the pending pass must leave the request accepted: status=%s node=%v err=%v", parked.Status, parked.Node, err)
	}
	if err := hub.inbox.store.recordQuotaRefreshAttempt(t.Context(), "machine-a", "claude", "", time.Now()); err != nil {
		t.Fatal(err)
	}
	parked.Deadline = time.Now().Add(-time.Second)
	hub.executeSpawnTask(t.Context(), parked)
	final, err := hub.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"machine-a", "claude", "last attempt finished"} {
		if !strings.Contains(final.Outcome.Fix, want) {
			t.Fatalf("timeout fix %q does not say %q", final.Outcome.Fix, want)
		}
	}
	if strings.Contains(final.Outcome.Fix, "failed") || strings.Contains(final.Outcome.Fix, "hub spawn worker occupancy") {
		t.Fatalf("timeout fix %q blames a failure or the queue", final.Outcome.Fix)
	}
	raw, err := json.Marshal(final.Outcome)
	if err != nil {
		t.Fatal(err)
	}
	var outcome map[string]any
	if err := json.Unmarshal(raw, &outcome); err != nil {
		t.Fatal(err)
	}
	refresh, _ := outcome["quota_refresh"].(map[string]any)
	if refresh["machine"] != "machine-a" || refresh["pool"] != "claude" || refresh["reason"] != nil {
		t.Fatalf("outcome %s does not name the finished attempt", raw)
	}
}

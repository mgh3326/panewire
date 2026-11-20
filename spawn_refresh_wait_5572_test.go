package panewire

import (
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// hk 5572: StartedAt keeps sub-millisecond precision while the stored outcome
// reads back in whole milliseconds. A refresh that finished in the millisecond
// the wait began is the request's own attempt, so the timeout names it instead
// of saying no attempt finished.
func TestSpawnRefreshWaitNamesAnOutcomeInTheStartMillisecond_5572(t *testing.T) {
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
	// The wait began 400µs into a millisecond; the refresh finished 500µs later,
	// still in that same millisecond. The store keeps only whole milliseconds.
	millisecond := time.Now().Truncate(time.Millisecond).Add(-time.Minute)
	started := millisecond.Add(400 * time.Microsecond)
	if err := hub.inbox.store.noteSpawnRefreshWaits(t.Context(), request.RequestID, task.Attempt, []hubSpawnRefreshWait{{Machine: "machine-a", Pool: "claude", StartedAt: started}}); err != nil {
		t.Fatal(err)
	}
	if err := hub.inbox.store.recordQuotaRefreshAttempt(t.Context(), "machine-a", "claude", "node_lost", millisecond.Add(900*time.Microsecond)); err != nil {
		t.Fatal(err)
	}
	waiting, err := hub.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || waiting.Status != "accepted" || waiting.Node != nil || len(waiting.RefreshWaits) != 1 {
		t.Fatalf("the link must be stored on the accepted request: status=%s node=%v waits=%+v err=%v", waiting.Status, waiting.Node, waiting.RefreshWaits, err)
	}
	waiting.Deadline = time.Now().Add(-time.Second)
	hub.executeSpawnTask(t.Context(), waiting)
	final, err := hub.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Outcome.QuotaRefresh == nil || final.Outcome.QuotaRefresh.Reason != "node_lost" {
		t.Fatalf("outcome %+v does not name the finished refresh that failed with node_lost", final.Outcome.QuotaRefresh)
	}
	if strings.Contains(final.Outcome.Fix, "had not finished a refresh attempt") || !strings.Contains(final.Outcome.Fix, "node_lost") {
		t.Fatalf("timeout fix %q says the refresh never finished", final.Outcome.Fix)
	}
	if !spawnTriedHas(final.Outcome.Tried, "machine-a", "refresh failed: node_lost") {
		t.Fatalf("tried %+v does not record the finished refresh", final.Outcome.Tried)
	}
}

// hk 5572: a request queued a refresh, the node failed it, and the next pass was
// refused inside the failure cooldown. That held pass keeps the wait link, so
// the timeout still names the refresh that failed and how it failed. Before this
// the held pass cleared the link and the timeout named nothing.
func TestSpawnRefreshWaitHeldAfterFailureNamesTheFailedRefresh_5572(t *testing.T) {
	retryHK(t)
	logs := &controlPlaneLogCapture{}
	hub := timeoutRefreshHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), logs)
	server := httptest.NewServer(hub.Handler())
	t.Cleanup(server.Close)
	node := t14NamedNode(t, hub, server, "machine-a", true, true)
	request := durableSpawnRequest()
	task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	// Pass 1 queues the refresh at T0 and the node fails it at T1.
	done := make(chan struct{})
	go func() { defer close(done); hub.executeSpawnTask(t.Context(), task) }()
	command := nodeCommandRead(t, node)
	if string(command["op"]) != `"quota.refresh"` {
		t.Fatalf("unexpected node command: %v", command)
	}
	nodeCommandReplyTo(t, node, command, map[string]any{"error": "refresh_failed"})
	<-done
	if !waitUntil(2*time.Second, func() bool {
		_, found := logs.find("hub quota refresh failed")
		return found
	}) {
		t.Fatal("the failed quota refresh was not reported")
	}
	queued, err := hub.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || queued.Status != "accepted" || queued.Node != nil || len(queued.RefreshWaits) != 1 {
		t.Fatalf("pass 1 must leave the request accepted on the refresh: status=%s node=%v waits=%+v err=%v", queued.Status, queued.Node, queued.RefreshWaits, err)
	}
	// Pass 2 at T2 is refused inside the cooldown after the failure and holds.
	hub.executeSpawnTask(t.Context(), queued)
	held, err := hub.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || held.Status != "accepted" || held.Node != nil {
		t.Fatalf("the held pass must leave the request accepted: status=%s node=%v err=%v", held.Status, held.Node, err)
	}
	if len(held.RefreshWaits) != 1 || held.RefreshWaits[0].Machine != "machine-a" || held.RefreshWaits[0].Pool != "claude" {
		t.Fatalf("the held pass dropped the wait link: %+v", held.RefreshWaits)
	}
	if held.RefreshWaits[0].StartedAt.IsZero() || !held.RefreshWaits[0].StartedAt.Equal(queued.RefreshWaits[0].StartedAt) {
		t.Fatalf("the held pass restarted the wait: held %v, queued %v", held.RefreshWaits[0].StartedAt, queued.RefreshWaits[0].StartedAt)
	}
	// The timeout at T3 names the refresh that failed.
	held.Deadline = time.Now().Add(-time.Second)
	hub.executeSpawnTask(t.Context(), held)
	final, err := hub.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"machine-a", "claude", "refresh_failed"} {
		if !strings.Contains(final.Outcome.Fix, want) {
			t.Fatalf("timeout fix %q does not name %q", final.Outcome.Fix, want)
		}
	}
	if strings.Contains(final.Outcome.Fix, "had not finished a refresh attempt") {
		t.Fatalf("timeout fix %q says the failed refresh never finished", final.Outcome.Fix)
	}
	if final.Outcome.QuotaRefresh == nil || final.Outcome.QuotaRefresh.Reason != "refresh_failed" {
		t.Fatalf("outcome %+v does not name the failed refresh", final.Outcome.QuotaRefresh)
	}
	if !spawnTriedHas(final.Outcome.Tried, "machine-a", "refresh failed: refresh_failed") {
		t.Fatalf("tried %+v does not record the failed refresh", final.Outcome.Tried)
	}
}

// spawnTriedHas reports whether tried holds an entry for host with reason.
func spawnTriedHas(tried []hubSpawnTried, host, reason string) bool {
	for _, entry := range tried {
		if entry.Host == host && entry.Reason == reason {
			return true
		}
	}
	return false
}

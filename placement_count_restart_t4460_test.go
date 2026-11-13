package panewire

import (
	"path/filepath"
	"testing"
	"time"
)

// hk 4460: a hub restart restores the placement counts of spawns already bound
// to a host and not yet settled, so the next decision still sees a job the
// previous hub decided onto that host before its claim shows (PR #484 hub.go:609).

// restartHub opens a hub on the inbox at path. Opening it again on the same path
// is a restart: the placement counts live in memory and only the store survives.
func restartHub(t *testing.T, path string) *HubServer {
	t.Helper()
	t.Setenv(crashReportDSNEnv, "")
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"}, InboxStorePath: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

// boundSpawn claims a request, decides it onto machine-a and binds it to that
// node, the state a spawn is in while it is dispatched and its claim is not yet
// seen. It returns the task as the store holds it after the bind.
func boundSpawn(t *testing.T, hub *HubServer, requestSeq int) hubSpawnTask {
	t.Helper()
	request := durableSpawnRequest()
	task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, "restart-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	dispatch := hubSpawnRequest{RequestID: t14RequestID(requestSeq), Machine: "machine-a", CWDKey: "project", Brief: hubSpawnBrief{Inline: "task"}, Args: []string{"--job", task.Job}, WaitSeconds: 600, DeadlineMS: task.Deadline.UnixMilli()}
	task, err = hub.inbox.store.bindSpawn(t.Context(), task.Request.RequestID, task.Attempt, dispatch)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

// A bound, unclaimed spawn survives the restart as a count on its host; the
// first heartbeat that shows its job turns the count into the claim.
func TestRestartRestoresCountOfBoundUnclaimedSpawn_t4460(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	before := restartHub(t, path)
	task := boundSpawn(t, before, 460)
	before.Close()

	after := restartHub(t, path)
	after.connect("machine-a", "test", "fixture", &hubAgent{}, true)
	if got := countedBridged(after); got != 1 {
		t.Fatalf("after restart the bound, unclaimed job counts %d on machine-a, want 1", got)
	}
	now := time.Now().UTC()
	after.observeActiveJobs("machine-a", []HubActiveJob{{JobID: task.Job, AgentLabel: "agent", Epoch: 1, StartedAt: now.Format(time.RFC3339)}}, now)
	if got := countedBridged(after); got != 1 {
		t.Fatalf("claimed job counts %d after restart, want 1 (counted twice)", got)
	}
	after.mu.Lock()
	left := len(after.placementCounts)
	after.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d restored counts outlive their claim", left)
	}
}

// A spawn that settled before the restart is not counted again: a refused
// request gave its count back, and a restored one must not take it back.
func TestRestartDoesNotRestoreSettledSpawn_t4460(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	before := restartHub(t, path)
	task := boundSpawn(t, before, 461)
	if changed, err := before.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 85, Stage: "launch", Reason: "spawn_failed:checkout", Host: "machine-a", NodeResult: true}); err != nil || !changed {
		t.Fatalf("completeSpawn = %v, %v", changed, err)
	}
	before.Close()

	after := restartHub(t, path)
	after.connect("machine-a", "test", "fixture", &hubAgent{}, true)
	if got := countedBridged(after); got != 0 {
		t.Fatalf("refused spawn counts %d after restart, want 0", got)
	}
}

// A count older than the bridge window is not restored: it would have ended
// without a claim had the hub kept running.
func TestRestartDropsCountsPastTheBridgeWindow_t4460(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	before := restartHub(t, path)
	task := boundSpawn(t, before, 462)
	old := time.Now().Add(-placementDefaultSpawnBridge - time.Minute).UnixMilli()
	if _, err := before.inbox.store.db.ExecContext(t.Context(), before.inbox.store.q(`UPDATE spawn_operations SET created_ms=? WHERE request_id=?`), old, task.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	before.Close()

	after := restartHub(t, path)
	after.connect("machine-a", "test", "fixture", &hubAgent{}, true)
	if got := countedBridged(after); got != 0 {
		t.Fatalf("count past the bridge window restored as %d, want 0", got)
	}
}

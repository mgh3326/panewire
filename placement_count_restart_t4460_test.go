package panewire

import (
	"path/filepath"
	"testing"
	"time"
)

// hk 4460: a hub restart restores the placement counts of spawns already bound
// to a host and not yet claimed, so the next decision still sees a job the
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

// boundSpawn claims a request, binds it to machine-a and stamps its placement,
// the state a spawn is in while it is dispatched and its claim is not yet seen.
// It mirrors the transport: the bind, then the placement stamp.
func boundSpawn(t *testing.T, hub *HubServer, requestSeq int) hubSpawnTask {
	t.Helper()
	request := durableSpawnRequest()
	task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, "restart-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	dispatch := hubSpawnRequest{RequestID: t14RequestID(requestSeq), Machine: "machine-a", CWDKey: "project", Brief: hubSpawnBrief{Inline: "task"}, Args: []string{"--job", task.Job}, WaitSeconds: 600, DeadlineMS: task.Deadline.UnixMilli()}
	task, err = hub.inbox.store.bindSpawn(t.Context(), task.Request.RequestID, task.Attempt, dispatch, time.Now().UTC())
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

// A count is dated from the placement, not the request: a request that waited
// in the coordinator queue longer than the bridge window and was placed just
// before the restart still counts, as its live count would.
func TestRestartCountsPlacementNotRequestAge_t4460(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	before := restartHub(t, path)
	task := boundSpawn(t, before, 463)
	old := time.Now().Add(-placementDefaultSpawnBridge - time.Minute).UnixMilli()
	if _, err := before.inbox.store.db.ExecContext(t.Context(), before.inbox.store.q(`UPDATE spawn_operations SET created_ms=? WHERE request_id=?`), old, task.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	before.Close()

	after := restartHub(t, path)
	after.connect("machine-a", "test", "fixture", &hubAgent{}, true)
	if got := countedBridged(after); got != 1 {
		t.Fatalf("placed just now but requested long ago: counts %d after restart, want 1", got)
	}
}

// A count placed before the bridge window is not restored: it would have ended
// without a claim had the hub kept running.
func TestRestartDropsCountsPastTheBridgeWindow_t4460(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	before := restartHub(t, path)
	task := boundSpawn(t, before, 462)
	old := time.Now().Add(-placementDefaultSpawnBridge - time.Minute).UnixMilli()
	if _, err := before.inbox.store.db.ExecContext(t.Context(), before.inbox.store.q(`UPDATE spawn_operations SET placed_ms=? WHERE request_id=?`), old, task.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	before.Close()

	after := restartHub(t, path)
	after.connect("machine-a", "test", "fixture", &hubAgent{}, true)
	if got := countedBridged(after); got != 0 {
		t.Fatalf("count past the bridge window restored as %d, want 0", got)
	}
}

// A launch whose claim was seen before the restart is not counted again, even
// when its job finished before the restart and no heartbeat shows it after: the
// claim is stored, not only held in memory.
func TestRestartDoesNotRestoreClaimedLaunch_t4460(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	before := restartHub(t, path)
	task := boundSpawn(t, before, 464)
	before.countPlacement(task.Job, "machine-a")
	if changed, err := before.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", NodeResult: true}); err != nil || !changed {
		t.Fatalf("completeSpawn = %v, %v", changed, err)
	}
	before.connect("machine-a", "test", "fixture", &hubAgent{}, true)
	now := time.Now().UTC()
	before.observeActiveJobs("machine-a", []HubActiveJob{{JobID: task.Job, AgentLabel: "agent", Epoch: 1, StartedAt: now.Format(time.RFC3339)}}, now)
	// The job finishes; the next heartbeat no longer lists it.
	before.observeActiveJobs("machine-a", nil, now.Add(time.Second))
	before.Close()

	after := restartHub(t, path)
	after.connect("machine-a", "test", "fixture", &hubAgent{}, true)
	if got := countedBridged(after); got != 0 {
		t.Fatalf("a finished job whose claim was seen counts %d after restart, want 0", got)
	}
}

// A restore that cannot read the store reports it, so the hub does not start
// placing blind.
func TestRestorePlacementCountsReportsStoreFailure_t4460(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	hub := restartHub(t, path)
	if err := hub.inbox.store.close(); err != nil {
		t.Fatal(err)
	}
	if err := hub.restorePlacementCounts(); err == nil {
		t.Fatal("restore on a closed store reported no error")
	}
}

// A claim whose persist fails keeps its count, so the next heartbeat that still
// lists the job tries the write again. Closing the store makes every write fail.
func TestFailedClaimPersistKeepsCountForRetry_t4460(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	hub := restartHub(t, path)
	task := boundSpawn(t, hub, 465)
	hub.countPlacement(task.Job, "machine-a")
	hub.connect("machine-a", "test", "fixture", &hubAgent{}, true)
	if err := hub.inbox.store.close(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	hub.observeActiveJobs("machine-a", []HubActiveJob{{JobID: task.Job, AgentLabel: "agent", Epoch: 1, StartedAt: now.Format(time.RFC3339)}}, now)
	hub.mu.Lock()
	_, kept := hub.placementCounts[task.Job]
	hub.mu.Unlock()
	if !kept {
		t.Fatal("a claim that failed to persist dropped its count; the next restart would forget it")
	}
}

// A retry is a new placement. A claim the earlier attempt left on the row must
// not keep a restart from counting the retry's bound launch.
func TestRestartCountsRetriedPlacementAfterEarlierClaim_t4460(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	before := restartHub(t, path)
	before.connect("machine-a", "test", "fixture", &hubAgent{}, true)
	task := boundSpawn(t, before, 466)
	before.countPlacement(task.Job, "machine-a")
	now := time.Now().UTC()
	before.observeActiveJobs("machine-a", []HubActiveJob{{JobID: task.Job, AgentLabel: "agent", Epoch: 1, StartedAt: now.Format(time.RFC3339)}}, now)
	// The node never started the first attempt: a retryable, cleanly refused end.
	refused := hubSpawnOutcome{RC: 124, Stage: "claim", Reason: "deadline_not_started", Host: "machine-a", Retryable: true, NodeResult: true}
	if changed, err := before.completeSpawn(t.Context(), task, refused); err != nil || !changed {
		t.Fatalf("completeSpawn = %v, %v", changed, err)
	}
	saved, err := before.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	retried, err := before.inbox.store.resumeCleanedSpawn(t.Context(), saved, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	dispatch := hubSpawnRequest{RequestID: t14RequestID(467), Machine: "machine-a", CWDKey: "project", Brief: hubSpawnBrief{Inline: "task"}, Args: []string{"--job", task.Job}, WaitSeconds: 600, DeadlineMS: retried.Deadline.UnixMilli()}
	if _, err := before.inbox.store.bindSpawn(t.Context(), retried.Request.RequestID, retried.Attempt, dispatch, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	before.Close()

	after := restartHub(t, path)
	after.connect("machine-a", "test", "fixture", &hubAgent{}, true)
	if got := countedBridged(after); got != 1 {
		t.Fatalf("retried, bound launch counts %d after restart, want 1 (the earlier attempt's claim hid it)", got)
	}
}

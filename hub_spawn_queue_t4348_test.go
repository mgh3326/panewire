package panewire

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// A spawn worker that wakes a host waits at most the caller's wake_wait for
// that host's heartbeat. Waiting for the whole spawn deadline instead holds one
// of the four coordinator workers for minutes, so requests behind it expire in
// the queue and report a landing stage that never ran (hk 4348).
func TestSpawnWakeWaitsAtMostWakeWait(t *testing.T) {
	hub, server := t14Hub(t)
	t14NamedNode(t, hub, server, "machine-a", true, true)
	hub.mu.Lock()
	hub.wakeTargets = map[string]HubWakeTarget{"asleep": {Machine: "asleep", Via: "machine-a"}}
	hub.mu.Unlock()

	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
	defer cancel()
	started := time.Now()
	err := directSpawnWake{h: hub}.Wake(ctx, "asleep", fleet.WakeRequest{WaitSeconds: 1, Reason: "test"})
	if err == nil {
		t.Fatal("a wake that never confirms must fail the placement")
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("wake held its caller for %v; wake_wait is 1s", elapsed)
	}
}

// An operation the coordinator finds past its deadline was never started, so
// its fix text names the queue wait rather than the landing stage the closed
// stage set reports by default.
func TestExpiredSpawnFixNamesTheQueueWait(t *testing.T) {
	hub := asyncSpawnHub(t)
	request := durableSpawnRequest()
	task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = hub.inbox.store.db.Exec(`UPDATE spawn_operations SET deadline_ms=? WHERE request_id=?`, time.Now().Add(-90*time.Second).UnixMilli(), request.RequestID); err != nil {
		t.Fatal(err)
	}
	expired, err := hub.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	hub.executeSpawnTask(t.Context(), expired)
	saved, err := hub.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if saved.Outcome.RC != 124 || saved.Outcome.Reason != "timeout" {
		t.Fatalf("expired task outcome = %+v", saved.Outcome)
	}
	for _, want := range []string{"spawn queue", "no node ran its landing"} {
		if !strings.Contains(saved.Outcome.Fix, want) {
			t.Fatalf("fix %q does not name %q", saved.Outcome.Fix, want)
		}
	}
	if task.Status != "accepted" {
		t.Fatalf("claimed status = %s", task.Status)
	}
}

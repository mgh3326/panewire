package panewire

import (
	"strings"
	"testing"
	"time"
)

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

package panewire

import (
	"context"
	"encoding/json"
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
	hub := wakingHub(t)
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

// A host whose wake_wait is absent or nonpositive takes the same default the
// CLI waker applies, so an unvalidated setting cannot leave the wait unbounded.
func TestSpawnWakeDefaultsNonpositiveWakeWait(t *testing.T) {
	hub := wakingHub(t)
	previous := defaultWakeWaitSeconds
	defaultWakeWaitSeconds = 1
	t.Cleanup(func() { defaultWakeWaitSeconds = previous })
	for _, wait := range []int{0, -5} {
		ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
		started := time.Now()
		err := directSpawnWake{h: hub}.Wake(ctx, "asleep", fleet.WakeRequest{WaitSeconds: wait, Reason: "test"})
		elapsed := time.Since(started)
		cancel()
		if err == nil {
			t.Fatalf("wake_wait=%d: a wake that never confirms must fail the placement", wait)
		}
		if elapsed > 3*time.Second {
			t.Fatalf("wake_wait=%d held its caller for %v; the default was not applied", wait, elapsed)
		}
	}
}

// wakingHub is a hub with one connected waker and one target that never comes
// up, so every wake it serves waits out its bound.
func wakingHub(t *testing.T) *HubServer {
	t.Helper()
	hub, server := t14Hub(t)
	t14NamedNode(t, hub, server, "machine-a", true, true)
	hub.mu.Lock()
	hub.wakeTargets = map[string]HubWakeTarget{"asleep": {Machine: "asleep", Via: "machine-a"}}
	hub.mu.Unlock()
	return hub
}

// An operation the coordinator finds past its deadline was never started, so
// its fix text names the queue wait rather than the landing stage the closed
// stage set reports by default. A bound operation had an earlier attempt whose
// ownership is uncertain, so it is told to inspect that node, not to resubmit.
func TestExpiredSpawnFixNamesTheQueueWait(t *testing.T) {
	for _, bound := range []bool{false, true} {
		name := "unbound"
		if bound {
			name = "bound"
		}
		t.Run(name, func(t *testing.T) {
			hub := asyncSpawnHub(t)
			request := durableSpawnRequest()
			task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if task.Status != "accepted" {
				t.Fatalf("claimed status = %s", task.Status)
			}
			if _, err = hub.inbox.store.db.Exec(`UPDATE spawn_operations SET deadline_ms=? WHERE request_id=?`, time.Now().Add(-90*time.Second).UnixMilli(), request.RequestID); err != nil {
				t.Fatal(err)
			}
			if bound {
				payload, err := json.Marshal(hubSpawnRequest{RequestID: "node-request-1", Machine: "machine-a"})
				if err != nil {
					t.Fatal(err)
				}
				if _, err = hub.inbox.store.db.Exec(`UPDATE spawn_operations SET node_id=?,node_payload=? WHERE request_id=?`, "node-request-1", string(payload), request.RequestID); err != nil {
					t.Fatal(err)
				}
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
			wants := []string{"spawn queue"}
			if bound {
				wants = append(wants, "may still hold a pane", "host=machine-a")
			} else {
				wants = append(wants, "no node was asked to run it")
			}
			for _, want := range wants {
				if !strings.Contains(saved.Outcome.Fix, want) {
					t.Fatalf("fix %q does not name %q", saved.Outcome.Fix, want)
				}
			}
		})
	}
}

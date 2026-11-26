package panewire

import (
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
)

func TestSpawnSentryRequesterRefusalsOmitErrorEvent(t *testing.T) {
	for _, reason := range []string{"admission_refused:task_scale", "requester_withdrawn", "brief_invalid", "usage", "spawn_failed:brief_invalid"} {
		t.Run(reason, func(t *testing.T) {
			h := asyncSpawnHub(t)
			transport := &spawnSentryTransport{}
			h.spawnSentry = spawnReasonSentryClient(t, transport)
			req := durableSpawnRequest()
			task, _, err := h.inbox.store.claimSpawn(t.Context(), req, "test-job", "parent", time.Now())
			if err != nil {
				t.Fatal(err)
			}
			result := hubSpawnOutcome{RC: 1, Stage: "placement", Reason: reason}
			if _, err = h.inbox.store.completeSpawn(t.Context(), task, result, time.Now()); err != nil {
				t.Fatal(err)
			}
			h.recordSpawnTrace(task, result)
			transport.mu.Lock()
			defer transport.mu.Unlock()
			for _, event := range transport.events {
				if event.Type != "transaction" && event.Level == sentry.LevelError {
					t.Fatalf("requester refusal %q sent error event: %+v", reason, event)
				}
			}
		})
	}
}

func TestSpawnSentryHostFailureKeepsErrorEvent(t *testing.T) {
	h := asyncSpawnHub(t)
	transport := &spawnSentryTransport{}
	h.spawnSentry = spawnReasonSentryClient(t, transport)
	req := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), req, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	result := hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed"}
	if _, err = h.inbox.store.completeSpawn(t.Context(), task, result, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.recordSpawnTrace(task, result)
	if event, _ := spawnReasonErrorEvent(t, transport); event.Level != sentry.LevelError {
		t.Fatalf("spawn_failed event level=%q, want error", event.Level)
	}
}

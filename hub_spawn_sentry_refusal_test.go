package panewire

import (
	"strings"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
)

func TestSpawnSentryRequesterRefusalsOmitErrorEvent(t *testing.T) {
	for _, reason := range []string{"requester_withdrawn", "brief_invalid", "usage", "spawn_failed:brief_invalid"} {
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
				if event.Type != "transaction" {
					t.Fatalf("requester refusal %q sent Sentry event: %+v", reason, event)
				}
			}
		})
	}
}

func TestSpawnSentryAdmissionRefusalIsWarning(t *testing.T) {
	h := asyncSpawnHub(t)
	transport := &spawnSentryTransport{}
	h.spawnSentry = spawnReasonSentryClient(t, transport)
	req := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), req, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	result := hubSpawnOutcome{RC: 70, Stage: "placement", Reason: "admission_refused: quota catalog unavailable"}
	if _, err = h.inbox.store.completeSpawn(t.Context(), task, result, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.recordSpawnTrace(task, result)
	event, _ := spawnReasonErrorEvent(t, transport)
	if event.Level != sentry.LevelWarning {
		t.Fatalf("admission refusal event level=%q, want warning", event.Level)
	}
	if got, want := strings.Join(event.Fingerprint, "|"), "spawn|placement|admission_refused:quota_catalog"; got != want {
		t.Fatalf("admission refusal fingerprint=%q, want %q", got, want)
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

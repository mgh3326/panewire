package panewire

import (
	"context"
	"testing"
	"time"
)

func TestClaudeDelayedSubmissionProof(t *testing.T) {
	text := task687Text(3162, task626Text)
	busy := task626Claude([]string{"⏺ Working…"}, "❯")
	echo := task626Claude([]string{"❯ " + text}, "❯")
	_, calls := task683FakeHerdr(t, "claude",
		[]string{busy, busy, echo},
		[]string{task626TranscriptOnly, task626TranscriptOnly, "❯ " + text})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result := relayInjectForHarness(ctx, "w1:p1", "claude", text, nil)
	if result.Outcome != relayInjectDelivered {
		t.Fatalf("delayed submitted nonce: got outcome %v, evidence %q; want delivered", result.Outcome, result.Evidence)
	}
	if got := calls(); task683Count(got, "prompt") != 1 || task683Count(got, "send-keys") != 0 {
		t.Fatalf("herdr calls = %q; want one prompt and no keypress", got)
	}
}

func TestClaudeMissingSubmissionProofStaysUnconfirmed(t *testing.T) {
	text := task687Text(3163, task626Text)
	_, calls := task683FakeHerdr(t, "claude", []string{task626ClaudeEmpty}, []string{task626TranscriptOnly})
	// Long enough for the fake herdr's first exec (slow on macOS) and the
	// prompt; short of the 8 s observation, so the context ends it.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result := relayInjectForHarness(ctx, "w1:p1", "claude", text, nil)
	if result.Outcome != relayInjectMaybeInPane {
		t.Fatalf("missing submitted nonce: got outcome %v, evidence %q; want unconfirmed", result.Outcome, result.Evidence)
	}
	if got := calls(); task683Count(got, "prompt") != 1 || task683Count(got, "send-keys") != 0 {
		t.Fatalf("herdr calls = %q; want one prompt and no keypress", got)
	}
}

func TestClaudeDelayedProofEmitsDelivery(t *testing.T) {
	item := relayHeld{Pane: "w1:p1", Lane: "lane-a", EventID: 3164, JobID: "relay-job-3164", Text: task626Text, DeliverPolicy: "idle"}
	busy := task626Claude([]string{"⏺ Working…"}, "❯")
	echo := "❯ " + relayNonce(item)
	_, _ = task683FakeHerdr(t, "claude", []string{busy, busy, task626Claude([]string{echo}, "❯")}, []string{task626TranscriptOnly, task626TranscriptOnly, echo})
	events := make(chan hubClientEvent, 8)
	client := &HubClient{}
	client.setRelayEmitter(func(event hubClientEvent) { events <- event })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client.relayBusyManager().deliver(ctx, []relayHeld{item}, false)
	close(events)
	for event := range events {
		if event.Kind == "relay.delivered" {
			return
		}
		if event.Kind == "relay.unconfirmed" {
			t.Fatalf("delayed submission emitted relay.unconfirmed: %s", event.Payload)
		}
	}
	t.Fatal("delayed submission did not emit relay.delivered")
}

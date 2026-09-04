package panewire

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func r19fCompletion(t *testing.T, root, jobID string, mtime time.Time) {
	t.Helper()
	dir := filepath.Join(root, "jobs", jobID, "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "00002-job.completed.json")
	body := `{"kind":"job.completed","epoch":1,"owner_lane":"lane-a","label":"wrk","host":"node-a","report_path":"/inbox/` + jobID + `/report.md","report_last_line":"VERDICT: DONE"}`
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatal(err)
	}
}

func r19fClient(t *testing.T, root string, store *Store) *HubClient {
	t.Helper()
	client, err := NewHubClient(HubClientConfig{URL: "ws://fixture.invalid", MachineID: "node-a", Token: "node-token", JobsInboxRoot: root, RelayStore: store, AllowInsecureForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// (a) A delivered relay must survive a node process restart. Removing the
// relay_sent write makes this assertion RED because the new client rescans the
// still-authoritative event file as a fresh report.
func TestR19fRestartDoesNotResendAcknowledgedRelay(t *testing.T) {
	root := t.TempDir()
	store := NewMemoryStore(t)
	r19fCompletion(t, root, "restart-acked", time.Now().Add(-time.Minute))
	first := r19fClient(t, root, store)
	events := first.jobCompletionEvents()
	if len(events) != 1 {
		t.Fatalf("first events=%d", len(events))
	}
	first.relaySent(events[0])
	if err := store.RecordRelayAck(t.Context(), events[0].relayKey, "delivered"); err != nil {
		t.Fatal(err)
	}
	if got := r19fClient(t, root, store).jobCompletionEvents(); len(got) != 0 {
		t.Fatalf("acknowledged relay resent after restart: %d", len(got))
	}
}

// (b) A write accepted by the websocket but lacking a hub result gets exactly
// one retry on the next connection, then remains quiet for that connection.
func TestR19fUnacknowledgedRelayRetriesOnceAfterReconnect(t *testing.T) {
	root := t.TempDir()
	store := NewMemoryStore(t)
	r19fCompletion(t, root, "restart-unacked", time.Now().Add(-time.Minute))
	first := r19fClient(t, root, store)
	events := first.jobCompletionEvents()
	if len(events) != 1 {
		t.Fatalf("first events=%d", len(events))
	}
	first.relaySent(events[0])
	restarted := r19fClient(t, root, store)
	if got := restarted.jobCompletionEvents(); len(got) != 1 {
		t.Fatalf("unacknowledged retry=%d", len(got))
	}
	if got := restarted.jobCompletionEvents(); len(got) != 0 {
		t.Fatalf("retry was not bounded to one connection: %d", len(got))
	}
}

func r19fHub(t *testing.T, now time.Time) (*HubServer, chan hubRelayInjectEvent, chan hubSubscriptionMessage) {
	t.Helper()
	routes := filepath.Join(t.TempDir(), "lanes.json")
	if err := os.WriteFile(routes, []byte(`{"lanes":{"lane-a":{"machine":"node-b","pane":"w1:p1"}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "operator-token", "node-a": "node-token", "node-b": "node-b-token"}, ReportRelayPath: routes, Now: func() time.Time { return now }, RelayReplayGrace: 10 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	relays := make(chan hubRelayInjectEvent, 1)
	hub.nodes["node-b"] = &hubNodeRecord{agent: &hubAgent{relays: relays}}
	sub := make(chan hubSubscriptionMessage, 1)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	hub.subscribers[&hubEventSubscriber{ctx: ctx, cancel: cancel, messages: sub}] = struct{}{}
	return hub, relays, sub
}

// (c) The hub is deliberately stateless, so it suppresses only a marked old
// first-scan replay. Removing the replay predicate makes this inject instead.
func TestR19fHubSuppressesOldMarkedReplay(t *testing.T) {
	now := time.Now().UTC()
	hub, relays, feed := r19fHub(t, now)
	event := hubJobEventPayload{JobID: "old-replay", Epoch: 1, OwnerLane: "lane-a", ReportPath: "report.md", EventTime: now.Add(-11 * time.Minute), Replay: true}
	hub.relayJobEventFrom("node-a", "job.completed", event)
	select {
	case inject := <-relays:
		t.Fatalf("old replay injected: %+v", inject)
	default:
	}
	select {
	case result := <-feed:
		if result.event == nil || result.event.Kind != "relay.replayed" {
			t.Fatalf("feed=%+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("old replay was not visible in operator feed")
	}
}

// The target's delivered/unconfirmed receipt is returned to the source with
// every component of the durable relay key; otherwise a node could only guess
// which same-job escalation to mark complete.
func TestR19fHubReturnsAcknowledgementToOriginatingNode(t *testing.T) {
	now := time.Now().UTC()
	hub, relays, _ := r19fHub(t, now)
	acks := make(chan hubRelayAckEvent, 1)
	hub.nodes["node-a"] = &hubNodeRecord{agent: &hubAgent{relayAcks: acks}}
	event := hubJobEventPayload{JobID: "ack-key", Epoch: 3, OwnerLane: "lane-a", ReportPath: "report.md", Reason: "needs review", EventTime: now}
	hub.relayJobEventFrom("node-a", "job.completed", event)
	<-relays
	pending, ok := hub.acknowledgeRelay("node-b", relayAckPayload{JobID: "ack-key", Pane: "w1:p1"})
	if !ok {
		t.Fatal("target acknowledgement was not pending")
	}
	hub.sendRelayAck(pending, "delivered")
	select {
	case ack := <-acks:
		if ack.Status != "delivered" || ack.Kind != "job.completed" || ack.JobID != event.JobID || ack.Epoch != event.Epoch || ack.ReportPath != event.ReportPath || ack.Reason != event.Reason {
			t.Fatalf("ack=%+v", ack)
		}
	case <-time.After(time.Second):
		t.Fatal("source did not receive relay acknowledgement")
	}
}

// (d) A completion whose file appears after node start is not replay and must
// follow the normal relay-inject path even during the hub grace window.
func TestR19fNewFileAfterNodeStartInjectsNormally(t *testing.T) {
	root := t.TempDir()
	client := r19fClient(t, root, nil)
	now := time.Now().UTC()
	client.relayStartedAt = now
	r19fCompletion(t, root, "fresh-file", now.Add(time.Second))
	events := client.jobCompletionEvents()
	if len(events) != 1 {
		t.Fatalf("events=%d", len(events))
	}
	var payload hubJobEventPayload
	if err := json.Unmarshal(events[0].Payload, &payload); err != nil || payload.Replay || !payload.EventTime.After(now) {
		t.Fatalf("payload=%s err=%v", events[0].Payload, err)
	}
	hub, relays, _ := r19fHub(t, now)
	hub.relayJobEventFrom("node-a", events[0].Kind, payload)
	select {
	case inject := <-relays:
		if inject.JobID != "fresh-file" {
			t.Fatalf("inject=%+v", inject)
		}
	case <-time.After(time.Second):
		t.Fatal("new completion was not injected")
	}
}

// (e) relay_sent retention follows PANEWIRE_JOB_ACTIVE_MAX_AGE. If its GC is
// removed, this leaves expired rows behind forever.
func TestR19fRelaySentGCUsesActiveMaxAge(t *testing.T) {
	t.Setenv("PANEWIRE_JOB_ACTIVE_MAX_AGE", "1h")
	store := NewMemoryStore(t)
	if err := store.RecordRelaySent(t.Context(), "expired", time.Now().Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadRelaySent(t.Context(), time.Now().Add(-hubJobActiveMaxAge())); err != nil {
		t.Fatal(err)
	}
	rows, err := store.LoadRelaySent(t.Context(), time.Now().Add(-hubJobActiveMaxAge()))
	if err != nil || len(rows) != 0 {
		t.Fatalf("expired relay rows=%d err=%v", len(rows), err)
	}
}

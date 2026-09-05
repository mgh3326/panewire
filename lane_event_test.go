package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"
)

func emitLaneEvent(t *testing.T, inbox, lane, eventID, text string) (int, string) {
	t.Helper()
	var stderr bytes.Buffer
	code := runEmitCLI([]string{"--kind", "lane.event", "--lane", lane, "--event-id", eventID, "--text", text, "--inbox-root", inbox}, &bytes.Buffer{}, &stderr, CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent.sock")})
	return code, stderr.String()
}

func scannedLaneEvent(t *testing.T, inbox string, store *Store) hubScannedRelayEvent {
	t.Helper()
	events := r20Node(inbox, store).jobCompletionEvents()
	if len(events) != 1 || events[0].Kind != "lane.event" {
		t.Fatalf("scanned events=%+v", events)
	}
	event, ok := decodeHubLaneEventPayload(events[0].Payload)
	if !ok {
		t.Fatalf("lane payload=%s", events[0].Payload)
	}
	return hubScannedRelayEvent{Kind: "lane.event", HubActiveJob: HubActiveJob{JobID: event.JobID, Epoch: event.Epoch, OwnerLane: event.OwnerLane}, EventID: event.EventID, Text: event.Text, Truncated: event.Truncated}
}

// AC-a, f, and h: the fixture is produced by panewire emit, persists before a
// route exists, is acknowledged to its source, and injects once after the
// destination registers without a hub restart.
func TestLaneEventPersistsUnroutedThenReplaysOnce(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	routes := r20LanesFile(t, `{"lanes":{}}`)
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "op", "host-a": "source", "host-b": "target"}, ReportRelayPath: routes, handoffkeep: client})
	if err != nil {
		t.Fatal(err)
	}
	source := &hubAgent{persisted: make(chan hubRelayPersistedEvent, 2)}
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 2), persisted: make(chan hubRelayPersistedEvent, 2)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: source}
	inbox, outbox := t.TempDir(), NewMemoryStore(t)
	defer outbox.Close()
	if code, stderr := emitLaneEvent(t, inbox, "lane-a", "producer-1", "payload"); code != ExitOK || !strings.Contains(stderr, "event recorded to file only") {
		t.Fatalf("emit code=%d stderr=%q", code, stderr)
	}
	scanned := scannedLaneEvent(t, inbox, outbox)
	hub.relayLaneEvent(hubJobEventPayload{JobID: scanned.JobID, Epoch: scanned.Epoch, OwnerLane: scanned.OwnerLane, EventID: scanned.EventID, Text: scanned.Text}, source)
	if fake.rowCount() != 1 {
		t.Fatalf("unrouted lane event was not persisted: rows=%d", fake.rowCount())
	}
	if acks := drainPersisted(source); len(acks) != 1 || acks[0].Lane != "lane-a" || acks[0].ProducerEventID != "producer-1" {
		t.Fatalf("source persisted acknowledgement=%+v", acks)
	}
	if injected := drainRelays(destination); injected != 0 {
		t.Fatalf("unrouted event injected %d times", injected)
	}
	if len(hub.jobs) != 0 {
		t.Fatalf("lane event entered the hub job registry: %+v", hub.jobs)
	}
	if err := os.WriteFile(routes, []byte(`{"lanes":{"lane-a":{"machine":"host-b","pane":"w1:p1"}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	hub.nodes["host-b"] = &hubNodeRecord{agent: destination}
	hub.replayUndeliveredLaneEvents(context.Background())
	select {
	case inject := <-destination.relays:
		want := "(같은 내용이 두 번 보이면 재실행 금지) [event] lane-a :: payload"
		if inject.Pane != "w1:p1" || inject.Text != want {
			t.Fatalf("injection=%+v want=%q", inject, want)
		}
	default:
		t.Fatal("registered destination did not receive replay")
	}
	// A second registration scan must not create a second in-memory injection.
	hub.replayUndeliveredLaneEvents(context.Background())
	if injected := drainRelays(destination); injected != 0 {
		t.Fatalf("replay injected again %d times", injected)
	}
}

func TestLaneEventDuplicateEmitIsLoudAndMakesNoSecondFile(t *testing.T) {
	inbox := t.TempDir()
	if code, _ := emitLaneEvent(t, inbox, "lane-a", "producer-1", "payload"); code != ExitOK {
		t.Fatalf("first emit code=%d", code)
	}
	code, stderr := emitLaneEvent(t, inbox, "lane-a", "producer-1", "changed")
	if code == ExitOK || !strings.Contains(stderr, "duplicate event_id") {
		t.Fatalf("duplicate code=%d stderr=%q", code, stderr)
	}
	entries, err := os.ReadDir(filepath.Join(inbox, "events-lane"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("lane event files=%+v err=%v", entries, err)
	}
}

func TestLaneEventConcurrentDuplicateNeverOverwritesTheFirstFile(t *testing.T) {
	inbox := t.TempDir()
	record := emitRecord{Type: "lane.event", OwnerLane: "lane-a", EventID: "producer-race", Text: "payload"}
	start := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			_, err := writeLaneEmitRecord(inbox, record)
			results <- err
		}()
	}
	close(start)
	first, second := <-results, <-results
	successes, duplicates := 0, 0
	for _, err := range []error{first, second} {
		if err == nil {
			successes++
		} else if errors.Is(err, errDuplicateLaneEventID) {
			duplicates++
		}
	}
	if successes != 1 || duplicates != 1 {
		t.Fatalf("concurrent results successes=%d duplicates=%d first=%v second=%v", successes, duplicates, first, second)
	}
	entries, err := os.ReadDir(filepath.Join(inbox, "events-lane"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("concurrent lane files=%+v err=%v", entries, err)
	}
}

func TestLaneEventHubDedupeUsesLaneAndProducerIDOnly(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, nil)
	source := &hubAgent{persisted: make(chan hubRelayPersistedEvent, 4)}
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 4)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}
	// JobID is only relay-envelope plumbing for this kind. These deliberately
	// differ to prove the dedupe key is exclusively (lane,event_id).
	hub.relayLaneEvent(hubJobEventPayload{JobID: "transport-a", Epoch: 1, OwnerLane: "lane-a", EventID: "producer-dedupe", Text: "first"}, source)
	hub.relayLaneEvent(hubJobEventPayload{JobID: "transport-b", Epoch: 99, OwnerLane: "lane-a", EventID: "producer-dedupe", Text: "changed"}, source)
	injections := drainRelays(destination)
	if fake.rowCount() != 1 || injections != 1 {
		t.Fatalf("lane/event dedupe rows=%d injections=%d", fake.rowCount(), injections)
	}
}

func TestLaneEventOutboxRetiresOnlyOnPersistedAckAcrossRestart(t *testing.T) {
	inbox, store := t.TempDir(), NewMemoryStore(t)
	defer store.Close()
	if code, _ := emitLaneEvent(t, inbox, "lane-a", "producer-outbox", "payload"); code != ExitOK {
		t.Fatalf("emit code=%d", code)
	}
	first := r20Node(inbox, store)
	events := first.jobCompletionEvents()
	if len(events) != 1 {
		t.Fatalf("first events=%d", len(events))
	}
	first.commitRelaySent(events[0])
	first.recordRelayPersisted(hubOutboundMessage{Type: "relay.persisted", Kind: "lane.event", JobID: laneEventTransportID("lane-a", "producer-outbox"), EventID: 100, Lane: "lane-a", ProducerEventID: "producer-outbox"})
	for restart := 0; restart < 3; restart++ {
		if events := r20Node(inbox, store).jobCompletionEvents(); len(events) != 0 {
			t.Fatalf("restart %d resent persisted lane event: %d", restart, len(events))
		}
	}
}

func TestLaneEventTextTruncatesAtNodeAndRejectsControlCharacters(t *testing.T) {
	inbox, outbox := t.TempDir(), NewMemoryStore(t)
	defer outbox.Close()
	if code, _ := emitLaneEvent(t, inbox, "lane-a", "producer-2", strings.Repeat("가", 1000)); code != ExitOK {
		t.Fatalf("long emit code=%d", code)
	}
	scanned := scannedLaneEvent(t, inbox, outbox)
	if len(scanned.Text) != laneEventTextLimit || !utf8.ValidString(scanned.Text) || !strings.Contains(scanned.Text, "[truncated]") || !scanned.Truncated {
		t.Fatalf("truncated text bytes=%d valid=%t payload=%q truncated=%t", len(scanned.Text), utf8.ValidString(scanned.Text), scanned.Text, scanned.Truncated)
	}
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, nil)
	source := &hubAgent{persisted: make(chan hubRelayPersistedEvent, 2)}
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 2)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}
	seen := r20t5Subscribe(t, hub)
	hub.relayLaneEvent(hubJobEventPayload{JobID: scanned.JobID, Epoch: scanned.Epoch, OwnerLane: scanned.OwnerLane, EventID: scanned.EventID, Text: scanned.Text, Truncated: scanned.Truncated}, source)
	if fake.rowCount() != 1 || drainRelays(destination) != 1 || len(seen("relay.truncated")) != 1 {
		t.Fatalf("rows=%d inject=%d truncated=%d", fake.rowCount(), drainRelays(destination), len(seen("relay.truncated")))
	}
	invalidInbox := t.TempDir()
	if code, _ := emitLaneEvent(t, invalidInbox, "lane-a", "producer-3", "bad\ntext"); code == ExitOK {
		t.Fatal("control-character text was accepted")
	}
	if _, err := os.Stat(filepath.Join(invalidInbox, "events-lane")); !os.IsNotExist(err) {
		t.Fatalf("invalid text wrote lane files: err=%v", err)
	}
	malformed, err := json.Marshal(map[string]string{"owner_lane": "lane-a", "event_id": "producer-3", "text": "bad\ntext"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := decodeHubLaneEventPayload(malformed); ok {
		t.Fatal("hub accepted control-character lane text")
	}
}

func TestLaneEventUsesDirectLaneWhileEscalationUsesParent(t *testing.T) {
	_, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1","parent":"lane-parent"},"lane-parent":{"machine":"host-a","pane":"w1:p2"}}}`, client, nil)
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 4), persisted: make(chan hubRelayPersistedEvent, 4)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: agent}
	hub.relayLaneEvent(hubJobEventPayload{JobID: laneEventTransportID("lane-a", "producer-4"), Epoch: 1, OwnerLane: "lane-a", EventID: "producer-4", Text: "payload"}, agent)
	hub.relayJobEvent("job.escalate", hubJobEventPayload{JobID: "job-parent", Epoch: 1, OwnerLane: "lane-a", Reason: "reason", ReportPath: "report.md"})
	first, second := <-agent.relays, <-agent.relays
	if first.Pane != "w1:p1" || second.Pane != "w1:p2" {
		t.Fatalf("direct=%+v parent=%+v", first, second)
	}
}

// lane.event must not alter the three established job relay contracts: a
// completed report stays on its owner lane, escalation and joined follow the
// parent, duplicate job reports do not inject twice, and report text remains
// subject to the existing 240-byte compatibility limit.
func TestJobRelayKindsKeepRoutingDedupeAndTruncation(t *testing.T) {
	_, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1","parent":"lane-parent"},"lane-parent":{"machine":"host-a","pane":"w1:p2"}}}`, client, nil)
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 8), persisted: make(chan hubRelayPersistedEvent, 8)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: agent}

	completed := hubJobEventPayload{JobID: "job-completed", Epoch: 1, OwnerLane: "lane-a", Label: "label", Host: "host-a", ReportPath: "report.md", ReportLastLine: strings.Repeat("x", 300)}
	hub.relayJobEvent("job.completed", completed)
	hub.relayJobEvent("job.completed", completed) // Existing key stays idempotent.
	hub.relayJobEvent("job.escalate", hubJobEventPayload{JobID: "job-escalate", Epoch: 1, OwnerLane: "lane-a", Label: "label", Host: "host-a", ReportPath: "report.md", Question: "question"})
	hub.relayJobEvent("job.joined", hubJobEventPayload{JobID: "job-joined", Epoch: 1, OwnerLane: "lane-a", Label: "label", Host: "host-a", ReportPath: "report.md", PR: "https://example.test/pr/1", Head: "abcdef012345"})

	completedInject, escalationInject, joinedInject := <-agent.relays, <-agent.relays, <-agent.relays
	if completedInject.Pane != "w1:p1" || !strings.Contains(completedInject.Text, strings.Repeat("x", 240)) || strings.Contains(completedInject.Text, strings.Repeat("x", 241)) {
		t.Fatalf("completed route/text=%+v", completedInject)
	}
	if escalationInject.Pane != "w1:p2" || joinedInject.Pane != "w1:p2" {
		t.Fatalf("parent routes escalation=%+v joined=%+v", escalationInject, joinedInject)
	}
	if injected := drainRelays(agent); injected != 0 {
		t.Fatalf("duplicate job event injected %d times", injected)
	}
}

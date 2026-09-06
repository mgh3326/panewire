package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestT22LanePersistedDeliveryRetiresStateWithoutHandoffkeep(t *testing.T) {
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, nil, nil)
	agent := &hubAgent{}
	hub.nodes["host-a"] = &hubNodeRecord{agent: agent}
	event := hubJobEventPayload{JobID: laneEventTransportID("lane-a", "delivery"), Epoch: 1, OwnerLane: "lane-a", EventID: "delivery", Text: "payload"}
	key := relayEventDedupeKey("lane.event", event)
	hub.rememberLanePersisted(key, 101)
	hub.mu.Lock()
	if !hub.r19a.rememberRelayTimeout(event.JobID) {
		hub.mu.Unlock()
		t.Fatal("timeout suppression entry was not added")
	}
	hub.mu.Unlock()
	if _, present := hub.lanePersisted[key]; !present {
		t.Fatal("lanePersisted is missing before relay.delivered")
	}
	hub.startRelayAckEvent("lane.event", event, "host-a", "w1:p1", 101)
	payload, err := json.Marshal(struct {
		Type    string          `json:"type"`
		Kind    string          `json:"kind"`
		Payload relayAckPayload `json:"payload"`
	}{Type: "event", Kind: "relay.delivered", Payload: relayAckPayload{JobID: event.JobID, Pane: "w1:p1"}})
	if err != nil {
		t.Fatal(err)
	}
	hub.handleAgentMessage("host-a", "test-remote", agent, payload)
	hub.mu.Lock()
	_, persisted := hub.lanePersisted[key]
	_, timedOut := hub.r19a.relayTimeouts[event.JobID]
	hub.mu.Unlock()
	if persisted {
		t.Fatal("lanePersisted retained the delivered lane event")
	}
	if timedOut {
		t.Fatal("relayTimeouts retained the delivered job")
	}
}

func TestT22LanePersistedDeliveryRetiresAfterInjection(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, nil)
	source := &hubAgent{persisted: make(chan hubRelayPersistedEvent, 1)}
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 1)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}
	event := hubJobEventPayload{JobID: laneEventTransportID("lane-a", "injected"), Epoch: 1, OwnerLane: "lane-a", EventID: "injected", Text: "payload"}
	key := relayEventDedupeKey("lane.event", event)
	hub.relayLaneEvent(event, source)
	if fake.rowCount() != 1 || drainRelays(destination) != 1 {
		t.Fatalf("durable lane injection rows=%d injections=%d", fake.rowCount(), drainRelays(destination))
	}
	if _, present := hub.lanePersisted[key]; !present {
		t.Fatal("lanePersisted is missing before destination delivery")
	}
	payload, err := json.Marshal(struct {
		Type    string          `json:"type"`
		Kind    string          `json:"kind"`
		Payload relayAckPayload `json:"payload"`
	}{Type: "event", Kind: "relay.delivered", Payload: relayAckPayload{JobID: event.JobID, Pane: "w1:p1"}})
	if err != nil {
		t.Fatal(err)
	}
	hub.handleAgentMessage("host-a", "test-remote", destination, payload)
	if _, present := hub.lanePersisted[key]; present {
		t.Fatal("lanePersisted retained the destination-delivered lane event")
	}
}

func TestT22LanePersistedCapUsesAccessLRU(t *testing.T) {
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, nil, nil)
	oldest := hubJobEventPayload{JobID: laneEventTransportID("lane-a", "oldest"), Epoch: 1, OwnerLane: "lane-a", EventID: "oldest", Text: "payload"}
	oldestKey := relayEventDedupeKey("lane.event", oldest)
	hub.rememberLanePersisted(oldestKey, 1)
	var expectedEviction string
	for index := 0; index < lanePersistedMaxEntries-1; index++ {
		event := hubJobEventPayload{OwnerLane: "lane-a", EventID: fmt.Sprintf("event-%d", index)}
		key := relayEventDedupeKey("lane.event", event)
		if index == 0 {
			expectedEviction = key
		}
		hub.rememberLanePersisted(key, int64(index+2))
	}
	// Producer ACK-loss recovery is also an access and must protect this key.
	hub.relayLaneEvent(oldest, &hubAgent{persisted: make(chan hubRelayPersistedEvent, 1)})
	newest := hubJobEventPayload{OwnerLane: "lane-a", EventID: "newest"}
	hub.rememberLanePersisted(relayEventDedupeKey("lane.event", newest), lanePersistedMaxEntries+1)
	hub.mu.Lock()
	length := len(hub.lanePersisted)
	_, oldestPresent := hub.lanePersisted[oldestKey]
	_, evictedPresent := hub.lanePersisted[expectedEviction]
	hub.mu.Unlock()
	if length > lanePersistedMaxEntries {
		t.Fatalf("lanePersisted entries=%d, cap=%d", length, lanePersistedMaxEntries)
	}
	if !oldestPresent || evictedPresent {
		t.Fatalf("LRU access eviction oldest_present=%t expected_eviction_present=%t", oldestPresent, evictedPresent)
	}
}

func TestT22RepeatedLaneAckTimeoutRecordsEveryAttempt(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, nil)
	source := &hubAgent{persisted: make(chan hubRelayPersistedEvent, 1)}
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 2)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}
	seen := r20t5Subscribe(t, hub)
	event := hubJobEventPayload{JobID: laneEventTransportID("lane-a", "timeout"), Epoch: 1, OwnerLane: "lane-a", EventID: "timeout", Text: "payload"}
	key := relayEventDedupeKey("lane.event", event)
	hub.relayLaneEvent(event, source)
	persisted := <-source.persisted
	initialAttempts := fake.laneAttemptsFor("lane-a", "timeout")
	if _, held := hub.relayDedupe[key]; !held {
		t.Fatal("first lane injection did not claim the dedupe key")
	}
	hub.expireRelayAck(event.JobID)
	if _, held := hub.relayDedupe[key]; held {
		t.Fatal("first timeout retained the lane dedupe claim")
	}
	if attempts := fake.laneAttemptsFor("lane-a", "timeout"); attempts != initialAttempts+1 {
		t.Fatalf("first timeout attempts=%d, want %d", attempts, initialAttempts+1)
	}
	// This is the same durable row's next delivery window. The timeout
	// suppression must not prevent its cleanup and durable attempt recording.
	hub.rememberRelayEvent(key, persisted.EventID)
	hub.startRelayAckEvent("lane.event", event, "host-a", "w1:p1", persisted.EventID)
	hub.expireRelayAck(event.JobID)
	if _, held := hub.relayDedupe[key]; held {
		t.Fatal("second timeout retained the lane dedupe claim")
	}
	if attempts := fake.laneAttemptsFor("lane-a", "timeout"); attempts != initialAttempts+2 {
		t.Fatalf("second timeout attempts=%d, want %d", attempts, initialAttempts+2)
	}
	if unconfirmed := seen("relay.unconfirmed"); len(unconfirmed) != 1 {
		t.Fatalf("relay.unconfirmed broadcasts=%d, want 1", len(unconfirmed))
	}
	hub.replayUndeliveredLaneEvents(context.Background())
	if exhausted := seen("relay.replay_exhausted"); len(exhausted) != 1 {
		t.Fatalf("relay.replay_exhausted broadcasts=%d, want 1 after three attempts", len(exhausted))
	}
}

func TestT22RelayTimeoutsCapAndDeliveryCleanup(t *testing.T) {
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, nil, nil)
	remember := func(jobID string) {
		hub.mu.Lock()
		hub.r19a.rememberRelayTimeout(jobID)
		hub.mu.Unlock()
	}
	remember("oldest")
	for index := 0; index < relayTimeoutsMaxEntries-1; index++ {
		remember(fmt.Sprintf("timeout-%d", index))
	}
	remember("oldest")
	remember("newest")
	hub.mu.Lock()
	length := len(hub.r19a.relayTimeouts)
	_, oldestPresent := hub.r19a.relayTimeouts["oldest"]
	_, evictedPresent := hub.r19a.relayTimeouts["timeout-0"]
	hub.mu.Unlock()
	if length > relayTimeoutsMaxEntries || !oldestPresent || evictedPresent {
		t.Fatalf("relayTimeouts entries=%d oldest_present=%t expected_eviction_present=%t", length, oldestPresent, evictedPresent)
	}
	event := hubJobEventPayload{JobID: "delivery", Epoch: 1, OwnerLane: "lane-a"}
	remember(event.JobID)
	hub.startRelayAckEvent("job.completed", event, "host-a", "w1:p1", 0)
	pending, acknowledged := hub.acknowledgeRelayPending("host-a", relayAckPayload{JobID: event.JobID, Pane: "w1:p1"})
	if !acknowledged {
		t.Fatal("delivery acknowledgement did not match its timeout cleanup window")
	}
	hub.markRelayEventDelivered(pending)
	hub.mu.Lock()
	_, retained := hub.r19a.relayTimeouts[event.JobID]
	hub.mu.Unlock()
	if retained {
		t.Fatal("relayTimeouts retained the delivered job")
	}
}

func TestT22ReplayExhaustedAnnouncedOnceAcrossHellos(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	fake.seedUndelivered(handoffkeepRelayEvent{ID: 404, Kind: "lane.event", JobID: laneEventTransportID("lane-a", "spent"), Epoch: 1, OwnerLane: "lane-a", EventID: "spent", Text: "payload", Attempts: relayReplayMaxAttempts})
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, nil)
	seen := r20t5Subscribe(t, hub)
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 1)}
	hub.connect("host-a", "test", "test-remote", agent, true)
	waitForT22(t, func() bool { return len(fake.queries("/v1/relay/events")) >= 1 })
	if exhausted := seen("relay.replay_exhausted"); len(exhausted) != 1 {
		t.Fatalf("first hello relay.replay_exhausted broadcasts=%d, want 1", len(exhausted))
	}
	hub.connect("host-a", "test", "test-remote", agent, true)
	waitForT22(t, func() bool { return len(fake.queries("/v1/relay/events")) >= 2 })
	if exhausted := seen("relay.replay_exhausted"); len(exhausted) != 1 {
		t.Fatalf("two hellos relay.replay_exhausted broadcasts=%d, want 1", len(exhausted))
	}
	if count := hub.ReplayExhaustedEventCount(); count != 1 {
		t.Fatalf("two hellos replay exhausted counter=%d, want 1", count)
	}
}

func TestT22ReplayExhaustedRememberedSetUsesLRUCap(t *testing.T) {
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, nil, nil)
	for id := int64(1); id <= relayReplayExhaustedMaxEntries; id++ {
		if !hub.countReplayExhaustedEvent(id) {
			t.Fatalf("first observation of durable row %d was suppressed", id)
		}
	}
	// A repeated observation is an access, so row 1 survives the next insert.
	if hub.countReplayExhaustedEvent(1) {
		t.Fatal("repeated durable row was counted twice")
	}
	if !hub.countReplayExhaustedEvent(relayReplayExhaustedMaxEntries + 1) {
		t.Fatal("new durable row was suppressed")
	}
	hub.mu.Lock()
	length := len(hub.replayExhausted)
	_, firstPresent := hub.replayExhausted[1]
	_, evictedPresent := hub.replayExhausted[2]
	hub.mu.Unlock()
	if length > relayReplayExhaustedMaxEntries || !firstPresent || evictedPresent {
		t.Fatalf("replay exhausted set entries=%d first_present=%t expected_eviction_present=%t", length, firstPresent, evictedPresent)
	}
}

func waitForT22(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for local fixture")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

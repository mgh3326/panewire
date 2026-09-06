package panewire

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

// r20t5Hub is r20Hub plus a connected node, which every acknowledgement test
// needs before the hub has anywhere to send relay.persisted.
func r20t5Hub(t *testing.T, lanes string, client *handoffkeepRelayClient, relayDepth int) (*HubServer, *hubAgent) {
	t.Helper()
	hub := r20Hub(t, lanes, client, nil)
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, relayDepth), persisted: make(chan hubRelayPersistedEvent, 16)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: agent}
	return hub, agent
}

const r20t5OneLane = `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`

func r20t5Event(jobID, reason string) hubJobEventPayload {
	return hubJobEventPayload{JobID: jobID, Epoch: 1, OwnerLane: "lane-a", Label: "wrk-a", Host: "host-a", ReportPath: "report.md", ReportLastLine: "done", Reason: reason}
}

// drainRelays reports how many injections are queued and empties the channel.
func drainRelays(agent *hubAgent) int {
	total := 0
	for {
		select {
		case <-agent.relays:
			total++
		default:
			return total
		}
	}
}

// drainPersisted collects the acknowledgements queued so far.
func drainPersisted(agent *hubAgent) []hubRelayPersistedEvent {
	var out []hubRelayPersistedEvent
	for {
		select {
		case event := <-agent.persisted:
			out = append(out, event)
		default:
			return out
		}
	}
}

// r20t5Subscribe attaches an operator event subscriber and returns a reader for
// the broadcast kinds a test cares about.
func r20t5Subscribe(t *testing.T, hub *HubServer) func(kind string) []json.RawMessage {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	subscriber := &hubEventSubscriber{ctx: ctx, cancel: cancel, messages: make(chan hubSubscriptionMessage, 32)}
	hub.mu.Lock()
	hub.subscribers[subscriber] = struct{}{}
	hub.mu.Unlock()
	var seen []hubEvent
	return func(kind string) []json.RawMessage {
		for {
			select {
			case message := <-subscriber.messages:
				if message.event != nil {
					seen = append(seen, *message.event)
				}
				continue
			default:
			}
			break
		}
		var out []json.RawMessage
		for _, event := range seen {
			if event.Kind == kind {
				out = append(out, event.Payload)
			}
		}
		return out
	}
}

// TP1 is the operator's symptom, reproduced: a local outbox row whose
// persisted_at never filled made the node resend on every restart, and every
// resend was swallowed by the hub. Three restarts must produce at most one
// resend, no re-injection at all, and a filled persisted_at.
func TestR20T5NodeRestartsResendOnceAndStopWithPersistedAt(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub, agent := r20t5Hub(t, r20t5OneLane, client, 8)

	inbox := t.TempDir()
	store := NewMemoryStore(t)
	defer store.Close()
	r20WriteEvent(t, inbox, "r20t5-restart", "00001-job.completed.json", `{"type":"job.completed","epoch":1,"owner_lane":"lane-a","label":"wrk-a","host":"host-a","report_path":"a.md","report_last_line":"done"}`, time.Time{})
	key := relayOutboxKey{Kind: "job.completed", JobID: "r20t5-restart", Epoch: 1, ReportPath: "a.md"}

	// The first send is the one that got lost: the hub persisted and injected,
	// but the node never saw relay.persisted, so its row stays unpersisted.
	first := r20Node(inbox, store)
	sent := first.jobCompletionEvents()
	if len(sent) != 1 {
		t.Fatalf("the first process sent %d events, want 1", len(sent))
	}
	completion, ok := decodeHubJobCompletionPayload(sent[0].Payload)
	if !ok {
		t.Fatalf("payload=%s", sent[0].Payload)
	}
	hub.relayJobCompletion(completion)
	if injected := drainRelays(agent); injected != 1 {
		t.Fatalf("the first send injected %d times, want 1", injected)
	}
	drainPersisted(agent)
	if state, err := store.RelayOutboxState(context.Background(), key); err != nil || state.Persisted {
		t.Fatalf("the reproduction needs an unpersisted row: state=%+v err=%v", state, err)
	}

	resends, reinjections := 0, 0
	for restart := 1; restart <= 3; restart++ {
		// Age the attempt out of the retry backoff, so persisted_at is the only
		// thing that can hold the row back.
		if err := store.RecordRelaySent(context.Background(), key, time.Now().Add(-2*relayOutboxBackoff)); err != nil {
			t.Fatal(err)
		}
		node := r20Node(inbox, store)
		for _, event := range node.jobCompletionEvents() {
			resends++
			payload, decoded := decodeHubJobCompletionPayload(event.Payload)
			if !decoded {
				t.Fatalf("restart %d payload=%s", restart, event.Payload)
			}
			hub.relayJobCompletion(payload)
			reinjections += drainRelays(agent)
			// The node is connected now, so it applies what the hub answers.
			for _, ack := range drainPersisted(agent) {
				node.recordRelayPersisted(hubOutboundMessage{Type: ack.Type, JobID: ack.JobID, Kind: ack.Kind, Epoch: ack.Epoch, ReportPath: ack.ReportPath, Reason: ack.Reason, EventID: ack.EventID})
			}
		}
	}
	if resends > 1 {
		t.Fatalf("three restarts resent the record %d times, want at most 1", resends)
	}
	if reinjections != 0 {
		t.Fatalf("the parent pane was re-injected %d times across restarts, want 0", reinjections)
	}
	state, err := store.RelayOutboxState(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if !state.Persisted {
		t.Fatal("persisted_at is still NULL after the hub answered the resend: the row will be resent forever")
	}
	if fake.rowCount() != 1 {
		t.Fatalf("handoffkeep holds %d rows for one event", fake.rowCount())
	}
}

// TP2: a resend of an event the hub already took is acknowledged again and
// injected no further. The re-POST is the attempt bump, not a second row.
func TestR20T5DedupeHitReacknowledgesWithoutReinjecting(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub, agent := r20t5Hub(t, r20t5OneLane, client, 8)

	hub.relayJobCompletion(r20t5Event("r20t5-dedupe", ""))
	hub.relayJobCompletion(r20t5Event("r20t5-dedupe", ""))

	if injected := drainRelays(agent); injected != 1 {
		t.Fatalf("injections=%d, want 1: a resend must not put the note in the pane twice", injected)
	}
	acknowledgements := drainPersisted(agent)
	if len(acknowledgements) != 2 {
		t.Fatalf("relay.persisted count=%d, want 2: the second send is the one whose outbox row is stuck", len(acknowledgements))
	}
	for index, ack := range acknowledgements {
		if ack.EventID == 0 || ack.EventID != acknowledgements[0].EventID || ack.Kind != "job.completed" || ack.ReportPath != "report.md" {
			t.Fatalf("acknowledgement %d=%+v", index, ack)
		}
	}
	if posts := fake.count(http.MethodPost, "/v1/relay/events"); posts != 2 {
		t.Fatalf("handoffkeep POSTs=%d, want 2", posts)
	}
	if fake.rowCount() != 1 {
		t.Fatalf("the resend created a second row: handoffkeep holds %d", fake.rowCount())
	}
	if attempts := fake.attemptsFor("job.completed", "r20t5-dedupe", 1, "report.md", ""); attempts != 2 {
		t.Fatalf("attempts=%d after a resend, want 2 (the duplicate POST is the only counter the contract has)", attempts)
	}
}

// TP3: 201 and 200 are both success, so both must acknowledge. Nothing pinned
// that property before, which is why the upstream hypothesis was plausible.
func TestR20T5BothCreatedAndDuplicateStatusesAcknowledge(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		status int
		job    string
	}{
		{"created", http.StatusCreated, "r20t5-201"},
		{"duplicate", http.StatusOK, "r20t5-200"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fake, client, closeServer := newFakeHandoffkeep(t)
			defer closeServer()
			fake.status = testCase.status
			hub, agent := r20t5Hub(t, r20t5OneLane, client, 4)

			hub.relayJobCompletion(r20t5Event(testCase.job, ""))

			acknowledgements := drainPersisted(agent)
			if len(acknowledgements) != 1 || acknowledgements[0].EventID == 0 {
				t.Fatalf("handoffkeep answered %d, acknowledgements=%+v", testCase.status, acknowledgements)
			}
			if injected := drainRelays(agent); injected != 1 {
				t.Fatalf("injections=%d, want 1", injected)
			}
		})
	}
}

// TP4: an event that found no destination leaves no trace in the dedupe map,
// so the resend after the node reconnects is persisted and injected normally.
func TestR20T5UnroutedEventStaysResendable(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, r20t5OneLane, client, nil)
	events := r20t5Subscribe(t, hub)

	// No node is connected yet: nothing is persisted and nothing is injected.
	hub.relayJobCompletion(r20t5Event("r20t5-unrouted", ""))
	if unrouted := events("relay.unrouted"); len(unrouted) != 1 {
		t.Fatalf("relay.unrouted broadcasts=%d, want 1", len(unrouted))
	}
	if posts := fake.count(http.MethodPost, "/v1/relay/events"); posts != 0 {
		t.Fatalf("an unrouted event was persisted anyway: POSTs=%d", posts)
	}
	key := relayEventDedupeKey("job.completed", r20t5Event("r20t5-unrouted", ""))
	hub.mu.Lock()
	_, blocked := hub.relayDedupe[key]
	hub.mu.Unlock()
	if blocked {
		t.Fatal("an unrouted event left a dedupe key behind: every later resend is swallowed by it")
	}

	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 4), persisted: make(chan hubRelayPersistedEvent, 4)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: agent}
	hub.relayJobCompletion(r20t5Event("r20t5-unrouted", ""))

	if injected := drainRelays(agent); injected != 1 {
		t.Fatalf("the resend after the node reconnected injected %d times, want 1", injected)
	}
	if acknowledgements := drainPersisted(agent); len(acknowledgements) != 1 {
		t.Fatalf("the resend was acknowledged %d times, want 1", len(acknowledgements))
	}
	if posts := fake.count(http.MethodPost, "/v1/relay/events"); posts != 1 {
		t.Fatalf("handoffkeep POSTs=%d, want 1", posts)
	}
}

// TP5: the row is durable even when the injection queue refuses it. The node
// must be released from its outbox; re-injection belongs to the hub's replay.
func TestR20T5InjectQueueFailureStillAcknowledgesAndClearsKey(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub, agent := r20t5Hub(t, r20t5OneLane, client, 1)
	events := r20t5Subscribe(t, hub)
	// Saturate the injection queue so queueRelay cannot take another.
	agent.relays <- hubRelayInjectEvent{Type: "relay.inject", JobID: "filler", Pane: "w1:p1", Text: "filler"}

	hub.relayJobCompletion(r20t5Event("r20t5-queuefull", ""))

	if posts := fake.count(http.MethodPost, "/v1/relay/events"); posts != 1 {
		t.Fatalf("the row must exist before the injection is attempted: POSTs=%d", posts)
	}
	acknowledgements := drainPersisted(agent)
	if len(acknowledgements) != 1 || acknowledgements[0].EventID == 0 {
		t.Fatalf("a persisted row was not acknowledged after a failed injection: %+v", acknowledgements)
	}
	if unrouted := events("relay.unrouted"); len(unrouted) != 1 {
		t.Fatalf("the failed injection was not observable: relay.unrouted=%d", len(unrouted))
	}
	hub.mu.Lock()
	_, blocked := hub.relayDedupe[relayEventDedupeKey("job.completed", r20t5Event("r20t5-queuefull", ""))]
	hub.mu.Unlock()
	if blocked {
		t.Fatal("a failed injection left a dedupe key behind")
	}
}

// TP6: reason is part of the key everywhere else. Two events that differ only
// in reason are two records, not one.
func TestR20T5DedupeKeyCountsReason(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub, agent := r20t5Hub(t, r20t5OneLane, client, 8)

	hub.relayJobCompletion(r20t5Event("r20t5-reason", "blocked"))
	hub.relayJobCompletion(r20t5Event("r20t5-reason", "resolved"))

	if injected := drainRelays(agent); injected != 2 {
		t.Fatalf("injections=%d, want 2: the second reason is a separate record", injected)
	}
	acknowledgements := drainPersisted(agent)
	if len(acknowledgements) != 2 || acknowledgements[0].EventID == acknowledgements[1].EventID {
		t.Fatalf("acknowledgements=%+v, want two distinct rows", acknowledgements)
	}
	if fake.rowCount() != 2 {
		t.Fatalf("handoffkeep holds %d rows, want 2", fake.rowCount())
	}
	for _, reason := range []string{"blocked", "resolved"} {
		if attempts := fake.attemptsFor("job.completed", "r20t5-reason", 1, "report.md", reason); attempts != 1 {
			t.Fatalf("reason=%q attempts=%d, want 1", reason, attempts)
		}
	}
}

// TP7: the startup replay is gated on attempts, and every replay it does spend
// is recorded. A row that has spent its attempts is reported, never dropped.
func TestR20T5ReplayHonorsAttemptsGateAndReportsExhaustion(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	fake.seedUndelivered(
		handoffkeepRelayEvent{ID: 41, Kind: "job.completed", JobID: "r20t5-fresh", Epoch: 1, OwnerLane: "lane-a", ReportPath: "fresh.md", ReportLastLine: "done", Attempts: 0},
		handoffkeepRelayEvent{ID: 42, Kind: "job.completed", JobID: "r20t5-last", Epoch: 1, OwnerLane: "lane-a", ReportPath: "last.md", ReportLastLine: "done", Attempts: 2},
		handoffkeepRelayEvent{ID: 43, Kind: "job.completed", JobID: "r20t5-spent", Epoch: 1, OwnerLane: "lane-a", ReportPath: "spent.md", ReportLastLine: "done", Attempts: 3},
	)
	hub, agent := r20t5Hub(t, r20t5OneLane, client, 8)
	events := r20t5Subscribe(t, hub)

	hub.replayUndeliveredRelayEvents(context.Background())

	replayed := map[string]bool{}
	for {
		select {
		case directive := <-agent.relays:
			replayed[directive.JobID] = true
			continue
		default:
		}
		break
	}
	if !replayed["r20t5-fresh"] || !replayed["r20t5-last"] {
		t.Fatalf("replayed=%v, want both rows under the attempt limit", replayed)
	}
	if replayed["r20t5-spent"] {
		t.Fatal("a row that had already spent its attempts was re-injected: this is the storm")
	}
	exhausted := events("relay.replay_exhausted")
	if len(exhausted) != 1 {
		t.Fatalf("relay.replay_exhausted broadcasts=%d, want 1", len(exhausted))
	}
	var payload struct {
		JobID    string `json:"job_id"`
		EventID  int64  `json:"event_id"`
		Attempts int    `json:"attempts"`
		Reason   string `json:"reason"`
	}
	if json.Unmarshal(exhausted[0], &payload) != nil || payload.JobID != "r20t5-spent" || payload.EventID != 43 || payload.Attempts != 3 || payload.Reason != "attempts_exhausted" {
		t.Fatalf("payload=%s", exhausted[0])
	}
	if hub.ReplayExhaustedEventCount() != 1 {
		t.Fatalf("counter=%d, want 1", hub.ReplayExhaustedEventCount())
	}
	// Each replay spends an attempt, and the row that was not replayed spends
	// nothing. Without this the gate above never converges.
	if got := fake.attemptsFor("job.completed", "r20t5-fresh", 1, "fresh.md", ""); got != 1 {
		t.Fatalf("r20t5-fresh attempts=%d, want 1", got)
	}
	if got := fake.attemptsFor("job.completed", "r20t5-last", 1, "last.md", ""); got != 3 {
		t.Fatalf("r20t5-last attempts=%d, want 3", got)
	}
	if got := fake.attemptsFor("job.completed", "r20t5-spent", 1, "spent.md", ""); got != 3 {
		t.Fatalf("r20t5-spent attempts=%d, want 3 (it must not be touched)", got)
	}
	if fake.rowCount() != 3 {
		t.Fatalf("the replay created rows: handoffkeep holds %d, want 3", fake.rowCount())
	}
}

// TP7b: a row handoffkeep already delivered is never re-injected, whatever the
// listing returns.
func TestR20T5ReplaySkipsDeliveredRows(t *testing.T) {
	_, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub, agent := r20t5Hub(t, r20t5OneLane, client, 4)
	hub.replayRelayEvent(handoffkeepRelayEvent{ID: 51, Kind: "job.completed", JobID: "r20t5-done", Epoch: 1, OwnerLane: "lane-a", ReportPath: "done.md", DeliveredAt: "2026-09-05T00:00:00Z"})
	if injected := drainRelays(agent); injected != 0 {
		t.Fatalf("a delivered row was re-injected %d times", injected)
	}
}

// TP8: an injection nobody confirmed is a spent attempt, and the replay gate
// has to see it.
func TestR20T5UnconfirmedInjectionRecordsAnAttempt(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub, agent := r20t5Hub(t, r20t5OneLane, client, 4)
	hub.mu.Lock()
	hub.r19a.relayAckTimeout = 10 * time.Millisecond
	hub.mu.Unlock()
	events := r20t5Subscribe(t, hub)

	hub.relayJobCompletion(r20t5Event("r20t5-unconfirmed", ""))
	if injected := drainRelays(agent); injected != 1 {
		t.Fatalf("injections=%d, want 1", injected)
	}
	// The node never acknowledges: the window expires on its own.
	deadline := time.Now().Add(3 * time.Second)
	for {
		if attempts := fake.attemptsFor("job.completed", "r20t5-unconfirmed", 1, "report.md", ""); attempts == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("an ack timeout did not record an attempt: attempts=%d, want 2", fake.attemptsFor("job.completed", "r20t5-unconfirmed", 1, "report.md", ""))
		}
		time.Sleep(5 * time.Millisecond)
	}
	// The durable attempt is recorded before the broadcast, so a fast runner
	// can observe attempts==2 in the small window before the event fanout.
	// Wait for the same deadline-bound asynchronous outcome rather than making
	// that scheduling window a platform-dependent failure.
	for {
		if unconfirmed := events("relay.unconfirmed"); len(unconfirmed) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("relay.unconfirmed broadcast was not observed after attempt recording")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if fake.rowCount() != 1 {
		t.Fatalf("the attempt bump created a row: handoffkeep holds %d", fake.rowCount())
	}
}

// TP9: without --handoffkeep-env none of this exists. The hub calls handoffkeep
// zero times and dedupes a resend exactly as it did before R20.
func TestR20T5WithoutHandoffkeepEnvKeepsPreR20Behavior(t *testing.T) {
	fake, _, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub, agent := r20t5Hub(t, r20t5OneLane, nil, 8)

	hub.relayJobCompletion(r20t5Event("r20t5-compat", ""))
	hub.relayJobCompletion(r20t5Event("r20t5-compat", ""))
	hub.replayUndeliveredRelayEvents(context.Background())

	if injected := drainRelays(agent); injected != 1 {
		t.Fatalf("injections=%d, want 1: a hub without handoffkeep still dedupes", injected)
	}
	if acknowledgements := drainPersisted(agent); len(acknowledgements) != 0 {
		t.Fatalf("a hub without handoffkeep sent relay.persisted: %+v", acknowledgements)
	}
	if calls := fake.sequence(); len(calls) != 0 {
		t.Fatalf("handoffkeep was called without the flag: %v", calls)
	}
}

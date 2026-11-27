package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

// hk 6887: the uncertain-batch warning path, driven through the node's
// delivery entry (relayBusyManager.deliver) and the hub's agent-message entry
// (handleAgentMessage). Each member is one held row; echoed names the members
// whose nonce the pane shows after the send, the rest stay unproven.

// t6887Items builds one held row per lane, with consecutive event ids from 801.
func t6887Items(lanes ...string) []relayHeld {
	items := make([]relayHeld, 0, len(lanes))
	for i, lane := range lanes {
		id := int64(801 + i)
		items = append(items, relayHeld{Pane: "w1:p1", Lane: lane, EventID: id, JobID: fmt.Sprintf("relay-job-%d", id), Text: fmt.Sprintf("member %d", i+1), HeldSince: time.Now(), DeliverPolicy: "idle"})
	}
	return items
}

// t6887Deliver runs one batch through the node. queued makes the post-send
// screen show the queued banner with the echoed members' text (queued_banner);
// otherwise the echoed text is in the transcript. A node that advertises the
// uncertain-batch summary is one whose hub connection accepts it.
func t6887Deliver(t *testing.T, items []relayHeld, echoed []int, queued, accepts bool) []hubClientEvent {
	t.Helper()
	var shown []relayHeld
	for _, index := range echoed {
		shown = append(shown, items[index])
	}
	text, _ := task687Batch(shown...)
	var post, postTranscript string
	if queued {
		post = task626Claude([]string{"❯ " + text, "Press up to edit queued messages", ""}, "❯")
		postTranscript = task626TranscriptOnly
	} else {
		lines := append(append([]string{}, task626Transcript...), text)
		post = task626Claude(lines, "❯")
		postTranscript = strings.Join(lines, "\n")
	}
	_, _ = task683FakeHerdr(t, "claude",
		[]string{task626ClaudeEmpty, post},
		[]string{task626TranscriptOnly, postTranscript})
	events := make(chan hubClientEvent, 64)
	client := &HubClient{}
	client.setRelayEmitter(func(event hubClientEvent) { events <- event })
	if accepts {
		peer := &hubClientConnection{}
		peer.acceptsRelayUncertainBatch.Store(true)
		client.signalsPeer.Store(peer)
	}
	client.relayBusyManager().deliver(context.Background(), items, false)
	var out []hubClientEvent
	for {
		select {
		case event := <-events:
			out = append(out, event)
		default:
			return out
		}
	}
}

// t6887Hub is the hub the node reports to: one worker lane under a design
// lane, and a connected node record for host-a.
func t6887Hub(t *testing.T) (*HubServer, *[]fleetEvent, *spawnSentryTransport, *hubAgent) {
	t.Helper()
	hub, events, transport := acceptObserveHub(t)
	agent := &hubAgent{}
	hub.mu.Lock()
	hub.nodes["host-a"] = &hubNodeRecord{agent: agent}
	hub.mu.Unlock()
	return hub, events, transport, agent
}

// t6887Feed sends one node event to the hub through its agent-message entry.
func t6887Feed(t *testing.T, hub *HubServer, agent *hubAgent, event hubClientEvent) {
	t.Helper()
	wire, err := json.Marshal(struct {
		Type    string          `json:"type"`
		Kind    string          `json:"kind"`
		Payload json.RawMessage `json:"payload"`
	}{Type: "event", Kind: event.Kind, Payload: event.Payload})
	if err != nil {
		t.Fatal(err)
	}
	if _, valid := parseHubInbound(wire); !valid {
		t.Fatalf("hub rejected %s: %s", event.Kind, event.Payload)
	}
	hub.handleAgentMessage("host-a", "fixture", agent, wire)
}

func t6887Kind(events []hubClientEvent, kind string) []hubClientEvent {
	var out []hubClientEvent
	for _, event := range events {
		if event.Kind == kind {
			out = append(out, event)
		}
	}
	return out
}

func t6887Dropped(t *testing.T, event hubClientEvent) relayDroppedPayload {
	t.Helper()
	var dropped relayDroppedPayload
	if err := json.Unmarshal(event.Payload, &dropped); err != nil {
		t.Fatal(err)
	}
	return dropped
}

// Item 1: a batch whose first row has no lane still sends a valid summary
// naming the worker lanes, and the hub raises exactly one warning for it.
func TestT6887UncertainBatchWithLaneLessFirstRowWarnsOnce(t *testing.T) {
	// Member 1 has no lane and is unproven with member 2, a worker row: the
	// uncertain set starts with the lane-less row, as group[0] did on the base.
	items := t6887Items("", "worker-a", "worker-a")
	events := t6887Deliver(t, items, []int{2}, false, true)

	summaries := t6887Kind(events, "relay.uncertain_batch")
	if len(summaries) != 1 {
		t.Fatalf("summaries=%d, want one: %+v", len(summaries), events)
	}
	batch, valid := decodeRelayUncertainBatchPayload(summaries[0].Payload)
	if !valid {
		t.Fatalf("hub decoder rejects the summary: %s", summaries[0].Payload)
	}
	if batch.Lane != "worker-a" || fmt.Sprint(batch.Lanes) != "[worker-a]" || batch.MemberCount != 2 {
		t.Fatalf("summary=%+v, want lane worker-a, lanes [worker-a], 2 members", batch)
	}
	for _, event := range t6887Kind(events, "relay.dropped") {
		if !t6887Dropped(t, event).BatchUncertain {
			t.Fatalf("row %s kept its own warning beside the summary", event.Payload)
		}
	}

	hub, _, transport, agent := t6887Hub(t)
	for _, event := range events {
		t6887Feed(t, hub, agent, event)
	}
	if len(transport.events) != 1 {
		t.Fatalf("Sentry warnings=%d, want one batch warning", len(transport.events))
	}
}

// Item 3 (gate): a hub that did not advertise relay_uncertain_batch gets no
// summary, and each row keeps its own warning, as a base hub did before the
// aggregate. The rows reach the hub as plain relay.dropped events.
func TestT6887UncertainBatchWithoutHubCapabilityKeepsPerRowWarnings(t *testing.T) {
	items := t6887Items("worker-a", "worker-a", "worker-a")
	events := t6887Deliver(t, items, []int{0}, false, false)

	if summaries := t6887Kind(events, "relay.uncertain_batch"); len(summaries) != 0 {
		t.Fatalf("hub without relay_uncertain_batch got a summary: %s", summaries[0].Payload)
	}
	dropped := t6887Kind(events, "relay.dropped")
	if len(dropped) != 2 {
		t.Fatalf("dropped rows=%d, want the two unproven members", len(dropped))
	}
	for _, event := range dropped {
		if t6887Dropped(t, event).BatchUncertain {
			t.Fatalf("row %s marked batch_uncertain with no summary to cover it", event.Payload)
		}
	}

	hub, _, transport, agent := t6887Hub(t)
	for _, event := range events {
		t6887Feed(t, hub, agent, event)
	}
	if len(transport.events) != 2 {
		t.Fatalf("Sentry warnings=%d, want one per dropped row", len(transport.events))
	}
}

// Item 3 (capability): the hub's feature frame names the summary, so a
// current hub gets it.
func TestT6887HubAdvertisesUncertainBatchFeature(t *testing.T) {
	if !slices.Contains(hubFeaturesFrame().Features, "relay_uncertain_batch") {
		t.Fatalf("hub.features=%v does not advertise relay_uncertain_batch", hubFeaturesFrame().Features)
	}
}

// Item 2: a queued_banner batch is uncertain only for the members whose nonce
// is missing. The mixed-lane uncertain set (design and worker) still gets one
// summary, which the hub counts once because it names a worker lane. A queued
// member whose nonce is proven is delivered and adds no warning.
func TestT6887QueuedBannerMixedLaneUncertainWarnsOnce(t *testing.T) {
	items := t6887Items("worker-a", "design-a", "worker-a")
	events := t6887Deliver(t, items, []int{0}, true, true)

	if delivered := t6887Kind(events, "relay.delivered"); len(delivered) != 1 || !strings.Contains(string(delivered[0].Payload), "queued") {
		t.Fatalf("delivered=%+v, want only the queued proven member", delivered)
	}
	summaries := t6887Kind(events, "relay.uncertain_batch")
	if len(summaries) != 1 {
		t.Fatalf("summaries=%d, want one for the two unproven members", len(summaries))
	}
	hub, _, transport, agent := t6887Hub(t)
	for _, event := range events {
		t6887Feed(t, hub, agent, event)
	}
	if len(transport.events) != 1 {
		t.Fatalf("Sentry warnings=%d, want one", len(transport.events))
	}
}

// Item 4: a real six-row batch through the node's delivery entry and the
// hub's agent-message entry. Four members show their nonce and are delivered;
// two stay unproven. The hub ends with each row's outcome, no held projection
// left behind, and one batch warning.
func TestT6887SixRowBatchThroughNodeAndHub(t *testing.T) {
	items := t6887Items("worker-a", "worker-a", "worker-a", "worker-a", "worker-a", "worker-a")
	events := t6887Deliver(t, items, []int{0, 1, 2, 3}, false, true)

	hub, fleet, transport, agent := t6887Hub(t)
	hub.mu.Lock()
	for _, item := range items {
		key := relayPendingKey(item.EventID, item.JobID)
		hub.relayHeld[item.EventID] = hubRelayHeldProjection{ID: item.EventID, Lane: item.Lane, Pane: item.Pane, Machine: "host-a", JobID: item.JobID}
		hub.relayControl.relayPending[key] = relayPending{machine: "host-a", pane: item.Pane, eventID: item.EventID, kind: "lane.event", held: true, event: hubJobEventPayload{JobID: item.JobID, OwnerLane: item.Lane}}
	}
	hub.mu.Unlock()
	for _, event := range events {
		t6887Feed(t, hub, agent, event)
	}

	outcomes := map[string][]string{}
	for _, event := range *fleet {
		outcomes[event.Job] = append(outcomes[event.Job], event.Outcome)
	}
	for i, item := range items {
		got := strings.Join(outcomes[item.JobID], ",")
		if i < 4 {
			if got != "delivered" {
				t.Fatalf("row %d outcomes=%q, want delivered", item.EventID, got)
			}
			continue
		}
		if !strings.Contains(got, "unconfirmed:") || !strings.Contains(got, "dropped:"+relayDroppedCheckPane) {
			t.Fatalf("row %d outcomes=%q, want unconfirmed and dropped", item.EventID, got)
		}
	}
	hub.mu.Lock()
	heldLeft := len(hub.relayHeld)
	hub.mu.Unlock()
	if heldLeft != 0 {
		t.Fatalf("held projection left %d rows, want none", heldLeft)
	}
	if len(transport.events) != 1 {
		t.Fatalf("Sentry warnings=%d, want one batch warning", len(transport.events))
	}
}

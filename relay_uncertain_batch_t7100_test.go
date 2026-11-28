package panewire

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// hk 7100: the no-lane uncertain batch warns once, and the capability wait for
// the hub's feature frame ends when the connection closes or the context is done.

// A batch whose members name no lane still sends one summary, and the hub
// raises exactly one warning for it. Before hk 7100 the node sent nothing and
// each lane-less row was dropped silently: the hub refuses a row with no lane.
func TestT7100NoLaneUncertainBatchWarnsOnce(t *testing.T) {
	items := t6887Items("", "")
	events := t6887Deliver(t, items, nil, false, true)

	summaries := t6887Kind(events, "relay.uncertain_batch")
	if len(summaries) != 1 {
		t.Fatalf("summaries=%d, want one for the no-lane batch: %+v", len(summaries), events)
	}
	batch, valid := decodeRelayUncertainBatchPayload(summaries[0].Payload)
	if !valid {
		t.Fatalf("hub decoder rejects the no-lane summary: %s", summaries[0].Payload)
	}
	if batch.Lane != "" || len(batch.Lanes) != 0 || batch.MemberCount != 2 {
		t.Fatalf("summary=%+v, want empty lane, no lanes, 2 members", batch)
	}
	if dropped := t6887Kind(events, "relay.dropped"); len(dropped) != 0 {
		t.Fatalf("lane-less rows sent relay.dropped the hub would refuse: %s", dropped[0].Payload)
	}

	hub, _, transport, agent := t6887Hub(t)
	for _, event := range events {
		t6887Feed(t, hub, agent, event)
	}
	if len(transport.events) != 1 {
		t.Fatalf("Sentry warnings=%d, want exactly one for the no-lane batch", len(transport.events))
	}
}

// When the connection closes, the decision waits for its replacement's features
// instead of deciding on the dead connection. The replacement binds its emitter
// before its featuresReady closes, so the decision returns at once once it
// arrives, not after the full hubFeaturesWait.
func TestT7100CapabilityWaitDecidesOnReplacementConnection(t *testing.T) {
	dead := &hubClientConnection{featuresReady: make(chan struct{}), closed: make(chan struct{})}
	close(dead.closed)
	client := &HubClient{}
	client.signalsPeer.Store(dead)
	// Simulated redial latency: the replacement connection takes over mid-wait.
	go func() {
		time.Sleep(50 * time.Millisecond)
		live := &hubClientConnection{featuresReady: make(chan struct{}), closed: make(chan struct{})}
		live.acceptsRelayUncertainBatch.Store(true)
		client.signalsPeer.Store(live)
		live.markFeaturesReady()
	}()

	start := time.Now()
	if !client.hubAcceptsRelayUncertainBatch(context.Background()) {
		t.Fatal("the replacement connection accepts the summary, but the decision did not use it")
	}
	if elapsed := time.Since(start); elapsed >= hubFeaturesWait/2 {
		t.Fatalf("decision took %s, want it to follow the replacement at once", elapsed)
	}
}

// The uncertain batch's reports reach the replacement connection, not the dead
// one: a closed connection's emitter would send them into a socket that is gone,
// and the hub would never learn the rows were dropped.
func TestT7100RowReportsReachReplacementConnection(t *testing.T) {
	items := t6887Items("worker-a", "worker-a", "worker-a")
	dead := &hubClientConnection{featuresReady: make(chan struct{}), closed: make(chan struct{})}
	close(dead.closed)
	deadEvents := make(chan hubClientEvent, 64)
	liveEvents := make(chan hubClientEvent, 64)
	client := &HubClient{}
	client.setRelayEmitter(func(event hubClientEvent) { deadEvents <- event })
	client.signalsPeer.Store(dead)
	// Simulated redial latency. serveConnection stores the replacement, binds
	// its emitter, then starts its read loop, which marks its features ready.
	go func() {
		time.Sleep(50 * time.Millisecond)
		live := &hubClientConnection{featuresReady: make(chan struct{}), closed: make(chan struct{})}
		live.acceptsRelayUncertainBatch.Store(true)
		client.signalsPeer.Store(live)
		client.setRelayEmitter(func(event hubClientEvent) { liveEvents <- event })
		live.markFeaturesReady()
	}()

	shown := items[:1]
	text, _ := task687Batch(shown...)
	lines := append(append([]string{}, task626Transcript...), text)
	_, _ = task683FakeHerdr(t, "claude",
		[]string{task626ClaudeEmpty, task626Claude(lines, "❯")},
		[]string{task626TranscriptOnly, strings.Join(lines, "\n")})
	client.relayBusyManager().deliver(context.Background(), items, false)

	drain := func(ch chan hubClientEvent) []hubClientEvent {
		var out []hubClientEvent
		for {
			select {
			case event := <-ch:
				out = append(out, event)
			default:
				return out
			}
		}
	}
	if stale := drain(deadEvents); len(stale) != 0 {
		t.Fatalf("the dead connection's emitter got %d reports, want none: %s", len(stale), stale[0].Payload)
	}
	live := drain(liveEvents)
	if summaries := t6887Kind(live, "relay.uncertain_batch"); len(summaries) != 1 {
		t.Fatalf("replacement got summaries=%d, want one", len(summaries))
	}
	dropped := t6887Kind(live, "relay.dropped")
	if len(dropped) != 2 {
		t.Fatalf("replacement got dropped=%d, want the two unproven rows", len(dropped))
	}
}

// The capability wait also returns as soon as the context is done.
func TestT7100CapabilityWaitEndsWhenContextIsDone(t *testing.T) {
	peer := &hubClientConnection{featuresReady: make(chan struct{})}
	client := &HubClient{}
	client.signalsPeer.Store(peer)
	ctx, cancel := context.WithCancel(context.Background())
	// Simulated latency: the caller gives up while the batch is waiting.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	if client.hubAcceptsRelayUncertainBatch(ctx) {
		t.Fatal("no feature frame arrived, so the summary must not be accepted")
	}
	if elapsed := time.Since(start); elapsed >= hubFeaturesWait/2 {
		t.Fatalf("wait lasted %s after the context was done, want it to end at once", elapsed)
	}
}

// The decoder keeps refusing a summary whose lane and lanes disagree, now that
// an empty lane is allowed.
func TestT7100UncertainBatchDecoderRejectsLaneWithoutLanes(t *testing.T) {
	raw, err := json.Marshal(relayUncertainBatchPayload{JobID: "relay-job-1", Pane: "w1:p1", Lane: "worker-a", MemberCount: 2, Verdict: "maybe_in_pane", Evidence: "e", ReplaySource: "hub_unconfirmed_once"})
	if err != nil {
		t.Fatal(err)
	}
	if _, valid := decodeRelayUncertainBatchPayload(raw); valid {
		t.Fatalf("decoder accepted a named lane with no lanes: %s", raw)
	}
}

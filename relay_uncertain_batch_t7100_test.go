package panewire

import (
	"context"
	"encoding/json"
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

// The capability wait returns as soon as the connection closes, instead of
// holding the batch for the full hubFeaturesWait while no hub can answer.
func TestT7100CapabilityWaitEndsWhenConnectionCloses(t *testing.T) {
	peer := &hubClientConnection{featuresReady: make(chan struct{}), closed: make(chan struct{})}
	client := &HubClient{}
	client.signalsPeer.Store(peer)
	// Simulated latency: the connection closes while the batch is waiting.
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(peer.closed)
	}()

	start := time.Now()
	accepts := client.hubAcceptsRelayUncertainBatch(context.Background())
	if elapsed := time.Since(start); elapsed >= hubFeaturesWait/2 {
		t.Fatalf("wait lasted %s after the connection closed, want it to end at once", elapsed)
	}
	if accepts {
		t.Fatal("a closed connection with no feature frame must not accept the summary")
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

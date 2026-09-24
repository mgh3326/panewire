package panewire

// #658: after #650 the hub logged 'relay replay retired' for the same rows on
// every node hello — 1,108 lines for 476 unique rows. Each hello runs
// replayUndeliveredLaneEvents (hub.go connect), and a row whose retire marker
// (delivered_to=hub/replay-retired:*) was already persisted but that still
// arrived in the undelivered listing was retired, logged and broadcast again.
// The retire path now skips a row already carrying the marker, so one row is
// retired and announced exactly once.

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestT658ReplayRetiredLogsOncePerRow(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	fake.retiredMarkerStaysUndelivered = true
	var logs bytes.Buffer
	hub := r20Hub(t, t650Lanes(t650Workers(1)), client, &logs)
	events := r20t5Subscribe(t, hub)
	stale := time.Now().UTC().Add(-(relayReplayMaxAge + time.Hour)).Format(time.RFC3339Nano)
	rows := []int64{41, 42}
	for _, id := range rows {
		eventID := fmt.Sprintf("t658-note-%d", id)
		fake.seedUndelivered(handoffkeepRelayEvent{ID: id, Kind: "lane.event", JobID: laneEventTransportID(t650Director, eventID), Epoch: 1, OwnerLane: t650Director, EventID: eventID, Text: "[consult-done] advice ready", ReceivedAt: stale})
	}

	// connect() schedules this replay on every node hello (hub.go).
	hub.replayUndeliveredLaneEvents(context.Background())
	hub.replayUndeliveredLaneEvents(context.Background())

	for _, id := range rows {
		if got := fake.deliveredToFor(id); got != "hub/replay-retired:stale" {
			t.Fatalf("row %d delivered_to=%q, want hub/replay-retired:stale", id, got)
		}
		if got := fake.count(http.MethodPost, fmt.Sprintf("/v1/relay/events/%d/delivered", id)); got != 1 {
			t.Fatalf("row %d retire POSTs=%d after two hellos, want 1", id, got)
		}
	}
	if got := strings.Count(logs.String(), `msg="relay replay retired"`); got != len(rows) {
		t.Fatalf("'relay replay retired' info logs=%d after two node hellos, want %d (one per row)", got, len(rows))
	}
	if got := len(events("relay.replay_retired")); got != len(rows) {
		t.Fatalf("relay.replay_retired broadcasts=%d after two node hellos, want %d", got, len(rows))
	}
}

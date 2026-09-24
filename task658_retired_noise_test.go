package panewire

// #658: after #650 the hub logged 'relay replay retired' for the same rows on
// every node hello — 1,108 lines for 476 unique rows. The startup replay and
// each hello's replay run concurrently and can list the same rows before any
// retire lands, and the retire marker write is idempotent server-side, so
// every overlapping replay announced the same retire. The retire path now
// claims the row under h.mu before marking and keeps the claim after success,
// so one row is retired and announced exactly once; a delivered_to marker
// check covers any listing that still returns a marked row.

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// t658LockedBuffer is a goroutine-safe sink for the hub logger: overlapping
// replays write log lines from concurrent goroutines.
type t658LockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *t658LockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *t658LockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

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

// The production mechanism: the startup replay (hub_cli.go RunMaintenance,
// all kinds) and the replay each node hello spawns (hub.go connect) run
// concurrently, so two replays can list the same rows before either retire
// lands. markDelivered is idempotent server-side, so without a hub-side claim
// every overlapping replay marked, logged and broadcast the same retire.
func TestT658ConcurrentReplaysRetireOncePerRow(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	var logs t658LockedBuffer
	hub := r20Hub(t, t650Lanes(t650Workers(1)), client, &logs)
	events := r20t5Subscribe(t, hub)
	stale := time.Now().UTC().Add(-(relayReplayMaxAge + time.Hour)).Format(time.RFC3339Nano)
	rows := []int64{41, 42}
	for _, id := range rows {
		eventID := fmt.Sprintf("t658-note-%d", id)
		fake.seedUndelivered(handoffkeepRelayEvent{ID: id, Kind: "lane.event", JobID: laneEventTransportID(t650Director, eventID), Epoch: 1, OwnerLane: t650Director, EventID: eventID, Text: "[consult-done] advice ready", ReceivedAt: stale})
	}

	// Hold every undelivered listing until a second one has arrived, so both
	// replays see the rows while they are still undelivered — no retire write
	// can run before a listing returns, so both replays race to retire.
	arrived := make(chan struct{}, 8)
	release := make(chan struct{})
	var once sync.Once
	fake.observe = func(method, path string) {
		if method == http.MethodGet && path == "/v1/relay/events" {
			arrived <- struct{}{}
			if len(arrived) == 2 {
				once.Do(func() { close(release) })
			}
			<-release
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hub.replayUndeliveredLaneEvents(context.Background())
		}()
	}
	wg.Wait()

	for _, id := range rows {
		if got := fake.deliveredToFor(id); got != "hub/replay-retired:stale" {
			t.Fatalf("row %d delivered_to=%q, want hub/replay-retired:stale", id, got)
		}
		if got := fake.count(http.MethodPost, fmt.Sprintf("/v1/relay/events/%d/delivered", id)); got != 1 {
			t.Fatalf("row %d retire POSTs=%d after two overlapping replays, want 1", id, got)
		}
	}
	if got := strings.Count(logs.String(), `msg="relay replay retired"`); got != len(rows) {
		t.Fatalf("'relay replay retired' info logs=%d after two overlapping replays, want %d (one per row)", got, len(rows))
	}
	if got := len(events("relay.replay_retired")); got != len(rows) {
		t.Fatalf("relay.replay_retired broadcasts=%d after two overlapping replays, want %d", got, len(rows))
	}
}

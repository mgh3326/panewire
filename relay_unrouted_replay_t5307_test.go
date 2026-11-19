package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// hk 5307: a lane.event row whose lane has no route stays undelivered for the
// replay window (relayReplayMaxAge). Every node hello and applied-report
// replay walks the undelivered rows again, so before this change each walk
// announced the same unroutable row again: a relay.unrouted broadcast and a
// "relay unrouted" WARN, about every two minutes in production. The row must
// be announced once per hub process, stay open so a route that returns still
// delivers it, and be retired when its window ends.

const t5307UnroutedWarn = "relay unrouted: no deliverable route for lane"

func TestT5307UnroutedReplayAnnouncesOnceUntilWindowEnds(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	var logs bytes.Buffer
	// The lane file names no lane, so director-1 has no route at all.
	hub := r20Hub(t, `{"lanes":{}}`, client, &logs)
	base := time.Now().UTC()
	clock := base
	hub.now = func() time.Time { return clock }
	received := base.Add(-time.Hour).Format(time.RFC3339Nano)
	fake.seedUndelivered(t961LaneRow(51, "director-1", "t5307-unrouted", received, 0))
	events := r20t5Subscribe(t, hub)

	// Six node-hello walks, two minutes apart: the production cadence.
	for walk := 0; walk < 6; walk++ {
		clock = base.Add(time.Duration(walk) * 2 * time.Minute)
		hub.replayUndeliveredLaneEvents(context.Background())
	}
	if got := strings.Count(logs.String(), t5307UnroutedWarn); got != 1 {
		t.Fatalf("unrouted WARN lines over six walks=%d, want 1\n%s", got, logs.String())
	}
	if got := t5307UnroutedBroadcasts(events); got != 1 {
		t.Fatalf("relay.unrouted broadcasts over six walks=%d, want 1", got)
	}
	fake.mu.Lock()
	closed := len(fake.deliveredTo)
	fake.mu.Unlock()
	if closed != 0 {
		t.Fatalf("unrouted row was closed inside its window (%d closed); it must stay open for a route that returns", closed)
	}

	// Past the window (received + relayReplayMaxAge) the walk retires the row
	// and announces nothing more about it.
	clock = base.Add(relayReplayMaxAge)
	hub.replayUndeliveredLaneEvents(context.Background())
	if got := strings.Count(logs.String(), t5307UnroutedWarn); got != 1 {
		t.Fatalf("unrouted WARN lines after the window=%d, want still 1", got)
	}
	if got := t5307UnroutedBroadcasts(events); got != 1 {
		t.Fatalf("relay.unrouted broadcasts after the window=%d, want still 1", got)
	}
	if got := t5307Retired(events); got != 1 {
		t.Fatalf("relay.replay_retired broadcasts after the window=%d, want 1", got)
	}
}

func t5307UnroutedBroadcasts(events func(string) []json.RawMessage) int {
	return len(events("relay.unrouted"))
}

func t5307Retired(events func(string) []json.RawMessage) int {
	return len(events("relay.replay_retired"))
}

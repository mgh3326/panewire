package panewire

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// #998: follow-ups to the #987 lane-stall work — held_count intersects the
// held projection with the rows this observation actually counted (so it can
// never exceed undelivered_count) and is computed only when an observation
// activates an episode.

// AC2: relayHeld still holding a retired and an exhausted row for the lane
// inflates nothing — only the held row the observation counted is annotated —
// and a non-activating observation never pays for the scan at all.
func TestT998LaneStallHeldCountCountsOnlyCountedRows(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	var logBuf bytes.Buffer
	hub := t987StallHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, &task198Notifier{}, &logBuf)
	var scans atomic.Int64
	hub.laneHeldCountProbe = func() { scans.Add(1) }
	t961ActiveNode(hub, "host-a", HubActiveJob{JobID: "t998-held", OwnerLane: "lane-a"})
	old := time.Now().UTC().Add(-(relayLaneStallAge + time.Minute)).Format(time.RFC3339Nano)
	// Row 51 is retired and row 52 out of attempts: both stay on the
	// undelivered listing and in relayHeld, but neither counts. Row 53 is the
	// one real held undelivered row.
	retired := t961LaneRow(51, "lane-a", "t998-retired", old, 1)
	retired.DeliveredTo = relayReplayRetiredMarker + "stale"
	fake.seedUndelivered(
		retired,
		t961LaneRow(52, "lane-a", "t998-exhausted", old, relayReplayMaxAttempts),
		t961LaneRow(53, "lane-a", "t998-held", old, 1),
	)
	hub.mu.Lock()
	for _, id := range []int64{51, 52, 53} {
		hub.relayHeld[id] = hubRelayHeldProjection{ID: id, Lane: "lane-a", Pane: "w1:p1", Machine: "host-a", HeldSince: old, DeliverPolicy: "idle", JobID: "t998-held"}
	}
	hub.mu.Unlock()
	events := r20t5Subscribe(t, hub)

	t961Sweep(hub) // first dampened observation: no activation, no scan
	if got := scans.Load(); got != 0 {
		t.Fatalf("held_count was computed on a non-activating observation: %d scans", got)
	}
	t961Sweep(hub) // second: the episode activates
	if got := scans.Load(); got != 1 {
		t.Fatalf("laneHeldCountLocked scans=%d, want 1 (the activation only)", got)
	}
	stalled := t961StallBroadcasts(events)
	if len(stalled) != 1 {
		t.Fatalf("relay.lane_stalled broadcasts=%d, want 1", len(stalled))
	}
	payload := stalled[0]
	if payload["undelivered_count"] != float64(1) || payload["held_count"] != float64(1) {
		t.Fatalf("relay.lane_stalled payload=%v, want undelivered_count=1 held_count=1", payload)
	}
	line := ""
	for _, candidate := range strings.Split(strings.TrimRight(logBuf.String(), "\n"), "\n") {
		if strings.Contains(candidate, "relay lane stalled") {
			line = candidate
		}
	}
	if line == "" || !strings.Contains(line, "held_count=1") || !strings.Contains(line, "undelivered_count=1") {
		t.Fatalf("the Warn line lacks held_count=1:\n%s", logBuf.String())
	}
	t961Sweep(hub) // a steady-state observation must not rescan either
	if got := scans.Load(); got != 1 {
		t.Fatalf("an already-active observation rescanned held rows: %d scans", got)
	}
}

func TestT998CloseIsConcurrentAndIdempotent(t *testing.T) {
	const iterations = 500
	dir := t.TempDir()
	for i := 0; i < iterations; i++ {
		hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "op"}})
		if err != nil {
			t.Fatal(err)
		}
		logFile, err := os.OpenFile(filepath.Join(dir, "hub.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND|os.O_TRUNC, 0o640)
		if err != nil {
			t.Fatal(err)
		}
		hub.logFile = logFile
		start := make(chan struct{})
		errs := make([]error, 2)
		var wg sync.WaitGroup
		for g := 0; g < 2; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				<-start
				errs[g] = hub.Close()
			}(g)
		}
		close(start)
		wg.Wait()
		if errs[0] != nil || errs[1] != nil {
			t.Fatalf("iteration %d: concurrent Close errs=%v,%v", i, errs[0], errs[1])
		}
		if err := hub.Close(); err != nil {
			t.Fatalf("iteration %d: third Close=%v", i, err)
		}
		if _, err := logFile.Write([]byte("x")); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("iteration %d: the log file survived Close: %v", i, err)
		}
	}
}

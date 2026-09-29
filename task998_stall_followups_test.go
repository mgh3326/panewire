package panewire

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
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
// activates an episode; Close bars and waits update-overdue flushes exactly
// like it already did lane-stall runs, and is safe for concurrent and
// repeated callers.

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

// t998GatedFlushHK parks sink-bound relay event POSTs while the gate is set —
// until release closes or the request context ends — so a test can hold an
// update-overdue flush genuinely in flight. A released or ungated POST is
// answered 503 so the notice stays queued and the failure is logged, and any
// POST arriving after the closed flag is a launch that outlived Close.
type t998GatedFlushHK struct {
	fake    *fakeHandoffkeep
	gate    atomic.Bool
	release chan struct{}
	started chan struct{}
	once    sync.Once
	closed  *atomic.Bool
	late    *atomic.Int64
	total   *atomic.Int64
}

func (g *t998GatedFlushHK) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost && r.URL.Path == "/v1/relay/events" {
		g.total.Add(1)
		if g.closed.Load() {
			g.late.Add(1)
		}
		if g.gate.Load() {
			g.once.Do(func() { close(g.started) })
			select {
			case <-g.release:
			case <-r.Context().Done():
				return
			}
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"error":"t998 gated"}`)
			return
		}
	}
	g.fake.ServeHTTP(w, r)
}

// AC3: a stream of startUpdateOverdueFlush calls racing Close under -race.
// Close cancels the in-flight flush's handoffkeep write instead of waiting
// the gate out, so it returns promptly; the flush goroutine is still drained
// before the log file closes, no flush starts once Close has begun (counted
// as POSTs reaching the fake after the log file closed), and no record is
// written to the closed file.
func TestT998UpdateOverdueFlushRacesClose(t *testing.T) {
	fake := &fakeHandoffkeep{nextID: 100, status: http.StatusCreated, rows: map[string]*handoffkeepRelayEvent{}}
	var logFileClosed atomic.Bool
	var latePosts, totalPosts atomic.Int64
	gated := &t998GatedFlushHK{fake: fake, release: make(chan struct{}), started: make(chan struct{}), closed: &logFileClosed, late: &latePosts, total: &totalPosts}
	server := httptest.NewServer(gated)
	t.Cleanup(server.Close)
	// A RED run must fail fast, not hang in server.Close on the parked
	// handler: registered after the server, this cleanup runs first and
	// releases the gate on any failure path (#998 tester NICE-2).
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(gated.release) }) })
	client, err := newHandoffkeepRelayClient(hubHandoffkeepEnv{URL: server.URL, Token: "test-token"}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	logFile, err := os.OpenFile(filepath.Join(t.TempDir(), "hub.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	var totalRecords, lateRecords atomic.Int64
	handler := &t987CloseCountingHandler{inner: slog.NewTextHandler(logFile, nil), total: &totalRecords, late: &lateRecords}
	hub := t987StallHub(t, `{"lanes":{"offload":{"sink":true}}}`, client, &task198Notifier{}, io.Discard)
	hub.mu.Lock()
	hub.logger = slog.New(handler)
	hub.updateOverduePending["node-a\x00v998.0"] = &hubUpdateOverdue{machine: "node-a", version: "v998.0", deadline: time.Now().UTC()}
	hub.mu.Unlock()
	hub.logFile = logFile

	// Gate the POSTs: the next flush parks inside its sink write.
	gated.gate.Store(true)
	hub.startUpdateOverdueFlush()
	select {
	case <-gated.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the overdue flush never reached handoffkeep")
	}

	closed := make(chan error, 1)
	go func() { closed <- hub.Close() }()
	const iterations = 200
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations/4; i++ {
				hub.startUpdateOverdueFlush()
			}
		}()
	}
	// The parked write is cancelled by Close, so Close returns promptly even
	// though the gate is never released inside the test; the bound is what a
	// regression (a flush deaf to the close signal) would blow through.
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close=%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return once the in-flight flush was cancelled")
	}
	releaseOnce.Do(func() { close(gated.release) })
	logFileClosed.Store(true)
	wg.Wait()

	// The flag committed before Close returned: the notice is still queued
	// (its POST failed), so only the closing flag bars a later launch.
	if !hub.updateOverdueFlushMu.TryLock() {
		t.Fatal("updateOverdueFlushMu was never freed after Close")
	}
	closing := hub.updateOverdueFlushClosing
	hub.updateOverdueFlushMu.Unlock()
	if !closing {
		t.Fatal("Close returned without committing updateOverdueFlushClosing")
	}
	for i := 0; i < 3; i++ {
		hub.startUpdateOverdueFlush()
	}
	hub.Sweep()
	hub.updateOverdueFlushes.Wait()
	if got := latePosts.Load(); got != 0 {
		t.Fatalf("update-overdue flushes reached handoffkeep after the log file closed: %d posts", got)
	}
	if got := lateRecords.Load(); got != 0 {
		t.Fatalf("log records written after the log file closed: %d", got)
	}
	if totalPosts.Load() == 0 || totalRecords.Load() == 0 {
		t.Fatalf("the race never exercised the flush (posts=%d records=%d)", totalPosts.Load(), totalRecords.Load())
	}
}

// AC4: two goroutines closing the same hub at once — the file is closed
// exactly once (a double close is itself an error), both calls return nil,
// and a third sequential Close is a cheap no-op.
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

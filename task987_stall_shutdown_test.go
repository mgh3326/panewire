package panewire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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

// #987: the lane-stall run's shutdown contract. Once Close has begun no run
// may start and an in-flight one is waited out; a cancelled maintenance
// context returns a run promptly and neutrally; fully inactive lane-stall
// alert entries are pruned so ephemeral lanes stop being candidates; and the
// alarm annotates how many of the lane's undelivered rows are held for a busy
// pane instead of hiding that inside undelivered_count.

// t987StallHub mirrors t961StallHub with a caller-supplied log destination.
func t987StallHub(t *testing.T, lanes string, client *handoffkeepRelayClient, notifier HubNotifier, logs io.Writer) *HubServer {
	t.Helper()
	hub, err := NewHubServer(HubServerConfig{
		Tokens:          map[string]string{"operator": "op", "host-a": "node-a", "host-b": "node-b"},
		ReportRelayPath: r20LanesFile(t, lanes),
		Notifier:        notifier,
		Logger:          slog.New(slog.NewTextHandler(logs, nil)),
		handoffkeep:     client,
	})
	if err != nil {
		t.Fatal(err)
	}
	return hub
}

func t987LaneStallEntry(hub *HubServer, lane string) *hubAlertState {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	return hub.alerts["lane-stall:"+lane]
}

func t987LaneStallEntries(hub *HubServer) []string {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	var out []string
	for key := range hub.alerts {
		if strings.HasPrefix(key, "lane-stall:") {
			out = append(out, key)
		}
	}
	return out
}

// t987GatedHK parks GETs while the gate is set — until release closes or the
// request's own context ends — so a test can hold a stall run genuinely in
// flight. Reads arriving after the closed flag is set are launches that
// outlived Close.
type t987GatedHK struct {
	fake    *fakeHandoffkeep
	gate    atomic.Bool
	release chan struct{}
	started chan struct{}
	once    sync.Once
	closed  *atomic.Bool
	late    *atomic.Int64
	total   *atomic.Int64
}

func (g *t987GatedHK) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
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
		}
	}
	g.fake.ServeHTTP(w, r)
}

// t987CloseCountingHandler is the recording handler for the Close race test:
// every record is written to the real log file, and one landing after the
// file was closed surfaces as os.ErrClosed from the inner handler.
type t987CloseCountingHandler struct {
	inner slog.Handler
	total *atomic.Int64
	late  *atomic.Int64
}

func (h *t987CloseCountingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *t987CloseCountingHandler) Handle(ctx context.Context, record slog.Record) error {
	h.total.Add(1)
	if err := h.inner.Handle(ctx, record); errors.Is(err, os.ErrClosed) {
		h.late.Add(1)
	}
	return nil
}

func (h *t987CloseCountingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &t987CloseCountingHandler{inner: h.inner.WithAttrs(attrs), total: h.total, late: h.late}
}

func (h *t987CloseCountingHandler) WithGroup(name string) slog.Handler {
	return &t987CloseCountingHandler{inner: h.inner.WithGroup(name), total: h.total, late: h.late}
}

// AC1: a stream of Sweep calls racing Close under -race. An in-flight run is
// waited out, no run starts once Close has begun (counted as reads reaching
// the fake after the log file closed), and no record is written to the closed
// file.
func TestT987LaneStallCloseRacesSweepStream(t *testing.T) {
	fake := &fakeHandoffkeep{nextID: 100, status: http.StatusCreated, rows: map[string]*handoffkeepRelayEvent{}}
	var logFileClosed atomic.Bool
	var lateGets, totalGets atomic.Int64
	gated := &t987GatedHK{fake: fake, release: make(chan struct{}), started: make(chan struct{}), closed: &logFileClosed, late: &lateGets, total: &totalGets}
	server := httptest.NewServer(gated)
	t.Cleanup(server.Close)
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
	hub := t987StallHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, &task198Notifier{}, io.Discard)
	hub.mu.Lock()
	hub.logger = slog.New(handler)
	hub.mu.Unlock()
	hub.logFile = logFile
	t961ActiveNode(hub, "host-a", HubActiveJob{JobID: "t987-race", OwnerLane: "lane-a"})
	fake.seedUndelivered(t961LaneRow(41, "lane-a", "t987-race", time.Now().UTC().Add(-(relayLaneStallAge + time.Minute)).Format(time.RFC3339Nano), 1))

	// Two ungated sweeps open the episode, so real Warn traffic has already
	// flowed through the recording handler before the file closes.
	t961Sweep(hub)
	t961Sweep(hub)
	if totalRecords.Load() == 0 {
		t.Fatal("the activated stall never reached the recording handler")
	}

	// Now gate the reads: the next run blocks inside its undelivered GET.
	gated.gate.Store(true)
	hub.Sweep()
	select {
	case <-gated.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the stall run never reached handoffkeep")
	}

	closed := make(chan error, 1)
	go func() { closed <- hub.Close() }()
	const iterations = 500
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations/4; i++ {
				hub.Sweep()
			}
		}()
	}
	select {
	case err := <-closed:
		t.Fatalf("Close returned %v while a stall run was in flight", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(gated.release)
	if err := <-closed; err != nil {
		t.Fatalf("Close=%v", err)
	}
	logFileClosed.Store(true)
	wg.Wait()

	// The flag committed before Close returned: no later Sweep may launch.
	if !hub.laneStallMu.TryLock() {
		t.Fatal("laneStallMu was never freed after Close")
	}
	closing := hub.laneStallClosing
	hub.laneStallMu.Unlock()
	if !closing {
		t.Fatal("Close returned without committing laneStallClosing")
	}
	for i := 0; i < 3; i++ {
		hub.Sweep()
	}
	hub.laneStallSweeps.Wait()
	if got := lateGets.Load(); got != 0 {
		t.Fatalf("lane-stall runs reached handoffkeep after the log file closed: %d reads", got)
	}
	if got := lateRecords.Load(); got != 0 {
		t.Fatalf("log records written after the log file closed: %d", got)
	}
	if totalGets.Load() == 0 || totalRecords.Load() == 0 {
		t.Fatalf("the race never exercised the run (gets=%d records=%d)", totalGets.Load(), totalRecords.Load())
	}
}

// t987HungLaneHK parks lane-scoped GETs until released or the request context
// ends — the hung fake handoffkeep AC2 measures cancellation against. The
// unscoped listing the startup replay uses keeps flowing.
type t987HungLaneHK struct {
	fake    *fakeHandoffkeep
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (h *t987HungLaneHK) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Query().Get("lane") != "" {
		h.once.Do(func() { close(h.started) })
		select {
		case <-h.release:
		case <-r.Context().Done():
		}
		return
	}
	h.fake.ServeHTTP(w, r)
}

// AC2: a maintenance context cancelled while a run is blocked on a hung
// handoffkeep read returns the run within a second and leaves every lane
// unobserved — no episode opens, no recovery, no broadcast.
func TestT987MaintenanceCancelReturnsStallRun(t *testing.T) {
	fake := &fakeHandoffkeep{nextID: 100, status: http.StatusCreated, rows: map[string]*handoffkeepRelayEvent{}}
	hung := &t987HungLaneHK{fake: fake, started: make(chan struct{}), release: make(chan struct{})}
	server := httptest.NewServer(hung)
	t.Cleanup(server.Close)
	defer close(hung.release) // never leave the fake hung on failure
	client, err := newHandoffkeepRelayClient(hubHandoffkeepEnv{URL: server.URL, Token: "test-token"}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	notifier := &task198Notifier{}
	hub, err := NewHubServer(HubServerConfig{
		Tokens:            map[string]string{"operator": "op", "host-a": "node-a"},
		ReportRelayPath:   r20LanesFile(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`),
		Notifier:          notifier,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		KeepaliveInterval: 5 * time.Millisecond,
		handoffkeep:       client,
	})
	if err != nil {
		t.Fatal(err)
	}
	t961ActiveNode(hub, "host-a", HubActiveJob{JobID: "t987-cancel", OwnerLane: "lane-a"})
	events := r20t5Subscribe(t, hub)
	maintenanceCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	maintenanceDone := make(chan struct{})
	go func() { hub.RunMaintenance(maintenanceCtx); close(maintenanceDone) }()

	select {
	case <-hung.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the stall run never reached handoffkeep")
	}
	cancel()
	runDone := make(chan struct{})
	go func() { hub.laneStallSweeps.Wait(); close(runDone) }()
	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("a cancelled maintenance context did not stop the in-flight stall run")
	}
	select {
	case <-maintenanceDone:
	case <-time.After(time.Second):
		t.Fatal("RunMaintenance did not return after cancellation")
	}
	if got := notifier.Accepted(); got != 0 {
		t.Fatalf("the cancelled run left alerts: %+v", notifier.Alerts())
	}
	if entries := t987LaneStallEntries(hub); len(entries) != 0 {
		t.Fatalf("the cancelled run left lane-stall alert entries: %v", entries)
	}
	if got := len(events("relay.lane_stalled")); got != 0 {
		t.Fatalf("the cancelled run broadcast %d lane stalls", got)
	}
}

// AC3: a lane observed healthy once leaves no lane-stall alert entry behind.
func TestT987LaneStallPrunesInactiveEntry(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := t961StallHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, &task198Notifier{})
	t961ActiveNode(hub, "host-a", HubActiveJob{JobID: "t987-young-job", OwnerLane: "lane-a"})
	fake.seedUndelivered(t961LaneRow(11, "lane-a", "t987-young", time.Now().UTC().Format(time.RFC3339Nano), 1))

	t961Sweep(hub)
	if got := len(fake.queries("/v1/relay/events")); got != 1 {
		t.Fatalf("undelivered GETs=%d, want 1 (the lane really was observed)", got)
	}
	if entries := t987LaneStallEntries(hub); len(entries) != 0 {
		t.Fatalf("a healthy lane's entry survived the sweep: %v", entries)
	}
	t961Sweep(hub)
	if entries := t987LaneStallEntries(hub); len(entries) != 0 {
		t.Fatalf("lane-stall entries after two healthy sweeps: %v", entries)
	}
}

// AC3: an open episode keeps its entry through deactivation and the recovery
// notification; the idle entry is dropped on the next observation.
func TestT987LaneStallKeepsEntryUntilRecovery(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	notifier := &task198Notifier{}
	hub := t961StallHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, notifier)
	t961ActiveNode(hub, "host-a", HubActiveJob{JobID: "t987-ep", OwnerLane: "lane-a"})
	old := time.Now().UTC().Add(-(relayLaneStallAge + time.Minute)).Format(time.RFC3339Nano)
	fake.seedUndelivered(t961LaneRow(21, "lane-a", "t987-ep", old, 1))

	t961Sweep(hub)
	t961Sweep(hub)
	if alerts := notifier.Alerts(); len(alerts) != 1 || alerts[0].Recovery {
		t.Fatalf("the episode did not open: %+v", alerts)
	}
	if state := t987LaneStallEntry(hub, "lane-a"); state == nil || !state.active {
		t.Fatalf("an open episode lost its alert entry: %+v", state)
	}

	if err := client.markDelivered(context.Background(), 21, "host-a", "w1:p1"); err != nil {
		t.Fatal(err)
	}
	t961Sweep(hub) // first clear observation: the episode is still open
	if state := t987LaneStallEntry(hub, "lane-a"); state == nil || !state.active {
		t.Fatalf("the episode closed after one clear observation: %+v", state)
	}
	t961Sweep(hub) // second clear observation: deactivation + recovery produced
	if alerts := notifier.Alerts(); len(alerts) != 2 || !alerts[1].Recovery {
		t.Fatalf("the recovery notification is missing: %+v", alerts)
	}
	if state := t987LaneStallEntry(hub, "lane-a"); state == nil {
		t.Fatal("the entry was dropped while its recovery was still owed")
	}
	t961Sweep(hub) // the now-idle entry prunes on the next observation
	if state := t987LaneStallEntry(hub, "lane-a"); state != nil {
		t.Fatalf("an idle lane-stall entry survived its recovery: %+v", state)
	}
}

// AC3: fifty ephemeral builder lanes observed once leave zero entries —
// laneStallCandidates can no longer accumulate them forever.
func TestT987LaneStallEphemeralLanesLeaveNoEntries(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	routes := make([]string, 50)
	for i := range routes {
		routes[i] = fmt.Sprintf(`"t987-e%02d":{"machine":"host-a","pane":"w1:p%d"}`, i, i)
	}
	hub := t961StallHub(t, `{"lanes":{`+strings.Join(routes, ",")+`}}`, client, &task198Notifier{})
	jobs := make([]HubActiveJob, 0, 25)
	for i := 0; i < 25; i++ {
		jobs = append(jobs, HubActiveJob{JobID: fmt.Sprintf("t987-e%02d-job", i), OwnerLane: fmt.Sprintf("t987-e%02d", i)})
	}
	t961ActiveNode(hub, "host-a", jobs...)

	t961Sweep(hub)
	if got := len(fake.queries("/v1/relay/events")); got != 25 {
		t.Fatalf("undelivered GETs=%d, want 25 (one per lane with work)", got)
	}
	if entries := t987LaneStallEntries(hub); len(entries) != 0 {
		t.Fatalf("%d lane-stall entries survived one healthy sweep: %v", len(entries), entries)
	}
}

// AC4: a stalled lane whose undelivered rows sit in the held projection is
// annotated, not excused — held_count rides the broadcast and the Warn line,
// and the dampened activation still needs its two observations.
func TestT987LaneStallHeldCountAnnotatesAlarm(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	var logBuf bytes.Buffer
	hub := t987StallHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, &task198Notifier{}, &logBuf)
	t961ActiveNode(hub, "host-a", HubActiveJob{JobID: "t987-held", OwnerLane: "lane-a"})
	old := time.Now().UTC().Add(-(relayLaneStallAge + time.Minute)).Format(time.RFC3339Nano)
	fake.seedUndelivered(
		t961LaneRow(41, "lane-a", "t987-held-a", old, 1),
		t961LaneRow(42, "lane-a", "t987-held-b", old, 1),
	)
	hub.mu.Lock()
	hub.relayHeld[41] = hubRelayHeldProjection{ID: 41, Lane: "lane-a", Pane: "w1:p1", Machine: "host-a", HeldSince: old, DeliverPolicy: "idle", JobID: "t987-held"}
	hub.relayHeld[42] = hubRelayHeldProjection{ID: 42, Lane: "lane-a", Pane: "w1:p1", Machine: "host-a", HeldSince: old, DeliverPolicy: "idle", JobID: "t987-held"}
	// A held row on another lane must not leak into lane-a's count.
	hub.relayHeld[77] = hubRelayHeldProjection{ID: 77, Lane: "lane-b", Pane: "w1:p9", Machine: "host-a", HeldSince: old, DeliverPolicy: "idle", JobID: "t987-other"}
	hub.mu.Unlock()
	events := r20t5Subscribe(t, hub)

	t961Sweep(hub) // first dampened observation: no broadcast yet
	if got := len(t961StallBroadcasts(events)); got != 0 {
		t.Fatalf("the alarm fired on the first observation: %d broadcasts", got)
	}
	t961Sweep(hub) // second: the episode activates
	stalled := t961StallBroadcasts(events)
	if len(stalled) != 1 {
		t.Fatalf("relay.lane_stalled broadcasts=%d, want 1", len(stalled))
	}
	payload := stalled[0]
	if payload["lane"] != "lane-a" || payload["undelivered_count"] != float64(2) || payload["held_count"] != float64(2) || payload["active_jobs"] != float64(1) {
		t.Fatalf("relay.lane_stalled payload=%v, want lane-a undelivered_count=2 held_count=2 active_jobs=1", payload)
	}
	line := ""
	for _, candidate := range strings.Split(strings.TrimRight(logBuf.String(), "\n"), "\n") {
		if strings.Contains(candidate, "relay lane stalled") {
			line = candidate
		}
	}
	if line == "" || !strings.Contains(line, "held_count=2") || !strings.Contains(line, "undelivered_count=2") {
		t.Fatalf("the Warn line lacks held_count:\n%s", logBuf.String())
	}
}

package panewire

import (
	"bytes"
	"context"
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

// #1009: bound Close's wait on the update-overdue flush. Close cancels the
// hub's close context before it waits the flush out, so a sink that accepts
// the connection but never answers can no longer stretch a hub stop to
// K x handoffkeepTimeout. The flush stops between notices once the context
// ends, a cancelled write stays queued without spending its first-attempt
// Warn, and ordinary lane-event relays keep their own context.

// t1009CloseBound is the AC1/AC2 contract: hub shutdown must not wait out
// K x handoffkeepTimeout on a dead sink.
const t1009CloseBound = 2 * time.Second

// t1009HungSink models a handoffkeep that accepts the connection and drains
// the POST body — so a client-side cancel is observable server-side — but
// never answers /v1/relay/events on its own: it parks until release closes
// or the request context ends. A released POST falls through to the fake
// with its body restored; every other request goes straight there.
type t1009HungSink struct {
	fake      *fakeHandoffkeep
	started   chan struct{}
	release   chan struct{}
	startOnce sync.Once
	posts     atomic.Int64
	cancelled atomic.Int64
}

func (s *t1009HungSink) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/v1/relay/events" {
		s.fake.ServeHTTP(w, r)
		return
	}
	s.posts.Add(1)
	raw, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	s.startOnce.Do(func() { close(s.started) })
	select {
	case <-s.release:
	case <-r.Context().Done():
		s.cancelled.Add(1)
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(raw))
	s.fake.ServeHTTP(w, r)
}

// t1009HungFixture is a hub whose update-overdue flush writes to the hung
// sink, plus the real log file and counting handler the after-Close
// assertions need.
type t1009HungFixture struct {
	hub          *HubServer
	sink         *t1009HungSink
	logPath      string
	totalRecords *atomic.Int64
	lateRecords  *atomic.Int64
	releaseOnce  *sync.Once
}

func t1009NewHungFixture(t *testing.T, pending int) *t1009HungFixture {
	t.Helper()
	fake := &fakeHandoffkeep{nextID: 100, status: http.StatusCreated, rows: map[string]*handoffkeepRelayEvent{}}
	sink := &t1009HungSink{fake: fake, started: make(chan struct{}), release: make(chan struct{})}
	server := httptest.NewServer(sink)
	t.Cleanup(server.Close)
	releaseOnce := &sync.Once{}
	t.Cleanup(func() { releaseOnce.Do(func() { close(sink.release) }) })
	client, err := newHandoffkeepRelayClient(hubHandoffkeepEnv{URL: server.URL, Token: "test-token"}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(t.TempDir(), "hub.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	var totalRecords, lateRecords atomic.Int64
	handler := &t987CloseCountingHandler{inner: slog.NewTextHandler(logFile, nil), total: &totalRecords, late: &lateRecords}
	hub := t987StallHub(t, `{"lanes":{"offload":{"sink":true}}}`, client, &task198Notifier{}, io.Discard)
	hub.mu.Lock()
	hub.logger = slog.New(handler)
	deadline := time.Now().UTC()
	for i := 0; i < pending; i++ {
		machine := fmt.Sprintf("t1009-node-%02d", i)
		hub.updateOverduePending[machine+"\x00v1.0"] = &hubUpdateOverdue{machine: machine, version: "v1.0", deadline: deadline}
	}
	hub.mu.Unlock()
	hub.logFile = logFile
	return &t1009HungFixture{hub: hub, sink: sink, logPath: logPath, totalRecords: &totalRecords, lateRecords: &lateRecords, releaseOnce: releaseOnce}
}

// t1009WaitFlushStarted parks the fixture's flush inside its first sink POST
// so Close races a genuinely in-flight handoffkeep write.
func (fx *t1009HungFixture) startStuckFlush(t *testing.T) {
	t.Helper()
	fx.hub.startUpdateOverdueFlush()
	select {
	case <-fx.sink.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the overdue flush never reached the sink")
	}
}

// t1009CloseAndAssert runs the AC1 assertions once the flush is stuck:
// Close within the bound, flush goroutine gone, no POST after Close began,
// no log record after the file closed, every notice still queued with zero
// attempts spent and no first-attempt Warn from the cancellation.
func (fx *t1009HungFixture) closeAndAssert(t *testing.T, pending int, shape string) {
	t.Helper()
	start := time.Now()
	closed := make(chan error, 1)
	go func() { closed <- fx.hub.Close() }()
	var elapsed time.Duration
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close=%v", err)
		}
		elapsed = time.Since(start)
	case <-time.After(t1009CloseBound):
		t.Fatalf("%s: Close was still waiting %v after it began; bound is %v", shape, t1009CloseBound, t1009CloseBound)
	}
	t.Logf("%s K=%d: Close returned in %v (bound %v)", shape, pending, elapsed, t1009CloseBound)

	drained := make(chan struct{})
	go func() { fx.hub.updateOverdueFlushes.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("the update-overdue flush goroutine outlived Close")
	}
	if got := fx.sink.posts.Load(); got != 1 {
		t.Fatalf("sink POSTs=%d, want exactly the one in flight when Close began", got)
	}
	if got := fx.lateRecords.Load(); got != 0 {
		t.Fatalf("log records written after the log file closed: %d", got)
	}
	fx.hub.mu.Lock()
	remaining := len(fx.hub.updateOverduePending)
	attempts := 0
	for _, notice := range fx.hub.updateOverduePending {
		attempts += notice.attempts
	}
	fx.hub.mu.Unlock()
	if remaining != pending {
		t.Fatalf("updateOverduePending=%d, want all %d cancelled notices still queued", remaining, pending)
	}
	if attempts != 0 {
		t.Fatalf("cancelled notices spent %d attempts, want 0", attempts)
	}
	contents, err := os.ReadFile(fx.logPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(contents), "update.overdue is not yet recorded") {
		t.Fatalf("the cancellation raised the first-attempt Warn:\n%s", contents)
	}
}

// AC1 + AC4: K pending notices behind a sink that never answers. Close must
// return within 2s having cancelled the in-flight write; the flush goroutine
// must be gone when Close returns; no POST may start after Close begins; no
// log record may land after the file closes; and every cancelled notice
// stays queued without spending its first-attempt Warn.
func TestT1009CloseBoundStuckFlush(t *testing.T) {
	for _, pending := range []int{1, 3, 10} {
		t.Run(fmt.Sprintf("K=%d", pending), func(t *testing.T) {
			fx := t1009NewHungFixture(t, pending)
			fx.startStuckFlush(t)
			fx.closeAndAssert(t, pending, "plain Close")
		})
	}
}

// AC2: the SIGTERM shape — maintenanceCtx cancelled first, then Close — is
// held to the same bound. The flush never consumed maintenanceCtx, so it is
// still parked when the maintenance cancel lands, and it is Close's own
// signal that stops it.
func TestT1009CloseBoundStuckFlushAfterMaintenanceCancel(t *testing.T) {
	for _, pending := range []int{1, 3, 10} {
		t.Run(fmt.Sprintf("K=%d", pending), func(t *testing.T) {
			fx := t1009NewHungFixture(t, pending)
			maintenanceCtx, cancelMaintenance := context.WithCancel(context.Background())
			fx.hub.mu.Lock()
			fx.hub.maintenanceCtx = maintenanceCtx
			fx.hub.mu.Unlock()
			defer cancelMaintenance()
			fx.startStuckFlush(t)
			cancelMaintenance()
			fx.closeAndAssert(t, pending, "maintenanceCtx cancelled, then Close")
		})
	}
}

// AC3a: with a sink that answers, the flush still records every queued
// notice and empties the queue — the close context changes nothing on the
// healthy path.
func TestT1009HealthyFlushRecordsEveryNotice(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := t987StallHub(t, `{"lanes":{"offload":{"sink":true}}}`, client, &task198Notifier{}, io.Discard)
	hub.mu.Lock()
	deadline := time.Now().UTC()
	for i := 0; i < 3; i++ {
		machine := fmt.Sprintf("t1009-ok-%02d", i)
		hub.updateOverduePending[machine+"\x00v1.0"] = &hubUpdateOverdue{machine: machine, version: "v1.0", deadline: deadline}
	}
	hub.mu.Unlock()

	hub.startUpdateOverdueFlush()
	done := make(chan struct{})
	go func() { hub.updateOverdueFlushes.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the healthy flush did not finish")
	}

	hub.mu.Lock()
	remaining := len(hub.updateOverduePending)
	hub.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("%d notices stayed queued behind a healthy sink", remaining)
	}
	if got := fake.count(http.MethodPost, "/v1/relay/events"); got != 3 {
		t.Fatalf("sink POSTs=%d, want 3", got)
	}
	fake.mu.Lock()
	delivered := 0
	for _, row := range fake.rows {
		if row.DeliveredTo == "sink/sink:offload" {
			delivered++
		}
	}
	fake.mu.Unlock()
	if delivered != 3 {
		t.Fatalf("sink deliveries=%d, want 3 (each recorded row marked delivered)", delivered)
	}
}

// AC3b: an ordinary lane-event relay — the path every caller outside the
// update-overdue flush takes — keeps context.Background(), so hub Close must
// not cancel one in flight and must not reach one started after Close.
func TestT1009CloseNeverCancelsOrdinaryRelay(t *testing.T) {
	fake := &fakeHandoffkeep{nextID: 100, status: http.StatusCreated, rows: map[string]*handoffkeepRelayEvent{}}
	sink := &t1009HungSink{fake: fake, started: make(chan struct{}), release: make(chan struct{})}
	server := httptest.NewServer(sink)
	t.Cleanup(server.Close)
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(sink.release) }) })
	client, err := newHandoffkeepRelayClient(hubHandoffkeepEnv{URL: server.URL, Token: "test-token"}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	hub := t987StallHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, &task198Notifier{}, io.Discard)
	ingress := func(eventID string) hubJobEventPayload {
		return hubJobEventPayload{JobID: laneEventTransportID("lane-a", eventID), Epoch: 1, OwnerLane: "lane-a", EventID: eventID, Text: "ordinary lane event", Host: "hub", Label: "t1009"}
	}

	result := make(chan relayLaneEventResult, 1)
	go func() { result <- hub.relayLaneEvent(ingress("t1009-ordinary"), nil) }()
	select {
	case <-sink.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the ordinary relay never reached the sink")
	}
	if err := hub.Close(); err != nil {
		t.Fatalf("Close=%v", err)
	}
	releaseOnce.Do(func() { close(sink.release) })
	select {
	case res := <-result:
		if res.PersistFailed || res.ID == 0 {
			t.Fatalf("Close broke the in-flight ordinary relay: %+v", res)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the ordinary relay never finished")
	}
	if got := sink.cancelled.Load(); got != 0 {
		t.Fatalf("Close cancelled the in-flight ordinary relay (%d cancelled POSTs)", got)
	}
	// A relay that starts after Close must still persist on its own context.
	res := hub.relayLaneEvent(ingress("t1009-ordinary-2"), nil)
	if res.PersistFailed || res.ID == 0 {
		t.Fatalf("an ordinary relay started after Close was not persisted: %+v", res)
	}
}

// AC5/NICE-1: pin the held.Lane == obs.lane term in laneHeldCountLocked. A
// held projection entry whose lane no longer matches the durable row's owner
// lane — a lane re-pointed between inject and sweep leaves exactly that
// divergence — must not be credited to this lane even though its durable id
// sits inside the observation's counted set. Dropping the term turns this
// RED: the foreign held row inflates held_count to 1.
func TestT1009LaneStallHeldCountSkipsHeldRowsFromOtherLanes(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	var logBuf bytes.Buffer
	hub := t987StallHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, &task198Notifier{}, &logBuf)
	t961ActiveNode(hub, "host-a", HubActiveJob{JobID: "t1009-held", OwnerLane: "lane-a"})
	old := time.Now().UTC().Add(-(relayLaneStallAge + time.Minute)).Format(time.RFC3339Nano)
	fake.seedUndelivered(t961LaneRow(61, "lane-a", "t1009-held", old, 1))
	hub.mu.Lock()
	// The durable row is lane-a's, but the held projection still names the
	// lane the pane reported before the lane was re-pointed.
	hub.relayHeld[61] = hubRelayHeldProjection{ID: 61, Lane: "lane-b", Pane: "w1:p9", Machine: "host-a", HeldSince: old, DeliverPolicy: "idle", JobID: "t1009-held"}
	hub.mu.Unlock()
	events := r20t5Subscribe(t, hub)

	t961Sweep(hub) // first dampened observation
	t961Sweep(hub) // second: the episode activates
	stalled := t961StallBroadcasts(events)
	if len(stalled) != 1 {
		t.Fatalf("relay.lane_stalled broadcasts=%d, want 1", len(stalled))
	}
	payload := stalled[0]
	if payload["undelivered_count"] != float64(1) || payload["held_count"] != float64(0) {
		t.Fatalf("relay.lane_stalled payload=%v, want undelivered_count=1 held_count=0 (the held projection belongs to lane-b)", payload)
	}
	line := ""
	for _, candidate := range strings.Split(strings.TrimRight(logBuf.String(), "\n"), "\n") {
		if strings.Contains(candidate, "relay lane stalled") {
			line = candidate
		}
	}
	if line == "" || !strings.Contains(line, "held_count=0") || !strings.Contains(line, "undelivered_count=1") {
		t.Fatalf("the Warn line lacks held_count=0:\n%s", logBuf.String())
	}
}

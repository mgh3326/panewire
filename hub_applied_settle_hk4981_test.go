package panewire

// hk 4981: the hub held replayMu across the whole applied-report settlement,
// so one Mac node's report (up to 64 lanes of handoffkeep listings) delayed
// every other node's connect() past its hello deadline and the node dropped
// the session. These tests pin the settlement outside the lock, the ordering
// contract that keeps a replay off the rows the report closes, the refusal of
// a report expireAppliedReport already released, and the session-end log line
// each side now writes.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// blockAppliedList makes the fake handoffkeep hold every undelivered listing
// for lane inside the request until release is called; entered is signalled
// when the first one is in flight.
func (rig *t989Rig) blockAppliedList(lane string) (entered chan struct{}, release func()) {
	entered = make(chan struct{}, 1)
	gate := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	rig.fake.mu.Lock()
	rig.fake.observeQuery = func(method, path, query string) {
		if method != http.MethodGet || !strings.Contains(query, "undelivered") || !strings.Contains(query, "lane="+lane) {
			return
		}
		select {
		case entered <- struct{}{}:
		default:
		}
		<-gate
	}
	rig.fake.mu.Unlock()
	rig.t.Cleanup(release)
	return entered, release
}

func (rig *t989Rig) helloWithReportCapability() {
	rig.send("host-a", rig.dest, `{"type":"hello","machine_id":"host-a","version":"panewire-t2777","capabilities":["applied_v1"]}`)
}

func (rig *t989Rig) reportApplied(lane string, ids ...int64) {
	payload, err := json.Marshal(map[string]any{"type": "applied", "applied": map[string][]int64{lane: ids}})
	if err != nil {
		rig.t.Fatal(err)
	}
	rig.send("host-a", rig.dest, string(payload))
}

// A slow handoffkeep must delay only the reporting node's own replay, never
// another node's registration.
func TestAppliedReportSettleDoesNotHoldReplayMu(t *testing.T) {
	rig := t989Start(t, "w1:pA")
	rig.herdr.getStatusByPane["w1:pA"] = "working"
	ids, _ := rig.deliverAndLoseAcks([]string{"already read"}, 1)
	rig.startHub()
	entered, release := rig.blockAppliedList(t989Lane)
	rig.helloWithReportCapability()
	rig.reportApplied(t989Lane, ids[0])
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the applied report never reached handoffkeep")
	}
	// The settlement is inside a handoffkeep call now. A second machine's
	// hello takes replayMu and must not wait for it.
	connected := make(chan struct{})
	go func() {
		rig.hub.connect("host-b", "panewire-t2777", "fixture", rig.src, true)
		close(connected)
	}()
	select {
	case <-connected:
	case <-time.After(time.Second):
		release()
		t.Fatal("connect() of another machine waited out the applied report settlement")
	}
	release()
	eventually(t, 5*time.Second, "the report closes the row", func() bool { return rig.row(ids[0]).DeliveredAt != "" })
}

// A capability-free fallback hello on a live session must not start a second
// full replay; the capable re-hello behind the pong carries the report that
// releases one.
func TestCapabilityFreeReHelloDoesNotReplayTwice(t *testing.T) {
	rig := t989Start(t, "w1:pA")
	rig.gapRows(2)
	rig.startHub()
	rig.helloWithReportCapability()
	rig.send("host-a", rig.dest, `{"type":"hello","machine_id":"host-a","version":"panewire-t2777"}`)
	// A window that proves nothing more happens: the fallback hello added no
	// replay of its own.
	if waitUntil(500*time.Millisecond, func() bool { return len(rig.dest.relays) > 0 }) {
		t.Fatalf("the capability-free re-hello replayed %d rows", drainRelays(rig.dest))
	}
	rig.hello()
	eventually(t, 5*time.Second, "the capable report releases the replay", func() bool { return len(rig.dest.relays) == 2 })
}

// The ordering contract: a replay that runs while a report settles counts only
// rows that report does not close. Settlement keeps appliedPending set for the
// whole window, so the walk skips them.
func TestReplaySkipsReportRowsWhileSettlementRuns(t *testing.T) {
	rig := t989Start(t, "w1:pA")
	rig.herdr.getStatusByPane["w1:pA"] = "working"
	ids, _ := rig.deliverAndLoseAcks([]string{"already read"}, 1)
	attempts := rig.row(ids[0]).Attempts
	rig.startHub()
	entered, release := rig.blockAppliedList(t989Lane)
	rig.helloWithReportCapability()
	rig.reportApplied(t989Lane, ids[0])
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the applied report never reached handoffkeep")
	}
	// This walk must both run (it takes replayMu's read side, which the old
	// lock-everything settlement held) and leave the settling node's rows
	// alone.
	walked := make(chan struct{})
	go func() {
		rig.hub.replayUndeliveredLaneEvents(context.Background())
		close(walked)
	}()
	select {
	case <-walked:
	case <-time.After(time.Second):
		release()
		t.Fatal("a replay walk waited for the applied report settlement")
	}
	if got := drainRelays(rig.dest); got != 0 {
		t.Fatalf("replay queued %d rows while their report was settling", got)
	}
	if row := rig.row(ids[0]); row.DeliveredAt != "" || row.Attempts != attempts {
		t.Fatalf("replay changed a row the settling report closes: %+v", row)
	}
	release()
	eventually(t, 5*time.Second, "the report closes the row", func() bool { return rig.row(ids[0]).DeliveredAt != "" })
	if got := drainRelays(rig.dest); got != 0 {
		t.Fatalf("replayed %d rows the report closed", got)
	}
}

// expireAppliedReport has already released the registration and told replay to
// re-offer the rows; a report that arrives afterwards must not close them (the
// next registration's report will, with the node's full list).
func TestAppliedReportAfterExpiryDoesNotSettle(t *testing.T) {
	rig := t989Start(t, "w1:pA")
	rig.herdr.getStatusByPane["w1:pA"] = "working"
	ids, _ := rig.deliverAndLoseAcks([]string{"already read"}, 1)
	rig.startHub()
	rig.helloWithReportCapability()
	rig.hub.mu.Lock()
	registration := rig.hub.nodes["host-a"]
	rig.hub.mu.Unlock()
	rig.hub.expireAppliedReport("host-a", registration)
	eventually(t, 5*time.Second, "expiry replays without the report", func() bool { return len(rig.dest.relays) == 1 })
	drainRelays(rig.dest)
	rig.reportApplied(t989Lane, ids[0])
	// A window that proves nothing more happens: the report is refused, so the
	// row stays open for the next registration's report.
	if waitUntil(time.Second, func() bool { return rig.row(ids[0]).DeliveredAt != "" }) {
		t.Fatal("a report that arrived after expiry settled a row the replay had released")
	}
}

// The classes the two candidate closers produce, on both sides.
func TestHubSessionEndReasonsNameTheQueueAndTheWriteDeadline(t *testing.T) {
	cases := []struct {
		err    error
		reason string
		code   int
	}{
		{errHubInboundQueueFull, "inbound_full", 0},
		{fmt.Errorf("write frame: %w", context.DeadlineExceeded), "write_timeout", 0},
		{fmt.Errorf("pong write: %w", context.DeadlineExceeded), "write_timeout", 0},
		{websocket.CloseError{Code: websocket.StatusGoingAway, Reason: "maintenance"}, "closed_by_hub", 1001},
		{errHubRedial, "redial", 0},
		{context.Canceled, "local_stop", 0},
		{errors.New("websocket read failed"), "read", 0},
	}
	for _, item := range cases {
		if reason, code := hubSessionEndReason(item.err); reason != item.reason || code != item.code {
			t.Fatalf("%v classified %q/%d, want %q/%d", item.err, reason, code, item.reason, item.code)
		}
	}
	if reason, code := hubNodeSessionEndReason(websocket.CloseError{Code: websocket.StatusNormalClosure}); reason != "closed_by_node" || code != 1000 {
		t.Fatalf("hub close classified %q/%d, want closed_by_node/1000", reason, code)
	}
}

// A session ended by the node's close frame is logged with the code and
// nothing that could carry the endpoint.
func TestHubLogsNodeSessionEndReason(t *testing.T) {
	logs := &syncBuffer{}
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "op", "machine-a": "node-token"}, Logger: slog.New(slog.NewTextHandler(logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hub.Close() })
	server := httptest.NewServer(hub.Handler())
	t.Cleanup(server.Close)
	connection := t14NamedNodeToken(t, hub, server, "machine-a", "node-token", true, true)
	if err := connection.Close(websocket.StatusNormalClosure, "done"); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "the hub logs the session end", func() bool {
		return len(logs.lines("node session ended")) > 0
	})
	line := strings.Join(logs.lines("node session ended"), "\n")
	for _, want := range []string{"machine=machine-a", "reason=closed_by_node", "close_code=1000"} {
		if !strings.Contains(line, want) {
			t.Fatalf("hub log %q lacks %q", line, want)
		}
	}
}

// hk4981Node builds a node client whose warnings a test can read.
func hk4981Node(t *testing.T, url string, warn func(string)) *HubClient {
	t.Helper()
	client, err := NewHubClient(HubClientConfig{URL: url, MachineID: "machine-a", Token: "node-token", AllowInsecureForTests: true, PingInterval: time.Hour,
		InitialBackoff: 30 * time.Second, MaxBackoff: time.Minute,
		Wait:                func(ctx context.Context, delay time.Duration) error { return nil },
		Warn:                warn,
		JobsInboxRoot:       t.TempDir(),
		sessionCollector:    func(context.Context) ([]HerdrAgentState, error) { return nil, nil },
		hostLoadCollector:   func(context.Context) (HubHostLoad, error) { return HubHostLoad{}, nil },
		hostMemoryCollector: func(context.Context) (*HubHostMemory, error) { return nil, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// findWarning returns the first captured warn line containing substring.
func findWarning(log *warningLog, substring string) string {
	for _, line := range log.all() {
		if strings.Contains(line, substring) {
			return line
		}
	}
	return ""
}

// The node's inbound queue is a session closer in its own right: its overrun
// ends the session and the new line names it.
func TestNodeSessionEndsWithInboundFullReason(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.CloseNow()
		frame := []byte(`{"type":"idle-wake.route","event_id":"evt-1","pane":"w1:p1","eligible":true,"lane":"lane-idle","text":"wake"}`)
		for range hubInboundQueueDepth + 32 {
			if err := connection.Write(request.Context(), websocket.MessageText, frame); err != nil {
				return
			}
		}
		<-request.Context().Done()
	}))
	t.Cleanup(server.Close)
	warnings := &warningLog{}
	client := hk4981Node(t, t14WSURL(server.URL, ""), warnings.add)
	// Holding the manager's lock parks the dispatcher on the first queued
	// frame, so the reader fills the queue exactly as a stalled handler does.
	manager := &idleWakeManager{store: NewMemoryStore(t), logger: slog.New(slog.NewTextHandler(io.Discard, nil)), now: time.Now}
	manager.mu.Lock()
	client.SetIdleWakeManager(manager)
	defer manager.mu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); client.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	eventually(t, 10*time.Second, "the node logs an inbound-full session end", func() bool {
		return findWarning(warnings, "reason=inbound_full") != ""
	})
	line := findWarning(warnings, "hub session ended")
	if !strings.Contains(line, "close_code=0") || !strings.Contains(line, "uptime=") {
		t.Fatalf("session end line %q lacks close_code or uptime", line)
	}
}

// A hub close frame ends the node session with its code, not with the
// transport error's text.
func TestNodeLogsHubSessionEndCloseCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.CloseNow()
		_ = connection.Close(websocket.StatusGoingAway, "maintenance")
	}))
	t.Cleanup(server.Close)
	warnings := &warningLog{}
	client := hk4981Node(t, t14WSURL(server.URL, ""), warnings.add)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); client.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	eventually(t, 10*time.Second, "the node logs the close code", func() bool {
		return findWarning(warnings, "reason=closed_by_hub close_code=1001") != ""
	})
}

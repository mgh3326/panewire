package panewire

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// lifecycleFakeHerdr is a Unix-socket NDJSON herdr: agent.list and tab.list
// answer from the test's current fixture, events.subscribe params are
// recorded, the first reject subscribes get an error reply, and accepted
// streams can be pushed envelopes.
type lifecycleFakeHerdr struct {
	mu       sync.Mutex
	writeMu  sync.Mutex
	path     string
	listener net.Listener
	agents   []map[string]any
	subs     [][]map[string]any
	reject   int
	streams  []net.Conn
	methods  []string
}

func newLifecycleFakeHerdr(t *testing.T, agents []map[string]any) *lifecycleFakeHerdr {
	t.Helper()
	base, err := os.MkdirTemp("/tmp", "pw-lc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	path := filepath.Join(base, "h.sock")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	f := &lifecycleFakeHerdr{path: path, listener: l, agents: agents}
	go f.serve()
	t.Cleanup(f.Close)
	return f
}

func (f *lifecycleFakeHerdr) serve() {
	for {
		c, err := f.listener.Accept()
		if err != nil {
			return
		}
		go f.conn(c)
	}
}

func (f *lifecycleFakeHerdr) conn(c net.Conn) {
	defer c.Close()
	reader := bufio.NewReader(c)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			f.handleLine(c, line)
		}
		if err != nil {
			return
		}
	}
}

func (f *lifecycleFakeHerdr) handleLine(c net.Conn, line []byte) {
	var req struct {
		ID     string          `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	if json.Unmarshal(line, &req) != nil || req.Method == "" {
		return
	}
	f.mu.Lock()
	f.methods = append(f.methods, req.Method)
	f.mu.Unlock()
	switch req.Method {
	case "agent.list":
		f.mu.Lock()
		agents := append([]map[string]any(nil), f.agents...)
		f.mu.Unlock()
		f.respond(c, req.ID, map[string]any{"agents": agents})
	case "tab.list":
		f.respond(c, req.ID, map[string]any{"tabs": []any{}})
	case "events.subscribe":
		f.handleSubscribe(c, req.ID, req.Params)
	default:
		f.respond(c, req.ID, map[string]any{})
	}
}

func (f *lifecycleFakeHerdr) handleSubscribe(c net.Conn, id string, params json.RawMessage) {
	var body struct {
		Subscriptions []map[string]any `json:"subscriptions"`
	}
	_ = json.Unmarshal(params, &body)
	f.mu.Lock()
	f.subs = append(f.subs, body.Subscriptions)
	reject := f.reject > 0
	if reject {
		f.reject--
	} else {
		f.streams = append(f.streams, c)
	}
	f.mu.Unlock()
	if reject {
		f.writeLine(c, map[string]any{"id": id, "error": map[string]any{"code": "invalid_params", "message": "unknown subscription type"}})
		return
	}
	f.respond(c, id, map[string]any{"type": "subscription_started"})
}

func (f *lifecycleFakeHerdr) respond(c net.Conn, id string, result any) {
	f.writeLine(c, map[string]any{"id": id, "result": result})
}

func (f *lifecycleFakeHerdr) writeLine(c net.Conn, v any) {
	b, _ := json.Marshal(v)
	f.writeMu.Lock()
	defer f.writeMu.Unlock()
	_, _ = fmt.Fprintf(c, "%s\n", b)
}

func (f *lifecycleFakeHerdr) push(v any) {
	b, _ := json.Marshal(v)
	f.mu.Lock()
	defer f.mu.Unlock()
	kept := f.streams[:0]
	for _, c := range f.streams {
		f.writeMu.Lock()
		_, err := fmt.Fprintf(c, "%s\n", b)
		f.writeMu.Unlock()
		if err == nil {
			kept = append(kept, c)
		}
	}
	f.streams = kept
}

func (f *lifecycleFakeHerdr) setAgents(agents []map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.agents = agents
}

func (f *lifecycleFakeHerdr) subscribeParams() [][]map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]map[string]any, len(f.subs))
	for i, subs := range f.subs {
		out[i] = append([]map[string]any(nil), subs...)
	}
	return out
}

func (f *lifecycleFakeHerdr) subscribeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.subs)
}

func (f *lifecycleFakeHerdr) methodCount(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, m := range f.methods {
		if m == method {
			n++
		}
	}
	return n
}

func (f *lifecycleFakeHerdr) methodCounts() map[string]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]int{}
	for _, m := range f.methods {
		out[m]++
	}
	return out
}

func (f *lifecycleFakeHerdr) Close() {
	_ = f.listener.Close()
	f.mu.Lock()
	for _, c := range f.streams {
		_ = c.Close()
	}
	f.mu.Unlock()
	_ = os.RemoveAll(filepath.Dir(f.path))
}

func lifecycleAgent(paneID string) map[string]any {
	return map[string]any{
		"pane_id": paneID, "workspace_id": "w1", "tab_id": "t-" + paneID,
		"agent": "claude", "name": "worker", "agent_status": "idle",
		"revision": 1, "state_change_seq": 1,
	}
}

func paneCreatedEnvelope(paneID string) map[string]any {
	return map[string]any{"event": "pane_created", "data": map[string]any{
		"type": "pane_created",
		"pane": map[string]any{
			"pane_id": paneID, "workspace_id": "w1", "tab_id": "t-" + paneID,
			"terminal_id": "term-" + paneID, "focused": false,
			"agent_status": "idle", "revision": 3,
		},
	}}
}

func agentDetectedEnvelope(paneID, agent string, released bool) map[string]any {
	data := map[string]any{"type": "pane_agent_detected", "pane_id": paneID, "workspace_id": "w1", "released": released}
	if agent == "" {
		data["agent"] = nil
	} else {
		data["agent"] = agent
	}
	return map[string]any{"event": "pane_agent_detected", "data": data}
}

func paneDepartureEnvelope(kind, paneID string) map[string]any {
	underscored := strings.ReplaceAll(kind, ".", "_")
	return map[string]any{"event": underscored, "data": map[string]any{
		"type": underscored, "pane_id": paneID, "workspace_id": "w1",
	}}
}

// lockedLogBuffer is an io.Writer for the daemon's slog handler that the test
// can also read while the daemon goroutine is still logging.
type lockedLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type lifecycleDaemonFixture struct {
	daemon *Daemon
	store  *Store
	logs   *lockedLogBuffer
}

func lifecycleSchemaCommand(t *testing.T) []string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "schema.json")
	body := `{"protocol":22,"schema_version":1,"schemas":{"request":{"methods":["events.subscribe","agent.list","tab.list","agent.read","agent.prompt","agent.wait","pane.read"],"subscriptions":["pane.agent_status_changed","pane.output_matched","pane.scroll_changed"]}}}`
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return []string{"cat", path}
}

type lifecycleDaemonOptions struct {
	stall        bool
	idleWake     bool
	resubMin     time.Duration
	snapshotWait time.Duration
}

func startLifecycleDaemon(t *testing.T, fake *lifecycleFakeHerdr, opts lifecycleDaemonOptions) *lifecycleDaemonFixture {
	t.Helper()
	store := NewMemoryStore(t)
	logs := &lockedLogBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	d := NewDaemon(Config{
		Store:         store,
		HerdrSocket:   fake.path,
		SchemaCommand: lifecycleSchemaCommand(t),
		Logger:        logger,
	})
	d.setCapabilities(GuardResult{Protocol: 22, Schema: 1, Events: true})
	d.lifecycleResubMin = opts.resubMin
	d.lifecycleSnapshotWait = opts.snapshotWait
	if opts.stall {
		// The same surface Start wires when stall detect is enabled: the
		// resubscribe channel the coverage check pulses, plus the manager.
		d.resub = make(chan struct{}, 1)
		manager, err := newStallDetectManager(store, t.TempDir(), StallDetectConfig{Enabled: true}, stallDeps{
			subscribed: func() (map[string]bool, time.Time) { return d.subscribedSet() },
			resubscribe: func() {
				select {
				case d.resub <- struct{}{}:
				default:
				}
			},
		}, logger)
		if err != nil {
			t.Fatal(err)
		}
		d.stall = manager
	}
	if opts.idleWake {
		manager, err := newIdleWakeManager(store, t.TempDir(), time.Minute,
			func(idleWakeRouteRequest) bool { return true },
			func(hubScannedRelayEvent) bool { return true }, logger)
		if err != nil {
			t.Fatal(err)
		}
		d.idleWake = manager
	}
	ctx, cancel := context.WithCancel(context.Background())
	go d.eventLoop(ctx)
	t.Cleanup(cancel)
	return &lifecycleDaemonFixture{daemon: d, store: store, logs: logs}
}

func lifecycleSubscribed(t *testing.T, d *Daemon) map[string]bool {
	t.Helper()
	set, _ := d.subscribedSet()
	if set == nil {
		return map[string]bool{}
	}
	return set
}

func subscriptionTypes(subs []map[string]any) map[string]int {
	types := map[string]int{}
	for _, sub := range subs {
		if kind, ok := sub["type"].(string); ok {
			types[kind]++
		}
	}
	return types
}

func hasPerPaneSubscription(subs []map[string]any, paneID string) bool {
	seen := map[string]bool{}
	for _, sub := range subs {
		if sub["pane_id"] != paneID {
			continue
		}
		switch sub["type"] {
		case "pane.agent_status_changed", "pane.output_matched", "pane.scroll_changed":
			seen[sub["type"].(string)] = true
		}
	}
	return len(seen) == 3
}

func lifecycleGlobalTypes() []string {
	return []string{"pane.created", "pane.closed", "pane.exited", "pane.agent_detected"}
}

// (a) The subscribe request carries the four global types plus the unchanged
// per-pane set; a host with no agents subscribes with the globals only.
func TestHerdrLifecycleSubscribeCarriesGlobalTypes(t *testing.T) {
	t.Run("with agents", func(t *testing.T) {
		fake := newLifecycleFakeHerdr(t, []map[string]any{lifecycleAgent("p1"), lifecycleAgent("p2")})
		client, err := newHerdrClient(fake.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		_, panes, err := client.Subscribe(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(panes) != 2 {
			t.Fatalf("covered panes=%v", panes)
		}
		eventually(t, 2*time.Second, "events.subscribe request", func() bool { return fake.subscribeCount() == 1 })
		subs := fake.subscribeParams()[0]
		types := subscriptionTypes(subs)
		for _, kind := range lifecycleGlobalTypes() {
			if types[kind] != 1 {
				t.Fatalf("subscription types=%v, want one %s", types, kind)
			}
		}
		for _, pane := range []string{"p1", "p2"} {
			if !hasPerPaneSubscription(subs, pane) {
				t.Fatalf("subscriptions missing per-pane set for %s: %v", pane, subs)
			}
		}
		if len(subs) != 4+6 {
			t.Fatalf("subscription count=%d, want 10: %v", len(subs), subs)
		}
	})
	t.Run("no agents", func(t *testing.T) {
		fake := newLifecycleFakeHerdr(t, nil)
		client, err := newHerdrClient(fake.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		_, panes, err := client.Subscribe(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(panes) != 0 {
			t.Fatalf("covered panes=%v", panes)
		}
		eventually(t, 2*time.Second, "events.subscribe request", func() bool { return fake.subscribeCount() == 1 })
		subs := fake.subscribeParams()[0]
		if len(subs) != 4 {
			t.Fatalf("subscriptions=%v, want the four globals only", subs)
		}
	})
}

// (b) An error reply to the globals is followed by one per-pane-only
// subscribe, logged once, and the daemon stays covered.
func TestHerdrLifecycleSubscribeFallsBackOnRejection(t *testing.T) {
	fake := newLifecycleFakeHerdr(t, []map[string]any{lifecycleAgent("p1")})
	fake.reject = 1
	fix := startLifecycleDaemon(t, fake, lifecycleDaemonOptions{})
	eventually(t, 3*time.Second, "fallback events.subscribe", func() bool { return fake.subscribeCount() == 2 })

	first, second := fake.subscribeParams()[0], fake.subscribeParams()[1]
	for _, kind := range lifecycleGlobalTypes() {
		if subscriptionTypes(first)[kind] != 1 {
			t.Fatalf("first subscribe missing %s: %v", kind, first)
		}
		if subscriptionTypes(second)[kind] != 0 {
			t.Fatalf("fallback subscribe still carries %s: %v", kind, second)
		}
	}
	if !hasPerPaneSubscription(second, "p1") {
		t.Fatalf("fallback subscribe missing per-pane set: %v", second)
	}
	eventually(t, 2*time.Second, "p1 covered after fallback", func() bool {
		return lifecycleSubscribed(t, fix.daemon)["p1"]
	})
	eventually(t, 2*time.Second, "rejection log", func() bool {
		return strings.Count(fix.logs.String(), "lifecycle subscriptions rejected") == 1
	})
	// The rejected attempt and the fallback are the whole story: no third
	// subscribe appears while the stream stays healthy.
	time.Sleep(300 * time.Millisecond) // prove nothing more happens
	if got := fake.subscribeCount(); got != 2 {
		t.Fatalf("subscribe count=%d after fallback, want 2", got)
	}
}

// (c) pane_agent_detected naming a live agent for an uncovered pane cycles
// the stream — with stall detect off and on — while pane_created, a null
// agent, and released detections never do.
func TestHerdrLifecycleAgentDetectedResubscribes(t *testing.T) {
	for _, stall := range []bool{false, true} {
		name := "stall off"
		if stall {
			name = "stall on"
		}
		t.Run(name, func(t *testing.T) {
			fake := newLifecycleFakeHerdr(t, []map[string]any{lifecycleAgent("p1")})
			fix := startLifecycleDaemon(t, fake, lifecycleDaemonOptions{stall: stall, resubMin: 300 * time.Millisecond})
			eventually(t, 3*time.Second, "initial subscribe", func() bool { return fake.subscribeCount() == 1 })

			fake.push(paneCreatedEnvelope("p2"))
			// pane_created is recorded with the id from data.pane.pane_id and
			// must not cycle the stream: herdr has not detected an agent yet.
			eventually(t, 2*time.Second, "pane_created event row", func() bool {
				var n int
				_ = fix.store.db.QueryRow(`SELECT count(*) FROM events WHERE event_kind='pane_created' AND pane_id='p2'`).Scan(&n)
				return n == 1
			})
			// Departures are not arrivals either: a null agent or a released
			// detection must not pull a cycle.
			fake.push(agentDetectedEnvelope("p3", "", false))
			fake.push(agentDetectedEnvelope("p4", "claude", true))
			time.Sleep(500 * time.Millisecond) // longer than the resub floor: any of these triggering would have landed
			if got := fake.subscribeCount(); got != 1 {
				t.Fatalf("pane_created or a departure detection caused a resubscribe: subscribe count=%d", got)
			}

			fake.setAgents([]map[string]any{lifecycleAgent("p1"), lifecycleAgent("p2")})
			fake.push(agentDetectedEnvelope("p2", "claude", false))
			eventually(t, 3*time.Second, "agent_detected resubscribe", func() bool { return fake.subscribeCount() == 2 })
			if !hasPerPaneSubscription(fake.subscribeParams()[1], "p2") {
				t.Fatalf("resubscribe missing per-pane set for p2: %v", fake.subscribeParams()[1])
			}
			eventually(t, 2*time.Second, "p2 covered", func() bool { return lifecycleSubscribed(t, fix.daemon)["p2"] })
		})
	}
}

// (d) A burst of detections causes one resubscribe; a trigger inside the
// interval after a cycle is deferred to the interval's end, never dropped.
func TestHerdrLifecycleResubscribeDebounce(t *testing.T) {
	fake := newLifecycleFakeHerdr(t, []map[string]any{lifecycleAgent("p1")})
	fix := startLifecycleDaemon(t, fake, lifecycleDaemonOptions{resubMin: 400 * time.Millisecond, snapshotWait: 50 * time.Millisecond})
	eventually(t, 3*time.Second, "initial subscribe", func() bool { return fake.subscribeCount() == 1 })

	// Ten detections inside the debounce window coalesce into one cycle.
	// Spaced just enough that a no-debounce mutant sees later events on the
	// next live stream; while the burst is in flight the panes stay unlisted
	// so every processed trigger still qualifies.
	burst := make([]map[string]any, 0, 11)
	burst = append(burst, lifecycleAgent("p1"))
	for i := 2; i <= 11; i++ {
		burst = append(burst, lifecycleAgent(fmt.Sprintf("p%d", i)))
		fake.push(agentDetectedEnvelope(fmt.Sprintf("p%d", i), "claude", false))
		time.Sleep(30 * time.Millisecond) // fixture pacing: spread the burst across the window
	}
	// Any detection that lands on the next stream after the debounced cycle
	// finds its pane already listed, so it cannot pull a second cycle no
	// matter how the fixture pacing stretches under load.
	fake.setAgents(burst)
	if !waitUntil(3*time.Second, func() bool { return fake.subscribeCount() == 2 }) {
		t.Fatalf("debounced resubscribe: subs=%d methods=%v logs=%s", fake.subscribeCount(), fake.methodCounts(), fix.logs.String())
	}
	if got := fake.subscribeCount(); got != 2 {
		t.Fatalf("burst of ten detections caused %d subscribes, want exactly one resubscribe", got-1)
	}

	// A trigger inside the interval after that cycle defers to the interval's
	// end and its pane still ends up covered. The push lands right after the
	// second subscribe is observed, well inside the 400ms floor.
	fake.setAgents([]map[string]any{lifecycleAgent("p1"), lifecycleAgent("p12")})
	fake.push(agentDetectedEnvelope("p12", "claude", false))
	eventually(t, 3*time.Second, "deferred resubscribe", func() bool { return fake.subscribeCount() == 3 })
	if !hasPerPaneSubscription(fake.subscribeParams()[2], "p12") {
		t.Fatalf("deferred resubscribe missing per-pane set for p12: %v", fake.subscribeParams()[2])
	}
	eventually(t, 2*time.Second, "p12 covered", func() bool { return lifecycleSubscribed(t, fix.daemon)["p12"] })

	// And nothing else cycles: the burst and the deferred trigger were the
	// whole story.
	time.Sleep(900 * time.Millisecond) // past interval + retry: stragglers would have cycled by now
	if got := fake.subscribeCount(); got != 3 {
		t.Fatalf("subscribe count=%d after debounce window, want 3", got)
	}
}

// (e) pane_closed and pane_exited drop the pane from the covered set without
// a resubscribe.
func TestHerdrLifecycleDepartureDropsCoveredPane(t *testing.T) {
	fake := newLifecycleFakeHerdr(t, []map[string]any{lifecycleAgent("p1"), lifecycleAgent("p2")})
	fix := startLifecycleDaemon(t, fake, lifecycleDaemonOptions{resubMin: 300 * time.Millisecond})
	eventually(t, 3*time.Second, "initial subscribe", func() bool { return fake.subscribeCount() == 1 })
	eventually(t, 2*time.Second, "initial covered set", func() bool {
		covered := lifecycleSubscribed(t, fix.daemon)
		return covered["p1"] && covered["p2"]
	})

	fake.push(paneDepartureEnvelope("pane.closed", "p2"))
	eventually(t, 2*time.Second, "p2 dropped from covered set", func() bool {
		covered := lifecycleSubscribed(t, fix.daemon)
		return !covered["p2"] && covered["p1"]
	})
	fake.push(paneDepartureEnvelope("pane.exited", "p1"))
	eventually(t, 2*time.Second, "p1 dropped from covered set", func() bool {
		return len(lifecycleSubscribed(t, fix.daemon)) == 0
	})
	time.Sleep(400 * time.Millisecond) // a departure must not cycle the stream
	if got := fake.subscribeCount(); got != 1 {
		t.Fatalf("departure caused a resubscribe: subscribe count=%d", got)
	}
}

// (f) Lifecycle envelopes log no "unknown herdr event" warning; a truly
// unknown field still does.
func TestHerdrLifecycleEventsAreKnown(t *testing.T) {
	fake := newLifecycleFakeHerdr(t, []map[string]any{lifecycleAgent("p1")})
	fix := startLifecycleDaemon(t, fake, lifecycleDaemonOptions{resubMin: time.Minute})
	eventually(t, 3*time.Second, "initial subscribe", func() bool { return fake.subscribeCount() == 1 })

	fake.push(paneCreatedEnvelope("p2"))
	fake.push(agentDetectedEnvelope("p2", "", false))
	fake.push(agentDetectedEnvelope("p3", "claude", true))
	fake.push(paneDepartureEnvelope("pane.closed", "p2"))
	fake.push(paneDepartureEnvelope("pane.exited", "p1"))
	for _, kind := range []string{"pane_created", "pane_agent_detected", "pane_agent_detected", "pane_closed", "pane_exited"} {
		want := 1
		if kind == "pane_agent_detected" {
			want = 2
		}
		eventually(t, 2*time.Second, kind+" event rows", func() bool { return fix.store.CountEventKind(kind) == want })
	}
	if strings.Contains(fix.logs.String(), "unknown herdr event") {
		t.Fatalf("lifecycle events logged unknown-event warnings: %s", fix.logs.String())
	}

	fake.push(map[string]any{"event": "pane_agent_status_changed", "data": map[string]any{
		"type": "pane_agent_status_changed", "pane_id": "p1", "workspace_id": "w1",
		"agent_status": "idle", "surprise_field": "fixture",
	}})
	eventually(t, 2*time.Second, "unknown-field warning", func() bool {
		return strings.Contains(fix.logs.String(), "unknown herdr event")
	})
}

// With idle-wake enabled, a burst of lifecycle events triggers one coalesced
// snapshot observation; the 5s poll is untouched.
func TestHerdrLifecycleEventTriggersIdleWakeSnapshot(t *testing.T) {
	fake := newLifecycleFakeHerdr(t, []map[string]any{lifecycleAgent("p1")})
	startLifecycleDaemon(t, fake, lifecycleDaemonOptions{idleWake: true, snapshotWait: 50 * time.Millisecond, resubMin: time.Minute})
	eventually(t, 3*time.Second, "initial subscribe", func() bool { return fake.subscribeCount() == 1 })
	// The post-subscribe snapshot is the first tab.list; each later snapshot
	// is exactly one more.
	eventually(t, 2*time.Second, "initial idle-wake snapshot", func() bool { return fake.methodCount("tab.list") == 1 })

	fake.push(paneCreatedEnvelope("p2"))
	fake.push(agentDetectedEnvelope("p2", "", false))
	fake.push(paneDepartureEnvelope("pane.closed", "pX"))
	fake.push(paneDepartureEnvelope("pane.exited", "pY"))
	fake.push(agentDetectedEnvelope("p3", "claude", true))
	eventually(t, 2*time.Second, "coalesced lifecycle snapshot", func() bool { return fake.methodCount("tab.list") == 2 })
	time.Sleep(300 * time.Millisecond) // well past the coalesce window, far inside the 5s poll
	if got := fake.methodCount("tab.list"); got != 2 {
		t.Fatalf("lifecycle burst produced %d snapshots, want 1 coalesced", got-1)
	}
}

// Decode pins the lifecycle payload shapes: pane_created's id lives at
// data.pane.pane_id, and agent_detected carries agent/released verbatim.
func TestHerdrLifecycleEventDecode(t *testing.T) {
	ev, ok := decodeHerdrEvent([]byte(`{"event":"pane_created","data":{"type":"pane_created","pane":{"pane_id":"p9","workspace_id":"w9","tab_id":"t9","terminal_id":"x","focused":false,"agent_status":"idle","revision":4}}}`))
	if !ok || ev.Kind != "pane_created" || ev.PaneID != "p9" || ev.WorkspaceID != "w9" || ev.AgentStatus != "" {
		t.Fatalf("pane_created decode=%+v ok=%v", ev, ok)
	}
	if string(ev.UnknownFields) != "{}" {
		t.Fatalf("pane_created unknown fields=%s", ev.UnknownFields)
	}

	ev, ok = decodeHerdrEvent([]byte(`{"event":"pane_agent_detected","data":{"type":"pane_agent_detected","pane_id":"p9","workspace_id":"w9","agent":"claude"}}`))
	if !ok || ev.PaneID != "p9" || string(ev.Agent) != `"claude"` || ev.Released {
		t.Fatalf("agent_detected decode=%+v ok=%v", ev, ok)
	}

	ev, ok = decodeHerdrEvent([]byte(`{"event":"pane_agent_detected","data":{"type":"pane_agent_detected","pane_id":"p9","workspace_id":"w9","agent":null,"released":true,"final_status":"done"}}`))
	if !ok || string(ev.Agent) != "null" || !ev.Released {
		t.Fatalf("departure decode=%+v ok=%v", ev, ok)
	}
	if string(ev.UnknownFields) != "{}" {
		t.Fatalf("departure unknown fields=%s", ev.UnknownFields)
	}

	for _, kind := range []string{"pane.closed", "pane.exited"} {
		ev, ok = decodeHerdrEvent([]byte(`{"event":"` + kind + `","data":{"type":"` + kind + `","pane_id":"p9","workspace_id":"w9"}}`))
		if !ok || ev.Kind != kind || ev.PaneID != "p9" {
			t.Fatalf("%s decode=%+v ok=%v", kind, ev, ok)
		}
	}

	// Dotted forms classify the same way.
	if !isPaneLifecycleEvent("pane.created") || !isPaneLifecycleEvent("pane_agent_detected") || isPaneLifecycleEvent("pane.output_matched") {
		t.Fatal("lifecycle kind classification wrong")
	}
}

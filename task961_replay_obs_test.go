package panewire

import (
	"bytes"
	"context"
	"encoding/json"
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

// #961: replay observability. The hub restart first-delivered rows that had
// been sitting undelivered for tens of minutes and nobody could see it — the
// feed announced only replay refusals, never a successful re-injection, and a
// stalled lane had no alarm at all.

const t961DirectorLane = `{"lanes":{"director-1":{"machine":"host-a","pane":"w1:p1"}}}`

func t961LaneRow(id int64, lane, eventID, receivedAt string, attempts int) handoffkeepRelayEvent {
	return handoffkeepRelayEvent{ID: id, Kind: "lane.event", JobID: laneEventTransportID(lane, eventID), Epoch: 1, OwnerLane: lane, EventID: eventID, Text: "[t961] note", Attempts: attempts, ReceivedAt: receivedAt}
}

type t961ReplayInjectedPayload struct {
	EventID    int64  `json:"event_id"`
	Kind       string `json:"kind"`
	Lane       string `json:"lane"`
	Machine    string `json:"machine"`
	Pane       string `json:"pane"`
	Attempts   int    `json:"attempts"`
	ReceivedAt string `json:"received_at"`
	Source     string `json:"source"`
}

func t961InjectedEvents(events func(string) []json.RawMessage) []t961ReplayInjectedPayload {
	var out []t961ReplayInjectedPayload
	for _, raw := range events("relay.replay_injected") {
		var payload t961ReplayInjectedPayload
		if json.Unmarshal(raw, &payload) != nil {
			continue
		}
		out = append(out, payload)
	}
	return out
}

// AC1a: a hub whose startup replay re-injects N undelivered lane.event rows
// announces exactly N relay.replay_injected events, each carrying the
// durable row id, the route it took, the post-bump attempt count, and
// source=startup.
func TestT961StartupReplayAnnouncesEachInjection(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, t961DirectorLane, client, nil)
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 8), persisted: make(chan hubRelayPersistedEvent, 8)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: agent}
	events := r20t5Subscribe(t, hub)
	received := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano)
	fake.seedUndelivered(
		t961LaneRow(11, "director-1", "t961-startup-a", received, 1),
		t961LaneRow(12, "director-1", "t961-startup-b", received, 0),
	)

	hub.replayUndeliveredRelayEvents(context.Background())

	injected := t961InjectedEvents(events)
	if len(injected) != 2 {
		t.Fatalf("relay.replay_injected broadcasts=%d, want 2", len(injected))
	}
	byID := map[int64]t961ReplayInjectedPayload{}
	for _, payload := range injected {
		byID[payload.EventID] = payload
	}
	for id, want := range map[int64]int{11: 2, 12: 1} {
		payload, found := byID[id]
		if !found {
			t.Fatalf("row %d had no relay.replay_injected; got %+v", id, injected)
		}
		if payload.Kind != "lane.event" || payload.Lane != "director-1" || payload.Machine != "host-a" || payload.Pane != "w1:p1" || payload.Source != "startup" || payload.ReceivedAt != received {
			t.Fatalf("row %d payload=%+v", id, payload)
		}
		if payload.Attempts != want {
			t.Fatalf("row %d attempts=%d, want %d (seed + the replay's own bump)", id, payload.Attempts, want)
		}
	}
	if got := drainRelays(agent); got != 2 {
		t.Fatalf("injected directives=%d, want 2", got)
	}
}

// AC1b: a row the startup replay could not route (destination node absent)
// is re-injected by the node-hello replay and announced with source=hello.
func TestT961HelloReplayAnnouncesEachInjection(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, t961DirectorLane, client, nil)
	received := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano)
	fake.seedUndelivered(
		t961LaneRow(21, "director-1", "t961-hello-a", received, 1),
		t961LaneRow(22, "director-1", "t961-hello-b", received, 1),
	)

	// Startup replay with the destination offline: unrouted, and crucially
	// no injection was queued so nothing is announced as injected.
	hub.replayUndeliveredRelayEvents(context.Background())
	events := r20t5Subscribe(t, hub)
	if got := len(t961InjectedEvents(events)); got != 0 {
		t.Fatalf("unrouted startup replay announced %d injections, want 0", got)
	}

	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 8), persisted: make(chan hubRelayPersistedEvent, 8)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: agent}
	hub.replayUndeliveredLaneEvents(context.Background())

	injected := t961InjectedEvents(events)
	if len(injected) != 2 {
		t.Fatalf("relay.replay_injected broadcasts=%d, want 2", len(injected))
	}
	for _, payload := range injected {
		if payload.Source != "hello" || payload.Attempts != 2 || (payload.EventID != 21 && payload.EventID != 22) {
			t.Fatalf("hello replay payload=%+v", payload)
		}
	}
	if got := drainRelays(agent); got != 2 {
		t.Fatalf("injected directives=%d, want 2", got)
	}
}

// AC1c: rows the replay refuses (retired, exhausted) or no longer lists
// (delivered) never produce relay.replay_injected — only their existing
// broadcasts.
func TestT961ReplayInjectedOnlyForQueuedInjections(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, t961DirectorLane, client, nil)
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 8), persisted: make(chan hubRelayPersistedEvent, 8)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: agent}
	events := r20t5Subscribe(t, hub)
	recent := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano)
	stale := time.Now().UTC().Add(-(relayReplayMaxAge + time.Hour)).Format(time.RFC3339Nano)
	fake.seedUndelivered(
		t961LaneRow(31, "director-1", "t961-stale", stale, 1),
		t961LaneRow(32, "director-1", "t961-exhausted", recent, relayReplayMaxAttempts),
		t961LaneRow(33, "director-1", "t961-delivered", recent, 1),
		t961LaneRow(34, "director-1", "t961-marker", recent, 1),
		t961LaneRow(35, "director-1", "t961-live", recent, 1),
	)
	// id 33 was delivered and id 34 was already retired before this process.
	fake.mu.Lock()
	fake.rows[fakeHandoffkeepLaneEventKey("director-1", "t961-delivered")].DeliveredAt = "2026-09-29T00:00:00Z"
	fake.rows[fakeHandoffkeepLaneEventKey("director-1", "t961-marker")].DeliveredTo = relayReplayRetiredMarker + "stale"
	fake.mu.Unlock()

	hub.replayUndeliveredRelayEvents(context.Background())

	injected := t961InjectedEvents(events)
	if len(injected) != 1 || injected[0].EventID != 35 || injected[0].Source != "startup" {
		t.Fatalf("relay.replay_injected=%+v, want exactly the live row 35", injected)
	}
	if got := len(events("relay.replay_retired")); got != 1 {
		t.Fatalf("relay.replay_retired=%d, want 1 (the stale row)", got)
	}
	if got := len(events("relay.replay_exhausted")); got != 1 {
		t.Fatalf("relay.replay_exhausted=%d, want 1 (the exhausted row)", got)
	}
	if got := drainRelays(agent); got != 1 {
		t.Fatalf("injected directives=%d, want 1", got)
	}
}

// AC2: --log-file tees the hub's log to a mode-0640 file without changing
// stderr by one byte and without ever writing a token value.
func TestT961HubLogFile(t *testing.T) {
	dir := t.TempDir()
	authPath := filepath.Join(dir, "hub-auth.env")
	if err := writeFile0600(authPath, "HUB_TOKEN_operator=t961-op-token\nHUB_TOKEN_host-a=t961-node-token\n"); err != nil {
		t.Fatal(err)
	}
	const token = "t961-handoffkeep-token-value"
	hkEnv := filepath.Join(dir, "handoffkeep.env")
	if err := writeFile0600(hkEnv, "HANDOFFKEEP_URL=http://127.0.0.1:18080\nHANDOFFKEEP_TOKEN="+token+"\n"); err != nil {
		t.Fatal(err)
	}
	// A fixed sequence is compared byte-for-byte, so the test clocks are
	// pinned by dropping the time attribute on both writers.
	dropTime := func(_ []string, attr slog.Attr) slog.Attr {
		if attr.Key == slog.TimeKey {
			return slog.Attr{}
		}
		return attr
	}
	sequence := func(hub *HubServer) {
		hub.logAuthorityLaneProtection()
		hub.logger.Info("t961 fixed info", "lane", "director-1")
		hub.logger.Warn("t961 fixed warn", "event_id", 42)
	}

	var stderrPlain bytes.Buffer
	hub, _, code, err := newHubServerForCLI([]string{"--hub-auth", authPath, "--handoffkeep-env", hkEnv, "--chat-desk-lane", "lane-a"}, slog.New(slog.NewTextHandler(&stderrPlain, &slog.HandlerOptions{ReplaceAttr: dropTime})))
	if err != nil || code != ExitOK {
		t.Fatalf("plain hub code=%d err=%v", code, err)
	}
	sequence(hub)

	var stderrTee bytes.Buffer
	logPath := filepath.Join(dir, "hub.log")
	hubTee, _, code, err := newHubServerForCLI([]string{"--hub-auth", authPath, "--handoffkeep-env", hkEnv, "--chat-desk-lane", "lane-a", "--log-file", logPath}, slog.New(slog.NewTextHandler(&stderrTee, &slog.HandlerOptions{ReplaceAttr: dropTime})))
	if err != nil || code != ExitOK {
		t.Fatalf("log-file hub code=%d err=%v", code, err)
	}
	sequence(hubTee)

	if stderrTee.String() != stderrPlain.String() {
		t.Fatalf("stderr changed with --log-file\nwithout:\n%s\nwith:\n%s", stderrPlain.String(), stderrTee.String())
	}
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("log file missing: %v", err)
	}
	if mode := info.Mode().Perm(); mode != 0o640 {
		t.Fatalf("log file mode=%o, want 0640", mode)
	}
	contents, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"authority-lane protection disabled", "t961 fixed info", "t961 fixed warn"} {
		if !strings.Contains(string(contents), want) {
			t.Fatalf("log file lacks %q:\n%s", want, contents)
		}
	}
	if strings.Contains(string(contents), token) {
		t.Fatal("handoffkeep token value appeared in the hub log file")
	}
}

// AC2b: an unusable --log-file path rejects hub startup instead of silently
// dropping the log copy.
func TestT961HubLogFileRejectsUnwritablePath(t *testing.T) {
	dir := t.TempDir()
	authPath := filepath.Join(dir, "hub-auth.env")
	if err := writeFile0600(authPath, "HUB_TOKEN_operator=t961-op-token\nHUB_TOKEN_host-a=t961-node-token\n"); err != nil {
		t.Fatal(err)
	}
	hub, _, code, err := newHubServerForCLI([]string{"--hub-auth", authPath, "--log-file", filepath.Join(dir, "absent-dir", "hub.log")}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if hub != nil || code != ExitConditionInvalid || err == nil {
		t.Fatalf("unwritable log file hub=%v code=%d err=%v", hub != nil, code, err)
	}
}

// t961StallHub builds a hub with a notifier and durable store for lane-stall
// tests. node records are installed directly, as the heartbeat fixtures do.
func t961StallHub(t *testing.T, lanes string, client *handoffkeepRelayClient, notifier HubNotifier) *HubServer {
	t.Helper()
	hub, err := NewHubServer(HubServerConfig{
		Tokens:          map[string]string{"operator": "op", "host-a": "node-a", "host-b": "node-b"},
		ReportRelayPath: r20LanesFile(t, lanes),
		Notifier:        notifier,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		handoffkeep:     client,
	})
	if err != nil {
		t.Fatal(err)
	}
	return hub
}

// t961ActiveNode registers a connected node holding the given active jobs, the
// heartbeat view sweepLaneStalls reads.
func t961ActiveNode(hub *HubServer, machine string, jobs ...HubActiveJob) {
	now := hub.now().UTC()
	active := make(map[string]HubActiveJob, len(jobs))
	for _, job := range jobs {
		active[job.JobID] = job
	}
	hub.mu.Lock()
	hub.nodes[machine] = &hubNodeRecord{machineID: machine, state: "connected", stateSince: now, lastPing: now, activeJobs: active}
	hub.mu.Unlock()
}

// t961Sweep runs one maintenance sweep and waits out the background
// lane-stall run it launched, restoring the synchronous shape assertions need.
func t961Sweep(hub *HubServer) {
	hub.Sweep()
	hub.laneStallSweeps.Wait()
}

func t961StallBroadcasts(events func(string) []json.RawMessage) []map[string]any {
	var out []map[string]any
	for _, raw := range events("relay.lane_stalled") {
		var payload map[string]any
		if json.Unmarshal(raw, &payload) != nil {
			continue
		}
		out = append(out, payload)
	}
	return out
}

// AC3: an undelivered row older than relayLaneStallAge on a lane with active
// work fires one dampened relay.lane_stalled alert and one broadcast naming
// the oldest row; delivering the row sends the recovery.
func TestT961LaneStallAlertsAndRecovers(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	notifier := &task198Notifier{}
	hub := t961StallHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"},"offload":{"sink":true}}}`, client, notifier)
	t961ActiveNode(hub, "host-a", HubActiveJob{JobID: "t961-job-1", OwnerLane: "lane-a"})
	received := time.Now().UTC().Add(-(relayLaneStallAge + time.Minute)).Format(time.RFC3339Nano)
	fake.seedUndelivered(t961LaneRow(41, "lane-a", "t961-stall-a", received, 1))
	events := r20t5Subscribe(t, hub)

	t961Sweep(hub) // first dampened observation
	if got := notifier.Accepted(); got != 0 {
		t.Fatalf("alert fired on the first observation (%d alerts)", got)
	}
	t961Sweep(hub) // second: the alert activates
	t961Sweep(hub) // the episode is already announced; a steady state repeats nothing

	alerts := notifier.Alerts()
	if len(alerts) != 1 {
		t.Fatalf("alerts=%+v, want exactly 1", alerts)
	}
	if alerts[0].Check != "relay.lane_stalled" || alerts[0].Recovery || alerts[0].Reason != "lane_stalled" {
		t.Fatalf("alert=%+v", alerts[0])
	}
	stalled := t961StallBroadcasts(events)
	if len(stalled) != 1 {
		t.Fatalf("relay.lane_stalled broadcasts=%d, want 1", len(stalled))
	}
	payload := stalled[0]
	if payload["lane"] != "lane-a" || payload["oldest_event_id"] != float64(41) || payload["oldest_received_at"] != received || payload["undelivered_count"] != float64(1) || payload["active_jobs"] != float64(1) {
		t.Fatalf("relay.lane_stalled payload=%v", payload)
	}

	// Delivering the row ends the episode after the same two observations.
	if err := client.markDelivered(context.Background(), 41, "host-a", "w1:p1"); err != nil {
		t.Fatal(err)
	}
	t961Sweep(hub)
	t961Sweep(hub)
	alerts = notifier.Alerts()
	if len(alerts) != 2 || !alerts[1].Recovery || alerts[1].Check != "relay.lane_stalled" {
		t.Fatalf("recovery alert missing: %+v", alerts)
	}
}

// AC3: a lane with no direct job still counts when one of its child lanes
// owns active work — the parent-of-an-owner shape resolveIdleWakeOwner uses.
func TestT961LaneStallCountsChildLaneWork(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	notifier := &task198Notifier{}
	hub := t961StallHub(t, `{"lanes":{"director-1":{"machine":"host-a","pane":"w1:p1"},"worker-lane":{"machine":"host-b","pane":"w1:p2","parent":"director-1"}}}`, client, notifier)
	t961ActiveNode(hub, "host-b", HubActiveJob{JobID: "t961-job-2", OwnerLane: "worker-lane"})
	fake.seedUndelivered(t961LaneRow(51, "director-1", "t961-stall-parent", time.Now().UTC().Add(-(relayLaneStallAge + time.Minute)).Format(time.RFC3339Nano), 1))

	t961Sweep(hub)
	t961Sweep(hub)
	alerts := notifier.Alerts()
	if len(alerts) != 1 || alerts[0].Check != "relay.lane_stalled" || alerts[0].MachineID != "director-1" {
		t.Fatalf("child-lane work did not stall the parent: %+v", alerts)
	}
}

// AC3: two stalled lanes are two independent episodes.
func TestT961LaneStallTwoLanesAlertSeparately(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	notifier := &task198Notifier{}
	hub := t961StallHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"},"lane-b":{"machine":"host-b","pane":"w1:p2"}}}`, client, notifier)
	t961ActiveNode(hub, "host-a", HubActiveJob{JobID: "t961-job-a", OwnerLane: "lane-a"})
	t961ActiveNode(hub, "host-b", HubActiveJob{JobID: "t961-job-b", OwnerLane: "lane-b"})
	old := time.Now().UTC().Add(-(relayLaneStallAge + time.Minute)).Format(time.RFC3339Nano)
	fake.seedUndelivered(
		t961LaneRow(61, "lane-a", "t961-stall-la", old, 1),
		t961LaneRow(62, "lane-b", "t961-stall-lb", old, 1),
	)
	events := r20t5Subscribe(t, hub)

	t961Sweep(hub)
	t961Sweep(hub)
	alerts := notifier.Alerts()
	if len(alerts) != 2 {
		t.Fatalf("alerts=%+v, want 2", alerts)
	}
	seen := map[string]bool{}
	for _, alert := range alerts {
		if alert.Check != "relay.lane_stalled" || alert.Recovery {
			t.Fatalf("alert=%+v", alert)
		}
		seen[alert.MachineID] = true
	}
	if !seen["lane-a"] || !seen["lane-b"] {
		t.Fatalf("lane-stall alerts did not name both lanes: %+v", alerts)
	}
	if got := len(t961StallBroadcasts(events)); got != 2 {
		t.Fatalf("relay.lane_stalled broadcasts=%d, want 2", got)
	}
}

// AC3: nothing announces while the row is young, the lane has no work, or the
// row is already settled (delivered, retired, or out of attempts).
func TestT961LaneStallNegativeCases(t *testing.T) {
	lanes := `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`
	old := time.Now().UTC().Add(-(relayLaneStallAge + time.Minute)).Format(time.RFC3339Nano)
	cases := []struct {
		name    string
		row     handoffkeepRelayEvent
		active  bool
		queries int // undelivered GETs expected across two sweeps
	}{
		{"row under the stall age", t961LaneRow(71, "lane-a", "t961-young", time.Now().UTC().Add(-(relayLaneStallAge - time.Minute)).Format(time.RFC3339Nano), 1), true, 2},
		{"lane without active work", t961LaneRow(72, "lane-a", "t961-idle", old, 1), false, 0},
		{"already retired row", func() handoffkeepRelayEvent {
			row := t961LaneRow(73, "lane-a", "t961-retired", old, 1)
			row.DeliveredTo = relayReplayRetiredMarker + "stale"
			return row
		}(), true, 2},
		{"attempts exhausted row", t961LaneRow(74, "lane-a", "t961-exhausted", old, relayReplayMaxAttempts), true, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake, client, closeServer := newFakeHandoffkeep(t)
			defer closeServer()
			notifier := &task198Notifier{}
			hub := t961StallHub(t, lanes, client, notifier)
			if tc.active {
				t961ActiveNode(hub, "host-a", HubActiveJob{JobID: "t961-job", OwnerLane: "lane-a"})
			} else {
				t961ActiveNode(hub, "host-a")
			}
			fake.seedUndelivered(tc.row)
			events := r20t5Subscribe(t, hub)
			t961Sweep(hub)
			t961Sweep(hub)
			if got := notifier.Accepted(); got != 0 {
				t.Fatalf("alerts=%d, want 0", got)
			}
			if got := len(t961StallBroadcasts(events)); got != 0 {
				t.Fatalf("relay.lane_stalled broadcasts=%d, want 0", got)
			}
			if got := len(fake.queries("/v1/relay/events")); got != tc.queries {
				t.Fatalf("undelivered queries=%d, want %d", got, tc.queries)
			}
		})
	}
}

// ---- adopted tester tests (round 1, t961_tester_test.go) -----------------
// These cover the invariants the first pass missed: every replay gate stays
// silent, one announce per row across startup+hello, read-error neutrality,
// file-mode tightening on pre-existing files, non-owned file rejection, tee
// WithAttrs/WithGroup fidelity, no secret reaching the log file, real lane
// names in alert text, and the concurrent race probe.

func t961tRecent() string { return time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano) }

func TestT961TesterGateMatrixNeverAnnounces(t *testing.T) {
	lanes := `{"lanes":{"director-1":{"machine":"host-a","pane":"w1:p1"},"offload":{"sink":true}}}`
	chatRow := func(id int64) handoffkeepRelayEvent {
		row := t961LaneRow(id, "director-1", "chat-7", t961tRecent(), 1)
		return row
	}
	stale := time.Now().UTC().Add(-(relayReplayMaxAge + time.Hour)).Format(time.RFC3339Nano)
	cases := []struct {
		name  string
		row   handoffkeepRelayEvent
		setup func(hub *HubServer)
		// agent: "ok" (buffered), "full" (unbuffered → queueRelay false), "none"
		agent string
	}{
		{"chat retire (store has no row)", chatRow(201), func(hub *HubServer) { hub.chatStore = newFakeChatStore() }, "ok"},
		{"chat defer (store error)", chatRow(202), func(hub *HubServer) {
			store := newFakeChatStore()
			store.err = errors.New("t961 tester store down")
			hub.chatStore = store
		}, "ok"},
		{"age gate (stale)", t961LaneRow(203, "director-1", "t961t-stale", stale, 1), nil, "ok"},
		{"exhausted", t961LaneRow(204, "director-1", "t961t-exh", t961tRecent(), relayReplayMaxAttempts), nil, "ok"},
		{"sink lane", t961LaneRow(205, "offload", "t961t-sink", t961tRecent(), 1), nil, "ok"},
		{"unknown lane", t961LaneRow(206, "no-such-lane", "t961t-unknown", t961tRecent(), 1), nil, "ok"},
		{"destination node offline", t961LaneRow(207, "director-1", "t961t-offline", t961tRecent(), 1), nil, "none"},
		{"lane.event enqueue fails", t961LaneRow(208, "director-1", "t961t-full", t961tRecent(), 1), nil, "full"},
		{"job.completed enqueue fails", handoffkeepRelayEvent{ID: 209, Kind: "job.completed", JobID: "t961t-job", Epoch: 1, OwnerLane: "director-1", ReportPath: "r.md", Attempts: 1, ReceivedAt: t961tRecent()}, nil, "full"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake, client, closeServer := newFakeHandoffkeep(t)
			defer closeServer()
			hub := r20Hub(t, lanes, client, io.Discard)
			var agent *hubAgent
			switch tc.agent {
			case "ok":
				agent = &hubAgent{relays: make(chan hubRelayInjectEvent, 8), persisted: make(chan hubRelayPersistedEvent, 8)}
			case "full":
				agent = &hubAgent{relays: make(chan hubRelayInjectEvent), persisted: make(chan hubRelayPersistedEvent, 8)}
			}
			if agent != nil {
				hub.nodes["host-a"] = &hubNodeRecord{agent: agent}
			}
			if tc.setup != nil {
				tc.setup(hub)
			}
			events := r20t5Subscribe(t, hub)
			fake.seedUndelivered(tc.row)
			hub.replayUndeliveredRelayEvents(context.Background())
			if got := t961InjectedEvents(events); len(got) != 0 {
				t.Fatalf("gate-stopped row was announced as injected: %+v", got)
			}
			if agent != nil {
				if got := drainRelays(agent); got != 0 {
					t.Fatalf("gate-stopped row queued %d directives", got)
				}
			}
		})
	}
}

// One injection is announced exactly once across startup + hello replays.
func TestT961TesterAnnouncedOnceAcrossStartupAndHello(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, t961DirectorLane, client, io.Discard)
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 8), persisted: make(chan hubRelayPersistedEvent, 8)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: agent}
	events := r20t5Subscribe(t, hub)
	fake.seedUndelivered(
		t961LaneRow(301, "director-1", "t961t-once", t961tRecent(), 0),
		handoffkeepRelayEvent{ID: 302, Kind: "job.completed", JobID: "t961t-job-ok", Epoch: 1, OwnerLane: "director-1", ReportPath: "r.md", Attempts: 0, ReceivedAt: t961tRecent()},
	)
	hub.replayUndeliveredRelayEvents(context.Background())
	hub.replayUndeliveredLaneEvents(context.Background())
	hub.replayUndeliveredLaneEvents(context.Background())
	injected := t961InjectedEvents(events)
	if len(injected) != 2 {
		t.Fatalf("relay.replay_injected=%+v, want exactly 2 (one per row)", injected)
	}
	for _, payload := range injected {
		if payload.Source != "startup" || payload.Attempts != 1 {
			t.Fatalf("payload=%+v, want source=startup attempts=1", payload)
		}
	}
	if got := drainRelays(agent); got != 2 {
		t.Fatalf("directives=%d, want 2", got)
	}
}

type t961tFlakyHK struct {
	fake *fakeHandoffkeep
	fail atomic.Bool
}

func (f *t961tFlakyHK) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if f.fail.Load() && r.Method == http.MethodGet {
		http.Error(w, "down", http.StatusServiceUnavailable)
		return
	}
	f.fake.ServeHTTP(w, r)
}

func t961tFlakyHandoffkeep(t *testing.T) (*t961tFlakyHK, *handoffkeepRelayClient) {
	t.Helper()
	flaky := &t961tFlakyHK{fake: &fakeHandoffkeep{nextID: 100, status: http.StatusCreated, rows: map[string]*handoffkeepRelayEvent{}}}
	server := httptest.NewServer(flaky)
	t.Cleanup(server.Close)
	client, err := newHandoffkeepRelayClient(hubHandoffkeepEnv{URL: server.URL, Token: "test-token"}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	return flaky, client
}

func TestT961TesterStallReadErrorNeitherOpensNorCloses(t *testing.T) {
	flaky, client := t961tFlakyHandoffkeep(t)
	notifier := &task198Notifier{}
	hub := t961StallHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"},"offload":{"sink":true}}}`, client, notifier)
	t961ActiveNode(hub, "host-a", HubActiveJob{JobID: "t961t-j1", OwnerLane: "lane-a"}, HubActiveJob{JobID: "t961t-j2", OwnerLane: "offload"})
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 8), persisted: make(chan hubRelayPersistedEvent, 8)}
	hub.mu.Lock()
	hub.nodes["host-a"].agent = agent
	hub.nodes["host-a"].lastKeepaliveSent = time.Now().Add(24 * time.Hour) // fixture has no websocket
	hub.mu.Unlock()
	old := time.Now().UTC().Add(-(relayLaneStallAge + time.Minute)).Format(time.RFC3339Nano)
	flaky.fake.seedUndelivered(t961LaneRow(401, "lane-a", "t961t-stall", old, 1), t961LaneRow(402, "offload", "t961t-sink", old, 1))
	events := r20t5Subscribe(t, hub)

	// A: reads fail from the start — no episode may open.
	flaky.fail.Store(true)
	for i := 0; i < 3; i++ {
		t961Sweep(hub)
	}
	if got := notifier.Accepted(); got != 0 {
		t.Fatalf("read errors opened an episode: %+v", notifier.Alerts())
	}
	// B: reads recover — episode opens after the dampened two observations.
	flaky.fail.Store(false)
	t961Sweep(hub)
	t961Sweep(hub)
	if alerts := notifier.Alerts(); len(alerts) != 1 || alerts[0].Recovery || alerts[0].MachineID != "lane-a" {
		t.Fatalf("alerts after recovery of reads=%+v, want one lane-a incident", alerts)
	}
	// C: row delivered but reads fail — the error must not close the episode.
	if err := client.markDelivered(context.Background(), 401, "host-a", "w1:p1"); err != nil {
		t.Fatal(err)
	}
	flaky.fail.Store(true)
	for i := 0; i < 3; i++ {
		t961Sweep(hub)
	}
	if alerts := notifier.Alerts(); len(alerts) != 1 {
		t.Fatalf("a handoffkeep read error ended the episode: %+v", alerts)
	}
	// D: reads recover — the clear is real now and recovery is sent.
	flaky.fail.Store(false)
	t961Sweep(hub)
	t961Sweep(hub)
	if alerts := notifier.Alerts(); len(alerts) != 2 || !alerts[1].Recovery {
		t.Fatalf("recovery missing after real clears: %+v", alerts)
	}
	// The sink lane never alerted, only one stall broadcast, and the alarm
	// never wrote to handoffkeep or queued a directive.
	if got := len(t961StallBroadcasts(events)); got != 1 {
		t.Fatalf("relay.lane_stalled broadcasts=%d, want 1", got)
	}
	for _, q := range flaky.fake.queries("/v1/relay/events") {
		if strings.Contains(q, "lane=offload") {
			t.Fatalf("sink lane was queried: %s", q)
		}
	}
	flaky.fake.mu.Lock()
	for _, call := range flaky.fake.calls {
		if call.Method != http.MethodGet && !(call.Method == http.MethodPost && strings.HasSuffix(call.Path, "/delivered")) {
			flaky.fake.mu.Unlock()
			t.Fatalf("stall sweep wrote to handoffkeep: %+v", call)
		}
	}
	postDelivered := 0
	for _, call := range flaky.fake.calls {
		if call.Method == http.MethodPost {
			postDelivered++
		}
	}
	flaky.fake.mu.Unlock()
	if postDelivered != 1 { // only the test's own markDelivered
		t.Fatalf("POSTs=%d, want 1 (the test's own markDelivered)", postDelivered)
	}
	if got := drainRelays(agent); got != 0 {
		t.Fatalf("stall sweep queued %d directives", got)
	}
}

func TestT961TesterStallOnePerEpisodeManySweeps(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	notifier := &task198Notifier{}
	hub := t961StallHub(t, `{"lanes":{"director-1":{"machine":"host-a","pane":"w1:p1"}}}`, client, notifier)
	t961ActiveNode(hub, "host-a", HubActiveJob{JobID: "t961t-j", OwnerLane: "director-1"})
	old := time.Now().UTC().Add(-(relayLaneStallAge + time.Minute)).Format(time.RFC3339Nano)
	fake.seedUndelivered(t961LaneRow(501, "director-1", "t961t-ep1", old, 1))
	events := r20t5Subscribe(t, hub)
	for i := 0; i < 10; i++ {
		t961Sweep(hub)
	}
	if alerts := notifier.Alerts(); len(alerts) != 1 {
		t.Fatalf("10 sweeps of one stall gave alerts=%+v, want 1", alerts)
	}
	if got := len(t961StallBroadcasts(events)); got != 1 {
		t.Fatalf("10 sweeps of one stall gave %d relay.lane_stalled broadcasts, want 1", got)
	}
	if err := client.markDelivered(context.Background(), 501, "host-a", "w1:p1"); err != nil {
		t.Fatal(err)
	}
	t961Sweep(hub)
	t961Sweep(hub)
	t961Sweep(hub)
	// A second, distinct stall is a second episode.
	fake.seedUndelivered(t961LaneRow(502, "director-1", "t961t-ep2", old, 1))
	for i := 0; i < 5; i++ {
		t961Sweep(hub)
	}
	alerts := notifier.Alerts()
	if len(alerts) != 3 || alerts[0].Recovery || !alerts[1].Recovery || alerts[2].Recovery {
		t.Fatalf("episodes=%+v, want incident, recovery, incident", alerts)
	}
	if got := len(t961StallBroadcasts(events)); got != 2 {
		t.Fatalf("relay.lane_stalled broadcasts=%d, want 2 (one per episode)", got)
	}
}

func t961tAuth(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "hub-auth.env")
	if err := writeFile0600(path, "HUB_TOKEN_operator=t961tester-OPTOKEN-aaaa\nHUB_TOKEN_host-a=t961tester-NODETOKEN-bbbb\n"); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestT961TesterLogFileTightensExistingWideFile(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "hub.log")
	if err := os.WriteFile(logPath, []byte("pre-existing line\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(logPath, 0o666); err != nil {
		t.Fatal(err)
	}
	hub, _, code, err := newHubServerForCLI([]string{"--hub-auth", t961tAuth(t, dir), "--log-file", logPath}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil || code != ExitOK {
		t.Fatalf("code=%d err=%v", code, err)
	}
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode != 0o640 {
		t.Fatalf("pre-existing 0666 log file left at mode=%o, want 0640", mode)
	}
	hub.logger.Info("t961 tester appended")
	contents, _ := os.ReadFile(logPath)
	if !strings.HasPrefix(string(contents), "pre-existing line\n") || !strings.Contains(string(contents), "t961 tester appended") {
		t.Fatalf("log file was not appended to:\n%s", contents)
	}
	// Close releases the file exactly once and is idempotent.
	file := hub.logFile
	if file == nil {
		t.Fatal("hub does not own the log file")
	}
	if err := hub.Close(); err != nil {
		t.Fatalf("Close=%v", err)
	}
	if _, err := file.Write([]byte("x")); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("file still open after Close: %v", err)
	}
	if err := hub.Close(); err != nil {
		t.Fatalf("second Close=%v", err)
	}
}

// A file the hub process can write but does not own (e.g. root-owned
// /dev/null, crw-rw-rw-) cannot be chmod'ed: startup is rejected. This is the
// shape the runbook's Option B would produce if the file were not owned by
// the documented unprivileged account.
func TestT961TesterLogFileNotOwnedRejectsStartup(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root owns everything")
	}
	dir := t.TempDir()
	hub, _, code, err := newHubServerForCLI([]string{"--hub-auth", t961tAuth(t, dir), "--log-file", "/dev/null"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if hub != nil || code != ExitConditionInvalid || err == nil {
		t.Fatalf("non-owned writable log file: hub=%v code=%d err=%v", hub != nil, code, err)
	}
	// Group-read-only file (install -m 0640 for a non-owner in the group):
	// modelled as owner-read-only here.
	ro := filepath.Join(dir, "ro.log")
	if err := os.WriteFile(ro, nil, 0o440); err != nil {
		t.Fatal(err)
	}
	hub, _, code, err = newHubServerForCLI([]string{"--hub-auth", t961tAuth(t, dir), "--log-file", ro}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if hub != nil || code != ExitConditionInvalid || err == nil {
		t.Fatalf("read-only log file: hub=%v code=%d err=%v", hub != nil, code, err)
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("startup error does not name the class: %v", err)
	}
}

func TestT961TesterTeeWithAttrsWithGroupByteIdentical(t *testing.T) {
	dropTime := func(_ []string, attr slog.Attr) slog.Attr {
		if attr.Key == slog.TimeKey {
			return slog.Attr{}
		}
		return attr
	}
	for _, level := range []slog.Level{slog.LevelDebug, slog.LevelWarn} {
		var plain, teed, file bytes.Buffer
		opts := &slog.HandlerOptions{Level: level, ReplaceAttr: dropTime}
		plainLogger := slog.New(slog.NewTextHandler(&plain, opts))
		teeLogger := slog.New(hubLogTeeHandler{primary: slog.NewTextHandler(&teed, opts), file: slog.NewTextHandler(&file, &slog.HandlerOptions{ReplaceAttr: dropTime})})
		for _, logger := range []*slog.Logger{plainLogger, teeLogger} {
			l := logger.With("component", "hub").WithGroup("relay").With("lane", "director-1")
			l.Debug("dbg", "k", 1)
			l.Info("info", "k", 2)
			l.WithGroup("inner").Warn("warn", "k", 3)
			logger.Error("top", "k", 4)
		}
		if plain.String() != teed.String() {
			t.Fatalf("level=%v stderr differs\nplain:\n%s\ntee:\n%s", level, plain.String(), teed.String())
		}
		if !strings.Contains(file.String(), `component=hub relay.lane=director-1 relay.inner.k=3`) {
			t.Fatalf("file lost attrs/groups:\n%s", file.String())
		}
		if strings.Contains(file.String(), "dbg") {
			t.Fatalf("file carried a Debug record:\n%s", file.String())
		}
	}
}

// No token, Authorization value, CF-Access value, cookie or relay payload body
// reaches the log file across the hub's real code paths.
func TestT961TesterNoSecretReachesLogFile(t *testing.T) {
	sentinels := map[string]string{
		"operator token":       "t961tester-OPTOKEN-aaaa",
		"node token":           "t961tester-NODETOKEN-bbbb",
		"handoffkeep token":    "t961tester-HKTOKEN-cccc",
		"wrong bearer":         "t961tester-WRONGBEARER-dddd",
		"cf access jwt":        "t961tester-CFJWT-eeee",
		"cookie":               "t961tester-COOKIE-ffff",
		"relay payload body":   "t961tester-PAYLOAD-gggg",
		"replayed row body":    "t961tester-REPLAYBODY-hhhh",
		"http ingress label":   "t961tester-LABEL-iiii",
		"cf certs url secret?": "t961tester-CERTSQ-jjjj",
	}
	delete(sentinels, "http ingress label")   // label is an operator-supplied display name, not a secret
	delete(sentinels, "cf certs url secret?") // certs URL is configuration, not a secret
	fake, hkClient, closeFake := newFakeHandoffkeep(t)
	defer closeFake()
	certs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "no", http.StatusInternalServerError) }))
	defer certs.Close()

	dir := t.TempDir()
	hkEnv := filepath.Join(dir, "handoffkeep.env")
	if err := writeFile0600(hkEnv, "HANDOFFKEEP_URL=http://127.0.0.1:18080\nHANDOFFKEEP_TOKEN="+sentinels["handoffkeep token"]+"\n"); err != nil {
		t.Fatal(err)
	}
	lanesPath := r20LanesFile(t, `{"lanes":{"director-1":{"machine":"host-a","pane":"w1:p1"}}}`)
	logPath := filepath.Join(dir, "hub.log")
	hub, _, code, err := newHubServerForCLI([]string{
		"--hub-auth", t961tAuth(t, dir), "--handoffkeep-env", hkEnv, "--chat-desk-lane", "director-1", "--lanes", lanesPath, "--log-file", logPath,
		"--cf-access-team", "t961tester", "--cf-access-aud", "t961tester-aud", "--cf-access-certs-url", certs.URL + "/certs",
	}, slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})))
	if err != nil || code != ExitOK {
		t.Fatalf("code=%d err=%v", code, err)
	}
	defer hub.Close()
	// The env file's URL is a dead address so its tokens stay purely file-side
	// fixtures; the live paths below run against the in-memory fake.
	hub.handoffkeep = hkClient
	handler := hub.Handler()
	send := func(method, path, bearer, body string) int {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		req.Header.Set("Cf-Access-Jwt-Assertion", sentinels["cf access jwt"])
		req.Header.Set("Cookie", "CF_Authorization="+sentinels["cookie"])
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec.Code
	}
	ingress := `{"kind":"lane.event","lane":"director-1","event_id":"t961t-ev-%d","text":"` + sentinels["relay payload body"] + `","label":"t961t","host":"mac-personal"}`
	for i, bearer := range []string{sentinels["wrong bearer"], sentinels["node token"], sentinels["operator token"]} {
		send(http.MethodPost, "/v1/relay/events", bearer, strings.Replace(ingress, "%d", string(rune('a'+i)), 1))
		send(http.MethodGet, "/v1/nodes", bearer, "")
		send(http.MethodGet, "/ui/data.json", bearer, "")
		send(http.MethodGet, "/v1/lanes", bearer, "")
	}
	// Persist failure path (logs status) with the payload still in flight.
	fake.mu.Lock()
	fake.status = http.StatusInternalServerError
	fake.mu.Unlock()
	send(http.MethodPost, "/v1/relay/events", sentinels["operator token"], strings.Replace(ingress, "%d", "z", 1))
	fake.mu.Lock()
	fake.status = http.StatusCreated
	fake.mu.Unlock()
	// Startup + hello replay of rows whose body is a sentinel, then a stall.
	old := time.Now().UTC().Add(-(relayLaneStallAge + time.Minute)).Format(time.RFC3339Nano)
	row := t961LaneRow(601, "director-1", "t961t-replay", old, 0)
	row.Text = sentinels["replayed row body"]
	fake.seedUndelivered(row)
	hub.replayUndeliveredRelayEvents(context.Background()) // node offline: unrouted
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 8), persisted: make(chan hubRelayPersistedEvent, 8)}
	t961ActiveNode(hub, "host-a", HubActiveJob{JobID: "t961t-j", OwnerLane: "director-1"})
	hub.mu.Lock()
	hub.nodes["host-a"].agent = agent
	hub.nodes["host-a"].lastKeepaliveSent = time.Now().Add(24 * time.Hour) // fixture has no websocket
	hub.mu.Unlock()
	hub.replayUndeliveredLaneEvents(context.Background()) // injected + announced
	fake.seedUndelivered(func() handoffkeepRelayEvent {
		r := t961LaneRow(602, "director-1", "t961t-stall", old, 1)
		r.Text = sentinels["replayed row body"]
		return r
	}())
	t961Sweep(hub)
	t961Sweep(hub)
	time.Sleep(200 * time.Millisecond) // let the CF certs refresh failure log

	contents, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	log := string(contents)
	for _, want := range []string{"relay replay injected", "relay lane stalled", "relay event was not persisted", "Cloudflare Access key refresh failed"} {
		if !strings.Contains(log, want) {
			t.Fatalf("path not exercised (%q missing):\n%s", want, log)
		}
	}
	for name, value := range sentinels {
		if strings.Contains(log, value) {
			t.Fatalf("%s reached the log file:\n%s", name, log)
		}
	}
	t.Logf("log file (%d lines) carries no sentinel", strings.Count(log, "\n"))
}

func TestT961TesterAlertTextNamesRealLanes(t *testing.T) {
	for _, lane := range []string{"director-1", "b953-fleet-tooling", "work-kairos", "operator-desk", "installer-180", "work-operator-desk", "work-director-1", "b900-investor-flow-json"} {
		for _, recovery := range []bool{false, true} {
			alert := HubAlert{Recovery: recovery, MachineID: hubAlertMachineID("lane-stall:" + lane), Reason: hubAlertReasonLaneStalled, Check: "relay.lane_stalled"}
			text := formatHubAlert(alert)
			if !strings.Contains(text, "machine: "+lane+"\n") || strings.Contains(text, "unknown") || strings.Contains(text, "REBOOT") {
				t.Fatalf("lane %q recovery=%v renders:\n%s", lane, recovery, text)
			}
		}
	}
	// Characterization: a label-shaped lane (hubAgentLabelPattern allows upper
	// case and ':') renders unknown. Lane names in the fleet are
	// machineIDPattern-shaped, so this documents the limit rather than gates.
	if text := formatHubAlert(HubAlert{MachineID: hubAlertMachineID("lane-stall:Director:1"), Reason: hubAlertReasonLaneStalled, Check: "relay.lane_stalled"}); !strings.Contains(text, "machine: unknown") {
		t.Fatalf("label-shaped lane renders:\n%s", text)
	}
}

// Race probe: the background stall sweep against concurrent hello replays,
// operator reads, and alert-state readers.
func TestT961TesterStallSweepConcurrentRace(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	notifier := &task198Notifier{}
	hub := t961StallHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"},"lane-b":{"machine":"host-b","pane":"w1:p2","parent":"lane-a"}}}`, client, notifier)
	t961ActiveNode(hub, "host-b", HubActiveJob{JobID: "t961t-r", OwnerLane: "lane-b"})
	old := time.Now().UTC().Add(-(relayLaneStallAge + time.Minute)).Format(time.RFC3339Nano)
	fake.seedUndelivered(t961LaneRow(701, "lane-a", "t961t-race", old, 1))
	handler := hub.Handler()
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 15; i++ {
				switch g {
				case 0:
					hub.Sweep()
				case 1:
					hub.replayUndeliveredLaneEvents(context.Background())
				case 2:
					req := httptest.NewRequest(http.MethodGet, "/ui/data.json", nil)
					req.Header.Set("Authorization", "Bearer op")
					handler.ServeHTTP(httptest.NewRecorder(), req)
				case 3:
					t961ActiveNode(hub, "host-b", HubActiveJob{JobID: "t961t-r", OwnerLane: "lane-b"})
				}
			}
		}(g)
	}
	wg.Wait()
	// Concurrent sweeps may have skipped each other's launches; two waited
	// sweeps make the observation count deterministic before asserting.
	t961Sweep(hub)
	t961Sweep(hub)
	if got := notifier.Accepted(); got != 1 {
		t.Fatalf("alerts=%d under concurrency, want 1: %+v", got, notifier.Alerts())
	}
}

// ---- fix-round-1: the sweep must not sit in front of failover -------------

// A handoffkeep fake whose GETs block until released. started is closed the
// first time a request reaches the server, so a test can prove a stall run is
// genuinely in flight before asserting on Sweep behavior.
func t961tBlockedHandoffkeep(t *testing.T) (*fakeHandoffkeep, *handoffkeepRelayClient, func(), <-chan struct{}) {
	t.Helper()
	fake, client, closeServer := newFakeHandoffkeep(t)
	release := make(chan struct{})
	started := make(chan struct{})
	var releaseOnce, startedOnce sync.Once
	fake.mu.Lock()
	fake.observe = func(method, _ string) {
		if method == http.MethodGet {
			startedOnce.Do(func() { close(started) })
			<-release
		}
	}
	fake.mu.Unlock()
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock) // never leave the httptest server hung on cleanup
	t.Cleanup(closeServer)
	return fake, client, unblock, started
}

// The characterization test inverted: a stall run blocked on handoffkeep must
// not delay the node-down alert or failover emitted by the same sweep.
func TestT961TesterStallSweepDoesNotDelayNodeAlert(t *testing.T) {
	fake, client, unblock, started := t961tBlockedHandoffkeep(t)
	defer unblock()
	notifier := &task198Notifier{}
	hub := t961StallHub(t, `{"lanes":{"l1":{"machine":"host-a","pane":"w1:p1"},"l2":{"machine":"host-a","pane":"w1:p2"}}}`, client, notifier)
	t961ActiveNode(hub, "host-a", HubActiveJob{JobID: "j1", OwnerLane: "l1"}, HubActiveJob{JobID: "j2", OwnerLane: "l2"})
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 8), persisted: make(chan hubRelayPersistedEvent, 8), failovers: make(chan hubFailoverEvent, 8)}
	past := hub.now().UTC().Add(-time.Hour)
	hub.mu.Lock()
	hub.nodes["host-a"].agent = agent
	hub.nodes["host-a"].lastKeepaliveSent = time.Now().Add(24 * time.Hour)
	hub.nodes["host-z"] = &hubNodeRecord{machineID: "host-z", state: "disconnected", stateSince: past, lastPing: past}
	hub.mu.Unlock()
	fake.seedUndelivered(t961LaneRow(801, "l1", "t961t-hung", past.Format(time.RFC3339Nano), 1))

	hub.Sweep() // host-z's first observation; the stall run blocks on its GET
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the stall run never reached handoffkeep")
	}
	start := time.Now()
	hub.Sweep() // run in flight → skipped; host-z's second observation + dispatch must not wait
	elapsed := time.Since(start)
	if elapsed > 2*time.Second {
		t.Fatalf("Sweep blocked %v behind a hung handoffkeep read", elapsed)
	}
	var alert bool
	for _, a := range notifier.Alerts() {
		if a.MachineID == "host-z" && !a.Recovery {
			alert = true
		}
	}
	if !alert {
		t.Fatalf("host-z down alert missing after the sweep: %+v", notifier.Alerts())
	}
	var failover bool
	for {
		select {
		case ev := <-agent.failovers:
			if ev.Machine == "host-z" && ev.Phase == hubFailoverPhaseDown {
				failover = true
			}
			continue
		default:
		}
		break
	}
	if !failover {
		t.Fatal("host-z failover down was not emitted in the same sweep")
	}
	unblock()
	hub.laneStallSweeps.Wait()
}

// Two sweeps while a stall run is in flight must not start a second run.
func TestT961LaneStallSweepSingleFlight(t *testing.T) {
	fake, client, unblock, started := t961tBlockedHandoffkeep(t)
	defer unblock()
	notifier := &task198Notifier{}
	hub := t961StallHub(t, `{"lanes":{"l1":{"machine":"host-a","pane":"w1:p1"},"l2":{"machine":"host-a","pane":"w1:p2"}}}`, client, notifier)
	t961ActiveNode(hub, "host-a", HubActiveJob{JobID: "j1", OwnerLane: "l1"})
	hub.Sweep() // starts the one run; it blocks on its first undelivered GET
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the stall run never reached handoffkeep")
	}
	hub.Sweep() // finds it in flight; must not launch a second
	unblock()
	hub.laneStallSweeps.Wait()
	// One run reads each lane with active work once (a single page); l2 has
	// no active jobs and is never queried. A second run, wrongly launched,
	// would double the count.
	if got := len(fake.queries("/v1/relay/events")); got != 1 {
		t.Fatalf("undelivered GETs=%d, want 1 (one lane with work, one run)", got)
	}
}

// Close waits out an in-flight stall run so it can finish logging.
func TestT961LaneStallCloseWaitsForRun(t *testing.T) {
	_, client, unblock, started := t961tBlockedHandoffkeep(t)
	defer unblock()
	notifier := &task198Notifier{}
	hub := t961StallHub(t, `{"lanes":{"l1":{"machine":"host-a","pane":"w1:p1"}}}`, client, notifier)
	t961ActiveNode(hub, "host-a", HubActiveJob{JobID: "j1", OwnerLane: "l1"})
	hub.Sweep() // launches the run; it blocks on the undelivered GET
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the stall run never reached handoffkeep")
	}
	closed := make(chan struct{})
	go func() {
		_ = hub.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close returned while the stall run was still blocked")
	case <-time.After(200 * time.Millisecond):
	}
	unblock()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the stall run finished")
	}
}

// A run that exhausts its budget mid-observation is a non-observation: it can
// neither open a new episode nor end an open one.
func TestT961LaneStallBudgetLeavesEpisodesNeutral(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	notifier := &task198Notifier{}
	hub := t961StallHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, client, notifier)
	t961ActiveNode(hub, "host-a", HubActiveJob{JobID: "t961-budget", OwnerLane: "lane-a"})
	old := time.Now().UTC().Add(-(relayLaneStallAge + time.Minute)).Format(time.RFC3339Nano)
	fake.seedUndelivered(t961LaneRow(91, "lane-a", "t961-budget", old, 1))

	// An already-spent budget evaluates no lane: no alert state is touched.
	spent, cancel := context.WithCancel(context.Background())
	cancel()
	hub.sweepLaneStalls(spent, hub.now().UTC())
	if got := notifier.Accepted(); got != 0 {
		t.Fatalf("exhausted budget opened an episode: %+v", notifier.Alerts())
	}
	hub.mu.Lock()
	_, episode := hub.alerts["lane-stall:lane-a"]
	hub.mu.Unlock()
	if episode {
		t.Fatal("exhausted budget touched lane-stall alert state")
	}

	// Open the episode for real, then spend the budget mid-read: the open
	// episode must survive unobserved sweeps exactly like a read error.
	t961Sweep(hub)
	t961Sweep(hub)
	if alerts := notifier.Alerts(); len(alerts) != 1 || alerts[0].Recovery {
		t.Fatalf("episode did not open: %+v", alerts)
	}
	// The fake reads observe once under its own mutex, so gate the slowdown
	// behind an atomic rather than swapping the hook with requests in flight.
	var slow atomic.Bool
	fake.mu.Lock()
	fake.observe = func(method, _ string) {
		if slow.Load() && method == http.MethodGet {
			time.Sleep(100 * time.Millisecond)
		}
	}
	fake.mu.Unlock()
	slow.Store(true)
	tight, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	hub.sweepLaneStalls(tight, hub.now().UTC())
	hub.sweepLaneStalls(tight, hub.now().UTC())
	if alerts := notifier.Alerts(); len(alerts) != 1 {
		t.Fatalf("budget exhaustion ended the episode: %+v", alerts)
	}
	slow.Store(false)
	time.Sleep(150 * time.Millisecond) // let the abandoned handlers drain
	if err := client.markDelivered(context.Background(), 91, "host-a", "w1:p1"); err != nil {
		t.Fatal(err)
	}
	t961Sweep(hub)
	t961Sweep(hub)
	if alerts := notifier.Alerts(); len(alerts) != 2 || !alerts[1].Recovery {
		t.Fatalf("recovery missing after real clears: %+v", alerts)
	}
}

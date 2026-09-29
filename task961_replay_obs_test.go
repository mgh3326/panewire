package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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

	hub.Sweep() // first dampened observation
	if got := notifier.Accepted(); got != 0 {
		t.Fatalf("alert fired on the first observation (%d alerts)", got)
	}
	hub.Sweep() // second: the alert activates
	hub.Sweep() // the episode is already announced; a steady state repeats nothing

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
	hub.Sweep()
	hub.Sweep()
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

	hub.Sweep()
	hub.Sweep()
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

	hub.Sweep()
	hub.Sweep()
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
			hub.Sweep()
			hub.Sweep()
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

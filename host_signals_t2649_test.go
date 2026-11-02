package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func TestHostSignalsPowerRoundTrips(t *testing.T) {
	for _, power := range []string{powerSourceAC, powerSourceBattery} {
		payload, err := json.Marshal(hubHeartbeatPayload{Status: "alive", Checks: map[string]HubCheckStatus{}, HostSignals: &HubHostSignals{PowerSource: power}})
		if err != nil {
			t.Fatal(err)
		}
		heartbeat, ok := decodeHubHeartbeatPayload(payload)
		if !ok || heartbeat.HostSignals == nil || heartbeat.HostSignals.PowerSource != power {
			t.Fatalf("%s: heartbeat=%+v ok=%t payload=%s", power, heartbeat, ok, payload)
		}
	}
}

// A bad or newer signal costs that signal, never the node's liveness.
func TestHostSignalsDecodeIsLenient(t *testing.T) {
	for name, tc := range map[string]struct {
		signals string
		want    string
	}{
		"newer signal beside power": {`{"power_source":"ac","cpu_busy_short":0.4}`, powerSourceAC},
		"only a newer signal":       {`{"cpu_busy_short":0.4}`, ""},
		"invalid power":             {`{"power_source":"solar"}`, ""},
		"power of the wrong type":   {`{"power_source":1}`, ""},
		"not an object":             {`"ac"`, ""},
		"null":                      {`null`, ""},
	} {
		heartbeat, ok := decodeHubHeartbeatPayload([]byte(`{"status":"alive","host_signals":` + tc.signals + `}`))
		if !ok {
			t.Errorf("%s: the heartbeat was rejected", name)
			continue
		}
		got := ""
		if heartbeat.HostSignals != nil {
			got = heartbeat.HostSignals.PowerSource
		}
		if got != tc.want {
			t.Errorf("%s: power=%q want %q", name, got, tc.want)
		}
	}
}

func TestParsePmsetPowerSource(t *testing.T) {
	for output, want := range map[string]string{
		"Now drawing from 'AC Power'\n -InternalBattery-0 (id=1)\t80%; AC attached; not charging present: true\n": powerSourceAC,
		"Now drawing from 'AC Power'\n": powerSourceAC, // a desktop Mac has no battery line
		"Now drawing from 'Battery Power'\n -InternalBattery-0 (id=1)\t80%; discharging; 4:00 remaining present: true\n": powerSourceBattery,
		"Now drawing from 'UPS Power'\n": powerSourceBattery,
		"":                               "",
		"something else\nNow drawing from 'AC Power'\n": "",
	} {
		if got := parsePmsetPowerSource(output); got != want {
			t.Errorf("%q: got %q want %q", output, got, want)
		}
	}
}

func TestCollectDarwinHostSignalsFallsBackOnPath(t *testing.T) {
	var calls [][]string
	run := func(_ context.Context, argv ...string) ([]byte, error) {
		calls = append(calls, append([]string(nil), argv...))
		if argv[0] == "/usr/bin/pmset" {
			return nil, errors.New("not there")
		}
		return []byte("Now drawing from 'Battery Power'\n"), nil
	}
	signals := collectDarwinHostSignals(t.Context(), run)
	if signals == nil || signals.PowerSource != powerSourceBattery {
		t.Fatalf("signals=%+v", signals)
	}
	if want := [][]string{{"/usr/bin/pmset", "-g", "batt"}, {"pmset", "-g", "batt"}}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls=%v want=%v", calls, want)
	}
	failing := func(context.Context, ...string) ([]byte, error) { return nil, errors.New("no pmset") }
	if signals := collectDarwinHostSignals(t.Context(), failing); signals != nil {
		t.Fatalf("a failed read reported %+v", signals)
	}
}

// The handshake: a node sends host_signals only after this connection's hub
// advertised them, because an older hub rejects the whole heartbeat otherwise.
func TestNodeSendsHostSignalsOnlyToAHubThatAdvertisedThem(t *testing.T) {
	client := &HubClient{
		jobsInboxRoot: t.TempDir(), assignedJobs: map[string]uint64{}, completedJobs: map[string]uint64{}, completedReports: map[string]struct{}{},
		hostLoadCollector:    func(context.Context) (HubHostLoad, error) { return HubHostLoad{}, errors.New("unavailable") },
		hostMemoryCollector:  func(context.Context) (*HubHostMemory, error) { return nil, errors.New("unavailable") },
		hostSignalsCollector: func(context.Context) *HubHostSignals { return &HubHostSignals{PowerSource: powerSourceAC} },
	}
	if payload := client.heartbeatEvent(t.Context()).Payload; strings.Contains(string(payload), "host_signals") {
		t.Fatalf("sent before the hub advertised them: %s", payload)
	}
	message, ok := parseHubOutbound([]byte(`{"type":"hub.features","features":["host_signals","newer"],"newer_field":1}`))
	if !ok || !reflect.DeepEqual(message.Features, []string{"host_signals", "newer"}) {
		t.Fatalf("message=%+v ok=%t", message, ok)
	}
	advertised := &hubClientConnection{}
	advertised.acceptsSignals.Store(true)
	client.signalsPeer.Store(advertised)
	if payload := client.heartbeatEvent(t.Context()).Payload; !strings.Contains(string(payload), `"host_signals":{"power_source":"ac"}`) {
		t.Fatalf("not sent after the hub advertised them: %s", payload)
	}
	// A late reader of the old connection can only touch that connection's flag.
	next := &hubClientConnection{}
	client.signalsPeer.Store(next)
	advertised.acceptsSignals.Store(true)
	if payload := client.heartbeatEvent(t.Context()).Payload; strings.Contains(string(payload), "host_signals") {
		t.Fatalf("the old connection's flag reached the new one: %s", payload)
	}
}

// End to end over a real socket: a new hub advertises, a new node then reports,
// and the hub records the report.
func TestHostSignalsHandshakeEndToEnd(t *testing.T) {
	hub, server := fsHub(t, "")
	fsRunNode(t, server.URL, func(client *HubClient) {
		client.hostSignalsCollector = func(context.Context) *HubHostSignals { return &HubHostSignals{PowerSource: powerSourceBattery} }
	})
	eventually(t, 10*time.Second, "power report reached the hub", func() bool {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		record := hub.nodes["node-a"]
		return record != nil && record.powerSource == powerSourceBattery && !record.powerAt.IsZero()
	})
}

// A node that fails over from a new hub to an old one must stop sending
// host_signals at once: the old hub would reject every heartbeat.
func TestNodeForgetsHostSignalsOnNewConnection(t *testing.T) {
	var mu sync.Mutex
	connections := 0
	var afterReconnect []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer func() { _ = connection.CloseNow() }()
		mu.Lock()
		connections++
		current := connections
		mu.Unlock()
		ctx := request.Context()
		if current == 1 {
			_ = wsjson.Write(ctx, connection, hubFeaturesEvent{Type: "hub.features", Features: []string{hubFeatureHostSignals}})
		}
		for {
			var message map[string]json.RawMessage
			if wsjson.Read(ctx, connection, &message) != nil {
				return
			}
			var kind string
			_ = json.Unmarshal(message["type"], &kind)
			switch kind {
			case "ping":
				_ = wsjson.Write(ctx, connection, map[string]string{"type": "pong"})
			case "event":
				var eventKind string
				_ = json.Unmarshal(message["kind"], &eventKind)
				if eventKind != "heartbeat" {
					continue
				}
				if current == 1 {
					if strings.Contains(string(message["payload"]), "host_signals") {
						return // the node reported: drop it, as a failover would
					}
					continue
				}
				mu.Lock()
				afterReconnect = append(afterReconnect, string(message["payload"]))
				mu.Unlock()
			}
		}
	}))
	t.Cleanup(server.Close)
	fsRunNode(t, server.URL, func(client *HubClient) {
		client.hostSignalsCollector = func(context.Context) *HubHostSignals { return &HubHostSignals{PowerSource: powerSourceAC} }
		client.hostLoadCollector = func(context.Context) (HubHostLoad, error) { return HubHostLoad{}, errors.New("unavailable") }
		client.hostMemoryCollector = func(context.Context) (*HubHostMemory, error) { return nil, errors.New("unavailable") }
	})
	eventually(t, 30*time.Second, "heartbeats on the second connection", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(afterReconnect) >= 2
	})
	mu.Lock()
	defer mu.Unlock()
	for _, payload := range afterReconnect {
		if strings.Contains(payload, "host_signals") {
			t.Fatalf("sent host_signals to a hub that never advertised them: %s", payload)
		}
	}
}

// A hub from before this change never advertises, so the node never sends the
// field that hub would reject. The frozen vocabulary models that hub's decoder.
func TestNewNodeHeartbeatIsAcceptedByAnOldHub(t *testing.T) {
	client := &HubClient{
		jobsInboxRoot: t.TempDir(), assignedJobs: map[string]uint64{}, completedJobs: map[string]uint64{}, completedReports: map[string]struct{}{},
		hostLoadCollector:    func(context.Context) (HubHostLoad, error) { return HubHostLoad{}, errors.New("unavailable") },
		hostMemoryCollector:  func(context.Context) (*HubHostMemory, error) { return nil, errors.New("unavailable") },
		hostSignalsCollector: func(context.Context) *HubHostSignals { return &HubHostSignals{PowerSource: powerSourceAC} },
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(client.heartbeatEvent(t.Context()).Payload, &fields); err != nil {
		t.Fatal(err)
	}
	oldHub := map[string]bool{"status": true, "checks": true, "host_load": true, "load_error": true, "host_memory": true, "quota": true, "active_jobs": true, "holds_active": true, "sessions": true, "snapshot_status": true, "truncated": true, "stall_detect": true}
	for name := range fields {
		if !oldHub[name] {
			t.Errorf("an old hub would reject the heartbeat for %q", name)
		}
	}
}

// An older node drops hub.features: its parser has no such type, and its read
// loop continues past an unparsable message.
func TestHubFeaturesEventShape(t *testing.T) {
	encoded, err := json.Marshal(hubFeaturesEvent{Type: "hub.features", Features: []string{hubFeatureHostSignals}})
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"type":"hub.features","features":["host_signals"]}` {
		t.Fatalf("encoded=%s", encoded)
	}
}

func powerTestHub(t *testing.T, laptops ...string) (*HubServer, PlacementPolicy, time.Time) {
	t.Helper()
	hub, _ := fsHub(t, "")
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	policy := DefaultPlacementPolicy()
	policy.LocalMachine, policy.SpillTargets, policy.Laptops = "mac-work", []string{"desktop"}, laptops
	free := 80.0
	for _, machine := range []string{"mac-work", "desktop"} {
		hub.connect(machine, "test", "fixture", &hubAgent{}, true)
		record := hub.nodes[machine]
		record.connectedSince = now.Add(-time.Hour)
		record.hostMemory = &HubHostMemory{FreePct: &free, Source: "proc_meminfo"}
	}
	return hub, policy, now
}

func powerReason(t *testing.T, hub *HubServer, policy PlacementPolicy, now time.Time, machine string) PlacementCandidate {
	t.Helper()
	for _, candidate := range hub.placementCandidates(policy, placementMetrics{}, "hub-only", now) {
		if candidate.Machine == machine {
			return candidate
		}
	}
	t.Fatalf("no candidate for %s", machine)
	return PlacementCandidate{}
}

// Acceptance 4.
func TestPlacementPowerRefusal(t *testing.T) {
	for _, tc := range []struct {
		name      string
		laptops   []string
		machine   string
		power     string
		reportAge time.Duration
		want      string // the reason text; "" is eligible
	}{
		{name: "laptop on battery is refused", laptops: []string{"mac-work"}, machine: "mac-work", power: powerSourceBattery, want: "not_accepting(on_battery)"},
		{name: "laptop on AC is eligible", laptops: []string{"mac-work"}, machine: "mac-work", power: powerSourceAC},
		{name: "laptop that never reported is refused", laptops: []string{"mac-work"}, machine: "mac-work", want: "not_accepting(power_unknown)"},
		{name: "laptop with a stale report is refused", laptops: []string{"mac-work"}, machine: "mac-work", power: powerSourceAC, reportAge: 3*time.Minute + time.Second, want: "not_accepting(power_unknown)"},
		{name: "laptop at the limit is eligible", laptops: []string{"mac-work"}, machine: "mac-work", power: powerSourceAC, reportAge: 3 * time.Minute},
		{name: "stale battery report reads as unknown", laptops: []string{"mac-work"}, machine: "mac-work", power: powerSourceBattery, reportAge: time.Hour, want: "not_accepting(power_unknown)"},
		{name: "Linux host without a report is unaffected", laptops: []string{"mac-work"}, machine: "desktop"},
		{name: "unlisted host on battery is unaffected", laptops: []string{"mac-work"}, machine: "desktop", power: powerSourceBattery},
		{name: "no laptops listed means no refusal", machine: "mac-work", power: powerSourceBattery},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hub, policy, now := powerTestHub(t, tc.laptops...)
			if tc.power != "" {
				record := hub.nodes[tc.machine]
				record.powerSource, record.powerAt = tc.power, now.Add(-tc.reportAge)
			}
			candidate := powerReason(t, hub, policy, now, tc.machine)
			if tc.want == "" {
				if strings.Contains(candidate.Reason, "not_accepting") || strings.Contains(candidate.Reason, "power") || strings.Contains(candidate.Reason, "battery") || !candidate.accepting {
					t.Fatalf("candidate=%+v", candidate)
				}
				return
			}
			if !strings.Contains(candidate.Reason, tc.want) || candidate.accepting {
				t.Fatalf("reason=%q accepting=%t, want %q", candidate.Reason, candidate.accepting, tc.want)
			}
			if placementUsable(candidate, policy, "hub-only") {
				t.Fatal("a refused host is usable")
			}
		})
	}
}

// A node gets one report window from connect, so a restart is not a refusal.
func TestPlacementPowerGraceAfterConnect(t *testing.T) {
	hub, policy, now := powerTestHub(t, "mac-work")
	record := hub.nodes["mac-work"]
	record.connectedSince = now.Add(-2 * time.Minute)
	if candidate := powerReason(t, hub, policy, now, "mac-work"); strings.Contains(candidate.Reason, "power_unknown") {
		t.Fatalf("refused inside the grace: %q", candidate.Reason)
	}
	record.connectedSince = now.Add(-3*time.Minute - time.Second)
	if candidate := powerReason(t, hub, policy, now, "mac-work"); !strings.Contains(candidate.Reason, "power_unknown") {
		t.Fatalf("not refused after the grace: %q", candidate.Reason)
	}
}

// The refusal changes the decision, and a disconnected laptop stays disconnected
// rather than gaining a power reason.
func TestPlacementPowerRefusalSpillsAndSkipsDisconnected(t *testing.T) {
	hub, policy, now := powerTestHub(t, "mac-work")
	record := hub.nodes["mac-work"]
	record.powerSource, record.powerAt = powerSourceBattery, now
	if result := hub.makePlacement(policy, placementMetrics{}, "hub-only", now); result.Decision != "desktop" {
		t.Fatalf("decision=%q, want desktop", result.Decision)
	}
	record.agent = nil
	if candidate := powerReason(t, hub, policy, now, "mac-work"); strings.Contains(candidate.Reason, "battery") {
		t.Fatalf("a disconnected host got a power reason: %q", candidate.Reason)
	}
}

func TestPlacementPolicyLaptopsParse(t *testing.T) {
	policy, err := ParsePlacementPolicy([]byte(`{"local_machine":"mac-work","spill_targets":["desktop"],"max_active_jobs":5,"laptops":["mac-work"]}`))
	if err != nil || !reflect.DeepEqual(policy.Laptops, []string{"mac-work"}) {
		t.Fatalf("policy=%+v err=%v", policy, err)
	}
	for _, laptops := range []string{`["BAD NAME"]`, `["` + hubOperatorMachineID + `"]`} {
		if _, err := ParsePlacementPolicy([]byte(`{"local_machine":"mac-work","max_active_jobs":5,"laptops":` + laptops + `}`)); err == nil {
			t.Errorf("laptops %s were accepted", laptops)
		}
	}
}

// A changed power source drops the cached placement answer.
func TestPowerChangeInvalidatesPlacementCache(t *testing.T) {
	hub, _, now := powerTestHub(t, "mac-work")
	var logs bytes.Buffer
	hub.logger = slog.New(slog.NewTextHandler(&logs, nil))
	record := hub.nodes["mac-work"]
	hub.placementCache = placementCache{key: "cached", at: now}
	hub.observeHostSignalsLocked(record, &HubHostSignals{PowerSource: powerSourceAC}, now)
	if hub.placementCache.key != "" {
		t.Fatal("first report kept the cache")
	}
	hub.placementCache = placementCache{key: "cached", at: now}
	hub.observeHostSignalsLocked(record, &HubHostSignals{PowerSource: powerSourceAC}, now.Add(time.Second))
	if hub.placementCache.key != "cached" || !record.powerAt.Equal(now.Add(time.Second)) {
		t.Fatalf("unchanged report: cache=%q powerAt=%v", hub.placementCache.key, record.powerAt)
	}
	hub.observeHostSignalsLocked(record, &HubHostSignals{PowerSource: powerSourceBattery}, now)
	if hub.placementCache.key != "" {
		t.Fatal("a changed source kept the cache")
	}
	// A report that ends a stale period clears a cached power_unknown, even when
	// the source is unchanged.
	hub.placementCache = placementCache{key: "cached", at: now}
	hub.observeHostSignalsLocked(record, &HubHostSignals{PowerSource: powerSourceBattery}, now.Add(placementPowerReportMaxAge))
	if hub.placementCache.key != "cached" {
		t.Fatal("a report at the limit dropped the cache")
	}
	hub.observeHostSignalsLocked(record, &HubHostSignals{PowerSource: powerSourceBattery}, now.Add(2*placementPowerReportMaxAge+time.Second))
	if hub.placementCache.key != "" {
		t.Fatal("a report ending a stale period kept the cache")
	}
	// The log line is the rollout's confirmation: first report and each change.
	if got := strings.Count(logs.String(), `msg="host power source"`); got != 2 {
		t.Fatalf("logged %d power lines, want 2: %s", got, logs.String())
	}
	if !strings.Contains(logs.String(), "machine=mac-work power_source=battery") {
		t.Fatalf("log lacks the machine and source: %s", logs.String())
	}
}

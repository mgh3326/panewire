package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The heartbeat names a node may send to a hub before hubFeatureHostActivity
// (the names decodeHubHeartbeatPayload and its host_memory check accepted then).
// A key outside these lists would make that hub reject the whole heartbeat.
var baseHeartbeatNames = []string{"status", "checks", "host_load", "load_error", "host_memory", "quota", "active_jobs", "holds_active", "host_signals", "readiness", "sessions", "snapshot_status", "truncated", "stall_detect", "pane_ids", "pane_ids_complete"}
var baseHostMemoryNames = []string{"free_pct", "compressed_mb", "swap_used_mb", "psi_some_avg10", "source", "swap_in_per_s", "pressure"}

// (b) The probe parses an ioreg IOHIDSystem layout: HIDIdleTime nanoseconds to
// whole seconds. The fixture follows that layout; a macOS capture should replace
// it before this ships to an operator Mac.
func TestHostActivityProbeParsesIOHIDIdleFixture(t *testing.T) {
	output, err := os.ReadFile(filepath.Join("testdata", "host_activity", "macos-ioreg-iohidsystem.txt"))
	if err != nil {
		t.Fatal(err)
	}
	var argv []string
	idle, err := collectUserIdle(t.Context(), "darwin", func(_ context.Context, args ...string) ([]byte, error) {
		argv = args
		return output, nil
	})
	if err != nil || idle != 4215 {
		t.Fatalf("idle=%d err=%v; want 4215 whole seconds from 4215000000000 ns", idle, err)
	}
	if len(argv) != 3 || argv[1] != "-c" || argv[2] != "IOHIDSystem" {
		t.Fatalf("probe ran %v; it may run only ioreg -c IOHIDSystem", argv)
	}
	if seconds, ok := parseHIDIdleSeconds("\"HIDIdleTime\" = 999999999"); !ok || seconds != 0 {
		t.Fatalf("sub-second idle=%d ok=%t; want 0 whole seconds", seconds, ok)
	}
}

// (b) The probe is absent on Linux, and when the switch is off it never runs.
func TestHostActivityIsAbsentOnLinuxAndWhenSwitchIsOff(t *testing.T) {
	ran := false
	_, err := collectUserIdle(t.Context(), "linux", func(context.Context, ...string) ([]byte, error) {
		ran = true
		return nil, nil
	})
	if !errors.Is(err, errUserIdleUnsupported) || ran {
		t.Fatalf("linux probe err=%v ran=%t; want unsupported without running a command", err, ran)
	}

	client := &HubClient{userIdleSwitch: func() bool { return false }, userIdleProbe: func(context.Context) (int64, error) {
		t.Fatal("the probe ran with the switch off")
		return 0, nil
	}}
	client.signalsPeer.Store(acceptingPeer())
	if got := client.hostActivityForHeartbeat(t.Context()); got != nil {
		t.Fatalf("switch off still reported host activity: %+v", got)
	}

	// The switch on but the hub without the feature: nothing is sent or probed.
	client = &HubClient{userIdleSwitch: func() bool { return true }, userIdleProbe: func(context.Context) (int64, error) {
		t.Fatal("the probe ran for a hub without host_activity")
		return 0, nil
	}}
	client.signalsPeer.Store(&hubClientConnection{})
	if got := client.hostActivityForHeartbeat(t.Context()); got != nil {
		t.Fatalf("hub without host_activity still received it: %+v", got)
	}
}

// (b) A failed probe omits the field and logs at most once an hour, and the line
// carries the condition only, never command output.
func TestHostActivityFailureLogsAtMostOncePerHour(t *testing.T) {
	var warnings []string
	now := time.Unix(1_000_000, 0)
	client := &HubClient{
		userIdleSwitch: func() bool { return true },
		userIdleProbe:  func(context.Context) (int64, error) { return 0, errors.New("ioreg unavailable") },
		warn:           func(message string) { warnings = append(warnings, message) },
		now:            func() time.Time { return now },
	}
	client.signalsPeer.Store(acceptingPeer())
	for _, after := range []time.Duration{0, 10 * time.Minute, 59 * time.Minute, time.Hour + time.Minute} {
		now = time.Unix(1_000_000, 0).Add(after)
		if got := client.hostActivityForHeartbeat(t.Context()); got != nil {
			t.Fatalf("a failed probe reported host activity: %+v", got)
		}
	}
	if len(warnings) != 2 || warnings[0] != "user idle unavailable: ioreg unavailable" {
		t.Fatalf("warnings=%q; want one line at start and one after an hour", warnings)
	}
}

// Test (b): an object or field a newer node adds, or a bad value, costs only
// itself. Only user_idle_s is stored: the object carries no other field.
func TestHostActivityDecodeIsLenient(t *testing.T) {
	for name, tc := range map[string]struct {
		activity string
		want     *int64
	}{
		"seconds":           {`{"user_idle_s":42}`, ptrInt64(42)},
		"newer name beside": {`{"user_idle_s":42,"app":"Safari"}`, ptrInt64(42)},
		"negative":          {`{"user_idle_s":-1}`, nil},
		"fraction":          {`{"user_idle_s":12.5}`, nil},
		"null":              {`{"user_idle_s":null}`, nil},
		"wrong type":        {`{"user_idle_s":"42"}`, nil},
		"absent":            {`{}`, nil},
		"not an object":     {`42`, nil},
	} {
		t.Run(name, func(t *testing.T) {
			heartbeat, ok := decodeHubHeartbeatPayload([]byte(`{"status":"alive","host_activity":` + tc.activity + `}`))
			if !ok {
				t.Fatal("the heartbeat was rejected")
			}
			switch {
			case tc.want == nil && heartbeat.HostActivity != nil && heartbeat.HostActivity.UserIdleS != nil:
				t.Fatalf("stored %d for %s", *heartbeat.HostActivity.UserIdleS, tc.activity)
			case tc.want != nil && (heartbeat.HostActivity == nil || heartbeat.HostActivity.UserIdleS == nil || *heartbeat.HostActivity.UserIdleS != *tc.want):
				t.Fatalf("heartbeat activity=%+v; want %d", heartbeat.HostActivity, *tc.want)
			}
		})
	}
}

// (f) An old hub never receives a name it does not know. With the feature
// missing, the node drops host_activity and host_memory.swap_out_per_s, and the
// rest of the heartbeat keeps the base names.
func TestHostActivityWireIsGatedByHubFeature(t *testing.T) {
	client, err := NewHubClient(HubClientConfig{
		URL:                   "ws://fixture.invalid",
		MachineID:             "node-a",
		Token:                 r6NodeAToken,
		JobsInboxRoot:         t.TempDir(),
		AllowInsecureForTests: true,
		hostLoadCollector:     func(context.Context) (HubHostLoad, error) { return HubHostLoad{}, nil },
		hostMemoryCollector: func(context.Context) (*HubHostMemory, error) {
			return &HubHostMemory{FreePct: memoryFloat(50), Source: "proc_meminfo", SwapInPerS: memoryFloat(1), SwapOutPerS: memoryFloat(3)}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	client.userIdleSwitch = func() bool { return true }
	client.userIdleProbe = func(context.Context) (int64, error) { return 42, nil }

	client.signalsPeer.Store(&hubClientConnection{})
	oldHub := decodedHeartbeatFields(t, client.heartbeatEvent(t.Context()).Payload)
	assertBaseNames(t, oldHub)
	if _, exists := oldHub["host_activity"]; exists {
		t.Fatal("a hub without host_activity received it")
	}

	client.signalsPeer.Store(acceptingPeer())
	newHub := decodedHeartbeatFields(t, client.heartbeatEvent(t.Context()).Payload)
	if _, exists := newHub["host_activity"]; !exists {
		t.Fatal("a hub with host_activity did not receive the object")
	}
	var memory map[string]json.RawMessage
	if err := json.Unmarshal(newHub["host_memory"], &memory); err != nil || string(memory["swap_out_per_s"]) != "3" {
		t.Fatalf("host_memory=%s err=%v; want swap_out_per_s 3", newHub["host_memory"], err)
	}
	heartbeat, ok := decodeHubHeartbeatPayload(client.heartbeatEvent(t.Context()).Payload)
	if !ok || heartbeat.HostActivity == nil || *heartbeat.HostActivity.UserIdleS != 42 || heartbeat.HostMemory.SwapOutPerS == nil {
		t.Fatalf("new hub decode lost the new fields: %+v ok=%t", heartbeat, ok)
	}
}

// (f) A node that predates host_activity still gives a new hub a valid heartbeat
// with no new fields.
func TestNewHubAcceptsOlderNodeHeartbeat(t *testing.T) {
	heartbeat, ok := decodeHubHeartbeatPayload([]byte(`{"status":"alive","host_memory":{"free_pct":50,"compressed_mb":null,"swap_used_mb":0,"psi_some_avg10":null,"swap_in_per_s":0,"pressure":0.1,"source":"proc_meminfo"}}`))
	if !ok || heartbeat.HostActivity != nil || heartbeat.HostMemory == nil || heartbeat.HostMemory.SwapOutPerS != nil {
		t.Fatalf("older node heartbeat=%+v ok=%t", heartbeat, ok)
	}
}

func acceptingPeer() *hubClientConnection {
	peer := &hubClientConnection{}
	peer.acceptsHostActivity.Store(true)
	return peer
}

func decodedHeartbeatFields(t *testing.T, payload []byte) map[string]json.RawMessage {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	return fields
}

func assertBaseNames(t *testing.T, fields map[string]json.RawMessage) {
	t.Helper()
	for name := range fields {
		if !hasName(baseHeartbeatNames, name) {
			t.Fatalf("heartbeat name %q is unknown to a hub before host_activity", name)
		}
	}
	var memory map[string]json.RawMessage
	if raw, ok := fields["host_memory"]; ok && json.Unmarshal(raw, &memory) == nil {
		for name := range memory {
			if !hasName(baseHostMemoryNames, name) {
				t.Fatalf("host_memory name %q is unknown to a hub before host_activity", name)
			}
		}
	}
}

func hasName(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func ptrInt64(value int64) *int64 { return &value }

// (f) The hub advertises host_activity in its features event, which is what lets
// a node send the object at all.
func TestHubAdvertisesHostActivityFeature(t *testing.T) {
	agent := &hubAgent{features: make(chan hubFeaturesEvent, 1)}
	agent.queueFeatures()
	select {
	case event := <-agent.features:
		if !hasName(event.Features, hubFeatureHostActivity) {
			t.Fatalf("hub features=%v; host_activity missing", event.Features)
		}
	default:
		t.Fatal("the hub queued no features event")
	}
}

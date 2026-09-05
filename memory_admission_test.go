package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHubHostMemoryJSONAndHeartbeatCompatibility(t *testing.T) {
	missing := hubHeartbeatPayload{Status: "alive", Checks: map[string]HubCheckStatus{"service": HubCheckOK}, HostMemory: &HubHostMemory{Source: "proc_meminfo"}}
	encoded, err := json.Marshal(missing)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"free_pct":null`) || !strings.Contains(string(encoded), `"compressed_mb":null`) || !strings.Contains(string(encoded), `"swap_used_mb":null`) || !strings.Contains(string(encoded), `"psi_some_avg10":null`) {
		t.Fatalf("memory measurements did not serialize as explicit nulls: %s", encoded)
	}

	for _, name := range []string{"heartbeat-macos-memory.json", "heartbeat-linux-memory.json"} {
		t.Run(name, func(t *testing.T) {
			payload := readMemoryFixture(t, name)
			heartbeat, ok := decodeHubHeartbeatPayload(payload)
			if !ok || heartbeat.HostMemory == nil || !heartbeat.HostMemory.valid() {
				t.Fatalf("memory heartbeat rejected: %+v ok=%t", heartbeat, ok)
			}
			roundTrip, err := json.Marshal(heartbeat)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := decodeHubHeartbeatPayload(roundTrip); !ok {
				t.Fatalf("memory heartbeat did not round-trip: %s", roundTrip)
			}
		})
	}
	legacy, ok := decodeHubHeartbeatPayload([]byte(`{"status":"alive","checks":{"service":"ok"}}`))
	if !ok || legacy.HostMemory != nil {
		t.Fatalf("legacy heartbeat compatibility lost: %+v ok=%t", legacy, ok)
	}
	invalidSource := []byte(`{"status":"alive","host_memory":{"free_pct":50,"compressed_mb":null,"swap_used_mb":0,"psi_some_avg10":null,"source":"other"}}`)
	if _, ok := decodeHubHeartbeatPayload(invalidSource); ok {
		t.Fatal("unknown memory source was accepted")
	}
	for _, payload := range [][]byte{
		[]byte(`{"status":"alive","host_memory":{"free_pct":100.1,"compressed_mb":null,"swap_used_mb":0,"psi_some_avg10":null,"source":"proc_meminfo"}}`),
		[]byte(`{"status":"alive","host_memory":{"free_pct":50,"compressed_mb":-1,"swap_used_mb":0,"psi_some_avg10":null,"source":"proc_meminfo"}}`),
		[]byte(`{"status":"alive","host_memory":{"free_pct":50,"compressed_mb":null,"swap_used_mb":-1,"psi_some_avg10":null,"source":"proc_meminfo"}}`),
		[]byte(`{"status":"alive","host_memory":{"free_pct":50,"compressed_mb":null,"swap_used_mb":0,"psi_some_avg10":-1,"source":"proc_meminfo"}}`),
	} {
		if _, ok := decodeHubHeartbeatPayload(payload); ok {
			t.Fatalf("invalid memory measurement was accepted: %s", payload)
		}
	}
	if (HubHostMemory{FreePct: memoryFloat(math.Inf(1)), Source: "proc_meminfo"}).valid() {
		t.Fatal("infinite memory measurement was accepted")
	}
}

func TestHostMemoryParsersUseCapturedFiles(t *testing.T) {
	memoryPressure := readMemoryFixture(t, "memory/macos-memory_pressure.txt")
	vmstat := readMemoryFixture(t, "memory/macos-vm_stat.txt")
	sysctl := readMemoryFixture(t, "memory/macos-sysctl.txt")
	run := func(_ context.Context, argv ...string) ([]byte, error) {
		switch strings.Join(argv, " ") {
		case "memory_pressure":
			return memoryPressure, nil
		case "sysctl -n vm.swapusage":
			return sysctl, nil
		case "vm_stat":
			return vmstat, nil
		case "sysctl -n hw.memsize":
			return sysctl, nil
		default:
			return nil, errors.New("unexpected fixture command")
		}
	}
	mac, err := collectDarwinHostMemory(t.Context(), run)
	if err != nil || mac.Source != "memory_pressure" || !sameMemoryFloat(mac.FreePct, 90) || !sameMemoryFloat(mac.CompressedMB, 0) || !sameMemoryFloat(mac.SwapUsedMB, 0) || mac.PSISomeAvg10 != nil {
		t.Fatalf("macOS capture parse=%+v err=%v", mac, err)
	}
	vmMemory, ok := parseDarwinVMStat(string(vmstat), "34359738368")
	if !ok || vmMemory.Source != "vm_stat" || !sameMemoryFloat(vmMemory.FreePct, (228595.0+2769006.0)/8388608.0*100) || !sameMemoryFloat(vmMemory.CompressedMB, 0) {
		t.Fatalf("vm_stat fallback parse=%+v ok=%t", vmMemory, ok)
	}

	x86Capture := readMemoryFixture(t, "memory/linux-x86_64-proc.txt")
	x86, ok := parseLinuxHostMemory(string(x86Capture), string(x86Capture))
	if !ok || !sameMemoryFloat(x86.FreePct, 30180264.0/32814612.0*100) || !sameMemoryFloat(x86.SwapUsedMB, 0) || !sameMemoryFloat(x86.PSISomeAvg10, 0) || x86.CompressedMB != nil {
		t.Fatalf("x86 capture parse=%+v ok=%t", x86, ok)
	}

	aarchCapture := readMemoryFixture(t, "memory/linux-aarch64-proc.txt")
	aarch, ok := parseLinuxHostMemory(string(aarchCapture), string(aarchCapture))
	if !ok || !sameMemoryFloat(aarch.FreePct, 6832400.0/8257232.0*100) || !sameMemoryFloat(aarch.SwapUsedMB, (524272.0-189536.0)/1024.0) || aarch.PSISomeAvg10 != nil {
		t.Fatalf("aarch64 capture parse=%+v ok=%t", aarch, ok)
	}
}

func TestHostMemoryPartialMeasurementsAreRetained(t *testing.T) {
	partialLinux, ok := parseLinuxHostMemory("SwapTotal: 2097152 kB\nSwapFree: 524288 kB\n", "some avg10=0.25 avg60=0.00 avg300=0.00 total=1\n")
	if !ok || partialLinux.FreePct != nil || !sameMemoryFloat(partialLinux.SwapUsedMB, 1536) || !sameMemoryFloat(partialLinux.PSISomeAvg10, .25) {
		t.Fatalf("partial Linux measurements were discarded: %+v ok=%t", partialLinux, ok)
	}
	linux, err := collectLinuxHostMemory(func(path string) ([]byte, error) {
		if path == "/proc/meminfo" {
			return []byte("SwapTotal: 2097152 kB\nSwapFree: 524288 kB\n"), nil
		}
		return []byte("some avg10=0.25 avg60=0.00 avg300=0.00 total=1\n"), nil
	})
	if err != nil || linux.FreePct != nil || !sameMemoryFloat(linux.SwapUsedMB, 1536) || !sameMemoryFloat(linux.PSISomeAvg10, .25) {
		t.Fatalf("partial Linux collection was omitted: %+v err=%v", linux, err)
	}

	darwin, err := collectDarwinHostMemory(t.Context(), func(_ context.Context, argv ...string) ([]byte, error) {
		switch strings.Join(argv, " ") {
		case "memory_pressure":
			return []byte("The system has 4096 (1 pages with a page size of 4096).\nPages used by compressor: 10\n"), nil
		case "sysctl -n vm.swapusage":
			return []byte("total = 3.00G used = 2.00G free = 1.00G\n"), nil
		default:
			return nil, errors.New("fallback unavailable")
		}
	})
	if err != nil || darwin.FreePct != nil || !sameMemoryFloat(darwin.CompressedMB, 10*4096.0/(1024*1024)) || !sameMemoryFloat(darwin.SwapUsedMB, 2048) {
		t.Fatalf("partial Darwin collection was omitted: %+v err=%v", darwin, err)
	}
}

func TestPlacementPolicyMemoryDefaultsAndOverrides(t *testing.T) {
	legacy, err := ParsePlacementPolicy([]byte(`{"local_machine":"host-a","spill_targets":["host-b"],"max_active_jobs":10,"load_ratio":0.6}`))
	if err != nil || !sameMemoryFloat(legacy.MemoryFreePctMin, 30) || !sameMemoryFloat(legacy.SwapUsedMBMax, 1536) {
		t.Fatalf("legacy policy lost memory defaults: %+v err=%v", legacy, err)
	}
	explicit, err := ParsePlacementPolicy([]byte(`{"local_machine":"host-a","spill_targets":["host-b"],"max_active_jobs":10,"load_ratio":0.6,"memory_free_pct_min":0,"swap_used_mb_max":17}`))
	if err != nil || !sameMemoryFloat(explicit.MemoryFreePctMin, 0) || !sameMemoryFloat(explicit.SwapUsedMBMax, 17) {
		t.Fatalf("explicit memory policy was replaced by default: %+v err=%v", explicit, err)
	}
	if _, err := ParsePlacementPolicy([]byte(`{"local_machine":"host-a","max_active_jobs":10,"load_ratio":0.6,"memory_free_pct_min":100.1}`)); err == nil {
		t.Fatal("out-of-range memory threshold was accepted")
	}
}

func TestPlacementMemoryAdmissionBoundaries(t *testing.T) {
	cases := []struct {
		name      string
		freePct   float64
		swapUsed  float64
		decision  string
		pressured bool
	}{
		{name: "free 29.9 rejects", freePct: 29.9, swapUsed: 0, decision: "unavailable", pressured: true},
		{name: "free 30 passes", freePct: 30, swapUsed: 0, decision: "host-a"},
		{name: "swap 1536 passes", freePct: 50, swapUsed: 1536, decision: "host-a"},
		{name: "swap 1536.1 rejects", freePct: 50, swapUsed: 1536.1, decision: "unavailable", pressured: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hub := memoryPlacementHub(t, "host-a")
			hub.nodes["host-a"].hostMemory = &HubHostMemory{FreePct: memoryFloat(tc.freePct), SwapUsedMB: memoryFloat(tc.swapUsed), Source: "proc_meminfo"}
			result := hub.makePlacement(memoryPlacementPolicy("host-a"), placementMetrics{}, "hub-only", time.Now())
			candidate := memoryCandidate(t, result, "host-a")
			if result.Decision != tc.decision || strings.Contains(candidate.Reason, "not_accepting(memory_pressure)") != tc.pressured {
				t.Fatalf("decision=%q reason=%q", result.Decision, candidate.Reason)
			}
		})
	}
}

func TestPlacementMemoryUnknownFailsOpen(t *testing.T) {
	cases := []struct {
		name   string
		memory *HubHostMemory
	}{
		{name: "free null", memory: &HubHostMemory{SwapUsedMB: memoryFloat(0), Source: "proc_meminfo"}},
		{name: "swap null", memory: &HubHostMemory{FreePct: memoryFloat(50), Source: "proc_meminfo"}},
		{name: "host memory absent"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hub := memoryPlacementHub(t, "host-a")
			hub.nodes["host-a"].hostMemory = tc.memory
			result := hub.makePlacement(memoryPlacementPolicy("host-a"), placementMetrics{}, "hub-only", time.Now())
			candidate := memoryCandidate(t, result, "host-a")
			if result.Decision != "host-a" || strings.Contains(candidate.Reason, "memory_pressure") || !strings.Contains(candidate.Reason, "memory_unknown") {
				t.Fatalf("unknown memory changed admission: %+v", result)
			}
		})
	}
}

func TestPlacementMemoryUnknownIsRecordedAlongsideExistingReasons(t *testing.T) {
	hub := memoryPlacementHub(t, "host-a")
	result := hub.makePlacement(memoryPlacementPolicy("host-a"), placementMetrics{}, "prometheus", time.Now())
	candidate := memoryCandidate(t, result, "host-a")
	if !strings.Contains(candidate.Reason, "load_unknown") || !strings.Contains(candidate.Reason, "memory_unknown") {
		t.Fatalf("unknown memory evidence was suppressed: %+v", result)
	}
}

func TestPlacementMemoryPressureSpillsOrBecomesUnavailable(t *testing.T) {
	hub := memoryPlacementHub(t, "host-a", "host-b")
	hub.nodes["host-a"].hostMemory = &HubHostMemory{FreePct: memoryFloat(29), SwapUsedMB: memoryFloat(0), Source: "proc_meminfo"}
	hub.nodes["host-b"].hostMemory = &HubHostMemory{FreePct: memoryFloat(50), SwapUsedMB: memoryFloat(0), Source: "proc_meminfo"}
	if result := hub.makePlacement(memoryPlacementPolicy("host-a", "host-b"), placementMetrics{}, "hub-only", time.Now()); result.Decision != "host-b" {
		t.Fatalf("memory pressure did not spill: %+v", result)
	}
	if result := hub.makePlacement(memoryPlacementPolicy("host-a"), placementMetrics{}, "hub-only", time.Now()); result.Decision != "unavailable" || result.Reason != "unavailable" {
		t.Fatalf("memory pressure local fallback was not fail-closed: %+v", result)
	}
}

func TestMemoryFieldsAreExposedByPlacementAndNodesAPIs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "placement.json")
	if err := os.WriteFile(path, []byte(`{"local_machine":"host-a","max_active_jobs":5,"load_ratio":0.5}`), 0600); err != nil {
		t.Fatal(err)
	}
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": r6OperatorToken, "host-a": r6NodeAToken}, PlacementPolicyPath: path})
	if err != nil {
		t.Fatal(err)
	}
	agent := &hubAgent{}
	hub.connect("host-a", "fixture", "fixture", agent, true)
	payload := []byte(`{"type":"event","kind":"heartbeat","payload":{"status":"alive","host_memory":{"free_pct":50,"compressed_mb":null,"swap_used_mb":0,"psi_some_avg10":null,"source":"proc_meminfo"}}}`)
	hub.handleAgentMessage("host-a", "fixture", agent, payload)

	placementRequest := httptest.NewRequest(http.MethodGet, "/v1/placement?class=worker", nil)
	placementRequest.Header.Set(hubAuthorizationHeader, "Bearer "+r6OperatorToken)
	placementResponse := httptest.NewRecorder()
	hub.Handler().ServeHTTP(placementResponse, placementRequest)
	if placementResponse.Code != http.StatusOK {
		t.Fatalf("placement status=%d", placementResponse.Code)
	}
	var placement struct {
		Candidates []map[string]json.RawMessage `json:"candidates"`
	}
	if err := json.NewDecoder(placementResponse.Body).Decode(&placement); err != nil || len(placement.Candidates) != 1 {
		t.Fatalf("placement response=%q err=%v", placementResponse.Body.String(), err)
	}
	for _, field := range []string{"memory_free_pct", "swap_used_mb", "memory_known"} {
		if _, exists := placement.Candidates[0][field]; !exists {
			t.Fatalf("placement omitted %s: %s", field, placementResponse.Body.String())
		}
	}

	nodesRequest := httptest.NewRequest(http.MethodGet, "/v1/nodes", nil)
	nodesRequest.Header.Set(hubAuthorizationHeader, "Bearer "+r6OperatorToken)
	nodesResponse := httptest.NewRecorder()
	hub.Handler().ServeHTTP(nodesResponse, nodesRequest)
	var nodes struct {
		Nodes []map[string]json.RawMessage `json:"nodes"`
	}
	if err := json.NewDecoder(nodesResponse.Body).Decode(&nodes); err != nil || len(nodes.Nodes) != 1 {
		t.Fatalf("nodes response=%q err=%v", nodesResponse.Body.String(), err)
	}
	if memory, exists := nodes.Nodes[0]["memory"]; !exists || !strings.Contains(string(memory), `"free_pct":50`) {
		t.Fatalf("nodes omitted memory: %s", nodesResponse.Body.String())
	}
}

func TestPlacementCacheKeepsSameMemoryHeartbeatAndInvalidatesChange(t *testing.T) {
	var calls atomic.Int32
	prom := placementPromServer(t, .1, 1, &calls)
	defer prom.Close()
	hub := placementHub(t, prom.URL)
	agent := &hubAgent{}
	hub.connect("mac-work", "fixture", "fixture", agent, true)
	memory := &HubHostMemory{FreePct: memoryFloat(50), SwapUsedMB: memoryFloat(0), Source: "proc_meminfo"}
	sendMemoryHeartbeat(t, hub, "mac-work", agent, memory)
	first := hub.placement(t.Context(), "worker", "cache-fixture")
	if first.Decision != "mac-work" || calls.Load() != 2 {
		t.Fatalf("first placement=%+v calls=%d", first, calls.Load())
	}
	hub.mu.Lock()
	cacheAt := hub.placementCache.at
	hub.mu.Unlock()

	sendMemoryHeartbeat(t, hub, "mac-work", agent, cloneHubHostMemory(memory))
	second := hub.placement(t.Context(), "worker", "cache-fixture")
	hub.mu.Lock()
	secondCacheAt := hub.placementCache.at
	hub.mu.Unlock()
	if second.Decision != "mac-work" || calls.Load() != 2 || !secondCacheAt.Equal(cacheAt) {
		t.Fatalf("same heartbeat invalidated placement cache: second=%+v calls=%d at=%s want=%s", second, calls.Load(), secondCacheAt, cacheAt)
	}

	sendMemoryHeartbeat(t, hub, "mac-work", agent, &HubHostMemory{FreePct: memoryFloat(20), SwapUsedMB: memoryFloat(0), Source: "proc_meminfo"})
	changed := hub.placement(t.Context(), "worker", "cache-fixture")
	if changed.Decision != "unavailable" || calls.Load() != 4 {
		t.Fatalf("changed heartbeat did not invalidate placement cache: changed=%+v calls=%d", changed, calls.Load())
	}
}

func TestHeartbeatEventIncludesCollectedHostMemory(t *testing.T) {
	want := &HubHostMemory{FreePct: memoryFloat(50), CompressedMB: memoryFloat(12), SwapUsedMB: memoryFloat(3), PSISomeAvg10: memoryFloat(.25), Source: "proc_meminfo"}
	client, err := NewHubClient(HubClientConfig{
		URL:                   "ws://fixture.invalid",
		MachineID:             "node-a",
		Token:                 r6NodeAToken,
		JobsInboxRoot:         t.TempDir(),
		AllowInsecureForTests: true,
		hostLoadCollector: func(context.Context) (HubHostLoad, error) {
			return HubHostLoad{}, nil
		},
		hostMemoryCollector: func(context.Context) (*HubHostMemory, error) {
			return cloneHubHostMemory(want), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	heartbeat, ok := decodeHubHeartbeatPayload(client.heartbeatEvent(t.Context()).Payload)
	if !ok || !equalHubHostMemory(heartbeat.HostMemory, want) {
		t.Fatalf("heartbeat omitted or changed collected memory: %+v ok=%t", heartbeat, ok)
	}
}

func sendMemoryHeartbeat(t *testing.T, hub *HubServer, machine string, agent *hubAgent, memory *HubHostMemory) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"type": "event", "kind": "heartbeat", "payload": map[string]any{"status": "alive", "host_memory": memory}})
	if err != nil {
		t.Fatal(err)
	}
	hub.handleAgentMessage(machine, "fixture", agent, payload)
}

func memoryPlacementHub(t *testing.T, machines ...string) *HubServer {
	t.Helper()
	tokens := map[string]string{"operator": "memory-operator-token"}
	for _, machine := range machines {
		tokens[machine] = "memory-node-token-" + machine
	}
	hub, err := NewHubServer(HubServerConfig{Tokens: tokens})
	if err != nil {
		t.Fatal(err)
	}
	for _, machine := range machines {
		hub.connect(machine, "fixture", "fixture", &hubAgent{}, true)
	}
	return hub
}

func memoryPlacementPolicy(local string, spill ...string) PlacementPolicy {
	return PlacementPolicy{LocalMachine: local, SpillTargets: spill, MaxActiveJobs: 5, LoadRatio: .5, MemoryFreePctMin: memoryFloat(30), SwapUsedMBMax: memoryFloat(1536)}
}

func memoryCandidate(t *testing.T, result PlacementResult, machine string) PlacementCandidate {
	t.Helper()
	for _, candidate := range result.Candidates {
		if candidate.Machine == machine {
			return candidate
		}
	}
	t.Fatalf("candidate %q missing: %+v", machine, result)
	return PlacementCandidate{}
}

func readMemoryFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func sameMemoryFloat(value *float64, want float64) bool {
	return value != nil && math.Abs(*value-want) < 1e-9
}

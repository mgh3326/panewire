package panewire

import (
	"bytes"
	"encoding/json"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var updatePlaceExplainGolden = flag.Bool("update-place-explain", false, "rewrite testdata/place_explain.golden")

// measuredLoadProm answers each placement query from its own per-machine
// table, so a test sets CPU, memory and swap-in independently. A query with
// no table answers an empty vector: that signal is unreported.
func measuredLoadProm(t *testing.T, tables map[string]map[string]string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result := []any{}
		for machine, value := range tables[r.URL.Query().Get("query")] {
			result = append(result, map[string]any{"metric": map[string]string{"machine_id": machine}, "value": []any{float64(1), value}})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "vector", "result": result}})
	}))
	t.Cleanup(server.Close)
	return server
}

func measuredLoadHub(t *testing.T, tables map[string]map[string]string, policy PlacementPolicy) *HubServer {
	t.Helper()
	hub := placementPowerHub(t, measuredLoadProm(t, tables).URL, policy)
	for _, machine := range append([]string{policy.LocalMachine}, policy.SpillTargets...) {
		hub.connect(machine, "test", "fixture", &hubAgent{}, true)
	}
	return hub
}

func measuredLoadPolicy() PlacementPolicy {
	return PlacementPolicy{LocalMachine: "mac-work", SpillTargets: []string{"desktop"}, MaxActiveJobs: 50}
}

func idleThermal() map[string]string { return map[string]string{"mac-work": "1", "desktop": "1"} }

func TestPlacementFreeCombinesMeasuredSignals(t *testing.T) {
	f := func(v float64) *float64 { return &v }
	cases := []struct {
		name    string
		signals PlacementSignals
		want    float64
	}{
		{"nothing reported counts as idle", PlacementSignals{}, 1},
		{"five-minute cpu", PlacementSignals{CPUBusy5m: f(.25)}, .75},
		{"a heavy last minute shows at once", PlacementSignals{CPUBusy5m: f(.2), CPUBusy1m: f(.9)}, .1},
		{"a quiet last minute does not forget sustained load", PlacementSignals{CPUBusy5m: f(.6), CPUBusy1m: f(.1)}, .4},
		{"memory is the bottleneck", PlacementSignals{CPUBusy5m: f(.1), CPUBusy1m: f(.1), MemAvailable: f(.3)}, .3},
		{"cpu is the bottleneck", PlacementSignals{CPUBusy5m: f(.8), MemAvailable: f(.9)}, .2},
		{"swap-in at the half rate halves it", PlacementSignals{CPUBusy5m: f(0), MemAvailable: f(.8), SwapInMBps: f(placementSwapInHalfMBps)}, .4},
		{"busy fraction above one is clamped", PlacementSignals{CPUBusy5m: f(1.5)}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := placementFree(tc.signals); got < tc.want-1e-9 || got > tc.want+1e-9 {
				t.Fatalf("free=%v, want %v", got, tc.want)
			}
		})
	}
}

// Acceptance 2a: at equal power the host with the lower measured load ranks
// first. CPU is equal; each case moves one of the signals a set of idle agent
// sessions actually moves. The loaded host is first in policy order, so only
// the score can put the other one ahead.
func TestPlacementPrefersLowerMeasuredLoadAtEqualPower(t *testing.T) {
	cases := []struct {
		name      string
		mem, swap map[string]string
	}{
		{"less memory available", map[string]string{"mac-work": "0.15", "desktop": "0.8"}, map[string]string{"mac-work": "0", "desktop": "0"}},
		{"swapping in", map[string]string{"mac-work": "0.8", "desktop": "0.8"}, map[string]string{"mac-work": "40", "desktop": "0"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			equal := map[string]string{"mac-work": "0.1", "desktop": "0.1"}
			hub := measuredLoadHub(t, map[string]map[string]string{
				placementCPUBusyQuery: equal, placementCPUBusy1mQuery: equal, placementMemAvailableQuery: tc.mem,
				placementSwapInQuery: tc.swap, placementThermalQuery: idleThermal(),
			}, measuredLoadPolicy())
			got := hub.placement(t.Context(), "worker", "repo")
			if got.Source != "prometheus" || got.Candidates[0].Machine != "desktop" || got.Candidates[0].Score <= got.Candidates[1].Score {
				t.Fatalf("lower measured load must rank first: %+v", got.Candidates)
			}
		})
	}
}

// Acceptance 2b: at equal measured load the stronger host ranks first, even
// as the second policy entry.
func TestPlacementPrefersStrongerHostAtEqualLoad(t *testing.T) {
	policy := measuredLoadPolicy()
	policy.Power = map[string]float64{"mac-work": 40, "desktop": 100}
	equal := map[string]string{"mac-work": "0.25", "desktop": "0.25"}
	hub := measuredLoadHub(t, map[string]map[string]string{
		placementCPUBusyQuery: equal, placementCPUBusy1mQuery: equal, placementMemAvailableQuery: {"mac-work": "0.5", "desktop": "0.5"},
		placementSwapInQuery: {"mac-work": "0", "desktop": "0"}, placementThermalQuery: idleThermal(),
	}, policy)
	got := hub.placement(t.Context(), "worker", "repo")
	strong, weak := got.Candidates[0], got.Candidates[1]
	if strong.Machine != "desktop" || strong.Score != 50 || weak.Score != 20 {
		t.Fatalf("stronger host must rank first with free*power: %+v", got.Candidates)
	}
}

// Acceptance 2c: no load filter remains. A host pinned at full CPU, swapping
// hard, under critical macOS pressure and above a stale load_ratio stays
// usable; only free memory under memory_free_pct_min refuses it.
func TestPlacementRefusesOnlyBelowMemoryFloor(t *testing.T) {
	policy := measuredLoadPolicy()
	policy.LoadRatio = .1 // a stale threshold from an old policy file
	busy := map[string]string{"mac-work": "1", "desktop": "1"}
	hub := measuredLoadHub(t, map[string]map[string]string{
		placementCPUBusyQuery: busy, placementCPUBusy1mQuery: busy, placementMemAvailableQuery: {"mac-work": "0.05", "desktop": "0.05"},
		placementSwapInQuery: {"mac-work": "500", "desktop": "500"}, placementThermalQuery: idleThermal(),
	}, policy)
	hub.nodes["mac-work"].hostMemory = &HubHostMemory{FreePct: memoryFloat(31), SwapUsedMB: memoryFloat(30000), SwapInPerS: memoryFloat(9000), Pressure: memoryFloat(2), Source: "vm_stat"}
	hub.nodes["desktop"].hostMemory = &HubHostMemory{FreePct: memoryFloat(85), SwapUsedMB: memoryFloat(0), PSISomeAvg10: memoryFloat(80), Pressure: memoryFloat(80), Source: "proc_meminfo"}
	got := hub.placement(t.Context(), "worker", "repo")
	for _, candidate := range got.Candidates {
		if candidate.Reason != "available" || !placementUsable(candidate, policy, got.Source) {
			t.Fatalf("load refused a host: %+v", candidate)
		}
	}
	if got.Decision != "mac-work" {
		t.Fatalf("decision=%q", got.Decision)
	}

	hub.nodes["mac-work"].hostMemory = &HubHostMemory{FreePct: memoryFloat(29), SwapUsedMB: memoryFloat(0), Source: "vm_stat"}
	hub.mu.Lock()
	hub.placementCache = placementCache{}
	hub.mu.Unlock()
	got = hub.placement(t.Context(), "worker", "repo")
	floored, _ := t607Candidate(got, "mac-work")
	if got.Decision != "desktop" || !strings.Contains(floored.Reason, "not_accepting(memory_pressure)") {
		t.Fatalf("memory floor must refuse: decision=%q candidate=%+v", got.Decision, floored)
	}
}

// Acceptance 3 (hub side): a job inside the spawn bridge lowers its host's
// score; one older than the bridge does not, nor does one without a start.
func TestPlacementSpawnBridgeCountsOnlyFreshJobs(t *testing.T) {
	idle := map[string]string{"mac-work": "0.1", "desktop": "0.1"}
	tables := map[string]map[string]string{
		placementCPUBusyQuery: idle, placementCPUBusy1mQuery: idle, placementMemAvailableQuery: {"mac-work": "0.9", "desktop": "0.9"},
		placementSwapInQuery: {"mac-work": "0", "desktop": "0"}, placementThermalQuery: idleThermal(),
	}
	now := time.Now().UTC()
	cases := []struct {
		name    string
		started string
		bridged int
	}{
		{"spawned 1 min ago", now.Add(-time.Minute).Format(time.RFC3339), 1},
		{"spawned 6 min ago", now.Add(-6 * time.Minute).Format(time.RFC3339), 0},
		{"start unknown", "", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hub := measuredLoadHub(t, tables, measuredLoadPolicy())
			hub.observeActiveJobs("mac-work", []HubActiveJob{{JobID: "job-fresh", AgentLabel: "agent", Epoch: 1, StartedAt: tc.started}}, now)
			got := hub.placement(t.Context(), "worker", "repo")
			host, _ := t607Candidate(got, "mac-work")
			other, _ := t607Candidate(got, "desktop")
			if diff := other.Score - host.Score - float64(tc.bridged)*placementSpawnBridgeCost; host.Signals.BridgedJobs != tc.bridged || diff > 1e-9 || diff < -1e-9 {
				t.Fatalf("bridged=%d host=%v other=%v", host.Signals.BridgedJobs, host.Score, other.Score)
			}
		})
	}
}

func TestPlacementSpawnBridgeIsConfigurable(t *testing.T) {
	jobs := map[string]HubActiveJob{"job": {JobID: "job", StartedAt: "2026-11-02T00:00:00Z"}}
	at := time.Date(2026, 11, 2, 0, 8, 0, 0, time.UTC)
	if got := countBridgedJobs(jobs, at, PlacementPolicy{}.spawnBridge()); got != 0 {
		t.Fatalf("default bridge counted an 8-minute-old job: %d", got)
	}
	if got := countBridgedJobs(jobs, at, PlacementPolicy{SpawnBridgeSeconds: 600}.spawnBridge()); got != 1 {
		t.Fatalf("10-minute bridge missed an 8-minute-old job: %d", got)
	}
	for _, raw := range []string{`-1`, `3601`} {
		if _, err := ParsePlacementPolicy([]byte(`{"local_machine":"mac-work","max_active_jobs":5,"spawn_bridge_seconds":` + raw + `}`)); err == nil {
			t.Fatalf("spawn_bridge_seconds %s accepted", raw)
		}
	}
	if _, err := ParsePlacementPolicy([]byte(`{"local_machine":"mac-work","max_active_jobs":5}`)); err != nil {
		t.Fatalf("a policy without load_ratio must load: %v", err)
	}
}

// The signals reach only a caller that asks for them: a pre-change CLI
// decodes candidates strictly and must keep accepting the default wire.
func TestPlacementSignalsWireIsOptIn(t *testing.T) {
	idle := map[string]string{"mac-work": "0.1", "desktop": "0.1"}
	hub := measuredLoadHub(t, map[string]map[string]string{placementCPUBusyQuery: idle, placementThermalQuery: idleThermal()}, measuredLoadPolicy())
	fetch := func(query string) []map[string]json.RawMessage {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/v1/placement?class=worker"+query, nil)
		request.Header.Set("Authorization", "Bearer "+r6OperatorToken)
		hub.Handler().ServeHTTP(recorder, request)
		var body struct {
			Candidates []map[string]json.RawMessage `json:"candidates"`
		}
		if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &body) != nil || len(body.Candidates) != 2 {
			t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body)
		}
		return body.Candidates
	}
	for _, candidate := range fetch("") {
		if _, present := candidate["signals"]; present {
			t.Fatalf("default wire carries signals: %v", candidate)
		}
		raw, _ := json.Marshal(candidate)
		strict := json.NewDecoder(bytes.NewReader(raw))
		strict.DisallowUnknownFields()
		var legacy legacyPlacementCandidate
		if err := strict.Decode(&legacy); err != nil {
			t.Fatalf("legacy decoder rejected the default wire: %v", err)
		}
	}
	// The second request hits the cache that the first one filled: asking
	// for signals must still find them, and not asking must not have
	// stripped them from the cached result.
	for _, candidate := range fetch("&signals=1") {
		var signals PlacementSignals
		if err := json.Unmarshal(candidate["signals"], &signals); err != nil || signals.CPUBusy5m == nil || *signals.CPUBusy5m != .1 || signals.Power != 100 {
			t.Fatalf("signals=%s err=%v", candidate["signals"], err)
		}
	}
}

// Acceptance 4: place --explain prints the signals behind each score. The
// golden file is the operator-facing table; every row with signals satisfies
// SCORE = (FREE*POWER - 10*BRIDGED) * WEIGHT, and the last row is an older hub
// that sends no signals.
func TestPlaceExplainGolden(t *testing.T) {
	f := func(v float64) *float64 { return &v }
	result := PlacementResult{Decision: "mac-personal", Source: "prometheus", Asof: time.Date(2026, 11, 2, 4, 42, 10, 0, time.UTC), Candidates: []PlacementCandidate{
		{Machine: "desktop", Score: 207.9, LoadRatio: .31, ActiveJobs: 4, Connected: true, CapacityWeight: 3, Reason: "available",
			Signals: &PlacementSignals{CPUBusy1m: f(.307), CPUBusy5m: f(.308), MemAvailable: f(.866), SwapInMBps: f(.006), Free: .693, Power: 100}},
		{Machine: "mac-dev", Score: 109.02, LoadRatio: .24, ActiveJobs: 6, Connected: true, CapacityWeight: 4.6, Reason: "available",
			Signals: &PlacementSignals{CPUBusy1m: f(.23), CPUBusy5m: f(.238), MemAvailable: f(.371), SwapInMBps: f(2.02), Free: .337, Power: 100, BridgedJobs: 1}},
		{Machine: "mac-personal", Score: 30.4, LoadRatio: .16, ActiveJobs: 2, Connected: true, CapacityWeight: 1.6, Reason: "available",
			Signals: &PlacementSignals{CPUBusy1m: f(.143), CPUBusy5m: f(.162), MemAvailable: f(.268), SwapInMBps: f(8.15), Free: .19, Power: 100}},
		{Machine: "rpi", Score: 29.61, LoadRatio: .01, Connected: true, CapacityWeight: .3, Reason: "memory_unknown",
			Signals: &PlacementSignals{CPUBusy5m: f(.013), Free: .987, Power: 100}},
		{Machine: "old-hub-host", Score: 90, Connected: true, Reason: "available"},
	}}
	var out bytes.Buffer
	renderPlacementExplain(&out, result)
	path := filepath.Join("testdata", "place_explain.golden")
	if *updatePlaceExplainGolden {
		if err := os.WriteFile(path, out.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), want) {
		t.Fatalf("explain output drifted from %s (rerun with -update-place-explain after review):\n%s", path, out.String())
	}
}

// The CLI asks for signals only when it renders them.
func TestPlaceCLIRequestsSignalsOnlyForExplain(t *testing.T) {
	var asked []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked = append(asked, r.URL.Query().Get("signals"))
		_ = json.NewEncoder(w).Encode(PlacementResult{Decision: "desktop", Source: "prometheus", Asof: time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC), Candidates: []PlacementCandidate{{Machine: "desktop", Score: 90, Connected: true, Reason: "available"}}})
	}))
	defer server.Close()
	tokenPath := filepath.Join(t.TempDir(), "operator.env")
	if err := os.WriteFile(tokenPath, []byte("HUB_MACHINE_ID=operator\nHUB_TOKEN="+r6OperatorToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	deps := hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true}
	args := []string{"--class", "worker", "--hub-url", server.URL, "--hub-token-env", tokenPath}
	var out, stderr bytes.Buffer
	if runPlaceCLI(args, &out, &stderr, deps) != ExitOK || runPlaceCLI(append(args, "--explain"), &out, &stderr, deps) != ExitOK {
		t.Fatalf("stderr=%q", stderr.String())
	}
	if len(asked) != 2 || asked[0] != "" || asked[1] != "1" {
		t.Fatalf("signals asked=%q", asked)
	}
}

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
		{"memory above the knee costs nothing", PlacementSignals{CPUBusy5m: f(.1), MemAvailable: f(.18)}, .9},
		{"memory below the knee scales down", PlacementSignals{CPUBusy5m: f(.1), MemAvailable: f(.05)}, .45},
		{"no memory left", PlacementSignals{CPUBusy5m: f(.1), MemAvailable: f(0)}, 0},
		{"swap-in at the half rate halves it", PlacementSignals{CPUBusy5m: f(0), SwapInMBps: f(placementDefaultSwapInHalfMBps)}, .5},
		{"bench: 25 MB/s costs about the measured 8%", PlacementSignals{CPUBusy5m: f(0), SwapInMBps: f(25)}, 1 / (1 + 25/placementDefaultSwapInHalfMBps)},
		{"busy fraction above one is clamped", PlacementSignals{CPUBusy5m: f(1.5)}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := (PlacementPolicy{}).placementFree(tc.signals); got < tc.want-1e-9 || got > tc.want+1e-9 {
				t.Fatalf("free=%v, want %v", got, tc.want)
			}
		})
	}
	if got := (PlacementPolicy{}).placementFree(PlacementSignals{SwapInMBps: f(25)}); got < .92 || got > .93 {
		t.Fatalf("default swap penalty at 25 MB/s=%v, want the bench's ~0.926", got)
	}
	if got := (PlacementPolicy{SwapInHalfMBps: 25}).placementFree(PlacementSignals{SwapInMBps: f(25)}); got != .5 {
		t.Fatalf("swap_in_half_mbps not honoured: %v", got)
	}
}

// The 2026-11-02 benchmark (docs/placement.md, n=1): the same Go workload
// took 37.6 s on mac-dev, 40.6 s on mac-work (the same machine, then paging
// in ~25 MB/s with the least memory available) and 96.0 s on desktop. With
// the measured idle power and the hosts' live signals from that hour,
// mac-dev and mac-work must both rank above desktop.
func TestPlacementBenchCalibratedRanking(t *testing.T) {
	policy := PlacementPolicy{LocalMachine: "desktop", SpillTargets: []string{"mac-work", "mac-dev"}, MaxActiveJobs: 50,
		Power: map[string]float64{"mac-dev": 100, "mac-work": 100, "desktop": 39}}
	hub := placementPowerHub(t, measuredLoadProm(t, map[string]map[string]string{
		placementCPUBusyQuery:      {"mac-dev": "0.14", "mac-work": "0.44", "desktop": "0.30"},
		placementCPUBusy1mQuery:    {"mac-dev": "0.14", "mac-work": "0.41", "desktop": "0.30"},
		placementMemAvailableQuery: {"mac-dev": "0.46", "mac-work": "0.18", "desktop": "0.86"},
		placementSwapInQuery:       {"mac-dev": "2.01", "mac-work": "25.01", "desktop": "0"},
		placementThermalQuery:      {"mac-dev": "1", "mac-work": "1", "desktop": "1"},
	}).URL, policy)
	for _, machine := range []string{"desktop", "mac-work", "mac-dev"} {
		hub.connect(machine, "test", "fixture", &hubAgent{}, true)
	}
	got := hub.placement(t.Context(), "worker", "repo")
	order := make([]string, 0, len(got.Candidates))
	for _, candidate := range got.Candidates {
		order = append(order, candidate.Machine)
	}
	if strings.Join(order, ",") != "mac-dev,mac-work,desktop" {
		t.Fatalf("order=%v candidates=%+v", order, got.Candidates)
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
		{"memory nearly exhausted", map[string]string{"mac-work": "0.05", "desktop": "0.8"}, map[string]string{"mac-work": "0", "desktop": "0"}},
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
	if strong.Machine != "desktop" || strong.Score != 75 || weak.Score != 30 {
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

// Stale or missing data never reads as idle. A host whose exporter stopped
// keeps a 5m CPU value for minutes after the last-minute one is gone: that
// host is load_unknown. A host without a Prometheus memory reading is scored
// on the node's own free_pct instead of as memory-idle.
func TestPlacementStaleOrMissingNeverReadsIdle(t *testing.T) {
	hub := measuredLoadHub(t, map[string]map[string]string{
		placementCPUBusyQuery:   {"mac-work": "0.05", "desktop": "0.2"},
		placementCPUBusy1mQuery: {"desktop": "0.2"}, // mac-work went quiet
		placementSwapInQuery:    {"mac-work": "0", "desktop": "0"},
		placementThermalQuery:   idleThermal(),
	}, staleOrMissingPolicy())
	hub.nodes["desktop"].hostMemory = &HubHostMemory{FreePct: memoryFloat(5), SwapUsedMB: memoryFloat(0), Source: "proc_meminfo"}
	got := hub.placement(t.Context(), "worker", "repo")
	stale, _ := t607Candidate(got, "mac-work")
	if got.Decision != "desktop" || !strings.Contains(stale.Reason, "load_unknown") || stale.MetricsKnown || got.Candidates[0].Machine != "desktop" {
		t.Fatalf("stale host read as idle: decision=%q candidates=%+v", got.Decision, got.Candidates)
	}
	fresh, _ := t607Candidate(got, "desktop")
	if fresh.Signals.MemAvailable == nil || *fresh.Signals.MemAvailable != .05 || fresh.Signals.Free != .4 {
		t.Fatalf("missing Prometheus memory did not fall back to free_pct: %+v", fresh.Signals)
	}
}

// A NaN sample (a 0/0 memory fraction) must not reach the score: encoding/json
// refuses NaN, which would turn every placement response into an empty 200.
func TestPlacementSkipsNonFiniteSamples(t *testing.T) {
	idle := map[string]string{"mac-work": "0.1", "desktop": "0.1"}
	hub := measuredLoadHub(t, map[string]map[string]string{
		placementCPUBusyQuery: idle, placementCPUBusy1mQuery: idle, placementThermalQuery: idleThermal(),
		placementMemAvailableQuery: {"mac-work": "NaN", "desktop": "0.5"},
		placementSwapInQuery:       {"mac-work": "+Inf", "desktop": "0"},
	}, measuredLoadPolicy())
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/placement?class=worker&signals=1", nil)
	request.Header.Set("Authorization", "Bearer "+r6OperatorToken)
	hub.Handler().ServeHTTP(recorder, request)
	var result PlacementResult
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil || recorder.Code != http.StatusOK {
		t.Fatalf("status=%d err=%v body=%q", recorder.Code, err, recorder.Body)
	}
	skipped, _ := t607Candidate(result, "mac-work")
	if skipped.Signals.MemAvailable != nil || skipped.Signals.SwapInMBps != nil || skipped.Score != 90 {
		t.Fatalf("non-finite samples reached the score: %+v score=%v", skipped.Signals, skipped.Score)
	}
}

// staleOrMissingPolicy lowers the memory floor so the fallback reading can
// sit below placementMemKnee and visibly change the score.
func staleOrMissingPolicy() PlacementPolicy {
	policy := measuredLoadPolicy()
	policy.MemoryFreePctMin = memoryFloat(0)
	return policy
}

func TestPlacementSwapInHalfPolicyKey(t *testing.T) {
	for _, raw := range []string{`-1`, `100001`, `"fast"`} {
		if _, err := ParsePlacementPolicy([]byte(`{"local_machine":"mac-work","max_active_jobs":5,"swap_in_half_mbps":` + raw + `}`)); err == nil {
			t.Fatalf("swap_in_half_mbps %s accepted", raw)
		}
	}
	policy, err := ParsePlacementPolicy([]byte(`{"local_machine":"mac-work","max_active_jobs":5,"swap_in_half_mbps":50}`))
	if err != nil || policy.swapInHalfMBps() != 50 || (PlacementPolicy{}).swapInHalfMBps() != placementDefaultSwapInHalfMBps {
		t.Fatalf("policy=%+v err=%v", policy, err)
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
	// A node clock a little ahead still counts; far ahead is not trusted,
	// so skew cannot stretch the bridge past its window.
	early := time.Date(2026, 11, 1, 23, 59, 30, 0, time.UTC)
	if got := countBridgedJobs(jobs, early, PlacementPolicy{}.spawnBridge()); got != 1 {
		t.Fatalf("start 30s ahead of the hub not counted: %d", got)
	}
	skewed := time.Date(2026, 11, 1, 23, 50, 0, 0, time.UTC)
	if got := countBridgedJobs(jobs, skewed, PlacementPolicy{}.spawnBridge()); got != 0 {
		t.Fatalf("start 10m ahead of the hub counted: %d", got)
	}
	// 9223372037 s wraps negative and 18446744074 s wraps to ~0.29 s when
	// multiplied by time.Second; both must be rejected on the int itself.
	for _, raw := range []string{`-1`, `3601`, `10000000000000`, `9223372037`, `18446744074`} {
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
	hub := measuredLoadHub(t, map[string]map[string]string{placementCPUBusyQuery: idle, placementCPUBusy1mQuery: idle, placementThermalQuery: idleThermal()}, measuredLoadPolicy())
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
	// Free and Score come from the formula itself, so every row with
	// signals is what the hub would compute (calibrated powers, no weight
	// except rpi's).
	policy := PlacementPolicy{Power: map[string]float64{"mac-dev": 100, "mac-work": 100, "desktop": 39, "rpi": 7}}
	row := func(machine string, loadRatio float64, jobs, bridged int, weight float64, reason string, signals PlacementSignals) PlacementCandidate {
		signals.Free, signals.Power, signals.BridgedJobs = policy.placementFree(signals), policy.power(machine), bridged
		w := weight
		if w == 0 {
			w = 1
		}
		return PlacementCandidate{Machine: machine, Score: placementScoreBase(signals.Free, bridged, signals.Power) * w, LoadRatio: loadRatio, ActiveJobs: jobs, Connected: true, CapacityWeight: weight, Reason: reason, Signals: &signals}
	}
	result := PlacementResult{Decision: "mac-dev", Source: "prometheus", Asof: time.Date(2026, 11, 2, 5, 23, 14, 0, time.UTC), Candidates: []PlacementCandidate{
		row("mac-dev", .14, 6, 1, 0, "available", PlacementSignals{CPUBusy1m: f(.14), CPUBusy5m: f(.14), MemAvailable: f(.46), SwapInMBps: f(2.01)}),
		row("mac-work", .44, 0, 0, 0, "available", PlacementSignals{CPUBusy1m: f(.41), CPUBusy5m: f(.44), MemAvailable: f(.18), SwapInMBps: f(25.01)}),
		row("desktop", .30, 1, 0, 0, "available", PlacementSignals{CPUBusy1m: f(.30), CPUBusy5m: f(.30), MemAvailable: f(.86), SwapInMBps: f(0)}),
		row("rpi", .01, 0, 0, .3, "memory_unknown", PlacementSignals{CPUBusy5m: f(.01)}),
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

package panewire

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

// hk 4465: the 2026-11-13 04:00:57Z placement (task 4172). Under
// `free*power - 10*bridged` oci scored +0.05 and won at 99.8 % CPU busy,
// because every other host's score was negative; the all-loaded rule then
// broke the tie by memory. rpi is the only host with free >= 0.05.
//
//	host     free   power  claims  base score
//	mac-dev  0.021  100    3       -27.9
//	desktop  0.018   50    2       -19.1
//	rpi      0.218   12    2       -17.4
//	oci      0.0017  30    0        +0.05
func TestPlacementFixture0400FloorChoosesTheOnlyHostAboveIt(t *testing.T) {
	type host struct {
		free, power float64
		claims      int
	}
	hosts := map[string]host{
		"mac-dev": {0.021, 100, 3},
		"desktop": {0.018, 50, 2},
		"rpi":     {0.218, 12, 2},
		"oci":     {0.0017, 30, 0},
	}
	cpu, mem, zero, one := map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}
	power := map[string]float64{}
	for name, h := range hosts {
		// The jobs term is 1/(1+claims/8); the cpu term makes up the rest of free.
		jobs := 1 / (1 + float64(h.claims)/8)
		cpu[name] = fmt.Sprintf("%.6f", 1-h.free/jobs)
		mem[name], zero[name], one[name] = "0.9", "0", "1"
		power[name] = h.power
	}
	prom := newCountFixtureProm(t, map[string]map[string]string{
		placementCPUBusyQuery: cpu, placementCPUBusy1mQuery: maps2(cpu), placementMemAvailableQuery: mem,
		placementSwapInQuery: zero, placementThermalQuery: one,
	})
	policy := PlacementPolicy{LocalMachine: "mac-dev", SpillTargets: []string{"desktop", "rpi", "oci"}, MaxActiveJobs: 50, Power: power}
	hub := placementPowerHub(t, prom.server.URL, policy)
	now := time.Now().UTC()
	for name, h := range hosts {
		hub.connect(name, "test", "fixture", &hubAgent{}, true)
		jobs := make([]HubActiveJob, h.claims)
		for i := range jobs {
			jobs[i] = HubActiveJob{JobID: "claimed-" + name + string(rune('a'+i)), AgentLabel: "agent", Epoch: 1, StartedAt: now.Add(-time.Minute).Format(time.RFC3339)}
		}
		hub.observeActiveJobs(name, jobs, now)
	}
	got := hub.placement(t.Context(), "worker", "repo")
	for name, h := range hosts {
		c, ok := t607Candidate(got, name)
		if !ok || math.Abs(c.Signals.Free-h.free) > 5e-4 {
			t.Fatalf("%s free=%v, want about %v", name, c.Signals.Free, h.free)
		}
		// The hub tells the spawn engine which hosts are under the floor, so the
		// engine can rank them after the others whatever their load bucket.
		if c.Signals.BelowFreeFloor != (name != "rpi") {
			t.Errorf("%s below_free_floor=%v", name, c.Signals.BelowFreeFloor)
		}
	}
	// Each claim costs 10*100/power: 30, 40, about 167 and 0.
	want := map[string]float64{"mac-dev": 2.1 - 30, "desktop": .9 - 40, "rpi": 2.616 - 2*1000.0/12, "oci": .051}
	for name, w := range want {
		if c, _ := t607Candidate(got, name); math.Abs(c.Score-w) > .05 {
			t.Errorf("%s score=%v, want about %v", name, c.Score, w)
		}
	}
	if got.Decision != "rpi" || got.Reason != "below_free_floor" {
		t.Fatalf("decision=%s reason=%q, want rpi below_free_floor (the base chose oci)", got.Decision, got.Reason)
	}
}

// Every host is under the floor and a burst of jobs is decided one after the
// other through the hub's own placement path (countPlacement charges each job to
// the host it was decided onto): the all-below pick must spread them, not send
// every job to the host with the best free*power.
func TestPlacementAllBelowFloorBurstSpreadsThroughThePlacementPath(t *testing.T) {
	free := map[string]float64{"mac-dev": 0.04, "rpi": 0.03, "desktop": 0.02}
	cpu, mem, zero, one := map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}
	for name, f := range free {
		cpu[name], mem[name], zero[name], one[name] = fmt.Sprintf("%.4f", 1-f), "0.9", "0", "1"
	}
	prom := newCountFixtureProm(t, map[string]map[string]string{
		placementCPUBusyQuery: cpu, placementCPUBusy1mQuery: maps2(cpu), placementMemAvailableQuery: mem,
		placementSwapInQuery: zero, placementThermalQuery: one,
	})
	policy := PlacementPolicy{LocalMachine: "mac-dev", SpillTargets: []string{"rpi", "desktop"}, MaxActiveJobs: 50}
	hub := placementPowerHub(t, prom.server.URL, policy)
	for name := range free {
		hub.connect(name, "test", "fixture", &hubAgent{}, true)
	}
	counts := map[string]int{}
	for i := range 9 {
		got := hub.placementFresh(t.Context(), "worker", "repo")
		if got.Reason != "all_loaded" {
			t.Fatalf("decision %d: reason %q, want all_loaded", i+1, got.Reason)
		}
		counts[got.Decision]++
		hub.countPlacement(fmt.Sprintf("burst-%d", i), got.Decision, "")
	}
	for name, n := range counts {
		if n < 2 || n > 4 {
			t.Fatalf("the burst of 9 went %v: %s took %d", counts, name, n)
		}
	}
}

// The free-floor flag rides in signals only for a caller that asks for it with
// floor=1: the previous release's `place --signals` decodes strictly and would
// reject a field it does not know.
func TestPlacementWireOffersTheFloorFlagOnlyOnRequest(t *testing.T) {
	result := PlacementResult{Decision: "a", Candidates: []PlacementCandidate{
		{Machine: "a", Signals: &PlacementSignals{Free: 0.01, BelowFreeFloor: true}},
		{Machine: "b", Signals: &PlacementSignals{Free: 0.5}},
	}}
	marshal := func(r PlacementResult) string {
		raw, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	if got := marshal(placementWire(result, true, false)); strings.Contains(got, "below_free_floor") || !strings.Contains(got, `"free":0.01`) {
		t.Fatalf("signals=1 alone must not carry the flag: %s", got)
	}
	if got := marshal(placementWire(result, true, true)); !strings.Contains(got, `"below_free_floor":true`) {
		t.Fatalf("floor=1 must carry it: %s", got)
	}
	if got := marshal(placementWire(result, false, true)); strings.Contains(got, "below_free_floor") || strings.Contains(got, "signals") {
		t.Fatalf("no signals, no flag: %s", got)
	}
	if !result.Candidates[0].Signals.BelowFreeFloor {
		t.Fatal("placementWire changed the cached result")
	}
}

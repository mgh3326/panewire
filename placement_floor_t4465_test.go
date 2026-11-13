package panewire

import (
	"fmt"
	"math"
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

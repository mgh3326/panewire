package panewire

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// placementPowerProm serves per-query vectors: busy answers the CPU
// utilisation query, load5 answers a legacy node_load5 query (so a regression
// to load5/cores is observable), and every other query reports no throttle.
func placementPowerProm(t *testing.T, busy, load5 map[string]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		samples := map[string]string{"mac-work": "1", "desktop": "1"}
		switch {
		case strings.Contains(query, "swapped_in") || strings.Contains(query, "pswpin"):
			samples = map[string]string{"mac-work": "0", "desktop": "0"}
		case strings.Contains(query, "node_load5"):
			samples = load5
		case strings.Contains(query, "node_cpu_seconds_total"):
			samples = busy
		}
		result := []any{}
		for machine, value := range samples {
			result = append(result, map[string]any{"metric": map[string]string{"machine_id": machine}, "value": []any{float64(1), value}})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "vector", "result": result}})
	}))
}

func placementPowerHub(t *testing.T, promURL string, policy PlacementPolicy) *HubServer {
	t.Helper()
	path := filepath.Join(t.TempDir(), "placement.json")
	data, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": r6OperatorToken, "mac-work": r6NodeAToken, "desktop": r6NodeBToken}, PlacementPolicyPath: path, PrometheusURL: promURL})
	if err != nil {
		t.Fatal(err)
	}
	return hub
}

func placementPowerJobs(machine string, count int) []HubActiveJob {
	jobs := make([]HubActiveJob, count)
	for i := range jobs {
		jobs[i] = HubActiveJob{JobID: "job-" + machine + string(rune('a'+i)), AgentLabel: "agent", Epoch: 1, OwnerLane: "owner-" + string(rune('a'+i))}
	}
	return jobs
}

// The load query must be CPU utilisation, not load5: macOS counts blocked
// threads in load5, so it is not a measure of how busy the host is.
func TestPlacementLoadQueryIsCPUUtilisation(t *testing.T) {
	// The metrics read issues its queries concurrently.
	var mu sync.Mutex
	var queries []string
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		queries = append(queries, r.URL.Query().Get("query"))
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "vector", "result": []any{}}})
	}))
	defer prom.Close()
	hub := placementHub(t, prom.URL)
	_, _ = hub.fetchPlacementMetrics(t.Context())
	mu.Lock()
	defer mu.Unlock()
	var cpu []string
	for _, query := range queries {
		if strings.Contains(query, "node_cpu_seconds_total") {
			cpu = append(cpu, query)
		}
		if strings.Contains(query, "node_load") {
			t.Fatalf("load averages must not feed placement: %q", query)
		}
	}
	// Two CPU windows: five minutes and the last scrape interval.
	if len(cpu) != 2 {
		t.Fatalf("cpu queries=%q", cpu)
	}
	for _, query := range cpu {
		for _, want := range []string{`mode="idle"`, "rate(", "avg by (machine_id)", "1 -"} {
			if !strings.Contains(query, want) {
				t.Fatalf("cpu query missing %q: %q", want, query)
			}
		}
	}
}

func TestPlacementCPUBusyFractionParsedPerMachine(t *testing.T) {
	prom := placementPowerProm(t, map[string]string{"mac-work": "0.0425", "desktop": "0.9"}, nil)
	defer prom.Close()
	hub := placementHub(t, prom.URL)
	metrics, err := hub.fetchPlacementMetrics(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got := metrics.load["mac-work"]; got != 0.0425 {
		t.Fatalf("mac-work busy=%v, want 0.0425 (%v)", got, metrics.load)
	}
	if got := metrics.load["desktop"]; got != 0.9 {
		t.Fatalf("desktop busy=%v, want 0.9", got)
	}
}

// One macOS host showed load5 54.7 on 10 cores while barely busy. Under the
// old load5/cores signal (5.47) it was excluded; CPU utilisation keeps it local.
func TestPlacementHighLoad5LowCPUStaysLocal(t *testing.T) {
	prom := placementPowerProm(t, map[string]string{"mac-work": "0.08", "desktop": "0.10"}, map[string]string{"mac-work": "5.47", "desktop": "0.1"})
	defer prom.Close()
	hub := placementPowerHub(t, prom.URL, PlacementPolicy{LocalMachine: "mac-work", SpillTargets: []string{"desktop"}, MaxActiveJobs: 5, LoadRatio: .5})
	hub.connect("mac-work", "test", "fixture", &hubAgent{}, true)
	hub.connect("desktop", "test", "fixture", &hubAgent{}, true)
	got := hub.placement(t.Context(), "worker", "repo")
	if got.Decision != "mac-work" || got.Source != "prometheus" {
		t.Fatalf("high load5 with low cpu must stay local: %+v", got)
	}
	local, _ := t607Candidate(got, "mac-work")
	if local.LoadRatio != 0.08 || strings.Contains(local.Reason, "load_ratio") {
		t.Fatalf("local candidate=%+v", local)
	}
}

// load_ratio keeps meaning "five-minute busy fraction" on the wire, but a
// busy CPU only ranks the host lower: a stale load_ratio threshold in the
// policy filters nothing.
func TestPlacementBusyCPURanksLowerWithoutFilter(t *testing.T) {
	prom := placementPowerProm(t, map[string]string{"mac-work": "0.62", "desktop": "0.10"}, nil)
	defer prom.Close()
	hub := placementPowerHub(t, prom.URL, PlacementPolicy{LocalMachine: "mac-work", SpillTargets: []string{"desktop"}, MaxActiveJobs: 5, LoadRatio: .5})
	hub.connect("mac-work", "test", "fixture", &hubAgent{}, true)
	hub.connect("desktop", "test", "fixture", &hubAgent{}, true)
	got := hub.placement(t.Context(), "worker", "repo")
	busy, _ := t607Candidate(got, "mac-work")
	if got.Candidates[0].Machine != "desktop" || busy.LoadRatio != .62 || strings.Contains(busy.Reason, "load_ratio") || !placementUsable(busy, PlacementPolicy{MaxActiveJobs: 5}, got.Source) {
		t.Fatalf("busy local must rank lower and stay usable: %+v", got)
	}
}

func TestPlacementScoreBase(t *testing.T) {
	cases := []struct {
		name    string
		free    float64
		bridged int
		power   float64
		want    float64
	}{
		{"idle strongest", 1, 0, 100, 100},
		{"idle half power", 1, 0, 50, 50},
		{"half free strongest", .5, 0, 100, 50},
		{"bridged job costs a tenth of a power-100 host", 1, 1, 100, 90},
		{"bridged job costs a fifth of a power-50 host", 1, 1, 50, 40},
		{"fully busy host", 0, 0, 100, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := placementScoreBase(tc.free, tc.bridged, tc.power); got != tc.want {
				t.Fatalf("score=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestPlacementPowerPolicyParseAndDefault(t *testing.T) {
	policy, err := ParsePlacementPolicy([]byte(`{"local_machine":"mac-work","spill_targets":["desktop"],"max_active_jobs":5,"load_ratio":0.5,"power":{"desktop":25.5}}`))
	if err != nil {
		t.Fatal(err)
	}
	if policy.power("desktop") != 25.5 || policy.power("mac-work") != 100 || policy.power("unlisted") != 100 {
		t.Fatalf("power lookup: %+v", policy.Power)
	}
	for _, power := range []string{"0", "-5", "10001", `"fast"`} {
		raw := `{"local_machine":"mac-work","spill_targets":["desktop"],"max_active_jobs":5,"load_ratio":0.5,"power":{"desktop":` + power + `}}`
		if _, err := ParsePlacementPolicy([]byte(raw)); err == nil {
			t.Fatalf("power %s accepted", power)
		}
	}
	if _, err := ParsePlacementPolicy([]byte(`{"local_machine":"mac-work","max_active_jobs":5,"load_ratio":0.5,"power":{"Bad Name":50}}`)); err == nil {
		t.Fatal("invalid machine id accepted in power")
	}
}

func TestPlacementPowerRanksStrongIdleHostFirst(t *testing.T) {
	prom := placementPowerProm(t, map[string]string{"mac-work": "0.2", "desktop": "0.2"}, nil)
	defer prom.Close()
	hub := placementPowerHub(t, prom.URL, PlacementPolicy{LocalMachine: "mac-work", SpillTargets: []string{"desktop"}, MaxActiveJobs: 5, LoadRatio: .5, Power: map[string]float64{"mac-work": 10, "desktop": 100}})
	hub.connect("mac-work", "test", "fixture", &hubAgent{}, true)
	hub.connect("desktop", "test", "fixture", &hubAgent{}, true)
	got := hub.placement(t.Context(), "worker", "repo")
	if len(got.Candidates) != 2 || got.Candidates[0].Machine != "desktop" {
		t.Fatalf("strong idle host must rank first: %+v", got.Candidates)
	}
	// The low-power host is ranked lower, not excluded.
	weak, ok := t607Candidate(got, "mac-work")
	if !ok || got.Decision != "mac-work" || weak.Reason != "memory_unknown" {
		t.Fatalf("low-power host must stay eligible: decision=%q weak=%+v", got.Decision, weak)
	}
}

// A job older than the spawn bridge is already in the metrics: counting it
// again was the stale-job-count bug, so it costs nothing, whatever the power.
func TestPlacementSettledJobsDoNotCost(t *testing.T) {
	prom := placementPowerProm(t, map[string]string{"mac-work": "0.2", "desktop": "0.2"}, nil)
	defer prom.Close()
	hub := placementPowerHub(t, prom.URL, PlacementPolicy{LocalMachine: "mac-work", SpillTargets: []string{"desktop"}, MaxActiveJobs: 50, Power: map[string]float64{"mac-work": 20}})
	hub.connect("mac-work", "test", "fixture", &hubAgent{}, true)
	hub.connect("desktop", "test", "fixture", &hubAgent{}, true)
	settled := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
	jobs := placementPowerJobs("mac-work", 20)
	for i := range jobs {
		jobs[i].StartedAt = settled
	}
	hub.observeActiveJobs("mac-work", jobs, time.Now().UTC())
	got := hub.placement(t.Context(), "worker", "repo")
	weak, _ := t607Candidate(got, "mac-work")
	strong, _ := t607Candidate(got, "desktop")
	if weak.ActiveJobs != 20 || weak.Signals.BridgedJobs != 0 || weak.Score != placementScoreBase(.8, 0, 20) || strong.Score != placementScoreBase(.8, 0, 100) {
		t.Fatalf("settled jobs changed the score: weak=%+v %+v strong=%+v", weak, weak.Signals, strong)
	}
}

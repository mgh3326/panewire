package panewire

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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
	var queries []string
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.Query().Get("query"))
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "vector", "result": []any{}}})
	}))
	defer prom.Close()
	hub := placementHub(t, prom.URL)
	_, _ = hub.fetchPlacementMetrics(t.Context())
	var load string
	for _, query := range queries {
		if strings.Contains(query, "node_cpu_seconds_total") {
			load = query
		}
		if strings.Contains(query, "node_load5") {
			t.Fatalf("load5 must not feed placement: %q", query)
		}
	}
	for _, want := range []string{`mode="idle"`, "rate(", "[5m]", "avg by (machine_id)", "1 -"} {
		if !strings.Contains(load, want) {
			t.Fatalf("cpu query missing %q: %q", want, load)
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

// load_ratio keeps meaning "busy fraction": a genuinely busy CPU still spills.
func TestPlacementBusyCPUSpills(t *testing.T) {
	prom := placementPowerProm(t, map[string]string{"mac-work": "0.62", "desktop": "0.10"}, nil)
	defer prom.Close()
	hub := placementPowerHub(t, prom.URL, PlacementPolicy{LocalMachine: "mac-work", SpillTargets: []string{"desktop"}, MaxActiveJobs: 5, LoadRatio: .5})
	hub.connect("mac-work", "test", "fixture", &hubAgent{}, true)
	hub.connect("desktop", "test", "fixture", &hubAgent{}, true)
	if got := hub.placement(t.Context(), "worker", "repo"); got.Decision != "desktop" {
		t.Fatalf("busy local must spill: %+v", got)
	}
}

func TestPlacementScoreBase(t *testing.T) {
	cases := []struct {
		name  string
		load  float64
		jobs  int
		power float64
		want  float64
	}{
		{"idle strongest", 0, 0, 100, 105},
		{"idle half power", 0, 0, 50, 102.5},
		{"one job strongest", 0, 1, 100, 95},
		{"one job half power doubles the penalty", 0, 1, 50, 82.5},
		{"one job double power halves the penalty", 0, 1, 200, 105},
		{"busy host earns no spare bonus", 1, 0, 100, 0},
		{"load above one is clamped for the bonus", 2, 0, 100, -100},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := placementScoreBase(tc.load, tc.jobs, tc.power); got != tc.want {
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

func TestPlacementPowerScalesJobPenalty(t *testing.T) {
	prom := placementPowerProm(t, map[string]string{"mac-work": "0.2", "desktop": "0.2"}, nil)
	defer prom.Close()
	hub := placementPowerHub(t, prom.URL, PlacementPolicy{LocalMachine: "mac-work", SpillTargets: []string{"desktop"}, MaxActiveJobs: 5, LoadRatio: .5, Power: map[string]float64{"mac-work": 20}})
	hub.connect("mac-work", "test", "fixture", &hubAgent{}, true)
	hub.connect("desktop", "test", "fixture", &hubAgent{}, true)
	hub.observeActiveJobs("mac-work", placementPowerJobs("mac-work", 1), time.Now().UTC())
	hub.observeActiveJobs("desktop", placementPowerJobs("desktop", 1), time.Now().UTC())
	got := hub.placement(t.Context(), "worker", "repo")
	weak, _ := t607Candidate(got, "mac-work")
	strong, _ := t607Candidate(got, "desktop")
	// Same load and one job each: the power-20 host pays 5x the job penalty.
	if want := placementScoreBase(.2, 1, 20); weak.Score != want {
		t.Fatalf("weak score=%v, want %v", weak.Score, want)
	}
	if want := placementScoreBase(.2, 1, 100); strong.Score != want {
		t.Fatalf("strong score=%v, want %v", strong.Score, want)
	}
	if strong.Score-weak.Score < 40 {
		t.Fatalf("job penalty not scaled by power: strong=%v weak=%v", strong.Score, weak.Score)
	}
}

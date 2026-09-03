package panewire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPlacementPrometheusPolicyBranches(t *testing.T) {
	cases := []struct {
		name      string
		load      float64
		thermal   float64
		localJobs int
		desktop   bool
		want      string
	}{
		{"local capacity", .23, 1, 0, true, "mac-work"},
		{"local overloaded", .53, 1, 0, true, "desktop"},
		{"thermal throttle", .12, .89, 0, true, "desktop"}, // Mutant ①: deleting throttle eligibility makes this RED.
		{"active job maximum", .12, 1, 5, true, "desktop"},
		{"spill unavailable", .53, 1, 0, false, "mac-work"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			prom := placementPromServer(t, tc.load, tc.thermal, nil)
			defer prom.Close()
			hub := placementHub(t, prom.URL)
			hub.connect("mac-work", "test", "fixture", &hubAgent{}, true)
			if tc.desktop {
				hub.connect("desktop", "test", "fixture", &hubAgent{}, true)
			}
			jobs := make([]HubActiveJob, tc.localJobs)
			for i := range jobs {
				jobs[i] = HubActiveJob{JobID: "job-" + string(rune('a'+i)), AgentLabel: "wrk", Epoch: 1}
			}
			hub.observeActiveJobs("mac-work", jobs, time.Now().UTC())
			got := hub.placement(t.Context(), "worker", "repo")
			if got.Decision != tc.want || got.Source != "prometheus" {
				t.Fatalf("decision=%+v", got)
			}
		})
	}
}

func TestPlacementHubOnlyNeverFailsAndCaches(t *testing.T) {
	var calls atomic.Int32
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "down", http.StatusServiceUnavailable)
	}))
	defer prom.Close()
	hub := placementHub(t, prom.URL)
	hub.connect("mac-work", "test", "fixture", &hubAgent{}, true)
	first := hub.placement(t.Context(), "worker", "repo")
	second := hub.placement(t.Context(), "worker", "repo")
	if first.Source != "hub-only" || first.Decision != "mac-work" || second.Source != "hub-only" {
		t.Fatalf("must degrade: first=%+v second=%+v", first, second)
	} // Mutant ②: returning a Prometheus failure turns this RED.
	if got := calls.Load(); got != 1 {
		t.Fatalf("prometheus calls=%d, want cached 1", got)
	}
}

func TestPlacementOperatorAuthentication(t *testing.T) {
	hub := placementHub(t, "")
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/placement?class=worker", nil)
	hub.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d", recorder.Code)
	} // Mutant ③: removing authorizeOperator makes this RED.
}

func TestPlaceCLIJSONAndExplain(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/placement" || r.URL.Query().Get("class") != "worker" || r.Header.Get("Authorization") != "Bearer "+r6OperatorToken {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(PlacementResult{Decision: "desktop", Source: "hub-only", Asof: time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC), Candidates: []PlacementCandidate{{Machine: "desktop", Score: 90, Connected: true, Reason: "available"}}})
	}))
	defer server.Close()
	tokenPath := filepath.Join(t.TempDir(), "operator.env")
	if err := os.WriteFile(tokenPath, []byte("HUB_MACHINE_ID=operator\nHUB_TOKEN="+r6OperatorToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	deps := hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true}
	var output, stderr bytes.Buffer
	args := []string{"--class", "worker", "--cwd", "repo", "--hub-url", server.URL, "--hub-token-env", tokenPath}
	if code := runPlaceCLI(args, &output, &stderr, deps); code != ExitOK || !strings.Contains(output.String(), `"decision":"desktop"`) {
		t.Fatalf("code=%d output=%q stderr=%q", code, output.String(), stderr.String())
	}
	output.Reset()
	if code := runPlaceCLI(append(args, "--explain"), &output, &stderr, deps); code != ExitOK || !strings.Contains(output.String(), "MACHINE\tSCORE") {
		t.Fatalf("explain code=%d output=%q", code, output.String())
	}
}

func placementHub(t *testing.T, promURL string) *HubServer {
	t.Helper()
	path := filepath.Join(t.TempDir(), "placement.json")
	policy := PlacementPolicy{LocalMachine: "mac-work", SpillTargets: []string{"desktop"}, MaxActiveJobs: 5, LoadRatio: .5}
	data, _ := json.Marshal(policy)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": r6OperatorToken, "mac-work": r6NodeAToken, "desktop": r6NodeBToken}, PlacementPolicyPath: path, PrometheusURL: promURL})
	if err != nil {
		t.Fatal(err)
	}
	return hub
}

func placementPromServer(t *testing.T, load, thermal float64, calls *atomic.Int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls != nil {
			calls.Add(1)
		}
		query := r.URL.Query().Get("query")
		value := load
		if strings.Contains(query, "thermal") {
			value = thermal
		}
		if strings.Contains(query, "MemAvailable") {
			value = 1024
		}
		desktop := "0.10"
		if strings.Contains(query, "thermal") {
			desktop = "1"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "vector", "result": []any{
			map[string]any{"metric": map[string]string{"machine_id": "mac-work"}, "value": []any{float64(1), formatPlacementValue(value)}},
			map[string]any{"metric": map[string]string{"machine_id": "desktop"}, "value": []any{float64(1), desktop}},
		}}})
	}))
}

func formatPlacementValue(value float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.4f", value), "0"), ".")
}

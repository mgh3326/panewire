package fleet

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mgh3326/panewire/internal/hubretry"
)

const statusTestOperation = "00000000-0000-4000-8000-000000000001"

func TestWaitSpawnContinuesThroughBound_t4577(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			json.NewEncoder(w).Encode(map[string]any{"status": "bound"})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"status": "succeeded", "result": map[string]any{"host": "node-b"}})
	}))
	defer server.Close()
	c := hubHTTP{base: server.URL, token: "fixture", client: server.Client(), statusPoll: 1}
	attempt, err := c.WaitSpawn(t.Context(), statusTestOperation)
	if err != nil || attempt.Host != "node-b" {
		t.Fatalf("bound operation stopped the wait: attempt=%+v err=%v", attempt, err)
	}
}

func TestWaitSpawnRefusalNamesHostAndProfile(t *testing.T) {
	for name, test := range map[string]struct {
		result map[string]any
		want   string
	}{
		"node refused": {map[string]any{"rc": 2, "reason": "cwd_unmapped", "host": "node-b", "profile": "sonnet"}, "host=node-b profile=sonnet"},
		"never placed": {map[string]any{"rc": 79, "reason": "configuration"}, "host=none profile=none"},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]any{"status": "failed", "result": test.result})
			}))
			defer server.Close()
			c := hubHTTP{base: server.URL, token: "fixture", client: server.Client()}
			_, err := c.WaitSpawn(t.Context(), statusTestOperation)
			if err == nil || !strings.Contains(err.Error(), test.want) || !strings.Contains(err.Error(), statusTestOperation) {
				t.Fatalf("err=%v want %q", err, test.want)
			}
		})
	}
}

func TestWaitSpawnSuccessReportsWhereItLanded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"status": "succeeded", "result": map[string]any{"rc": 0, "host": "node-b", "profile": "sonnet", "pane": "w:p9"}})
	}))
	defer server.Close()
	c := hubHTTP{base: server.URL, token: "fixture", client: server.Client()}
	attempt, err := c.WaitSpawn(t.Context(), statusTestOperation)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	attempt.print(&out, "job")
	if got := out.String(); got != "SPAWNED job=job host=node-b profile=sonnet pane=w:p9\n" {
		t.Fatalf("output=%q", got)
	}
}

func TestWaitSpawnCarriesUnregisteredLane(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"status": "succeeded", "result": map[string]any{"rc": 0, "host": "node-b", "profile": "sonnet", "pane": "w:p9", "lane_unregistered": true}})
	}))
	defer server.Close()
	c := hubHTTP{base: server.URL, token: "fixture", client: server.Client()}
	attempt, err := c.WaitSpawn(t.Context(), statusTestOperation)
	if err != nil || !attempt.LaneUnregistered {
		t.Fatalf("attempt=%+v err=%v", attempt, err)
	}
}

// One lost status read used to answer rc75 while the operation kept running.
func TestWaitSpawnSurvivesTransientStatusFailures(t *testing.T) {
	for name, fault := range map[string]func(http.ResponseWriter){
		"bad gateway":  func(w http.ResponseWriter) { w.WriteHeader(http.StatusBadGateway) },
		"dropped conn": func(w http.ResponseWriter) { panic(http.ErrAbortHandler) },
	} {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) <= 3 {
					fault(w)
					return
				}
				json.NewEncoder(w).Encode(map[string]any{"status": "succeeded", "result": map[string]any{"rc": 0}})
			}))
			defer server.Close()
			c := hubHTTP{base: server.URL, token: "fixture", client: server.Client(), statusPoll: 1}
			if _, err := c.WaitSpawn(t.Context(), statusTestOperation); err != nil {
				t.Fatalf("transient failure ended the wait: %v", err)
			}
		})
	}
}

func TestWaitSpawnSustainedOutageStillAnswersUnavailable(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	// The per-read retry is off so this counts the status reads WaitSpawn
	// itself makes before it gives up; the retry is covered by hub_retry_test.go.
	c := hubHTTP{base: server.URL, token: "fixture", client: server.Client(), statusPoll: 1, retry: hubretry.Policy{Budget: -1}}
	_, err := c.WaitSpawn(t.Context(), statusTestOperation)
	if ErrorCode(err) != exitStateConflict || !strings.Contains(err.Error(), "spawn status unavailable") {
		t.Fatalf("err=%v", err)
	}
	if got := int(calls.Load()); got != spawnStatusFailureLimit+1 {
		t.Fatalf("calls=%d", got)
	}
}

// A definitive answer is not a blip: retrying a 404 hides a wrong operation id.
func TestWaitSpawnDefinitiveStatusIsNotRetried(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	c := hubHTTP{base: server.URL, token: "fixture", client: server.Client(), statusPoll: 1}
	if _, err := c.WaitSpawn(t.Context(), statusTestOperation); ErrorCode(err) != exitStateConflict {
		t.Fatalf("err=%v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls=%d", calls.Load())
	}
}

// The hub coordinator has no local seat, so every candidate refusing must not
// read as "enable local spawns", and the refusal must say who was asked.
func TestPlacementExhaustionNamesTriedHosts(t *testing.T) {
	p := &placePlan{candidates: []placeCandidate{{name: "node-a", outcome: "hub_cwd_unmapped"}, {name: "node-b", outcome: "hub_cwd_unmapped"}, {name: "node-c"}}}
	got := p.triedNotes()
	if got != " (tried: node-a hub_cwd_unmapped; node-b hub_cwd_unmapped)" {
		t.Fatalf("notes=%q", got)
	}
	hint := spawnRefusalFix(fail(exitLocalDisabled, "local spawns are disabled on this host; no remote candidate completed%s", got))
	// Both causes stay named: a node refusal or a client with local.spawn off.
	if strings.Contains(hint, "Pass -p") || !strings.Contains(hint, "local.spawn") || !strings.Contains(hint, "spawn.json") {
		t.Fatalf("hint=%q", hint)
	}
}

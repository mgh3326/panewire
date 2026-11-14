package panewire

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 4615: the hub keeps a durable placement snapshot per attempt at bind
// and delivers the hk placement record through an idempotent outbox when the
// attempt reaches a terminal result.

type placementOutboxHK struct {
	mu       sync.Mutex
	requests []map[string]any
	server   *httptest.Server
}

func placementOutboxServer(t *testing.T) *placementOutboxHK {
	t.Helper()
	h := &placementOutboxHK{}
	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/placements" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		b, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(b, &body)
		h.mu.Lock()
		h.requests = append(h.requests, body)
		h.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]string{"status": "recorded"})
	}))
	t.Cleanup(h.server.Close)
	isolateRootSvcCred(t)
	t.Setenv("HANDOFFKEEP_URL", h.server.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "svc-placement-test")
	return h
}

func (h *placementOutboxHK) records() []map[string]any {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]map[string]any(nil), h.requests...)
}

// placementOutboxHub builds a hub whose record delivery posts to the test hk
// server and whose state dir is disposable.
func placementOutboxHub(t *testing.T, storePath string) *HubServer {
	t.Helper()
	t.Setenv(crashReportDSNEnv, "")
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken}, InboxStorePath: storePath})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	stateDir := t.TempDir()
	if real, e := filepath.EvalSymlinks(stateDir); e == nil {
		stateDir = real
	}
	h.placementAppFn = func() (*fleet.App, error) {
		return &fleet.App{StateDir: stateDir, Out: io.Discard, Err: io.Discard}, nil
	}
	return h
}

// placementTraceSnapshot is the trace the placing App emits at bind.
func placementTraceSnapshot() fleet.PlacementTrace {
	return fleet.PlacementTrace{
		DecidedAt:   "2026-11-14T00:00:00Z",
		Source:      "auto",
		Ranking:     "score",
		StateSource: "hub",
		Pool:        "claude-sub",
		Family:      "claude",
		Profile:     "claude-sonnet",
		Role:        "worker",
		TaskID:      42,
		Chosen:      "machine-a",
		Candidates: []fleet.PlacementCandidateRecord{
			{Host: "machine-b", Outcome: "skipped", Reason: "not_chosen"},
			{Host: "machine-a", Outcome: "chosen", Reason: "available"},
		},
	}
}

// boundSpawnTask claims a request and binds it to machine-a, the state a
// real spawn has when its placement snapshot is taken.
func boundSpawnTask(t *testing.T, h *HubServer, req fleet.Submission) hubSpawnTask {
	t.Helper()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), req, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	task, err = h.inbox.store.bindSpawn(t.Context(), req.RequestID, task.Attempt, hubSpawnRequest{RequestID: "node-req-1", Machine: "machine-a"}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func TestHubPlacementRecordTerminalOutcomes(t *testing.T) {
	hk := placementOutboxServer(t)
	for name, tc := range map[string]struct {
		outcome hubSpawnOutcome
		want    string
		reason  string
	}{
		"landed":  {hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "pane-7", Profile: "claude-sonnet", Model: "sonnet", Pool: "claude-sub", NodeResult: true}, "placed", ""},
		"failed":  {hubSpawnOutcome{RC: 1, Stage: "landing", Reason: "spawn_failed:launch", Host: "machine-a", NodeResult: true}, "failed", "spawn_failed:launch"},
		"refused": {hubSpawnOutcome{RC: 70, Stage: "placement", Reason: "spawn_failed:trust_prompt", Host: "machine-a", NodeResult: true}, "refused", "spawn_failed:trust_prompt"},
	} {
		t.Run(name, func(t *testing.T) {
			h := placementOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
			req := durableSpawnRequest()
			req.RequestID = map[string]string{"landed": "d676220b-e763-4af1-aefd-000000000011", "failed": "d676220b-e763-4af1-aefd-000000000012", "refused": "d676220b-e763-4af1-aefd-000000000013"}[name]
			task := boundSpawnTask(t, h, req)
			h.notePlacementTrace(task, placementTraceSnapshot())
			changed, err := h.completeSpawn(t.Context(), task, tc.outcome)
			if err != nil || !changed {
				t.Fatalf("completeSpawn changed=%v err=%v", changed, err)
			}
			records := hk.records()
			var rec map[string]any
			for _, r := range records {
				if r["job_id"] == "test-job" {
					rec = r
				}
			}
			if rec == nil {
				t.Fatalf("no placement record delivered; records=%v", records)
			}
			if rec["outcome"] != tc.want || rec["attempt"].(float64) != 1 || rec["chosen_host"] != "machine-a" {
				t.Fatalf("record=%v", rec)
			}
			if rec["reason"] != tc.reason {
				t.Fatalf("reason=%v, want %q", rec["reason"], tc.reason)
			}
			candidates, _ := rec["candidates"].([]any)
			var chosen map[string]any
			for _, raw := range candidates {
				c := raw.(map[string]any)
				if c["host"] == "machine-a" {
					chosen = c
				}
			}
			if chosen == nil || len(candidates) != 2 {
				t.Fatalf("original trace missing from candidates=%v", candidates)
			}
		})
	}
}

func TestHubPlacementRecordSurvivesRestartExactlyOnce(t *testing.T) {
	hk := placementOutboxServer(t)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	req := durableSpawnRequest()
	req.RequestID = "d676220b-e763-4af1-aefd-000000000014"
	h1 := placementOutboxHub(t, path)
	task := boundSpawnTask(t, h1, req)
	h1.notePlacementTrace(task, placementTraceSnapshot())
	// The hub restarts between bind and the node's answer: only the durable
	// snapshot survives.
	h1.Close()
	h2 := placementOutboxHub(t, path)
	current, err := h2.inbox.store.spawnTask(t.Context(), req.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := h2.completeSpawn(t.Context(), current, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "pane-7", NodeResult: true})
	if err != nil || !changed {
		t.Fatalf("completeSpawn changed=%v err=%v", changed, err)
	}
	if n := len(hk.records()); n != 1 {
		t.Fatalf("records=%d, want exactly one", n)
	}
	// A duplicate delivery is a no-op: the recorded flag seals the snapshot.
	h2.deliverPlacementRecord(t.Context(), req.RequestID, current.Attempt)
	h2.notePlacementTrace(current, placementTraceSnapshot())
	if n := len(hk.records()); n != 1 {
		t.Fatalf("duplicate delivery produced %d records", n)
	}
}

func TestHubPlacementSnapshotLookupByRequestAndAttempt(t *testing.T) {
	s := openSpawnStore(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	payload, _ := json.Marshal(placementTraceSnapshot())
	if err := s.saveSpawnPlacementSnapshot(t.Context(), "req-lookup", 2, payload); err != nil {
		t.Fatal(err)
	}
	got, recorded, err := s.spawnPlacementSnapshot(t.Context(), "req-lookup", 2)
	if err != nil || recorded || string(got) != string(payload) {
		t.Fatalf("snapshot=%s recorded=%v err=%v", got, recorded, err)
	}
	// A recorded attempt's snapshot ignores later writes.
	if err := s.markSpawnPlacementRecorded(t.Context(), "req-lookup", 2); err != nil {
		t.Fatal(err)
	}
	if err := s.saveSpawnPlacementSnapshot(t.Context(), "req-lookup", 2, []byte(`{"stale":true}`)); err != nil {
		t.Fatal(err)
	}
	got, recorded, err = s.spawnPlacementSnapshot(t.Context(), "req-lookup", 2)
	if err != nil || !recorded || string(got) != string(payload) {
		t.Fatalf("sealed snapshot=%s recorded=%v err=%v", got, recorded, err)
	}
	if payload, recorded, err := s.spawnPlacementSnapshot(t.Context(), "req-absent", 1); err != nil || recorded || payload != nil {
		t.Fatalf("absent snapshot=%s recorded=%v err=%v", payload, recorded, err)
	}
}

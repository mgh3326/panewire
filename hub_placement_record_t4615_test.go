package panewire

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 4615: the hub keeps a durable placement snapshot per attempt at bind
// and delivers the hk placement record through an idempotent outbox when the
// attempt reaches a terminal result.

type placementOutboxHK struct {
	mu           sync.Mutex
	requests     []map[string]any
	lookup       []fleet.PlacementRecord
	gets         int
	lookupStatus int
	lookupHook   func()
	lookupHold   <-chan struct{}
	methods      []string
	lookupJob    string
	lookupJobs   []string
	server       *httptest.Server
}

func placementOutboxServer(t *testing.T) *placementOutboxHK {
	t.Helper()
	h := &placementOutboxHK{}
	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/placements" && r.Method == http.MethodGet {
			h.mu.Lock()
			h.gets++
			h.methods = append(h.methods, "GET")
			h.lookupJob = r.URL.Query().Get("job")
			h.lookupJobs = append(h.lookupJobs, h.lookupJob)
			status := h.lookupStatus
			hook := h.lookupHook
			hold := h.lookupHold
			h.lookupHook = nil
			found := append([]fleet.PlacementRecord{}, h.lookup...)
			h.mu.Unlock()
			if hook != nil {
				hook()
			}
			if hold != nil {
				// Simulate an hk lookup that never answers before its deadline.
				select {
				case <-hold:
				case <-r.Context().Done():
					return
				}
			}
			if status != 0 {
				w.WriteHeader(status)
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"placements": found})
			return
		}
		if r.URL.Path != "/v1/placements" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		b, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(b, &body)
		h.mu.Lock()
		h.methods = append(h.methods, "POST")
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
		outcome       hubSpawnOutcome
		want          string
		reason        string
		wantHost      string
		wantCandidate string
	}{
		"landed": {hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "pane-7", Profile: "claude-sonnet", Model: "sonnet", Pool: "claude-sub", NodeResult: true}, "placed", "", "machine-a", "chosen"},
		// A failed launch still marks the bound host chosen: the node
		// claimed the job, then the launch failed.
		"failed": {hubSpawnOutcome{RC: 1, Stage: "landing", Reason: "spawn_failed:launch", Host: "machine-a", NodeResult: true}, "failed", "spawn_failed:launch", "machine-a", "chosen"},
		// Contact is not launch acceptance: a refused record names no host.
		"refused": {hubSpawnOutcome{RC: 70, Stage: "placement", Reason: "spawn_failed:trust_prompt", Host: "machine-a", NodeResult: true}, "refused", "spawn_failed:trust_prompt", "", "refused"},
		// A bound attempt starved in the queue before dispatch: no node
		// answer ever arrived, so no host claimed the job — refused.
		"starved": {hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Retryable: true, Host: "machine-a"}, "refused", "timeout", "", "refused"},
	} {
		t.Run(name, func(t *testing.T) {
			h := placementOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
			req := durableSpawnRequest()
			req.RequestID = map[string]string{"landed": "d676220b-e763-4af1-aefd-000000000011", "failed": "d676220b-e763-4af1-aefd-000000000012", "refused": "d676220b-e763-4af1-aefd-000000000013", "starved": "d676220b-e763-4af1-aefd-000000000017"}[name]
			task := boundSpawnTask(t, h, req)
			h.notePlacementTrace(task, placementTraceSnapshot(), "")
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
			if rec["outcome"] != tc.want || rec["attempt"].(float64) != 1 {
				t.Fatalf("record=%v", rec)
			}
			if got, _ := rec["chosen_host"].(string); got != tc.wantHost {
				t.Fatalf("chosen_host=%q, want %q (record=%v)", got, tc.wantHost, rec)
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
			if got := chosen["outcome"]; got != tc.wantCandidate {
				t.Fatalf("bound host candidate outcome=%v, want %q (record=%v)", got, tc.wantCandidate, rec)
			}
		})
	}
}

// The App's final emission carries the record finishPlacement completed —
// before the terminal outcome commits — so delivery waits for that commit
// and then posts the record verbatim, including pane_id and model.
func TestHubPlacementRecordFinishedRecordPostsVerbatim(t *testing.T) {
	hk := placementOutboxServer(t)
	h := placementOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	req := durableSpawnRequest()
	req.RequestID = "d676220b-e763-4af1-aefd-000000000016"
	task := boundSpawnTask(t, h, req)
	tr := placementTraceSnapshot()
	tr.Outcome = "placed"
	tr.Record = &fleet.PlacementRecord{
		JobID: task.Job, Attempt: task.Attempt, TaskID: 42,
		DecidedAt: tr.DecidedAt, ToolVersion: "test",
		Source: "auto", Ranking: "score", Outcome: "placed",
		Candidates:         tr.Candidates,
		PlacementExecution: fleet.PlacementExecution{Host: "machine-a", PaneID: "pane-9", Model: "opus", Pool: "claude-sub", Profile: "claude-sonnet", Family: "claude"},
	}
	h.notePlacementTrace(task, tr, "")
	if records := hk.records(); len(records) != 0 {
		t.Fatalf("non-terminal attempt produced %d posts, want none", len(records))
	}
	changed, err := h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", NodeResult: true}, time.Now())
	if err != nil || !changed {
		t.Fatalf("completeSpawn changed=%v err=%v", changed, err)
	}
	h.notePlacementTrace(task, tr, "")
	records := hk.records()
	if len(records) != 1 {
		t.Fatalf("finished record produced %d posts, want one", len(records))
	}
	rec := records[0]
	if rec["pane_id"] != "pane-9" || rec["model"] != "opus" || rec["chosen_host"] != "machine-a" {
		t.Fatalf("finished record lost execution fields: %v", rec)
	}
}

func TestHubPlacementRecordSurvivesRestartExactlyOnce(t *testing.T) {
	hk := placementOutboxServer(t)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	req := durableSpawnRequest()
	req.RequestID = "d676220b-e763-4af1-aefd-000000000014"
	h1 := placementOutboxHub(t, path)
	task := boundSpawnTask(t, h1, req)
	h1.notePlacementTrace(task, placementTraceSnapshot(), "")
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
	h2.notePlacementTrace(current, placementTraceSnapshot(), "")
	if n := len(hk.records()); n != 1 {
		t.Fatalf("duplicate delivery produced %d records", n)
	}
}

// A hub that committed the terminal result but died before enqueuing must
// still produce the record: the flush rebuilds it from the snapshot instead
// of sealing the row.
func TestHubPlacementRecordRebuiltAfterCrashBeforeEnqueue(t *testing.T) {
	hk := placementOutboxServer(t)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	req := durableSpawnRequest()
	req.RequestID = "d676220b-e763-4af1-aefd-000000000015"
	h := placementOutboxHub(t, path)
	task := boundSpawnTask(t, h, req)
	h.notePlacementTrace(task, placementTraceSnapshot(), "")
	// Commit the terminal status through the store directly, bypassing the
	// hub's deliverPlacementRecord hook — the state a crash leaves behind.
	if _, err := h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "pane-7", NodeResult: true}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if n := len(hk.records()); n != 0 {
		t.Fatalf("record produced before flush: %d", n)
	}
	h.flushPlacementRecords(t.Context())
	if n := len(hk.records()); n != 1 {
		t.Fatalf("flush produced %d records, want exactly one", n)
	}
	_, _, recorded, err := h.inbox.store.spawnPlacementSnapshot(t.Context(), req.RequestID, task.Attempt)
	if err != nil || !recorded {
		t.Fatalf("snapshot recorded=%v err=%v after delivery", recorded, err)
	}
}

func TestHubPlacementSnapshotLookupByRequestAndAttempt(t *testing.T) {
	s := openSpawnStore(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	payload, _ := json.Marshal(placementTraceSnapshot())
	if err := s.saveSpawnPlacementSnapshot(t.Context(), "req-lookup", 2, payload, ""); err != nil {
		t.Fatal(err)
	}
	got, _, recorded, err := s.spawnPlacementSnapshot(t.Context(), "req-lookup", 2)
	if err != nil || recorded || string(got) != string(payload) {
		t.Fatalf("snapshot=%s recorded=%v err=%v", got, recorded, err)
	}
	// A recorded attempt's snapshot ignores later writes.
	if err := s.markSpawnPlacementRecorded(t.Context(), "req-lookup", 2); err != nil {
		t.Fatal(err)
	}
	if err := s.saveSpawnPlacementSnapshot(t.Context(), "req-lookup", 2, []byte(`{"stale":true}`), ""); err != nil {
		t.Fatal(err)
	}
	got, _, recorded, err = s.spawnPlacementSnapshot(t.Context(), "req-lookup", 2)
	if err != nil || !recorded || string(got) != string(payload) {
		t.Fatalf("sealed snapshot=%s recorded=%v err=%v", got, recorded, err)
	}
	if payload, _, recorded, err := s.spawnPlacementSnapshot(t.Context(), "req-absent", 1); err != nil || recorded || payload != nil {
		t.Fatalf("absent snapshot=%s recorded=%v err=%v", payload, recorded, err)
	}
}

// When the snapshot write fails outright, the finished record is spooled and
// the save is deferred to the durable file: after a restart with hk
// reachable again, one flush must land the row and deliver the record.
func TestHubPlacementRecordFallbackDeliveredAfterRecovery(t *testing.T) {
	hk := placementOutboxServer(t)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	stateDir := t.TempDir()
	if real, err := filepath.EvalSymlinks(stateDir); err == nil {
		stateDir = real
	}
	delivery, err := json.Marshal(placementDelivery{StateDir: stateDir})
	if err != nil {
		t.Fatal(err)
	}
	req := durableSpawnRequest()
	req.RequestID = "d676220b-e763-4af1-aefd-000000000018"
	h1 := placementOutboxHub(t, path)
	task := boundSpawnTask(t, h1, req)
	changed, err := h1.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", NodeResult: true}, time.Now())
	if err != nil || !changed {
		t.Fatalf("completeSpawn changed=%v err=%v", changed, err)
	}
	// hk unreachable while the snapshot write fails: the record can only be
	// spooled, and the deferred file keeps the row-insert obligation.
	os.Setenv("HANDOFFKEEP_URL", "http://127.0.0.1:1")
	rec := fleet.PlacementRecord{JobID: "test-job", Attempt: task.Attempt, Outcome: "placed", Host: "machine-a", Family: "claude"}
	tr := placementTraceSnapshot()
	tr.Record = &rec
	payload, err := json.Marshal(tr)
	if err != nil {
		t.Fatal(err)
	}
	h1.deferPlacementTraceSave(req.RequestID, task.Attempt, payload, string(delivery))
	h1.enqueueFinishedPlacementRecord(t.Context(), task, rec, string(delivery))
	if n := len(hk.records()); n != 0 {
		t.Fatalf("record delivered while hk down: %d", n)
	}
	if _, err := os.Stat(h1.inbox.deferredPath); err != nil {
		t.Fatalf("deferred save not persisted: %v", err)
	}
	h1.Close()
	// hk reachable again; the restarted hub knows nothing in memory: the
	// deferred file and the spool are the only state.
	os.Setenv("HANDOFFKEEP_URL", hk.server.URL)
	h2 := placementOutboxHub(t, path)
	h2.flushPlacementRecords(t.Context())
	records := hk.records()
	if len(records) == 0 {
		t.Fatal("flush did not deliver the record")
	}
	for _, record := range records[1:] {
		if !reflect.DeepEqual(records[0], record) {
			t.Fatal("delivery retry changed the immutable record")
		}
	}
	if _, err := os.Stat(h2.inbox.deferredPath); !os.IsNotExist(err) {
		t.Fatalf("deferred file remains after the row landed: %v", err)
	}
	_, _, recorded, err := h2.inbox.store.spawnPlacementSnapshot(t.Context(), req.RequestID, task.Attempt)
	if err != nil || !recorded {
		t.Fatalf("snapshot recorded=%v err=%v after terminal result", recorded, err)
	}
}

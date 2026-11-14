package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/internal/fleet"
)

type dispatchPlacementHK struct {
	mu       sync.Mutex
	records  []fleet.PlacementRecord
	changed  bool
	hold     <-chan struct{}
	started  chan<- struct{}
	timedOut chan<- struct{}
}

type dispatchPlacementOtherHost struct{ retrySource }

type dispatchPlacementOutageSource struct {
	retrySource
	once sync.Once
	fail func()
}

func (s *dispatchPlacementOutageSource) Presence(ctx context.Context) ([]fleet.PresenceRecord, error) {
	s.once.Do(s.fail)
	return s.retrySource.Presence(ctx)
}

func nonemptyDeferredDirectory(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "keep"), []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func (s dispatchPlacementOtherHost) Presence(context.Context) ([]fleet.PresenceRecord, error) {
	return []fleet.PresenceRecord{{HostID: "machine-b", AccountKey: "p:0000000000000001", LastSeen: s.now}}, nil
}

func newDispatchPlacementHK(t *testing.T) *dispatchPlacementHK {
	t.Helper()
	hk := &dispatchPlacementHK{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && (r.URL.Path == "/v1/tasks/42" || r.URL.Path == "/v1/tasks/43"):
			id := strings.TrimPrefix(r.URL.Path, "/v1/tasks/")
			_, _ = io.WriteString(w, `{"id":`+id+`,"title":"work (grade B, T1)","state":"ready","refs":{}}`)
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/documents/"):
			_, _ = io.WriteString(w, `{"document":{},"changed":true}`)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/placements":
			hk.mu.Lock()
			records := append([]fleet.PlacementRecord{}, hk.records...)
			hk.mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"placements": records})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/placements":
			var record fleet.PlacementRecord
			if json.NewDecoder(r.Body).Decode(&record) != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			hk.mu.Lock()
			hold, started, timedOut := hk.hold, hk.started, hk.timedOut
			hk.mu.Unlock()
			if hold != nil {
				select {
				case started <- struct{}{}:
				default:
				}
				select {
				case <-hold:
				case <-r.Context().Done():
					select {
					case timedOut <- struct{}{}:
					default:
					}
					return
				}
			}
			hk.mu.Lock()
			for _, existing := range hk.records {
				if existing.JobID == record.JobID && existing.Attempt == record.Attempt {
					hk.changed = hk.changed || !reflect.DeepEqual(existing, record)
					hk.mu.Unlock()
					w.WriteHeader(http.StatusConflict)
					_, _ = io.WriteString(w, `{"error":"duplicate"}`)
					return
				}
			}
			hk.records = append(hk.records, record)
			hk.mu.Unlock()
			_, _ = io.WriteString(w, `{"status":"recorded"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	isolateRootSvcCred(t)
	t.Setenv("HANDOFFKEEP_URL", server.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "test-placement-token")
	return hk
}

func (hk *dispatchPlacementHK) payloadChanged() bool {
	hk.mu.Lock()
	defer hk.mu.Unlock()
	return hk.changed
}

func placementDeliveryDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestLegacyPlacementReconciliation_t4581(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		name := "equal record seals without posting"
		if conflict {
			name = "different record holds and notifies owner"
		}
		t.Run(name, func(t *testing.T) {
			hk := placementOutboxServer(t)
			dbPath := filepath.Join(t.TempDir(), "inbox.sqlite3")
			hub := placementOutboxHub(t, dbPath)
			request := durableSpawnRequest()
			task := boundSpawnTask(t, hub, request)
			old := placementTraceSnapshot()
			payload, _ := json.Marshal(old)
			if err := hub.inbox.store.saveSpawnPlacementSnapshot(t.Context(), request.RequestID, task.Attempt, payload, ""); err != nil {
				t.Fatal(err)
			}
			legacy := old
			legacy.Candidates = append(legacy.Candidates, fleet.PlacementCandidateRecord{Host: "legacy-refusal", Outcome: "skipped", Reason: "hub_cwd_unmapped"})
			legacyPayload, _ := json.Marshal(legacy)
			deferred, _ := json.Marshal([]placementDeferredSave{{RequestID: request.RequestID, Attempt: task.Attempt, Payload: legacyPayload}})
			if err := os.WriteFile(hub.inbox.deferredPath, deferred, 0o600); err != nil {
				t.Fatal(err)
			}
			outcome := hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed", Host: "machine-a", NodeResult: true}
			if changed, err := hub.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil || !changed {
				t.Fatalf("commit result: changed=%v err=%v", changed, err)
			}
			remote := fleet.PlacementRecord{
				JobID: task.Job, Attempt: task.Attempt, TaskID: 42, DecidedAt: "2026-11-14T00:00:00Z", ToolVersion: fleet.BuildVersion,
				Source: "auto", Ranking: "score", StateSource: "hub", Outcome: "failed", Reason: "spawn_failed",
				Candidates:         legacy.Candidates,
				PlacementExecution: fleet.PlacementExecution{Host: "machine-a", Family: "claude", Profile: "claude-sonnet", Pool: "claude-sub", Role: "worker"},
			}
			if conflict {
				remote.Reason = "different_final_result"
			}
			hk.mu.Lock()
			hk.lookup = []fleet.PlacementRecord{remote}
			hk.mu.Unlock()
			hub.flushPlacementRecords(t.Context())
			hub.flushPlacementRecords(t.Context())
			saved, _, recorded, err := hub.inbox.store.spawnPlacementSnapshot(t.Context(), request.RequestID, task.Attempt)
			if err != nil || recorded == conflict {
				t.Fatalf("recorded=%v conflict=%v err=%v", recorded, conflict, err)
			}
			if conflict && !reflect.DeepEqual(saved, payload) {
				t.Fatal("conflicting hk record changed the existing snapshot")
			}
			if records := hk.records(); len(records) != 0 {
				t.Fatalf("existing record was posted again: %v", records)
			}
			hk.mu.Lock()
			gets := hk.gets
			hk.mu.Unlock()
			if gets == 0 {
				t.Fatal("legacy reconciliation did not check hk")
			}
			if !conflict {
				if hub.hasDeferredTraceSaves() {
					t.Fatal("identical record did not retire its deferred trace")
				}
				return
			}
			if !hub.hasDeferredTraceSaves() {
				t.Fatal("conflicting legacy trace was lost")
			}
			var noticeText string
			for _, n := range hub.readSpawnNotifications(t.Context(), spawnNoticePending) {
				if strings.Contains(n.raw, "legacy_placement_record_conflict") {
					var result hubSpawnOutcome
					_ = json.Unmarshal([]byte(n.raw), &result)
					noticeText = spawnNotificationEvent(n, result).Text
				}
			}
			if !strings.Contains(noticeText, "PLACEMENT_RECORD_HELD") || !strings.Contains(noticeText, "legacy_placement_record_conflict") {
				t.Fatalf("owner notice does not describe the held record: %q", noticeText)
			}
			if err := hub.Close(); err != nil {
				t.Fatal(err)
			}
			hub = placementOutboxHub(t, dbPath)
			hub.flushPlacementRecords(t.Context())
			if !hub.hasDeferredTraceSaves() {
				t.Fatal("restart lost the conflicting legacy trace")
			}
			var notices int
			err = hub.inbox.store.db.QueryRow(`SELECT count(*) FROM spawn_notifications WHERE request_id=? AND owner=? AND result LIKE ?`, request.RequestID, task.Owner, "%legacy_placement_record_conflict%").Scan(&notices)
			if err != nil || notices != 1 {
				t.Fatalf("owner conflict notices=%d err=%v", notices, err)
			}
		})
	}
	t.Run("different local delivery paths use the deferred trace settings", func(t *testing.T) {
		hk := placementOutboxServer(t)
		hub := placementOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
		request := durableSpawnRequest()
		task := boundSpawnTask(t, hub, request)
		oldDelivery, _ := json.Marshal(placementDelivery{StateDir: placementDeliveryDir(t)})
		newDelivery, _ := json.Marshal(placementDelivery{StateDir: placementDeliveryDir(t)})
		old := placementTraceSnapshot()
		oldPayload, _ := json.Marshal(old)
		if err := hub.inbox.store.saveSpawnPlacementSnapshot(t.Context(), request.RequestID, task.Attempt, oldPayload, string(oldDelivery)); err != nil {
			t.Fatal(err)
		}
		legacy := old
		legacy.Candidates = append(legacy.Candidates, fleet.PlacementCandidateRecord{Host: "legacy-refusal", Outcome: "skipped", Reason: "hub_cwd_unmapped"})
		legacyPayload, _ := json.Marshal(legacy)
		deferred, _ := json.Marshal([]placementDeferredSave{{RequestID: request.RequestID, Attempt: task.Attempt, Payload: legacyPayload, Delivery: string(newDelivery)}})
		if err := os.WriteFile(hub.inbox.deferredPath, deferred, 0o600); err != nil {
			t.Fatal(err)
		}
		outcome := hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed", Host: "machine-a", NodeResult: true}
		if changed, err := hub.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil || !changed {
			t.Fatalf("commit result: changed=%v err=%v", changed, err)
		}
		hub.flushPlacementRecords(t.Context())
		if records := hk.records(); len(records) != 1 || records[0]["reason"] != "spawn_failed" {
			t.Fatalf("legacy trace with different local paths was not posted: %v", records)
		}
		hk.mu.Lock()
		gets := hk.gets
		hk.mu.Unlock()
		if gets == 0 {
			t.Fatal("legacy recovery posted without checking the existing hk key")
		}
		_, delivery, recorded, err := hub.inbox.store.spawnPlacementSnapshot(t.Context(), request.RequestID, task.Attempt)
		if err != nil || !recorded || delivery != string(newDelivery) {
			t.Fatalf("recorded=%v delivery=%q err=%v", recorded, delivery, err)
		}
	})
	t.Run("earlier delivery spool holds a legacy attempt", func(t *testing.T) {
		hk := placementOutboxServer(t)
		hub := placementOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
		request := durableSpawnRequest()
		task := boundSpawnTask(t, hub, request)
		oldDelivery, _ := json.Marshal(placementDelivery{StateDir: placementDeliveryDir(t)})
		newDelivery, _ := json.Marshal(placementDelivery{StateDir: placementDeliveryDir(t)})
		old := placementTraceSnapshot()
		oldPayload, _ := json.Marshal(old)
		if err := hub.inbox.store.saveSpawnPlacementSnapshot(t.Context(), request.RequestID, task.Attempt, oldPayload, string(oldDelivery)); err != nil {
			t.Fatal(err)
		}
		legacy := old
		legacy.Candidates = append(legacy.Candidates, fleet.PlacementCandidateRecord{Host: "legacy-refusal", Outcome: "skipped", Reason: "hub_cwd_unmapped"})
		legacyPayload, _ := json.Marshal(legacy)
		deferred, _ := json.Marshal([]placementDeferredSave{{RequestID: request.RequestID, Attempt: task.Attempt, Payload: legacyPayload, Delivery: string(newDelivery)}})
		if err := os.WriteFile(hub.inbox.deferredPath, deferred, 0o600); err != nil {
			t.Fatal(err)
		}
		app, err := hub.placementRecordApp(string(oldDelivery))
		if err != nil {
			t.Fatal(err)
		}
		t.Setenv("HANDOFFKEEP_URL", "http://127.0.0.1:1")
		if _, err := app.EnqueuePlacementRecord(t.Context(), fleet.PlacementRecord{JobID: task.Job, Attempt: task.Attempt, Outcome: "failed"}); err != nil {
			t.Fatal(err)
		}
		t.Setenv("HANDOFFKEEP_URL", hk.server.URL)
		outcome := hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed", Host: "machine-a", NodeResult: true}
		if changed, err := hub.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil || !changed {
			t.Fatalf("commit result: changed=%v err=%v", changed, err)
		}
		hub.flushPlacementRecords(t.Context())
		if !app.PlacementSpooled(task.Job, task.Attempt) || !hub.hasDeferredTraceSaves() || len(hk.records()) != 0 {
			t.Fatal("legacy recovery bypassed the earlier spool")
		}
		hk.mu.Lock()
		gets := hk.gets
		hk.mu.Unlock()
		if gets != 0 {
			t.Fatal("legacy recovery looked up an attempt with an earlier spool")
		}
	})
}

func TestLegacyPlacementLookupFailure_t4581(t *testing.T) {
	for _, name := range []string{"unavailable without a snapshot", "duplicate key", "incomplete list", "malformed key", "missing version field", "snapshot changes during lookup"} {
		t.Run(name, func(t *testing.T) {
			hk := placementOutboxServer(t)
			hub := placementOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
			request := durableSpawnRequest()
			task := boundSpawnTask(t, hub, request)
			old, _ := json.Marshal(placementTraceSnapshot())
			if name != "unavailable without a snapshot" {
				if err := hub.inbox.store.saveSpawnPlacementSnapshot(t.Context(), request.RequestID, task.Attempt, old, ""); err != nil {
					t.Fatal(err)
				}
			} else {
				old = nil
			}
			trace := placementTraceSnapshot()
			trace.Candidates = append(trace.Candidates, fleet.PlacementCandidateRecord{Host: "legacy-refusal", Outcome: "skipped", Reason: "hub_cwd_unmapped"})
			payload, _ := json.Marshal(trace)
			deferred, _ := json.Marshal([]placementDeferredSave{{RequestID: request.RequestID, Attempt: task.Attempt, Payload: payload}})
			if err := os.WriteFile(hub.inbox.deferredPath, deferred, 0o600); err != nil {
				t.Fatal(err)
			}
			outcome := hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed", Host: "machine-a", NodeResult: true}
			if changed, err := hub.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil || !changed {
				t.Fatalf("commit result: changed=%v err=%v", changed, err)
			}
			hk.mu.Lock()
			switch name {
			case "unavailable without a snapshot":
				hk.lookupStatus = http.StatusServiceUnavailable
			case "duplicate key":
				record := fleet.PlacementRecord{JobID: task.Job, Attempt: task.Attempt}
				hk.lookup = []fleet.PlacementRecord{record, record}
			case "incomplete list":
				hk.lookup = make([]fleet.PlacementRecord, 1000)
				for i := range hk.lookup {
					hk.lookup[i] = fleet.PlacementRecord{JobID: "another-job", Attempt: i + 1}
				}
			case "malformed key":
				hk.lookup = []fleet.PlacementRecord{{Attempt: task.Attempt}}
			case "missing version field":
				hk.lookup = []fleet.PlacementRecord{{
					JobID: task.Job, Attempt: task.Attempt, TaskID: 42, DecidedAt: trace.DecidedAt,
					Source: "auto", Ranking: "score", StateSource: "hub", Outcome: "failed", Reason: "spawn_failed",
					Candidates:         trace.Candidates,
					PlacementExecution: fleet.PlacementExecution{Host: "machine-a", Family: "claude", Profile: "claude-sonnet", Pool: "claude-sub", Role: "worker"},
				}}
				hk.lookupOmitVersion = true
			case "snapshot changes during lookup":
				newer := placementTraceSnapshot()
				newer.Chosen = "machine-b"
				old, _ = json.Marshal(newer)
				hk.lookupHook = func() {
					if err := hub.inbox.store.saveSpawnPlacementSnapshot(t.Context(), request.RequestID, task.Attempt, old, ""); err != nil {
						t.Errorf("change snapshot during lookup: %v", err)
					}
				}
			}
			hk.mu.Unlock()
			hub.flushPlacementRecords(t.Context())
			got, _, recorded, err := hub.inbox.store.spawnPlacementSnapshot(t.Context(), request.RequestID, task.Attempt)
			if err != nil || recorded || !reflect.DeepEqual(got, old) {
				t.Fatalf("unproven lookup changed the snapshot: recorded=%v err=%v", recorded, err)
			}
			if !hub.hasDeferredTraceSaves() || len(hk.records()) != 0 {
				t.Fatal("unproven lookup lost the trace or posted a record")
			}
			hk.mu.Lock()
			gets := hk.gets
			hk.mu.Unlock()
			if gets == 0 {
				t.Fatal("legacy recovery did not attempt the hk lookup")
			}
		})
	}
}

func TestLegacyPlacementLookupDoesNotStarveOtherRecords_t4581(t *testing.T) {
	hk := placementOutboxServer(t)
	hub := placementOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	var deferred []placementDeferredSave
	for i, job := range []string{"legacy-a", "legacy-b", "ready-job"} {
		request := durableSpawnRequest()
		request.RequestID = t14RequestID(459980 + i)
		for j, arg := range request.Args {
			switch arg {
			case "test-job":
				request.Args[j] = job
			case "42":
				request.Args[j] = []string{"42", "43", "44"}[i]
			}
		}
		task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, job, "parent", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		trace := placementTraceSnapshot()
		old, _ := json.Marshal(trace)
		if err := hub.inbox.store.saveSpawnPlacementSnapshot(t.Context(), request.RequestID, task.Attempt, old, ""); err != nil {
			t.Fatal(err)
		}
		if i < 2 {
			trace.Candidates = append(trace.Candidates, fleet.PlacementCandidateRecord{Host: "legacy-refusal", Outcome: "skipped", Reason: "hub_cwd_unmapped"})
			payload, _ := json.Marshal(trace)
			deferred = append(deferred, placementDeferredSave{RequestID: request.RequestID, Attempt: task.Attempt, Payload: payload})
			continue
		}
		outcome := hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed", Host: "machine-a", NodeResult: true}
		if changed, err := hub.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil || !changed {
			t.Fatalf("complete independent attempt: changed=%v err=%v", changed, err)
		}
	}
	raw, _ := json.Marshal(deferred)
	if err := os.WriteFile(hub.inbox.deferredPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	hold := make(chan struct{})
	defer close(hold)
	hk.mu.Lock()
	hk.lookupHold = hold
	hk.mu.Unlock()
	hub.flushPlacementRecords(t.Context())
	hk.mu.Lock()
	methods := append([]string(nil), hk.methods...)
	hk.mu.Unlock()
	if len(methods) != 2 || methods[0] != "GET" || methods[1] != "POST" {
		t.Fatalf("slow legacy lookups delayed the independent record: methods=%v", methods)
	}
	if records := hk.records(); len(records) != 1 || records[0]["job_id"] != "ready-job" {
		t.Fatalf("independent record was not delivered: %v", records)
	}
	hub.flushPlacementRecords(t.Context())
	hk.mu.Lock()
	jobs := append([]string(nil), hk.lookupJobs...)
	hk.mu.Unlock()
	if len(jobs) != 2 || jobs[0] == jobs[1] {
		t.Fatalf("slow legacy lookups were not rotated across passes: %v", jobs)
	}
}

func (hk *dispatchPlacementHK) snapshot() []fleet.PlacementRecord {
	hk.mu.Lock()
	defer hk.mu.Unlock()
	return append([]fleet.PlacementRecord(nil), hk.records...)
}

func dispatchPlacementHub(t *testing.T, config HubServerConfig, stateDir string, stopReplacement *atomic.Bool) *HubServer {
	t.Helper()
	h, err := NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	h.placementAppFn = func() (*fleet.App, error) {
		return &fleet.App{StateDir: stateDir, Home: stateDir, Out: io.Discard, Err: io.Discard}, nil
	}
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		if stopReplacement != nil && stopReplacement.Load() {
			<-h.closeCtx.Done()
			return nil, context.Canceled
		}
		app := retryDispatchApp(t, out, errout)
		app.StateDir = stateDir
		app.Config.Hosts["machine-b"] = map[string]any{}
		app.Config.HostOrder = []string{"desktop", "machine-b"}
		app.Source = claimedHarnessSource{retrySource{now: app.Now()}}
		return app, nil
	}
	return h
}

// The placement App is gone when a node refuses. The original candidate
// evidence must survive the unbind and hub restart, and the final node result
// must produce one record for the original logical attempt.
func TestDispatchPlacementRecordSurvivesRefusalAndRestart_t4581(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	hk := newDispatchPlacementHK(t)
	config := HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token", "machine-b": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")}
	stateDir := t.TempDir()
	if real, err := filepath.EvalSymlinks(stateDir); err == nil {
		stateDir = real
	}
	var stopReplacement atomic.Bool
	h := dispatchPlacementHub(t, config, stateDir, &stopReplacement)
	server := httptest.NewServer(h.Handler())
	desktop := t14NamedNode(t, h, server, "desktop", true, true)
	h.startSpawnCoordinator(t.Context())
	request := durableSpawnRequest()
	status, _, _ := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, request)
	if status != http.StatusAccepted {
		t.Fatalf("submission status=%d", status)
	}
	var initial hubSpawnTask
	var err error
	eventually(t, 3*time.Second, "placement to bind or report a refusal", func() bool {
		initial, err = h.inbox.store.spawnTask(t.Context(), request.RequestID)
		return err == nil && initial.Status != "accepted"
	})
	if initial.Node == nil {
		t.Fatalf("placement status=%s reason=%s", initial.Status, initial.Outcome.Reason)
	}
	first := t14ReadNodeSpawn(t, desktop)
	stopReplacement.Store(true)
	payload, _, recorded, err := h.inbox.store.spawnPlacementSnapshot(t.Context(), request.RequestID, 1)
	if err != nil || recorded || len(payload) == 0 {
		t.Fatalf("bound attempt has no pending placement snapshot: recorded=%v err=%v", recorded, err)
	}
	if len(hk.snapshot()) != 0 {
		t.Fatal("placement record was delivered before the node answered")
	}
	if err := wsjson.Write(t.Context(), desktop, nodeRefusal(first.RequestID, "cwd_unmapped", true)); err != nil {
		t.Fatal(err)
	}
	var saved hubSpawnTask
	eventually(t, 3*time.Second, "the refused binding to return to placement", func() bool {
		saved, err = h.inbox.store.spawnTask(t.Context(), request.RequestID)
		return err == nil && saved.Status == "accepted" && saved.Node == nil
	})
	_ = desktop.CloseNow()
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	server.Close()
	h = dispatchPlacementHub(t, config, stateDir, nil)
	server = httptest.NewServer(h.Handler())
	defer server.Close()
	t14NamedNode(t, h, server, "desktop", true, true)
	other := t14NamedNode(t, h, server, "machine-b", true, true)
	h.startSpawnCoordinator(t.Context())
	second := t14ReadNodeSpawn(t, other)
	if second.RequestID == first.RequestID {
		t.Fatal("the second host received the refused node request id")
	}
	if err := wsjson.Write(t.Context(), other, map[string]any{"type": "job.spawn.result", "request_id": second.RequestID, "rc": 70, "job_id": "", "pane": "", "stdout_tail": "", "error": "spawn_failed", "stage": "launch", "retryable": false}); err != nil {
		t.Fatal(err)
	}
	var records []fleet.PlacementRecord
	eventually(t, 5*time.Second, "one placement record after the final node result", func() bool {
		records = hk.snapshot()
		return len(records) > 0
	})
	if len(records) != 1 {
		t.Fatalf("placement posts=%d, want one", len(records))
	}
	record := records[0]
	if record.JobID != "test-job" || record.Attempt != 1 || record.Outcome != "failed" || record.Host != "machine-b" || record.Profile != "sonnet" || record.Source != "auto" {
		t.Fatalf("placement result job=%s attempt=%d outcome=%s host=%s profile=%s source=%s", record.JobID, record.Attempt, record.Outcome, record.Host, record.Profile, record.Source)
	}
	var firstReason string
	for _, candidate := range record.Candidates {
		if candidate.Host == "desktop" {
			firstReason = candidate.Reason
		}
	}
	if firstReason != "hub_cwd_unmapped" {
		t.Fatalf("refused host reason=%q, want hub_cwd_unmapped", firstReason)
	}
	h.flushPlacementRecords(t.Context())
	if count := len(hk.snapshot()); count != 1 {
		t.Fatalf("reconciliation delivered %d placement records", count)
	}
	if hk.payloadChanged() {
		t.Fatal("the same attempt was retried with a different placement record")
	}
}

func TestDispatchPlacementRecordNotStartedRemainsRefused_t4581(t *testing.T) {
	hk := placementOutboxServer(t)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	h := placementOutboxHub(t, path)
	request := durableSpawnRequest()
	task := boundSpawnTask(t, h, request)
	h.notePlacementTrace(task, placementTraceSnapshot(), "")
	outcome := hubSpawnOutcome{RC: 124, Stage: "claim", Reason: "deadline_not_started", Host: "machine-a", Retryable: true, NodeResult: true}
	if changed, err := h.completeSpawn(t.Context(), task, outcome); err != nil || !changed {
		t.Fatalf("complete not-started outcome: changed=%v err=%v", changed, err)
	}
	records := hk.records()
	if len(records) != 1 || records[0]["outcome"] != "refused" || records[0]["chosen_host"] != nil {
		t.Fatalf("not-started placement record count=%d", len(records))
	}
}

func TestPlacementRecordKeepsAutoDecisionAfterClaimedHarnessRetry_t4581(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	hk := newDispatchPlacementHK(t)
	config := HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")}
	stateDir := t.TempDir()
	if real, err := filepath.EvalSymlinks(stateDir); err == nil {
		stateDir = real
	}
	h := dispatchPlacementHub(t, config, stateDir, nil)
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.StateDir, app.Quota = stateDir, claimedHarnessQuota{}
		return app, nil
	}
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	node := t14NamedNode(t, h, server, "desktop", true, true)
	h.startSpawnCoordinator(t.Context())
	request := durableSpawnRequest()
	status, _, _ := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, request)
	if status != http.StatusAccepted {
		t.Fatalf("submission status=%d", status)
	}
	first := t14ReadNodeSpawn(t, node)
	firstProfile := hubSpawnArg(first.Args, "-m")
	payload, _, _, err := h.inbox.store.spawnPlacementSnapshot(t.Context(), request.RequestID, 1)
	if err != nil {
		t.Fatal(err)
	}
	var initial fleet.PlacementTrace
	if err := json.Unmarshal(payload, &initial); err != nil || initial.Source != "auto" {
		t.Fatalf("initial placement source=%s err=%v", initial.Source, err)
	}
	if err := wsjson.Write(t.Context(), node, map[string]any{"type": "job.spawn.result", "request_id": first.RequestID, "rc": 70, "job_id": "", "pane": "", "stdout_tail": "", "error": "launch_proof_failed", "stage": "launch", "retryable": true}); err != nil {
		t.Fatal(err)
	}
	second := t14ReadNodeSpawn(t, node)
	secondProfile := hubSpawnArg(second.Args, "-m")
	if secondProfile == "" || secondProfile == firstProfile {
		t.Fatalf("alternate profile=%q after %q", secondProfile, firstProfile)
	}
	if err := wsjson.Write(t.Context(), node, map[string]any{"type": "job.spawn.result", "request_id": second.RequestID, "rc": 70, "job_id": "", "pane": "", "stdout_tail": "", "error": "spawn_failed", "stage": "launch", "retryable": false}); err != nil {
		t.Fatal(err)
	}
	var records []fleet.PlacementRecord
	eventually(t, 5*time.Second, "placement record for the final harness", func() bool {
		records = hk.snapshot()
		return len(records) > 0
	})
	if len(records) != 1 || records[0].Source != "auto" || records[0].Host != "desktop" || records[0].Profile != secondProfile {
		t.Fatalf("final record count=%d source=%s host=%s profile=%s", len(records), records[0].Source, records[0].Host, records[0].Profile)
	}
	for _, candidate := range initial.Candidates {
		found := false
		for _, actual := range records[0].Candidates {
			found = found || actual.Host == candidate.Host
		}
		if !found {
			t.Fatalf("original candidate %s was lost after the harness retry", candidate.Host)
		}
	}
}

// A retry can arrive after the outcome commits but before its outbox record
// is enqueued, including when the hub restarts in that window.
func TestDispatchPlacementRecordSurvivesImmediateReplay_t4581(t *testing.T) {
	hk := placementOutboxServer(t)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	h := placementOutboxHub(t, path)
	request := durableSpawnRequest()
	task := boundSpawnTask(t, h, request)
	h.notePlacementTrace(task, placementTraceSnapshot(), "")
	outcome := hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed", Host: "machine-a", Retryable: true, NodeResult: true}
	if changed, err := h.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil || !changed {
		t.Fatalf("commit node outcome: changed=%v err=%v", changed, err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	h = placementOutboxHub(t, path)
	finished, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := h.inbox.store.resumeCleanedSpawn(t.Context(), finished, time.Now())
	if err != nil || resumed.Attempt != task.Attempt+1 {
		t.Fatalf("resume attempt=%d err=%v", resumed.Attempt, err)
	}
	// A delayed save from the old placing App must not overwrite the frozen
	// result after a newer attempt has started.
	stale := placementTraceSnapshot()
	stale.Chosen = "machine-b"
	raw, err := json.Marshal(stale)
	if err != nil {
		t.Fatal(err)
	}
	h.deferPlacementTraceSave(request.RequestID, task.Attempt, raw, "")
	h.flushPlacementRecords(t.Context())
	records := hk.records()
	if len(records) != 1 {
		t.Fatalf("finished attempt produced %d placement records, want one", len(records))
	}
	if records[0]["outcome"] != "failed" || records[0]["chosen_host"] != "machine-a" || records[0]["attempt"] != float64(task.Attempt) {
		t.Fatalf("finished attempt lost its placement outcome or host")
	}
}

// A placement snapshot can be deferred when the store is temporarily unable
// to write it. A replay must retain that prior attempt before advancing.
func TestReplayPreservesDeferredPlacementTrace_t4581(t *testing.T) {
	hk := placementOutboxServer(t)
	h := placementOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(placementTraceSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	h.deferPlacementTraceSave(request.RequestID, task.Attempt, payload, "")
	outcome := hubSpawnOutcome{RC: 124, Stage: "placement", Reason: "timeout", Retryable: true}
	if changed, err := h.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil || !changed {
		t.Fatalf("commit timeout: changed=%v err=%v", changed, err)
	}
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	if _, err := h.inbox.store.db.Exec(`CREATE TRIGGER fail_replay_snapshot BEFORE INSERT ON spawn_placement_snapshots BEGIN SELECT RAISE(FAIL, 'injected snapshot failure'); END`); err != nil {
		t.Fatal(err)
	}
	status, _, _ := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, request)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("replay with unavailable snapshot status=%d", status)
	}
	if current, err := h.inbox.store.spawnTask(t.Context(), request.RequestID); err != nil || current.Status != "retryable" || current.Attempt != task.Attempt {
		t.Fatalf("failed replay changed attempt: status=%s attempt=%d err=%v", current.Status, current.Attempt, err)
	}
	if _, err := h.inbox.store.db.Exec(`DROP TRIGGER fail_replay_snapshot`); err != nil {
		t.Fatal(err)
	}
	status, _, _ = t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, request)
	if status != http.StatusAccepted {
		t.Fatalf("replay status=%d", status)
	}
	h.flushPlacementRecords(t.Context())
	records := hk.records()
	if len(records) != 1 || records[0]["attempt"] != float64(task.Attempt) || records[0]["reason"] != "timeout" {
		t.Fatalf("replay lost deferred attempt: records=%v", records)
	}
	t.Run("newer deferred trace replaces its base", func(t *testing.T) {
		hub := placementOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
		request := durableSpawnRequest()
		task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		old := placementTraceSnapshot()
		hub.notePlacementTrace(task, old, "")
		if _, err := hub.inbox.store.db.Exec(`CREATE TRIGGER fail_snapshot_update BEFORE UPDATE ON spawn_placement_snapshots BEGIN SELECT RAISE(FAIL, 'injected snapshot failure'); END`); err != nil {
			t.Fatal(err)
		}
		newer := old
		newer.Chosen = "machine-b"
		hub.notePlacementTrace(task, newer, "")
		if _, err := hub.inbox.store.db.Exec(`DROP TRIGGER fail_snapshot_update`); err != nil {
			t.Fatal(err)
		}
		hub.retryDeferredTraceSaves()
		payload, _, _, err := hub.inbox.store.spawnPlacementSnapshot(t.Context(), request.RequestID, task.Attempt)
		var got fleet.PlacementTrace
		if err != nil || json.Unmarshal(payload, &got) != nil || got.Chosen != "machine-b" {
			t.Fatalf("newer deferred trace did not replace its base: chosen=%q err=%v", got.Chosen, err)
		}
	})
	t.Run("store outage keeps the last confirmed base", func(t *testing.T) {
		hk := placementOutboxServer(t)
		storePath := filepath.Join(t.TempDir(), "inbox.sqlite3")
		hub := placementOutboxHub(t, storePath)
		request := durableSpawnRequest()
		task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		old := placementTraceSnapshot()
		hub.notePlacementTrace(task, old, "")
		if err := hub.inbox.store.db.Close(); err != nil {
			t.Fatal(err)
		}
		newer := old
		newer.Candidates = append(newer.Candidates, fleet.PlacementCandidateRecord{Host: "earlier-refusal", Outcome: "skipped", Reason: "hub_cwd_unmapped"})
		newer.Record = &fleet.PlacementRecord{JobID: task.Job, Attempt: task.Attempt, Outcome: "refused", Reason: "quota_unknown", Candidates: newer.Candidates}
		hub.notePlacementTrace(task, newer, "")
		if err := hub.Close(); err != nil {
			t.Fatal(err)
		}
		hub = placementOutboxHub(t, storePath)
		hub.retryDeferredTraceSaves()
		outcome := hubSpawnOutcome{RC: 75, Stage: "placement", Reason: "quota_unknown", Retryable: true}
		if changed, err := hub.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil || !changed {
			t.Fatalf("commit terminal result: changed=%v err=%v", changed, err)
		}
		hub.flushPlacementRecords(t.Context())
		records := hk.records()
		if len(records) != 1 || records[0]["reason"] != "quota_unknown" {
			t.Fatalf("outage lost the terminal placement record: records=%v", records)
		}
		candidates, _ := records[0]["candidates"].([]any)
		found := false
		for _, raw := range candidates {
			candidate, _ := raw.(map[string]any)
			found = found || candidate["host"] == "earlier-refusal"
		}
		if !found {
			t.Fatal("outage lost the earlier refused host")
		}
	})
	t.Run("placement seeds the base before an outage", func(t *testing.T) {
		t.Setenv("XDG_STATE_HOME", t.TempDir())
		hk := newDispatchPlacementHK(t)
		config := HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")}
		stateDir := t.TempDir()
		if real, err := filepath.EvalSymlinks(stateDir); err == nil {
			stateDir = real
		}
		hub := dispatchPlacementHub(t, config, stateDir, nil)
		request := durableSpawnRequest()
		task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		old, _ := json.Marshal(placementTraceSnapshot())
		if err := hub.inbox.store.saveSpawnPlacementSnapshot(t.Context(), request.RequestID, task.Attempt, old, ""); err != nil {
			t.Fatal(err)
		}
		outage := make(chan struct{})
		hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
			app := retryDispatchApp(t, out, errout)
			app.StateDir = stateDir
			app.Source = &dispatchPlacementOutageSource{retrySource: retrySource{now: app.Now()}, fail: func() {
				_ = hub.inbox.store.db.Close()
				close(outage)
			}}
			return app, nil
		}
		server := httptest.NewServer(hub.Handler())
		defer server.Close()
		node := t14NamedNode(t, hub, server, "desktop", true, true)
		hub.startSpawnCoordinator(t.Context())
		select {
		case <-outage:
		case <-time.After(3 * time.Second):
			t.Fatal("placement did not reach the simulated store outage")
		}
		eventually(t, 3*time.Second, "deferred trace saved during store outage", func() bool {
			_, err := os.Stat(hub.inbox.deferredPath)
			return err == nil
		})
		_ = node.CloseNow()
		if err := hub.Close(); err != nil {
			t.Fatal(err)
		}
		hub = dispatchPlacementHub(t, config, stateDir, nil)
		hub.retryDeferredTraceSaves()
		if _, err := os.Stat(hub.inbox.deferredPath); !os.IsNotExist(err) {
			t.Fatalf("deferred trace did not recover after the store returned: %v", err)
		}
		outcome := hubSpawnOutcome{RC: 124, Stage: "placement", Reason: "timeout", Retryable: true}
		if changed, err := hub.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil || !changed {
			t.Fatalf("commit terminal result: changed=%v err=%v", changed, err)
		}
		hub.flushPlacementRecords(t.Context())
		records := hk.snapshot()
		if len(records) != 1 || records[0].Reason != "timeout" {
			t.Fatalf("outage prevented final placement delivery: records=%v", records)
		}
		found := false
		for _, candidate := range records[0].Candidates {
			found = found || candidate.Host == "desktop"
		}
		if !found {
			t.Fatal("outage lost the placement pass's candidate trace")
		}
	})
	t.Run("placement restores a deferred pass before ranking again", func(t *testing.T) {
		t.Setenv("XDG_STATE_HOME", t.TempDir())
		newDispatchPlacementHK(t)
		config := HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")}
		hub := dispatchPlacementHub(t, config, t.TempDir(), nil)
		request := durableSpawnRequest()
		task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		old := placementTraceSnapshot()
		hub.notePlacementTrace(task, old, "")
		if _, err := hub.inbox.store.db.Exec(`CREATE TRIGGER fail_snapshot_update BEFORE UPDATE ON spawn_placement_snapshots BEGIN SELECT RAISE(FAIL, 'injected snapshot failure'); END`); err != nil {
			t.Fatal(err)
		}
		deferred := old
		deferred.Candidates = append(deferred.Candidates, fleet.PlacementCandidateRecord{Host: "earlier-refusal", Outcome: "skipped", Reason: "hub_cwd_unmapped"})
		hub.notePlacementTrace(task, deferred, "")
		if _, err := hub.inbox.store.db.Exec(`DROP TRIGGER fail_snapshot_update`); err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(hub.Handler())
		defer server.Close()
		t14NamedNode(t, hub, server, "desktop", true, true)
		hub.executeSpawnTask(t.Context(), task)
		payload, _, _, err := hub.inbox.store.spawnPlacementSnapshot(t.Context(), request.RequestID, task.Attempt)
		var trace fleet.PlacementTrace
		if err != nil || json.Unmarshal(payload, &trace) != nil {
			t.Fatalf("read placement trace: %v", err)
		}
		for _, candidate := range trace.Candidates {
			if candidate.Host == "earlier-refusal" {
				return
			}
		}
		t.Fatal("new placement pass lost the previously deferred candidate")
	})
	t.Run("replay waits for deferred retirement", func(t *testing.T) {
		hub := placementOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
		request := durableSpawnRequest()
		task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		payload, _ := json.Marshal(placementTraceSnapshot())
		hub.deferPlacementTraceSave(request.RequestID, task.Attempt, payload, "")
		outcome := hubSpawnOutcome{RC: 124, Stage: "placement", Reason: "timeout", Retryable: true}
		if changed, err := hub.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil || !changed {
			t.Fatalf("commit timeout: changed=%v err=%v", changed, err)
		}
		path := hub.inbox.deferredPath
		hub.inbox.deferredPath = nonemptyDeferredDirectory(t)
		server := httptest.NewServer(hub.Handler())
		defer server.Close()
		status, _, _ := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, request)
		if status != http.StatusServiceUnavailable {
			t.Fatalf("replay advanced before deferred retirement: status=%d", status)
		}
		current, err := hub.inbox.store.spawnTask(t.Context(), request.RequestID)
		if err != nil || current.Status != "retryable" || current.Attempt != task.Attempt {
			t.Fatalf("failed retirement changed attempt: status=%s attempt=%d err=%v", current.Status, current.Attempt, err)
		}
		hub.inbox.deferredPath = path
		status, _, _ = t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, request)
		if status != http.StatusAccepted {
			t.Fatalf("replay did not resume after retirement repair: status=%d", status)
		}
	})
}

func TestPlacementDeliveryDoesNotBlockOtherDispatch_t4581(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	hk := newDispatchPlacementHK(t)
	hold := make(chan struct{})
	started, timedOut := make(chan struct{}, 4), make(chan struct{}, 4)
	hk.mu.Lock()
	hk.hold, hk.started, hk.timedOut = hold, started, timedOut
	hk.mu.Unlock()
	release := sync.OnceFunc(func() { close(hold) })
	config := HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-b": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")}
	stateDir := t.TempDir()
	if real, err := filepath.EvalSymlinks(stateDir); err == nil {
		stateDir = real
	}
	h := dispatchPlacementHub(t, config, stateDir, nil)
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.StateDir = stateDir
		app.Config.Hosts["machine-b"] = map[string]any{}
		app.Config.HostOrder = []string{"desktop", "machine-b"}
		app.Source = dispatchPlacementOtherHost{retrySource{now: app.Now()}}
		return app, nil
	}
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	defer release()
	request := durableSpawnRequest()
	task := boundSpawnTask(t, h, request)
	h.notePlacementTrace(task, placementTraceSnapshot(), "")
	outcome := hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed", Host: "machine-a", NodeResult: true}
	if changed, err := h.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil || !changed {
		t.Fatalf("commit pending placement record: changed=%v err=%v", changed, err)
	}
	// The owner notice is already delivered; only placement delivery is held.
	if _, err := h.inbox.store.db.Exec(`UPDATE spawn_notifications SET delivered=1 WHERE request_id=?`, request.RequestID); err != nil {
		t.Fatal(err)
	}
	h.startSpawnCoordinator(t.Context())
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("placement delivery did not reach the held hk server")
	}
	other := t14NamedNode(t, h, server, "machine-b", true, true)
	next := durableSpawnRequest()
	next.RequestID = t14RequestID(458999)
	next.Args = append([]string(nil), next.Args...)
	for i, arg := range next.Args {
		switch arg {
		case "test-job":
			next.Args[i] = "follow-job"
		case "42":
			next.Args[i] = "43"
		}
	}
	status, _, _ := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, next)
	if status != http.StatusAccepted {
		t.Fatalf("independent submission status=%d", status)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	answered := make(chan error, 1)
	go func() {
		_, payload, err := other.Read(ctx)
		if err == nil {
			message, valid := parseHubOutbound(payload)
			if !valid || message.Type != "job.spawn" || hubSpawnArg(message.Args, "--job") != "follow-job" {
				err = errors.New("unexpected node frame")
			}
		}
		answered <- err
	}()
	select {
	case err := <-answered:
		if err != nil {
			t.Fatalf("independent dispatch failed: %v", err)
		}
	case <-timedOut:
		saved, err := h.inbox.store.spawnTask(t.Context(), next.RequestID)
		t.Fatalf("independent dispatch waited for held placement delivery: status=%s host=%s reason=%s err=%v", saved.Status, spawnTaskHost(saved), saved.Outcome.Reason, err)
	case <-ctx.Done():
		t.Fatal("independent dispatch did not run while placement delivery was held")
	}
}

func TestDispatchWaitsForDurablePlacementSnapshot_t4581(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	newDispatchPlacementHK(t)
	config := HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")}
	stateDir := t.TempDir()
	if real, err := filepath.EvalSymlinks(stateDir); err == nil {
		stateDir = real
	}
	h := dispatchPlacementHub(t, config, stateDir, nil)
	if _, err := h.inbox.store.db.Exec(`CREATE TRIGGER fail_placement_snapshot BEFORE INSERT ON spawn_placement_snapshots BEGIN SELECT RAISE(FAIL, 'injected snapshot failure'); END`); err != nil {
		t.Fatal(err)
	}
	h.placementAppMu.Lock()
	deferredPath := h.inbox.deferredPath
	h.inbox.deferredPath = filepath.Join(t.TempDir(), "missing", "deferred.json") // The missing directory prevents the deferred write.
	h.placementAppMu.Unlock()
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	node := t14NamedNode(t, h, server, "desktop", true, true)
	h.startSpawnCoordinator(t.Context())
	request := durableSpawnRequest()
	status, _, _ := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, request)
	if status != http.StatusAccepted {
		t.Fatalf("submission status=%d", status)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	frames := make(chan error, 1)
	go func() {
		_, payload, err := node.Read(ctx)
		if err == nil {
			message, valid := parseHubOutbound(payload)
			if !valid || message.Type != "job.spawn" {
				err = errors.New("unexpected node frame")
			}
		}
		frames <- err
	}()
	eventually(t, 3*time.Second, "snapshot write failure to be observed", func() bool {
		h.placementAppMu.Lock()
		defer h.placementAppMu.Unlock()
		return len(h.placementDeferredSaves) > 0
	})
	// Window: neither the snapshot nor its deferred file can be saved, so no
	// node frame may be sent until storage is repaired.
	select {
	case err := <-frames:
		t.Fatalf("node received a frame without a durable snapshot: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	if _, err := h.inbox.store.db.Exec(`DROP TRIGGER fail_placement_snapshot`); err != nil {
		t.Fatal(err)
	}
	h.placementAppMu.Lock()
	h.inbox.deferredPath = deferredPath
	h.placementAppMu.Unlock()
	h.kickSpawnPlacement()
	select {
	case err := <-frames:
		if err != nil {
			t.Fatalf("node frame after storage repair: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("placement did not resume after snapshot storage recovered")
	}
	payload, _, recorded, err := h.inbox.store.spawnPlacementSnapshot(t.Context(), request.RequestID, 1)
	if err != nil || recorded || len(payload) == 0 {
		t.Fatalf("sent attempt has no pending snapshot: recorded=%v err=%v", recorded, err)
	}
}

func TestPlacementSnapshotIgnoresDeferredStaleTrace_t4581(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "inbox.sqlite3")
	h := placementOutboxHub(t, storePath)
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	old := placementTraceSnapshot()
	old.Chosen = "machine-a"
	raw, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	h.deferPlacementTraceSave(request.RequestID, task.Attempt, raw, "")
	newer := placementTraceSnapshot()
	newer.Chosen = "machine-b"
	// A directory cannot be removed as the deferred file. The newer DB save
	// must still proceed, and the old file must not win after a restart.
	deferredPath := h.inbox.deferredPath
	h.inbox.deferredPath = nonemptyDeferredDirectory(t)
	h.notePlacementTrace(task, newer, "")
	h.inbox.deferredPath = deferredPath
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	h = placementOutboxHub(t, storePath)
	h.retryDeferredTraceSaves()
	payload, _, _, err := h.inbox.store.spawnPlacementSnapshot(t.Context(), request.RequestID, task.Attempt)
	var got fleet.PlacementTrace
	if err != nil || json.Unmarshal(payload, &got) != nil || got.Chosen != "machine-b" {
		t.Fatalf("deferred older trace replaced newer choice: chosen=%s err=%v", got.Chosen, err)
	}
	t.Run("failed cleanup keeps dispatch and fences a known base", func(t *testing.T) {
		t.Setenv("XDG_STATE_HOME", t.TempDir())
		newDispatchPlacementHK(t)
		config := HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")}
		stateDir := t.TempDir()
		hub := dispatchPlacementHub(t, config, stateDir, nil)
		request := durableSpawnRequest()
		task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		old := placementTraceSnapshot()
		hub.notePlacementTrace(task, old, "")
		if _, err := hub.inbox.store.db.Exec(`CREATE TRIGGER fail_snapshot_update BEFORE UPDATE ON spawn_placement_snapshots BEGIN SELECT RAISE(FAIL, 'injected snapshot failure'); END`); err != nil {
			t.Fatal(err)
		}
		deferred := old
		deferred.Chosen = "machine-b"
		hub.notePlacementTrace(task, deferred, "")
		if _, err := hub.inbox.store.db.Exec(`DROP TRIGGER fail_snapshot_update`); err != nil {
			t.Fatal(err)
		}
		path := hub.inbox.deferredPath
		hub.inbox.deferredPath = nonemptyDeferredDirectory(t)
		server := httptest.NewServer(hub.Handler())
		defer server.Close()
		node := t14NamedNode(t, hub, server, "desktop", true, true)
		hub.startSpawnCoordinator(t.Context())
		t14ReadNodeSpawn(t, node)
		_ = node.CloseNow()
		if err := hub.Close(); err != nil {
			t.Fatal(err)
		}
		server.Close()
		hub = dispatchPlacementHub(t, config, stateDir, nil)
		hub.retryDeferredTraceSaves()
		payload, _, _, err := hub.inbox.store.spawnPlacementSnapshot(t.Context(), request.RequestID, task.Attempt)
		var got fleet.PlacementTrace
		if err != nil || json.Unmarshal(payload, &got) != nil || got.Chosen != "desktop" {
			t.Fatalf("old deferred trace replaced the dispatched snapshot: chosen=%q err=%v", got.Chosen, err)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("superseded deferred file was not retired: %v", err)
		}
	})
	t.Run("delivery settings are part of the base", func(t *testing.T) {
		storePath := filepath.Join(t.TempDir(), "inbox.sqlite3")
		hub := placementOutboxHub(t, storePath)
		request := durableSpawnRequest()
		task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		trace := placementTraceSnapshot()
		hub.notePlacementTrace(task, trace, "base")
		if _, err := hub.inbox.store.db.Exec(`CREATE TRIGGER fail_snapshot_update BEFORE UPDATE ON spawn_placement_snapshots BEGIN SELECT RAISE(FAIL, 'injected snapshot failure'); END`); err != nil {
			t.Fatal(err)
		}
		hub.notePlacementTrace(task, trace, "deferred")
		if _, err := hub.inbox.store.db.Exec(`DROP TRIGGER fail_snapshot_update`); err != nil {
			t.Fatal(err)
		}
		path := hub.inbox.deferredPath
		hub.inbox.deferredPath = nonemptyDeferredDirectory(t)
		hub.notePlacementTrace(task, trace, "newer")
		hub.inbox.deferredPath = path
		if err := hub.Close(); err != nil {
			t.Fatal(err)
		}
		hub = placementOutboxHub(t, storePath)
		hub.retryDeferredTraceSaves()
		_, delivery, _, err := hub.inbox.store.spawnPlacementSnapshot(t.Context(), request.RequestID, task.Attempt)
		if err != nil || delivery != "newer" {
			t.Fatalf("old deferred save replaced delivery settings: %q err=%v", delivery, err)
		}
	})
	t.Run("ambiguous legacy trace is retained", func(t *testing.T) {
		hk := placementOutboxServer(t)
		hub := placementOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
		request := durableSpawnRequest()
		task := boundSpawnTask(t, hub, request)
		old, _ := json.Marshal(placementTraceSnapshot())
		if err := hub.inbox.store.saveSpawnPlacementSnapshot(t.Context(), request.RequestID, task.Attempt, old, ""); err != nil {
			t.Fatal(err)
		}
		newer := placementTraceSnapshot()
		newer.Chosen = "machine-b"
		payload, _ := json.Marshal(newer)
		legacy, _ := json.Marshal([]placementDeferredSave{{RequestID: request.RequestID, Attempt: task.Attempt, Payload: payload}})
		if err := os.WriteFile(hub.inbox.deferredPath, legacy, 0o600); err != nil {
			t.Fatal(err)
		}
		outcome := hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed", Host: "machine-a", NodeResult: true}
		if changed, err := hub.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil || !changed {
			t.Fatalf("commit result: changed=%v err=%v", changed, err)
		}
		// A previously spooled record must wait too; a spool flush must not
		// bypass the conflicting trace.
		t.Setenv("HANDOFFKEEP_URL", "http://127.0.0.1:1")
		hub.enqueueFinishedPlacementRecord(t.Context(), task, fleet.PlacementRecord{JobID: task.Job, Attempt: task.Attempt, Outcome: "failed", Reason: "spawn_failed"}, "")
		t.Setenv("HANDOFFKEEP_URL", hk.server.URL)
		hub.flushPlacementRecords(t.Context())
		if records := hk.records(); len(records) != 0 {
			t.Fatalf("ambiguous trace produced %d placement records", len(records))
		}
		if _, err := os.Stat(hub.inbox.deferredPath); err != nil {
			t.Fatalf("ambiguous deferred trace was lost: %v", err)
		}
		app, err := hub.placementRecordApp("")
		if err != nil || !app.PlacementSpooled(task.Job, task.Attempt) {
			t.Fatalf("conflicting placement record was not retained: %v", err)
		}
	})
	t.Run("legacy trace is carried into a new placement pass", func(t *testing.T) {
		t.Setenv("XDG_STATE_HOME", t.TempDir())
		hk := newDispatchPlacementHK(t)
		config := HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")}
		hub := dispatchPlacementHub(t, config, t.TempDir(), nil)
		request := durableSpawnRequest()
		task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		old := placementTraceSnapshot()
		payload, _ := json.Marshal(old)
		if err := hub.inbox.store.saveSpawnPlacementSnapshot(t.Context(), request.RequestID, task.Attempt, payload, ""); err != nil {
			t.Fatal(err)
		}
		legacyTrace := old
		legacyTrace.Candidates = append(legacyTrace.Candidates, fleet.PlacementCandidateRecord{Host: "legacy-refusal", Outcome: "skipped", Reason: "hub_cwd_unmapped"})
		legacyPayload, _ := json.Marshal(legacyTrace)
		legacy, _ := json.Marshal([]placementDeferredSave{{RequestID: request.RequestID, Attempt: task.Attempt, Payload: legacyPayload}})
		if err := os.WriteFile(hub.inbox.deferredPath, legacy, 0o600); err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(hub.Handler())
		defer server.Close()
		t14NamedNode(t, hub, server, "desktop", true, true)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		hub.executeSpawnTask(ctx, task)
		current, err := hub.inbox.store.spawnTask(t.Context(), request.RequestID)
		if err != nil || current.Node == nil {
			t.Fatalf("legacy trace blocked new placement: status=%s err=%v", current.Status, err)
		}
		if records := hk.snapshot(); len(records) != 0 {
			t.Fatalf("a nonterminal legacy attempt posted a record: %v", records)
		}
		payload, _, _, err = hub.inbox.store.spawnPlacementSnapshot(t.Context(), request.RequestID, task.Attempt)
		var trace fleet.PlacementTrace
		if err != nil || json.Unmarshal(payload, &trace) != nil {
			t.Fatalf("read placement trace: %v", err)
		}
		for _, candidate := range trace.Candidates {
			if candidate.Host == "legacy-refusal" {
				return
			}
		}
		t.Fatal("new placement lost a host from the legacy trace")
	})
	t.Run("legacy terminal trace absent in hk is merged and posted", func(t *testing.T) {
		hk := placementOutboxServer(t)
		hub := placementOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
		request := durableSpawnRequest()
		task := boundSpawnTask(t, hub, request)
		old := placementTraceSnapshot()
		payload, _ := json.Marshal(old)
		if err := hub.inbox.store.saveSpawnPlacementSnapshot(t.Context(), request.RequestID, task.Attempt, payload, ""); err != nil {
			t.Fatal(err)
		}
		legacyTrace := old
		legacyTrace.Candidates = append(legacyTrace.Candidates, fleet.PlacementCandidateRecord{Host: "legacy-refusal", Outcome: "skipped", Reason: "hub_cwd_unmapped"})
		legacyPayload, _ := json.Marshal(legacyTrace)
		legacy, _ := json.Marshal([]placementDeferredSave{{RequestID: request.RequestID, Attempt: task.Attempt, Payload: legacyPayload}})
		if err := os.WriteFile(hub.inbox.deferredPath, legacy, 0o600); err != nil {
			t.Fatal(err)
		}
		outcome := hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed", Host: "machine-a", NodeResult: true}
		if changed, err := hub.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil || !changed {
			t.Fatalf("commit result: changed=%v err=%v", changed, err)
		}
		hub.flushPlacementRecords(t.Context())
		hub.flushPlacementRecords(t.Context())
		records := hk.records()
		if len(records) != 1 || records[0]["reason"] != "spawn_failed" {
			t.Fatalf("legacy terminal trace did not produce its final record: %v", records)
		}
		candidates, ok := records[0]["candidates"].([]any)
		if !ok || len(candidates) != 3 || candidates[2].(map[string]any)["host"] != "legacy-refusal" {
			t.Fatalf("legacy candidate missing from final record: %v", records[0]["candidates"])
		}
		hk.mu.Lock()
		gets, methods, lookupJob := hk.gets, append([]string(nil), hk.methods...), hk.lookupJob
		hk.mu.Unlock()
		if gets == 0 || len(methods) < 2 || methods[0] != "GET" || methods[1] != "POST" || lookupJob != task.Job {
			t.Fatalf("legacy reconciliation did not check its existing key before posting: methods=%v job=%q", methods, lookupJob)
		}
	})
	t.Run("shared spool respects each attempt's delivery settings", func(t *testing.T) {
		hk := placementOutboxServer(t)
		hub := placementOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
		app, err := hub.placementRecordApp("")
		if err != nil {
			t.Fatal(err)
		}
		firstDelivery, _ := json.Marshal(placementDelivery{StateDir: app.StateDir})
		secondDelivery, _ := json.Marshal(placementDelivery{StateDir: app.StateDir, Home: t.TempDir()})
		firstRequest := durableSpawnRequest()
		first := boundSpawnTask(t, hub, firstRequest)
		old, _ := json.Marshal(placementTraceSnapshot())
		if err := hub.inbox.store.saveSpawnPlacementSnapshot(t.Context(), firstRequest.RequestID, first.Attempt, old, string(firstDelivery)); err != nil {
			t.Fatal(err)
		}
		newer := placementTraceSnapshot()
		newer.Chosen = "machine-b"
		payload, _ := json.Marshal(newer)
		legacy, _ := json.Marshal([]placementDeferredSave{{RequestID: firstRequest.RequestID, Attempt: first.Attempt, Payload: payload, Delivery: string(firstDelivery)}})
		if err := os.WriteFile(hub.inbox.deferredPath, legacy, 0o600); err != nil {
			t.Fatal(err)
		}
		outcome := hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed", Host: "machine-a", NodeResult: true}
		if changed, err := hub.inbox.store.completeSpawn(t.Context(), first, outcome, time.Now()); err != nil || !changed {
			t.Fatalf("commit first result: changed=%v err=%v", changed, err)
		}
		t.Setenv("HANDOFFKEEP_URL", "http://127.0.0.1:1")
		hub.enqueueFinishedPlacementRecord(t.Context(), first, fleet.PlacementRecord{JobID: first.Job, Attempt: first.Attempt, Outcome: "failed", Reason: "spawn_failed"}, string(firstDelivery))
		t.Setenv("HANDOFFKEEP_URL", hk.server.URL)
		secondRequest := durableSpawnRequest()
		secondRequest.RequestID = t14RequestID(459990)
		for i := 0; i+1 < len(secondRequest.Args); i++ {
			if secondRequest.Args[i] == "--task" {
				secondRequest.Args[i+1] = "43"
			}
		}
		second, _, err := hub.inbox.store.claimSpawn(t.Context(), secondRequest, "other-job", "parent", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if err := hub.inbox.store.saveSpawnPlacementSnapshot(t.Context(), secondRequest.RequestID, second.Attempt, old, string(secondDelivery)); err != nil {
			t.Fatal(err)
		}
		if changed, err := hub.inbox.store.completeSpawn(t.Context(), second, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "admission_refused"}, time.Now()); err != nil || !changed {
			t.Fatalf("commit second result: changed=%v err=%v", changed, err)
		}
		hub.flushPlacementRecords(t.Context())
		records := hk.records()
		if len(records) != 1 || records[0]["job_id"] != "other-job" {
			t.Fatalf("shared spool delivered the blocked attempt or lost the independent record: %v", records)
		}
		if !app.PlacementSpooled(first.Job, first.Attempt) {
			t.Fatal("blocked attempt's spool file was removed")
		}
	})
}

func TestPlacementDeferredSavePreservesUnreadableFile_t4581(t *testing.T) {
	h := asyncSpawnHub(t)
	path := h.inbox.deferredPath
	original := []byte(`{"incomplete":`)
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(placementTraceSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	h.deferPlacementTraceSave("request", 1, payload, "")
	got, err := os.ReadFile(path)
	if err != nil || string(got) != string(original) {
		t.Fatalf("unreadable deferred file was overwritten: err=%v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	h.deferPlacementTraceSave("request", 1, payload, "")
	got, err = os.ReadFile(path)
	var saves []placementDeferredSave
	if err != nil || json.Unmarshal(got, &saves) != nil || len(saves) != 1 || saves[0].RequestID != "request" {
		t.Fatalf("deferred save did not resume after repair: err=%v", err)
	}
	t.Run("unrelated dispatch", func(t *testing.T) {
		t.Setenv("XDG_STATE_HOME", t.TempDir())
		newDispatchPlacementHK(t)
		config := HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")}
		hub := dispatchPlacementHub(t, config, t.TempDir(), nil)
		if err := os.WriteFile(hub.inbox.deferredPath, original, 0o600); err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(hub.Handler())
		defer server.Close()
		node := t14NamedNode(t, hub, server, "desktop", true, true)
		hub.startSpawnCoordinator(t.Context())
		request := durableSpawnRequest()
		status, _, _ := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, request)
		if status != http.StatusAccepted {
			t.Fatalf("submission status=%d", status)
		}
		message := t14ReadNodeSpawn(t, node)
		if message.Type != "job.spawn" {
			t.Fatalf("unrelated spawn did not dispatch: %s", message.Type)
		}
		if got, err := os.ReadFile(hub.inbox.deferredPath); err != nil || string(got) != string(original) {
			t.Fatalf("unreadable deferred data was not preserved: err=%v", err)
		}
	})
	t.Run("retry with a durable snapshot", func(t *testing.T) {
		placementOutboxServer(t)
		hub := placementOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
		request := durableSpawnRequest()
		task := boundSpawnTask(t, hub, request)
		if err := hub.inbox.store.saveSpawnPlacementSnapshot(t.Context(), request.RequestID, task.Attempt, payload, ""); err != nil {
			t.Fatal(err)
		}
		outcome := hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed", Host: "machine-a", Retryable: true, NodeResult: true}
		if changed, err := hub.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil || !changed {
			t.Fatalf("commit result: changed=%v err=%v", changed, err)
		}
		if err := os.WriteFile(hub.inbox.deferredPath, original, 0o600); err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(hub.Handler())
		defer server.Close()
		status, _, _ := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, request)
		if status != http.StatusAccepted {
			t.Fatalf("replay with a durable snapshot was blocked: status=%d", status)
		}
		if got, err := os.ReadFile(hub.inbox.deferredPath); err != nil || string(got) != string(original) {
			t.Fatalf("unreadable deferred data was not preserved: err=%v", err)
		}
	})
}

func TestPendingPlacementTraceDoesNotMaskFinalResult_t4581(t *testing.T) {
	hk := placementOutboxServer(t)
	h := placementOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	trace := placementTraceSnapshot()
	trace.Outcome, trace.Reason = "failed", "quota_refresh_pending"
	trace.Record = &fleet.PlacementRecord{JobID: task.Job, Attempt: task.Attempt, Outcome: trace.Outcome, Reason: trace.Reason, Candidates: trace.Candidates}
	h.notePlacementTrace(task, trace, "")
	node := hubSpawnRequest{RequestID: t14RequestID(459000), Machine: "machine-a"}
	task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, node, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	outcome := hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed", Host: "machine-a", NodeResult: true}
	if changed, err := h.completeSpawn(t.Context(), task, outcome); err != nil || !changed {
		t.Fatalf("complete final node result: changed=%v err=%v", changed, err)
	}
	records := hk.records()
	if len(records) != 1 || records[0]["reason"] != "spawn_failed" {
		t.Fatalf("final result missing or replaced by pending-quota record: posts=%d", len(records))
	}
}

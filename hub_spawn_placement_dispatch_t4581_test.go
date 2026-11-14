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
	h := asyncSpawnHub(t)
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
	h.notePlacementTrace(task, newer, "")
	h.retryDeferredTraceSaves()
	payload, _, _, err := h.inbox.store.spawnPlacementSnapshot(t.Context(), request.RequestID, task.Attempt)
	var got fleet.PlacementTrace
	if err != nil || json.Unmarshal(payload, &got) != nil || got.Chosen != "machine-b" {
		t.Fatalf("deferred older trace replaced newer choice: chosen=%s err=%v", got.Chosen, err)
	}
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

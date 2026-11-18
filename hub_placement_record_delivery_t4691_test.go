package panewire

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 4691: placement records stopped reaching hk after the hub outbox (#548)
// took over queue-core spawns. The hub's delivery App writes its warnings to
// io.Discard, so an hk refusal, a spool failure or a credential problem left
// the record pending with nothing in the hub log. These tests drive a real hub
// spawn to the succeeded status through the production path (submit, placement,
// bind, node answer, snapshot, outbox) against a fake hk.

// refusalHK is a fake hk whose POST /v1/placements answer the test chooses.
type refusalHK struct {
	mu      sync.Mutex
	status  int    // POST status; 0 records and answers 200
	body    string // POST body for a refusal
	posts   int
	records []fleet.PlacementRecord
	bodies  [][]byte
}

func newRefusalHK(t *testing.T) *refusalHK {
	t.Helper()
	hk := &refusalHK{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tasks/42":
			_, _ = io.WriteString(w, `{"id":42,"title":"work (grade B, T1)","state":"ready","refs":{}}`)
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/documents/"):
			_, _ = io.WriteString(w, `{"document":{},"changed":true}`)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/placements":
			_, _ = io.WriteString(w, `{"placements":[]}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/placements":
			hk.mu.Lock()
			defer hk.mu.Unlock()
			hk.posts++
			if hk.status != 0 {
				w.WriteHeader(hk.status)
				_, _ = io.WriteString(w, hk.body)
				return
			}
			raw, _ := io.ReadAll(r.Body)
			hk.bodies = append(hk.bodies, raw)
			var record fleet.PlacementRecord
			if json.Unmarshal(raw, &record) != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			hk.records = append(hk.records, record)
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

func (hk *refusalHK) recorded() []fleet.PlacementRecord {
	hk.mu.Lock()
	defer hk.mu.Unlock()
	return append([]fleet.PlacementRecord(nil), hk.records...)
}

func (hk *refusalHK) postCount() int {
	hk.mu.Lock()
	defer hk.mu.Unlock()
	return hk.posts
}

// succeededHubSpawn runs one hub queue-core spawn to the succeeded status: the
// hub places it, binds the node, the node answers rc 0 and the hub verifies the
// landed pane. The hub builds its delivery App the way production does
// (placementAppFn unset). It returns the hub, its log and the request id.
func succeededHubSpawn(t *testing.T, prepare ...func(stateDir string)) (*HubServer, *lockedBuffer, string) {
	t.Helper()
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	stateDir := t.TempDir()
	if real, err := filepath.EvalSymlinks(stateDir); err == nil {
		stateDir = real
	}
	for _, p := range prepare {
		p(stateDir)
	}
	logs := &lockedBuffer{}
	config := HubServerConfig{
		Tokens:         map[string]string{"operator": t14OperatorToken, "desktop": "node-token"},
		InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3"),
		Logger:         slog.New(slog.NewTextHandler(logs, nil)),
	}
	h, err := NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.StateDir = stateDir
		return app, nil
	}
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	node := t14NamedNode(t, h, server, "desktop", true, true)
	h.startSpawnCoordinator(t.Context())
	request := durableSpawnRequest()
	if status, _, _ := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, request); status != http.StatusAccepted {
		t.Fatalf("submission status=%d", status)
	}
	sent := t14ReadNodeSpawn(t, node)
	landed := map[string]any{
		"type": "job.spawn.result", "request_id": sent.RequestID, "rc": 0, "job_id": "test-job", "pane": "w1:p9", "stdout_tail": "",
		"receipt": map[string]any{"job": "test-job", "host": "desktop", "pane": "w1:p9", "profile": "sonnet", "model": "m", "role": "implementer", "cwd": "/work/project.test-job", "label": "test-job", "kind": "claude"},
	}
	if err := wsjson.Write(t.Context(), node, landed); err != nil {
		t.Fatal(err)
	}
	command, asked := readNodeCommand(t, node)
	if !asked || command["op"] != "pane.read" {
		t.Fatalf("the hub did not verify the landed pane: %v", command)
	}
	if err := wsjson.Write(t.Context(), node, map[string]any{"type": "node.command.result", "request_id": command["request_id"], "op": "pane.read", "result": map[string]any{"outcome": "read", "agent_status": "working", "focused": false, "composer": "empty"}}); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "the request to settle as succeeded", func() bool {
		task, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
		return err == nil && task.Status == "succeeded"
	})
	return h, logs, request.RequestID
}

// A landed hub spawn leaves its placement record in hk.
func TestHubSpawnSucceededPlacementRecordReachesHK_t4691(t *testing.T) {
	hk := newRefusalHK(t)
	h, _, requestID := succeededHubSpawn(t)
	var records []fleet.PlacementRecord
	if !waitUntil(5*time.Second, func() bool { records = hk.recorded(); return len(records) > 0 }) {
		payload, delivery, recorded, err := h.inbox.store.spawnPlacementSnapshot(t.Context(), requestID, 1)
		t.Fatalf("no placement record reached hk after the spawn succeeded: recorded=%v err=%v payload=%s delivery=%s", recorded, err, payload, delivery)
	}
	// hk rejects unknown JSON keys, so a hub-built record may carry only the
	// wire fields the producer pins (internal/fleet placement_record_test.go).
	hk.mu.Lock()
	var wire map[string]json.RawMessage
	err := json.Unmarshal(hk.bodies[0], &wire)
	hk.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	allowed := strings.Fields("job_id attempt task_id decided_at tool_version source ranking state_source outcome reason candidates chosen_host family model role profile pool pane_id repo account_key")
	for key := range wire {
		if !slices.Contains(allowed, key) {
			t.Errorf("hub-built record carries a field hk does not take: %q", key)
		}
	}
	if len(records) != 1 || records[0].JobID != "test-job" || records[0].Attempt != 1 || records[0].Outcome != "placed" {
		t.Fatalf("records=%+v", records)
	}
	eventually(t, 5*time.Second, "the snapshot to be sealed after delivery", func() bool {
		_, _, recorded, err := h.inbox.store.spawnPlacementSnapshot(t.Context(), requestID, 1)
		return err == nil && recorded
	})
}

// An hk refusal that is not permanent (a token without the placement grant,
// here a 403) leaves the record spooled and its snapshot pending. The hub says
// so once with the request id and hk's answer, however often the flush retries.
func TestHubPlacementRecordRefusedByHKLogsOnce_t4691(t *testing.T) {
	hk := newRefusalHK(t)
	hk.status, hk.body = http.StatusForbidden, `{"error":"forbidden","reason":"token has no host grant"}`
	h, logs, requestID := succeededHubSpawn(t)
	eventually(t, 5*time.Second, "the first refused post", func() bool { return hk.postCount() > 0 })
	for range 3 {
		h.flushPlacementRecords(t.Context())
	}
	if n := hk.postCount(); n < 3 {
		t.Fatalf("the outbox stopped retrying after %d posts", n)
	}
	if len(hk.recorded()) != 0 {
		t.Fatal("a refused record reached hk")
	}
	_, _, recorded, err := h.inbox.store.spawnPlacementSnapshot(t.Context(), requestID, 1)
	if err != nil || recorded {
		t.Fatalf("a record hk refused must stay pending: recorded=%v err=%v", recorded, err)
	}
	var lines []string
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "placement record not delivered") {
			lines = append(lines, line)
		}
	}
	if len(lines) != 1 {
		t.Fatalf("want one placement stall line, got %d in:\n%s", len(lines), logs.String())
	}
	for _, want := range []string{"request_id=" + requestID, "job=test-job", "attempt=1", "hk HTTP 403", "token has no host grant"} {
		if !strings.Contains(lines[0], want) {
			t.Fatalf("stall line lacks %q: %s", want, lines[0])
		}
	}
	if strings.Contains(logs.String(), "test-placement-token") {
		t.Fatal("the hub log carries the hk credential")
	}
}

// A permanent hk refusal (a 422 here) quarantines the record and seals the
// snapshot, so it is never retried: that drop is logged, with hk's reason.
func TestHubPlacementRecordQuarantinedByHKIsLogged_t4691(t *testing.T) {
	hk := newRefusalHK(t)
	hk.status, hk.body = http.StatusUnprocessableEntity, `{"error":"invalid_placement","reason":"tool_version is not a release"}`
	h, logs, requestID := succeededHubSpawn(t)
	eventually(t, 5*time.Second, "the snapshot to be sealed", func() bool {
		_, _, recorded, err := h.inbox.store.spawnPlacementSnapshot(t.Context(), requestID, 1)
		return err == nil && recorded
	})
	var lines []string
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "placement record refused by hk") {
			lines = append(lines, line)
		}
	}
	if len(lines) != 1 {
		t.Fatalf("want one quarantine line, got %d in:\n%s", len(lines), logs.String())
	}
	for _, want := range []string{"request_id=" + requestID, "job=test-job", "attempt=1", "hk HTTP 422", "tool_version is not a release"} {
		if !strings.Contains(lines[0], want) {
			t.Fatalf("quarantine line lacks %q: %s", want, lines[0])
		}
	}
	if len(hk.recorded()) != 0 {
		t.Fatal("a refused record reached hk")
	}
}

// A record hk refused for an outage is delivered by the same outbox once hk
// recovers.
func TestHubPlacementRecordDeliveredAfterRefusalClears_t4691(t *testing.T) {
	hk := newRefusalHK(t)
	hk.status, hk.body = http.StatusServiceUnavailable, `{"error":"unavailable"}`
	h, logs, requestID := succeededHubSpawn(t)
	eventually(t, 5*time.Second, "the first refused post", func() bool { return hk.postCount() > 0 })
	hk.mu.Lock()
	hk.status = 0
	hk.mu.Unlock()
	h.flushPlacementRecords(t.Context())
	eventually(t, 5*time.Second, "the record to reach hk once it recovers", func() bool { return len(hk.recorded()) == 1 })
	eventually(t, 5*time.Second, "the snapshot to be sealed", func() bool {
		_, _, recorded, err := h.inbox.store.spawnPlacementSnapshot(t.Context(), requestID, 1)
		return err == nil && recorded
	})
	if !strings.Contains(logs.String(), "hk HTTP 503") {
		t.Fatalf("the outage was not logged:\n%s", logs.String())
	}
}

// The stall note logs a reason once per attempt, logs a changed reason again,
// and forgets an attempt once its record is delivered.
func TestHubPlacementStallLogsOncePerReason_t4691(t *testing.T) {
	logs := &lockedBuffer{}
	h := &HubServer{logger: slog.New(slog.NewTextHandler(logs, nil)), now: time.Now}
	count := func() int { return strings.Count(logs.String(), "placement record not delivered") }
	h.notePlacementStall("req", "job", 1, "hk_unreachable")
	h.notePlacementStall("req", "job", 1, "hk_unreachable")
	if count() != 1 {
		t.Fatalf("a repeated reason logged %d times", count())
	}
	h.notePlacementStall("req", "job", 1, "hk HTTP 403 forbidden")
	h.notePlacementStall("req", "job", 2, "hk_unreachable")
	if count() != 3 {
		t.Fatalf("a changed reason and a second attempt logged %d lines, want 3", count())
	}
	if !h.reportPlacementDelivery("req", "job", 1, fleet.PlacementDelivery{Delivered: true}) {
		t.Fatal("a delivered record must seal its snapshot")
	}
	h.notePlacementStall("req", "job", 1, "hk_unreachable")
	if count() != 4 {
		t.Fatalf("a stall after delivery was not logged again: %d", count())
	}
	if h.reportPlacementDelivery("req", "job", 2, fleet.PlacementDelivery{Reason: "hk_unreachable"}) {
		t.Fatal("an undelivered record must not seal its snapshot")
	}
}

// A spool that cannot be created fails every flush the same way. The hub says
// so once, with the request id, job and a reason that names the spool, rather
// than a warning per flush.
func TestHubPlacementRecordSpoolFailureLogsOnce_t4691(t *testing.T) {
	newRefusalHK(t)
	h, logs, requestID := succeededHubSpawn(t, func(stateDir string) {
		// A regular file where the spool directory belongs.
		if err := os.WriteFile(filepath.Join(stateDir, "placements"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	})
	if !waitUntil(5*time.Second, func() bool { return strings.Contains(logs.String(), "reason=spool_") }) {
		t.Fatalf("the spool failure was not reported:\n%s", logs.String())
	}
	for range 3 {
		h.flushPlacementRecords(t.Context())
	}
	// The enqueue path (no spooled file yet) fails the same way and reports once.
	for range 3 {
		h.deliverPlacementRecord(t.Context(), requestID, 1)
	}
	if n := strings.Count(logs.String(), "spool_enqueue_failed"); n != 1 {
		t.Fatalf("enqueue failure logged %d times:\n%s", n, logs.String())
	}
	var lines []string
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "placement record") && !strings.Contains(line, "hub spawn") && !strings.Contains(line, "spool_enqueue_failed") {
			lines = append(lines, line)
		}
	}
	if len(lines) != 1 {
		t.Fatalf("want one placement line for a persistent spool failure, got %d:\n%s", len(lines), strings.Join(lines, "\n"))
	}
	for _, want := range []string{"request_id=" + requestID, "job=test-job", "attempt=1", "reason=spool_"} {
		if !strings.Contains(lines[0], want) {
			t.Fatalf("line lacks %q: %s", want, lines[0])
		}
	}
}

// With the asynchronous worker on, a succeeded attempt that never saved a
// snapshot is reported when it completes: the worker only walks snapshot rows.
func TestHubSucceededAttemptWithoutSnapshotIsReported_t4691(t *testing.T) {
	newRefusalHK(t)
	h := placementOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	logs := &lockedBuffer{}
	h.logger = slog.New(slog.NewTextHandler(logs, nil))
	h.placementAsync.Store(true)
	task := boundSpawnTask(t, h, durableSpawnRequest())
	outcome := hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "w1:p9", NodeResult: true}
	if changed, err := h.completeSpawn(t.Context(), task, outcome); err != nil || !changed {
		t.Fatalf("complete: changed=%v err=%v", changed, err)
	}
	out := logs.String()
	if !strings.Contains(out, "a landed attempt has no placement snapshot") || !strings.Contains(out, "request_id="+task.Request.RequestID) {
		t.Fatalf("the missing snapshot was not reported:\n%s", out)
	}
}

package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/internal/spawnargs"
)

// spawnDropHub answers the node's ping, sends each queued raw frame once, and
// records every job.spawn.result the node writes back.
type spawnDropHub struct {
	frames [][]byte

	mu      sync.Mutex
	results []map[string]any
}

func (hub *spawnDropHub) serve(t *testing.T) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.CloseNow()
		sent := false
		for {
			var message map[string]any
			if wsjson.Read(request.Context(), connection, &message) != nil {
				return
			}
			switch message["type"] {
			case "ping":
				_ = wsjson.Write(request.Context(), connection, map[string]any{"type": "pong"})
				if !sent {
					sent = true
					for _, frame := range hub.frames {
						if connection.Write(request.Context(), websocket.MessageText, frame) != nil {
							return
						}
					}
				}
			case "job.spawn.result":
				hub.mu.Lock()
				hub.results = append(hub.results, message)
				hub.mu.Unlock()
			}
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func (hub *spawnDropHub) resultCount() int {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	return len(hub.results)
}

func (hub *spawnDropHub) firstResult(t *testing.T) map[string]any {
	t.Helper()
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if len(hub.results) == 0 {
		t.Fatal("the node answered no spawn result")
	}
	return hub.results[0]
}

// badSpawnFrame is a job.spawn the node's argument rule refuses: the operator
// request holds a '#' the node's value class does not admit.
func badSpawnFrame(requestID string) []byte {
	return []byte(fmt.Sprintf(`{"type":"job.spawn","request_id":%q,"cwd_key":"project","brief":{"doc":"briefs/job-a.md"},"args":["--job","job-a","--operator-request","hk:doc/x#y"],"wait_seconds":60,"deadline_ms":%d,"operator_authorized":true}`, requestID, time.Now().Add(time.Hour).UnixMilli()))
}

func spawnDropClient(t *testing.T, jobsRoot, endpoint string, warn func(string)) *HubClient {
	t.Helper()
	client, err := NewHubClient(HubClientConfig{URL: endpoint, MachineID: "machine-a", Token: "node-token", AllowInsecureForTests: true, PingInterval: 20 * time.Millisecond,
		JobsInboxRoot:       jobsRoot,
		Warn:                warn,
		sessionCollector:    func(context.Context) ([]HerdrAgentState, error) { return nil, nil },
		hostLoadCollector:   func(context.Context) (HubHostLoad, error) { return HubHostLoad{}, nil },
		hostMemoryCollector: func(context.Context) (*HubHostMemory, error) { return nil, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

type warningLog struct {
	mu       sync.Mutex
	messages []string
}

func (log *warningLog) add(message string) {
	log.mu.Lock()
	log.messages = append(log.messages, message)
	log.mu.Unlock()
}

func (log *warningLog) all() []string {
	log.mu.Lock()
	defer log.mu.Unlock()
	return append([]string(nil), log.messages...)
}

// nodeAgainstFakeHub runs one node session against a fake hub that sends
// frames once, and returns the hub plus the node's warn lines.
func nodeAgainstFakeHub(t *testing.T, jobsRoot string, frames ...[]byte) (*spawnDropHub, *warningLog) {
	t.Helper()
	hub := &spawnDropHub{frames: frames}
	server := hub.serve(t)
	warnings := &warningLog{}
	client := spawnDropClient(t, jobsRoot, t14WSURL(server.URL, ""), warnings.add)
	pinned := time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)
	client.now = func() time.Time { return pinned }
	connection, _, err := websocket.Dial(t.Context(), client.endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = client.serve(ctx, connection)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return hub, warnings
}

// A job.spawn the node's argument rule refuses must not vanish: the node
// answers the hub with a refusal for that request id, so the hub's operation
// ends failed instead of waiting out its request deadline. Nothing runs.
func TestNodeAnswersArgRefusedSpawn(t *testing.T) {
	requestID := "d676220b-e763-4af1-aefd-0000000000a1"
	if _, ok := parseHubOutbound(badSpawnFrame(requestID)); ok {
		t.Fatal("fixture must be refused by the node's argument rule")
	}
	jobsRoot := t.TempDir()
	hub, warnings := nodeAgainstFakeHub(t, jobsRoot, badSpawnFrame(requestID))

	eventually(t, 5*time.Second, "the node answered the refused spawn", func() bool { return hub.resultCount() >= 1 })
	result := hub.firstResult(t)
	if result["type"] != "job.spawn.result" || result["request_id"] != requestID || result["error"] != "invalid_args" {
		t.Fatalf("refusal result %v", result)
	}
	if tail, _ := result["stdout_tail"].(string); !strings.Contains(tail, "--operator-request") || !strings.Contains(tail, "[A-Za-z0-9._:/@-]") {
		t.Fatalf("refusal does not name the flag and rule: %q", tail)
	}
	if _, err := os.Stat(filepath.Join(jobsRoot, "job-a")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the node executed a refused spawn: %v", err)
	}
	if lines := warnings.all(); len(lines) != 1 || !strings.Contains(lines[0], "type=job.spawn") || !strings.Contains(lines[0], "class=argument_pattern") || strings.Contains(lines[0], "hk:doc/x#y") {
		t.Fatalf("want one text-free warning naming the class, got %q", lines)
	}
}

// A job.spawn the node cannot accept for any other reason answers the hub the
// same way when its request id is readable, so the operation still ends
// instead of waiting for a result nothing will send.
func TestNodeAnswersShapeRefusedSpawn(t *testing.T) {
	requestID := "d676220b-e763-4af1-aefd-0000000000a2"
	frame := []byte(fmt.Sprintf(`{"type":"job.spawn","request_id":%q,"cwd_key":"project","brief":{"doc":"briefs/job-a.md"},"args":["--job","job-a"],"wait_seconds":0,"deadline_ms":%d,"operator_authorized":true}`, requestID, time.Now().Add(time.Hour).UnixMilli()))
	if _, ok := parseHubOutbound(frame); ok {
		t.Fatal("fixture must be refused for its shape")
	}
	jobsRoot := t.TempDir()
	hub, warnings := nodeAgainstFakeHub(t, jobsRoot, frame)

	eventually(t, 5*time.Second, "the node answered the refused spawn", func() bool { return hub.resultCount() >= 1 })
	result := hub.firstResult(t)
	if result["request_id"] != requestID || result["error"] != "invalid_request" {
		t.Fatalf("refusal result %v", result)
	}
	if lines := warnings.all(); len(lines) != 1 || !strings.Contains(lines[0], "class=invalid_request") {
		t.Fatalf("want one warning naming the class, got %q", lines)
	}
}

// A job.spawn whose request id cannot be read stays a logged drop: there is
// nothing to answer.
func TestNodeDropsSpawnWithoutReadableRequestID(t *testing.T) {
	frame := []byte(fmt.Sprintf(`{"type":"job.spawn","request_id":"not-a-request","cwd_key":"project","brief":{"doc":"briefs/job-a.md"},"args":["--job","job-a","--operator-request","hk:doc/x#y"],"wait_seconds":60,"deadline_ms":%d,"operator_authorized":true}`, time.Now().Add(time.Hour).UnixMilli()))
	jobsRoot := t.TempDir()
	hub, warnings := nodeAgainstFakeHub(t, jobsRoot, frame)

	eventually(t, 5*time.Second, "the node logged the drop", func() bool { return len(warnings.all()) >= 1 })
	time.Sleep(200 * time.Millisecond) // a window in which a wrong answer would arrive
	if hub.resultCount() != 0 {
		t.Fatalf("answered a frame with no readable request id: %v", hub.firstResult(t))
	}
	if lines := warnings.all(); len(lines) != 1 || !strings.Contains(lines[0], "class=argument_pattern") {
		t.Fatalf("want one warning naming the class, got %q", lines)
	}
}

// The refusal the node writes is the result the hub already parses: an
// operation bound to that node request ends terminal failed with the flag and
// rule, and dispatch returns without waiting for the deadline.
func TestHubMarksArgRefusedSpawnFailedWithoutDeadline(t *testing.T) {
	h := asyncSpawnHub(t)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	node := t14Node(t, h, server, true)
	request := autoSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	transport := &directSpawnTransport{h: h, task: task}
	started := time.Now()
	done := make(chan error, 1)
	go func() {
		_, err := transport.Spawn(boundedContext(t), refusalNodeRequest(request, 907, "machine-a"))
		done <- err
	}()
	sent := t14ReadNodeSpawn(t, node)
	tail := hubSpawnTailWithStderr("", "--operator-request value has a character outside [A-Za-z0-9._:/@-]\nFix: correct the named flag")
	refusal := spawnResultWire(hubSpawnResult{RequestID: sent.RequestID, RC: ExitUsage, Error: "invalid_args", StdoutTail: tail}, false)
	if err := wsjson.Write(t.Context(), node, refusal); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("dispatch waited %s for a refusal it had already received", elapsed)
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if saved.Status != "failed" || !strings.Contains(saved.Outcome.Reason, "invalid_args") || !strings.Contains(saved.Outcome.Reason, "--operator-request") {
		t.Fatalf("arg refusal did not end the operation failed: status=%s outcome=%+v", saved.Status, saved.Outcome)
	}
	if saved.Outcome.Retryable {
		t.Fatalf("an argument refusal released the reservation for an identical replay: %+v", saved.Outcome)
	}
}

// The hub refuses at submit a request whose node args the node's own rule
// would drop, and names the flag; the same request with a valid value is
// accepted.
func TestHubSubmitRefusesSpawnArgsTheNodeWouldDrop(t *testing.T) {
	h := asyncSpawnHub(t)
	request := durableSpawnRequest()
	request.Args = append(request.Args, "--operator-request", "hk:doc/x#y")
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	submit := func(body []byte) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/v1/spawns", strings.NewReader(string(body)))
		r.Header.Set("Authorization", "Bearer "+t14OperatorToken)
		w := httptest.NewRecorder()
		h.Handler().ServeHTTP(w, r)
		return w
	}
	w := submit(raw)
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "--operator-request") || !strings.Contains(w.Body.String(), "[A-Za-z0-9._:/@-]") {
		t.Fatalf("submit accepted a node-refused arg: %d %s", w.Code, w.Body)
	}
	if _, err := h.inbox.store.spawnTask(t.Context(), request.RequestID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("a refused submission bound an operation: %v", err)
	}
	request.Args[len(request.Args)-1] = "hk:doc/x/y"
	if raw, err = json.Marshal(request); err != nil {
		t.Fatal(err)
	}
	if w = submit(raw); w.Code != http.StatusAccepted {
		t.Fatalf("valid operator request refused: %d %s", w.Code, w.Body)
	}
}

// A node that marks an argument refusal retryable still ends the operation
// failed with the reservation held: replaying the same saved request meets the
// same rule, so the operator corrects the flag instead.
func TestArgRefusalStaysFailedWhenNodeMarksRetryable(t *testing.T) {
	h := asyncSpawnHub(t)
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	outcome := hubSpawnOutcome{RC: ExitUsage, Stage: "placement", Reason: "invalid_args: --operator-request value has a character outside [A-Za-z0-9._:/@-]", Host: "machine-a", Retryable: true, NodeResult: true}
	if _, err = h.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil {
		t.Fatal(err)
	}
	var status string
	var reservations int
	if err = h.inbox.store.db.QueryRow(`SELECT status FROM spawn_operations`).Scan(&status); err != nil || status != "failed" {
		t.Fatalf("status=%q %v, want failed", status, err)
	}
	if err = h.inbox.store.db.QueryRow(`SELECT count(*) FROM spawn_reservations`).Scan(&reservations); err != nil || reservations != 1 {
		t.Fatalf("reservations=%d %v, want the reservation held", reservations, err)
	}
}

// A valid spawn's hub args and the node's acceptance of them are unchanged:
// the same flags, byte for byte, and the frame the hub builds still parses.
func TestSpawnArgsValidRequestPreserved(t *testing.T) {
	want := []string{
		"--keep",
		"--operator-request", "hk:doc/decision/2026-11-11/model-routing-escalation/A17-6",
		"--requested-by", "desk",
		"--purpose", "operator-request",
		"-m", "sonnet",
		"-w", "workers",
		"-l", "job-a",
		"--t", "T2",
		"--job", "job-a",
		"--owner", "desk",
		"--effort", "high",
		"--role", "implementer",
		"--lane", "job-a",
		"--parent", "desk",
		"--task", "3965",
		"--report-relative", "job-a/report.md",
		"-L", "live",
		"--urgency", "normal",
		"--author-family", "anthropic",
		"--builder-job", "job-b",
		"--grade", "B",
		"--review-pass", "2",
		"--override-reason", "hk 3965",
		"--containment", "hk:3965",
	}
	if !validHubSpawnArgsForMode(want, true) {
		t.Fatalf("the node refuses the preserved valid args: %s", spawnargs.Refusal(want, true))
	}
	frame := fmt.Sprintf(`{"type":"job.spawn","request_id":"d676220b-e763-4af1-aefd-0000000000b1","cwd_key":"project","brief":{"doc":"briefs/job-a.md"},"args":%s,"wait_seconds":60,"deadline_ms":%d,"operator_authorized":true}`, mustJSON(want), time.Now().Add(time.Hour).UnixMilli())
	parsed, ok := parseHubOutbound([]byte(frame))
	if !ok || !parsed.OperatorAuthorized || strings.Join(parsed.Args, " ") != strings.Join(want, " ") {
		t.Fatalf("the node no longer accepts the hub's valid frame: %v %+v", ok, parsed)
	}
}

package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/internal/fleet"
)

func TestAsyncSpawnLateResultReconcilesAgainstSameNode(t *testing.T) {
	for _, outcome := range []string{nodePaneReadOK, nodePaneChanged, "completed-and-reaped", "completed-wrong-pane"} {
		t.Run(outcome, func(t *testing.T) {
			h := asyncSpawnHub(t)
			if strings.HasPrefix(outcome, "completed-") {
				paneID := "pane-1"
				if outcome == "completed-wrong-pane" {
					paneID = "another-pane"
				}
				h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
					return &fleet.App{JobStore: spawnJobsFixture{record: map[string]any{"state": "completed", "host": "machine-a", "pane_id": paneID, "label": "test-job", "profile": "codex-sol", "task_id": 42}}, Out: out, Err: errout}, nil
				}
			}

			server := httptest.NewServer(h.Handler())
			defer server.Close()
			node := t14Node(t, h, server, true)
			req := durableSpawnRequest()
			task, _, err := h.inbox.store.claimSpawn(t.Context(), req, "test-job", "parent", time.Now())
			if err != nil {
				t.Fatal(err)
			}
			spec := *req.Repository.Checkout
			dispatch := hubSpawnRequest{RequestID: t14RequestID(881), Machine: "machine-a", CWDKey: "project", Brief: hubSpawnBrief{Inline: "task"}, Checkout: &hubSpawnCheckout{Origin: spec.Origin, Mode: spec.Mode, Branch: spec.Branch, Sha: spec.Sha, Dir: spec.Dir}, Args: []string{"--job", "test-job", "-l", "test-job", "-m", "codex-sol", "--t", "T1", "--task", "42", "--owner", "parent", "--role", "worker"}, WaitSeconds: 600, DeadlineMS: task.Deadline.UnixMilli()}
			task, err = h.inbox.store.bindSpawn(t.Context(), req.RequestID, task.Attempt, dispatch)
			if err != nil {
				t.Fatal(err)
			}
			first := &directSpawnTransport{h: h, task: task}
			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := first.dispatch(ctx, dispatch); done <- err }()
			sent := t14ReadNodeSpawn(t, node)
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("missing result was success")
				}
			case <-time.After(time.Second):
				t.Fatal("deadline did not return")
			}
			saved, err := h.inbox.store.spawnTask(t.Context(), req.RequestID)
			if err != nil || saved.Status != "deadline_exceeded" {
				t.Fatalf("timeout lost ownership: %+v %v", saved, err)
			}
			second := &directSpawnTransport{h: h, task: saved}
			ctx2, cancel2 := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel2()
			go func() { _, err := second.dispatch(ctx2, *saved.Node); done <- err }()
			replay := t14ReadNodeSpawn(t, node)
			if replay.RequestID != sent.RequestID {
				t.Fatal("recovery created a new execution request")
			}
			receipt := fleet.ExecutionReceipt{Job: "test-job", Host: "machine-a", Pane: "pane-1", Profile: "codex-sol", Model: "model-fixture", Role: "worker", CWD: "/work/" + spec.Dir, Label: "test-job", Kind: "codex"}
			if err = wsjson.Write(t.Context(), node, map[string]any{"type": "job.spawn.result", "request_id": replay.RequestID, "rc": 0, "job_id": "test-job", "pane": "pane-1", "stdout_tail": "OK pane=pane-1 job=test-job", "receipt": receipt, "stage": "landing", "stage_ms": map[string]int64{"landing": 12}}); err != nil {
				t.Fatal(err)
			}
			command := nodeCommandRead(t, node)
			var args nodePaneReadArgs
			if json.Unmarshal(command["args"], &args) != nil || args.PaneID != receipt.Pane || args.Cwd != receipt.CWD || args.Name != receipt.Label || args.Kind != receipt.Kind {
				t.Fatalf("late success lacked exact node check: %+v", command)
			}
			result := PaneReadResult{Outcome: outcome}
			if outcome == nodePaneReadOK {
				result.AgentStatus = "working"
				result.Composer = nodeComposerUnknown
			}
			if strings.HasPrefix(outcome, "completed-") {
				result.Outcome = nodePaneGone
			}
			nodeCommandReplyTo(t, node, command, map[string]any{"result": result})
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("reconciliation did not return")
			}
			saved, err = h.inbox.store.spawnTask(t.Context(), req.RequestID)
			want := "deadline_exceeded"
			if outcome == nodePaneReadOK || outcome == "completed-and-reaped" {
				want = "succeeded"
			}
			if err != nil || saved.Status != want || saved.Node.Machine != "machine-a" {
				t.Fatalf("reconciled=%+v want=%s err=%v", saved, want, err)
			}
		})
	}
}
func TestAsyncSpawnLostEventRetriesWithStableJobIdentity(t *testing.T) {
	h := asyncSpawnHub(t)
	fake, client, stop := newFakeHandoffkeep(t)
	defer stop()
	h.handoffkeep = client
	req := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), req, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "pane-1"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	fake.status = 500
	fake.mu.Unlock()
	h.flushSpawnNotifications(t.Context())
	var delivered int
	if err = h.inbox.store.db.QueryRow(`SELECT delivered FROM spawn_notifications`).Scan(&delivered); err != nil || delivered != 0 {
		t.Fatalf("lost event ACKed: %d %v", delivered, err)
	}
	fake.mu.Lock()
	fake.status = http.StatusCreated
	fake.mu.Unlock()
	h.flushSpawnNotifications(t.Context())
	if err = h.inbox.store.db.QueryRow(`SELECT delivered FROM spawn_notifications`).Scan(&delivered); err != nil || delivered != 1 {
		t.Fatalf("recovery not ACKed: %d %v", delivered, err)
	}
	if _, err = h.inbox.store.db.Exec(`UPDATE spawn_notifications SET delivered=0`); err != nil {
		t.Fatal(err)
	}
	h.flushSpawnNotifications(t.Context())
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.rows) != 1 {
		t.Fatalf("lost ACK duplicated event: %+v", fake.rows)
	}
	for _, row := range fake.rows {
		if row.JobID != "test-job" || row.Kind != "lane.event" || !strings.HasPrefix(row.Text, "[panewire] SPAWNED test-job") {
			t.Fatalf("job identity lost: %+v", row)
		}
	}
}
func TestAsyncSpawnWorkersBoundedAndIndependentOfHTTP(t *testing.T) {
	h := asyncSpawnHub(t)
	req := durableSpawnRequest()
	for i := 0; i < hubSpawnWorkerCount+2; i++ {
		current := req
		current.RequestID = t14RequestID(900 + i)
		current.Args = append([]string(nil), req.Args...)
		for j, v := range current.Args {
			if v == "test-job" {
				current.Args[j] = fmt.Sprintf("job-%d", i)
			}
			if v == "42" {
				current.Args[j] = fmt.Sprint(42 + i)
			}
		}
		if _, _, err := h.inbox.store.claimSpawn(t.Context(), current, fmt.Sprintf("job-%d", i), "parent", time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	started := make(chan struct{}, hubSpawnWorkerCount+2)
	release := make(chan struct{})
	defer close(release)
	var active, peak atomic.Int32
	h.spawnNewApp = func(io.Writer, io.Writer) (*fleet.App, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		started <- struct{}{}
		<-release
		return nil, errors.New("fixture configuration refusal")
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	h.startSpawnCoordinator(ctx)
	for range hubSpawnWorkerCount {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("bounded workers did not start")
		}
	}
	select {
	case <-started:
		t.Fatalf("more than %d workers executed", hubSpawnWorkerCount)
	case <-time.After(30 * time.Millisecond):
	}
	if peak.Load() != hubSpawnWorkerCount {
		t.Fatal(peak.Load())
	}
}

func TestAsyncSpawnOutlivesSubmittingHTTPContext(t *testing.T) {
	h := asyncSpawnHub(t)
	request := durableSpawnRequest()
	raw, _ := json.Marshal(request)
	ctx, cancel := context.WithCancel(t.Context())
	r := httptest.NewRequest("POST", "/v1/spawns", strings.NewReader(string(raw))).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer "+t14OperatorToken)
	w := httptest.NewRecorder()
	h.Handler().ServeHTTP(w, r)
	if w.Code != 202 {
		t.Fatalf("accept %d %s", w.Code, w.Body)
	}
	cancel()
	executed := make(chan struct{}, 1)
	h.spawnNewApp = func(io.Writer, io.Writer) (*fleet.App, error) {
		executed <- struct{}{}
		return nil, errors.New("fixture admission refusal")
	}
	h.startSpawnCoordinator(t.Context())
	select {
	case <-executed:
	case <-time.After(time.Second):
		t.Fatal("HTTP cancellation cancelled accepted work")
	}
}
func TestNodeSpawnExpiredAbsoluteDeadlineSkipsEngine(t *testing.T) {
	client := &HubClient{}
	called := false
	client.spawnEngine = func(context.Context, string, string, string, string, []string) (*hubSpawnOutput, fleet.Execution) {
		called = true
		return &hubSpawnOutput{}, fleet.Execution{}
	}
	message := hubOutboundMessage{RequestID: t14RequestID(999), DeadlineMS: time.Now().Add(-time.Second).UnixMilli(), WaitSeconds: 600, Args: []string{"--job", "test-job"}}
	result := client.runClaimedHubSpawn(t.Context(), message)
	if called || result.Error != "deadline_not_started" || !result.Retryable || result.RC != 124 {
		t.Fatalf("expired dispatch launched: %v %+v", called, result)
	}
}

func TestAsyncSpawnRefusesOldNodeBeforeBinding(t *testing.T) {
	h := asyncSpawnHub(t)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	t14Node(t, h, server, true)
	h.mu.Lock()
	delete(h.nodes["machine-a"].capabilities, "spawn_ops_v1")
	h.mu.Unlock()
	req := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), req, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	transport := directSpawnTransport{h: h, task: task}
	_, err = transport.Spawn(t.Context(), fleet.HubSpawnRequest{RequestID: t14RequestID(997), Machine: "machine-a"})
	if err == nil || transport.bound || !strings.Contains(err.Error(), "hub_spawn_capability_required") {
		t.Fatalf("old node bound: %v %+v", err, transport)
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), req.RequestID)
	if err != nil || saved.Node != nil {
		t.Fatalf("old node received ownership: %+v %v", saved, err)
	}
}
func TestSpawnFailureFixIsBoundedAndSeparateFromTelemetry(t *testing.T) {
	fix := spawnResultFix("ignored output\nstderr: refused\nFix: Run herdr workspace list on the executing host.\n")
	if fix != "Run herdr workspace list on the executing host." {
		t.Fatal(fix)
	}
	if got := spawnResultFix("Fix: " + strings.Repeat("x", 1000)); len(got) > 512 {
		t.Fatalf("unbounded fix=%d", len(got))
	}
}

package panewire

import (
	"io"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/internal/fleet"
)

func parentUnreadableAnswer(requestID string) map[string]any {
	return map[string]any{
		"type": "job.spawn.result", "request_id": requestID, "rc": 75,
		"job_id": "", "pane": "", "stdout_tail": "stderr: parent metadata unreadable: handoffkeep unavailable\nFix: restore access",
		"error": "spawn_failed", "stage": "placement", "retryable": true,
	}
}

func TestParentUnreadableAutoPlacementTriesNextHost_t6760(t *testing.T) {
	t.Setenv(crashReportDSNEnv, "")
	policy := backoffPolicyFile(t, `{"local_machine":"machine-a","spill_targets":["machine-b"],"max_active_jobs":5,"launch_backoff_minutes":10}`)
	h, err := NewHubServer(HubServerConfig{
		Tokens:         map[string]string{"operator": t14OperatorToken, "machine-a": "node-token", "machine-b": "node-token"},
		InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3"), PlacementPolicyPath: policy,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: spawnJobsFixture{}, Out: out, Err: errout}, nil
	}
	clock := time.Now().UTC()
	h.now = func() time.Time { return clock }
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	firstNode := t14NamedNode(t, h, server, "machine-a", true, true)
	secondNode := t14NamedNode(t, h, server, "machine-b", true, true)
	request := expiringRefusalRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", clock)
	if err != nil {
		t.Fatal(err)
	}
	transport := &directSpawnTransport{h: h, task: task}
	type result struct {
		reply fleet.HubSpawnReply
		err   error
	}
	first := make(chan result, 1)
	go func() {
		reply, err := dispatchPlacedSpawn(boundedContext(t), transport, refusalNodeRequest(request, 67601, "machine-a"))
		first <- result{reply, err}
	}()
	sent := t14ReadNodeSpawn(t, firstNode)
	if err = wsjson.Write(t.Context(), firstNode, parentUnreadableAnswer(sent.RequestID)); err != nil {
		t.Fatal(err)
	}
	got := <-first
	if got.err != nil || !got.reply.NextHost || got.reply.Error != "spawn_failed:parent_unreadable" {
		t.Fatalf("unreadable parent refusal did not move on: reply=%+v err=%v", got.reply, got.err)
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if saved.Status != "accepted" || saved.Node != nil || len(saved.Exclusions) != 1 || saved.Exclusions[0].Machine != "machine-a" || saved.Exclusions[0].ExpiresMS != clock.Add(h.hostRefusalBackoff()).UnixMilli() {
		t.Fatalf("refusal did not save an expiring host exclusion: %+v", saved)
	}
	second := make(chan result, 1)
	go func() {
		reply, err := dispatchPlacedSpawn(boundedContext(t), transport, refusalNodeRequest(request, 67602, "machine-b"))
		second <- result{reply, err}
	}()
	sent = t14ReadNodeSpawn(t, secondNode)
	if sent.RequestID != t14RequestID(67602) {
		t.Fatalf("next candidate got request %s, want the same operation's next attempt", sent.RequestID)
	}
	if err = wsjson.Write(t.Context(), secondNode, map[string]any{
		"type": "job.spawn.result", "request_id": sent.RequestID, "rc": 75,
		"job_id": "", "pane": "", "stdout_tail": "stderr: parent job test-job not found: --parent-job must name an existing job\nFix: use an existing job",
		"error": "spawn_failed", "stage": "placement", "retryable": true,
	}); err != nil {
		t.Fatal(err)
	}
	secondResult := <-second
	if secondResult.err != nil || secondResult.reply.NextHost {
		t.Fatalf("parent-not-found should stay terminal: reply=%+v err=%v", secondResult.reply, secondResult.err)
	}
	saved, err = h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Node == nil || saved.Node.Machine != "machine-b" || saved.Node.RequestID != t14RequestID(67602) {
		t.Fatalf("next candidate was not bound to the same request: %+v %v", saved, err)
	}
}

func TestParentUnreadablePinnedAndNotFoundStayOnTheirHost_t6760(t *testing.T) {
	for _, tc := range []struct {
		name   string
		text   string
		pinned bool
	}{
		{"pinned unreadable", "parent metadata unreadable: handoffkeep unavailable", true},
		{"parent missing", "parent job test-job not found: --parent-job must name an existing job", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := asyncSpawnHub(t)
			request := autoSpawnRequest()
			if tc.pinned {
				for i := 0; i+1 < len(request.Args); i++ {
					if request.Args[i] == "--host" {
						request.Args[i+1] = "machine-a"
					}
				}
				request.Args = append(request.Args, "--containment", "hk:6760", "--override-reason", "test pin")
			}
			task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
			if err != nil {
				t.Fatal(err)
			}
			node := hubSpawnRequest{RequestID: t14RequestID(67603), Machine: "machine-a", Args: request.Args, WaitSeconds: 60}
			task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, node, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			task = sentSpawnFixture(t, h.inbox.store, task)
			transport := &directSpawnTransport{h: h, task: task, bound: true}
			reason := "spawn_failed:parent_unreadable"
			if !tc.pinned {
				reason = "spawn_failed:state_conflict"
			}
			moved, err := transport.nextHostAfterRefusal(t.Context(), node, hubSpawnResult{RC: 75, Error: "spawn_failed", Stage: "placement", StdoutTail: "stderr: " + tc.text, Retryable: true})
			if err != nil || moved {
				t.Fatalf("refusal moved despite %s: moved=%v err=%v", reason, moved, err)
			}
			if len(transport.task.Exclusions) != 0 {
				t.Fatalf("a refusal that did not move on added exclusions: %+v", transport.task.Exclusions)
			}
		})
	}
}

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

func taskPrecheckUnavailableAnswer(requestID string) map[string]any {
	return map[string]any{
		"type": "job.spawn.result", "request_id": requestID, "rc": 70,
		"job_id": "", "pane": "", "stdout_tail": "stderr: task precheck unavailable: handoffkeep: connection refused\nFix: restore hk access",
		"error": "spawn_failed", "stage": "placement", "retryable": true,
	}
}

func TestTaskPrecheckUnavailableAutoPlacementTriesNextHost_t6812(t *testing.T) {
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
		reply, err := dispatchPlacedSpawn(boundedContext(t), transport, refusalNodeRequest(request, 681201, "machine-a"))
		first <- result{reply, err}
	}()
	sent := t14ReadNodeSpawn(t, firstNode)
	if err = wsjson.Write(t.Context(), firstNode, taskPrecheckUnavailableAnswer(sent.RequestID)); err != nil {
		t.Fatal(err)
	}
	got := <-first
	if got.err != nil || !got.reply.NextHost || got.reply.Error != "spawn_failed:task_precheck_unavailable" {
		t.Fatalf("unavailable task precheck did not move on: reply=%+v err=%v", got.reply, got.err)
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
		reply, err := dispatchPlacedSpawn(boundedContext(t), transport, refusalNodeRequest(request, 681202, "machine-b"))
		second <- result{reply, err}
	}()
	sent = t14ReadNodeSpawn(t, secondNode)
	if sent.RequestID != t14RequestID(681202) {
		t.Fatalf("next candidate got request %s, want the same operation's next attempt", sent.RequestID)
	}
	if err = wsjson.Write(t.Context(), secondNode, map[string]any{
		"type": "job.spawn.result", "request_id": sent.RequestID, "rc": 75,
		"job_id": "", "pane": "", "stdout_tail": "stderr: parent job test-job not found: --parent-job must name an existing job\nFix: use an existing job",
		"error": "spawn_failed", "stage": "placement", "retryable": true,
	}); err != nil {
		t.Fatal(err)
	}
	if result := <-second; result.err != nil || result.reply.NextHost {
		t.Fatalf("terminal second-host response=%+v", result)
	}
}

func TestTaskPrecheckUnavailablePinnedHostKeepsRefusal_t6812(t *testing.T) {
	h := asyncSpawnHub(t)
	request := autoSpawnRequest()
	for i := 0; i+1 < len(request.Args); i++ {
		if request.Args[i] == "--host" {
			request.Args[i+1] = "machine-a"
		}
	}
	request.Args = append(request.Args, "--containment", "hk:6812", "--override-reason", "test pin")
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	node := hubSpawnRequest{RequestID: t14RequestID(681203), Machine: "machine-a", Args: request.Args, WaitSeconds: 60}
	task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, node, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	task = sentSpawnFixture(t, h.inbox.store, task)
	transport := &directSpawnTransport{h: h, task: task, bound: true}
	result := hubSpawnResult{RC: 70, Error: "spawn_failed", Stage: "placement", StdoutTail: "stderr: task precheck unavailable: handoffkeep: connection refused", Retryable: true}
	if reason := spawnResultReason(result); reason != "spawn_failed:task_precheck_unavailable" {
		t.Fatalf("pinned request lost the refusal identity: %q", reason)
	}
	moved, err := transport.nextHostAfterRefusal(t.Context(), node, result)
	if err != nil || moved {
		t.Fatalf("pinned refusal moved hosts: moved=%v err=%v", moved, err)
	}
	if len(transport.task.Exclusions) != 0 {
		t.Fatalf("pinned refusal added an exclusion: %+v", transport.task.Exclusions)
	}
}

func TestHKAnsweredTaskPrecheckRefusalStaysTerminal_t6812(t *testing.T) {
	h := asyncSpawnHub(t)
	request := autoSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	node := hubSpawnRequest{RequestID: t14RequestID(681204), Machine: "machine-a", Args: request.Args}
	task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, node, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	task = sentSpawnFixture(t, h.inbox.store, task)
	transport := &directSpawnTransport{h: h, task: task, bound: true}
	result := hubSpawnResult{RC: 2, Error: "spawn_failed", Stage: "placement", StdoutTail: "stderr: task unavailable: merged", Retryable: true}
	if reason := spawnResultReason(result); reason == "spawn_failed:task_precheck_unavailable" {
		t.Fatalf("hk-answered task refusal was mislabeled as an outage: %q", reason)
	}
	moved, err := transport.nextHostAfterRefusal(t.Context(), node, result)
	if err != nil || moved {
		t.Fatalf("hk-answered task refusal moved hosts: moved=%v err=%v", moved, err)
	}
	if len(transport.task.Exclusions) != 0 {
		t.Fatalf("hk-answered task refusal added an exclusion: %+v", transport.task.Exclusions)
	}
}

func TestTaskPrecheckUnavailableDoesNotCountAsLaunchFailure_t6812(t *testing.T) {
	if launchFailureReason("spawn_failed:task_precheck_unavailable") {
		t.Fatal("task precheck outage counted as a launch failure")
	}
}

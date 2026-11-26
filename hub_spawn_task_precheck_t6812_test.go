package panewire

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/internal/fleet"
)

type taskPrecheckTwoAccountSource struct{ now time.Time }

func (s taskPrecheckTwoAccountSource) Hosts(context.Context, string) ([]fleet.HostView, error) {
	return nil, nil
}

func (s taskPrecheckTwoAccountSource) Presence(context.Context) ([]fleet.PresenceRecord, error) {
	return []fleet.PresenceRecord{
		{HostID: "machine-a", AccountKey: "p:0000000000000001", LastSeen: s.now},
		{HostID: "machine-b", AccountKey: "p:0000000000000002", LastSeen: s.now},
	}, nil
}

func (taskPrecheckTwoAccountSource) SharedAccount(_ context.Context, key string) (fleet.SharedAccount, error) {
	zero := 0.0
	return fleet.SharedAccount{Key: key, Observed: true, Fresh: true, Quota: &fleet.PoolView{Status: "ok", Class: "spend", Blocking: &zero}}, nil
}

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

func TestTaskPrecheckHostExclusionReachesCoordinatorAccountSelection_t6812(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	retryHK(t)
	policy := backoffPolicyFile(t, `{"local_machine":"machine-a","spill_targets":["machine-b"],"max_active_jobs":5,"launch_backoff_minutes":10}`)
	h, err := NewHubServer(HubServerConfig{
		Tokens:         map[string]string{"operator": t14OperatorToken, "machine-a": "node-token", "machine-b": "node-token"},
		InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3"), PlacementPolicyPath: policy,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	now := time.Unix(1_800_000_000, 0).UTC()
	h.now = func() time.Time { return now }
	withoutSpawnBackoff(h)
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.Config.Hosts = map[string]map[string]any{"machine-a": {}, "machine-b": {}}
		app.Config.HostOrder = []string{"machine-a", "machine-b"}
		app.Source = taskPrecheckTwoAccountSource{now: app.Now()}
		return app, nil
	}
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	t14NamedNode(t, h, server, "machine-a", true, true)
	nodeB := t14NamedNode(t, h, server, "machine-b", true, true)
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", now)
	if err != nil {
		t.Fatal(err)
	}
	node := hubSpawnRequest{RequestID: t14RequestID(681205), Machine: "machine-a", Args: []string{"--job", "test-job", "-m", "sonnet", "-l", "test-job", "--role", "implementer"}}
	task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, node, now)
	if err != nil {
		t.Fatal(err)
	}
	task = sentSpawnFixture(t, h.inbox.store, task)
	transport := &directSpawnTransport{h: h, task: task, bound: true}
	moved, err := transport.nextHostAfterRefusal(t.Context(), node, hubSpawnResult{
		RC: 70, Error: "spawn_failed", Stage: "placement", Retryable: true,
		StdoutTail: "stderr: task precheck unavailable: handoffkeep: connection refused",
	})
	if err != nil || !moved {
		t.Fatalf("task precheck refusal did not return to coordinator: moved=%v err=%v", moved, err)
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || len(saved.Exclusions) != 1 || saved.Exclusions[0].Machine != "machine-a" || saved.Exclusions[0].ExpiresMS != now.Add(h.hostRefusalBackoff()).UnixMilli() {
		t.Fatalf("saved host exclusion=%+v err=%v", saved.Exclusions, err)
	}

	h.startSpawnCoordinator(t.Context())
	type nodeRead struct {
		message hubOutboundMessage
		err     error
	}
	readCtx, cancelRead := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancelRead()
	read := make(chan nodeRead, 1)
	go func() {
		_, payload, err := nodeB.Read(readCtx)
		if err != nil {
			read <- nodeRead{err: err}
			return
		}
		message, valid := parseHubOutbound(payload)
		if !valid || message.Type != "job.spawn" {
			read <- nodeRead{err: errors.New("machine-b received a non-spawn message")}
			return
		}
		read <- nodeRead{message: message}
	}()
	var received nodeRead
	var placement hubSpawnTask
	eventually(t, 15*time.Second, "machine-b spawn dispatch or another placement outcome", func() bool {
		select {
		case received = <-read:
			return true
		default:
		}
		placement, err = h.inbox.store.spawnTask(t.Context(), request.RequestID)
		return err == nil && (placement.Status == "failed" || placement.ParkReason != "" && placement.ParkReason != spawnParkLaunchBackoff || placement.Node != nil && placement.Node.Machine != "machine-b")
	})
	if received.err != nil || received.message.Type != "job.spawn" {
		t.Fatalf("machine-b was not dispatched after excluding machine-a: state=%+v read_error=%v", placement, received.err)
	}
	sent := received.message
	if sent.RequestID == t14RequestID(681205) {
		t.Fatalf("coordinator did not select machine-b's distinct account after excluding machine-a: %+v", sent)
	}
	if err := wsjson.Write(t.Context(), nodeB, map[string]any{
		"type": "job.spawn.result", "request_id": sent.RequestID, "rc": 70,
		"job_id": "", "pane": "", "stdout_tail": "stderr: terminal test refusal",
		"error": "spawn_failed", "stage": "placement", "retryable": false,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestLongTaskPrecheckErrorKeepsRefusalCode_t6812(t *testing.T) {
	result := hubSpawnResult{
		RC: 70, Error: "spawn_failed",
		StdoutTail: "stderr: task precheck unavailable: handoffkeep: " + strings.Repeat("upstream detail ", 100) + "\nFix: restore handoffkeep",
	}
	if got := spawnFailureCode(result); got != "task_precheck_unavailable" {
		t.Fatalf("long precheck refusal code=%q, want task_precheck_unavailable", got)
	}
}

func TestWithHostRefusalExclusionsIncludesParentUnreadable_t6812(t *testing.T) {
	args := []string{"-m", "auto", "--exclude-host", "operator-choice"}
	live := []hubSpawnExclusion{
		{Machine: "machine-a", Reason: "spawn_failed:task_precheck_unavailable"},
		{Machine: "machine-b", Reason: "spawn_failed:parent_unreadable"},
		{Machine: "machine-c", Reason: "spawn_failed:brief_invalid"},
		{Machine: "machine-d", Reason: "spawn_failed:quota_full", Pool: "claude"},
	}
	got := withHostRefusalExclusions(args, live)
	if value := hubSpawnArg(got, "--exclude-host"); value != "machine-a,machine-b,operator-choice" {
		t.Fatalf("host exclusions=%q, want both next-host refusals merged with existing exclusions", value)
	}
	if args[3] != "operator-choice" {
		t.Fatalf("input args were modified: %v", args)
	}
}

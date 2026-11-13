package panewire

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/internal/fleet"
)

// spawnNotifyHub routes the owner lane "parent" to a connected destination node
// and stores relays in a fake handoffkeep, so a test observes real injections.
func spawnNotifyHub(t *testing.T, policy string) (*HubServer, *fakeHandoffkeep, *hubAgent) {
	t.Helper()
	return spawnNotifyHubLane(t, policy, "parent")
}

// spawnNotifyHubLane is spawnNotifyHub with the connected destination serving
// the named lane.
func spawnNotifyHubLane(t *testing.T, policy, lane string) (*HubServer, *fakeHandoffkeep, *hubAgent) {
	t.Helper()
	return spawnNotifyHubLanes(t, policy, lane)
}

// spawnNotifyHubLanes serves every named lane from the one destination node,
// each on its own pane (w1:p1, w1:p2, ...).
func spawnNotifyHubLanes(t *testing.T, policy string, lanes ...string) (*HubServer, *fakeHandoffkeep, *hubAgent) {
	t.Helper()
	routes := make([]string, len(lanes))
	for i, lane := range lanes {
		routes[i] = fmt.Sprintf(`"%s":{"machine":"machine-b","pane":"w1:p%d"}`, lane, i+1)
	}
	t.Setenv(crashReportDSNEnv, "")
	fake, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	config := HubServerConfig{
		Tokens:          map[string]string{"operator": t14OperatorToken, "machine-a": "node-token", "machine-b": "desk-token"},
		InboxStorePath:  filepath.Join(t.TempDir(), "inbox.sqlite3"),
		ReportRelayPath: r20LanesFile(t, `{"lanes":{`+strings.Join(routes, ",")+`}}`),
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		handoffkeep:     client,
	}
	if policy != "" {
		config.DeliveryPolicyPath = filepath.Join(t.TempDir(), "delivery.yaml")
		if err := os.WriteFile(config.DeliveryPolicyPath, []byte(policy), 0600); err != nil {
			t.Fatal(err)
		}
	}
	h, err := NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 8), persisted: make(chan hubRelayPersistedEvent, 8)}
	h.mu.Lock()
	h.nodes["machine-b"] = &hubNodeRecord{agent: destination}
	h.mu.Unlock()
	return h, fake, destination
}

func spawnFailedInjections(destination *hubAgent) []hubRelayInjectEvent {
	var injected []hubRelayInjectEvent
	for {
		select {
		case relay := <-destination.relays:
			injected = append(injected, relay)
		default:
			return injected
		}
	}
}

func TestSpawnFailedInjectsOneEventIntoOwnerLane(t *testing.T) {
	h, fake, destination := spawnNotifyHub(t, "")
	req := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), req, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	outcome := hubSpawnOutcome{RC: 2, Stage: "placement", Reason: "spawn_failed: cwd is not a directory", Fix: "Check the checkout.", Host: "machine-a"}
	for range 2 { // A repeated outcome is the same transition, not a second event.
		if _, err = h.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	for range 3 {
		h.flushSpawnNotifications(t.Context())
	}
	injected := spawnFailedInjections(destination)
	if len(injected) != 1 {
		t.Fatalf("owner lane received %d SPAWN_FAILED injections, want exactly 1: %+v", len(injected), injected)
	}
	for _, want := range []string{"SPAWN_FAILED", "test-job", "request=" + req.RequestID, "stage=placement", "cwd is not a directory", "Check the checkout."} {
		if !strings.Contains(injected[0].Text, want) {
			t.Fatalf("injection omits %q: %q", want, injected[0].Text)
		}
	}
	if injected[0].Pane != "w1:p1" {
		t.Fatalf("injection went to pane %q", injected[0].Pane)
	}
	if fake.rowCount() != 1 {
		t.Fatalf("durable rows=%d, want 1", fake.rowCount())
	}
	var delivered int
	if err = h.inbox.store.db.QueryRow(`SELECT delivered FROM spawn_notifications`).Scan(&delivered); err != nil || delivered != 1 {
		t.Fatalf("outbox row not retired: %d %v", delivered, err)
	}
}

// A lane that digests reports must still be interrupted by a failed spawn; a
// report-typed failure used to wait in the desk digest.
func TestSpawnFailedInterruptsDigestingLane(t *testing.T) {
	policy := "default: inbox\nlanes:\n  parent:\n    default: inbox\n    inbox_summary:\n      digest_prefix: inbox/parent/digest/\n"
	h, _, destination := spawnNotifyHub(t, policy)
	task, _, err := h.inbox.store.claimSpawn(t.Context(), durableSpawnRequest(), "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 2, Stage: "placement", Reason: "spawn_failed"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.flushSpawnNotifications(t.Context())
	if injected := spawnFailedInjections(destination); len(injected) != 1 {
		t.Fatalf("digesting lane received %d injections, want 1", len(injected))
	}
}

func TestSpawnedStaysQuietForDigestingLane(t *testing.T) {
	policy := "default: inbox\nlanes:\n  parent:\n    default: inbox\n    inbox_summary:\n      digest_prefix: inbox/parent/digest/\n"
	h, _, destination := spawnNotifyHub(t, policy)
	task, _, err := h.inbox.store.claimSpawn(t.Context(), durableSpawnRequest(), "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "pane-1"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.flushSpawnNotifications(t.Context())
	if injected := spawnFailedInjections(destination); len(injected) != 0 {
		t.Fatalf("SPAWNED interrupted a digesting lane: %+v", injected)
	}
}

// The notification text must survive handoffkeep's and the relay's limits: a
// row they reject would stay undelivered forever and block the outbox head.
func TestSpawnFailedTextStaysWithinLaneEventContract(t *testing.T) {
	h, _, destination := spawnNotifyHub(t, "")
	task, _, err := h.inbox.store.claimSpawn(t.Context(), durableSpawnRequest(), "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	outcome := hubSpawnOutcome{RC: 2, Stage: "placement", Reason: "spawn_failed: " + strings.Repeat("€", 2000), Fix: "line one\nline two " + strings.Repeat("x", 4000)}
	if _, err = h.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.flushSpawnNotifications(t.Context())
	injected := spawnFailedInjections(destination)
	if len(injected) != 1 {
		t.Fatalf("oversized failure was not delivered: %+v", injected)
	}
	if !validLaneEventText(strings.TrimPrefix(injected[0].Text, "(같은 내용이 두 번 보이면 재실행 금지) ")) {
		t.Fatalf("injected text breaks the lane event contract: %q", injected[0].Text)
	}
}

func TestSpawnFailedDeliveredThroughNodeResult(t *testing.T) {
	h, _, destination := spawnNotifyHub(t, "")
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	node := t14Node(t, h, server, true)
	req := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), req, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	dispatch := hubSpawnRequest{RequestID: t14RequestID(741), Machine: "machine-a", CWDKey: "project", Brief: hubSpawnBrief{Inline: "task"}, Args: []string{"--job", "test-job"}, WaitSeconds: 600, DeadlineMS: task.Deadline.UnixMilli()}
	if task, err = h.inbox.store.bindSpawn(t.Context(), req.RequestID, task.Attempt, dispatch, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	transport := directSpawnTransport{h: h, task: task}
	done := make(chan error, 1)
	go func() { _, err := transport.dispatch(t.Context(), dispatch); done <- err }()
	sent := t14ReadNodeSpawn(t, node)
	tail := "stderr: symlink path refused: /jobs-root\nFix: Run panewire spawn --help and supply or correct the exact flag named in the refusal."
	if err = wsjson.Write(t.Context(), node, map[string]any{"type": "job.spawn.result", "request_id": sent.RequestID, "rc": 2, "job_id": "", "pane": "", "stdout_tail": tail, "error": "spawn_failed", "stage": "placement", "retryable": true}); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), req.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(saved.Outcome.Reason, "symlink path refused: /jobs-root") || !strings.HasPrefix(saved.Outcome.Reason, "spawn_failed") {
		t.Fatalf("outcome reason lost the node refusal: %q", saved.Outcome.Reason)
	}
	h.flushSpawnNotifications(t.Context())
	injected := spawnFailedInjections(destination)
	if len(injected) != 1 || !strings.Contains(injected[0].Text, "symlink path refused: /jobs-root") {
		t.Fatalf("owner lane did not receive the refusal: %+v", injected)
	}
}

func TestSpawnOutcomeReasonCarriesRefusalAndKeepsStatusCodes(t *testing.T) {
	for _, test := range []struct{ code, tail, want string }{
		{"spawn_failed", "stderr: task has another claimant without a job binding\nFix: x", "spawn_failed: task has another claimant without a job binding"},
		{"spawn_failed", "OK-ish stdout\nstderr: warn: first\nreal refusal\nFix: x", "spawn_failed: warn: first; real refusal"},
		{"spawn_disabled", "spawn.json rule enabled_false\nstderr: Fix: x", "spawn_disabled: spawn.json rule enabled_false"},
		{"cwd_unmapped", "stderr: Fix: x", "cwd_unmapped"},
		{"spawn_failed", "stderr: bad Prefix: value\nFix: x", "spawn_failed: bad Prefix: value"},
		{"spawn_failed", "stderr: Suffix: only, no fix line", "spawn_failed: Suffix: only, no fix line"},
		{"spawn_failed", "stderr: applied HotFix: value", "spawn_failed: applied HotFix: value"},
		{"spawn_failed", "stderr: quota gate: stderr: boom\nFix: x", "spawn_failed: quota gate: stderr: boom"},
		{"spawn_failed", "stdout mentions stderr: here\nstderr: real refusal\nFix: x", "spawn_failed: real refusal"},
		{"spawn_failed", "", "spawn_failed"},
		{"spawn_failed", "stdout only, no refusal section", "spawn_failed"},
		{"timeout", "stderr: late\nFix: x", "timeout"},
		{"disconnected", "stderr: late\nFix: x", "disconnected"},
		{"unknown", "stderr: late\nFix: x", "unknown"},
	} {
		if got := spawnOutcomeReason(test.code, spawnResultRefusal(test.tail)); got != test.want {
			t.Errorf("%s/%q reason=%q want %q", test.code, test.tail, got, test.want)
		}
	}
	long := spawnOutcomeReason("spawn_failed", spawnResultRefusal("stderr: "+strings.Repeat("€", 1000)+"\nFix: x"))
	if len(long) > hubSpawnReasonLimit || !validLaneEventText(long) {
		t.Fatalf("reason exceeds its bound: %d bytes", len(long))
	}
	secret := spawnOutcomeReason("spawn_failed", spawnResultRefusal("stderr: fetch https://user:hunter2@example.test/x failed\nFix: x"))
	if strings.Contains(secret, "hunter2") {
		t.Fatalf("reason kept a credential: %q", secret)
	}
}

func TestSpawnTraceReasonNormalizesRefusalText(t *testing.T) {
	for reason, want := range map[string]string{
		"spawn_failed: symlink path refused: /jobs-root":                "spawn_failed",
		"spawn_failed:symlink_path: symlink path refused: /jobs-root":   "spawn_failed:symlink_path",
		"spawn_failed:quota_unknown":                                    "spawn_failed:quota_unknown",
		"spawn_failed:made_up_code: free text":                          "spawn_failed",
		"spawn_failed:":                                                 "spawn_failed",
		"admission_refused: quota gate unavailable":                     "admission_refused:other",
		"admission_refused: missing seat policy for worker-x":           "admission_refused:seat_policy_missing",
		"admission_refused: quota catalog unavailable":                  "admission_refused:quota_catalog",
		"admission_refused":                                             "admission_refused",
		"parent_job_not_found: parent job pj was not found":             "parent_job_not_found",
		"quota_unconfirmed:no_usable_presence: target account unusable": "quota_unconfirmed",
		"quota_unconfirmed":                                             "quota_unconfirmed",
		"checkout_refused:dirty":                                        "checkout_refused",
		"timeout":                                                       "timeout",
		"some_unrecognized_free_text value":                             "spawn_failed",
	} {
		if got := spawnTraceReason(reason); got != want {
			t.Errorf("%q normalized to %q, want %q", reason, got, want)
		}
	}
}

func TestAdmissionParentJobNotFoundReason(t *testing.T) {
	h, _, destination := spawnNotifyHub(t, "")
	clock := time.Unix(1_800_000_000, 0)
	root := t.TempDir()
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{
			JobStore: spawnJobsFixture{}, Home: root, Jobs: filepath.Join(root, "jobs"), Out: out, Err: errout,
			Now:   func() time.Time { return clock },
			Sleep: func(_ context.Context, d time.Duration) error { clock = clock.Add(d); return nil },
		}, nil
	}
	req := durableSpawnRequest()
	for i := 0; i+1 < len(req.Args); i++ {
		if req.Args[i] == "--task" {
			req.Args[i], req.Args[i+1] = "--parent-job", "never-existed"
		}
	}
	req.Args = append(req.Args, "--report-relative", "test-job/report.md")
	task, _, err := h.inbox.store.claimSpawn(t.Context(), req, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	h.executeSpawnTask(t.Context(), task)
	saved, err := h.inbox.store.spawnTask(t.Context(), req.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if saved.Outcome.RC != 75 || !strings.HasPrefix(saved.Outcome.Reason, "parent_job_not_found: ") || !strings.Contains(saved.Outcome.Reason, "never-existed") {
		t.Fatalf("outcome=%+v", saved.Outcome)
	}
	if !strings.Contains(saved.Outcome.Fix, "--parent-job") {
		t.Fatalf("fix=%q", saved.Outcome.Fix)
	}
	h.flushSpawnNotifications(t.Context())
	injected := spawnFailedInjections(destination)
	if len(injected) != 1 || !strings.Contains(injected[0].Text, "reason=parent_job_not_found") {
		t.Fatalf("injected=%+v", injected)
	}
}

func TestAdmissionUnconfirmedQuotaReason(t *testing.T) {
	err := &fleet.ExitError{Code: 4, Message: "quota gate unavailable"}
	if got := fleet.AdmissionRefusalReason(err); got != "quota_unconfirmed" {
		t.Fatalf("rc=4 reason=%q", got)
	}
	if status := spawnOutcomeStatus(hubSpawnOutcome{RC: 4, Reason: spawnOutcomeReason("quota_unconfirmed", "x"), Retryable: true}); status != "retryable" {
		t.Fatalf("status=%q", status)
	}
}

package panewire

import (
	"bytes"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"

	"github.com/mgh3326/panewire/internal/fleet"
)

// reconcilingRequest is durableSpawnRequest submitted from a second lane, so
// the submitting lane (--parent) and the owner lane are different panes.
func reconcilingRequest() fleet.Submission {
	req := durableSpawnRequest()
	req.Args = append(append([]string(nil), req.Args...), "--parent", "submitter")
	return req
}

// bindDispatch claims req as job test-job and binds it to machine-a, the state
// a dispatch starts from. It returns the task and the node request to dispatch.
func bindDispatch(t *testing.T, h *HubServer, req fleet.Submission) (hubSpawnTask, hubSpawnRequest) {
	t.Helper()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), req, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	node := hubSpawnRequest{RequestID: t14RequestID(741), Machine: "machine-a", CWDKey: "project", Brief: hubSpawnBrief{Inline: "task"}, Args: []string{"--job", "test-job"}, WaitSeconds: 600, DeadlineMS: task.Deadline.UnixMilli()}
	if task, err = h.inbox.store.bindSpawn(t.Context(), req.RequestID, task.Attempt, node, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	return sentSpawnFixture(t, h.inbox.store, task), node
}

func reconcilingStatus(t *testing.T, h *HubServer, task hubSpawnTask) string {
	t.Helper()
	got, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	return got.Status
}

// reconcile moves task to reconciling the way an unknown node answer does.
func reconcile(t *testing.T, h *HubServer, task hubSpawnTask) {
	t.Helper()
	outcome := hubSpawnOutcome{RC: 75, Stage: "landing", Reason: "unknown", Host: "machine-a", NodeResult: true}
	if changed, err := h.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil || !changed {
		t.Fatalf("reconcile changed=%v err=%v", changed, err)
	}
	if got := reconcilingStatus(t, h, task); got != "reconciling" {
		t.Fatalf("setup: status %q, want reconciling", got)
	}
}

// deliveredSpawnTexts flushes the outbox and returns what reached the lanes.
func deliveredSpawnTexts(t *testing.T, h *HubServer, destination *hubAgent) []hubRelayInjectEvent {
	t.Helper()
	h.flushSpawnNotifications(t.Context())
	return spawnFailedInjections(destination)
}

// A dispatch whose node answers unknown leaves the request reconciling. When
// the request later lands, the owner gets one SPAWNED and nothing before it.
func TestReconcilingUnknownThenLandedNotifiesOwnerOnlyOnce(t *testing.T) {
	h, _, destination := spawnNotifyHubLanes(t, "", "parent", "submitter")
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	node := t14Node(t, h, server, true)
	task, request := bindDispatch(t, h, reconcilingRequest())
	transport := directSpawnTransport{h: h, task: task}
	done := make(chan error, 1)
	go func() { _, err := transport.dispatch(t.Context(), request); done <- err }()
	sent := t14ReadNodeSpawn(t, node)
	t14WriteSpawnResult(t, node, sent.RequestID, 75, "", "", "", "unknown")
	<-done
	if got := reconcilingStatus(t, h, task); got != "reconciling" {
		t.Fatalf("setup: status %q, want reconciling", got)
	}
	if injected := deliveredSpawnTexts(t, h, destination); len(injected) != 0 {
		t.Fatalf("reconciling notified a lane: %+v", injected)
	}
	landed := hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "w9:p1", Profile: "codex-sol"}
	if changed, err := h.inbox.store.completeSpawn(t.Context(), task, landed, time.Now()); err != nil || !changed {
		t.Fatalf("landing changed=%v err=%v", changed, err)
	}
	assertOneSpawnNotice(t, deliveredSpawnTexts(t, h, destination), "SPAWNED", "w1:p1")
}

// The same as the unknown case when the node is not connected at dispatch.
func TestReconcilingDisconnectedThenLandedNotifiesOwnerOnlyOnce(t *testing.T) {
	h, _, destination := spawnNotifyHubLanes(t, "", "parent", "submitter")
	task, request := bindDispatch(t, h, reconcilingRequest())
	transport := directSpawnTransport{h: h, task: task}
	if _, err := transport.dispatch(t.Context(), request); err == nil {
		t.Fatal("a disconnected dispatch answered without a pending error")
	}
	if got := reconcilingStatus(t, h, task); got != "reconciling" {
		t.Fatalf("setup: status %q, want reconciling", got)
	}
	if injected := deliveredSpawnTexts(t, h, destination); len(injected) != 0 {
		t.Fatalf("reconciling notified a lane: %+v", injected)
	}
	landed := hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "w9:p1", Profile: "codex-sol"}
	if changed, err := h.inbox.store.completeSpawn(t.Context(), task, landed, time.Now()); err != nil || !changed {
		t.Fatalf("landing changed=%v err=%v", changed, err)
	}
	assertOneSpawnNotice(t, deliveredSpawnTexts(t, h, destination), "SPAWNED", "w1:p1")
}

// A reconciling request that is orphaned notifies its owner and the submitting
// lane once each, exactly as a failure does without a reconciling step.
func TestReconcilingThenFailedNotifiesOwnerAndSubmitterOnce(t *testing.T) {
	h, _, destination := spawnNotifyHubLanes(t, "", "parent", "submitter")
	task, _ := bindDispatch(t, h, reconcilingRequest())
	reconcile(t, h, task)
	if injected := deliveredSpawnTexts(t, h, destination); len(injected) != 0 {
		t.Fatalf("reconciling notified a lane: %+v", injected)
	}
	orphaned := hubSpawnOutcome{RC: 75, Stage: "landing", Reason: spawnOrphanedReason, Host: "machine-a", NodeResult: true}
	if changed, err := h.inbox.store.completeSpawn(t.Context(), task, orphaned, time.Now()); err != nil || !changed {
		t.Fatalf("orphaning changed=%v err=%v", changed, err)
	}
	injected := deliveredSpawnTexts(t, h, destination)
	if len(injected) != 2 {
		t.Fatalf("lanes received %d SPAWN_FAILED, want one for the owner and one for the submitter: %+v", len(injected), injected)
	}
	panes := map[string]int{}
	for _, event := range injected {
		if !strings.Contains(event.Text, "SPAWN_FAILED") || strings.Count(event.Text, "SPAWN_FAILED") != 1 {
			t.Fatalf("not one SPAWN_FAILED: %q", event.Text)
		}
		panes[event.Pane]++
	}
	if panes["w1:p1"] != 1 || panes["w1:p2"] != 1 {
		t.Fatalf("SPAWN_FAILED panes %v, want owner w1:p1 and submitter w1:p2 once each", panes)
	}
}

// Deadlines keep their one SPAWN_FAILED, both when reached directly and when a
// reconciling request runs out its deadline.
func TestDeadlineNotifiesOwnerOnceWithOrWithoutReconciling(t *testing.T) {
	for _, reconcileFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "after reconciling"}[reconcileFirst], func(t *testing.T) {
			h, _, destination := spawnNotifyHubLanes(t, "", "parent", "submitter")
			task, _ := bindDispatch(t, h, reconcilingRequest())
			if reconcileFirst {
				reconcile(t, h, task)
			}
			timeout := hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Host: "machine-a"}
			if changed, err := h.inbox.store.completeSpawn(t.Context(), task, timeout, time.Now()); err != nil || !changed {
				t.Fatalf("timeout changed=%v err=%v", changed, err)
			}
			if got := reconcilingStatus(t, h, task); got != "deadline_exceeded" {
				t.Fatalf("status %q, want deadline_exceeded", got)
			}
			injected := deliveredSpawnTexts(t, h, destination)
			assertOneSpawnNotice(t, injected, "SPAWN_FAILED", "w1:p1")
			if !strings.Contains(injected[0].Text, "reason=timeout") {
				t.Fatalf("deadline notice omits its reason: %q", injected[0].Text)
			}
		})
	}
}

// assertOneSpawnNotice requires exactly one injection, carrying want, into pane.
func assertOneSpawnNotice(t *testing.T, injected []hubRelayInjectEvent, want, pane string) {
	t.Helper()
	if len(injected) != 1 {
		texts := make([]string, len(injected))
		for i, event := range injected {
			texts[i] = event.Text
		}
		t.Fatalf("lanes received %d notices, want exactly one %s: %q", len(injected), want, texts)
	}
	if !strings.Contains(injected[0].Text, want) || injected[0].Pane != pane {
		t.Fatalf("notice %q to pane %q, want %s to %s", injected[0].Text, injected[0].Pane, want, pane)
	}
}

// The reconciling transition is logged as its own Info line, captures no Sentry
// event and does not use up the trace the terminal outcome records.
func TestReconcilingLogsWithoutSentryAndLeavesTheTerminalTrace(t *testing.T) {
	var logs bytes.Buffer
	t.Setenv(crashReportDSNEnv, "")
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3"), Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	transport := &spawnSentryTransport{}
	h.spawnSentry = spawnReasonSentryClient(t, transport)
	task, _ := bindDispatch(t, h, durableSpawnRequest())
	reconciling := hubSpawnOutcome{RC: 75, Stage: "landing", Reason: "unknown", Host: "machine-a", NodeResult: true, StageMS: map[string]int64{"landing": 50}}
	if changed, err := h.inbox.store.completeSpawn(t.Context(), task, reconciling, time.Now()); err != nil || !changed {
		t.Fatalf("reconciling changed=%v err=%v", changed, err)
	}
	h.recordSpawnTrace(task, reconciling)
	transport.mu.Lock()
	events := len(transport.events)
	transport.mu.Unlock()
	if events != 0 {
		t.Fatalf("reconciling captured %d Sentry events, want none", events)
	}
	if strings.Contains(logs.String(), "hub spawn refused") || !strings.Contains(logs.String(), "hub spawn reconciling") {
		t.Fatalf("journal line for reconciling: %q", logs.String())
	}
	timeout := hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Host: "machine-a", NodeResult: true, StageMS: map[string]int64{"landing": 50}}
	if changed, err := h.inbox.store.completeSpawn(t.Context(), task, timeout, time.Now()); err != nil || !changed {
		t.Fatalf("timeout changed=%v err=%v", changed, err)
	}
	h.recordSpawnTrace(task, timeout)
	transport.mu.Lock()
	defer transport.mu.Unlock()
	var transactions, failures int
	for _, event := range transport.events {
		if event.Type == "transaction" {
			transactions++
		} else if event.Level == sentry.LevelError && strings.Join(event.Fingerprint, ":") == "spawn:landing:timeout" {
			failures++
		}
	}
	if transactions != 1 || failures != 1 || len(transport.events) != 2 {
		t.Fatalf("terminal outcome traffic: transactions=%d failures=%d total=%d", transactions, failures, len(transport.events))
	}
}

package panewire

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/internal/fleet"
)

// runUnknownRetry lets the node answer whichever status and inventory reads
// the reconciler actually sends, so the test can distinguish no retry from a
// retry without waiting for a missing command.
func runUnknownRetry(t *testing.T, f *quotaReplaceFixture, action poolAction, attempt string) {
	t.Helper()
	runUnknownRetryWithStatus(t, f, action, attempt, "idle")
}

func runUnknownRetryWithStatus(t *testing.T, f *quotaReplaceFixture, action poolAction, attempt, paneStatus string) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, raw, err := f.node.Read(ctx)
			if err != nil {
				return
			}
			var message struct {
				Type      string `json:"type"`
				RequestID string `json:"request_id"`
				Op        string `json:"op"`
			}
			if json.Unmarshal(raw, &message) != nil || message.Type != "node.command" {
				continue
			}
			var result map[string]any
			switch message.Op {
			case nodeCommandPaneCloseStatus:
				result = map[string]any{"state": fleet.CloseOpUnknown, "released": false, "settled": false, "attempt": attempt}
			case nodeCommandAgentsList:
				result = map[string]any{"agents": []nodeAgentRow{}, "panes": []nodeAgentRow{{PaneID: roleHeldPane, Name: f.job, AgentStatus: paneStatus, Cwd: "/work/project"}}, "panes_ok": true}
			default:
				return
			}
			if wsjson.Write(ctx, f.node, map[string]any{"type": "node.command.result", "request_id": message.RequestID, "op": message.Op, "result": result}) != nil {
				return
			}
		}
	}()
	defer func() { cancel(); <-done }()
	if err := f.hub.reconcilePoolAction(ctx, action); err != nil {
		t.Fatal(err)
	}
}

func TestPoolActionUnknownRetriesAgainAfterRefusedSuccessor(t *testing.T) {
	f, action := poolActionFixture(t)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f.hub.now = func() time.Time { return start }
	runUnknownRetry(t, f, action, action.Data.Args.Attempt)
	actions, err := f.hub.inbox.store.activePoolActions(t.Context())
	if err != nil || len(actions) != 2 {
		t.Fatalf("first retry: %+v %v", actions, err)
	}
	var firstRetry poolAction
	for _, row := range actions {
		if row.ID != action.ID {
			firstRetry = row
		}
	}
	// A new socket is needed because canceling the fixture reader closes the
	// old one, as it does when a real node reconnects.
	restartPoolActionHub(t, f)
	if err := runPoolActionStep(t, f.hub, f.node, firstRetry, poolStatus(fleet.CloseOpRefused, false, false)); err != nil {
		t.Fatal(err)
	}
	restartPoolActionHub(t, f)
	original := readOnlyPoolAction(t, f.hub)
	f.hub.now = func() time.Time { return start.Add(11 * time.Second) }
	runUnknownRetry(t, f, original, original.Data.Args.Attempt)
	actions, err = f.hub.inbox.store.activePoolActions(t.Context())
	if err != nil || len(actions) != 2 {
		t.Fatalf("after refused successor got %d pending requests, want original and a fresh retry: %v", len(actions), err)
	}
	for _, row := range actions {
		if row.ID != original.ID && row.ID == firstRetry.ID {
			t.Fatal("a refused request id was reused")
		}
	}
	if events := spawnFailedInjections(f.destination); len(events) != 0 {
		t.Fatalf("refused close sent a notice: %+v", events)
	}
	if id, err := f.hub.inbox.store.pendingPoolAction(t.Context(), f.job); err != nil || id == "" {
		t.Fatalf("original uncertainty lost its fence: %q %v", id, err)
	}
}

func TestPoolActionUnknownRetryBackoffSurvivesRestart(t *testing.T) {
	f, action := poolActionFixture(t)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f.hub.now = func() time.Time { return start }
	runUnknownRetry(t, f, action, action.Data.Args.Attempt)
	actions, err := f.hub.inbox.store.activePoolActions(t.Context())
	if err != nil || len(actions) != 2 {
		t.Fatalf("first retry: %+v %v", actions, err)
	}
	var retry poolAction
	for _, row := range actions {
		if row.ID != action.ID {
			retry = row
		}
	}
	restartPoolActionHub(t, f)
	if err := runPoolActionStep(t, f.hub, f.node, retry, poolStatus(fleet.CloseOpRefused, false, false)); err != nil {
		t.Fatal(err)
	}
	restartPoolActionHub(t, f)
	original := readOnlyPoolAction(t, f.hub)
	f.hub.now = func() time.Time { return start.Add(5 * time.Second) }
	runUnknownRetry(t, f, original, original.Data.Args.Attempt)
	if actions, err = f.hub.inbox.store.activePoolActions(t.Context()); err != nil || len(actions) != 1 {
		t.Fatalf("early retry left %d active actions: %v", len(actions), err)
	}
	restartPoolActionHub(t, f)
	original = readOnlyPoolAction(t, f.hub)
	f.hub.now = func() time.Time { return start.Add(11 * time.Second) }
	runUnknownRetry(t, f, original, original.Data.Args.Attempt)
	if actions, err = f.hub.inbox.store.activePoolActions(t.Context()); err != nil || len(actions) != 2 {
		t.Fatalf("retry after backoff left %d active actions: %v", len(actions), err)
	}
}

func TestPoolActionUnknownDoesNotRetryBusyPane(t *testing.T) {
	f, action := poolActionFixture(t)
	runUnknownRetryWithStatus(t, f, action, action.Data.Args.Attempt, "working")
	if actions, err := f.hub.inbox.store.activePoolActions(t.Context()); err != nil || len(actions) != 1 {
		t.Fatalf("busy pane left %d pending requests, want only the original: %v", len(actions), err)
	}
}

func TestPoolActionUnknownSuccessorCanRetryWhileOriginalIsPending(t *testing.T) {
	f, action := poolActionFixture(t)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f.hub.now = func() time.Time { return start }
	runUnknownRetry(t, f, action, action.Data.Args.Attempt)
	actions, err := f.hub.inbox.store.activePoolActions(t.Context())
	if err != nil || len(actions) != 2 {
		t.Fatalf("first retry: %+v %v", actions, err)
	}
	var retry poolAction
	for _, row := range actions {
		if row.ID != action.ID {
			retry = row
		}
	}
	args := *action.Data.Args
	args.RequestID = retry.ID
	retry.Data.Args = &args
	if err := f.hub.inbox.store.savePoolAction(t.Context(), retry); err != nil {
		t.Fatal(err)
	}
	restartPoolActionHub(t, f)
	f.hub.now = func() time.Time { return start.Add(11 * time.Second) }
	runUnknownRetry(t, f, retry, retry.Data.Args.Attempt)
	if actions, err = f.hub.inbox.store.activePoolActions(t.Context()); err != nil || len(actions) != 3 {
		t.Fatalf("unknown successor left %d pending requests, want three: %v", len(actions), err)
	}
}

func TestPoolActionUnknownSuccessorHonorsRetryBackoff(t *testing.T) {
	f, original := poolActionFixture(t)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f.hub.now = func() time.Time { return start }
	runUnknownRetry(t, f, original, original.Data.Args.Attempt)
	actions, err := f.hub.inbox.store.activePoolActions(t.Context())
	if err != nil || len(actions) != 2 {
		t.Fatalf("first retry: %+v %v", actions, err)
	}
	var successor poolAction
	for _, row := range actions {
		if row.ID != original.ID {
			successor = row
		}
	}
	args := *original.Data.Args
	args.RequestID = successor.ID
	successor.Data.Args = &args
	if err := f.hub.inbox.store.savePoolAction(t.Context(), successor); err != nil {
		t.Fatal(err)
	}
	restartPoolActionHub(t, f)
	f.hub.now = func() time.Time { return start.Add(5 * time.Second) }
	runUnknownRetry(t, f, successor, successor.Data.Args.Attempt)
	if actions, err = f.hub.inbox.store.activePoolActions(t.Context()); err != nil || len(actions) != 2 {
		t.Fatalf("unknown successor retried early: %d actions, %v", len(actions), err)
	}
	restartPoolActionHub(t, f)
	f.hub.now = func() time.Time { return start.Add(11 * time.Second) }
	runUnknownRetry(t, f, successor, successor.Data.Args.Attempt)
	if actions, err = f.hub.inbox.store.activePoolActions(t.Context()); err != nil || len(actions) != 3 {
		t.Fatalf("unknown successor did not retry after backoff: %d actions, %v", len(actions), err)
	}
}

func TestPoolActionUnknownDoesNotRetryANewerAttempt(t *testing.T) {
	f, action := poolActionFixture(t)
	runUnknownRetry(t, f, action, "00009-job.reprompted.json")
	if actions, err := f.hub.inbox.store.activePoolActions(t.Context()); err != nil || len(actions) != 1 {
		t.Fatalf("newer attempt left %d pending requests, want only the original: %v", len(actions), err)
	}
}

func TestPoolActionUnknownDoesNotRetryAfterCapabilityWithdrawal(t *testing.T) {
	f, action := poolActionFixture(t)
	f.hub.mu.Lock()
	delete(f.hub.nodes[action.Node].capabilities, hubCapabilityCloseOps)
	f.hub.mu.Unlock()
	runUnknownRetry(t, f, action, action.Data.Args.Attempt)
	if actions, err := f.hub.inbox.store.activePoolActions(t.Context()); err != nil || len(actions) != 1 {
		t.Fatalf("withdrawn node left %d pending requests, want only the original: %v", len(actions), err)
	}
}

func TestPoolActionRetryDoesNotCloseARePromptedAttempt(t *testing.T) {
	f, original := poolActionFixture(t)
	runUnknownRetry(t, f, original, original.Data.Args.Attempt)
	actions, err := f.hub.inbox.store.activePoolActions(t.Context())
	if err != nil || len(actions) != 2 {
		t.Fatalf("first retry: %+v %v", actions, err)
	}
	var retry poolAction
	for _, row := range actions {
		if row.ID != original.ID {
			retry = row
		}
	}
	restartPoolActionHub(t, f)
	done := make(chan error, 1)
	go func() { done <- f.hub.reconcilePoolAction(t.Context(), retry) }()
	status := nodeCommandRead(t, f.node)
	nodeCommandReplyTo(t, f.node, status, map[string]any{"result": map[string]any{"state": nodeCloseOpNone, "attempt": "00009-job.reprompted.json"}})
	command := nodeCommandRead(t, f.node)
	var op string
	if err := json.Unmarshal(command["op"], &op); err != nil {
		t.Fatal(err)
	}
	if op == nodeCommandAgentsList {
		nodeCommandReplyTo(t, f.node, command, map[string]any{"result": map[string]any{"agents": []nodeAgentRow{}, "panes": []nodeAgentRow{{PaneID: roleHeldPane, Name: f.job, AgentStatus: "idle", Cwd: "/work/project"}}, "panes_ok": true}})
		command = nodeCommandRead(t, f.node)
		if err := json.Unmarshal(command["op"], &op); err != nil {
			t.Fatal(err)
		}
	}
	observed := op
	if op == nodeCommandPaneCloseOp {
		nodeCommandReplyTo(t, f.node, command, map[string]any{"result": map[string]any{"outcome": nodeCloseOpMismatch}})
		command = nodeCommandRead(t, f.node)
	}
	nodeCommandReplyTo(t, f.node, command, map[string]any{"result": map[string]any{"state": fleet.CloseOpCancelled, "released": true, "attempt": "00009-job.reprompted.json"}})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if observed != nodeCommandPaneCloseRelease {
		t.Fatalf("retry sent %q for a newer attempt; want release", observed)
	}
}

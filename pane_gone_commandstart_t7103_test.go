package panewire

// hk 7103: a pane.close marks a completed job gone only when the job started
// before the close command began. The start is the job's spawn: its bind time
// (the pane is made then), or its created time when it was never bound. A job
// placed on a reused pane id after the close began must keep PaneGone unset, even
// when its completion arrives late.

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// t7103Spawn creates a spawn for job with created time created, and binds it at
// bound when bound is non-zero. It returns the spawn's request id.
func t7103Spawn(t *testing.T, h *HubServer, seq int, job string, created, bound time.Time) {
	t.Helper()
	request := durableSpawnRequest()
	request.RequestID = t14RequestID(71030 + seq)
	request.Args = replaceSpawnArg(replaceSpawnArg(request.Args, "--job", job), "--task", fmt.Sprint(seq))
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, job, "parent", created)
	if err != nil {
		t.Fatal(err)
	}
	if bound.IsZero() {
		return
	}
	if _, err := h.inbox.store.bindSpawn(t.Context(), task.Request.RequestID, task.Attempt, hubSpawnRequest{RequestID: request.RequestID, Machine: "node-a"}, bound); err != nil {
		t.Fatal(err)
	}
}

func TestPaneCloseMarksByJobStartNotCompletionArrival_t7103(t *testing.T) {
	start := time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)
	clock := start.Add(-time.Hour)
	hub, err := NewHubServer(HubServerConfig{
		InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3"),
		Tokens:         map[string]string{"operator": r6OperatorToken, "node-a": r6NodeAToken},
		Now:            func() time.Time { return clock }, GracePeriod: time.Hour, OrphanGrace: time.Hour, KeepaliveInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hub.Close() })
	useLegacyOwnerStore(hub)
	agent := &hubAgent{revocations: make(chan hubJobRevokedEvent, 2)}
	hub.connect("node-a", "t7103", "fixture", agent, false)

	// Each job's spawn start decides the mark, not when its completion arrived.
	t7103Spawn(t, hub, 1, "old-occupant", start.Add(-2*time.Minute), start.Add(-time.Minute))
	t7103Spawn(t, hub, 2, "bound-after-close", start.Add(-2*time.Minute), start.Add(time.Second))
	t7103Spawn(t, hub, 3, "new-occupant", start.Add(time.Second), time.Time{})
	// "no-spawn" has no spawn record: it is marked as the base code did.
	jobs := []HubActiveJob{
		{JobID: "old-occupant", AgentLabel: "agent-a", LastEventSeq: 1, Epoch: 1, Pane: "reused-pane"},
		{JobID: "bound-after-close", AgentLabel: "agent-b", LastEventSeq: 1, Epoch: 1, Pane: "reused-pane"},
		{JobID: "new-occupant", AgentLabel: "agent-c", LastEventSeq: 1, Epoch: 1, Pane: "reused-pane"},
		{JobID: "no-spawn", AgentLabel: "agent-d", LastEventSeq: 1, Epoch: 1, Pane: "reused-pane"},
	}
	hub.observeActiveJobs("node-a", jobs, clock)

	// The old occupant's completion arrives late, after the close began.
	for _, id := range []string{"old-occupant", "bound-after-close", "new-occupant", "no-spawn"} {
		arrival := start.Add(5 * time.Second)
		if !hub.observeJobCompletion("node-a", hubJobEventPayload{JobID: id, Epoch: 1}, arrival) {
			t.Fatalf("completion of %s was rejected", id)
		}
	}

	closed := hubNodeCommandResult{Result: []byte(`{"outcome":"closed"}`)}
	hub.releaseClosedPaneLanes("node-a", nodePaneCloseArgs{PaneID: "reused-pane"}, closed, start)

	want := map[string]bool{"old-occupant": true, "bound-after-close": false, "new-occupant": false, "no-spawn": true}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	for id, marked := range want {
		if got := hub.jobs[id].PaneGone; got != marked {
			t.Errorf("%s: PaneGone=%t, want %t", id, got, marked)
		}
	}
}

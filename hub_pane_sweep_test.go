package panewire

import (
	"slices"
	"testing"
	"time"
)

// TestHubPaneSweepAsksEachNodeToCloseEndedPanes runs one tick against a
// connected node and a node with no connection. The connected node must get
// pane.sweep with the configured age, and its reported close must come back in
// the tick; the unconnected node is counted as failed without a send.
func TestHubPaneSweepAsksEachNodeToCloseEndedPanes(t *testing.T) {
	hub, server := t14Hub(t)
	hub.paneSweepAge = time.Hour
	connection := t14Node(t, hub, server, false)
	hub.mu.Lock()
	hub.nodes["machine-b"] = &hubNodeRecord{machineID: "machine-b", state: "disconnected"}
	hub.mu.Unlock()
	done := make(chan hubPaneSweepTick, 1)
	go func() { done <- hub.sweepEndedPanes(t.Context()) }()
	command := nodeCommandRead(t, connection)
	if string(command["op"]) != `"pane.sweep"` || string(command["args"]) != `{"age_s":3600}` {
		t.Fatalf("node received %v", command)
	}
	nodeCommandReplyTo(t, connection, command, map[string]any{"result": map[string]any{"closed": []map[string]any{{"job": "job-a", "pane": "w1:p1", "role": "reviewer", "reason": "job_revoked"}}}})
	tick := <-done
	if tick.Nodes != 2 || !slices.Equal(tick.Closed, []string{"machine-a:job-a:w1:p1:job_revoked"}) || !slices.Equal(tick.Failed, []string{"machine-b=disconnected"}) {
		t.Fatalf("tick=%+v", tick)
	}
}

// TestHubPaneSweepCountsANodeErrorAsFailed checks that a node's sweep_failed
// reply is recorded against that node and closes nothing.
func TestHubPaneSweepCountsANodeErrorAsFailed(t *testing.T) {
	hub, server := t14Hub(t)
	hub.paneSweepAge = time.Hour
	connection := t14Node(t, hub, server, false)
	done := make(chan hubPaneSweepTick, 1)
	go func() { done <- hub.sweepEndedPanes(t.Context()) }()
	command := nodeCommandRead(t, connection)
	nodeCommandReplyTo(t, connection, command, map[string]any{"error": "sweep_failed"})
	tick := <-done
	if len(tick.Closed) != 0 || !slices.Equal(tick.Failed, []string{"machine-a=sweep_failed"}) {
		t.Fatalf("tick=%+v", tick)
	}
}

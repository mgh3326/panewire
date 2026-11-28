package panewire

// hk 7103: a pane.close marks gone only the job of the agent the close names.
// The node checks that label before it closes the pane, so the label identifies
// the occupant. A different job on a reused pane id keeps PaneGone unset.

import (
	"path/filepath"
	"testing"
	"time"
)

// t7103Hub returns a hub with node-a connected and no jobs.
func t7103Hub(t *testing.T, clock time.Time) *HubServer {
	t.Helper()
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
	hub.connect("node-a", "t7103", "fixture", &hubAgent{revocations: make(chan hubJobRevokedEvent, 2)}, false)
	return hub
}

// t7103Close delivers a pane.close outcome closed for pane, naming agent name.
func t7103Close(hub *HubServer, pane, name string, at time.Time) {
	closed := hubNodeCommandResult{Result: []byte(`{"outcome":"closed"}`)}
	hub.releaseClosedPaneLanes("node-a", nodePaneCloseArgs{PaneID: pane, Name: name}, closed, at)
}

func t7103PaneGone(t *testing.T, hub *HubServer, job string) bool {
	t.Helper()
	hub.mu.Lock()
	defer hub.mu.Unlock()
	return hub.jobs[job].PaneGone
}

// The closed occupant is marked however late its completion arrived: the close
// lands while the job is still running, and it completes afterwards.
func TestPaneCloseMarksClosedOccupantWhoseCompletionArrivesLate_t7103(t *testing.T) {
	start := time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)
	hub := t7103Hub(t, start.Add(-time.Minute))
	hub.observeActiveJobs("node-a", []HubActiveJob{{JobID: "occupant", AgentLabel: "agent-a", LastEventSeq: 1, Epoch: 1, Pane: "reused-pane"}}, start.Add(-time.Minute))
	t7103Close(hub, "reused-pane", "agent-a", start)
	if !hub.observeJobCompletion("node-a", hubJobEventPayload{JobID: "occupant", Epoch: 1}, start.Add(5*time.Second)) {
		t.Fatal("late completion was rejected")
	}
	if !t7103PaneGone(t, hub, "occupant") {
		t.Fatal("the closed occupant was not marked pane gone")
	}
}

// Another agent's job on the reused pane id is not marked by a close that names
// a different agent.
func TestPaneCloseSparesAnotherAgentsJobOnTheReusedPane_t7103(t *testing.T) {
	start := time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)
	hub := t7103Hub(t, start.Add(-time.Minute))
	hub.observeActiveJobs("node-a", []HubActiveJob{{JobID: "other", AgentLabel: "agent-b", LastEventSeq: 1, Epoch: 1, Pane: "reused-pane"}}, start.Add(-time.Minute))
	if !hub.observeJobCompletion("node-a", hubJobEventPayload{JobID: "other", Epoch: 1}, start.Add(time.Second)) {
		t.Fatal("completion was rejected")
	}
	t7103Close(hub, "reused-pane", "agent-a", start)
	if t7103PaneGone(t, hub, "other") {
		t.Fatal("a close naming agent-a marked agent-b's job on the same pane")
	}
}

// A resubmitted job for the same agent is marked by the close that names that agent.
func TestPaneCloseMarksResubmittedJobOfTheNamedAgent_t7103(t *testing.T) {
	start := time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)
	hub := t7103Hub(t, start.Add(-time.Minute))
	hub.observeActiveJobs("node-a", []HubActiveJob{
		{JobID: "first", AgentLabel: "agent-a", LastEventSeq: 1, Epoch: 1, Pane: "reused-pane"},
		{JobID: "resubmit", AgentLabel: "agent-a", LastEventSeq: 1, Epoch: 1, Pane: "reused-pane"},
	}, start.Add(-time.Minute))
	t7103Close(hub, "reused-pane", "agent-a", start)
	for _, job := range []string{"first", "resubmit"} {
		if !t7103PaneGone(t, hub, job) {
			t.Fatalf("%s, a job of the named agent, was not marked pane gone", job)
		}
	}
}

// A close with no name keeps the base behaviour: completed jobs on the pane are
// marked, live ones are not.
func TestPaneCloseWithoutNameMarksCompletedJobsAsBefore_t7103(t *testing.T) {
	start := time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)
	hub := t7103Hub(t, start.Add(-time.Minute))
	hub.observeActiveJobs("node-a", []HubActiveJob{
		{JobID: "done", AgentLabel: "agent-a", LastEventSeq: 1, Epoch: 1, Pane: "reused-pane"},
		{JobID: "live", AgentLabel: "agent-b", LastEventSeq: 1, Epoch: 1, Pane: "reused-pane"},
	}, start.Add(-time.Minute))
	if !hub.observeJobCompletion("node-a", hubJobEventPayload{JobID: "done", Epoch: 1}, start.Add(-time.Second)) {
		t.Fatal("completion was rejected")
	}
	t7103Close(hub, "reused-pane", "", start)
	if !t7103PaneGone(t, hub, "done") {
		t.Fatal("a completed job on the pane was not marked by an unnamed close")
	}
	if t7103PaneGone(t, hub, "live") {
		t.Fatal("a live job was marked by an unnamed close")
	}
}

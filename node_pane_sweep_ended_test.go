package panewire

import (
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

// TestNodePaneSweepArgsStayWithinAYear checks the age bounds a node accepts
// from the hub: zero through one year, inclusive.
func TestNodePaneSweepArgsStayWithinAYear(t *testing.T) {
	cases := []struct {
		name  string
		age   int64
		valid bool
	}{
		{"zero", 0, true},
		{"one year", nodePaneSweepMaxAgeSeconds, true},
		{"negative", -1, false},
		{"over one year", nodePaneSweepMaxAgeSeconds + 1, false},
	}
	for _, c := range cases {
		if got := validNodePaneSweepArgs(nodePaneSweepArgs{AgeS: c.age}); got != c.valid {
			t.Fatalf("%s: valid=%v want %v", c.name, got, c.valid)
		}
	}
}

// TestNodePaneSweepResultNamesEachClose checks that a result without a closed
// list is rejected, and that each close must carry its job, pane, role and reason.
func TestNodePaneSweepResultNamesEachClose(t *testing.T) {
	good := fleet.SweptPane{Job: "job-a", Pane: "w1:p1", Role: "reviewer", Reason: "job_revoked"}
	cases := []struct {
		name   string
		result nodePaneSweepResult
		valid  bool
	}{
		{"no closed list", nodePaneSweepResult{}, false},
		{"empty closed list", nodePaneSweepResult{Closed: []fleet.SweptPane{}}, true},
		{"named close", nodePaneSweepResult{Closed: []fleet.SweptPane{good}}, true},
		{"missing reason", nodePaneSweepResult{Closed: []fleet.SweptPane{{Job: "job-a", Pane: "w1:p1", Role: "reviewer"}}}, false},
		{"missing role", nodePaneSweepResult{Closed: []fleet.SweptPane{{Job: "job-a", Pane: "w1:p1", Reason: "job_revoked"}}}, false},
	}
	for _, c := range cases {
		if got := validNodePaneSweepResult(c.result); got != c.valid {
			t.Fatalf("%s: valid=%v want %v", c.name, got, c.valid)
		}
	}
}

// TestNodePaneSweepNeedsTheJobsRoot checks that a node with no jobs inbox
// refuses pane.sweep instead of closing panes it cannot match to jobs.
func TestNodePaneSweepNeedsTheJobsRoot(t *testing.T) {
	client := &HubClient{}
	_, code := nodeRun(t, client, nodeCommandPaneSweep, nodePaneSweepArgs{AgeS: 3600})
	if code != "jobs_root_unset" {
		t.Fatalf("code=%q", code)
	}
}

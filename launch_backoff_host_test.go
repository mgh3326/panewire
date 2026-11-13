package panewire

import (
	"testing"
	"time"
)

// Launch backoff benches a (host, kind) pair only for the host's own launch
// faults. A landing or queue timeout, a late claim, the hub's unknown or
// reconciling outcomes and checkout state are not the host failing to launch.
// The reason table is in docs/placement.md, "Launch-failure backoff".

func TestLaunchBackoffSkipsHubAndTransportFaults(t *testing.T) {
	hub, server := fsHub(t, "")
	fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	until := func() map[string]time.Time { return readinessState(t, hub, "node-a").BackoffUntil }

	cases := []struct {
		rc     int
		reason string
	}{
		{124, "timeout"},              // landing timeout while the node's read loop was blocked
		{124, "deadline_not_started"}, // node claimed the request after its deadline
		{75, "unknown"},               // hub could not tell whether the launch happened
		{75, "reconciling"},
		{75, "journal_conflict"},
		{2, "invalid_args"},
		{2, "invalid_request"},
		{81, "checkout_refused:busy"},
		{81, "checkout_refused:origin-mismatch"},
		{1, ""}, // unclassified: no node reason, so nothing shows a launch fault
	}
	for _, tc := range cases {
		// Two in a row would bench the pair if the reason counted.
		hub.observeLaunchOutcome("node-a", "opus", tc.rc, tc.reason, now)
		hub.observeLaunchOutcome("node-a", "opus", tc.rc, tc.reason, now.Add(time.Second))
		if got := until(); len(got) != 0 {
			t.Fatalf("rc=%d reason=%q benched the host: %v", tc.rc, tc.reason, got)
		}
	}
}

func TestLaunchBackoffCountsHostLaunchFailures(t *testing.T) {
	hub, server := fsHub(t, "")
	fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	until := func() map[string]time.Time { return readinessState(t, hub, "node-a").BackoffUntil }

	cases := []struct {
		rc     int
		reason string
	}{
		{1, "spawn_failed"},
		{1, "spawn_failed:trust_prompt: workspace not trusted"},
		{2, "spawn_disabled"},
		{2, "cwd_unmapped"},
		{2, "brief_doc_unavailable"},
	}
	for _, tc := range cases {
		// A success clears the previous case's backoff, so each case starts at zero.
		hub.observeLaunchOutcome("node-a", "opus", 0, "", now)
		hub.observeLaunchOutcome("node-a", "opus", tc.rc, tc.reason, now)
		if got := until(); len(got) != 0 {
			t.Fatalf("one rc=%d reason=%q started a backoff: %v", tc.rc, tc.reason, got)
		}
		hub.observeLaunchOutcome("node-a", "opus", tc.rc, tc.reason, now.Add(time.Second))
		if got := until(); len(got) != 1 {
			t.Fatalf("two rc=%d reason=%q did not bench the host: %v", tc.rc, tc.reason, got)
		}
	}
}

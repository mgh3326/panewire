package panewire

import (
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// A launch that stops at the login screen (AUTH_UNAVAILABLE, rc 88) raises the
// one-time login_missing need for its (host, kind). While that need stands the
// hub publishes the kind in the host's readiness value, which automatic
// placement drops (hk 5686). Clean reports lift it only after a full cycle, the
// same anti-flap clear as the operator notice.

func TestLoginNeedPublishedWhileStandingAndLiftedAfterClear(t *testing.T) {
	hub, server := fsHub(t, "")
	fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	oneTimeSentry(t, hub)
	loginNeeds := func() []string { return readinessState(t, hub, "node-a").LoginNeeds }

	if got := loginNeeds(); len(got) != 0 {
		t.Fatalf("a host with no need publishes %v", got)
	}
	refused := hub.now().UTC()
	hub.noteOneTimeActionRefusal("node-a", "agy-flash38-medium", hubSpawnResult{RC: fleet.ExitAuthUnavailable, StdoutTail: "AUTH_UNAVAILABLE: selected quota account login missing"})
	settleOneTimeNotices(t, hub)
	if got := loginNeeds(); len(got) != 1 || got[0] != "agy" {
		t.Fatalf("standing need: LoginNeeds=%v want [agy]", got)
	}

	// The node's own probe reads the login present; that alone does not lift the
	// need, and one clean report is not a full cycle.
	clean := func(at time.Time) {
		t.Helper()
		hub.noteReadinessActions("node-a", readinessWith(at, "agy", fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginPresent}), at)
		settleOneTimeNotices(t, hub)
	}
	clean(refused.Add(time.Minute))
	if got := loginNeeds(); len(got) != 1 {
		t.Fatalf("one clean report lifted the need: LoginNeeds=%v", got)
	}
	clean(refused.Add(time.Minute + oneTimeActionClearCycle))
	if got := loginNeeds(); len(got) != 0 {
		t.Fatalf("a confirmed clear still publishes LoginNeeds=%v", got)
	}
}

package panewire

import (
	"errors"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"

	"github.com/mgh3326/panewire/internal/fleet"
)

// A launch that stops at the login screen (AUTH_UNAVAILABLE, rc 88) raises the
// one-time login_missing need for its (host, kind). While that need stands the
// hub publishes the kind in the host's readiness value, which automatic
// placement drops (hk 5686). The need stands from its first sighting, whether
// or not the operator notice was delivered. Clean reports lift it only after a
// full cycle, the same anti-flap clear as the notice.

// loginNeedsOf waits for the host's first readiness value, then reads its
// login_needs. fsConnectNode waits for the publisher row only, and the
// heartbeat publishes that row before it publishes readiness.
func loginNeedsOf(t *testing.T, hub *HubServer, machine string) []string {
	t.Helper()
	eventually(t, 2*time.Second, "readiness value of "+machine, func() bool {
		_, ok := fsLatest(hub, machine, fleetStateKindReadiness, "")
		return ok
	})
	return readinessState(t, hub, machine).LoginNeeds
}

func TestLoginNeedPublishedWhileStandingAndLiftedAfterClear(t *testing.T) {
	hub, server := fsHub(t, "")
	fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	oneTimeSentry(t, hub)

	if got := loginNeedsOf(t, hub, "node-a"); len(got) != 0 {
		t.Fatalf("a host with no need publishes %v", got)
	}
	refused := hub.now().UTC()
	hub.noteOneTimeActionRefusal("node-a", "agy-flash38-medium", hubSpawnResult{RC: fleet.ExitAuthUnavailable, StdoutTail: "AUTH_UNAVAILABLE: selected quota account login missing"})
	settleOneTimeNotices(t, hub)
	if got := loginNeedsOf(t, hub, "node-a"); len(got) != 1 || got[0] != "agy" {
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
	if got := loginNeedsOf(t, hub, "node-a"); len(got) != 1 {
		t.Fatalf("one clean report lifted the need: LoginNeeds=%v", got)
	}
	clean(refused.Add(time.Minute + oneTimeActionClearCycle))
	if got := loginNeedsOf(t, hub, "node-a"); len(got) != 0 {
		t.Fatalf("a confirmed clear still publishes LoginNeeds=%v", got)
	}
}

// With the operator notice undelivered (Sentry refuses it), the need still
// stands and placement still skips the host. The next sighting retries the
// notice, and the need stays standing once it is delivered.
func TestLoginNeedStandsWhileNoticeUndelivered(t *testing.T) {
	hub, server := fsHub(t, "")
	fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	hub.oneTimeNotify = func(*sentry.Event) (bool, error) { return false, errors.New("sentry unavailable") }
	refusal := hubSpawnResult{RC: fleet.ExitAuthUnavailable, StdoutTail: "AUTH_UNAVAILABLE: selected quota account login missing"}

	hub.noteOneTimeActionRefusal("node-a", "agy-flash38-medium", refusal)
	settleOneTimeNotices(t, hub)
	if got := loginNeedsOf(t, hub, "node-a"); len(got) != 1 || got[0] != "agy" {
		t.Fatalf("undelivered notice: LoginNeeds=%v want [agy]", got)
	}

	transport := oneTimeSentry(t, hub)
	hub.noteOneTimeActionRefusal("node-a", "agy-flash38-medium", refusal)
	settleOneTimeNotices(t, hub)
	if got := oneTimeMessages(t, hub, transport); len(got) != 1 {
		t.Fatalf("the retried notice was not delivered once: %q", got)
	}
	if got := loginNeedsOf(t, hub, "node-a"); len(got) != 1 || got[0] != "agy" {
		t.Fatalf("delivered notice: LoginNeeds=%v want [agy]", got)
	}
}

// A readiness publish does not wait for the one-time lock, which the notice and
// wait workers hold across store writes (hk 5686 review).
func TestLoginNeedPublishDoesNotWaitForStorage(t *testing.T) {
	hub, server := fsHub(t, "")
	fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	hub.oneTimeMu.Lock()
	done := make(chan struct{})
	go func() {
		hub.publishFleetReadiness("node-a", hub.now())
		close(done)
	}()
	select {
	case <-done:
		hub.oneTimeMu.Unlock()
	case <-time.After(2 * time.Second):
		hub.oneTimeMu.Unlock()
		<-done
		t.Fatal("a readiness publish waited for the one-time lock held by a store write")
	}
}

// An rc 88 that is not the verified login-screen refusal (an account the host
// cannot confirm) raises neither a login need nor an operator notice.
func TestAccountRefusalRaisesNoLoginNeed(t *testing.T) {
	hub, server := fsHub(t, "")
	fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	transport := oneTimeSentry(t, hub)
	hub.noteOneTimeActionRefusal("node-a", "agy-flash38-medium", hubSpawnResult{RC: fleet.ExitAuthUnavailable, StdoutTail: "AUTH_UNAVAILABLE: executing host node-a cannot confirm the selected account x: no_usable_presence"})
	settleOneTimeNotices(t, hub)
	if got := loginNeedsOf(t, hub, "node-a"); len(got) != 0 {
		t.Fatalf("an account refusal raised a login need: LoginNeeds=%v", got)
	}
	if got := oneTimeMessages(t, hub, transport); len(got) != 0 {
		t.Fatalf("an account refusal sent a notice: %q", got)
	}
}

// A need on a kind the node does not report on (kiro) cannot clear by report,
// so it is not published as a placement skip.
func TestLoginNeedOfUncoveredKindIsNotPublished(t *testing.T) {
	hub, server := fsHub(t, "")
	fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	hub.oneTimeMu.Lock()
	hub.oneTimeActions = map[oneTimeActionKey]oneTimeActionState{{"node-a", "kiro", oneTimeLoginMissing}: {ToldAt: hub.now().UTC()}}
	hub.refreshLoginNeedsLocked()
	hub.oneTimeMu.Unlock()
	hub.publishFleetReadiness("node-a", hub.now())
	if got := loginNeedsOf(t, hub, "node-a"); len(got) != 0 {
		t.Fatalf("an uncovered kind was published as a skip: LoginNeeds=%v", got)
	}
}

// A readiness report computed before the refusal, but received after it, cannot
// start or confirm the clear of an untold login need (hk 5686 review). Two such
// clean reports one cycle apart would clear it if the refusal did not bound them.
func TestLoginNeedNotClearedByReportBeforeRefusal(t *testing.T) {
	hub, server := fsHub(t, "")
	fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	hub.oneTimeNotify = func(*sentry.Event) (bool, error) { return false, errors.New("sentry unavailable") }
	refused := hub.now().UTC()
	hub.noteOneTimeActionRefusal("node-a", "agy-flash38-medium", hubSpawnResult{RC: fleet.ExitAuthUnavailable, StdoutTail: loginScreenAuthMessage + "\nFix: sign in"})
	settleOneTimeNotices(t, hub)
	clean := func(checked, received time.Time) {
		t.Helper()
		hub.noteReadinessActions("node-a", readinessWith(checked, "agy", fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginPresent}), received)
		settleOneTimeNotices(t, hub)
	}
	clean(refused.Add(-time.Minute), refused.Add(time.Second))
	clean(refused.Add(-time.Minute+oneTimeActionClearCycle), refused.Add(time.Second+oneTimeActionClearCycle))
	if got := loginNeedsOf(t, hub, "node-a"); len(got) != 1 || got[0] != "agy" {
		t.Fatalf("stale reports cleared the need: LoginNeeds=%v want [agy]", got)
	}
}

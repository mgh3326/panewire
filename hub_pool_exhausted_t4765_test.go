package panewire

import (
	"strings"
	"testing"
	"time"
)

// beyondWaitableReset is a free-limit reset past the pane's wait horizon
// (poolFreeLimitWaitableWithin, policy/quota-stop max_wait): the pane hands
// over instead of waiting, so the hub still notifies. The hand-over tests
// use it to pin the notifying side of that line.
func beyondWaitableReset() time.Time {
	return time.Now().Add(2 * time.Hour)
}

// A free-limit reset inside the pane's wait horizon sends no hand-over line:
// the pane waits it out and retries (hk 4765). The 13-minute reset is the
// incident's shape (free again shortly, owner must not be told to hand over).
func TestPoolExhaustedWaitableFreeLimitSendsNoHandover(t *testing.T) {
	f := newSweepFixture(t)
	job := f.addJob(t, "implementer", "devin", sweepDevin, "idle")
	task, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), job)
	if err != nil {
		t.Fatal(err)
	}
	e := poolExhaustion{Pool: "devin", Account: sweepDevin, Cause: poolCauseFreeLimit, Reset: time.Now().Add(13 * time.Minute)}
	if result := f.hub.sweepPoolJob(t.Context(), e, task); result.action != poolJobSkipped || result.why != "waitable_reset" {
		t.Fatalf("result=%+v, want a waitable_reset skip", result)
	}
	if events := spawnFailedInjections(f.destination); len(events) != 0 {
		t.Fatalf("a waitable free limit notified: %+v", events)
	}
	if got := f.state(job); got != "spawned" {
		t.Fatalf("state=%s, want spawned", got)
	}
}

// A free-limit stop cools the free band until its reset, so the next
// selection skips the free rows while the paid rungs stay placeable
// (hk 4765). Selection honoring the cooling is pinned on the fleet side by
// TestSelectFreeBandCoolingLeavesPaidRungsEligible_T4582.
func TestPoolExhaustedFreeLimitCoolsFreeBandUntilReset(t *testing.T) {
	f := newSweepFixture(t)
	now := time.Now().UTC().Truncate(time.Second)
	reset := now.Add(13 * time.Minute)
	f.hub.sweepExhaustedPool(t.Context(), poolExhaustion{Pool: "devin", Account: sweepDevin, Cause: poolCauseFreeLimit, Reset: reset})
	snap := f.hub.fleetSnapshot(now)
	if got := snap.Cooling["devin-free"]; !got.Equal(reset) {
		t.Fatalf("free band cooling = %v, want %v", got, reset)
	}
	if got, ok := snap.Cooling["devin-paid"]; ok {
		t.Fatalf("a free-limit stop cooled the paid band: %v", got)
	}
	if events := spawnFailedInjections(f.destination); len(events) != 0 {
		t.Fatalf("the cooling sweep notified: %+v", events)
	}
	if got := f.hub.fleetSnapshot(reset.Add(time.Minute)).Cooling; len(got) != 0 {
		t.Fatalf("cooling remains past the reset: %v", got)
	}
}

// The sweep log still names what a waitable pass skipped, so a quiet pass is
// visible without an owner notice.
func TestPoolExhaustedWaitableFreeLimitLoggedAsSkipped(t *testing.T) {
	f := newSweepFixture(t)
	f.addJob(t, "implementer", "devin", sweepDevin, "idle")
	reset := time.Now().Add(13 * time.Minute)
	f.publishFreeLimit(t, sweepDevin, reset, time.Now())
	eventually(t, 5*time.Second, "the waitable sweep", func() bool { return f.sweeps() == 1 })
	for _, line := range f.logs.lines("pool exhausted sweep") {
		if !strings.Contains(line, "waitable_reset=1") {
			t.Fatalf("sweep line hides the skip: %q", line)
		}
	}
	if events := spawnFailedInjections(f.destination); len(events) != 0 {
		t.Fatalf("a waitable free limit notified: %+v", events)
	}
}

package fleet

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// F4 (hk 5063): pane sweep must not block on a job's end lock while holding
// Exclusive (nodeCommandMu). It takes the end lock non-blockingly; if busy,
// the pane is kept with reason=hub_close_owns_end and retried on the next pass.
func TestPaneSweepDoesNotHoldExclusiveDuringEndLockWait_t5063(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("r", "reviewer", "")
	putEvent(t, f.a, "r", "job.completed", map[string]any{})
	f.now = f.now.Add(61 * time.Minute)

	// Pre-take the job's end lock.
	unlock, err := LockJobEnd(filepath.Join(f.a.Jobs, "r"))
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()

	var exclusiveTakes, exclusiveReleases int
	f.sweeper.Exclusive = func() func() {
		exclusiveTakes++
		return func() {
			exclusiveReleases++
		}
	}

	start := time.Now()
	closed, err := f.sweeper.SweepEnded(context.Background(), f.a, time.Hour)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("sweep error: %v", err)
	}
	if len(closed) != 0 {
		t.Fatalf("closed %v, want 0 panes closed while end lock is held", closed)
	}
	// Must return immediately (non-blocking take), well under any lock timeout (e.g. 5s).
	if elapsed > 2*time.Second {
		t.Fatalf("sweep took %v, want fast non-blocking return (< 2s)", elapsed)
	}
	// Must assert kept with reason=hub_close_owns_end
	if !f.infoHas("task pane kept", "job=r", "reason=hub_close_owns_end") {
		t.Fatal("expected pane kept with reason=hub_close_owns_end")
	}
	// Exclusive must have been taken and released in balance.
	if exclusiveTakes == 0 || exclusiveTakes != exclusiveReleases {
		t.Fatalf("Exclusive unbalanced: takes=%d releases=%d", exclusiveTakes, exclusiveReleases)
	}

	// Now unlock the end lock and run the sweep again; the pane must close.
	unlock()
	closed, err = f.sweeper.SweepEnded(context.Background(), f.a, time.Hour)
	if err != nil {
		t.Fatalf("second sweep error: %v", err)
	}
	if len(closed) != 1 {
		t.Fatalf("second sweep closed %v, want 1 pane closed", closed)
	}
	if exclusiveTakes != exclusiveReleases {
		t.Fatalf("Exclusive unbalanced after second sweep: takes=%d releases=%d", exclusiveTakes, exclusiveReleases)
	}
}

// F6 / M1b (hk 5063): pane sweep must defer when a job's ledger holds an open
// hub-close intent, keeping the pane open and the job unreaped.
func TestEndedSweepDefersToOpenHubClose_t5063(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "")
	putEvent(t, f.a, "r", "job.completed", map[string]any{})
	f.now = f.now.Add(61 * time.Minute)

	j, err := f.a.job("r")
	if err != nil {
		t.Fatal(err)
	}
	var attempt string
	for _, ev := range j.Events {
		if ev.Kind == "job.spawned" {
			attempt = ev.name
		}
	}
	in := HubCloseIntent{
		RequestID: "req-1", Attempt: attempt, Pane: pane, Label: "agent-r", EndReason: "quota_stop",
	}
	if err := f.a.recordHubCloseIntent(context.Background(), "r", in); err != nil {
		t.Fatal(err)
	}

	closed, err := f.sweeper.SweepEnded(context.Background(), f.a, time.Hour)
	if err != nil {
		t.Fatalf("sweep error: %v", err)
	}
	if len(closed) != 0 || len(f.closed()) != 0 || f.reaped("r") != nil {
		t.Fatalf("open hub close was swept or reaped: closed=%v herdr=%v reaped=%v", closed, f.closed(), f.reaped("r"))
	}
	if !f.infoHas("job=r", "reason=hub_close_owns_end") {
		t.Fatal("expected sweep to log reason=hub_close_owns_end")
	}
}

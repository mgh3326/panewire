package fleet

import (
	"strings"
	"testing"
	"time"
)

// hk 4919: the stale alarm counts only panes the sweep's close would take. The
// count and the close share one ownership predicate (paneOwnershipProof), so a
// row the close refuses to own is a keep, never a pane this host failed to
// collect. A named idle row carrying another live session is the clearest case:
// the sweep keeps it as session_mismatch on every pass, and counting it would
// latch the hub's ended_panes alarm on a pane the sweep never closes.
func TestEndedSweepDoesNotCountAPaneAnotherSessionHolds(t *testing.T) {
	f := newPaneSweepFixture(t)
	var reported []int
	f.sweeper.EndedOpen = func(open int) { reported = append(reported, open) }
	pane := f.job("r", "reviewer", "")
	f.status[pane] = "done"
	f.sessions[pane] = `{"value":"sess-someone-else"}`
	putEvent(t, f.a, "r", "job.completed", map[string]any{})
	if closes := f.endedSweep(1441, time.Hour); len(closes) != 0 || len(f.closed()) != 0 {
		t.Fatalf("a pane another session holds was closed: %+v closed=%v", closes, f.closed())
	}
	if !f.infoHas("task pane kept", "job=r", "reason=session_mismatch") {
		t.Fatalf("info lines = %q", f.info)
	}
	if len(reported) != 1 || reported[0] != 0 {
		t.Fatalf("a pane the close refuses to own was counted stale: %v info=%v", reported, f.info)
	}
	// The per-reason textfile still names the refusal: it is the diagnostic
	// gauge, not the alarm count.
	if metric := readPaneSweepMetric(t, f.a); !strings.Contains(metric, `panewire_ended_job_panes_open{reason="session_mismatch"} 1`) {
		t.Fatalf("metric = %q", metric)
	}
}

// The close proves the pane twice: once on the pass's snapshot and once on a
// fresh listing under the acting lock, after the screen was read. A session
// that changes in between is an observed reassignment — another agent now owns
// the pane — so the pass keeps it and the old snapshot must not count it. The
// count's proof is the close's, including what that second check saw.
func TestEndedSweepDoesNotCountAPaneReassignedWhileItsScreenIsRead(t *testing.T) {
	f := newPaneSweepFixture(t)
	var reported []int
	f.sweeper.EndedOpen = func(open int) { reported = append(reported, open) }
	pane := f.job("r", "reviewer", "")
	f.status[pane] = "done"
	putEvent(t, f.a, "r", "job.completed", map[string]any{})
	f.onPaneRead(func() { f.sessions[pane] = `{"value":"sess-new"}` })
	if closes := f.endedSweep(1441, time.Hour); len(closes) != 0 || len(f.closed()) != 0 {
		t.Fatalf("a pane reassigned while its screen was read was closed: %+v closed=%v", closes, f.closed())
	}
	if !f.infoHas("task pane kept", "job=r", "reason=session_mismatch") {
		t.Fatalf("info lines = %q", f.info)
	}
	if len(reported) != 1 || reported[0] != 0 {
		t.Fatalf("a pane reassigned before the close was counted stale: %v info=%v", reported, f.info)
	}
	// The pane is kept for diagnosis, so the gauge still names it.
	if metric := readPaneSweepMetric(t, f.a); !strings.Contains(metric, `panewire_ended_job_panes_open{reason="session_mismatch"} 1`) {
		t.Fatalf("metric = %q", metric)
	}
}

// The count's identity test is the close's proof, not paneSweepMatches: past
// the deadline the close takes a pane on its recorded session alone, whatever
// the row's name or tab says. A close that fails then leaves a pane the sweep
// failed to collect, and the alarm must count it even though the grace's
// name/tab match would not (review finding on #657).
func TestEndedSweepCountsAProofOwnedPaneWhoseCloseFails(t *testing.T) {
	f := newPaneSweepFixture(t)
	var reported []int
	f.sweeper.EndedOpen = func(open int) { reported = append(reported, open) }
	pane := f.job("r", "reviewer", "", map[string]any{"tab_id": "t-moved"})
	f.status[pane] = "done"
	putEvent(t, f.a, "r", "job.completed", map[string]any{})
	base := f.runner.Do
	f.runner.Do = func(c Command) Result {
		if c.Name == "herdr" && len(c.Args) == 3 && c.Args[1] == "close" {
			return Result{Code: 1, Err: "pane busy"}
		}
		return base(c)
	}
	if closes := f.endedSweep(1501, time.Hour); len(closes) != 0 || f.reaped("r") != nil {
		t.Fatalf("a close herdr refused reported a swept pane: %+v reaped=%v", closes, f.reaped("r"))
	}
	if !f.infoHas("task pane kept", "job=r", "reason=close_failed") {
		t.Fatalf("info lines = %q", f.info)
	}
	if len(reported) != 1 || reported[0] != 1 {
		t.Fatalf("a proof-owned pane whose close failed was not counted stale: %v info=%v", reported, f.info)
	}
}

// The count also requires the pass's close deadline to have passed: the hub may
// order --pane-sweep-age longer than the alarm's day, and a pane inside that
// window is one the sweep is still correctly waiting on, not a failed
// collection. A drifted idle row reaches the pass's not-idle branch, which does
// not exclude it, so only the deadline keeps it out of the count (review finding
// on #658).
func TestEndedSweepDoesNotCountAPaneBeforeItsCloseDeadline(t *testing.T) {
	f := newPaneSweepFixture(t)
	var reported []int
	f.sweeper.EndedOpen = func(open int) { reported = append(reported, open) }
	pane := f.job("r", "reviewer", "", map[string]any{"tab_id": "t-moved"})
	f.status[pane] = "done"
	putEvent(t, f.a, "r", "job.completed", map[string]any{})
	// 25 h past the end, with a two-day sweep age: not due, so kept.
	f.endedSweep(1501, 48*time.Hour)
	if len(f.closed()) != 0 || f.reaped("r") != nil {
		t.Fatalf("a pane inside the sweep age was closed: closed=%v reaped=%v", f.closed(), f.reaped("r"))
	}
	if !f.infoHas("task pane kept", "job=r", "reason=pane-mismatch") {
		t.Fatalf("info lines = %q", f.info)
	}
	if len(reported) != 1 || reported[0] != 0 {
		t.Fatalf("a pane the sweep is still waiting on was counted stale: %v info=%v", reported, f.info)
	}

	// Past the deadline the same pane is due; a close that fails is a failed
	// collection and counts.
	base := f.runner.Do
	f.runner.Do = func(c Command) Result {
		if c.Name == "herdr" && len(c.Args) == 3 && c.Args[1] == "close" {
			return Result{Code: 1, Err: "pane busy"}
		}
		return base(c)
	}
	f.endedSweep(2881, 48*time.Hour)
	if !f.infoHas("task pane kept", "job=r", "reason=close_failed") {
		t.Fatalf("info lines = %q", f.info)
	}
	if len(reported) != 2 || reported[1] != 1 {
		t.Fatalf("a due pane whose close failed was not counted: %v info=%v", reported, f.info)
	}
}

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

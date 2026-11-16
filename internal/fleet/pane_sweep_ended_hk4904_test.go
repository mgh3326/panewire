package fleet

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// hk 4904: a spawner that could not read the pane's harness session recorded
// the placeholder "unknown" with source "unavailable" (harness_session.go).
// Comparing that placeholder with the row herdr lists matched nothing, so a
// finished reviewer's pane stayed session_mismatch on every pass and the sweep
// never closed it. A receipt that proves no session now proves the pane the way
// panewire reap does: the row carries the job's name and tab and is not
// working. A receipt that recorded a real session keeps the stricter proof.
func TestEndedSweepClosesAReviewerWhoseRecordedSessionIsThePlaceholder(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "", map[string]any{
		"harness_session": map[string]any{"kind": "codex", "id": "unknown", "source": "unavailable"},
	})
	f.status[pane] = "done"
	putEvent(t, f.a, "r", "job.completed", map[string]any{})
	closes := f.endedSweep(61, time.Hour)
	if len(closes) != 1 || closes[0].Reason != "job_completed" || closes[0].Role != "reviewer" {
		t.Fatalf("a finished reviewer's pane with no recorded session stayed open: %+v info=%v", closes, f.info)
	}
	// The row still carried a live session: the fallback proof, not a missing
	// session, is what closed the pane.
	if got := f.sessions[pane]; got != `{"value":"sess-r"}` {
		t.Fatalf("pane row session = %q", got)
	}
	if len(f.closed()) != 1 || f.reaped("r") == nil {
		t.Fatalf("closed=%v reaped=%v", f.closed(), f.reaped("r"))
	}
}

// The fallback proof needs a pane that is not working: a row named for the job
// whose status is working is a pane still in use, and the receipt proves no
// session that would let the deadline override that.
func TestEndedSweepKeepsAWorkingPaneWithNoRecordedSession(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "", map[string]any{
		"harness_session": map[string]any{"kind": "cmd", "id": "unknown", "source": "unavailable"},
	})
	f.status[pane] = "working"
	putEvent(t, f.a, "r", "job.completed", map[string]any{})
	if closes := f.endedSweep(61, time.Hour); len(closes) != 0 || len(f.closed()) != 0 {
		t.Fatalf("a working pane with no recorded session was closed: %+v closed=%v", closes, f.closed())
	}
	if !f.infoHas("task pane kept", "job=r", "reason=session_unrecorded") {
		t.Fatalf("info lines = %q", f.info)
	}
}

// hk's secret guard refuses a reviewer's screen for good: the same bytes fail
// on every pass. The whole screen is kept in the job directory and the pane
// closes, instead of holding the finished pane on the host for days.
func TestEndedSweepClosesAPaneWhoseScreenHkRefuses(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "")
	putEvent(t, f.a, "r", "job.completed", map[string]any{})
	f.hk.Handle(func(request hkRequest) (int, string, bool) {
		if request.Method == http.MethodPut && strings.HasPrefix(request.Path, "/v1/documents/") {
			return http.StatusBadRequest, `{"error":"secret_like_content","pattern":"private_key"}`, true
		}
		return 0, "", false
	})
	closes := f.endedSweep(61, time.Hour)
	if len(closes) != 1 || closes[0].Reason != "job_completed" {
		t.Fatalf("a screen hk refuses for good kept the pane: %+v info=%v", closes, f.info)
	}
	if len(f.closed()) != 1 || f.reaped("r") == nil {
		t.Fatalf("closed=%v reaped=%v", f.closed(), f.reaped("r"))
	}
	screen := filepath.Join(f.a.Jobs, "r", PaneScreenFile)
	body, err := os.ReadFile(screen)
	if err != nil {
		t.Fatalf("the screen hk refused was not kept: %v", err)
	}
	if !strings.Contains(string(body), "last screen of "+pane) {
		t.Fatalf("local screen = %q", body)
	}
	if info, err := os.Stat(screen); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("local screen mode = %v err=%v, want 0600", info.Mode().Perm(), err)
	}
}

// A pass whose job ended more than a day ago and whose pane herdr still lists
// not working reports the count the node sends the hub (hk 4904): the host is
// collecting finished panes. A pane open inside its day is not stale.
func TestEndedSweepReportsItsStaleOpenPanes(t *testing.T) {
	f := newPaneSweepFixture(t)
	var reported []int
	f.sweeper.EndedOpen = func(open int) { reported = append(reported, open) }
	pane := f.job("r", "reviewer", "")
	f.status[pane] = "done"
	putEvent(t, f.a, "r", "job.completed", map[string]any{})
	f.endedSweep(61, time.Hour)
	if len(reported) != 1 || reported[0] != 0 {
		t.Fatalf("a pane open inside its day was reported stale: %v", reported)
	}

	// hk away keeps the pane (its screen cannot be saved), so a day after the
	// job's end the sweep is holding a finished pane.
	g := newPaneSweepFixture(t)
	var stale []int
	g.sweeper.EndedOpen = func(open int) { stale = append(stale, open) }
	kept := g.job("k", "reviewer", "")
	g.status[kept] = "done"
	putEvent(t, g.a, "k", "job.completed", map[string]any{})
	g.hk.Close()
	g.endedSweep(1441, time.Hour)
	if len(stale) != 1 || stale[0] != 1 {
		t.Fatalf("a pane open a day after its end was not reported stale: %v info=%v", stale, g.info)
	}
}

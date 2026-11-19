package fleet

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// reviewerHoldRunner drives a reviewer's pane from the fake clock: status and
// screen come from functions of the elapsed time, so a test can resume the pane
// (working) or leave it on the stop. Any other herdr call fails the test, as in
// quotaStopFixture.
func reviewerHoldRunner(t *testing.T, a *App, f *fakeRunner, status func(time.Duration) string, screen func(time.Duration) string) {
	t.Helper()
	start := a.Now()
	// As herdr does, state_change_seq moves on each status change, so a pane that
	// went back to idle is read again rather than skipped as already read.
	seq, last := int64(7), ""
	f.Do = func(c Command) Result {
		if c.Name != "herdr" {
			return Result{}
		}
		elapsed := a.Now().Sub(start)
		switch {
		case strings.Join(c.Args, " ") == "agent list":
			s := status(elapsed)
			if last != "" && s != last {
				seq++
			}
			last = s
			return Result{Out: fmt.Sprintf(`{"result":{"agents":[{"pane_id":"w:p1","agent_status":%q,"state_change_seq":%d}]}}`, s, seq)}
		case len(c.Args) > 2 && c.Args[0] == "agent" && c.Args[1] == "read":
			return Result{Out: screen(elapsed)}
		}
		t.Errorf("unexpected herdr call %v", c.Args)
		return Result{}
	}
}

// shortResetScreen is the devin stop with a reset two minutes away (hk 5355).
func shortResetScreen(t *testing.T) string {
	t.Helper()
	return "▲ Something went wrong — Reached free model rate limit … reset in 2 minutes (at 00:02 UTC). Send a message to retry\n❭ Ask Devin to build features, fix bugs, or work on your code\n"
}

// A reviewer stopped with a two-minute reset is held, not ended: the manager
// resumes the pane at the reset (here the pane goes back to work), and the job
// is not lost. The hold is recorded once.
func TestWatcherReviewerShortResetIsHeldThenResumed(t *testing.T) {
	a, f, h, _ := quotaStopFixture(t, "reviewer", "idle", "")
	screen := shortResetScreen(t)
	reviewerHoldRunner(t, a, f,
		func(e time.Duration) string {
			if e < 2*time.Minute+10*time.Second {
				return "idle"
			}
			return "working"
		},
		func(time.Duration) string { return screen })
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if got := str(h.record(t, "j"), "state"); got == "lost" {
		t.Fatalf("hk record lost: a reviewer resumed at its short reset was ended")
	}
	if kinds := eventKindsOf(t, a, "j"); countOf(kinds, quotaWaitKind) != 1 {
		t.Fatalf("hold events=%d, want one: %v", countOf(kinds, quotaWaitKind), kinds)
	}
	if countOf(eventKindsOf(t, a, "j"), "job.lost") != 0 {
		t.Fatal("job.lost recorded for a resumed reviewer")
	}
}

// A held reviewer whose pane is still on the stop when the wait runs out (the
// manager never resumed it) is ended as an idle stopped reviewer is: the hk
// record is lost with the quota_stop end reason and job.lost is announced.
func TestWatcherReviewerHoldRunsOutOnTheStopEnds(t *testing.T) {
	a, f, h, _ := quotaStopFixture(t, "reviewer", "idle", "")
	screen := shortResetScreen(t)
	reviewerHoldRunner(t, a, f,
		func(time.Duration) string { return "idle" },
		func(time.Duration) string { return screen })
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	rec := h.record(t, "j")
	if str(rec, "state") != "lost" || !strings.HasPrefix(str(rec, "end_reason"), "quota_stop pool=devin-free") {
		t.Fatalf("hk record %v, want lost with the quota_stop end reason", rec)
	}
	if p := lostPayload(t, a); p == nil || str(p, "reason") != "quota_stop" {
		t.Fatalf("job.lost payload=%v, want reason quota_stop", p)
	}
}

// A second stop after the one hold (the manager resumed, the pane stopped
// again) is ended at once: nothing is held twice.
func TestWatcherReviewerSecondStopAfterHoldEndsAtOnce(t *testing.T) {
	a, f, h, _ := quotaStopFixture(t, "reviewer", "idle", "")
	screen := shortResetScreen(t)
	reviewerHoldRunner(t, a, f,
		func(e time.Duration) string {
			if e >= 2*time.Minute && e < 2*time.Minute+10*time.Second {
				return "working"
			}
			return "idle"
		},
		func(time.Duration) string { return screen })
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if countOf(eventKindsOf(t, a, "j"), quotaWaitKind) != 1 {
		t.Fatalf("holds=%d, want one", countOf(eventKindsOf(t, a, "j"), quotaWaitKind))
	}
	if got := str(h.record(t, "j"), "state"); got != "lost" {
		t.Fatalf("hk state=%q after a second stop, want lost", got)
	}
}

func countOf(kinds []string, kind string) int {
	n := 0
	for _, k := range kinds {
		if k == kind {
			n++
		}
	}
	return n
}

// A watcher restarted inside a reviewer's wait keeps the hold the earlier watcher
// wrote (mergewake review, hk 5355): the job is not ended before the wait runs out
// while the manager's resume is still pending.
func TestWatcherReviewerRestartInsideTheWaitKeepsTheHold(t *testing.T) {
	a, f, h, _ := quotaStopFixture(t, "reviewer", "idle", "")
	screen := shortResetScreen(t)
	reviewerHoldRunner(t, a, f,
		func(time.Duration) string { return "idle" },
		func(time.Duration) string { return screen })
	// The earlier watcher's durable hold: its wait still has three minutes to run.
	if e := a.event(context.Background(), "j", quotaWaitKind, map[string]any{
		"pane_id": "w:p1", "pool": "devin-free",
		"reset_at": a.Now().Add(2 * time.Minute).UTC().Format(time.RFC3339),
		"until":    a.Now().Add(3 * time.Minute).UTC().Format(time.RFC3339),
	}); e != nil {
		t.Fatal(e)
	}
	start := a.Now()
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if waited := a.Now().Sub(start); waited < 2*time.Minute {
		t.Fatalf("the restarted watcher ended the reviewer after %v, inside the inherited wait", waited)
	}
	if str(h.record(t, "j"), "state") != "lost" {
		t.Fatalf("hk record %v, want lost once the wait ran out", h.record(t, "j"))
	}
}

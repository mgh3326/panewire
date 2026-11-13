package fleet

import (
	"slices"
	"testing"
	"time"
)

// The hub-ordered sweep (hk 4512): the hub asks each node to close the pane of
// every job that ended more than an age ago, whatever herdr shows the pane
// doing. A job's end time is its terminal event's time, the fixture's base
// minute 0 here.

// endedSweep runs one ended-job pass at minute m after the base time.
func (f *paneSweepFixture) endedSweep(minute int, age time.Duration) []SweptPane {
	f.t.Helper()
	f.now = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC).Add(time.Duration(minute) * time.Minute)
	closes, err := f.sweeper.SweepEnded(f.t.Context(), f.a, age)
	if err != nil {
		f.t.Fatal(err)
	}
	return closes
}

func TestEndedSweepClosesAWorkingReviewerPaneOlderThanTheAge(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "")
	f.status[pane] = "working"
	putEvent(t, f.a, "r", "job.revoked", map[string]any{})
	if closes := f.endedSweep(59, time.Hour); len(closes) != 0 || len(f.closed()) != 0 {
		t.Fatalf("pane closed before the age: %v", closes)
	}
	closes := f.endedSweep(61, time.Hour)
	want := SweptPane{Job: "r", Pane: pane, Role: "reviewer", Reason: "job_revoked"}
	if len(closes) != 1 || closes[0] != want {
		t.Fatalf("closes = %+v, want [%+v]", closes, want)
	}
	if got := f.closed(); !slices.Equal(got, []string{pane}) {
		t.Fatalf("herdr closed %v, want [%s]", got, pane)
	}
	if payload := f.reaped("r"); payload["reason"] != "job_revoked" || payload["pane_id"] != pane {
		t.Fatalf("job.reaped payload = %v", payload)
	}
}

func TestEndedSweepKeepsAKeptJobsPane(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("k", "reviewer", "", map[string]any{"keep": true})
	putEvent(t, f.a, "k", "job.revoked", map[string]any{})
	if closes := f.endedSweep(61, time.Hour); len(closes) != 0 || len(f.closed()) != 0 {
		t.Fatalf("a --keep pane was closed: %+v", closes)
	}
}

func TestEndedSweepKeepsACompletedWorkerWithAnOpenPR(t *testing.T) {
	g := newSweepGitFake(t)
	f := newPaneSweepFixture(t)
	f.withGit(g)
	cwd := g.worktree(t, "w")
	f.job("w", "worker", "42", map[string]any{"cwd": cwd})
	putEvent(t, f.a, "w", "job.completed", map[string]any{})
	f.prChanges["42"] = []PRChange{{Number: 9, State: "open", Head: paneSweepTestHead, Repo: "mgh3326/panewire"}}
	if closes := f.endedSweep(61, time.Hour); len(closes) != 0 || len(f.closed()) != 0 {
		t.Fatalf("a completed worker on an open PR was closed: %+v", closes)
	}
	if !f.infoHas("task pane kept", "job=w", "reason=open_pr", "pr=9") {
		t.Fatalf("info lines = %q", f.info)
	}
}

func TestEndedSweepClosesAPaneWhoseNameWasLostAtItsWorktree(t *testing.T) {
	g := newSweepGitFake(t)
	f := newPaneSweepFixture(t)
	f.withGit(g)
	cwd := g.worktree(t, "r")
	pane := f.job("r", "reviewer", "", map[string]any{"cwd": cwd})
	f.names[pane] = ""
	f.cwds[pane] = cwd
	putEvent(t, f.a, "r", "job.revoked", map[string]any{})
	closes := f.endedSweep(61, time.Hour)
	if len(closes) != 1 || closes[0].Pane != pane {
		t.Fatalf("closes = %+v, want the nameless pane %s", closes, pane)
	}
}

package fleet

import (
	"strings"
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

func (f *paneSweepFixture) onPaneRead(hook func()) {
	prev := f.runner.Do
	f.runner.Do = func(c Command) Result {
		r := prev(c)
		if c.Name == "herdr" && strings.HasPrefix(strings.Join(c.Args, " "), "pane read ") {
			hook()
		}
		return r
	}
}

func (f *paneSweepFixture) onAgentList(n int, hook func()) {
	prev := f.runner.Do
	lists := 0
	f.runner.Do = func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
			lists++
			if lists == n {
				hook()
			}
		}
		return prev(c)
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
	f.job("w", "implementer", "42", map[string]any{"cwd": cwd})
	putEvent(t, f.a, "w", "job.completed", map[string]any{})
	f.prChanges["42"] = []PRChange{{Number: 9, State: "open", Head: paneSweepTestHead, Repo: "mgh3326/panewire"}}
	if closes := f.endedSweep(61, time.Hour); len(closes) != 0 || len(f.closed()) != 0 {
		t.Fatalf("a completed worker on an open PR was closed: %+v", closes)
	}
	if !f.infoHas("task pane kept", "job=w", "reason=open_pr", "pr=9") {
		t.Fatalf("info lines = %q", f.info)
	}
}

func TestEndedSweepKeepsAPaneReassignedWhileItsScreenIsRead(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "")
	f.status[pane] = "working"
	putEvent(t, f.a, "r", "job.revoked", map[string]any{})
	f.onPaneRead(func() { f.names[pane] = "someone-else" })
	if closes := f.endedSweep(61, time.Hour); len(closes) != 0 || len(f.closed()) != 0 {
		t.Fatalf("a reassigned pane was closed: %+v closed=%v", closes, f.closed())
	}
	if !f.infoHas("task pane kept", "job=r", "reason=ownership-unproven") {
		t.Fatalf("info lines = %q", f.info)
	}
}

func TestEndedSweepKeepsAPaneRemovedWhileItsScreenIsRead(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "")
	f.status[pane] = "working"
	putEvent(t, f.a, "r", "job.revoked", map[string]any{})
	f.onPaneRead(func() { f.shut[pane] = true })
	if closes := f.endedSweep(61, time.Hour); len(closes) != 0 || len(f.closed()) != 0 {
		t.Fatalf("a removed pane was closed: %+v closed=%v", closes, f.closed())
	}
	if !f.infoHas("task pane kept", "job=r", "reason=ownership-unproven") {
		t.Fatalf("info lines = %q", f.info)
	}
}

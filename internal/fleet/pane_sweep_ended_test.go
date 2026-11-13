package fleet

import (
	"slices"
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

// onPaneRead runs hook after herdr has answered a pane read, so the pane is
// reassigned or removed while the sweep holds the screen it read.
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

// onAgentList runs hook when herdr is asked for its agent list the nth time,
// before the list is answered.
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

// Hk 4562610630 and hk 4562610925: the ended-job sweep closes a pane whatever
// its status, but only while the job's pane is still proven by a fresh row at
// the moment of the close. A pane reassigned or removed after the sweep read
// its screen must be kept.

func TestEndedSweepKeepsAPaneReassignedWhileItsScreenIsRead(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "")
	f.status[pane] = "working"
	putEvent(t, f.a, "r", "job.revoked", map[string]any{})
	f.onPaneRead(func() { f.names[pane] = "someone-else" })
	if closes := f.endedSweep(61, time.Hour); len(closes) != 0 || len(f.closed()) != 0 {
		t.Fatalf("a reassigned pane was closed: %+v closed=%v", closes, f.closed())
	}
	if !f.infoHas("task pane kept", "job=r", "reason=pane-mismatch") {
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
	if !f.infoHas("task pane kept", "job=r", "reason=pane-gone") {
		t.Fatalf("info lines = %q", f.info)
	}
}

func TestEndedSweepReadsNoScreenOfAPaneReassignedAfterItsListing(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "")
	f.status[pane] = "working"
	putEvent(t, f.a, "r", "job.revoked", map[string]any{})
	f.onAgentList(2, func() { f.names[pane] = "someone-else" })
	if closes := f.endedSweep(61, time.Hour); len(closes) != 0 || len(f.closed()) != 0 {
		t.Fatalf("a reassigned pane was closed: %+v closed=%v", closes, f.closed())
	}
	for _, c := range f.runner.Calls {
		if c.Name == "herdr" && len(c.Args) >= 2 && c.Args[0] == "pane" && c.Args[1] == "read" {
			t.Fatalf("the screen of a reassigned pane was read: %v", c.Args)
		}
	}
}

// Hk 4562780613 and hk 4562780592: a sweep stores the screen it read only when
// the pane is still the job's after the read. A pane reassigned or removed
// during the read must have its screen discarded, not uploaded as the job's.
// The unchanged control proves the screen document check is not vacuous.
func TestEndedSweepStoresNoScreenOfAPaneChangedDuringTheRead(t *testing.T) {
	for name, change := range map[string]func(f *paneSweepFixture, pane string){
		"unchanged":  nil,
		"reassigned": func(f *paneSweepFixture, pane string) { f.names[pane] = "someone-else" },
		"removed":    func(f *paneSweepFixture, pane string) { f.shut[pane] = true },
	} {
		t.Run(name, func(t *testing.T) {
			f := newPaneSweepFixture(t)
			hk := useFakeHK(t)
			pane := f.job("r", "reviewer", "42")
			f.status[pane] = "working"
			putEvent(t, f.a, "r", "job.revoked", map[string]any{})
			if change != nil {
				f.onPaneRead(func() { change(f, pane) })
			}
			closes := f.endedSweep(61, time.Hour)
			put := screenDocPut(t, hk, "r")
			if change == nil {
				if put == nil || len(closes) != 1 {
					t.Fatalf("unchanged pane: screen put = %v, closes = %+v; want a stored screen and a close", put, closes)
				}
				return
			}
			if put != nil {
				t.Fatalf("a screen of a pane changed during its read was stored as the job's: %v", put)
			}
			if len(closes) != 0 || len(f.closed()) != 0 {
				t.Fatalf("a pane changed during its read was closed: %+v closed=%v", closes, f.closed())
			}
		})
	}
}

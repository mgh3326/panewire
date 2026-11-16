package fleet

import (
	"strings"
	"testing"
	"time"
)

// The hub-ordered sweep (hk 4693): the hub asks each node to close the pane
// of every job that ended more than an age ago, whatever herdr shows the pane
// doing. A job's end time is its terminal event's time, the fixture's base
// minute 0 here. A pane is the job's own when the recorded pane id and the
// harness session id job.spawned recorded both match herdr (decision 265).

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

// A completed reviewer's pane, proven its own by pane id and harness session,
// closes once the job's end is older than the age — whatever it shows.
func TestEndedSweepClosesACompletedReviewer(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "")
	f.status[pane] = "working"
	putEvent(t, f.a, "r", "job.completed", map[string]any{})
	closes := f.endedSweep(61, time.Hour)
	if len(closes) != 1 || closes[0].Job != "r" || closes[0].Pane != pane {
		t.Fatalf("closes = %+v", closes)
	}
	if got := f.closed(); len(got) != 1 || got[0] != pane {
		t.Fatalf("closed panes = %v", got)
	}
	if f.reaped("r") == nil {
		t.Fatal("the closed job was not reaped")
	}
	if doc := screenDocPut(t, f.hk, "r"); doc == nil || doc["body"] != "last screen of "+pane {
		t.Fatalf("the pane's last screen was not saved to hk: %v", doc)
	}
}

// A reviewer ended inside the age is not yet due.
func TestEndedSweepKeepsAReviewerInsideTheAge(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("r", "reviewer", "")
	putEvent(t, f.a, "r", "job.completed", map[string]any{})
	if closes := f.endedSweep(59, time.Hour); len(closes) != 0 || len(f.closed()) != 0 {
		t.Fatalf("a reviewer inside the age was closed: %+v", closes)
	}
}

// A different session in the job's pane is another job's agent: the pane is
// kept and counted session_mismatch (decision 265).
func TestEndedSweepKeepsAPaneWhoseSessionDoesNotMatch(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "")
	f.sessions[pane] = `{"value":"sess-other"}`
	putEvent(t, f.a, "r", "job.completed", map[string]any{})
	if closes := f.endedSweep(61, time.Hour); len(closes) != 0 || len(f.closed()) != 0 {
		t.Fatalf("a pane with another session was closed: %+v", closes)
	}
	if f.reaped("r") != nil {
		t.Fatal("the mismatched job was reaped")
	}
	if !f.infoHas("task pane kept", "job=r", "reason=session_mismatch") {
		t.Fatalf("info lines = %q", f.info)
	}
	metric := readPaneSweepMetric(t, f.a)
	if !strings.Contains(metric, `panewire_ended_job_panes_open{reason="session_mismatch"} 1`) {
		t.Fatalf("metric = %q", metric)
	}
}

// A job whose receipt recorded no harness session proves its pane by the facts
// panewire reap uses (hk 4904): the row carries the job's own name and tab and
// is not working. A pane another agent took over is not the job's, so the same
// receipt keeps it as session_unrecorded.
func TestEndedSweepProvesAReceiptWithNoSessionByTheReapFacts(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("r", "reviewer", "", map[string]any{"harness_session": map[string]any{"kind": "omp", "id": ""}})
	putEvent(t, f.a, "r", "job.completed", map[string]any{})
	if closes := f.endedSweep(61, time.Hour); len(closes) != 1 || len(f.closed()) != 1 {
		t.Fatalf("a finished pane with no recorded session stayed open: %+v closed=%v info=%v", closes, f.closed(), f.info)
	}

	g := newPaneSweepFixture(t)
	pane := g.job("r", "reviewer", "", map[string]any{"harness_session": map[string]any{"kind": "omp", "id": ""}})
	g.names[pane] = "agent-someone-else"
	putEvent(t, g.a, "r", "job.completed", map[string]any{})
	if closes := g.endedSweep(61, time.Hour); len(closes) != 0 || len(g.closed()) != 0 {
		t.Fatalf("a pane another agent holds was closed: %+v closed=%v", closes, g.closed())
	}
	if !g.infoHas("task pane kept", "job=r", "reason=session_unrecorded") {
		t.Fatalf("info lines = %q", g.info)
	}
}

// A job that never spawned has no job.spawned record — nothing to prove by —
// so its pane is never a candidate.
func TestEndedSweepNeverTouchesAJobWithNoSpawnRecord(t *testing.T) {
	f := newPaneSweepFixture(t)
	putEvent(t, f.a, "j", "job.claim", map[string]any{"owner_lane": "owner", "task_id": "42", "role": "reviewer"})
	f.status["p-j"], f.names["p-j"] = "idle", "agent-j"
	if closes := f.endedSweep(1441, time.Hour); len(closes) != 0 || len(f.closed()) != 0 {
		t.Fatalf("a job with no spawn record was swept: %+v", closes)
	}
}

// A --keep pane waits out its day (decision 268).
func TestEndedSweepKeepsAKeptJobsPaneUntilItsDay(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("k", "reviewer", "", map[string]any{"keep": true})
	putEvent(t, f.a, "k", "job.revoked", map[string]any{})
	if closes := f.endedSweep(61, time.Hour); len(closes) != 0 || len(f.closed()) != 0 {
		t.Fatalf("a --keep pane was closed inside its day: %+v", closes)
	}
	if closes := f.endedSweep(1439, time.Hour); len(closes) != 0 || len(f.closed()) != 0 {
		t.Fatalf("a --keep pane was closed before 24h: %+v", closes)
	}
	if closes := f.endedSweep(1441, time.Hour); len(closes) != 1 || len(f.closed()) != 1 {
		t.Fatalf("a --keep pane did not close at 24h: %+v closed=%v", closes, f.closed())
	}
}

// A completed implementer whose branch is on an open PR stays the PR's
// author; the pane is kept past the age.
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

// The close re-proves the pane under the acting lock: a session that changed
// while the screen was read is another agent, and the pane is kept.
func TestEndedSweepKeepsAPaneReassignedWhileItsScreenIsRead(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "")
	f.status[pane] = "working"
	putEvent(t, f.a, "r", "job.revoked", map[string]any{})
	f.onPaneRead(func() { f.sessions[pane] = `{"value":"sess-new"}` })
	if closes := f.endedSweep(61, time.Hour); len(closes) != 0 || len(f.closed()) != 0 {
		t.Fatalf("a reassigned pane was closed: %+v closed=%v", closes, f.closed())
	}
	if !f.infoHas("task pane kept", "job=r", "reason=session_mismatch") {
		t.Fatalf("info lines = %q", f.info)
	}
}

// A pane gone between the screen read and the close re-read is reaped as
// gone; no close runs.
func TestEndedSweepReapsAPaneRemovedWhileItsScreenIsRead(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "")
	f.status[pane] = "working"
	putEvent(t, f.a, "r", "job.revoked", map[string]any{})
	f.onPaneRead(func() { f.shut[pane] = true })
	closes := f.endedSweep(61, time.Hour)
	if len(closes) != 1 || closes[0].Reason != "pane_gone" || len(f.closed()) != 0 {
		t.Fatalf("a removed pane: closes=%+v closed=%v", closes, f.closed())
	}
	if f.reaped("r") == nil {
		t.Fatal("the job whose pane vanished was not reaped")
	}
}

// A close herdr refuses keeps the pane and counts close_failed; the next pass
// tries again.
func TestEndedSweepKeepsAPaneWhoseCloseFails(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("r", "reviewer", "")
	putEvent(t, f.a, "r", "job.completed", map[string]any{})
	base := f.runner.Do
	f.runner.Do = func(c Command) Result {
		if c.Name == "herdr" && len(c.Args) == 3 && c.Args[1] == "close" {
			return Result{Code: 1, Err: "pane busy"}
		}
		return base(c)
	}
	if closes := f.endedSweep(61, time.Hour); len(closes) != 0 {
		t.Fatalf("closes = %+v", closes)
	}
	if f.reaped("r") != nil {
		t.Fatal("the job whose close failed was reaped")
	}
	if !f.infoHas("task pane kept", "job=r", "reason=close_failed") {
		t.Fatalf("info lines = %q", f.info)
	}
	if metric := readPaneSweepMetric(t, f.a); !strings.Contains(metric, `panewire_ended_job_panes_open{reason="close_failed"} 1`) {
		t.Fatalf("metric = %q", metric)
	}
	f.runner.Do = base
	if closes := f.endedSweep(62, time.Hour); len(closes) != 1 {
		t.Fatalf("the failed close did not retry: %+v", closes)
	}
}

// A screen hk refuses keeps the pane and counts screen_save_failed.
func TestEndedSweepKeepsAPaneWhoseScreenUploadFails(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("r", "reviewer", "")
	putEvent(t, f.a, "r", "job.completed", map[string]any{})
	f.hk.Close()
	if closes := f.endedSweep(61, time.Hour); len(closes) != 0 || len(f.closed()) != 0 {
		t.Fatalf("a pane whose screen could not be saved was closed: %+v", closes)
	}
	if !f.infoHas("task pane kept", "job=r", "reason=screen_save_failed") {
		t.Fatalf("info lines = %q", f.info)
	}
}

// A pass that left nothing open still writes the metric file, so a stale
// reason clears.
func TestEndedSweepWritesAnEmptyMetric(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("r", "reviewer", "")
	putEvent(t, f.a, "r", "job.completed", map[string]any{})
	f.endedSweep(61, time.Hour)
	metric := readPaneSweepMetric(t, f.a)
	if strings.Contains(metric, "panewire_ended_job_panes_open{") {
		t.Fatalf("closed panes still counted: %q", metric)
	}
	if !strings.Contains(metric, "# TYPE panewire_ended_job_panes_open gauge") {
		t.Fatalf("metric = %q", metric)
	}
}

// A --keep implementer whose task ended closes a day after the sweep first
// saw the end — a task-derived end has no earlier record (decision 268).
func TestEndedSweepClosesAKepWorkerADayAfterItsTaskEnd(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("k", "implementer", "42", map[string]any{"keep": true})
	f.tasks["42"] = "merged"
	f.at(0)
	f.at(1439)
	if len(f.closed()) != 0 {
		t.Fatalf("a --keep pane was closed before its day was out")
	}
	f.at(1440)
	if len(f.closed()) != 1 || f.reaped("k") == nil {
		t.Fatalf("a --keep pane did not close a day after its task ended: %v", f.info)
	}
}

// A worker that completed once, was reprompted for another round and whose
// task then merged is live again: the old completion does not date its end.
// The task-derived end starts with the first pass that saw it, so the pane is
// not closed on that pass and closes one hour later.
func TestEndedSweepDatesARepromptedWorkersTaskEndFromItsFirstSighting(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("w", "implementer", "42")
	putEvent(t, f.a, "w", "job.completed", map[string]any{})
	putEvent(t, f.a, "w", "job.reprompted", map[string]any{"reason": "quota_stop_resumed"})
	f.tasks["42"] = "merged"
	if closes := f.endedSweep(61, time.Hour); len(closes) != 0 || len(f.closed()) != 0 {
		t.Fatalf("a reprompted worker's pane was closed on the first sighting of its task end: %+v", closes)
	}
	if closes := f.endedSweep(121, time.Hour); len(closes) != 1 || len(f.closed()) != 1 {
		t.Fatalf("a reprompted worker's pane did not close an hour after its task end was first seen: %+v closed=%v info=%v", closes, f.closed(), f.info)
	}
}

// A task that reopens and ends again restarts the deadline: the first
// sighting of the earlier end must not date the new one, or the newly ended
// worker's pane would close at once (and a --keep pane lose its day).
func TestEndedSweepRestartsTheDeadlineWhenATaskReopensAndEndsAgain(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("w", "implementer", "42")
	f.status[pane] = "working"
	f.tasks["42"] = "merged"
	f.endedSweep(0, time.Hour)
	f.tasks["42"] = "developing"
	f.endedSweep(30, time.Hour)
	f.tasks["42"] = "merged"
	if closes := f.endedSweep(61, time.Hour); len(closes) != 0 || len(f.closed()) != 0 {
		t.Fatalf("a re-ended task reused the earlier sighting: %+v", closes)
	}
	if closes := f.endedSweep(121, time.Hour); len(closes) != 1 || len(f.closed()) != 1 {
		t.Fatalf("a re-ended task did not close an hour after its own end: %+v closed=%v info=%v", closes, f.closed(), f.info)
	}
}

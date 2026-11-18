package fleet

import (
	"os"
	"slices"
	"strings"
	"testing"
)

// A job a successor replaced (a job.replaced event, docs/pane-sweep.md
// "Replaced jobs") counts as ended: the successor owns the task and the PR, so
// the job's pane closes on the first sweep that finds it not working, whatever
// its PR or blocked state, and its worktree stays (hk 5308).

// replacedJob seeds a job bound to task, with cwd as its worktree, that a
// successor replaced; the pane is idle unless the test changes it.
func replacedJob(f *paneSweepFixture, id, task, cwd string) string {
	f.t.Helper()
	pane := f.sweptJob(id, task, cwd)
	putEvent(f.t, f.a, id, "job.replaced", map[string]any{"replaced_by": "succ-" + id, "task_id": task})
	return pane
}

// A replaced job whose pane sits on a quota error (blocked) closes, and its
// last screen stays in the job directory.
func TestPaneSweepClosesAReplacedJobsBlockedPane(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("j", "implementer", "42")
	putEvent(t, f.a, "j", "job.replaced", map[string]any{"replaced_by": "succ-j", "task_id": "42"})
	f.status[pane] = "blocked"
	f.at(0)
	if !slices.Equal(f.closed(), []string{pane}) {
		t.Fatalf("closed = %v, want the blocked pane %s", f.closed(), pane)
	}
	if f.reaped("j") == nil {
		t.Fatalf("no job.reaped event; info = %q", f.info)
	}
	if !f.localScreenFile("j") {
		t.Fatal("the replaced pane's last screen was not kept in the job directory")
	}
}

// A replaced job whose branch has an open PR closes anyway: the PR is its
// successor's. The worktree is kept, not removed.
func TestPaneSweepClosesAReplacedJobWhoseBranchHasAnOpenPR(t *testing.T) {
	g := newSweepGitFake(t)
	f := newPaneSweepFixture(t)
	f.withGit(g)
	cwd := g.worktree(t, "j")
	pane := replacedJob(f, "j", "42", cwd)
	f.tasks["42"] = "deployed"
	g.head = paneSweepPRHead
	f.prChanges["42"] = []PRChange{{Number: 306, State: "open", Head: paneSweepPRHead}}
	f.at(0)
	if !slices.Equal(f.closed(), []string{pane}) {
		t.Fatalf("closed = %v, want %s; info = %q", f.closed(), pane, f.info)
	}
	if f.infoHas("reason=open_pr") {
		t.Fatalf("the open-PR keep ran before the replaced rule; info = %q", f.info)
	}
	if len(g.removed) != 0 {
		t.Fatalf("worktree removed for a replaced job: %v", g.removed)
	}
	if _, err := os.Stat(cwd); err != nil {
		t.Fatalf("worktree gone: %v", err)
	}
	if !f.infoHas("task worktree kept", "job=j", "reason=replaced") {
		t.Fatalf("info lines = %q", f.info)
	}
}

// A replaced job's working pane is never forced: it is kept, and closes on the
// first sweep after it stops.
func TestPaneSweepKeepsAReplacedJobsWorkingPaneUntilItStops(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("j", "implementer", "42")
	putEvent(t, f.a, "j", "job.replaced", map[string]any{"replaced_by": "succ-j", "task_id": "42"})
	f.status[pane] = "working"
	f.at(0)
	if len(f.closed()) != 0 {
		t.Fatalf("a working pane was closed: %v", f.closed())
	}
	if !f.infoHas("task pane kept", "job=j", "reason=pane-working") {
		t.Fatalf("info lines = %q", f.info)
	}
	f.status[pane] = "idle"
	f.at(1)
	if !slices.Equal(f.closed(), []string{pane}) {
		t.Fatalf("closed = %v after the pane stopped; info = %q", f.closed(), f.info)
	}
}

// A pane another job's label has taken is refused: the replaced job's close
// does not touch it.
func TestPaneSweepRefusesAReplacedJobsPaneThatChangedHands(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("j", "implementer", "42")
	putEvent(t, f.a, "j", "job.replaced", map[string]any{"replaced_by": "succ-j", "task_id": "42"})
	f.status[pane], f.names[pane] = "blocked", "agent-other"
	f.at(0)
	if len(f.closed()) != 0 || f.reaped("j") != nil {
		t.Fatalf("a pane that changed hands was closed: %v", f.closed())
	}
	if !f.infoHas("task pane kept", "job=j", "reason=pane-mismatch") {
		t.Fatalf("info lines = %q", f.info)
	}
}

// The owner lane hears one info line naming the job and its successor, under
// an event id keyed by the job.
func TestPaneSweepTellsTheOwnerLaneAboutAReplacedClose(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("j", "implementer", "42")
	putEvent(t, f.a, "j", "job.replaced", map[string]any{"replaced_by": "succ-j", "task_id": "42"})
	f.status[pane] = "blocked"
	f.at(0)
	var notice []string
	for _, c := range f.runner.Calls {
		if c.Name == "panewire" {
			notice = c.Args
		}
	}
	if notice == nil {
		t.Fatal("no panewire emit for the owner lane")
	}
	if !slices.Contains(notice, "owner") || !slices.Contains(notice, "replaced-close-j") || !slices.Contains(notice, "info") {
		t.Fatalf("emit args = %q, want lane owner, event id replaced-close-j, type info", notice)
	}
	text := notice[slices.Index(notice, "--text")+1]
	if !strings.Contains(text, "[panewire] closed j (replaced by succ-j) on ") {
		t.Fatalf("text = %q", text)
	}
}

package fleet

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The sweep's gaps (hk 3383): a task state that belongs to a category, a fix
// author whose task is its parent's, a job that was closed, and the worktree
// of a non-merged end.

// closeJob appends the record `panewire job close` writes: flat, with the
// outcome beside the kind.
func (f *paneSweepFixture) closeJob(id, outcome string) {
	f.t.Helper()
	dir := filepath.Join(f.a.Jobs, id, "events")
	files, _ := os.ReadDir(dir)
	record := map[string]any{"kind": "job.revoked", "job_id": id, "outcome": outcome, "reason": "test", "created_at": f.now.Add(time.Duration(len(files)) * time.Nanosecond).Format(time.RFC3339Nano)}
	b, err := json.Marshal(record)
	if err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%04d-job.revoked.json", len(files))), b, 0600); err != nil {
		f.t.Fatal(err)
	}
}

// fixAuthor seeds a worker spawned with --parent-job: its claim names no task
// of its own, only the job it inherited from. extra adds receipt fields.
func (f *paneSweepFixture) fixAuthor(id, parent string, extra ...map[string]any) string {
	f.t.Helper()
	pane := "p-" + id
	putEvent(f.t, f.a, id, "job.claim", map[string]any{"owner_lane": "owner", "parent_lane": "parent", "t_level": "T1", "role": "implementer", "task_inherited_from": parent})
	spawned := map[string]any{"pane_id": pane, "tab_id": "t-" + pane, "label": "agent-" + id, "profile": "cmd-dsflash41", "family": "deepseek"}
	for _, more := range extra {
		for key, value := range more {
			spawned[key] = value
		}
	}
	putEvent(f.t, f.a, id, "job.spawned", spawned)
	f.status[pane], f.names[pane] = "idle", "agent-"+id
	return pane
}

// (a) A task in a state the sweep has no name for, whose category is completed,
// ends the job as completed. Without the category read nothing closes it.
func TestPaneSweepFollowsTheTaskCategory(t *testing.T) {
	for _, tc := range []struct{ state, category, want string }{
		{"deployed", "completed", "completed"},
		{"released", "completed", "completed"},
		{"abandoned", "canceled", "lost"},
	} {
		t.Run(tc.state, func(t *testing.T) {
			f := newPaneSweepFixture(t)
			pane := f.job("j", "implementer", "42")
			f.tasks["42"], f.category["42"] = tc.state, tc.category
			f.at(0)
			f.at(11)
			if !slices.Equal(f.closed(), []string{pane}) {
				t.Fatalf("closed %v, want [%s]", f.closed(), pane)
			}
			if state := f.store.record(t, "j")["state"]; state != tc.want {
				t.Fatalf("hk job state = %v, want %s", state, tc.want)
			}
			if payload := f.reaped("j"); payload["reason"] != "task_"+tc.state {
				t.Fatalf("job.reaped payload = %v", payload)
			}
		})
	}
}

// A category the sweep does not end on keeps the pane, even for a state name
// the fallback would end on: the category is the answer when it is present.
func TestPaneSweepKeepsAPaneWhoseCategoryIsLive(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("j", "implementer", "42")
	f.tasks["42"], f.category["42"] = "merged", "started"
	f.at(0)
	f.at(60)
	if len(f.closed()) != 0 {
		t.Fatalf("pane closed for a task in a live category")
	}
}

// (b) A fix author is judged on its parent's task, found in the parent's job
// record on this node or, failing that, in hk.
func TestPaneSweepJudgesAFixAuthorOnItsParentsTask(t *testing.T) {
	t.Run("parent job on the node", func(t *testing.T) {
		f := newPaneSweepFixture(t)
		f.job("parent", "builder", "42")
		pane := f.fixAuthor("fix", "parent")
		f.tasks["42"], f.category["42"] = "deployed", "completed"
		f.at(0)
		f.at(11)
		if !slices.Equal(f.closed(), []string{pane}) {
			t.Fatalf("closed %v, want [%s]", f.closed(), pane)
		}
		if payload := f.reaped("fix"); payload["reason"] != "task_deployed" {
			t.Fatalf("job.reaped payload = %v", payload)
		}
		if !f.infoHas("task pane closed", "job=fix", "task=42", "task_state=deployed") {
			t.Fatalf("info lines = %q", f.info)
		}
	})
	t.Run("parent job in hk", func(t *testing.T) {
		f := newPaneSweepFixture(t)
		f.store.records["parent"] = map[string]any{"job_id": "parent", "task_id": float64(42), "state": "completed"}
		pane := f.fixAuthor("fix", "parent")
		f.tasks["42"], f.category["42"] = "deployed", "completed"
		f.at(0)
		f.at(11)
		if !slices.Equal(f.closed(), []string{pane}) {
			t.Fatalf("closed %v, want [%s]", f.closed(), pane)
		}
	})
	t.Run("parent named only by the hk record", func(t *testing.T) {
		f := newPaneSweepFixture(t)
		f.store.records["parent"] = map[string]any{"job_id": "parent", "task_id": float64(42), "state": "completed"}
		pane := f.fixAuthor("fix", "")
		f.store.records["fix"] = map[string]any{"job_id": "fix", "state": "spawned", "parent_job": "parent"}
		f.tasks["42"], f.category["42"] = "deployed", "completed"
		f.at(0)
		f.at(11)
		if !slices.Equal(f.closed(), []string{pane}) {
			t.Fatalf("closed %v, want [%s]", f.closed(), pane)
		}
	})
	t.Run("parent task still live", func(t *testing.T) {
		f := newPaneSweepFixture(t)
		f.job("parent", "builder", "42")
		f.fixAuthor("fix", "parent")
		f.tasks["42"], f.category["42"] = "developing", "started"
		f.at(0)
		f.at(60)
		if len(f.closed()) != 0 {
			t.Fatalf("a fix author whose parent's task is live was closed")
		}
	})
	t.Run("no task resolves", func(t *testing.T) {
		f := newPaneSweepFixture(t)
		f.fixAuthor("fix", "nobody")
		f.at(0)
		f.at(60)
		if len(f.closed()) != 0 || f.reaped("fix") != nil {
			t.Fatalf("a fix author with no task was closed")
		}
		if !f.infoHas("task pane kept", "job=fix", "reason=no_task") {
			t.Fatalf("info lines = %q", f.info)
		}
	})
}

// (c) A job `panewire job close` revoked is swept whatever its task state.
func TestPaneSweepClosesARevokedJobWhoseTaskIsLive(t *testing.T) {
	for _, outcome := range []string{"abandoned", "superseded", "completed"} {
		t.Run(outcome, func(t *testing.T) {
			f := newPaneSweepFixture(t)
			hk := useFakeHK(t)
			pane := f.job("j", "implementer", "42")
			f.tasks["42"], f.category["42"] = "developing", "started"
			f.closeJob("j", outcome)
			f.at(0)
			f.at(5)
			if len(f.closed()) != 0 {
				t.Fatalf("pane closed inside the grace")
			}
			f.at(11)
			if !slices.Equal(f.closed(), []string{pane}) {
				t.Fatalf("closed %v, want [%s]", f.closed(), pane)
			}
			// hk 3702: the stored screen is the copy; no local file is written.
			if put := screenDocPut(t, hk, "j"); put == nil || put["body"] != "last screen of "+pane {
				t.Fatalf("screen document = %v, want the pane's screen", put)
			}
			if f.localScreenFile("j") {
				t.Fatalf("a stored screen left %s in the job dir", PaneScreenFile)
			}
			want := "lost"
			if outcome == "completed" {
				want = "completed"
			}
			if state := f.store.record(t, "j")["state"]; state != want {
				t.Fatalf("hk job state = %v, want %s", state, want)
			}
			if payload := f.reaped("j"); payload["reason"] != "job_"+outcome {
				t.Fatalf("job.reaped payload = %v", payload)
			}
			if !f.infoHas("task pane closed", "job=j", "reason=job_"+outcome) {
				t.Fatalf("info lines = %q", f.info)
			}
		})
	}
}

func TestPaneSweepClosesAJobThatCompleted(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("j", "implementer", "42")
	f.tasks["42"], f.category["42"] = "developing", "started"
	putEvent(t, f.a, "j", "job.completed", map[string]any{})
	f.at(0)
	f.at(11)
	if !slices.Equal(f.closed(), []string{pane}) {
		t.Fatalf("closed %v, want [%s]", f.closed(), pane)
	}
	if payload := f.reaped("j"); payload["reason"] != "job_completed" {
		t.Fatalf("job.reaped payload = %v", payload)
	}
}

// A closed job needs no task and no readable hk: its own record is the reason.
func TestPaneSweepClosesARevokedJobWithoutAReadableTask(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("j", "implementer", "42")
	f.taskErr = fmt.Errorf("hk down")
	f.closeJob("j", "abandoned")
	f.at(0)
	f.at(11)
	if !slices.Equal(f.closed(), []string{pane}) {
		t.Fatalf("closed %v, want [%s]", f.closed(), pane)
	}
	f2 := newPaneSweepFixture(t)
	pane2 := f2.job("k", "implementer", "")
	f2.closeJob("k", "abandoned")
	f2.at(0)
	f2.at(11)
	if !slices.Equal(f2.closed(), []string{pane2}) {
		t.Fatalf("a closed job with no task: closed %v, want [%s]", f2.closed(), pane2)
	}
}

// (d) A working pane is never closed, closed job or not.
func TestPaneSweepKeepsAWorkingPaneOfARevokedJob(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("j", "implementer", "42")
	f.tasks["42"], f.category["42"] = "developing", "started"
	f.closeJob("j", "abandoned")
	f.status[pane] = "working"
	f.at(0)
	f.at(60)
	if len(f.closed()) != 0 || f.reaped("j") != nil {
		t.Fatalf("a working pane was closed")
	}
	if !f.infoHas("task pane kept", "job=j", "reason=pane-working") {
		t.Fatalf("info lines = %q", f.info)
	}
}

// A job revived after its close (a reprompt) is live again and keeps its pane.
func TestPaneSweepKeepsARevivedJob(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("j", "implementer", "42")
	f.tasks["42"], f.category["42"] = "developing", "started"
	f.closeJob("j", "abandoned")
	putEvent(t, f.a, "j", "job.reprompted", map[string]any{})
	f.at(0)
	f.at(60)
	if len(f.closed()) != 0 {
		t.Fatalf("a revived job's pane was closed")
	}
}

// Only an implementer is swept, closed or not.
func TestPaneSweepNeverClosesAProtectedRoleForItsClosing(t *testing.T) {
	for _, role := range []string{"builder", "lead", "captain"} {
		t.Run(role, func(t *testing.T) {
			f := newPaneSweepFixture(t)
			f.job("j", role, "42")
			f.closeJob("j", "abandoned")
			f.at(0)
			f.at(60)
			if len(f.closed()) != 0 {
				t.Fatalf("a %s pane was closed", role)
			}
		})
	}
}

// abandonedFixture is a closed, abandoned job whose receipt names a node
// worktree, with a sweeper that can reach git.
func abandonedFixture(t *testing.T) (*paneSweepFixture, *sweepGitFake, string) {
	t.Helper()
	g := newSweepGitFake(t)
	f := newPaneSweepFixture(t)
	f.withGit(g)
	cwd := g.worktree(t, "job-a")
	f.sweptJob("a", "42", cwd)
	f.tasks["42"], f.category["42"] = "developing", "started"
	f.closeJob("a", "abandoned")
	return f, g, cwd
}

// (e) An abandoned dirty worktree is saved, then removed; its branch stays
// while its tip is not on origin, and the patch exists before the removal.
func TestSweepSavesThenRemovesAnAbandonedWorktree(t *testing.T) {
	f, g, cwd := abandonedFixture(t)
	g.dirty = true
	g.diff = "diff --git a/x b/x\n+abandoned edit\n"
	patch := filepath.Join(f.a.Jobs, "a", AbandonedWorktreeFile)
	var atRemove []byte
	g.onRemove = func(string) { atRemove, _ = os.ReadFile(patch) }
	f.at(0)
	f.at(11)
	if !slices.Equal(g.removed, []string{cwd}) {
		t.Fatalf("removed worktrees = %v, want [%s]", g.removed, cwd)
	}
	got, err := os.ReadFile(patch)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{" M file", "+abandoned edit", "job-a", "job_abandoned"} {
		if !strings.Contains(string(got), want) {
			t.Fatalf("patch lacks %q:\n%s", want, got)
		}
	}
	if string(atRemove) != string(got) {
		t.Fatalf("the patch was not complete before the removal: %q", atRemove)
	}
	if info, err := os.Stat(patch); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("patch mode = %v, %v", info, err)
	}
	if len(g.deleted) != 0 || len(g.forced) != 0 {
		t.Fatalf("branches deleted %v, forced %v: the tip is not on origin", g.deleted, g.forced)
	}
	if !f.infoHas("task worktree removed", "job=a", "worktree=job-a", "reason=job_abandoned") {
		t.Fatalf("info lines = %q", f.info)
	}
	if !f.infoHas("task branch kept", "job=a", "branch=wk/job", "reason=not_on_origin_kept") {
		t.Fatalf("info lines = %q", f.info)
	}
}

// A branch whose tip origin already holds is deleted with -D.
func TestSweepDeletesTheBranchOfAnAbandonedWorktreeWhenItIsOnOrigin(t *testing.T) {
	f, g, _ := abandonedFixture(t)
	g.onOrigin = true
	f.at(0)
	f.at(11)
	if !slices.Equal(g.forced, []string{"wk/job"}) {
		t.Fatalf("force-deleted branches = %v, want [wk/job]", g.forced)
	}
	if !f.infoHas("task branch removed", "job=a", "branch=wk/job", "reason=on_origin") {
		t.Fatalf("info lines = %q", f.info)
	}
}

func TestSweepKeepsTheBranchWhenOriginCannotBeFetched(t *testing.T) {
	f, g, _ := abandonedFixture(t)
	g.onOrigin, g.fetchErr = true, true
	f.at(0)
	f.at(11)
	if len(g.forced) != 0 || len(g.removed) != 1 {
		t.Fatalf("forced %v, removed %v", g.forced, g.removed)
	}
}

// A dropped task is a non-merged end as well; its worktree goes the same way.
func TestSweepSavesThenRemovesADroppedTasksWorktree(t *testing.T) {
	g := newSweepGitFake(t)
	f := newPaneSweepFixture(t)
	f.withGit(g)
	cwd := g.worktree(t, "job-a")
	f.sweptJob("a", "42", cwd)
	f.tasks["42"] = "dropped"
	g.dirty, g.diff = true, "+edit\n"
	f.at(0)
	f.at(11)
	if !slices.Equal(g.removed, []string{cwd}) {
		t.Fatalf("removed worktrees = %v, want [%s]", g.removed, cwd)
	}
	if _, err := os.Stat(filepath.Join(f.a.Jobs, "a", AbandonedWorktreeFile)); err != nil {
		t.Fatalf("no patch saved for a dropped task: %v", err)
	}
	if f.reaped("a") == nil {
		t.Fatalf("the dropped job's pane was not closed")
	}
}

// A merged end keeps the clean-only rule even if the job was abandoned.
func TestSweepKeepsADirtyWorktreeOfAMergedTaskThoughTheJobWasAbandoned(t *testing.T) {
	f, g, _ := abandonedFixture(t)
	g.dirty = true
	f.tasks["42"], f.category["42"] = "deployed", "completed"
	f.at(0)
	f.at(11)
	if len(g.removed) != 0 {
		t.Fatalf("removed %v", g.removed)
	}
	if !f.infoHas("task worktree kept", "reason=dirty_kept") {
		t.Fatalf("info lines = %q", f.info)
	}
	if _, err := os.Stat(filepath.Join(f.a.Jobs, "a", AbandonedWorktreeFile)); err == nil {
		t.Fatalf("a merged end saved a patch")
	}
}

// A closed job that finished its work while the task is still open keeps its
// worktree for the task's owner.
func TestSweepKeepsTheWorktreeOfACompletedJobWhoseTaskIsOpen(t *testing.T) {
	g := newSweepGitFake(t)
	f := newPaneSweepFixture(t)
	f.withGit(g)
	f.sweptJob("a", "42", g.worktree(t, "job-a"))
	f.tasks["42"], f.category["42"] = "developing", "started"
	f.closeJob("a", "completed")
	f.at(0)
	f.at(11)
	if len(g.removed) != 0 {
		t.Fatalf("removed %v", g.removed)
	}
	if !f.infoHas("task worktree kept", "job=a", "reason=task_open_kept") {
		t.Fatalf("info lines = %q", f.info)
	}
}

// (f) A path another live job uses is kept, and nothing is saved or removed.
func TestSweepKeepsAnAbandonedWorktreeAnotherLiveJobUses(t *testing.T) {
	f, g, cwd := abandonedFixture(t)
	g.dirty = true
	f.sweptJob("b", "43", cwd)
	f.tasks["43"], f.category["43"] = "developing", "started"
	f.at(0)
	f.at(11)
	if len(g.removed) != 0 || len(g.forced) != 0 {
		t.Fatalf("removed %v, forced %v", g.removed, g.forced)
	}
	if !f.infoHas("task worktree kept", "job=a", "reason=shared_kept") {
		t.Fatalf("info lines = %q", f.info)
	}
	if _, err := os.Stat(filepath.Join(f.a.Jobs, "a", AbandonedWorktreeFile)); err == nil {
		t.Fatalf("a patch was saved for a worktree that stayed")
	}
}

// A worktree whose changes cannot be read keeps its changes.
func TestSweepKeepsAnAbandonedWorktreeWhenItsChangesCannotBeSaved(t *testing.T) {
	f, g, _ := abandonedFixture(t)
	base := f.runner.Do
	f.runner.Do = func(c Command) Result {
		if c.Name == "git" && len(c.Args) > 2 && c.Args[2] == "diff" {
			return Result{Code: 1, Err: "bad object"}
		}
		return base(c)
	}
	f.at(0)
	f.at(11)
	if len(g.removed) != 0 {
		t.Fatalf("removed %v", g.removed)
	}
	if !f.infoHas("task worktree kept", "job=a", "reason=patch_failed_kept") {
		t.Fatalf("info lines = %q", f.info)
	}
	if f.reaped("a") == nil {
		t.Fatalf("the pane stayed open for a worktree failure")
	}
}

// git diff HEAD leaves untracked files out and the removal deletes them, so
// their contents must be in the saved patch.
func TestSweepSavesTheContentsOfUntrackedFilesBeforeRemoving(t *testing.T) {
	f, g, cwd := abandonedFixture(t)
	g.dirty = true
	g.untracked = []string{"new_file.go", "dir/other.txt"}
	touchUntracked(t, cwd, g.untracked...)
	f.at(0)
	f.at(11)
	if !slices.Equal(g.removed, []string{cwd}) {
		t.Fatalf("removed worktrees = %v, want [%s]", g.removed, cwd)
	}
	got, err := os.ReadFile(filepath.Join(f.a.Jobs, "a", AbandonedWorktreeFile))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"+contents of new_file.go", "+contents of dir/other.txt"} {
		if !strings.Contains(string(got), want) {
			t.Fatalf("patch lacks %q:\n%s", want, got)
		}
	}
}

// An untracked file that cannot be captured keeps the worktree.
func TestSweepKeepsAnAbandonedWorktreeWhenUntrackedFilesCannotBeCaptured(t *testing.T) {
	f, g, _ := abandonedFixture(t)
	g.lsFailed = true
	f.at(0)
	f.at(11)
	if len(g.removed) != 0 {
		t.Fatalf("removed %v", g.removed)
	}
	if !f.infoHas("task worktree kept", "job=a", "reason=patch_failed_kept") {
		t.Fatalf("info lines = %q", f.info)
	}
}

// touchUntracked makes the untracked files the fake git lists exist in cwd.
func touchUntracked(t *testing.T, cwd string, names ...string) {
	t.Helper()
	for _, name := range names {
		path := filepath.Join(cwd, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

// A named pipe among the untracked files would block git's read, so it is
// never diffed: the capture fails and the worktree stays. The fake would answer
// a diff of it, so only the type check keeps it from being called.
func TestSweepKeepsAnAbandonedWorktreeHoldingANamedPipe(t *testing.T) {
	f, g, cwd := abandonedFixture(t)
	touchUntracked(t, cwd, "plain.txt")
	if err := syscall.Mkfifo(filepath.Join(cwd, "pipe"), 0600); err != nil {
		t.Skipf("no named pipes here: %v", err)
	}
	g.untracked = []string{"plain.txt", "pipe"}
	var diffed []string
	base := f.runner.Do
	f.runner.Do = func(c Command) Result {
		if c.Name == "git" && slices.Contains(c.Args, "--no-index") {
			diffed = append(diffed, c.Args[len(c.Args)-1])
		}
		return base(c)
	}
	f.at(0)
	f.at(11)
	if slices.Contains(diffed, "pipe") {
		t.Fatalf("the named pipe was handed to git diff")
	}
	if len(g.removed) != 0 {
		t.Fatalf("removed %v", g.removed)
	}
	if !f.infoHas("task worktree kept", "job=a", "reason=patch_failed_kept") {
		t.Fatalf("info lines = %q", f.info)
	}
}

// A symlink is captured as a link, not followed.
func TestSweepCapturesAnUntrackedSymlinkWithoutFollowingIt(t *testing.T) {
	f, g, cwd := abandonedFixture(t)
	if err := os.Symlink("/nonexistent/target", filepath.Join(cwd, "link")); err != nil {
		t.Fatal(err)
	}
	g.untracked = []string{"link"}
	f.at(0)
	f.at(11)
	if !slices.Equal(g.removed, []string{cwd}) {
		t.Fatalf("removed %v, want [%s]", g.removed, cwd)
	}
}

// The end is the latest record's: a job re-closed as completed between the
// pass and the close is a completed one, so its worktree is not discarded.
func TestSweepRecomputesAClosedJobsEndBeforeClosing(t *testing.T) {
	f, g, _ := abandonedFixture(t)
	g.dirty = true
	base := f.runner.Do
	reclosed := false
	f.runner.Do = func(c Command) Result {
		if c.Name == "herdr" && len(c.Args) > 1 && c.Args[0] == "pane" && c.Args[1] == "read" && !reclosed {
			reclosed = true
			f.closeJob("a", "completed")
		}
		return base(c)
	}
	f.at(0)
	f.at(11)
	if !reclosed || f.reaped("a") == nil {
		t.Fatalf("the job was not closed (reclosed %v)", reclosed)
	}
	if payload := f.reaped("a"); payload["reason"] != "job_completed" {
		t.Fatalf("job.reaped payload = %v, want job_completed", payload)
	}
	if state := f.store.record(t, "a")["state"]; state != "completed" {
		t.Fatalf("hk job state = %v, want completed", state)
	}
	if len(g.removed) != 0 {
		t.Fatalf("removed %v: the job completed, its task is open", g.removed)
	}
	if !f.infoHas("task worktree kept", "job=a", "reason=task_open_kept") {
		t.Fatalf("info lines = %q", f.info)
	}
}

// A job whose history cannot be read may still use the path: when its event
// files name it, the worktree stays; when they do not, it does not block.
func TestSweepKeepsAWorktreeAnUnreadableJobNames(t *testing.T) {
	for _, tc := range []struct {
		name    string
		named   bool
		removed int
	}{{"names the path", true, 0}, {"names another path", false, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			f, g, cwd := abandonedFixture(t)
			dir := filepath.Join(f.a.Jobs, "z", "events")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			path := "/elsewhere"
			if tc.named {
				path = cwd
			}
			// No kind: the job cannot be parsed, as with a hub revocation marker.
			if err := os.WriteFile(filepath.Join(dir, "00001-x.json"), []byte(`{"type":"job.revoked","cwd":"`+path+`"}`), 0600); err != nil {
				t.Fatal(err)
			}
			f.at(0)
			f.at(11)
			if len(g.removed) != tc.removed {
				t.Fatalf("removed %v, want %d", g.removed, tc.removed)
			}
			if tc.named && !f.infoHas("task worktree kept", "job=a", "reason=shared_kept") {
				t.Fatalf("info lines = %q", f.info)
			}
		})
	}
}

// A job that is terminal but not reaped may still have its pane at work in the
// directory: the worktree stays while herdr lists that pane.
func TestSweepKeepsAWorktreeATerminalJobsPaneStillHolds(t *testing.T) {
	f, g, cwd := abandonedFixture(t)
	other := f.sweptJob("b", "43", cwd)
	f.tasks["43"], f.category["43"] = "developing", "started"
	putEvent(t, f.a, "b", "job.completed", map[string]any{})
	f.status[other] = "working"
	f.at(0)
	f.at(11)
	if len(g.removed) != 0 {
		t.Fatalf("removed %v while the other pane was open", g.removed)
	}
	if !f.infoHas("task worktree kept", "job=a", "reason=shared_kept") {
		t.Fatalf("info lines = %q", f.info)
	}
}

func TestSweepRemovesAWorktreeATerminalJobsPaneHasLeft(t *testing.T) {
	f, g, cwd := abandonedFixture(t)
	other := f.sweptJob("b", "43", cwd)
	f.tasks["43"], f.category["43"] = "developing", "started"
	putEvent(t, f.a, "b", "job.completed", map[string]any{})
	delete(f.status, other)
	f.at(0)
	f.at(11)
	if !slices.Equal(g.removed, []string{cwd}) {
		t.Fatalf("removed %v, want [%s]", g.removed, cwd)
	}
}

// Commits made on a detached HEAD sit in no branch: a ref keeps them before
// the worktree goes, and a ref that cannot be made keeps the worktree.
func TestSweepKeepsDetachedCommitsReachableBeforeRemoving(t *testing.T) {
	f, g, cwd := abandonedFixture(t)
	g.detached = true
	f.at(0)
	f.at(11)
	if !slices.Equal(g.refs, []string{"refs/panewire/abandoned/a"}) || !slices.Equal(g.removed, []string{cwd}) {
		t.Fatalf("refs %v, removed %v", g.refs, g.removed)
	}
}

func TestSweepKeepsADetachedWorktreeWhenItsRefCannotBeMade(t *testing.T) {
	f, g, _ := abandonedFixture(t)
	g.detached, g.refErr = true, true
	f.at(0)
	f.at(11)
	if len(g.removed) != 0 {
		t.Fatalf("removed %v", g.removed)
	}
	if !f.infoHas("task worktree kept", "job=a", "reason=ref_failed_kept") {
		t.Fatalf("info lines = %q", f.info)
	}
}

// An origin branch deleted since the last fetch leaves a stale tracking ref;
// the refresh prunes it before the ref can prove the tip is on origin.
func TestSweepPrunesBeforeProvingTheTipIsOnOrigin(t *testing.T) {
	f, g, _ := abandonedFixture(t)
	g.onOrigin = true
	f.at(0)
	f.at(11)
	if len(g.fetches) != 1 || !slices.Equal(g.fetches[0], []string{"--prune", "origin"}) {
		t.Fatalf("fetches = %v, want one fetch --prune origin", g.fetches)
	}
}

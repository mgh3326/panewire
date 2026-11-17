package fleet

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// The job-owned checkout tail (hk 5091, docs/pane-sweep.md): a <job>-checkout
// the node prepared for the job alone ends with the job, whatever the task
// did; a -c worktree keeps today's rules; and the checkout.sweep command
// clears the checkouts that already lie on the hosts.

// checkoutFixture is a consult job whose receipt cwd is the detached
// <job>-checkout the node prepared, ended by its job.completed record.
func checkoutFixture(t *testing.T) (*paneSweepFixture, *sweepGitFake, string, string) {
	t.Helper()
	g := newSweepGitFake(t)
	g.detached = true
	f := newPaneSweepFixture(t)
	f.withGit(g)
	id := "consult-42-ab12cd"
	cwd := g.worktree(t, id+"-checkout")
	f.job(id, "consult", "", map[string]any{"cwd": cwd})
	putEvent(t, f.a, id, "job.completed", map[string]any{})
	return f, g, id, cwd
}

// An answered consult's checkout is removed when its pane is swept; the job
// directory stays, the patch is saved, and the detached HEAD gets a ref.
func TestSweepWorktreeRemovesAnAnsweredConsultCheckout(t *testing.T) {
	f, g, id, cwd := checkoutFixture(t)
	f.endedSweep(61, time.Hour)
	if len(g.removed) != 1 || g.removed[0] != cwd {
		t.Fatalf("checkout removals = %v, want %s; info=%v", g.removed, cwd, f.info)
	}
	if _, err := os.Stat(filepath.Join(f.a.Jobs, id)); err != nil {
		t.Fatalf("the job directory was removed with its checkout: %v", err)
	}
	if _, err := os.Stat(filepath.Join(f.a.Jobs, id, AbandonedWorktreeFile)); err != nil {
		t.Fatalf("the checkout's patch was not saved: %v", err)
	}
	if !slices.Contains(g.refs, "refs/panewire/abandoned/"+id) {
		t.Fatalf("the detached HEAD got no keeping ref: %v", g.refs)
	}
	if len(g.deleted) != 0 || len(g.forced) != 0 {
		t.Fatalf("a detached checkout's removal touched a branch: %v %v", g.deleted, g.forced)
	}
}

// A checkout another live job's receipt still names is shared: the sweep
// keeps it even though this job's own record ended.
func TestSweepWorktreeKeepsASharedConsultCheckout(t *testing.T) {
	f, g, _, cwd := checkoutFixture(t)
	f.job("other", "implementer", "7", map[string]any{"cwd": cwd})
	f.endedSweep(61, time.Hour)
	if len(g.removed) != 0 {
		t.Fatalf("a checkout a live job shares was removed: %v", g.removed)
	}
	if !f.infoHas("task worktree kept", "reason=shared_kept") {
		t.Fatalf("info lines = %q", f.info)
	}
}

// A dirty consult checkout is saved to the job directory, then removed.
func TestSweepWorktreeSavesADirtyConsultCheckout(t *testing.T) {
	f, g, id, cwd := checkoutFixture(t)
	g.dirty = true
	g.diff = "diff --git a/notes.txt b/notes.txt\n+consult finding\n"
	f.endedSweep(61, time.Hour)
	if len(g.removed) != 1 || g.removed[0] != cwd {
		t.Fatalf("checkout removals = %v, want %s; info=%v", g.removed, cwd, f.info)
	}
	body, err := os.ReadFile(filepath.Join(f.a.Jobs, id, AbandonedWorktreeFile))
	if err != nil {
		t.Fatalf("the checkout's patch was not saved: %v", err)
	}
	if !strings.Contains(string(body), "consult finding") {
		t.Fatalf("the patch does not hold the checkout's diff:\n%s", body)
	}
}

// A linked worktree a design session prepared with -c under its own name —
// here one that merely ends in -checkout — is not the job's own checkout: on
// an end that proves nothing about the task it keeps today's
// task_open_kept rule.
func TestSweepWorktreeKeepsADesignSessionCheckout(t *testing.T) {
	g := newSweepGitFake(t)
	f := newPaneSweepFixture(t)
	f.withGit(g)
	cwd := g.worktree(t, "session-checkout")
	f.job("d", "implementer", "42", map[string]any{"cwd": cwd})
	f.tasks["42"], f.category["42"] = "developing", "started"
	j, err := f.a.job("d")
	if err != nil {
		t.Fatal(err)
	}
	f.sweeper.sweepWorktree(f.t.Context(), f.a, j, paneSweepEnd{state: "completed", reason: "job_completed", byJob: true})
	if len(g.removed) != 0 {
		t.Fatalf("a design session's -c worktree was removed: %v", g.removed)
	}
	if !f.infoHas("task worktree kept", "reason=task_open_kept") {
		t.Fatalf("info lines = %q", f.info)
	}
}

// checkoutSweepFixture seeds the node-side pass's inputs: job records whose
// receipts name *-checkout worktrees, and an orphan owner lookup.
func checkoutSweepFixture(t *testing.T) (*paneSweepFixture, *sweepGitFake, *CheckoutSweep) {
	t.Helper()
	g := newSweepGitFake(t)
	g.detached = true
	f := newPaneSweepFixture(t)
	f.withGit(g)
	return f, g, &CheckoutSweep{WorkRoot: g.workRoot, OrphanOwner: func(string) string { return "" }, Info: f.sweeper.Info}
}

// A *-checkout whose job ended and whose pane is gone is removed; one whose
// job is still running or whose pane herdr still lists is kept, with the
// reason; and a directory no record or journal attributes is kept.
func TestCheckoutSweepRemovesOnlyNonLiveJobCheckouts(t *testing.T) {
	f, g, sweep := checkoutSweepFixture(t)
	ctx := context.Background()

	// ended: job.completed and the pane is gone from herdr.
	ended := "consult-9-ended1"
	endedDir := g.worktree(t, ended+"-checkout")
	endedPane := f.job(ended, "consult", "", map[string]any{"cwd": endedDir})
	putEvent(t, f.a, ended, "job.completed", map[string]any{})
	delete(f.status, endedPane)

	// live: the job never ended.
	live := "consult-9-live01"
	liveDir := g.worktree(t, live+"-checkout")
	f.job(live, "consult", "", map[string]any{"cwd": liveDir})

	// pane_open: ended, but herdr still lists its pane.
	panned := "consult-9-pane01"
	pannedDir := g.worktree(t, panned+"-checkout")
	f.job(panned, "consult", "", map[string]any{"cwd": pannedDir})
	putEvent(t, f.a, panned, "job.completed", map[string]any{})

	// no_job: nothing attributes the directory.
	g.worktree(t, "mystery-checkout")

	rows, err := sweep.Pass(ctx, f.a)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]CheckoutSweepRow{}
	for _, row := range rows {
		got[row.Path] = row
	}
	if row := got[endedDir]; row.Outcome != "removed" || row.Job != ended {
		t.Fatalf("ended checkout row = %+v", row)
	}
	if _, err := os.Stat(endedDir); !os.IsNotExist(err) {
		t.Fatalf("the ended checkout was not removed")
	}
	if row := got[liveDir]; row.Outcome != "kept" || row.Reason != sweepCheckoutJobLive {
		t.Fatalf("live checkout row = %+v", row)
	}
	if row := got[pannedDir]; row.Outcome != "kept" || row.Reason != sweepCheckoutPaneOpen {
		t.Fatalf("pane-open checkout row = %+v", row)
	}
	if row := got[filepath.Join(g.workRoot, "mystery-checkout")]; row.Outcome != "kept" || row.Reason != sweepCheckoutNoJob {
		t.Fatalf("unattributed checkout row = %+v", row)
	}
	if len(g.removed) != 1 || g.removed[0] != endedDir {
		t.Fatalf("checkout removals = %v, want only %s", g.removed, endedDir)
	}
}

// A *-checkout a spawn journal attributes to a job whose local record is gone
// is removed through the same guarded path; its patch lands in a job
// directory the record would have used.
func TestCheckoutSweepRemovesAnOrphanedCheckout(t *testing.T) {
	f, g, sweep := checkoutSweepFixture(t)
	sweep.OrphanOwner = func(dir string) string {
		if strings.HasSuffix(dir, "consult-gone-checkout") {
			return "consult-gone"
		}
		return ""
	}
	g.worktree(t, "consult-gone-checkout")
	// The journal names the job but its record dir is gone: stand one up so
	// the patch has somewhere to land, as a record that outlived its events
	// would.
	if err := os.MkdirAll(filepath.Join(f.a.Jobs, "consult-gone"), 0700); err != nil {
		t.Fatal(err)
	}
	rows, err := sweep.Pass(context.Background(), f.a)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Outcome != "removed" || rows[0].Job != "consult-gone" {
		t.Fatalf("orphan rows = %+v", rows)
	}
	if _, err := os.Stat(filepath.Join(f.a.Jobs, "consult-gone", AbandonedWorktreeFile)); err != nil {
		t.Fatalf("the orphan's patch was not saved: %v", err)
	}
}

// --dry-run reports would_remove for every removable checkout and changes
// nothing: no removal, no patch, no ref.
func TestCheckoutSweepDryRunChangesNothing(t *testing.T) {
	f, g, sweep := checkoutSweepFixture(t)
	sweep.DryRun = true
	id := "consult-9-dry001"
	dir := g.worktree(t, id+"-checkout")
	pane := f.job(id, "consult", "", map[string]any{"cwd": dir})
	putEvent(t, f.a, id, "job.completed", map[string]any{})
	delete(f.status, pane)
	g.dirty = true
	g.diff = "diff --git a/x b/x\n+work\n"

	rows, err := sweep.Pass(context.Background(), f.a)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Outcome != "would_remove" || rows[0].Reason != "discard" {
		t.Fatalf("dry-run rows = %+v", rows)
	}
	if len(g.removed) != 0 || len(g.refs) != 0 {
		t.Fatalf("a dry run mutated git state: removed=%v refs=%v", g.removed, g.refs)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("a dry run removed the checkout: %v", err)
	}
	if _, err := os.Stat(filepath.Join(f.a.Jobs, id, AbandonedWorktreeFile)); !os.IsNotExist(err) {
		t.Fatalf("a dry run wrote a patch: %v", err)
	}
}

// A *-checkout whose owning job's record points at it but whose name is not
// the job-derived one is not the job's own checkout: the pass keeps it under
// the tail's open-task rule rather than removing it on a directory-name match.
func TestCheckoutSweepKeepsANamedCheckoutThatIsNotTheJobsOwn(t *testing.T) {
	f, g, sweep := checkoutSweepFixture(t)
	j := "implementer9"
	dir := g.worktree(t, "session-checkout")
	f.job(j, "implementer", "42", map[string]any{"cwd": dir})
	putEvent(t, f.a, j, "job.completed", map[string]any{})
	delete(f.status, "p-"+j)

	rows, err := sweep.Pass(context.Background(), f.a)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Outcome != "kept" || rows[0].Reason != sweepWorktreeTaskOpen {
		t.Fatalf("rows = %+v", rows)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("a non-owned checkout was removed: %v", err)
	}
}

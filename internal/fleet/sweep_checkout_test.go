package fleet

import (
	"context"
	"errors"
	"fmt"
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
	return f, g, &CheckoutSweep{WorkRoot: g.workRoot, OrphanOwner: func(string) CheckoutOrphan { return CheckoutOrphan{} },
		OrphanRecords: f.store, OrphanPatches: filepath.Join(t.TempDir(), "orphan-checkouts"), Info: f.sweeper.Info}
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

// orphanSweep seeds one orphan the node's spawns attribute to the ended job
// consult-gone, which has no job directory left.
func orphanSweep(t *testing.T) (*paneSweepFixture, *sweepGitFake, *CheckoutSweep, *CheckoutOrphan) {
	t.Helper()
	f, g, sweep := checkoutSweepFixture(t)
	orphan := &CheckoutOrphan{Job: "consult-gone", Pane: "p-gone", Label: "consult-gone"}
	dir := g.worktree(t, "consult-gone-checkout")
	if err := os.MkdirAll(f.a.Jobs, 0700); err != nil {
		t.Fatal(err)
	}
	sweep.OrphanOwner = func(asked string) CheckoutOrphan {
		if asked == dir {
			return *orphan
		}
		return CheckoutOrphan{}
	}
	f.store.records["consult-gone"] = map[string]any{"job_id": "consult-gone", "state": "completed"}
	return f, g, sweep, orphan
}

// A *-checkout a spawn journal attributes to an ended job whose local record
// is gone is removed through the same guarded path; its patch lands under the
// orphan patch directory, and the job directory is not made again.
func TestCheckoutSweepRemovesAnOrphanedCheckout(t *testing.T) {
	f, g, sweep, _ := orphanSweep(t)
	g.diff = "diff --git a/x b/x\n+orphaned work\n"
	var held []string
	sweep.OrphanHold = func(_ context.Context, clone string) (func(), error) {
		held = append(held, "taken "+clone)
		return func() { held = append(held, fmt.Sprintf("released after %d removals", len(g.removed))) }, nil
	}
	rows, err := sweep.Pass(context.Background(), f.a)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Outcome != "removed" || rows[0].Job != "consult-gone" {
		t.Fatalf("orphan rows = %+v; info=%v", rows, f.info)
	}
	body, err := os.ReadFile(filepath.Join(sweep.OrphanPatches, "consult-gone", AbandonedWorktreeFile))
	if err != nil || !strings.Contains(string(body), "orphaned work") {
		t.Fatalf("the orphan's patch was not saved: %v\n%s", err, body)
	}
	if _, err := os.Lstat(filepath.Join(f.a.Jobs, "consult-gone")); !os.IsNotExist(err) {
		t.Fatalf("the gone job directory was made again: %v", err)
	}
	if want := []string{"taken " + g.clone, "released after 1 removals"}; !slices.Equal(held, want) {
		t.Fatalf("lock = %q, want %q", held, want)
	}
}

// An orphan whose job directory is still there keeps its patch in it.
func TestCheckoutSweepSavesAnOrphanPatchInAnExistingJobDir(t *testing.T) {
	f, _, sweep, _ := orphanSweep(t)
	if err := os.MkdirAll(filepath.Join(f.a.Jobs, "consult-gone"), 0700); err != nil {
		t.Fatal(err)
	}
	rows, err := sweep.Pass(context.Background(), f.a)
	if err != nil || len(rows) != 1 || rows[0].Outcome != "removed" {
		t.Fatalf("rows = %+v err=%v", rows, err)
	}
	if _, err := os.Stat(filepath.Join(f.a.Jobs, "consult-gone", AbandonedWorktreeFile)); err != nil {
		t.Fatalf("the orphan's patch was not saved: %v", err)
	}
}

// An orphan is kept whenever its end, its panes or its patch cannot be proven
// or saved, and when the second look right before the removal finds it in use.
func TestCheckoutSweepKeepsAnUnprovenOrphan(t *testing.T) {
	for _, test := range []struct {
		name, reason string
		arrange      func(f *paneSweepFixture, sweep *CheckoutSweep, orphan *CheckoutOrphan)
	}{
		{"no record reader", sweepCheckoutJobUnconfirmed, func(_ *paneSweepFixture, s *CheckoutSweep, _ *CheckoutOrphan) { s.OrphanRecords = nil }},
		{"hk unreadable", sweepCheckoutJobUnconfirmed, func(f *paneSweepFixture, _ *CheckoutSweep, _ *CheckoutOrphan) { f.store.getErr = errors.New("hk down") }},
		{"another job's record", sweepCheckoutJobUnconfirmed, func(f *paneSweepFixture, _ *CheckoutSweep, _ *CheckoutOrphan) {
			f.store.records["consult-gone"]["job_id"] = "consult-other"
		}},
		{"record's pane open", sweepCheckoutPaneOpen, func(f *paneSweepFixture, _ *CheckoutSweep, _ *CheckoutOrphan) {
			f.store.records["consult-gone"]["pane_id"] = "p-moved"
			f.status["p-moved"] = "idle"
		}},
		{"record's label open", sweepCheckoutPaneOpen, func(f *paneSweepFixture, _ *CheckoutSweep, _ *CheckoutOrphan) {
			f.store.records["consult-gone"]["label"] = "renamed"
			f.status["p-x"], f.names["p-x"] = "idle", "renamed"
		}},
		{"pane list unreadable", sweepCheckoutAgentsUnknown, func(f *paneSweepFixture, _ *CheckoutSweep, _ *CheckoutOrphan) { f.paneList = false }},
		{"nowhere to save the patch", sweepWorktreePatchFailed, func(_ *paneSweepFixture, s *CheckoutSweep, _ *CheckoutOrphan) { s.OrphanPatches = "" }},
		{"id that is no directory name", sweepCheckoutNoJob, func(_ *paneSweepFixture, _ *CheckoutSweep, o *CheckoutOrphan) { o.Job = "../escape" }},
		{"clone busy", sweepCheckoutCloneBusy, func(_ *paneSweepFixture, s *CheckoutSweep, _ *CheckoutOrphan) {
			s.OrphanHold = func(context.Context, string) (func(), error) { return nil, errors.New("busy") }
		}},
		{"spawn claimed during the pass", sweepCheckoutJobLive, func(_ *paneSweepFixture, s *CheckoutSweep, o *CheckoutOrphan) {
			s.OrphanHold = func(context.Context, string) (func(), error) {
				o.InFlight = true
				return func() {}, nil
			}
		}},
		{"pane opened during the pass", sweepCheckoutPaneOpen, func(f *paneSweepFixture, s *CheckoutSweep, _ *CheckoutOrphan) {
			s.OrphanHold = func(context.Context, string) (func(), error) {
				f.status["p-gone"] = "idle"
				return func() {}, nil
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, g, sweep, orphan := orphanSweep(t)
			test.arrange(f, sweep, orphan)
			rows, err := sweep.Pass(context.Background(), f.a)
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 || rows[0].Outcome != "kept" || rows[0].Reason != test.reason {
				t.Fatalf("rows = %+v, want kept %s", rows, test.reason)
			}
			if len(g.removed) != 0 {
				t.Fatalf("a kept orphan was removed: %v", g.removed)
			}
		})
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

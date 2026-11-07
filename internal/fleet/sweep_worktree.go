package fleet

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/mgh3326/panewire/internal/durable"
)

// The sweep's worktree tail (docs/pane-sweep.md): after it closes a job's pane
// it removes the worktree the node prepared for that job, and the job's local
// branch with it. A merged end removes only a clean worktree whose HEAD
// merged; a non-merged end (a canceled task, a job closed abandoned or
// superseded) saves the worktree's changes first and removes it whatever its
// state, so the next attempt can use the directory. Only a worktree of the
// clone under the node's own work root is the node's; a requester's design
// worktree, a reviewer's, and every remote branch stay with mergewake.

// AbandonedWorktreeFile is the job-directory file a non-merged end saves the
// worktree's `git status --porcelain` and `git diff` to before it removes it.
const AbandonedWorktreeFile = "abandoned-worktree.patch"

// The reason on a sweep worktree line. A removal logs sweepWorktreeRemoved; a
// keep logs why it kept the worktree.
const (
	sweepWorktreeRemoved      = "removed"
	sweepWorktreeDisabled     = "disabled_kept"
	sweepWorktreeNotNode      = "not_node_worktree"
	sweepWorktreeTaskOpen     = "task_open_kept"
	sweepWorktreePatchFailed  = "patch_failed_kept"
	sweepWorktreeShared       = "shared_kept"
	sweepWorktreeSymlink      = "symlink_kept"
	sweepWorktreeDirty        = "dirty_kept"
	sweepWorktreeFetchFailed  = "fetch_failed_kept"
	sweepWorktreeUnmerged     = "unmerged_kept"
	sweepWorktreeRemoveFailed = "remove_failed_kept"
)

// The reason on a sweep branch line of a non-merged end.
const (
	sweepBranchOnOrigin    = "on_origin"
	sweepBranchNotOnOrigin = "not_on_origin_kept"
	sweepBranchFetchFailed = "fetch_failed_kept"
)

// sweepWorktree keeps or removes the worktree of a job the sweep just closed.
// It never fails the pass: a removal that errors is a warning and a keep line,
// and the next candidate is judged as usual.
func (s *TaskPaneSweeper) sweepWorktree(ctx context.Context, a *App, j Job, end paneSweepEnd) {
	cwd := str(j.Receipt, "cwd")
	base := ""
	if cwd != "" {
		base = filepath.Base(filepath.Clean(cwd))
	}
	keep := func(reason string) {
		s.info("task worktree kept", "job", j.ID, "worktree", base, "reason", reason)
	}
	if !boolean(a.Config.Sweep, "remove_worktrees", true) {
		keep(sweepWorktreeDisabled)
		return
	}
	clone, branch, ok := s.nodeWorktree(ctx, a, cwd)
	if !ok {
		keep(sweepWorktreeNotNode)
		return
	}
	// A merged end proves the work landed; a non-merged end proves nothing and
	// the work is saved before it goes. Any other end (a closed job whose task
	// is still open) keeps the worktree for the task's owner.
	if !end.merged && !end.discard {
		keep(sweepWorktreeTaskOpen)
		return
	}
	if s.worktreeInUse(a, j, cwd) {
		keep(sweepWorktreeShared)
		return
	}
	if sweepSymlink(cwd) {
		keep(sweepWorktreeSymlink)
		return
	}
	if end.discard && !end.merged {
		s.discardWorktree(ctx, a, j, end, clone, branch, cwd, base)
		return
	}
	if r := a.call(ctx, "git", "-C", cwd, "status", "--porcelain"); r.Code != 0 || r.Out != "" {
		keep(sweepWorktreeDirty)
		return
	}
	// origin/* refs are a cache: refresh them in the clone before they prove
	// the HEAD merged. A remote that cannot be read keeps the worktree.
	if err := a.must(ctx, "git", "-C", clone, "fetch", "origin"); err != nil {
		a.warn("worktree retained: %v", err)
		keep(sweepWorktreeFetchFailed)
		return
	}
	if err := a.must(ctx, "git", "-C", cwd, "merge-base", "--is-ancestor", "HEAD", "refs/remotes/origin/main"); err != nil {
		keep(sweepWorktreeUnmerged)
		return
	}
	if err := a.must(ctx, "git", "-C", clone, "worktree", "remove", cwd); err != nil {
		a.warn("worktree retained: %v", err)
		keep(sweepWorktreeRemoveFailed)
		return
	}
	s.info("task worktree removed", "job", j.ID, "worktree", base, "reason", sweepWorktreeRemoved)
	// -d, never -D: git refuses a branch that never merged. A branch another
	// worktree still holds is left for that worktree.
	if branch == "" || s.branchHeld(ctx, a, clone, branch) {
		return
	}
	if err := a.must(ctx, "git", "-C", clone, "branch", "-d", branch); err != nil {
		a.warn("branch retained: %v", err)
	}
}

// discardWorktree removes the worktree of a non-merged end. The worktree's
// status and diff are saved to the job directory first, and a patch that
// cannot be saved keeps the worktree. The local branch goes (-D) only when its
// tip is already on origin, so no commit exists nowhere else.
func (s *TaskPaneSweeper) discardWorktree(ctx context.Context, a *App, j Job, end paneSweepEnd, clone, branch, cwd, base string) {
	keep := func(reason string) {
		s.info("task worktree kept", "job", j.ID, "worktree", base, "reason", reason)
	}
	status := a.call(ctx, "git", "-C", cwd, "status", "--porcelain")
	diff := a.call(ctx, "git", "-C", cwd, "diff", "--binary", "HEAD")
	if status.Code != 0 || diff.Code != 0 {
		keep(sweepWorktreePatchFailed)
		return
	}
	if err := saveAbandonedWorktree(a, j.ID, base, end.reason, status.Out, diff.Out); err != nil {
		a.warn("worktree retained: %v", err)
		keep(sweepWorktreePatchFailed)
		return
	}
	if err := a.must(ctx, "git", "-C", clone, "worktree", "remove", "--force", cwd); err != nil {
		a.warn("worktree retained: %v", err)
		keep(sweepWorktreeRemoveFailed)
		return
	}
	s.info("task worktree removed", "job", j.ID, "worktree", base, "reason", end.reason, "patch", AbandonedWorktreeFile)
	if branch == "" || s.branchHeld(ctx, a, clone, branch) {
		return
	}
	// origin/* refs are a cache: refresh them before they prove the tip is
	// there. A tip that cannot be proven on origin keeps the branch.
	keepBranch := func(reason string) {
		s.info("task branch kept", "job", j.ID, "branch", branch, "reason", reason)
	}
	if err := a.must(ctx, "git", "-C", clone, "fetch", "origin"); err != nil {
		a.warn("branch retained: %v", err)
		keepBranch(sweepBranchFetchFailed)
		return
	}
	if r := a.call(ctx, "git", "-C", clone, "branch", "-r", "--contains", branch, "--list", "origin/*"); r.Code != 0 || strings.TrimSpace(r.Out) == "" {
		keepBranch(sweepBranchNotOnOrigin)
		return
	}
	if err := a.must(ctx, "git", "-C", clone, "branch", "-D", branch); err != nil {
		a.warn("branch retained: %v", err)
		return
	}
	s.info("task branch removed", "job", j.ID, "branch", branch, "reason", sweepBranchOnOrigin)
}

// saveAbandonedWorktree writes the patch file through an os.Root on the job
// directory, with its mode set before any byte is written.
func saveAbandonedWorktree(a *App, jobID, base, reason, status, diff string) error {
	root, err := os.OpenRoot(filepath.Join(a.Jobs, jobID))
	if err != nil {
		return err
	}
	defer root.Close()
	contents := "# panewire abandoned worktree " + base + " of job " + jobID + " (" + reason + ")\n" +
		"# git status --porcelain\n" + status + "\n# git diff HEAD\n" + diff
	return durable.ReplaceFileAtomicallyInRoot(root, AbandonedWorktreeFile, []byte(contents), 0600)
}

// nodeWorktree reports the clone and branch of a receipt cwd the node
// prepared: a linked worktree of a clone that is a direct child of the node's
// work root. Anything else — the clone itself, a worktree of another clone, a
// path outside the work root — is not the node's to remove.
func (s *TaskPaneSweeper) nodeWorktree(ctx context.Context, a *App, cwd string) (clone, branch string, ok bool) {
	if cwd == "" || s.WorkRoot == "" || !sweepWithin(s.WorkRoot, cwd) {
		return "", "", false
	}
	r := a.call(ctx, "git", "-C", cwd, "rev-parse", "--path-format=absolute", "--git-dir", "--git-common-dir")
	if r.Code != 0 {
		return "", "", false
	}
	lines := strings.Split(strings.TrimSuffix(r.Out, "\n"), "\n")
	if len(lines) != 2 || !filepath.IsAbs(lines[0]) || !filepath.IsAbs(lines[1]) {
		return "", "", false
	}
	gitDir, common := filepath.Clean(lines[0]), filepath.Clean(lines[1])
	// A linked worktree keeps its git dir under <clone>/.git/worktrees; the
	// canonical checkout's git dir is the common dir itself.
	if gitDir == common || filepath.Base(common) != ".git" {
		return "", "", false
	}
	clone = filepath.Dir(common)
	if !sweepSamePath(s.WorkRoot, filepath.Dir(clone)) {
		return "", "", false
	}
	if b := a.call(ctx, "git", "-C", cwd, "branch", "--show-current"); b.Code == 0 {
		branch = strings.TrimSpace(b.Out)
	}
	return clone, branch, true
}

// worktreeInUse reports that another live job's receipt cwd is the same path.
// Two live jobs can share one worktree (they share `panewire.cmd-relay`), so
// neither may remove it under the other. "Live" is panewire's own sense: not
// terminal and not reaped. An unreadable job list counts as in use, so no
// path is removed blind.
func (s *TaskPaneSweeper) worktreeInUse(a *App, j Job, cwd string) bool {
	entries, err := os.ReadDir(a.Jobs)
	if err != nil {
		return true
	}
	for _, entry := range entries {
		if !entry.IsDir() || !validID(entry.Name()) || entry.Name() == j.ID {
			continue
		}
		other, err := a.job(entry.Name())
		if err != nil || other.Terminal || other.Reaped {
			continue
		}
		if sweepSamePath(str(other.Receipt, "cwd"), cwd) {
			return true
		}
	}
	return false
}

// branchHeld reports that a worktree still holds the branch. A list that
// cannot be read is treated as held, so the branch is never deleted blind.
func (s *TaskPaneSweeper) branchHeld(ctx context.Context, a *App, clone, branch string) bool {
	r := a.call(ctx, "git", "-C", clone, "worktree", "list", "--porcelain")
	if r.Code != 0 {
		return true
	}
	for _, line := range strings.Split(r.Out, "\n") {
		if strings.TrimSpace(line) == "branch refs/heads/"+branch {
			return true
		}
	}
	return false
}

// sweepSymlink reports that the worktree path is itself a symlink. Ancestors
// are already covered by sweepWithin's resolution of the work root.
func sweepSymlink(path string) bool {
	info, err := os.Lstat(path)
	return err == nil && info.Mode()&os.ModeSymlink != 0
}

// sweepWithin reports that path is strictly under root, with symlinks resolved
// when the filesystem allows it.
func sweepWithin(root, path string) bool {
	rel, err := filepath.Rel(sweepResolved(root), sweepResolved(path))
	return err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// sweepSamePath compares two paths the way git prints them: symlinks resolved
// when possible, cleaned forms otherwise.
func sweepSamePath(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	return sweepResolved(a) == sweepResolved(b)
}

func sweepResolved(path string) string {
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		return filepath.Clean(resolved)
	}
	return filepath.Clean(path)
}

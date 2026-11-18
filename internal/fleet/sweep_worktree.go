package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/mgh3326/panewire/internal/durable"
	"github.com/mgh3326/panewire/internal/harnessstate"
)

// The sweep's worktree tail (docs/pane-sweep.md): after it closes a job's pane
// it removes the worktree the node prepared for that job, and the job's local
// branch with it. A merged end removes only a clean worktree whose HEAD
// merged; a non-merged end (a canceled task, a job closed abandoned or
// superseded) saves the worktree's changes first and removes it whatever its
// state, so the next attempt can use the directory. A checkout the node
// prepared for the job alone — the <job>-checkout a spec spawn, a consult or
// a canonical checkout gets — belongs to no task and ends with the job,
// whatever the task did (hk 5091). Only a worktree of the
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
	sweepWorktreeRefFailed    = "ref_failed_kept"
	sweepWorktreeShared       = "shared_kept"
	sweepWorktreeSymlink      = "symlink_kept"
	sweepWorktreeDirty        = "dirty_kept"
	sweepWorktreeFetchFailed  = "fetch_failed_kept"
	sweepWorktreeUnmerged     = "unmerged_kept"
	sweepWorktreeRemoveFailed = "remove_failed_kept"
	// sweepWorktreeHandoverKept: a hub hand-over could not push the old
	// worktree's snapshot (or a submodule held work it does not carry), so
	// the successor's note names this worktree (hk 4633).
	sweepWorktreeHandoverKept = "handover_kept"
)

// handoverKept reports whether a hand-over keeps cwd (HandoverKeeps over
// the app's jobs directory, the candidate job's own mark first).
func handoverKept(a *App, job, cwd string) bool {
	if _, err := os.Lstat(filepath.Join(a.Jobs, job, HandoverKeepFile)); !errors.Is(err, os.ErrNotExist) {
		return true
	}
	return HandoverKeeps(a.Jobs, cwd)
}

// HandoverKeeps reports whether any job's HandoverKeepFile under jobs names
// path, a directory inside it, or one containing it (jobs may share a
// worktree; a job's cwd may be an execution lane inside it). Entries that
// are not directories (the jobs root holds lock and flag files) are skipped.
// An unreadable jobs directory or mark keeps the worktree.
func HandoverKeeps(jobs, path string) bool {
	entries, err := os.ReadDir(jobs)
	if err != nil {
		return true
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		body, err := os.ReadFile(filepath.Join(jobs, entry.Name(), HandoverKeepFile))
		top := strings.TrimSpace(string(body))
		switch {
		case errors.Is(err, os.ErrNotExist):
		case err != nil:
			return true
		case path == "" || sweepSamePath(top, path) || sweepWithin(top, path) || sweepWithin(path, top):
			return true
		}
	}
	return false
}

// HandoverKeepFile, in a job's directory, keeps that job's worktree from
// every sweep: the hub hand-over's successor was told to copy work from it
// (hk 4633, docs/quota-stop.md "An exhausted pool").
const HandoverKeepFile = "handover-keep"

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
	clone, branch, owned, reason := s.worktreePlan(ctx, a, j, cwd, end)
	if reason != "" {
		keep(reason)
		return
	}
	if reason := s.removeWorktree(ctx, a, j, end, clone, branch, cwd, base, owned); reason != "" {
		keep(reason)
	}
}

// worktreePlan runs every keep check the worktree tail shares with the
// checkout sweep command, in the order the tail has always applied them. It
// returns the clone and branch nodeWorktree proved, whether the job owns the
// checkout (sweepJobCheckout), and "" when nothing keeps the worktree — or
// the keep reason.
func (s *TaskPaneSweeper) worktreePlan(ctx context.Context, a *App, j Job, cwd string, end paneSweepEnd) (clone, branch string, owned bool, reason string) {
	if !boolean(a.Config.Sweep, "remove_worktrees", true) {
		return "", "", false, sweepWorktreeDisabled
	}
	if handoverKept(a, j.ID, cwd) {
		return "", "", false, sweepWorktreeHandoverKept
	}
	clone, branch, ok := s.nodeWorktree(ctx, a, cwd)
	if !ok {
		return "", "", false, sweepWorktreeNotNode
	}
	owned = sweepJobCheckout(j, cwd)
	// A merged end proves the work landed; a non-merged end proves nothing and
	// the work is saved before it goes. A checkout the node prepared for this
	// job alone (sweepJobCheckout) belongs to no task and ends with the job.
	// Any other end (a closed job whose task is still open) keeps the
	// worktree for the task's owner.
	if !owned && !end.merged && !end.discard {
		return "", "", false, sweepWorktreeTaskOpen
	}
	if s.worktreeInUse(ctx, a, j, cwd) {
		return "", "", false, sweepWorktreeShared
	}
	if sweepSymlink(cwd) {
		return "", "", false, sweepWorktreeSymlink
	}
	// Harness state excluded by prepare must remain safe from either removal path.
	args := []string{"-C", cwd, "ls-files", "--others", "--ignored", "-z"}
	for _, pattern := range strings.Split(strings.TrimSuffix(harnessstate.Excludes, "\n"), "\n") {
		args = append(args, "--exclude="+pattern)
	}
	if r := a.call(ctx, "git", args...); r.Code != 0 || r.Out != "" {
		return "", "", false, sweepWorktreeDirty
	}
	return clone, branch, owned, ""
}

// removeWorktree removes the worktree worktreePlan cleared and returns the
// keep reason when it stays. A job-owned checkout or a non-merged end goes
// through discardWorktree, which saves the work first; any other cleared end
// is the merged path.
func (s *TaskPaneSweeper) removeWorktree(ctx context.Context, a *App, j Job, end paneSweepEnd, clone, branch, cwd, base string, owned bool) string {
	if owned || (end.discard && !end.merged) {
		return s.discardWorktree(ctx, a, j, end, clone, branch, cwd, base)
	}
	if r := a.call(ctx, "git", "-C", cwd, "status", "--porcelain"); r.Code != 0 || r.Out != "" {
		return sweepWorktreeDirty
	}
	// origin/* refs are a cache: refresh them in the clone before they prove
	// the HEAD merged. A remote that cannot be read keeps the worktree.
	if err := a.must(ctx, "git", "-C", clone, "fetch", "origin"); err != nil {
		a.warn("worktree retained: %v", err)
		return sweepWorktreeFetchFailed
	}
	if err := a.must(ctx, "git", "-C", cwd, "merge-base", "--is-ancestor", "HEAD", "refs/remotes/origin/main"); err != nil {
		return sweepWorktreeUnmerged
	}
	if err := a.must(ctx, "git", "-C", clone, "worktree", "remove", cwd); err != nil {
		a.warn("worktree retained: %v", err)
		return sweepWorktreeRemoveFailed
	}
	s.info("task worktree removed", "job", j.ID, "worktree", base, "reason", sweepWorktreeRemoved)
	// -d, never -D: git refuses a branch that never merged. A branch another
	// worktree still holds is left for that worktree.
	if branch == "" || s.branchHeld(ctx, a, clone, branch) {
		return ""
	}
	if err := a.must(ctx, "git", "-C", clone, "branch", "-d", branch); err != nil {
		a.warn("branch retained: %v", err)
	}
	return ""
}

// sweepJobCheckout reports that cwd is the checkout the node prepared for
// this job alone: the job's receipt names it and its directory carries the
// job-derived name a spec spawn or a canonical checkout gets
// (checkoutWorktreeName). A -c worktree keeps the requester's own directory
// name, so it matches only when it was deliberately named <job>-checkout.
func sweepJobCheckout(j Job, cwd string) bool {
	return cwd != "" && filepath.Base(filepath.Clean(cwd)) == checkoutWorktreeName(j.ID)
}

// discardWorktree removes the worktree of a job-owned checkout or a
// non-merged end, returning the keep reason when it stays. The worktree's
// status, its diff and the contents of its untracked files are saved to the job
// directory first, and a patch that cannot be saved keeps the worktree. The local branch goes (-D) only when its
// tip is already on origin, so no commit exists nowhere else.
func (s *TaskPaneSweeper) discardWorktree(ctx context.Context, a *App, j Job, end paneSweepEnd, clone, branch, cwd, base string) string {
	status := a.call(ctx, "git", "-C", cwd, "status", "--porcelain")
	diff := a.call(ctx, "git", "-C", cwd, "diff", "--binary", "HEAD")
	if status.Code != 0 || diff.Code != 0 {
		return sweepWorktreePatchFailed
	}
	// git diff HEAD leaves untracked files out and the removal below deletes
	// them, so each goes into the patch as a new-file diff. One that cannot be
	// captured keeps the worktree. Ignored files are build output, not work.
	untracked, ok := sweepUntrackedDiff(ctx, a, cwd)
	if !ok {
		return sweepWorktreePatchFailed
	}
	if err := saveAbandonedWorktree(a, j.ID, base, end.reason, status.Out, diff.Out+untracked); err != nil {
		a.warn("worktree retained: %v", err)
		return sweepWorktreePatchFailed
	}
	// Commits made on a detached HEAD are in no branch, and the diff above holds
	// only the uncommitted changes. A ref under refs/panewire keeps them from
	// garbage collection; one that cannot be made keeps the worktree.
	if branch == "" {
		if err := a.must(ctx, "git", "-C", cwd, "update-ref", "refs/panewire/abandoned/"+j.ID, "HEAD"); err != nil {
			a.warn("worktree retained: %v", err)
			return sweepWorktreeRefFailed
		}
	}
	if err := a.must(ctx, "git", "-C", clone, "worktree", "remove", "--force", cwd); err != nil {
		a.warn("worktree retained: %v", err)
		return sweepWorktreeRemoveFailed
	}
	s.info("task worktree removed", "job", j.ID, "worktree", base, "reason", end.reason, "patch", AbandonedWorktreeFile)
	if branch == "" || s.branchHeld(ctx, a, clone, branch) {
		return ""
	}
	// origin/* refs are a cache: refresh them, pruning the ones origin no
	// longer has, before they prove the tip is there. A tip that cannot be
	// proven on origin keeps the branch.
	keepBranch := func(reason string) {
		s.info("task branch kept", "job", j.ID, "branch", branch, "reason", reason)
	}
	if err := a.must(ctx, "git", "-C", clone, "fetch", "--prune", "origin"); err != nil {
		a.warn("branch retained: %v", err)
		keepBranch(sweepBranchFetchFailed)
		return ""
	}
	if r := a.call(ctx, "git", "-C", clone, "branch", "-r", "--contains", branch, "--list", "origin/*"); r.Code != 0 || strings.TrimSpace(r.Out) == "" {
		keepBranch(sweepBranchNotOnOrigin)
		return ""
	}
	if err := a.must(ctx, "git", "-C", clone, "branch", "-D", branch); err != nil {
		a.warn("branch retained: %v", err)
		return ""
	}
	s.info("task branch removed", "job", j.ID, "branch", branch, "reason", sweepBranchOnOrigin)
	return ""
}

// sweepUntrackedDiff renders every untracked, non-ignored file of the worktree
// as a diff against /dev/null. git diff --no-index exits 1 when the files
// differ, which is the normal answer here. An entry that is not a regular file
// or a symlink fails the capture, so the worktree stays.
func sweepUntrackedDiff(ctx context.Context, a *App, cwd string) (string, bool) {
	list := a.call(ctx, "git", "-C", cwd, "ls-files", "--others", "--exclude-standard", "-z")
	if list.Code != 0 {
		return "", false
	}
	var out strings.Builder
	for _, name := range strings.Split(list.Out, "\x00") {
		if name == "" {
			continue
		}
		// A named pipe or device would block git's read: only a regular file
		// or a symlink (read as a link, never followed) can be captured.
		info, err := os.Lstat(filepath.Join(cwd, name))
		if err != nil || (!info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0) {
			return "", false
		}
		r := a.call(ctx, "git", "-C", cwd, "diff", "--binary", "--no-index", "--", "/dev/null", name)
		if r.Code != 0 && r.Code != 1 {
			return "", false
		}
		out.WriteString(r.Out)
	}
	return out.String(), true
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
		"# git status --porcelain\n" + status + "\n# git diff HEAD, then each untracked file as a new file\n" + diff
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

// worktreeInUse reports that another job still uses the receipt cwd. Two live
// jobs can share one worktree (they share `panewire.cmd-relay`), so neither may
// remove it under the other. Another job uses it while it is not terminal, and
// while it is terminal but not reaped and herdr still lists its pane: a closed
// job's pane may still be at work in the directory. A job whose history cannot
// be read counts as using it when its event files mention the path or cannot be
// read at all, and so does an unreadable job list or herdr, so no path is
// removed blind.
func (s *TaskPaneSweeper) worktreeInUse(ctx context.Context, a *App, j Job, cwd string) bool {
	entries, err := os.ReadDir(a.Jobs)
	if err != nil {
		return true
	}
	var agents []Agent
	agentsRead := false
	for _, entry := range entries {
		if !entry.IsDir() || !validID(entry.Name()) || entry.Name() == j.ID {
			continue
		}
		other, err := a.job(entry.Name())
		if err != nil {
			if eventsMention(filepath.Join(a.Jobs, entry.Name(), "events"), cwd) {
				return true
			}
			continue
		}
		if other.Reaped || !sweepSamePath(str(other.Receipt, "cwd"), cwd) {
			continue
		}
		if !other.Terminal {
			return true
		}
		if other.Pane == "" {
			continue
		}
		if !agentsRead {
			if agents, err = a.agents(ctx); err != nil {
				return true
			}
			agentsRead = true
		}
		if slices.ContainsFunc(agents, func(ag Agent) bool { return ag.Pane == other.Pane }) {
			return true
		}
	}
	return false
}

// eventsMention reports that a job's event files cannot be shown not to name
// the path: any file in dir that contains it, or any read that fails. A job
// with no events directory names nothing.
func eventsMention(dir, path string) bool {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	if err != nil {
		return true
	}
	quoted, err := json.Marshal(path)
	if err != nil {
		return true
	}
	needle := strings.Trim(string(quoted), `"`)
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		contents, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil || strings.Contains(string(contents), needle) {
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

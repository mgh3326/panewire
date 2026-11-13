package fleet

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// sweepGitFake answers the git calls the sweep's worktree tail makes. The fs
// it describes is a temp work root holding the clone and the job worktrees;
// the clone path a linked worktree reports is the one prepare would create.
type sweepGitFake struct {
	workRoot  string
	clone     string
	dirty     bool
	unmerged  bool
	fetchErr  bool
	removeErr bool
	held      map[string]bool // branch name → another worktree holds it
	removed   []string
	deleted   []string
	// A non-merged end: what the worktree holds, whether its branch tip is on
	// origin, the branches deleted with -D, and a hook run as the worktree is
	// removed.
	diff string
	// untracked names the files git diff HEAD does not show; ls-files lists
	// them and diff --no-index renders each as "+<name> contents".
	untracked []string
	lsFailed  bool
	// detached: the worktree is on a detached HEAD. refs are the update-ref
	// calls made, refErr fails them, fetches records each fetch's arguments.
	detached bool
	refs     []string
	refErr   bool
	fetches  [][]string
	onOrigin bool
	forced   []string
	onRemove func(path string)
	// head is the worktree's HEAD (headErr makes it unreadable); originTip is
	// what refs/remotes/origin/<branch> resolves to ("" means it does not
	// exist).
	head      string
	headErr   bool
	originTip string
	// remoteTip is the branch tip ls-remote reads from origin itself
	// (originTip when empty, "none" when origin has no such branch);
	// lsRemoteErr makes origin unreadable.
	remoteTip   string
	lsRemoteErr bool
}

func newSweepGitFake(t *testing.T) *sweepGitFake {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	g := &sweepGitFake{workRoot: root, clone: filepath.Join(root, "panewire"), held: map[string]bool{}, head: paneSweepTestHead}
	if err := os.MkdirAll(filepath.Join(g.clone, ".git"), 0700); err != nil {
		t.Fatal(err)
	}
	return g
}

// worktree makes a linked worktree directory under the work root and returns
// its path. The directory name is the job id so each case gets its own.
func (g *sweepGitFake) worktree(t *testing.T, name string) string {
	t.Helper()
	dir := filepath.Join(g.workRoot, name)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func (g *sweepGitFake) do(c Command) Result {
	args := c.Args
	switch {
	case len(args) >= 4 && args[2] == "rev-parse" && args[3] == "HEAD":
		if g.headErr {
			return Result{Code: 128, Err: "fatal: not a git repository"}
		}
		return Result{Out: g.head + "\n"}
	case len(args) >= 5 && args[2] == "rev-parse" && args[3] == "--verify":
		if g.originTip == "" {
			return Result{Code: 1}
		}
		return Result{Out: g.originTip + "\n"}
	case len(args) >= 5 && args[2] == "rev-parse" && args[3] == "--path-format=absolute":
		cwd := args[1]
		gitDir := filepath.Join(g.clone, ".git", "worktrees", filepath.Base(cwd))
		return Result{Out: gitDir + "\n" + filepath.Join(g.clone, ".git") + "\n"}
	case len(args) >= 3 && args[2] == "ls-remote":
		if g.lsRemoteErr {
			return Result{Code: 128, Err: "could not read from remote repository"}
		}
		tip := g.remoteTip
		if tip == "" {
			tip = g.originTip
		}
		if tip == "" || tip == "none" {
			return Result{Code: 2}
		}
		return Result{Out: tip + "\t" + args[len(args)-1] + "\n"}
	case len(args) >= 4 && args[2] == "branch" && args[3] == "--show-current":
		if g.detached {
			return Result{}
		}
		return Result{Out: "wk/job\n"}
	case len(args) >= 5 && args[2] == "update-ref":
		if g.refErr {
			return Result{Code: 1, Err: "cannot lock ref"}
		}
		g.refs = append(g.refs, args[3])
		return Result{}
	case len(args) >= 4 && args[2] == "status":
		if g.dirty {
			return Result{Out: " M file\n"}
		}
		return Result{}
	case len(args) >= 4 && args[2] == "fetch":
		g.fetches = append(g.fetches, args[3:])
		if g.fetchErr {
			return Result{Code: 1, Err: "no remote"}
		}
		return Result{}
	case len(args) >= 4 && args[2] == "merge-base":
		if g.unmerged {
			return Result{Code: 1}
		}
		return Result{}
	case len(args) >= 4 && args[2] == "ls-files" && slices.Contains(args, "--ignored"):
		return Result{}
	case len(args) >= 4 && args[2] == "ls-files":
		if g.lsFailed {
			return Result{Code: 1, Err: "ls-files failed"}
		}
		return Result{Out: strings.Join(g.untracked, "\x00") + "\x00"}
	case len(args) >= 4 && args[2] == "diff" && slices.Contains(args, "--no-index"):
		return Result{Code: 1, Out: "diff --git a/" + args[len(args)-1] + " b/" + args[len(args)-1] + "\n+contents of " + args[len(args)-1] + "\n"}
	case len(args) >= 4 && args[2] == "diff":
		return Result{Out: g.diff}
	case len(args) >= 4 && args[2] == "branch" && args[3] == "-r":
		if g.onOrigin {
			return Result{Out: "  origin/wk/job\n"}
		}
		return Result{}
	case len(args) >= 5 && args[2] == "branch" && args[3] == "-D":
		g.forced = append(g.forced, args[4])
		return Result{}
	case len(args) >= 5 && args[2] == "worktree" && args[3] == "remove":
		path := args[len(args)-1]
		if g.removeErr {
			return Result{Code: 1, Err: "worktree locked"}
		}
		if g.onRemove != nil {
			g.onRemove(path)
		}
		g.removed = append(g.removed, path)
		_ = os.RemoveAll(path)
		return Result{}
	case len(args) >= 4 && args[2] == "worktree" && args[3] == "list":
		var lines []string
		for branch, held := range g.held {
			if held {
				lines = append(lines, "branch refs/heads/"+branch)
			}
		}
		if len(lines) == 0 {
			return Result{}
		}
		return Result{Out: strings.Join(lines, "\n") + "\n"}
	case len(args) >= 5 && args[2] == "branch" && args[3] == "-d":
		g.deleted = append(g.deleted, args[4])
		return Result{}
	}
	return Result{Code: 1, Err: "unsupported git call: " + strings.Join(args, " ")}
}

// withGit routes the fixture's git calls to g and gives the sweeper their
// work root.
func (f *paneSweepFixture) withGit(g *sweepGitFake) {
	base := f.runner.Do
	f.runner.Do = func(c Command) Result {
		if c.Name == "git" {
			return g.do(c)
		}
		return base(c)
	}
	f.sweeper.WorkRoot = g.workRoot
}

// sweptJob seeds a job bound to task whose receipt cwd is cwd.
func (f *paneSweepFixture) sweptJob(id, task, cwd string) string {
	return f.job(id, "implementer", task, map[string]any{"cwd": cwd})
}

func TestSweepRemovesANodeWorktreeAndItsBranch(t *testing.T) {
	g := newSweepGitFake(t)
	f := newPaneSweepFixture(t)
	f.withGit(g)
	cwd := g.worktree(t, "job-a")
	f.sweptJob("a", "42", cwd)
	f.tasks["42"] = "merged"
	f.at(0)
	f.at(11)
	if !slices.Equal(g.removed, []string{cwd}) {
		t.Fatalf("removed worktrees = %v, want [%s]", g.removed, cwd)
	}
	if !slices.Equal(g.deleted, []string{"wk/job"}) {
		t.Fatalf("deleted branches = %v, want [wk/job]", g.deleted)
	}
	if !f.infoHas("task worktree removed", "job=a", "worktree=job-a", "reason=removed") {
		t.Fatalf("info lines = %q", f.info)
	}
}

func TestSweepKeepsADirtyWorktree(t *testing.T) {
	g := newSweepGitFake(t)
	g.dirty = true
	f := newPaneSweepFixture(t)
	f.withGit(g)
	f.sweptJob("a", "42", g.worktree(t, "job-a"))
	f.tasks["42"] = "merged"
	f.at(0)
	f.at(11)
	if len(g.removed) != 0 || len(g.deleted) != 0 {
		t.Fatalf("removed %v, deleted %v", g.removed, g.deleted)
	}
	if !f.infoHas("task worktree kept", "job=a", "reason=dirty_kept") {
		t.Fatalf("info lines = %q", f.info)
	}
}

func TestSweepKeepsAWorktreeWhoseHeadIsNotMerged(t *testing.T) {
	g := newSweepGitFake(t)
	g.unmerged = true
	f := newPaneSweepFixture(t)
	f.withGit(g)
	f.sweptJob("a", "42", g.worktree(t, "job-a"))
	f.tasks["42"] = "merged"
	f.at(0)
	f.at(11)
	if len(g.removed) != 0 {
		t.Fatalf("removed %v", g.removed)
	}
	if !f.infoHas("task worktree kept", "job=a", "reason=unmerged_kept") {
		t.Fatalf("info lines = %q", f.info)
	}
}

// A worktree outside the node's work root is not the node's: a requester's
// design worktree looks like any other path to the sweep.
func TestSweepKeepsAWorktreeOutsideTheWorkRoot(t *testing.T) {
	g := newSweepGitFake(t)
	f := newPaneSweepFixture(t)
	f.withGit(g)
	outside, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// The fake still answers as if it were a linked worktree of the clone, so
	// only the work-root rule keeps it.
	f.sweptJob("a", "42", outside)
	f.tasks["42"] = "merged"
	f.at(0)
	f.at(11)
	if len(g.removed) != 0 || len(g.deleted) != 0 {
		t.Fatalf("removed %v, deleted %v", g.removed, g.deleted)
	}
	if !f.infoHas("task worktree kept", "job=a", "reason=not_node_worktree") {
		t.Fatalf("info lines = %q", f.info)
	}
}

func TestSweepKeepsAWorktreeAnotherLiveJobUses(t *testing.T) {
	g := newSweepGitFake(t)
	f := newPaneSweepFixture(t)
	f.withGit(g)
	cwd := g.worktree(t, "job-a")
	f.sweptJob("a", "42", cwd)
	// A second live job on the same path: its task is still being worked, so
	// the sweep never closes it in this pass.
	f.sweptJob("b", "43", cwd)
	f.tasks["42"] = "merged"
	f.tasks["43"] = "developing"
	f.at(0)
	f.at(11)
	if len(g.removed) != 0 || len(g.deleted) != 0 {
		t.Fatalf("removed %v, deleted %v", g.removed, g.deleted)
	}
	if !f.infoHas("task worktree kept", "job=a", "reason=shared_kept") {
		t.Fatalf("info lines = %q", f.info)
	}
}

func TestSweepKeepsWorktreesWhenTheSwitchIsOff(t *testing.T) {
	g := newSweepGitFake(t)
	f := newPaneSweepFixture(t)
	f.withGit(g)
	f.a.Config.Sweep = map[string]any{"remove_worktrees": false}
	f.sweptJob("a", "42", g.worktree(t, "job-a"))
	f.tasks["42"] = "merged"
	f.at(0)
	f.at(11)
	if len(g.removed) != 0 || len(g.deleted) != 0 {
		t.Fatalf("removed %v, deleted %v", g.removed, g.deleted)
	}
	if !f.infoHas("task worktree kept", "job=a", "reason=disabled_kept") {
		t.Fatalf("info lines = %q", f.info)
	}
}

// A removal that fails warns and keeps the worktree; the pass goes on to the
// next swept job and returns no error.
func TestSweepContinuesAfterAWorktreeRemovalFails(t *testing.T) {
	g := newSweepGitFake(t)
	g.removeErr = true
	f := newPaneSweepFixture(t)
	f.withGit(g)
	first := g.worktree(t, "job-a")
	second := g.worktree(t, "job-b")
	f.sweptJob("a", "42", first)
	f.sweptJob("b", "43", second)
	f.tasks["42"] = "merged"
	f.tasks["43"] = "merged"
	f.at(0)
	f.at(11)
	if !slices.Equal(f.closed(), []string{"p-a", "p-b"}) {
		t.Fatalf("closed panes = %v", f.closed())
	}
	if len(g.removed) != 0 {
		t.Fatalf("removed %v despite the failure", g.removed)
	}
	if !f.infoHas("task worktree kept", "job=a", "reason=remove_failed_kept") || !f.infoHas("task worktree kept", "job=b", "reason=remove_failed_kept") {
		t.Fatalf("info lines = %q", f.info)
	}
	if out := f.a.Err.(*bytes.Buffer).String(); !strings.Contains(out, "worktree retained") {
		t.Fatalf("no warning recorded in %q", out)
	}
}

// The branch is left alone while another worktree still holds it.
func TestSweepKeepsABranchAnotherWorktreeHolds(t *testing.T) {
	g := newSweepGitFake(t)
	g.held["wk/job"] = true
	f := newPaneSweepFixture(t)
	f.withGit(g)
	f.sweptJob("a", "42", g.worktree(t, "job-a"))
	f.tasks["42"] = "merged"
	f.at(0)
	f.at(11)
	if len(g.removed) != 1 {
		t.Fatalf("removed = %v", g.removed)
	}
	if len(g.deleted) != 0 {
		t.Fatalf("deleted %v although another worktree holds the branch", g.deleted)
	}
}

func TestSweepKeepsAWorktreeReachedThroughASymlink(t *testing.T) {
	g := newSweepGitFake(t)
	f := newPaneSweepFixture(t)
	f.withGit(g)
	real := g.worktree(t, "job-a")
	link := filepath.Join(g.workRoot, "link-a")
	if err := os.Symlink(real, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	f.sweptJob("a", "42", link)
	f.tasks["42"] = "merged"
	f.at(0)
	f.at(11)
	if len(g.removed) != 0 {
		t.Fatalf("removed %v through a symlink", g.removed)
	}
	if !f.infoHas("task worktree kept", "job=a", "reason=symlink_kept") {
		t.Fatalf("info lines = %q", f.info)
	}
}

func TestSweepKeepsAWorktreeWhenTheFetchFails(t *testing.T) {
	g := newSweepGitFake(t)
	g.fetchErr = true
	f := newPaneSweepFixture(t)
	f.withGit(g)
	f.sweptJob("a", "42", g.worktree(t, "job-a"))
	f.tasks["42"] = "merged"
	f.at(0)
	f.at(11)
	if len(g.removed) != 0 {
		t.Fatalf("removed %v", g.removed)
	}
	if !f.infoHas("task worktree kept", "job=a", "reason=fetch_failed_kept") {
		t.Fatalf("info lines = %q", f.info)
	}
}

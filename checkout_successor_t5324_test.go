package panewire

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

// A snapshot hand-over's successor (hk 5324): the commit it starts from lives
// under refs/panewire/handover/<job>/<commit>, so the prepare names that ref
// as its fetch. The worktree checks out branch <branch> at the snapshot
// commit and still tracks origin/<branch> for a plain `git push`.

// pushSnapshotRef commits on top of the fixture's feat tip and pushes it to a
// hand-over ref on origin, the way worktree.handover_snapshot leaves it.
func (fixture *nodeWorktreeFixture) pushSnapshotRef(t *testing.T, job string) (commit, ref string) {
	t.Helper()
	tmp := filepath.Join(fixture.root, "snapshot."+job)
	nodeGit(t, fixture.root, "clone", "-q", fixture.origin, tmp)
	nodeGit(t, tmp, "checkout", "-q", "--detach", fixture.feat)
	nodeGit(t, tmp, "commit", "-q", "--allow-empty", "-m", "snapshot")
	commit = nodeGit(t, tmp, "rev-parse", "HEAD")
	ref = handoverSnapshotRef(job, commit)
	nodeGit(t, tmp, "push", "-q", "origin", "HEAD:"+ref)
	return commit, ref
}

func TestNodePrepareSuccessorStartsAtTheSnapshotCommit5324(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	snap, ref := fixture.pushSnapshotRef(t, "job-a")
	dir := filepath.Join(fixture.workRoot, "repo.job")
	args := fixture.prepareArgs(fleet.HubCheckoutSuccessor, "feat", snap, ref, "repo.job")

	result, code := nodeRun(t, client, nodeCommandWorktreePrepare, args)
	if code != "" || result["outcome"] != nodePreparePrepared {
		t.Fatalf("prepare outcome=%v code=%q", result["outcome"], code)
	}
	worktree, _ := result["worktree"].(map[string]any)
	if worktree["path"] != dir || worktree["branch"] != "feat" || worktree["head"] != snap {
		t.Fatalf("worktree=%v, want feat at the snapshot %s", worktree, snap)
	}
	if got := nodeGit(t, dir, "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"); got != "origin/feat" {
		t.Fatalf("upstream=%q, want origin/feat", got)
	}
	// The snapshot commit contains the pushed tip, so a plain push carries
	// the successor's work on the branch it continues.
	nodeGit(t, dir, "commit", "-q", "--allow-empty", "-m", "successor")
	pushed := nodeGit(t, dir, "rev-parse", "HEAD")
	nodeGit(t, dir, "push", "-q")
	if got := nodeGit(t, fixture.origin, "rev-parse", "feat"); got != pushed {
		t.Fatalf("origin/feat=%s, want the successor's %s", got, pushed)
	}
}

// Someone pushed to the branch after the snapshot: its tip is no longer an
// ancestor of the snapshot commit, so the successor is stale and nothing is
// prepared.
func TestNodePrepareSuccessorSnapshotStale5324(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	snap, ref := fixture.pushSnapshotRef(t, "job-a")
	fixture.pushFixtureBranch(t, "feat", fixture.feat)
	dir := filepath.Join(fixture.workRoot, "repo.job")
	args := fixture.prepareArgs(fleet.HubCheckoutSuccessor, "feat", snap, ref, "repo.job")

	result, code := nodeRun(t, client, nodeCommandWorktreePrepare, args)
	if code != "" || result["outcome"] != nodePrepareSuccessorStale {
		t.Fatalf("prepare outcome=%v code=%q, want %q", result["outcome"], code, nodePrepareSuccessorStale)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("a stale prepare left %s behind", dir)
	}
}

// A fetch that succeeds but does not carry the named commit is refused
// sha-missing: the ref and the sha are checked independently.
func TestNodePrepareSuccessorSnapshotCommitMissing5324(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	_, ref := fixture.pushSnapshotRef(t, "job-a")
	tmp := filepath.Join(fixture.root, "unpushed")
	nodeGit(t, fixture.root, "clone", "-q", fixture.origin, tmp)
	nodeGit(t, tmp, "checkout", "-q", "-b", "never-pushed")
	nodeGit(t, tmp, "commit", "-q", "--allow-empty", "-m", "local only")
	missing := nodeGit(t, tmp, "rev-parse", "HEAD")
	args := fixture.prepareArgs(fleet.HubCheckoutSuccessor, "feat", missing, ref, "repo.job")

	result, code := nodeRun(t, client, nodeCommandWorktreePrepare, args)
	if code != "" || result["outcome"] != nodePrepareShaMissing {
		t.Fatalf("prepare outcome=%v code=%q, want %q", result["outcome"], code, nodePrepareShaMissing)
	}
}

// A snapshot of a branch that was never pushed: origin has no <branch>, the
// snapshot commit is the whole line, and the successor starts there with no
// upstream to set (review r4623417484).
func TestNodePrepareSuccessorSnapshotOfUnpushedBranch5324(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	tmp := filepath.Join(fixture.root, "unpushed-snapshot")
	nodeGit(t, fixture.root, "clone", "-q", fixture.origin, tmp)
	nodeGit(t, tmp, "checkout", "-q", "-b", "local-only")
	nodeGit(t, tmp, "commit", "-q", "--allow-empty", "-m", "never pushed")
	nodeGit(t, tmp, "commit", "-q", "--allow-empty", "-m", "snapshot")
	snap := nodeGit(t, tmp, "rev-parse", "HEAD")
	ref := handoverSnapshotRef("job-a", snap)
	nodeGit(t, tmp, "push", "-q", "origin", "HEAD:"+ref)
	dir := filepath.Join(fixture.workRoot, "repo.job")
	args := fixture.prepareArgs(fleet.HubCheckoutSuccessor, "local-only", snap, ref, "repo.job")

	result, code := nodeRun(t, client, nodeCommandWorktreePrepare, args)
	if code != "" || result["outcome"] != nodePreparePrepared {
		t.Fatalf("prepare outcome=%v code=%q", result["outcome"], code)
	}
	worktree, _ := result["worktree"].(map[string]any)
	if worktree["path"] != dir || worktree["branch"] != "local-only" || worktree["head"] != snap {
		t.Fatalf("worktree=%v, want local-only at the snapshot %s", worktree, snap)
	}
	if out, err := exec.Command("git", "-C", dir, "rev-parse", "--abbrev-ref", "@{u}").CombinedOutput(); err == nil {
		t.Fatalf("upstream=%s, want none", out)
	}
}

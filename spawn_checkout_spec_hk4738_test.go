package panewire

import (
	"path/filepath"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 4738: a spawn may name its checkout instead of holding one, and the spec
// the submitter builds (fleet.HubCheckout) is exactly what the node hands
// worktree.prepare. These tests drive the real git fixture with that spec: a
// detached worktree at the exact commit, and a refusal when the fetched ref
// does not contain the commit. The fixture's origin is its local file://
// remote, which worktree.prepare accepts, to keep the test off the network;
// the submitter itself takes the repository's clone URL.

func TestSpecSpawnPreparesTheWorktreeAtTheCommit(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	spec := fleet.HubCheckout{Origin: "file://" + fixture.origin, Mode: "detached", Sha: fixture.feat, Fetch: "pull/1/head", Dir: "repo.job"}
	args := map[string]any{"origin": spec.Origin, "mode": spec.Mode, "branch": "", "sha": spec.Sha, "fetch": spec.Fetch, "dir": spec.Dir}

	result, code := nodeRun(t, client, nodeCommandWorktreePrepare, args)
	if code != "" || result["outcome"] != nodePreparePrepared {
		t.Fatalf("prepare outcome=%v code=%q", result["outcome"], code)
	}
	dir := filepath.Join(fixture.workRoot, spec.Dir)
	worktree, _ := result["worktree"].(map[string]any)
	if worktree["path"] != dir || worktree["branch"] != "" || worktree["head"] != fixture.feat {
		t.Fatalf("worktree=%v want detached at %s in %s", worktree, fixture.feat, dir)
	}
	if head := nodeGit(t, dir, "rev-parse", "HEAD"); head != fixture.feat {
		t.Fatalf("HEAD=%s want the spec's commit %s", head, fixture.feat)
	}

	// The same spec again is the same worktree, not a second one.
	if result, _ = nodeRun(t, client, nodeCommandWorktreePrepare, args); result["outcome"] != nodePrepareReused {
		t.Fatalf("second prepare outcome=%v", result["outcome"])
	}
}

// A spec that names a commit the fetched ref does not contain is refused:
// the node must never prepare a worktree the submitter did not name.
func TestSpecSpawnRefusesACommitTheRefDoesNotContain(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	// An orphan branch on origin is real and fetchable, and shares no history
	// with the pull ref the spec fetches.
	fixture.pushFixtureBranch(t, "unrelated", "")
	unrelated := nodeGit(t, fixture.origin, "rev-parse", "refs/heads/unrelated")
	spec := fleet.HubCheckout{Origin: "file://" + fixture.origin, Mode: "detached", Sha: unrelated, Fetch: "pull/1/head", Dir: "repo.job"}
	args := map[string]any{"origin": spec.Origin, "mode": spec.Mode, "branch": "", "sha": spec.Sha, "fetch": spec.Fetch, "dir": spec.Dir}

	result, code := nodeRun(t, client, nodeCommandWorktreePrepare, args)
	if code != "" || result["outcome"] != nodePrepareShaMissing {
		t.Fatalf("mismatched commit outcome=%v code=%q", result["outcome"], code)
	}
	if entries, _ := filepath.Glob(filepath.Join(fixture.workRoot, spec.Dir)); len(entries) != 0 {
		t.Fatalf("a refused spec must leave no worktree behind: %v", entries)
	}
}

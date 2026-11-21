package panewire

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

// A same-node successor (hk 5870): the predecessor's worktree still holds the
// branch the successor continues. When that worktree is a clean checkout at
// the pushed tip, the node removes it and the successor checks the branch out
// here; a holder with anything unsaved is refused as before, and nothing is
// forced.

// holdBranch registers a linked worktree of the clone on branch, the way a
// finished predecessor leaves one behind, and returns its path.
func (fixture *nodeWorktreeFixture) holdBranch(t *testing.T, name, branch string) string {
	t.Helper()
	fixture.cloneRepo(t)
	held := filepath.Join(fixture.workRoot, name)
	nodeGit(t, fixture.clone, "worktree", "add", "-q", held, branch)
	return held
}

// registeredWorktrees lists the worktree paths git knows for the clone.
func (fixture *nodeWorktreeFixture) registeredWorktrees(t *testing.T) string {
	t.Helper()
	return nodeGit(t, fixture.clone, "worktree", "list", "--porcelain")
}

func TestNodePrepareSuccessorReleasesCleanPushedHolder5870(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	held := fixture.holdBranch(t, "repo.parent", "feat")
	dir := filepath.Join(fixture.workRoot, "repo.job")
	args := fixture.prepareArgs(fleet.HubCheckoutSuccessor, "feat", fixture.feat, "", "repo.job")

	result, code := nodeRun(t, client, nodeCommandWorktreePrepare, args)
	if code != "" || result["outcome"] != nodePreparePrepared {
		t.Fatalf("prepare outcome=%v code=%q reason=%v, want prepared", result["outcome"], code, result["reason"])
	}
	worktree, _ := result["worktree"].(map[string]any)
	if worktree["path"] != dir || worktree["branch"] != "feat" || worktree["head"] != fixture.feat {
		t.Fatalf("worktree=%v, want feat at %s in %s", worktree, fixture.feat, dir)
	}
	if _, err := os.Stat(held); !os.IsNotExist(err) {
		t.Fatalf("the clean predecessor worktree %s survived (err=%v)", held, err)
	}
	if out := fixture.registeredWorktrees(t); strings.Contains(out, held) {
		t.Fatalf("the predecessor worktree is still registered: %s", out)
	}
	if got := nodeGit(t, dir, "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"); got != "origin/feat" {
		t.Fatalf("upstream=%q, want origin/feat", got)
	}
	nodeGit(t, dir, "commit", "-q", "--allow-empty", "-m", "successor")
	pushed := nodeGit(t, dir, "rev-parse", "HEAD")
	nodeGit(t, dir, "push", "-q")
	if got := nodeGit(t, fixture.origin, "rev-parse", "feat"); got != pushed {
		t.Fatalf("origin/feat=%s, want the successor's %s", got, pushed)
	}
}

// The snapshot hand-over case: the holder is the clean predecessor at the
// pushed tip, and the successor starts at the snapshot commit above it.
func TestNodePrepareSuccessorReleasesHolderAtSnapshot5870(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	held := fixture.holdBranch(t, "repo.parent", "feat")
	snap, ref := fixture.pushSnapshotRef(t, "job-a")
	dir := filepath.Join(fixture.workRoot, "repo.job")
	args := fixture.prepareArgs(fleet.HubCheckoutSuccessor, "feat", snap, ref, "repo.job")

	result, code := nodeRun(t, client, nodeCommandWorktreePrepare, args)
	if code != "" || result["outcome"] != nodePreparePrepared {
		t.Fatalf("prepare outcome=%v code=%q reason=%v, want prepared", result["outcome"], code, result["reason"])
	}
	worktree, _ := result["worktree"].(map[string]any)
	if worktree["path"] != dir || worktree["branch"] != "feat" || worktree["head"] != snap {
		t.Fatalf("worktree=%v, want feat at the snapshot %s", worktree, snap)
	}
	if _, err := os.Stat(held); !os.IsNotExist(err) {
		t.Fatalf("the clean predecessor worktree %s survived (err=%v)", held, err)
	}
}

// A holder that would lose something is refused exactly as before, and it is
// left on disk with its branch: the node never forces a removal.
func TestNodePrepareSuccessorKeepsHolderWithUnsavedWork5870(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T, fixture *nodeWorktreeFixture, held string)
		file  string
	}{
		{
			name: "an ignored file",
			setup: func(t *testing.T, fixture *nodeWorktreeFixture, held string) {
				exclude := filepath.Join(fixture.clone, ".git", "info", "exclude")
				f, err := os.OpenFile(exclude, os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.WriteString("scratch.log\n"); err != nil {
					t.Fatal(err)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(held, "scratch.log"), []byte("kept\n"), 0600); err != nil {
					t.Fatal(err)
				}
			},
			file: "scratch.log",
		},
		{
			name: "an unpushed commit",
			setup: func(t *testing.T, fixture *nodeWorktreeFixture, held string) {
				nodeGit(t, held, "commit", "-q", "--allow-empty", "-m", "local only")
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newNodeWorktreeFixture(t)
			client := fixture.client()
			held := fixture.holdBranch(t, "repo.parent", "feat")
			tc.setup(t, fixture, held)
			dir := filepath.Join(fixture.workRoot, "repo.job")
			args := fixture.prepareArgs(fleet.HubCheckoutSuccessor, "feat", fixture.feat, "", "repo.job")

			result, code := nodeRun(t, client, nodeCommandWorktreePrepare, args)
			if code != "" || result["outcome"] != nodePrepareBranchInUse {
				t.Fatalf("prepare outcome=%v code=%q, want %q", result["outcome"], code, nodePrepareBranchInUse)
			}
			if reason, _ := result["reason"].(string); reason != "worktree="+held {
				t.Fatalf("reason=%q, want worktree=%s", reason, held)
			}
			if _, err := os.Stat(held); err != nil {
				t.Fatalf("the holder %s was removed: %v", held, err)
			}
			if tc.file != "" {
				if _, err := os.Stat(filepath.Join(held, tc.file)); err != nil {
					t.Fatalf("the holder's unsaved %s was lost: %v", tc.file, err)
				}
			}
			if _, err := os.Stat(dir); !os.IsNotExist(err) {
				t.Fatalf("a refused prepare created %s (err=%v)", dir, err)
			}
		})
	}
}

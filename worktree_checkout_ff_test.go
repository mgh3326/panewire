package panewire

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestNodeWorktreePrepareFastForward(t *testing.T) {
	for _, reuse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reuse=%t", reuse), func(t *testing.T) {
			f := newNodeWorktreeFixture(t)
			f.cloneRepo(t)
			dir := filepath.Join(f.workRoot, "repo.job")
			nodeGit(t, f.clone, "branch", "job", f.main)
			if reuse {
				nodeGit(t, f.clone, "worktree", "add", "-q", dir, "job")
			}
			result, code := nodeRun(t, f.client(), nodeCommandWorktreePrepare, f.prepareArgs("branch", "job", f.feat, "", "repo.job"))
			want := nodePreparePrepared
			if reuse {
				want = nodePrepareReused
			}
			if code != "" || result["outcome"] != want || result["fast_forwarded"] != true {
				t.Fatalf("result=%v code=%q; want %s and fast_forwarded", result, code, want)
			}
			if head := nodeGit(t, dir, "rev-parse", "HEAD"); head != f.feat {
				t.Fatalf("head=%s want %s", head, f.feat)
			}
			wt, _ := result["worktree"].(map[string]any)
			if wt["head"] != f.feat {
				t.Fatalf("reported worktree=%v", wt)
			}
		})
	}
}

func TestNodeWorktreePrepareBranchRelations(t *testing.T) {
	for _, reuse := range []bool{false, true} {
		for _, relation := range []string{"ahead", "diverged"} {
			t.Run(fmt.Sprintf("reuse=%t/%s", reuse, relation), func(t *testing.T) {
				f := newNodeWorktreeFixture(t)
				f.cloneRepo(t)
				base, expected := f.feat, f.main
				if relation == "diverged" {
					base, expected = f.main, f.feat
				}
				nodeGit(t, f.clone, "branch", "job", base)
				dir := filepath.Join(f.workRoot, "repo.job")
				nodeGit(t, f.clone, "worktree", "add", "-q", dir, "job")
				nodeGit(t, dir, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-q", "--allow-empty", "-m", "local")
				local := nodeGit(t, dir, "rev-parse", "HEAD")
				if !reuse {
					nodeGit(t, f.clone, "worktree", "remove", dir)
				}
				result, _ := nodeRun(t, f.client(), nodeCommandWorktreePrepare, f.prepareArgs("branch", "job", expected, "", "repo.job"))
				want := fmt.Sprintf("local=%s expected=%s relation=%s", utf8PrefixWithin(local, 7), utf8PrefixWithin(expected, 7), relation)
				if result["outcome"] != nodePrepareBranchElsewhere || result["reason"] != want {
					t.Fatalf("result=%v want reason=%q", result, want)
				}
				if head := nodeGit(t, f.clone, "rev-parse", "job"); head != local {
					t.Fatalf("branch moved to %s", head)
				}
			})
		}
	}
}

func TestNodeWorktreePrepareDirtyBehind(t *testing.T) {
	f := newNodeWorktreeFixture(t)
	f.cloneRepo(t)
	dir := filepath.Join(f.workRoot, "repo.job")
	nodeGit(t, f.clone, "worktree", "add", "-q", "-b", "job", dir, f.main)
	if err := os.WriteFile(filepath.Join(dir, "wip"), []byte("work"), 0600); err != nil {
		t.Fatal(err)
	}
	result, _ := nodeRun(t, f.client(), nodeCommandWorktreePrepare, f.prepareArgs("branch", "job", f.feat, "", "repo.job"))
	if result["outcome"] != nodeOutcomeDirty {
		t.Fatalf("result=%v", result)
	}
	if head := nodeGit(t, dir, "rev-parse", "HEAD"); head != f.main {
		t.Fatalf("dirty head moved to %s", head)
	}
}

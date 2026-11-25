package panewire

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestNodeWorktreePrepareDetachedRefusesSwappedJobDir(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	fixture.cloneRepo(t)

	dir := filepath.Join(fixture.workRoot, "repo.job")
	other := filepath.Join(fixture.workRoot, "repo.other")
	nodeGit(t, fixture.clone, "worktree", "add", "-q", "--detach", dir, fixture.feat)
	nodeGit(t, fixture.clone, "worktree", "add", "-q", "--detach", other, fixture.feat)
	nodeGit(t, other, "sparse-checkout", "init", "--cone")
	nodeGit(t, other, "sparse-checkout", "set", "kept")

	client.nodePrepareJobDirChecked = func(checked string) {
		if checked != dir {
			t.Fatalf("checked %s, want %s", checked, dir)
		}
		if err := os.Rename(dir, dir+".held"); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(other, dir); err != nil {
			t.Fatal(err)
		}
	}
	args := fixture.prepareArgs("detached", "", fixture.feat, "refs/heads/feat", "repo.job")
	result, code := nodeRun(t, client, nodeCommandWorktreePrepare, args)
	if sparse := nodeGit(t, other, "config", "--get", "core.sparseCheckout"); sparse != "true" {
		t.Errorf("other worktree core.sparseCheckout=%q, want true", sparse)
	}
	if code != "" || result["outcome"] != nodePrepareJobDirLink {
		t.Fatalf("prepare code=%q result=%v, want %q", code, result, nodePrepareJobDirLink)
	}
}

func TestNodeWorktreePrepareRecordsJobDirAfterPrecedingPrepare(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	fixture.cloneRepo(t)

	dir := filepath.Join(fixture.workRoot, "repo.job")
	git := client.nodeGitRunner()
	preceded := false
	client.nodeGit = func(ctx context.Context, args ...string) ([]byte, error) {
		// Interleave the first prepare at the second prepare's version probe,
		// immediately before it enters the path that takes the clone lock.
		if !preceded && len(args) == 1 && args[0] == "--version" {
			preceded = true
			if _, err := os.Lstat(dir); !os.IsNotExist(err) {
				t.Fatalf("job dir should start absent, got %v", err)
			}
			first := fixture.prepareArgs("branch", "other", fixture.main, "", "repo.job")
			result, code := nodeRun(t, fixture.client(), nodeCommandWorktreePrepare, first)
			if code != "" || result["outcome"] != nodePreparePrepared {
				t.Fatalf("first prepare code=%q result=%v, want prepared", code, result)
			}
			nodeGit(t, fixture.clone, "push", "-q", "origin", fixture.feat+":refs/heads/other")
		}
		return git(ctx, args...)
	}
	second := fixture.prepareArgs("branch", "other", fixture.feat, "", "repo.job")
	result, code := nodeRun(t, client, nodeCommandWorktreePrepare, second)
	if code != "" || result["outcome"] != nodePrepareReused {
		t.Fatalf("second prepare code=%q result=%v, want reused", code, result)
	}
	if head := nodeGit(t, dir, "rev-parse", "HEAD"); head != fixture.feat {
		t.Fatalf("second prepare HEAD=%s, want %s", head, fixture.feat)
	}
}

package panewire

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The runner models Git being interrupted after it creates the linked
// worktree metadata but before panewire receives a successful add result.
func TestNodeWorktreePrepareCancelledAddCanReplay(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	fixture.cloneRepo(t)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	client := fixture.client()
	client.nodeGit = func(ctx context.Context, args ...string) ([]byte, error) {
		if strings.Contains(strings.Join(args, " "), "worktree add") {
			if _, err := exec.CommandContext(ctx, "git", args...).Output(); err != nil {
				return nil, err
			}
			dir := args[len(args)-2]
			if _, err := exec.Command("git", "-C", fixture.clone, "worktree", "lock", "--reason", "initializing", dir).CombinedOutput(); err != nil {
				t.Fatalf("create interrupted-add lock state: %v", err)
			}
			cancel()
			return nil, testSignaledExit(t)
		}
		return exec.CommandContext(ctx, "git", args...).Output()
	}

	args := nodeWorktreePrepareArgs{Origin: fixture.origin, Mode: "branch", Branch: "feat", Dir: "repo.job"}
	prepared, code := client.nodeWorktreePrepare(ctx, args)
	result, ok := prepared.(nodeWorktreePrepareResult)
	if code != "" || !ok || result.Outcome != nodeOutcomeCancelled {
		t.Fatalf("interrupted prepare=%+v code=%q, want cancelled", prepared, code)
	}

	client.nodeGit = nil
	prepared, code = client.nodeWorktreePrepare(t.Context(), args)
	result, ok = prepared.(nodeWorktreePrepareResult)
	if code != "" || !ok || (result.Outcome != nodePreparePrepared && result.Outcome != nodePrepareReused) {
		t.Fatalf("replay=%+v code=%q, want prepared or reused", prepared, code)
	}
}

func TestNodeWorktreePrepareCancelledDetachedAddCanReplay(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	fixture.cloneRepo(t)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	client := fixture.client()
	client.nodeGit = func(ctx context.Context, args ...string) ([]byte, error) {
		if strings.Contains(strings.Join(args, " "), "worktree add") {
			if _, err := exec.CommandContext(ctx, "git", args...).Output(); err != nil {
				return nil, err
			}
			dir := args[len(args)-2]
			if _, err := exec.Command("git", "-C", fixture.clone, "worktree", "lock", "--reason", "initializing", dir).CombinedOutput(); err != nil {
				t.Fatalf("create interrupted detached-add lock state: %v", err)
			}
			cancel()
			return nil, testSignaledExit(t)
		}
		return exec.CommandContext(ctx, "git", args...).Output()
	}

	args := nodeWorktreePrepareArgs{Origin: fixture.origin, Mode: "detached", Sha: fixture.main, Fetch: "refs/heads/main", Dir: "repo.detached"}
	prepared, code := client.nodeWorktreePrepare(ctx, args)
	result, ok := prepared.(nodeWorktreePrepareResult)
	if code != "" || !ok || result.Outcome != nodeOutcomeCancelled {
		t.Fatalf("interrupted detached prepare=%+v code=%q, want cancelled", prepared, code)
	}
	client.nodeGit = nil
	prepared, code = client.nodeWorktreePrepare(t.Context(), args)
	result, ok = prepared.(nodeWorktreePrepareResult)
	if code != "" || !ok || (result.Outcome != nodePreparePrepared && result.Outcome != nodePrepareReused) {
		t.Fatalf("detached replay=%+v code=%q, want prepared or reused", prepared, code)
	}
}

func TestNodeWorktreePrepareRefusesForeignInitializingLock(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	fixture.cloneRepo(t)
	dir, _ := fixture.addWorktree(t, "job")
	if _, err := exec.Command("git", "-C", fixture.clone, "worktree", "lock", "--reason", "initializing", dir).CombinedOutput(); err != nil {
		t.Fatal(err)
	}
	prepared, code := fixture.client().nodeWorktreePrepare(t.Context(), nodeWorktreePrepareArgs{
		Origin: fixture.origin,
		Mode:   "branch",
		Branch: "job/job",
		Sha:    fixture.main,
		Dir:    "repo.job",
	})
	result, ok := prepared.(nodeWorktreePrepareResult)
	if code != "" || !ok || result.Outcome != nodeRemoveLocked {
		t.Fatalf("foreign locked prepare=%+v code=%q, want locked", prepared, code)
	}
	listing, err := exec.Command("git", "-C", fixture.clone, "worktree", "list", "--porcelain").CombinedOutput()
	if err != nil || !strings.Contains(string(listing), "locked initializing") {
		t.Fatalf("foreign lock after refused prepare=%q err=%v", listing, err)
	}
}

func TestNodeWorktreeAddCleanupRequiresInitializingLockAndSignal(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reason string
		cancel bool
	}{
		{name: "different lock reason", reason: "operator", cancel: true},
		{name: "ordinary failed add", reason: "initializing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newNodeWorktreeFixture(t)
			fixture.cloneRepo(t)
			dir := filepath.Join(fixture.workRoot, "repo.job")
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			client := fixture.client()
			git := func(ctx context.Context, args ...string) ([]byte, error) {
				if strings.Contains(strings.Join(args, " "), "worktree add") {
					out, err := exec.CommandContext(ctx, "git", args...).CombinedOutput()
					if err != nil {
						return out, err
					}
					if _, err := exec.Command("git", "-C", fixture.clone, "worktree", "lock", "--reason", tc.reason, dir).CombinedOutput(); err != nil {
						t.Fatal(err)
					}
					if tc.cancel {
						cancel()
						return nil, testSignaledExit(t)
					}
					return nil, errors.New("exit status 128")
				}
				return exec.CommandContext(ctx, "git", args...).CombinedOutput()
			}
			client.nodeGit = git
			_, _ = client.nodeWorktreePrepare(ctx, nodeWorktreePrepareArgs{Origin: fixture.origin, Mode: "branch", Branch: "feat", Sha: fixture.feat, Dir: "repo.job"})
			listing, err := exec.Command("git", "-C", fixture.clone, "worktree", "list", "--porcelain").CombinedOutput()
			if err != nil || !strings.Contains(string(listing), "locked "+tc.reason) {
				t.Fatalf("worktree metadata after failed add=%q err=%v", listing, err)
			}
		})
	}
}

func TestNodeWorktreeAddCleanupDoesNotPruneMissingUnlockedWorktree(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	fixture.cloneRepo(t)
	missing, _ := fixture.addWorktree(t, "missing")
	if err := exec.Command("git", "-C", fixture.clone, "worktree", "lock", "--reason", "keep metadata", missing).Run(); err != nil {
		t.Fatal(err)
	}
	if err := exec.Command("git", "-C", fixture.clone, "worktree", "unlock", missing).Run(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(missing); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(fixture.workRoot, "repo.job")
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	client := fixture.client()
	git := func(ctx context.Context, args ...string) ([]byte, error) {
		if strings.Contains(strings.Join(args, " "), "worktree add") {
			out, err := exec.CommandContext(ctx, "git", args...).CombinedOutput()
			if err != nil {
				return out, err
			}
			if _, err := exec.Command("git", "-C", fixture.clone, "worktree", "lock", "--reason", "initializing", dir).CombinedOutput(); err != nil {
				t.Fatal(err)
			}
			cancel()
			return nil, testSignaledExit(t)
		}
		return exec.CommandContext(ctx, "git", args...).CombinedOutput()
	}
	client.nodeGit = git
	_, _ = client.nodeWorktreePrepare(ctx, nodeWorktreePrepareArgs{Origin: fixture.origin, Mode: "branch", Branch: "feat", Sha: fixture.feat, Dir: "repo.job"})
	listing, err := exec.Command("git", "-C", fixture.clone, "worktree", "list", "--porcelain").CombinedOutput()
	if err != nil || !strings.Contains(string(listing), "worktree "+missing) {
		t.Fatalf("missing worktree metadata was pruned: listing=%q err=%v", listing, err)
	}
}

func TestNodeWorktreePrepareDetachedReclaimsOwnedInterruptedAdd(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	fixture.cloneRepo(t)
	dir := filepath.Join(fixture.workRoot, "repo.detached")
	if _, err := exec.Command("git", "-C", fixture.clone, "worktree", "add", "--detach", dir, fixture.main).CombinedOutput(); err != nil {
		t.Fatal(err)
	}
	if _, err := exec.Command("git", "-C", fixture.clone, "worktree", "lock", "--reason", "initializing", dir).CombinedOutput(); err != nil {
		t.Fatal(err)
	}
	intent := nodeWorktreeAddIntent{Path: filepath.Clean(dir), Mode: "detached", Sha: fixture.main}
	if _, err := nodeWriteWorktreeAddIntent(t.Context(), fixture.client().nodeGitRunner(), fixture.clone, dir, intent); err != nil {
		t.Fatal(err)
	}
	prepared, code := fixture.client().nodeWorktreePrepare(t.Context(), nodeWorktreePrepareArgs{
		Origin: fixture.origin, Mode: "detached", Sha: fixture.main, Fetch: "refs/heads/main", Dir: "repo.detached",
	})
	result, ok := prepared.(nodeWorktreePrepareResult)
	if code != "" || !ok || result.Outcome != nodePreparePrepared {
		t.Fatalf("detached replay=%+v code=%q, want prepared", prepared, code)
	}
}

func testSignaledExit(t *testing.T) error {
	t.Helper()
	cmd := exec.Command("sh", "-c", "kill -TERM $$")
	if err := cmd.Run(); err == nil {
		t.Fatal("signal fixture unexpectedly exited successfully")
	} else {
		return err
	}
	return nil
}

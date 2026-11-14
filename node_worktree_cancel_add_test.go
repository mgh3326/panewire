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
	client.nodeAdd = func(ctx context.Context, record func(nodeAddProcess) error, args ...string) error {
		proc, err := runAddChild(ctx, args...)
		if err != nil {
			return err
		}
		dir := args[len(args)-2]
		if _, err := exec.Command("git", "-C", fixture.clone, "worktree", "lock", "--reason", "initializing", dir).CombinedOutput(); err != nil {
			t.Fatalf("create interrupted-add lock state: %v", err)
		}
		if err := record(proc); err != nil {
			t.Fatalf("record add identity: %v", err)
		}
		cancel()
		return testSignaledExit(t)
	}

	args := nodeWorktreePrepareArgs{Origin: fixture.origin, Mode: "branch", Branch: "feat", Dir: "repo.job"}
	prepared, code := client.nodeWorktreePrepare(ctx, args)
	result, ok := prepared.(nodeWorktreePrepareResult)
	if code != "" || !ok || result.Outcome != nodeOutcomeCancelled {
		t.Fatalf("interrupted prepare=%+v code=%q, want cancelled", prepared, code)
	}

	client.nodeAdd = nil
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
	client.nodeAdd = func(ctx context.Context, record func(nodeAddProcess) error, args ...string) error {
		proc, err := runAddChild(ctx, args...)
		if err != nil {
			return err
		}
		dir := args[len(args)-2]
		if _, err := exec.Command("git", "-C", fixture.clone, "worktree", "lock", "--reason", "initializing", dir).CombinedOutput(); err != nil {
			t.Fatalf("create interrupted detached-add lock state: %v", err)
		}
		if err := record(proc); err != nil {
			t.Fatalf("record add identity: %v", err)
		}
		cancel()
		return testSignaledExit(t)
	}

	args := nodeWorktreePrepareArgs{Origin: fixture.origin, Mode: "detached", Sha: fixture.main, Fetch: "refs/heads/main", Dir: "repo.detached"}
	prepared, code := client.nodeWorktreePrepare(ctx, args)
	result, ok := prepared.(nodeWorktreePrepareResult)
	if code != "" || !ok || result.Outcome != nodeOutcomeCancelled {
		t.Fatalf("interrupted detached prepare=%+v code=%q, want cancelled", prepared, code)
	}
	client.nodeAdd = nil
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
			client.nodeAdd = func(ctx context.Context, record func(nodeAddProcess) error, args ...string) error {
				proc, err := runAddChild(ctx, args...)
				if err != nil {
					return err
				}
				if _, err := exec.Command("git", "-C", fixture.clone, "worktree", "lock", "--reason", tc.reason, dir).CombinedOutput(); err != nil {
					t.Fatal(err)
				}
				if err := record(proc); err != nil {
					t.Fatalf("record add identity: %v", err)
				}
				if tc.cancel {
					cancel()
					return testSignaledExit(t)
				}
				return errors.New("exit status 128")
			}
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
	client.nodeAdd = func(ctx context.Context, record func(nodeAddProcess) error, args ...string) error {
		proc, err := runAddChild(ctx, args...)
		if err != nil {
			return err
		}
		if _, err := exec.Command("git", "-C", fixture.clone, "worktree", "lock", "--reason", "initializing", dir).CombinedOutput(); err != nil {
			t.Fatal(err)
		}
		if err := record(proc); err != nil {
			t.Fatalf("record add identity: %v", err)
		}
		cancel()
		return testSignaledExit(t)
	}
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
	intent := nodeWorktreeAddIntent{Path: filepath.Clean(dir), Mode: "detached", Sha: fixture.main, Add: exitedAddProcess(t)}
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

func TestNodeWorktreeVerifyReuseRefusesRewrittenOriginWithoutChangingCheckout(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	args := nodeWorktreePrepareArgs{Origin: fixture.origin, Mode: "branch", Branch: "feat", Dir: "repo.job"}
	prepared, code := client.nodeWorktreePrepare(t.Context(), args)
	if code != "" || prepared.(nodeWorktreePrepareResult).Outcome != nodePreparePrepared {
		t.Fatalf("prepare=%+v code=%q", prepared, code)
	}
	dir := filepath.Join(fixture.workRoot, "repo.job")
	seed := filepath.Join(fixture.root, "seed")
	nodeGit(t, seed, "push", "-q", "--force", "origin", "main:feat")
	args.Sha, args.VerifyReuse = fixture.feat, true
	prepared, code = client.nodeWorktreePrepare(t.Context(), args)
	if code != "" || prepared.(nodeWorktreePrepareResult).Outcome != nodePrepareBranchElsewhere {
		t.Fatalf("verification=%+v code=%q", prepared, code)
	}
	if head := nodeGit(t, dir, "rev-parse", "HEAD"); head != fixture.feat {
		t.Fatalf("checkout moved to %s, want %s", head, fixture.feat)
	}
}

func TestNodeWorktreeInspectReadOnlyVerifiesCheckout(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	args := nodeWorktreePrepareArgs{Origin: fixture.origin, Mode: "branch", Branch: "feat", Dir: "repo.job"}
	prepared, code := client.nodeWorktreePrepare(t.Context(), args)
	if code != "" || prepared.(nodeWorktreePrepareResult).Outcome != nodePreparePrepared {
		t.Fatalf("prepare=%+v code=%q", prepared, code)
	}
	dir := filepath.Join(fixture.workRoot, "repo.job")
	inspect := func() nodeWorktreeInspectResult {
		result := nodeWorktreeInspect(t.Context(), client.nodeGitRunner(), dir)
		result.Check = nodeWorktreeVerifyReadOnly(t.Context(), client.nodeGitRunner(), result.Worktree)
		return result
	}
	if result := inspect(); result.Check != "ok" || result.Worktree.Head != fixture.feat {
		t.Fatalf("inspect=%+v", result)
	}
	if err := os.WriteFile(filepath.Join(dir, "dirty"), []byte("work"), 0600); err != nil {
		t.Fatal(err)
	}
	if result := inspect(); result.Check != "dirty" {
		t.Fatalf("dirty inspect=%+v", result)
	}
	if err := os.Remove(filepath.Join(dir, "dirty")); err != nil {
		t.Fatal(err)
	}
	nodeGit(t, fixture.clone, "worktree", "lock", dir)
	if result := inspect(); result.Check != "locked" {
		t.Fatalf("locked inspect=%+v", result)
	}
	if head := nodeGit(t, dir, "rev-parse", "HEAD"); head != fixture.feat {
		t.Fatalf("inspection moved checkout to %s", head)
	}
}

func TestNodeWorktreeInspectRequiresExpectedCheckoutPath(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	args := nodeWorktreePrepareArgs{Origin: fixture.origin, Mode: "branch", Branch: "feat", Dir: "repo.job"}
	prepared, code := client.nodeWorktreePrepare(t.Context(), args)
	if code != "" || prepared.(nodeWorktreePrepareResult).Outcome != nodePreparePrepared {
		t.Fatalf("prepare=%+v code=%q", prepared, code)
	}
	dir := filepath.Join(fixture.workRoot, "repo.job")
	result, code := nodeRun(t, client, nodeCommandWorktreeInspect, map[string]any{"cwd": dir, "expected_dir": "repo.other"})
	if code != "" || result["worktree"] != nil || result["check"] != "mismatch" {
		t.Fatalf("inspection=%v code=%q", result, code)
	}
	result, code = nodeRun(t, client, nodeCommandWorktreeInspect, map[string]any{"cwd": dir, "expected_dir": "repo.job", "verify_branch": true})
	if code != "" || result["check"] != "ok" {
		t.Fatalf("verified inspection=%v code=%q", result, code)
	}
}

func TestNodeWorktreeVerifyReuseKeepsPinnedHeadWhenOriginAdvances(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	args := nodeWorktreePrepareArgs{Origin: fixture.origin, Mode: "branch", Branch: "feat", Dir: "repo.job"}
	prepared, code := client.nodeWorktreePrepare(t.Context(), args)
	if code != "" || prepared.(nodeWorktreePrepareResult).Outcome != nodePreparePrepared {
		t.Fatalf("prepare=%+v code=%q", prepared, code)
	}
	seed := filepath.Join(fixture.root, "seed")
	nodeGit(t, seed, "commit", "-q", "--allow-empty", "-m", "advance")
	nodeGit(t, seed, "push", "-q", "origin", "feat")
	args.Sha, args.VerifyReuse = fixture.feat, true
	prepared, code = client.nodeWorktreePrepare(t.Context(), args)
	if code != "" || prepared.(nodeWorktreePrepareResult).Outcome != nodePrepareReused {
		t.Fatalf("reuse=%+v code=%q", prepared, code)
	}
	if head := nodeGit(t, filepath.Join(fixture.workRoot, "repo.job"), "rev-parse", "HEAD"); head != fixture.feat {
		t.Fatalf("checkout moved to %s", head)
	}
}

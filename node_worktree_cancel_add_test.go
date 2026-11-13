package panewire

import (
	"context"
	"os/exec"
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
			return nil, context.Canceled
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

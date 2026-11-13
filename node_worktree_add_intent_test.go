package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// interruptedAddRunner is a Git runner whose worktree add creates the linked
// worktree, takes lockReason, and then dies from a signal. With crashed set,
// the cleanup listing that nodeAddWorktree runs after the signal fails, so the
// lock and the add intent stay on disk: the state a node restart finds when
// its add was killed mid-flight. Without crashed, the cleanup runs as it does
// for an ordinary signal and removes an initializing worktree itself.
func interruptedAddRunner(t *testing.T, fixture *nodeWorktreeFixture, lockReason string, crashed bool) relayCommandRunner {
	t.Helper()
	added := false
	return func(ctx context.Context, args ...string) ([]byte, error) {
		joined := strings.Join(args, " ")
		if added && crashed && strings.Contains(joined, "worktree list") {
			return nil, errors.New("listing unavailable after crash")
		}
		if strings.Contains(joined, "worktree add") {
			if _, err := exec.CommandContext(ctx, "git", args...).Output(); err != nil {
				return nil, err
			}
			dir := args[len(args)-2]
			if _, err := exec.Command("git", "-C", fixture.clone, "worktree", "lock", "--reason", lockReason, dir).CombinedOutput(); err != nil {
				t.Fatalf("create interrupted-add lock state: %v", err)
			}
			added = true
			return nil, testSignaledExit(t)
		}
		return exec.CommandContext(ctx, "git", args...).Output()
	}
}

// Finding (PR #493 thread 4561204551, deferred to hk 4519): an interrupted add
// must still replay after a different worktree in the same clone has added
// successfully. Before the fix every add shared one intent file, so the second
// add's success deleted the first add's intent and the replay returned locked.
func TestNodeWorktreeReplayKeepsIntentAcrossOtherWorktreeAdds(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	fixture.cloneRepo(t)
	interrupted := fixture.client()
	interrupted.nodeGit = interruptedAddRunner(t, fixture, "initializing", true)
	first := nodeWorktreePrepareArgs{Origin: fixture.origin, Mode: "branch", Branch: "feat", Sha: fixture.feat, Dir: "repo.a"}
	prepared, code := interrupted.nodeWorktreePrepare(t.Context(), first)
	result, ok := prepared.(nodeWorktreePrepareResult)
	if code != "" || !ok || result.Outcome != nodeOutcomeGitFailed {
		t.Fatalf("crashed prepare=%+v code=%q, want git failed", prepared, code)
	}

	second := nodeWorktreePrepareArgs{Origin: fixture.origin, Mode: "branch", Branch: "other", Sha: fixture.main, Dir: "repo.b"}
	prepared, code = fixture.client().nodeWorktreePrepare(t.Context(), second)
	result, ok = prepared.(nodeWorktreePrepareResult)
	if code != "" || !ok || result.Outcome != nodePreparePrepared {
		t.Fatalf("other worktree prepare=%+v code=%q, want prepared", prepared, code)
	}

	prepared, code = fixture.client().nodeWorktreePrepare(t.Context(), first)
	result, ok = prepared.(nodeWorktreePrepareResult)
	if code != "" || !ok || (result.Outcome != nodePreparePrepared && result.Outcome != nodePrepareReused) {
		t.Fatalf("replay after other add=%+v code=%q, want prepared or reused", prepared, code)
	}
}

// Finding F2 (hk 4519 comment): a signaled add that left no worktree at its
// path, because Git rolled back or the listing shows nothing there, must clear
// its own intent. Before the fix the intent file stayed behind.
func TestNodeWorktreeSignaledAddWithoutWorktreeClearsIntent(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	fixture.cloneRepo(t)
	client := fixture.client()
	client.nodeGit = func(ctx context.Context, args ...string) ([]byte, error) {
		if strings.Contains(strings.Join(args, " "), "worktree add") {
			return nil, testSignaledExit(t)
		}
		return exec.CommandContext(ctx, "git", args...).Output()
	}
	prepared, code := client.nodeWorktreePrepare(t.Context(), nodeWorktreePrepareArgs{Origin: fixture.origin, Mode: "branch", Branch: "feat", Sha: fixture.feat, Dir: "repo.job"})
	result, ok := prepared.(nodeWorktreePrepareResult)
	if code != "" || !ok || result.Outcome != nodeOutcomeGitFailed {
		t.Fatalf("signaled add without worktree prepare=%+v code=%q, want git failed", prepared, code)
	}
	leftover, err := filepath.Glob(filepath.Join(fixture.clone, ".git", "panewire-worktree-add-intent*"))
	if err != nil || len(leftover) != 0 {
		t.Fatalf("add intent left after signaled add without worktree: files=%v err=%v", leftover, err)
	}
}

// Pin F1(a) (hk 4519 comment): reclaim only an initializing lock. An intent
// that matches this add must not reclaim a worktree whose lock has another
// reason.
func TestNodeWorktreeReplayRefusesMatchingIntentWithOtherLockReason(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	fixture.cloneRepo(t)
	interrupted := fixture.client()
	interrupted.nodeGit = interruptedAddRunner(t, fixture, "operator", true)
	args := nodeWorktreePrepareArgs{Origin: fixture.origin, Mode: "branch", Branch: "feat", Sha: fixture.feat, Dir: "repo.job"}
	if _, code := interrupted.nodeWorktreePrepare(t.Context(), args); code != "" {
		t.Fatalf("interrupted prepare code=%q", code)
	}

	prepared, code := fixture.client().nodeWorktreePrepare(t.Context(), args)
	result, ok := prepared.(nodeWorktreePrepareResult)
	if code != "" || !ok || result.Outcome != nodeRemoveLocked {
		t.Fatalf("replay with operator lock=%+v code=%q, want locked", prepared, code)
	}
	listing, err := exec.Command("git", "-C", fixture.clone, "worktree", "list", "--porcelain").CombinedOutput()
	if err != nil || !strings.Contains(string(listing), "locked operator") {
		t.Fatalf("operator lock after refused replay=%q err=%v", listing, err)
	}
}

// Pin F1(b) (hk 4519 comment): reclaim only when the recorded intent matches
// this add's content. An interrupted detached add of one sha must not be
// reclaimed by a replay that asks for another sha.
func TestNodeWorktreeReplayRefusesIntentForDifferentSha(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	fixture.cloneRepo(t)
	interrupted := fixture.client()
	interrupted.nodeGit = interruptedAddRunner(t, fixture, "initializing", true)
	if _, code := interrupted.nodeWorktreePrepare(t.Context(), nodeWorktreePrepareArgs{Origin: fixture.origin, Mode: "detached", Sha: fixture.main, Fetch: "refs/heads/main", Dir: "repo.detached"}); code != "" {
		t.Fatalf("interrupted detached prepare code=%q", code)
	}

	prepared, code := fixture.client().nodeWorktreePrepare(t.Context(), nodeWorktreePrepareArgs{Origin: fixture.origin, Mode: "detached", Sha: fixture.feat, Fetch: "refs/pull/1/head", Dir: "repo.detached"})
	result, ok := prepared.(nodeWorktreePrepareResult)
	if code != "" || !ok || result.Outcome != nodeRemoveLocked {
		t.Fatalf("replay with other sha=%+v code=%q, want locked", prepared, code)
	}
	listing, err := exec.Command("git", "-C", fixture.clone, "worktree", "list", "--porcelain").CombinedOutput()
	if err != nil || !strings.Contains(string(listing), "locked initializing") {
		t.Fatalf("initializing lock after refused replay=%q err=%v", listing, err)
	}
}

// Pin F1(c) (hk 4519 comment): the add cleanup is gated on the process
// signal, not on the prepare context. Every other signal fixture also cancels
// the context, so this case is the only one that would catch a regression to
// a context gate.
func TestNodeWorktreeSignaledAddCleansUpWithoutCancelledContext(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	fixture.cloneRepo(t)
	dir := filepath.Join(fixture.workRoot, "repo.job")
	client := fixture.client()
	client.nodeGit = interruptedAddRunner(t, fixture, "initializing", false)
	prepared, code := client.nodeWorktreePrepare(t.Context(), nodeWorktreePrepareArgs{Origin: fixture.origin, Mode: "branch", Branch: "feat", Sha: fixture.feat, Dir: "repo.job"})
	result, ok := prepared.(nodeWorktreePrepareResult)
	if code != "" || !ok || result.Outcome != nodeOutcomeGitFailed {
		t.Fatalf("signaled add prepare=%+v code=%q, want git failed", prepared, code)
	}
	listing, err := exec.Command("git", "-C", fixture.clone, "worktree", "list", "--porcelain").CombinedOutput()
	if err != nil || strings.Contains(string(listing), "worktree "+dir) {
		t.Fatalf("initializing worktree survived signaled cleanup: listing=%q err=%v", listing, err)
	}
}

// Pin F1(d) (hk 4519 comment): git runs with LC_ALL=C whatever the node's own
// locale, so the parsed porcelain and error text do not depend on the host.
func TestDefaultNodeCommandRunnerPinsGitLocale(t *testing.T) {
	binDir := t.TempDir()
	probe := filepath.Join(binDir, "git")
	if err := os.WriteFile(probe, []byte("#!/bin/sh\nprintf '%s' \"$LC_ALL\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("LC_ALL", "en_US.UTF-8")
	out, err := defaultNodeCommandRunner("git")(t.Context(), "version")
	if err != nil || string(out) != "C" {
		t.Fatalf("git LC_ALL=%q err=%v, want C", out, err)
	}
}

// Finding (mergewake medium on #535): a node upgraded while a pre-upgrade add
// was interrupted holds that add's intent in the shared legacy file. Replay
// must still reclaim the initializing worktree from it, and clear the legacy
// file once it has used it.
func TestNodeWorktreePrepareReclaimsLegacySharedIntent(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	fixture.cloneRepo(t)
	dir := filepath.Join(fixture.workRoot, "repo.detached")
	if _, err := exec.Command("git", "-C", fixture.clone, "worktree", "add", "--detach", dir, fixture.main).CombinedOutput(); err != nil {
		t.Fatal(err)
	}
	if _, err := exec.Command("git", "-C", fixture.clone, "worktree", "lock", "--reason", "initializing", dir).CombinedOutput(); err != nil {
		t.Fatal(err)
	}
	legacy, err := json.Marshal(nodeWorktreeAddIntent{Path: filepath.Clean(dir), Mode: "detached", Sha: fixture.main})
	if err != nil {
		t.Fatal(err)
	}
	legacyPath := filepath.Join(fixture.clone, ".git", "panewire-worktree-add-intent.json")
	if err := os.WriteFile(legacyPath, legacy, 0600); err != nil {
		t.Fatal(err)
	}
	prepared, code := fixture.client().nodeWorktreePrepare(t.Context(), nodeWorktreePrepareArgs{
		Origin: fixture.origin, Mode: "detached", Sha: fixture.main, Fetch: "refs/heads/main", Dir: "repo.detached",
	})
	result, ok := prepared.(nodeWorktreePrepareResult)
	if code != "" || !ok || result.Outcome != nodePreparePrepared {
		t.Fatalf("replay with legacy intent=%+v code=%q, want prepared", prepared, code)
	}
	leftover, err := filepath.Glob(filepath.Join(fixture.clone, ".git", "panewire-worktree-add-intent*"))
	if err != nil || len(leftover) != 0 {
		t.Fatalf("intent files left after legacy reclaim: files=%v err=%v", leftover, err)
	}
}

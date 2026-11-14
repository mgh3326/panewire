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

// runAddChild runs one worktree add the way the node does and reports the
// identity of the git child, read while that child is alive.
func runAddChild(ctx context.Context, args ...string) (nodeAddProcess, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	if err := cmd.Start(); err != nil {
		return nodeAddProcess{}, err
	}
	proc, _ := nodeReadAddProcess(cmd.Process.Pid)
	if err := cmd.Wait(); err != nil {
		return proc, err
	}
	return proc, nil
}

// runningAddProcess starts a process and reports its identity, modelling an add
// child that outlived the node that ran it. The returned function stops it.
func runningAddProcess(t *testing.T) (nodeAddProcess, func()) {
	t.Helper()
	cmd := exec.Command("cat")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	proc, ok := nodeReadAddProcess(cmd.Process.Pid)
	if !ok || proc.Start == "" {
		t.Fatalf("identity of a live child=%+v ok=%v", proc, ok)
	}
	stop := func() {
		_ = stdin.Close()
		_ = cmd.Wait()
	}
	t.Cleanup(stop)
	return proc, stop
}

// exitedAddProcess reports the identity of a process that has already exited,
// read while it was alive.
func exitedAddProcess(t *testing.T) nodeAddProcess {
	t.Helper()
	proc, stop := runningAddProcess(t)
	stop()
	return proc
}

// interruptedAddSeams models one node's worktree add: the add creates the linked
// worktree, takes lockReason, records the identity of the git child that ran it,
// and dies from a signal. With crashed set, the cleanup listing after the signal
// fails, so the lock and the intent stay on disk: the state a node restart finds
// when its add was killed mid-flight. Without crashed, the cleanup runs as it
// does for an ordinary signal and removes an initializing worktree itself.
func interruptedAddSeams(t *testing.T, fixture *nodeWorktreeFixture, lockReason string, crashed bool) (relayCommandRunner, nodeAddRunner) {
	t.Helper()
	added := false
	git := func(ctx context.Context, args ...string) ([]byte, error) {
		if added && crashed && strings.Contains(strings.Join(args, " "), "worktree list") {
			return nil, errors.New("listing unavailable after crash")
		}
		return exec.CommandContext(ctx, "git", args...).Output()
	}
	add := func(ctx context.Context, record func(nodeAddProcess) error, args ...string) error {
		proc, err := runAddChild(ctx, args...)
		if err != nil {
			return err
		}
		dir := args[len(args)-2]
		if _, err := exec.Command("git", "-C", fixture.clone, "worktree", "lock", "--reason", lockReason, dir).CombinedOutput(); err != nil {
			t.Fatalf("create interrupted-add lock state: %v", err)
		}
		if err := record(proc); err != nil {
			t.Fatalf("record add identity: %v", err)
		}
		added = true
		return testSignaledExit(t)
	}
	return git, add
}

func worktreeListing(t *testing.T, fixture *nodeWorktreeFixture) string {
	t.Helper()
	out, err := exec.Command("git", "-C", fixture.clone, "worktree", "list", "--porcelain").CombinedOutput()
	if err != nil {
		t.Fatalf("worktree list: %v", err)
	}
	return string(out)
}

func addIntentFiles(t *testing.T, fixture *nodeWorktreeFixture) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(fixture.clone, ".git", "panewire-worktree-add-intent*"))
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// lockInterruptedDetachedAdd leaves the linked worktree an interrupted detached
// add would: created at main and locked initializing.
func lockInterruptedDetachedAdd(t *testing.T, fixture *nodeWorktreeFixture, dir string) {
	t.Helper()
	if _, err := exec.Command("git", "-C", fixture.clone, "worktree", "add", "--detach", dir, fixture.main).CombinedOutput(); err != nil {
		t.Fatal(err)
	}
	if _, err := exec.Command("git", "-C", fixture.clone, "worktree", "lock", "--reason", "initializing", dir).CombinedOutput(); err != nil {
		t.Fatal(err)
	}
}

// The add identity is the pair a replay compares: a live child reads back
// unchanged, and a child that has exited reads as gone.
func TestNodeAddProcessIdentityTracksExit(t *testing.T) {
	live, stop := runningAddProcess(t)
	if nodeAddProcessExited(live) {
		t.Fatalf("live child reads as exited: %+v", live)
	}
	if again, ok := nodeReadAddProcess(live.PID); !ok || again != live {
		t.Fatalf("identity read back=%+v ok=%v, want %+v", again, ok, live)
	}
	if nodeAddProcessExited(nodeAddProcess{}) {
		t.Fatal("an identity that was never recorded must never read as exited")
	}
	stop()
	if !nodeAddProcessExited(live) {
		t.Fatalf("exited child reads as running: %+v", live)
	}
}

// Finding (CodeRabbit Major r4562395949 on #493): a node that dies mid-add
// leaves its git child running. A replay must not remove the path that child is
// still writing, even though the recorded intent matches and git reports the
// initializing lock. Only after the child has exited may replay reclaim it.
func TestNodeWorktreeReplayKeepsLiveAddWorktree(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	fixture.cloneRepo(t)
	dir := filepath.Join(fixture.workRoot, "repo.detached")
	lockInterruptedDetachedAdd(t, fixture, dir)
	proc, stop := runningAddProcess(t)
	intent := nodeWorktreeAddIntent{Path: filepath.Clean(dir), Mode: "detached", Sha: fixture.main, Add: proc}
	if _, err := nodeWriteWorktreeAddIntent(t.Context(), defaultNodeCommandRunner(), fixture.clone, dir, intent); err != nil {
		t.Fatal(err)
	}
	args := nodeWorktreePrepareArgs{Origin: fixture.origin, Mode: "detached", Sha: fixture.main, Fetch: "refs/heads/main", Dir: "repo.detached"}

	prepared, code := fixture.client().nodeWorktreePrepare(t.Context(), args)
	result, ok := prepared.(nodeWorktreePrepareResult)
	if code != "" || !ok || result.Outcome != nodeRemoveLocked {
		t.Fatalf("replay while the add child lives=%+v code=%q, want locked", prepared, code)
	}
	listing := worktreeListing(t, fixture)
	if !strings.Contains(listing, "worktree "+dir) || !strings.Contains(listing, "locked initializing") {
		t.Fatalf("live add's worktree was touched: listing=%q", listing)
	}
	if left := addIntentFiles(t, fixture); len(left) != 1 {
		t.Fatalf("intent of the live add=%v, want it kept for the replay after exit", left)
	}

	stop()
	prepared, code = fixture.client().nodeWorktreePrepare(t.Context(), args)
	result, ok = prepared.(nodeWorktreePrepareResult)
	if code != "" || !ok || result.Outcome != nodePreparePrepared {
		t.Fatalf("replay after the add child exited=%+v code=%q, want prepared", prepared, code)
	}
}

// Finding (lead-verify F3 on #535, hk 4589): the forced remove after a signaled
// add trusts the initializing lock at the path. It must require the add child
// this attempt recorded to be gone instead; a live child keeps its worktree and
// intent, and the next replay reclaims once that child exits.
func TestNodeWorktreeSignaledAddKeepsWorktreeWhileChildLives(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	fixture.cloneRepo(t)
	dir := filepath.Join(fixture.workRoot, "repo.job")
	proc, stop := runningAddProcess(t)
	client := fixture.client()
	client.nodeAdd = func(ctx context.Context, record func(nodeAddProcess) error, args ...string) error {
		if _, err := exec.CommandContext(ctx, "git", args...).CombinedOutput(); err != nil {
			return err
		}
		if _, err := exec.Command("git", "-C", fixture.clone, "worktree", "lock", "--reason", "initializing", dir).CombinedOutput(); err != nil {
			t.Fatalf("create interrupted-add lock state: %v", err)
		}
		if err := record(proc); err != nil {
			t.Fatalf("record add identity: %v", err)
		}
		return testSignaledExit(t)
	}
	args := nodeWorktreePrepareArgs{Origin: fixture.origin, Mode: "branch", Branch: "feat", Sha: fixture.feat, Dir: "repo.job"}
	prepared, code := client.nodeWorktreePrepare(t.Context(), args)
	result, ok := prepared.(nodeWorktreePrepareResult)
	if code != "" || !ok || result.Outcome != nodeOutcomeGitFailed {
		t.Fatalf("signaled add prepare=%+v code=%q, want git failed", prepared, code)
	}
	listing := worktreeListing(t, fixture)
	if !strings.Contains(listing, "worktree "+dir) || !strings.Contains(listing, "locked initializing") {
		t.Fatalf("worktree of a live add child was removed: listing=%q", listing)
	}
	if left := addIntentFiles(t, fixture); len(left) != 1 {
		t.Fatalf("intent of the live add=%v, want it kept", left)
	}

	stop()
	client.nodeAdd = nil
	prepared, code = client.nodeWorktreePrepare(t.Context(), args)
	result, ok = prepared.(nodeWorktreePrepareResult)
	if code != "" || !ok || (result.Outcome != nodePreparePrepared && result.Outcome != nodePrepareReused) {
		t.Fatalf("replay after the add child exited=%+v code=%q, want prepared or reused", prepared, code)
	}
	if left := addIntentFiles(t, fixture); len(left) != 0 {
		t.Fatalf("intent files left after reclaim: %v", left)
	}
}

// Pin F3 (hk 4589): a matching intent without a recorded add process is no
// evidence that the add child is gone, so the initializing lock alone must not
// authorize a forced remove.
func TestNodeWorktreeReplayRefusesIntentWithoutAddProcess(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	fixture.cloneRepo(t)
	dir := filepath.Join(fixture.workRoot, "repo.detached")
	lockInterruptedDetachedAdd(t, fixture, dir)
	intent := nodeWorktreeAddIntent{Path: filepath.Clean(dir), Mode: "detached", Sha: fixture.main}
	if _, err := nodeWriteWorktreeAddIntent(t.Context(), defaultNodeCommandRunner(), fixture.clone, dir, intent); err != nil {
		t.Fatal(err)
	}
	prepared, code := fixture.client().nodeWorktreePrepare(t.Context(), nodeWorktreePrepareArgs{
		Origin: fixture.origin, Mode: "detached", Sha: fixture.main, Fetch: "refs/heads/main", Dir: "repo.detached",
	})
	result, ok := prepared.(nodeWorktreePrepareResult)
	if code != "" || !ok || result.Outcome != nodeRemoveLocked {
		t.Fatalf("replay without an add identity=%+v code=%q, want locked", prepared, code)
	}
	if listing := worktreeListing(t, fixture); !strings.Contains(listing, "locked initializing") {
		t.Fatalf("initializing lock after refused replay=%q", listing)
	}
}

// Finding (PR #493 thread 4561204551, deferred to hk 4519): an interrupted add
// must still replay after a different worktree in the same clone has added
// successfully. Every add writes its own intent file, so the other add's success
// leaves this add's intent in place.
func TestNodeWorktreeReplayKeepsIntentAcrossOtherWorktreeAdds(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	fixture.cloneRepo(t)
	interrupted := fixture.client()
	interrupted.nodeGit, interrupted.nodeAdd = interruptedAddSeams(t, fixture, "initializing", true)
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
// path, because git rolled back or the listing shows nothing there, must clear
// its own intent.
func TestNodeWorktreeSignaledAddWithoutWorktreeClearsIntent(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	fixture.cloneRepo(t)
	client := fixture.client()
	client.nodeAdd = func(context.Context, func(nodeAddProcess) error, ...string) error {
		return testSignaledExit(t)
	}
	prepared, code := client.nodeWorktreePrepare(t.Context(), nodeWorktreePrepareArgs{Origin: fixture.origin, Mode: "branch", Branch: "feat", Sha: fixture.feat, Dir: "repo.job"})
	result, ok := prepared.(nodeWorktreePrepareResult)
	if code != "" || !ok || result.Outcome != nodeOutcomeGitFailed {
		t.Fatalf("signaled add without worktree prepare=%+v code=%q, want git failed", prepared, code)
	}
	if left := addIntentFiles(t, fixture); len(left) != 0 {
		t.Fatalf("add intent left after signaled add without worktree: files=%v", left)
	}
}

// Pin F1(a) (hk 4519 comment): reclaim only an initializing lock. An intent that
// matches this add must not reclaim a worktree whose lock has another reason.
func TestNodeWorktreeReplayRefusesMatchingIntentWithOtherLockReason(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	fixture.cloneRepo(t)
	interrupted := fixture.client()
	interrupted.nodeGit, interrupted.nodeAdd = interruptedAddSeams(t, fixture, "operator", true)
	args := nodeWorktreePrepareArgs{Origin: fixture.origin, Mode: "branch", Branch: "feat", Sha: fixture.feat, Dir: "repo.job"}
	if _, code := interrupted.nodeWorktreePrepare(t.Context(), args); code != "" {
		t.Fatalf("interrupted prepare code=%q", code)
	}

	prepared, code := fixture.client().nodeWorktreePrepare(t.Context(), args)
	result, ok := prepared.(nodeWorktreePrepareResult)
	if code != "" || !ok || result.Outcome != nodeRemoveLocked {
		t.Fatalf("replay with operator lock=%+v code=%q, want locked", prepared, code)
	}
	if listing := worktreeListing(t, fixture); !strings.Contains(listing, "locked operator") {
		t.Fatalf("operator lock after refused replay=%q", listing)
	}
}

// Pin F1(b) (hk 4519 comment): reclaim only when the recorded intent matches
// this add's content. An interrupted detached add of one sha must not be
// reclaimed by a replay that asks for another sha.
func TestNodeWorktreeReplayRefusesIntentForDifferentSha(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	fixture.cloneRepo(t)
	interrupted := fixture.client()
	interrupted.nodeGit, interrupted.nodeAdd = interruptedAddSeams(t, fixture, "initializing", true)
	if _, code := interrupted.nodeWorktreePrepare(t.Context(), nodeWorktreePrepareArgs{Origin: fixture.origin, Mode: "detached", Sha: fixture.main, Fetch: "refs/heads/main", Dir: "repo.detached"}); code != "" {
		t.Fatalf("interrupted detached prepare code=%q", code)
	}

	prepared, code := fixture.client().nodeWorktreePrepare(t.Context(), nodeWorktreePrepareArgs{Origin: fixture.origin, Mode: "detached", Sha: fixture.feat, Fetch: "refs/pull/1/head", Dir: "repo.detached"})
	result, ok := prepared.(nodeWorktreePrepareResult)
	if code != "" || !ok || result.Outcome != nodeRemoveLocked {
		t.Fatalf("replay with other sha=%+v code=%q, want locked", prepared, code)
	}
	if listing := worktreeListing(t, fixture); !strings.Contains(listing, "locked initializing") {
		t.Fatalf("initializing lock after refused replay=%q", listing)
	}
}

// Pin F1(c) (hk 4519 comment): the add cleanup is gated on the process signal,
// not on the prepare context. Every other signal fixture also cancels the
// context, so this case is the only one that would catch a regression to a
// context gate.
func TestNodeWorktreeSignaledAddCleansUpWithoutCancelledContext(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	fixture.cloneRepo(t)
	dir := filepath.Join(fixture.workRoot, "repo.job")
	client := fixture.client()
	client.nodeGit, client.nodeAdd = interruptedAddSeams(t, fixture, "initializing", false)
	prepared, code := client.nodeWorktreePrepare(t.Context(), nodeWorktreePrepareArgs{Origin: fixture.origin, Mode: "branch", Branch: "feat", Sha: fixture.feat, Dir: "repo.job"})
	result, ok := prepared.(nodeWorktreePrepareResult)
	if code != "" || !ok || result.Outcome != nodeOutcomeGitFailed {
		t.Fatalf("signaled add prepare=%+v code=%q, want git failed", prepared, code)
	}
	if listing := worktreeListing(t, fixture); strings.Contains(listing, "worktree "+dir) {
		t.Fatalf("initializing worktree survived signaled cleanup: listing=%q", listing)
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
	out, err := defaultNodeCommandRunner()(t.Context(), "version")
	if err != nil || string(out) != "C" {
		t.Fatalf("git LC_ALL=%q err=%v, want C", out, err)
	}
}

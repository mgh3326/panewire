package panewire

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Every ASCII control byte is refused up front in ref names, fetch refs,
// regenerable globs and origins: none of them is a value git should see.
func TestNodeRefNamesRefuseEveryControlByte(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	controls := []byte{0x7f}
	for b := byte(0); b < 0x20; b++ {
		controls = append(controls, b)
	}
	for _, b := range controls {
		bad := "feat" + string([]byte{b}) + "x"
		if validNodeRefName(bad) {
			t.Errorf("ref %q accepted", bad)
		}
		if validNodeFetchRef("refs/heads/" + bad) {
			t.Errorf("fetch ref %q accepted", bad)
		}
		if validNodeGlob(bad + ".log") {
			t.Errorf("glob %q accepted", bad)
		}
		if _, ok := nodeOriginRepo(fixture.origin + string([]byte{b})); ok {
			t.Errorf("origin with byte %#x accepted", b)
		}
		branch := fixture.prepareArgs("branch", bad, "", "", "repo.job")
		if _, code := nodeRun(t, client, nodeCommandWorktreePrepare, branch); code != "invalid_args" {
			t.Errorf("branch with byte %#x: code=%q", b, code)
		}
		remove := fixture.removeArgs(filepath.Join(fixture.workRoot, "repo.job"), fixture.main, nil)
		remove["base"] = bad
		if _, code := nodeRun(t, client, nodeCommandWorktreeRemove, remove); code != "invalid_args" {
			t.Errorf("base with byte %#x: code=%q", b, code)
		}
	}
	if !validNodeRefName("feat/x") {
		t.Fatal("plain ref refused")
	}
}

// A one-character scp host is a drive letter (C:owner/repo), not a remote.
func TestNodeOriginRefusesDriveLetterHost(t *testing.T) {
	for _, raw := range []string{"C:owner/repo", "c:owner/repo.git", "C:/owner/repo"} {
		if _, ok := nodeOriginRepo(raw); ok {
			t.Errorf("origin %q accepted", raw)
		}
		if _, ok := nodeCloneRemoteRepo("git@" + raw); ok {
			t.Errorf("clone remote git@%s accepted", raw)
		}
	}
	if got, ok := nodeOriginRepo("gh:owner/repo.git"); !ok || got != "owner/repo" {
		t.Fatalf("two-character host: got %q %v", got, ok)
	}
	fixture := newNodeWorktreeFixture(t)
	args := fixture.prepareArgs("detached", "", fixture.main, "refs/heads/main", "repo.job")
	args["origin"] = "C:owner/repo"
	if _, code := nodeRun(t, fixture.client(), nodeCommandWorktreePrepare, args); code != "invalid_args" {
		t.Fatalf("drive-letter origin code=%q", code)
	}
}

// An existing clone's scp remote is split at its first ':'; only the part
// before it can hold userinfo, so an '@' in the repo path is kept.
func TestNodeCloneRemoteRepoAtInPath(t *testing.T) {
	for raw, want := range map[string]string{
		"git@example.invalid:owner/re@po.git":       "owner/re@po",
		"example.invalid:owner/re@po.git":           "owner/re@po",
		"a@b@example.invalid:owner/repo.git":        "owner/repo",
		"ssh://git@example.invalid/owner/re@po.git": "owner/re@po",
	} {
		if got, ok := nodeCloneRemoteRepo(raw); !ok || got != want {
			t.Errorf("%s: got %q %v, want %q", raw, got, ok, want)
		}
	}
	for _, raw := range []string{"git@:owner/repo", "-git@example.invalid:owner/repo", "git@example.invalid:owner/re po"} {
		if _, ok := nodeCloneRemoteRepo(raw); ok {
			t.Errorf("%q accepted", raw)
		}
	}
	// Caller-supplied origins keep refusing any '@'.
	if _, ok := nodeOriginRepo("example.invalid:owner/re@po.git"); ok {
		t.Fatal("caller origin with '@' accepted")
	}
}

// A caller that goes away while the per-clone lock is contended is told so;
// busy (with retry_after_s) stays for a lock still held when the wait ends.
func TestNodeGitOpLockCancelledCaller(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	fixture.cloneRepo(t)
	dir, head := fixture.addWorktree(t, "cancel")
	release, err := tryNodeGitOpLock(fixture.clone + ".git-op.lock")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	client := fixture.client()
	client.nodeGitLockWait = 30 * time.Second

	for _, tc := range []struct {
		op   string
		args map[string]any
	}{
		{nodeCommandWorktreePrepare, fixture.prepareArgs("detached", "", fixture.feat, "pull/1/head", "repo.job")},
		{nodeCommandWorktreeRemove, fixture.removeArgs(dir, head, nil)},
	} {
		raw, err := json.Marshal(tc.args)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		timer := time.AfterFunc(200*time.Millisecond, cancel)
		started := time.Now()
		result, code := client.runNodeCommand(ctx, tc.op, raw)
		timer.Stop()
		cancel()
		if code != "" {
			t.Fatalf("%s: code=%q", tc.op, code)
		}
		encoded, err := json.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		if _, valid := canonicalNodeCommandResult(tc.op, encoded); !valid {
			t.Fatalf("%s: %s is outside the documented shape", tc.op, encoded)
		}
		var decoded map[string]any
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded["outcome"] != nodeOutcomeCancelled || decoded["retry_after_s"] != nil {
			t.Fatalf("%s: cancelled caller got %s", tc.op, encoded)
		}
		if elapsed := time.Since(started); elapsed > 10*time.Second {
			t.Fatalf("%s: cancellation took %s", tc.op, elapsed)
		}
	}

	// An already-cancelled caller never takes a free lock.
	release()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if unlock, err := acquireNodeGitOpLock(ctx, fixture.clone+".git-op.lock", time.Second); err == nil {
		unlock()
		t.Fatal("cancelled caller took the lock")
	}
}

// prepare and remove both name the lock from one canonical clone path. A
// clone path that cannot be resolved (here a dangling symlink) fails closed
// instead of falling back to a second, unresolved lock name.
func TestNodeGitOpLockPathResolutionFailure(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	if err := os.Symlink(filepath.Join(fixture.workRoot, "repo-missing"), fixture.clone); err != nil {
		t.Skip(err)
	}
	if _, err := nodeCanonicalClonePath(fixture.clone); err == nil {
		t.Fatal("dangling clone symlink resolved")
	}
	// Hold the name the unresolved fallback would have used: an op that
	// still invented it would answer busy here.
	release, err := tryNodeGitOpLock(fixture.clone + ".git-op.lock")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	client := fixture.client()
	client.nodeGitLockWait = 50 * time.Millisecond
	result, code := nodeRun(t, client, nodeCommandWorktreePrepare, fixture.prepareArgs("detached", "", fixture.feat, "pull/1/head", "repo.job"))
	if code != "" || result["outcome"] != nodeOutcomeGitFailed {
		t.Fatalf("outcome=%v code=%q", result["outcome"], code)
	}
	if _, err := os.Lstat(filepath.Join(fixture.workRoot, "repo-missing")); !os.IsNotExist(err) {
		t.Fatalf("clone created through a dangling symlink: %v", err)
	}

	// A resolvable clone, named through a symlinked work root (prepare) or
	// through git's resolved common dir (remove), gives one lock name.
	link := filepath.Join(t.TempDir(), "root-link")
	if err := os.Symlink(fixture.workRoot, link); err != nil {
		t.Skip(err)
	}
	real := filepath.Join(fixture.workRoot, "real")
	if err := os.Mkdir(real, 0o700); err != nil {
		t.Fatal(err)
	}
	a, errA := nodeGitOpLockPath(filepath.Join(link, "real"))
	b, errB := nodeGitOpLockPath(real)
	if errA != nil || errB != nil || a != b {
		t.Fatalf("lock names %q (%v) and %q (%v)", a, errA, b, errB)
	}
}

func mustNodeGitOpLockPath(t *testing.T, clone string) string {
	t.Helper()
	lockPath, err := nodeGitOpLockPath(clone)
	if err != nil {
		t.Fatalf("lock path for %s: %v", clone, err)
	}
	return lockPath
}

package fleet

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// F3 (hk 5063): if serveWatchLock fails (e.g. because the socket directory is
// not private), the watcher continues to watch the job without lock-lending
// or hub-close awareness rather than aborting.
func TestWatcherRunsWhenLockLendingSocketFails_t5063(t *testing.T) {
	base := filepath.Join(t.TempDir(), "base")
	if err := os.MkdirAll(base, 0o700); err != nil {
		t.Fatal(err)
	}
	// Make the base dir non-private (0755) so watchLockSocketDir fails on long paths.
	if err := os.Chmod(base, 0o755); err != nil {
		t.Fatal(err)
	}
	prev := watchLockBaseDirHook
	watchLockBaseDirHook = func() string { return base }
	t.Cleanup(func() { watchLockBaseDirHook = prev })

	a, _ := hubCloseFixture(t)
	// Create a long job path to force the /tmp/panewire-<uid>/<hash> path.
	longID := "j-" + strings.Repeat("x", 100)
	if e := a.event(context.Background(), longID, "job.spawned", map[string]any{
		"pane_id": "w:p1", "label": "agent-long", "role": "reviewer", "herdr_session": "old-session",
	}); e != nil {
		t.Fatal(e)
	}

	// Watch must run and succeed (return nil) despite serveWatchLock failing.
	if err := a.watch(context.Background(), []string{longID}); err != nil {
		t.Fatalf("watch returned error on lending socket failure: %v", err)
	}

	// Because lending was disabled, hubCloseWatchLock was not taken; the probe
	// must not report aware.
	jobDir := filepath.Join(a.Jobs, longID)
	status, err := ProbeHubCloseWatcher(jobDir)
	if err != nil {
		t.Fatal(err)
	}
	if status == HubCloseWatcherAware {
		t.Fatalf("ProbeHubCloseWatcher = %v, want not aware", status)
	}
}

// F3 (hk 5063): serveWatchLock cleans up its socket directory on stop.
func TestServeWatchLockCleansUpSocketDir_t5063(t *testing.T) {
	base := filepath.Join(t.TempDir(), "base")
	if err := os.MkdirAll(base, 0o700); err != nil {
		t.Fatal(err)
	}
	prev := watchLockBaseDirHook
	watchLockBaseDirHook = func() string { return base }
	t.Cleanup(func() { watchLockBaseDirHook = prev })

	// Temp job dir with long path.
	tempJobs := t.TempDir()
	longJob := filepath.Join(tempJobs, "job-"+strings.Repeat("y", 100))
	if err := os.MkdirAll(longJob, 0o700); err != nil {
		t.Fatal(err)
	}

	lock, err := os.OpenFile(filepath.Join(longJob, jobWatchLock), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()

	stop, err := serveWatchLock(longJob, lock)
	if err != nil {
		t.Fatal(err)
	}

	sockDir, err := watchLockSocketDir(longJob)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(sockDir); err != nil {
		t.Fatalf("socket dir %s missing while serving: %v", sockDir, err)
	}

	stop()

	// The hash dir under /tmp/panewire-<uid> must be removed.
	if _, err := os.Stat(sockDir); !os.IsNotExist(err) {
		t.Fatalf("socket dir %s still exists after stop(): %v", sockDir, err)
	}
}

// F6 / M4b, M4c (hk 5063): verifyPrivateDir enforces user ownership (M4b), and
// watchLockSocketDir enforces the privacy of /tmp/panewire-<uid> (M4c).
func TestPrivateDirOwnerAndPrivacyGuards_t5063(t *testing.T) {
	temp := t.TempDir()
	if err := os.Chmod(temp, 0o700); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(temp)
	if err != nil {
		t.Fatal(err)
	}

	// M4b: verifyPrivateDir must reject a directory whose uid != current uid.
	if err := verifyPrivateDir(temp, info, os.Getuid()); err != nil {
		t.Fatalf("verifyPrivateDir rejected current user: %v", err)
	}
	if err := verifyPrivateDir(temp, info, os.Getuid()+1); err == nil {
		t.Fatal("verifyPrivateDir accepted mismatched uid")
	}

	// M4c: watchLockSocketDir must fail when base dir is group/other accessible.
	base := filepath.Join(t.TempDir(), "base")
	if err := os.MkdirAll(base, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(base, 0o755); err != nil {
		t.Fatal(err)
	}
	prev := watchLockBaseDirHook
	watchLockBaseDirHook = func() string { return base }
	t.Cleanup(func() { watchLockBaseDirHook = prev })

	longPath := filepath.Join(temp, strings.Repeat("z", 100))
	if _, err := watchLockSocketDir(longPath); err == nil {
		t.Fatal("watchLockSocketDir succeeded with non-private base dir")
	}
}

// Review finding 4613454419 (hk 5063): when lending is disabled, the watcher
// must not spin on an open hub-close intent; it exits immediately so the node
// can take the fence without stalling.
func TestWatcherExitsOnOpenIntentWhenLendingDisabled_t5063(t *testing.T) {
	base := filepath.Join(t.TempDir(), "base")
	if err := os.MkdirAll(base, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(base, 0o755); err != nil {
		t.Fatal(err)
	}
	prev := watchLockBaseDirHook
	watchLockBaseDirHook = func() string { return base }
	t.Cleanup(func() { watchLockBaseDirHook = prev })

	a, _ := hubCloseFixture(t)
	longID := "j-" + strings.Repeat("x", 100)
	if e := a.event(context.Background(), longID, "job.spawned", map[string]any{
		"pane_id": "w:p1", "label": "agent-long", "role": "reviewer", "herdr_session": "old-session",
	}); e != nil {
		t.Fatal(e)
	}
	// Record an open hub-close intent before watch starts.
	if e := a.event(context.Background(), longID, hubCloseIntentKind, map[string]any{
		"request_id": "req-1", "attempt": "old-session", "pane": "w:p1", "label": "agent-long", "end_reason": hubCloseReason,
	}); e != nil {
		t.Fatal(e)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Watch must exit immediately (return nil) without sleeping or stalling on the intent.
	if err := a.watch(ctx, []string{longID}); err != nil {
		t.Fatalf("watch returned error on open intent when degraded: %v", err)
	}

	// Once watch returns, its watch lock is released so FenceWatchers can take the fence.
	jobDir := filepath.Join(a.Jobs, longID)
	release, ok, err := FenceWatchers(jobDir)
	if err != nil || !ok {
		t.Fatalf("FenceWatchers failed after watcher exited: ok=%v err=%v", ok, err)
	}
	release()
}

// Review finding 4613443497 (hk 5063): a degraded watcher holding
// hubCloseWatchLockDegraded reports HubCloseWatcherDegraded and does not block
// HubCloseWatchersReady or CloseOpsReady for the node.
func TestDegradedWatcherDoesNotBlockHubCloseWatchersReady_t5063(t *testing.T) {
	jobs := t.TempDir()
	jobDir := filepath.Join(jobs, "job-deg")
	holdLock(t, jobDir, jobWatchLock)
	holdLock(t, jobDir, hubCloseWatchLockDegraded)

	status, err := ProbeHubCloseWatcher(jobDir)
	if err != nil {
		t.Fatal(err)
	}
	if status != HubCloseWatcherDegraded {
		t.Fatalf("ProbeHubCloseWatcher = %v, want HubCloseWatcherDegraded", status)
	}

	ready, err := HubCloseWatchersReady(context.Background(), jobs, listing("  101 /usr/local/bin/panewire _watch job-deg\n"))
	if err != nil || !ready {
		t.Fatalf("HubCloseWatchersReady = %v, err = %v, want true", ready, err)
	}

	now := time.Now().UTC()
	if err := EnableCloseOps(context.Background(), jobs, listing("  101 /usr/local/bin/panewire _watch job-deg\n"), now); err != nil {
		t.Fatalf("EnableCloseOps failed with degraded watcher: %v", err)
	}
	if err := AdvertiseCloseOps(jobs, now); err != nil {
		t.Fatalf("AdvertiseCloseOps failed: %v", err)
	}
	if !CloseOpsReady(context.Background(), jobs, listing("  101 /usr/local/bin/panewire _watch job-deg\n")) {
		t.Fatal("CloseOpsReady = false, want true with degraded watcher")
	}
}

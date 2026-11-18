package fleet

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// F3 (hk 5063): if serveWatchLock fails (e.g. because the socket directory is
// not private), the watcher continues to watch the job without lock-lending
// or hub-close awareness rather than aborting. It holds only the watch lock;
// ProbeHubCloseWatcher reports HubCloseWatcherIncompatible while it runs.
func TestWatcherRunsWhenLockLendingSocketFails_t5063(t *testing.T) {
	base, err := os.MkdirTemp("/tmp", "pw-sock-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
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

	var hookRan bool
	sleep := a.Sleep
	a.Sleep = func(ctx context.Context, d time.Duration) error {
		hookRan = true
		jobDir := filepath.Join(a.Jobs, longID)
		status, err := ProbeHubCloseWatcher(jobDir)
		if err != nil {
			t.Errorf("ProbeHubCloseWatcher in sleep hook: %v", err)
		}
		if status != HubCloseWatcherIncompatible {
			t.Errorf("ProbeHubCloseWatcher in sleep hook = %v, want HubCloseWatcherIncompatible", status)
		}
		return sleep(ctx, d)
	}

	// Watch must run and succeed (return nil) despite serveWatchLock failing.
	if err := a.watch(context.Background(), []string{longID}); err != nil {
		t.Fatalf("watch returned error on lending socket failure: %v", err)
	}

	if !hookRan {
		t.Fatal("Sleep hook did not run to observe watcher during execution")
	}
}

// F3 (hk 5063): serveWatchLock cleans up its socket directory on stop.
func TestServeWatchLockCleansUpSocketDir_t5063(t *testing.T) {
	base, err := os.MkdirTemp("/tmp", "pw-sock-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
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
	var stopOnce sync.Once
	stopClean := func() {
		stopOnce.Do(stop)
	}
	t.Cleanup(stopClean)

	sockDir, err := watchLockSocketDir(longJob)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(sockDir); err != nil {
		t.Fatalf("socket dir %s missing while serving: %v", sockDir, err)
	}

	stopClean()

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
	base, err := os.MkdirTemp("/tmp", "pw-sock-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
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
	base, err := os.MkdirTemp("/tmp", "pw-sock-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
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

	j, err := a.job(longID)
	if err != nil {
		t.Fatal(err)
	}
	var attempt string
	for _, ev := range j.Events {
		if ev.Kind == "job.spawned" {
			attempt = ev.name
		}
	}
	if attempt == "" {
		t.Fatal("no job.spawned event found")
	}

	// Record an open hub-close intent before watch starts.
	in := HubCloseIntent{
		RequestID: "close-1",
		Attempt:   attempt,
		Pane:      "w:p1",
		Label:     "agent-long",
		EndReason: hubCloseReason,
	}
	if err := a.recordHubCloseIntent(context.Background(), longID, in); err != nil {
		t.Fatal(err)
	}

	a.Sleep = func(ctx context.Context, d time.Duration) error {
		t.Fatalf("a.Sleep called (%v); watcher should exit immediately when lending disabled on open intent", d)
		return nil
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

// Review findings PRRT_kwDOUGxnCM6473EX & PRRT_kwDOUGxnCM648nmI (hk 5063): a
// watcher whose lock-lending socket fails runs holding only .panewire-watch.lock;
// it reads as HubCloseWatcherIncompatible, HubCloseWatchersReady is false while
// it runs, and close-ops cannot be enabled.
func TestNonLendingWatcherIsIncompatible_t5063(t *testing.T) {
	jobs := t.TempDir()
	jobDir := filepath.Join(jobs, "job-deg")
	holdLock(t, jobDir, jobWatchLock)

	status, err := ProbeHubCloseWatcher(jobDir)
	if err != nil {
		t.Fatal(err)
	}
	if status != HubCloseWatcherIncompatible {
		t.Fatalf("ProbeHubCloseWatcher = %v, want HubCloseWatcherIncompatible", status)
	}

	ready, err := HubCloseWatchersReady(context.Background(), jobs, listing("  101 /usr/local/bin/panewire _watch job-deg\n"))
	if err != nil {
		t.Fatal(err)
	}
	if ready {
		t.Fatal("HubCloseWatchersReady = true, want false with non-lending watcher")
	}

	now := time.Now().UTC()
	if err := EnableCloseOps(context.Background(), jobs, listing("  101 /usr/local/bin/panewire _watch job-deg\n"), now); err == nil {
		t.Fatal("EnableCloseOps succeeded with non-lending watcher, want failure")
	}
}

package fleet

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// #662 consult NCxM: the node borrows the watcher's own locked description.
// When the watcher's reference closes (it exits or is killed), the lock stays
// held, so no other watcher can take it; releasing the borrowed reference
// frees it.
func TestBorrowedWatchLockOutlivesTheWatcher_t4632(t *testing.T) {
	dir := t.TempDir()
	lock, err := os.OpenFile(filepath.Join(dir, jobWatchLock), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if !tryLock(lock) {
		t.Fatal("watch lock busy")
	}
	stop, err := serveWatchLock(dir, lock)
	if err != nil {
		t.Fatal(err)
	}
	release, err := BorrowWatchLock(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	stop()
	lock.Close() // the watcher is gone
	if _, ok, err := FenceWatchers(dir); err != nil || ok {
		t.Fatalf("watch lock free after the watcher exited: ok=%v err=%v", ok, err)
	}
	release()
	free, ok, err := FenceWatchers(dir)
	if err != nil || !ok {
		t.Fatalf("watch lock still held after the node released: ok=%v err=%v", ok, err)
	}
	free()
}

// A running watcher lends its lock: the node's borrow succeeds while it runs
// and the probe reads it as aware; with no watcher, borrowing fails.
func TestRunningWatcherLendsItsLock_t4632(t *testing.T) {
	a, _ := hubCloseFixture(t)
	dir := filepath.Join(a.Jobs, "j")
	if _, err := BorrowWatchLock(context.Background(), dir); err == nil {
		t.Fatal("borrowed with no watcher running")
	}
	sleep := a.Sleep
	lent := false
	a.Sleep = func(ctx context.Context, d time.Duration) error {
		if !lent {
			if release, err := BorrowWatchLock(ctx, dir); err == nil {
				lent = true
				release()
			}
		}
		return sleep(ctx, d)
	}
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if !lent {
		t.Fatal("the running watcher did not lend its lock")
	}
}

// #662 round 4: the lending socket lives in a directory only this user can
// enter, made before the socket binds; a pre-existing directory open to
// others is refused, not used.
func TestWatchLockSocketDirIsPrivate_t4632(t *testing.T) {
	// t.TempDir can already exceed sockaddr_un's limit on macOS. Start under
	// the short /tmp spelling so both path-selection branches are exercised.
	base, err := os.MkdirTemp("/tmp", "pw-sock-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	for _, tc := range []struct {
		name string
		job  string
		long bool
	}{
		{"short", filepath.Join(base, "job"), false},
		{"long", filepath.Join(base, strings.Repeat("j", 100)), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.Mkdir(tc.job, 0o700); err != nil {
				t.Fatal(err)
			}
			dir, err := watchLockSocketDir(tc.job)
			if err != nil {
				t.Fatal(err)
			}
			// The fallback is outside base and must be removed separately.
			t.Cleanup(func() { _ = os.Chmod(dir, 0o700); _ = os.RemoveAll(dir) })
			if gotLong := dir != filepath.Join(tc.job, ".panewire-watch-sock"); gotLong != tc.long {
				t.Fatalf("socket dir %s: long-path branch=%v, want %v", dir, gotLong, tc.long)
			}
			info, err := os.Stat(dir)
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode().Perm() != 0o700 {
				t.Fatalf("socket dir %s mode %v; want 0700", dir, info.Mode().Perm())
			}
			if err := os.Chmod(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			if _, err := watchLockSocketDir(tc.job); err == nil {
				t.Fatal("used a socket directory open to others")
			}
		})
	}
}

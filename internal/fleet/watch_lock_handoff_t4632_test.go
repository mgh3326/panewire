package fleet

import (
	"context"
	"os"
	"path/filepath"
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

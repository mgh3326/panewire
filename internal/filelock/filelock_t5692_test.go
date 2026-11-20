package filelock

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// T5692: Acquire waits for a held lock only while its context is live. A
// holder that outlasts the context returns the context's error, not a lock.
func TestAcquireGivesUpWhenHolderOutlastsContext_t5692(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".wrk-events.lock")
	holder, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0o666)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	if err := syscall.Flock(int(holder.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	release, err := Acquire(ctx, path)
	if err == nil {
		release()
		t.Fatal("Acquire took a lock that another descriptor holds")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Acquire error = %v, want the context deadline", err)
	}
}

// T5692: the release from Acquire frees the lock for the next holder.
func TestAcquireReleasesForNextHolder_t5692(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".wrk-events.lock")
	release, err := Acquire(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	release()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	again, err := Acquire(ctx, path)
	if err != nil {
		t.Fatalf("lock was not released: %v", err)
	}
	again()
}

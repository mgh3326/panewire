package filelock

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// T5801: a context that ends between the pre-flock check and the flock must
// be honoured after the flock succeeds: Acquire returns the context's error,
// and the lock it just took is dropped, so the next Acquire gets it at once.
// The hook cancels at that exact point; no sleep orders the test.
func TestAcquireRechecksContextAfterFlock_t5801(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".wrk-events.lock")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release, err := acquire(ctx, path, cancel)
	if err == nil {
		release()
		t.Fatal("Acquire returned a lock although its context ended before the flock")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Acquire error = %v, want the context's error", err)
	}

	after, cancelAfter := context.WithTimeout(context.Background(), time.Second)
	defer cancelAfter()
	again, err := Acquire(after, path)
	if err != nil {
		t.Fatalf("lock was left held after the refused Acquire: %v", err)
	}
	again()
}

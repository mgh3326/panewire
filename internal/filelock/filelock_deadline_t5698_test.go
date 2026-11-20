package filelock

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// T5698: a poll never takes the lock once its context is done. With the
// deadline already past and the lock free, the first non-blocking attempt
// would succeed, so Acquire must refuse before it tries at all.
func TestAcquireRefusesLockAfterDeadline_t5698(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".wrk-events.lock")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	release, err := Acquire(ctx, path)
	if err == nil {
		release()
		t.Fatal("Acquire took the lock after its context was done")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Acquire error = %v, want the context's error", err)
	}

	// A refused Acquire must not leave the lock held behind it.
	after, cancelAfter := context.WithTimeout(context.Background(), time.Second)
	defer cancelAfter()
	again, err := Acquire(after, path)
	if err != nil {
		t.Fatalf("lock was left held by the refused Acquire: %v", err)
	}
	again()
}

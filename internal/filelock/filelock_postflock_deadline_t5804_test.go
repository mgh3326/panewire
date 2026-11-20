package filelock

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// T5804: a context whose only end is its deadline (no cancel call, so
// ctx.Done is non-nil and ctx.Err is nil until the runtime timer fires) must
// still be honoured after the flock: the deadline passes between the
// pre-flock check and the flock, Acquire returns context.DeadlineExceeded,
// and the lock it just took is dropped. The hook stalls past the deadline;
// that stall is simulated latency, not an ordering sleep.
func TestAcquireRechecksDeadlineOnlyAfterFlock_t5804(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".wrk-events.lock")
	deadline := time.Now().Add(50 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	ran := false
	stall := func() {
		ran = true
		// Simulated latency: hold the caller until the deadline has passed.
		time.Sleep(time.Until(deadline) + 10*time.Millisecond)
	}

	release, err := acquire(ctx, path, stall)
	if err == nil {
		release()
		t.Fatal("Acquire returned a lock although its deadline passed before the flock")
	}
	if !ran {
		t.Fatal("the flock hook never ran; the deadline expired before the pre-flock check, so the post-flock check was not exercised")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Acquire error = %v, want context.DeadlineExceeded", err)
	}

	after, cancelAfter := context.WithTimeout(context.Background(), time.Second)
	defer cancelAfter()
	again, err := Acquire(after, path)
	if err != nil {
		t.Fatalf("lock was left held after the refused Acquire: %v", err)
	}
	again()
}

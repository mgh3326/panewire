// Package filelock takes the exclusive flock that serialises a job's event
// writers (<job dir>/.wrk-events.lock), with the wait bounded by a context.
package filelock

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"
)

// pollInterval paces the retries while a non-blocking flock finds the lock held.
const pollInterval = 20 * time.Millisecond

// Acquire opens (creating it if needed) the lock file at path and takes it
// exclusively. The wait ends when ctx is done, and the error then wraps
// ctx.Err(), so a stalled holder cannot hold the caller past its own
// deadline. A context that can never be done blocks until the lock is free.
// The returned release drops the lock and closes the file.
func Acquire(ctx context.Context, path string) (func(), error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0o666)
	if err != nil {
		return nil, err
	}
	if err := lock(ctx, f); err != nil {
		f.Close()
		return nil, fmt.Errorf("lock %s: %w", path, err)
	}
	return func() {
		syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, nil
}

// lock mirrors the bounded events-lock wait in internal/arbiter (flockContext).
func lock(ctx context.Context, f *os.File) error {
	if ctx.Done() == nil {
		return syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
	}
	for {
		// A done context ends the wait before another attempt, so a lock that
		// frees after the deadline is never taken (T5698).
		if err := ctx.Err(); err != nil {
			return err
		}
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			// The flock can succeed just after the deadline; drop it then.
			if err := ctx.Err(); err != nil {
				syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
				return err
			}
			return nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EINTR) {
			return err
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

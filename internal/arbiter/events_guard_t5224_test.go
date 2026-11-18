package arbiter

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// AppendEventIf runs its guard once, under the events lock and before the
// event is numbered; a guard error appends nothing and comes back as it is
// (hk 5224).
func TestAppendEventIfRunsTheGuardUnderTheEventsLock_t5224(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	if _, e := s.AppendEvent(ctx, "j1", "job.note", nil, nil); e != nil {
		t.Fatal(e)
	}
	refusal := errors.New("superseded")
	calls := 0
	_, e := s.AppendEventIf(ctx, "j1", "job.note", nil, nil, func() error {
		calls++
		probe, err := os.OpenFile(filepath.Join(s.Inbox, "j1", ".wrk-events.lock"), os.O_WRONLY|os.O_CREATE, 0o644)
		if err != nil {
			t.Error(err)
			return err
		}
		defer probe.Close()
		if err := syscall.Flock(int(probe.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
			t.Error("the guard ran without the events lock held")
		}
		return refusal
	})
	if !errors.Is(e, refusal) || calls != 1 {
		t.Fatalf("append = %v after %d guard calls, want the guard's own error once", e, calls)
	}
	seq, e := s.AppendEventIf(ctx, "j1", "job.note", nil, nil, func() error { return nil })
	if e != nil || seq != 2 {
		t.Fatalf("append after a refusal = %d, %v; want seq 2 (the refusal recorded nothing)", seq, e)
	}
}

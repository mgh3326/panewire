package arbiter

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// #572 r4576228684: a stalled holder of a job's events lock does not hold an
// event writer past its context; the write fails and records nothing, and it
// succeeds once the lock is free.
func TestAppendEventHonorsCancellationOnTheEventsLock_t4632(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	if _, e := s.AppendEvent(ctx, "j1", "job.note", nil, nil); e != nil {
		t.Fatal(e)
	}
	holder, e := os.OpenFile(filepath.Join(s.Inbox, "j1", ".wrk-events.lock"), os.O_WRONLY|os.O_CREATE, 0o644)
	if e != nil {
		t.Fatal(e)
	}
	defer holder.Close()
	if e := syscall.Flock(int(holder.Fd()), syscall.LOCK_EX); e != nil {
		t.Fatal(e)
	}
	short, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, e := s.AppendEvent(short, "j1", "job.note", nil, nil); e == nil {
		t.Fatal("append succeeded under a held events lock")
	}
	if waited := time.Since(start); waited > 5*time.Second {
		t.Fatalf("append waited %v past its 200ms context", waited)
	}
	if e := syscall.Flock(int(holder.Fd()), syscall.LOCK_UN); e != nil {
		t.Fatal(e)
	}
	seq, e := s.AppendEvent(ctx, "j1", "job.note", nil, nil)
	if e != nil || seq != 2 {
		t.Fatalf("append after release = %d, %v; want seq 2 (the failed write recorded nothing)", seq, e)
	}
}

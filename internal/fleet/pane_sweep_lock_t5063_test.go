package fleet

import (
	"context"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// F4 (hk 5063): the pane sweep must not hold Exclusive (nodeCommandMu) while
// waiting for a job's end lock.
func TestPaneSweepDoesNotHoldExclusiveDuringEndLockWait_t5063(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("r", "reviewer", "")
	putEvent(t, f.a, "r", "job.completed", map[string]any{})
	f.now = f.now.Add(61 * time.Minute)

	unlock, err := LockJobEnd(filepath.Join(f.a.Jobs, "r"))
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()

	looked := make(chan struct{})
	screenRead := false
	previous := f.runner.Do
	f.runner.Do = func(c Command) Result {
		r := previous(c)
		line := strings.Join(c.Args, " ")
		if c.Name == "herdr" && strings.HasPrefix(line, "pane read ") {
			screenRead = true
		}
		if c.Name == "herdr" && line == "agent list" && screenRead {
			screenRead = false
			close(looked)
		}
		return r
	}

	var exclusiveActive atomic.Bool
	f.sweeper.Exclusive = func() func() {
		exclusiveActive.Store(true)
		return func() { exclusiveActive.Store(false) }
	}

	type result struct {
		closed []SweptPane
		err    error
	}
	done := make(chan result, 1)
	go func() {
		closed, err := f.sweeper.SweepEnded(context.Background(), f.a, time.Hour)
		done <- result{closed, err}
	}()

	select {
	case <-looked:
	case <-time.After(5 * time.Second):
		t.Fatal("sweep did not reach agent list")
	}

	// Give a short window for lockUnchangedEnd to start waiting on the end lock.
	time.Sleep(50 * time.Millisecond)

	// While blocked on the end lock, Exclusive must not be held.
	if exclusiveActive.Load() {
		t.Fatal("Exclusive was held while waiting for the job's end lock")
	}

	// Release the end lock so the sweep can proceed.
	unlock()

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("sweep failed: %v", got.err)
		}
		if len(got.closed) != 1 {
			t.Fatalf("closed %v, want 1 pane closed", got.closed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("sweep did not finish after end lock was released")
	}
}

// F6 / M1b (hk 5063): pane sweep must defer when a job's ledger holds an open
// hub-close intent, keeping the pane open and the job unreaped.
func TestEndedSweepDefersToOpenHubClose_t5063(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "")
	putEvent(t, f.a, "r", "job.completed", map[string]any{})
	f.now = f.now.Add(61 * time.Minute)

	j, err := f.a.job("r")
	if err != nil {
		t.Fatal(err)
	}
	var attempt string
	for _, ev := range j.Events {
		if ev.Kind == "job.spawned" {
			attempt = ev.name
		}
	}
	in := HubCloseIntent{
		RequestID: "req-1", Attempt: attempt, Pane: pane, Label: "agent-r", EndReason: "quota_stop",
	}
	if err := f.a.recordHubCloseIntent(context.Background(), "r", in); err != nil {
		t.Fatal(err)
	}

	closed, err := f.sweeper.SweepEnded(context.Background(), f.a, time.Hour)
	if err != nil {
		t.Fatalf("sweep error: %v", err)
	}
	if len(closed) != 0 || len(f.closed()) != 0 || f.reaped("r") != nil {
		t.Fatalf("open hub close was swept or reaped: closed=%v herdr=%v reaped=%v", closed, f.closed(), f.reaped("r"))
	}
}

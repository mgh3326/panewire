package fleet

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The watcher revives the attempt while the sweep waits for its end lock.
func TestEndedSweepKeepsRepromptedJobAfterEndLockWait_t4632(t *testing.T) {
	for _, tc := range []struct {
		name     string
		endAgain bool
	}{
		{"still live", false},
		{"ended again on the same pane", true},
	} {
		t.Run(tc.name, func(t *testing.T) { testSweepRevivedDuringEndLockWait(t, tc.endAgain) })
	}
}

func testSweepRevivedDuringEndLockWait(t *testing.T, endAgain bool) {
	t.Helper()
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "")
	putEvent(t, f.a, "r", "job.completed", map[string]any{})
	f.now = f.now.Add(61 * time.Minute)
	unlock, err := LockJobEnd(filepath.Join(f.a.Jobs, "r"))
	if err != nil {
		t.Fatal(err)
	}
	locked := true
	defer func() {
		if locked {
			unlock()
		}
	}()
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
		t.Fatal("sweep did not reach its final pane proof")
	}
	if err := f.a.event(context.Background(), "r", "job.reprompted", map[string]any{"pane_id": pane}); err != nil {
		t.Fatal(err)
	}
	if endAgain {
		if err := f.a.event(context.Background(), "r", "job.completed", map[string]any{}); err != nil {
			t.Fatal(err)
		}
	}
	unlock()
	locked = false
	select {
	case got := <-done:
		if got.err != nil || len(got.closed) != 0 || len(f.closed()) != 0 || f.reaped("r") != nil {
			t.Fatalf("revived pane was closed or reaped: closes=%v herdr=%v reaped=%v err=%v", got.closed, f.closed(), f.reaped("r"), got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("sweep did not finish")
	}
}

func TestRestoreWaitsForGuardedEnd_t4632(t *testing.T) {
	a := testApp(t)
	f := ledgerRunner(t, a)
	seedJob(t, a, "j", "implementer")
	seedLedger(t, a, "j")
	if err := a.releaseQuota(context.Background(), Job{ID: "j"}); err != nil {
		t.Fatal(err)
	}
	previous := f.Do
	f.Do = func(c Command) Result {
		if strings.Join(c.Args, " ") == "agent list" {
			return Result{Out: `{"result":{"agents":[{"pane_id":"w:p1","tab_id":"w:t1","name":"agent-a","agent_status":"working"}]}}`}
		}
		return previous(c)
	}
	unlock, err := LockJobEnd(filepath.Join(a.Jobs, "j"))
	if err != nil {
		t.Fatal(err)
	}
	locked := true
	defer func() {
		if locked {
			unlock()
		}
	}()
	started, done := make(chan struct{}), make(chan error, 1)
	go func() { close(started); done <- a.lifecycle(context.Background(), "restore", []string{"j"}) }()
	<-started
	// This window proves the restore cannot reclaim while the end is guarded.
	select {
	case err := <-done:
		t.Fatalf("restore completed while end lock held: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	if state, _ := ledgerState(t, a, "j"); state != "released" {
		t.Fatalf("restore reclaimed before guarded end completed: %s", state)
	}
	unlock()
	locked = false
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("restore did not finish after the lock was released")
	}
	if state, _ := ledgerState(t, a, "j"); state != "claimed" {
		t.Fatalf("restore did not reclaim after guarded end: %s", state)
	}
}

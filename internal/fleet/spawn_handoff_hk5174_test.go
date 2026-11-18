package fleet

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// busyDuring reports whether the job's operation lock is held right now.
func operationLockHeld(a *App, job string) bool {
	unlock, err := TryLockJobOperation(filepath.Join(a.Jobs, job))
	if err == nil {
		unlock()
		return false
	}
	return errors.Is(err, ErrJobOperationBusy)
}

// callsOf reads the calls under the fake's lock: a spawn goroutine may still
// be appending to them (hk 5282: a DATA RACE under -race).
func callsOf(f *fakeRunner) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, c := range f.Calls {
		out = append(out, strings.Join(c.Args, " "))
	}
	return out
}

// The spawn holds the job's operation lock from before its claim until it is
// published, so a fast pane's `done` waits for the spawn's own job.spawned
// instead of adopting or completing a half-finished attempt.
func TestSpawnHoldsOperationLockThroughPublicationAndDoneWaits(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	original := f.Do
	var once sync.Once
	var heldAtSend bool
	doneResult := make(chan error, 1)
	doneFinished := make(chan struct{})
	f.Do = func(c Command) Result {
		if len(c.Args) > 1 && c.Args[0] == "pane" && c.Args[1] == "send-text" {
			once.Do(func() {
				heldAtSend = operationLockHeld(a, s.Job)
				report := filepath.Join(a.Jobs, s.Job, "report.md")
				writeReport(t, report, "fast report\n")
				go func() {
					doneResult <- a.lifecycle(context.Background(), "done", []string{s.Job, "--report", report})
					close(doneFinished)
				}()
				// Window that proves nothing more happens: the done must not
				// complete while the spawn is still confirming the brief.
				select {
				case <-doneFinished:
					t.Error("done completed while the spawn still held the job")
				case <-time.After(300 * time.Millisecond):
				}
			})
		}
		return original(c)
	}
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	if !heldAtSend {
		t.Fatal("the operation lock was free while the brief was being confirmed")
	}
	select {
	case err := <-doneResult:
		if err != nil {
			t.Logf("done: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("done never ran after the spawn released the job")
	}
	kinds := eventKindsOf(t, a, s.Job)
	spawned, completed := slices.Index(kinds, "job.spawned"), slices.Index(kinds, "job.completed")
	if spawned < 0 || completed < 0 || completed < spawned {
		t.Fatalf("done did not follow the spawn's own receipt: %v", kinds)
	}
	if operationLockHeld(a, s.Job) {
		t.Fatal("lock still held after the spawn returned")
	}
}

// A failed spawn releases the lock only after its failure cleanup has written
// the attempt's end.
func TestSpawnFailureReleasesOperationLockAfterAttemptEnd(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	original := f.Do
	f.Do = func(c Command) Result {
		r := original(c)
		if strings.HasPrefix(strings.Join(c.Args, " "), "agent list") {
			r.Out = strings.ReplaceAll(r.Out, `"working"`, `"idle"`)
		}
		return r
	}
	if e := a.Spawn(context.Background(), s); code(e) != exitSubmitUnconfirmed {
		t.Fatalf("err=%v", e)
	}
	if operationLockHeld(a, s.Job) {
		t.Fatal("lock held after a failed spawn")
	}
	kinds := eventKindsOf(t, a, s.Job)
	if !slices.Contains(kinds, "job.spawn_retryable") || slices.Index(kinds, "job.brief_sent") > slices.Index(kinds, "job.spawn_retryable") {
		t.Fatalf("attempt end missing or before the brief receipt: %v", kinds)
	}
}

// The tab create came back after the launch cutoff: the spawn closes that exact
// tab, starts no harness, sends no brief and refuses with 70.
func TestSpawnClosesTabCreatedAfterLaunchCutoff(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	original := f.Do
	f.Do = func(c Command) Result {
		if strings.HasPrefix(strings.Join(c.Args, " "), "tab create") {
			// Simulated latency: the create answers after the cutoff.
			time.Sleep(900 * time.Millisecond)
		}
		return original(c)
	}
	ctx, cancel := context.WithTimeout(t.Context(), briefSubmitReserve+defaultFloor+500*time.Millisecond)
	defer cancel()
	e := a.Spawn(ctx, s)
	if code(e) != exitConfig || !strings.Contains(errText(e), "SUBMIT_WINDOW_UNAVAILABLE") {
		t.Fatalf("err=%v", e)
	}
	calls := callsOf(f)
	if !slices.Contains(calls, "tab close w:t1") {
		t.Fatalf("the tab this attempt created was not closed: %v", calls)
	}
	for _, c := range calls {
		if strings.HasPrefix(c, "pane run") || strings.HasPrefix(c, "agent start") || strings.HasPrefix(c, "pane send-text") {
			t.Fatalf("harness or brief started after the launch cutoff: %q", c)
		}
	}
	if kinds := eventKindsOf(t, a, s.Job); !slices.Contains(kinds, "job.spawn_retryable") || slices.Contains(kinds, "job.cleanup_pending") {
		t.Fatalf("closed tab should leave a retryable attempt: %v", kinds)
	}
}

// A create the node cannot account for (the node budget ended under it) may
// have made a tab: the claim is kept and cleanup is recorded pending.
func TestSpawnKeepsClaimWhenTabCreateOutcomeIsUnknown(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	original := f.Do
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	f.Do = func(c Command) Result {
		if strings.HasPrefix(strings.Join(c.Args, " "), "tab create") {
			cancel()
			return Result{Code: 124, Err: "timed out"}
		}
		return original(c)
	}
	if e := a.Spawn(ctx, s); e == nil {
		t.Fatal("spawn succeeded")
	}
	kinds := eventKindsOf(t, a, s.Job)
	if !slices.Contains(kinds, "job.cleanup_pending") || slices.Contains(kinds, "job.spawn_retryable") {
		t.Fatalf("an ambiguous create must not be marked retryable: %v", kinds)
	}
}

// A retry claim cannot land between `done` validating a brief receipt and
// appending the adopted job.spawned: done holds the operation lock for the
// whole of it, and a spawn takes the same lock before its claim.
func TestSpawnWaitsForOperationLockBeforeClaiming(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	if e := os.MkdirAll(filepath.Join(a.Jobs, s.Job), 0700); e != nil {
		t.Fatal(e)
	}
	unlock, err := TryLockJobOperation(filepath.Join(a.Jobs, s.Job))
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- a.Spawn(context.Background(), s) }()
	// Window that proves nothing more happens: while the lock is held the
	// spawn must make no claim and launch nothing.
	select {
	case e := <-finished:
		t.Fatalf("spawn ran under a held operation lock: %v", e)
	case <-time.After(400 * time.Millisecond):
	}
	if _, err := os.Stat(filepath.Join(a.Jobs, s.Job, "events")); err == nil {
		t.Fatalf("events written while another command held the job: %v", eventKindsOf(t, a, s.Job))
	}
	for _, c := range callsOf(f) {
		if strings.HasPrefix(c, "tab create") {
			t.Fatal("pane launched while another command held the job")
		}
	}
	unlock()
	select {
	case e := <-finished:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("spawn did not proceed after the lock was released")
	}
}

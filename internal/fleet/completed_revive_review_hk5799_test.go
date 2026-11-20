package fleet

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
)

// holdWatchLock makes job's watcher lock held, as a started watcher holds it
// for its life, so watchActive reads the job as watched.
func holdWatchLock(t *testing.T, a *App, job string) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(a.Jobs, job, ".panewire-watch.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
}

// A submitter's submission-only directory (submission.json, no events/) on this
// node must not disable the revive for the node's own jobs (hk 5799 review).
func TestCompletedPaneReviveIgnoresASubmissionOnlyDirectory_hk5799(t *testing.T) {
	f, pane, started := reviveFixture(t)
	remote := filepath.Join(f.a.Jobs, "remote-job")
	if err := os.MkdirAll(remote, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(remote, "submission.json"), []byte(`{"job":"remote-job"}`), 0600); err != nil {
		t.Fatal(err)
	}
	f.status[pane] = "idle"
	f.at(2)
	f.status[pane] = "working"
	f.at(3)
	if got := reprompts(t, f.a, "j"); len(got) != 1 {
		t.Fatalf("a submission-only directory blocked the revive: reprompts=%v", got)
	}
	if !slices.Equal(*started, []string{"j"}) {
		t.Fatalf("watchers started %v, want the revived job's", *started)
	}
}

// A revived job whose first watcher start fails keeps its revive. A later pass
// starts its watcher once, without writing another job.reprompted (hk 5799
// review).
func TestCompletedPaneReviveRetriesAFailedWatcherStart_hk5799(t *testing.T) {
	f, pane, _ := reviveFixture(t)
	var calls []string
	failFirst := true
	reviveStartWatch = func(_ *App, id string) error {
		calls = append(calls, id)
		if failFirst {
			failFirst = false
			return errors.New("spawn failed")
		}
		holdWatchLock(t, f.a, id)
		return nil
	}
	f.status[pane] = "idle"
	f.at(2)
	f.status[pane] = "working"
	f.at(3)
	if got := reprompts(t, f.a, "j"); len(got) != 1 {
		t.Fatalf("reprompts after the revive = %v, want one", got)
	}
	if !slices.Equal(calls, []string{"j"}) {
		t.Fatalf("start calls after the revive = %v, want one failed attempt", calls)
	}

	f.at(4)
	if !slices.Equal(calls, []string{"j", "j"}) {
		t.Fatalf("start calls after the next pass = %v, want the retry", calls)
	}
	f.at(5)
	if len(calls) != 2 {
		t.Fatalf("a started watcher was started again: %v", calls)
	}
	if got := reprompts(t, f.a, "j"); len(got) != 1 {
		t.Fatalf("the retry wrote another job.reprompted: %v", got)
	}
	events, err := f.a.events("j")
	if err != nil {
		t.Fatal(err)
	}
	if ended := viewLedger(events).Ended; ended != "" {
		t.Fatalf("retried job ends as %q", ended)
	}
}

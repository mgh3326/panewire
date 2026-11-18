package fleet

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// A quota announcement after a local end (here a revoke) must not be written:
// as the job's newest end it would read as a live hand-over and mask the
// revoke (hk 4761, #593 round 6).
func TestQuotaStopAnnounceDoesNotMaskARevoke_t4761(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	if err := a.event(context.Background(), "j", "job.revoked", map[string]any{"pane_id": "w:p1"}); err != nil {
		t.Fatal(err)
	}
	before, _ := a.job("j")
	if err := a.quotaStopAnnounce(context.Background(), Job{ID: "j", Pane: "w:p1"}, quotastop.Stop{Pool: "claude"}); !errors.Is(err, errQuotaStopSuperseded) {
		t.Fatalf("announce after revoke: %v, want errQuotaStopSuperseded", err)
	}
	after, _ := a.job("j")
	if len(after.Events) != len(before.Events) {
		t.Fatalf("events grew after the revoke: %+v", after.Events[len(before.Events):])
	}
	if after.Ended != "job.revoked" {
		t.Fatalf("newest end = %q, want job.revoked", after.Ended)
	}
}

// The announcement waits for the job's end lock, so it orders against the
// other local ends and settlement.
func TestQuotaStopAnnounceWaitsForTheEndLock_t4761(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	unlock, err := LockJobEnd(filepath.Join(a.Jobs, "j"))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- a.quotaStopAnnounce(context.Background(), Job{ID: "j", Pane: "w:p1"}, quotastop.Stop{Pool: "claude"})
	}()
	select {
	case err := <-done:
		unlock()
		t.Fatalf("the announcement ran under a held end lock: %v", err)
	case <-time.After(300 * time.Millisecond): // proves the writer waits: nothing more happens in this window
	}
	unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the announcement did not run after the end lock was released")
	}
}

// The operation-lock wait ends when its context does, not only at its bound.
func TestJobOperationLockWaitHonorsContext_t4761(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	unlock, err := TryLockJobOperation(filepath.Join(a.Jobs, "j"))
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = a.lockJobOperation(ctx, "j", quotaRecordLockWait)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("wait took %v; the context was not honored", took)
	}
}

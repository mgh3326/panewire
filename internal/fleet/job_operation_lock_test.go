package fleet

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// The hub's posted-result end of a reviewer job holds the job's operation lock
// across its checks and the hk write (hk 4684). A quota-stop record the watcher
// writes meanwhile must wait for it, so it either lands before the node looks
// (and the node refuses) or after the write (and sees the end).
func TestQuotaStopAnnounceWaitsForTheJobOperationLock(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	unlock, err := TryLockJobOperation(filepath.Join(a.Jobs, "j"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := TryLockJobOperation(filepath.Join(a.Jobs, "j")); !errors.Is(err, ErrJobOperationBusy) {
		t.Fatalf("second lock: %v, want ErrJobOperationBusy", err)
	}
	done := make(chan error, 1)
	go func() {
		done <- a.quotaStopAnnounce(context.Background(), Job{ID: "j", Pane: "w:p1"}, quotastop.Stop{Pool: "claude"})
	}()
	select {
	case err := <-done:
		t.Fatalf("the announcement ran under a held lock: %v", err)
	case <-time.After(300 * time.Millisecond): // proves the writer waits: nothing more happens in this window
	}
	unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the announcement did not run after the lock was released")
	}
}

// The intent record takes the same lock.
func TestQuotaIntentRecordWaitsForTheJobOperationLock(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	unlock, err := TryLockJobOperation(filepath.Join(a.Jobs, "j"))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- a.recordQuotaIntent(context.Background(), Job{ID: "j", Pane: "w:p1"}, quotastop.Stop{Pool: "claude"})
	}()
	select {
	case err := <-done:
		t.Fatalf("the intent was recorded under a held lock: %v", err)
	case <-time.After(300 * time.Millisecond): // proves the writer waits: nothing more happens in this window
	}
	unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the intent was not recorded after the lock was released")
	}
}

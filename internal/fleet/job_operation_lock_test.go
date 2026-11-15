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

// The ended-job pane sweep reads a reviewer's end from the job's ledger: the
// event the hub's posted-result end appends must read as an end to the sweep's
// own predicate, for a reviewer, or its pane is never closed (hk 4693).
func TestRecordJobEndedReadsAsAnEndToThePaneSweep(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	if err := RecordJobEnded(context.Background(), a.Jobs, "j", "w:p1", "reviewer_result_posted"); err != nil {
		t.Fatal(err)
	}
	j, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	if !j.Terminal || j.Ended != "job.lost" || !paneSweepClosed(j) {
		t.Fatalf("job after the end: terminal=%v ended=%q closed=%v, want the sweep to see an end", j.Terminal, j.Ended, paneSweepClosed(j))
	}
}

// A quota-stop record that waited for the lock while the hub's posted-result end
// ran must not append its announcement afterwards: the watcher's snapshot is
// stale, announcedQuotaStop would read the newest event as a live hand-over, and
// the hub could start a replacement for a job hk already holds lost without a
// successor pointer. Under the lock it re-reads the job and writes nothing.
func TestQuotaStopRecordsWriteNothingAfterThePostedResultEnd(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	stale, err := a.job("j") // the watcher's snapshot, taken before the end
	if err != nil {
		t.Fatal(err)
	}
	if err := RecordJobEnded(context.Background(), a.Jobs, "j", "w:p1", ReviewerResultPosted); err != nil {
		t.Fatal(err)
	}
	before, _ := a.job("j")

	if err := a.quotaStopAnnounce(context.Background(), stale, quotastop.Stop{Pool: "claude"}); err != nil {
		t.Fatal(err)
	}
	if err := a.recordQuotaIntent(context.Background(), stale, quotastop.Stop{Pool: "claude"}); err != nil {
		t.Fatal(err)
	}

	after, _ := a.job("j")
	if len(after.Events) != len(before.Events) {
		t.Fatalf("events grew from %d to %d after the end: %+v", len(before.Events), len(after.Events), after.Events[len(before.Events):])
	}
	if _, announced := announcedQuotaStop(after); announced {
		t.Fatal("a stale announcement re-armed a hand-over for an ended job")
	}
}

// The node writes hk first and the ledger event second, so a failure between the
// two leaves hk lost with the end_reason and no ledger event. A quota-stop record
// that takes the lock in that gap must see the end from hk and write nothing.
func TestQuotaStopRecordsWriteNothingWhenOnlyHkHoldsThePostedResultEnd(t *testing.T) {
	a, _, h, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	stale, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	h.records["j"]["state"] = "lost"
	h.records["j"]["end_reason"] = ReviewerResultPosted
	before := len(stale.Events)

	if err := a.quotaStopAnnounce(context.Background(), stale, quotastop.Stop{Pool: "claude"}); err != nil {
		t.Fatal(err)
	}
	if err := a.recordQuotaIntent(context.Background(), stale, quotastop.Stop{Pool: "claude"}); err != nil {
		t.Fatal(err)
	}

	after, _ := a.job("j")
	if len(after.Events) != before {
		t.Fatalf("events grew from %d to %d while hk held the end: %+v", before, len(after.Events), after.Events[before:])
	}
}

// The newest end is decided in ledger order, not timestamp order. A clock
// stepped backwards stamps a later event earlier: a re-prompt that follows the
// end in the ledger sorts before it by time, and folding by timestamp would
// still read the job as ended and drop a live attempt's hand-over record.
func TestEndedByResultPostedFoldsTheLedgerInSequenceOrder(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	if err := RecordJobEnded(context.Background(), a.Jobs, "j", "w:p1", ReviewerResultPosted); err != nil {
		t.Fatal(err)
	}
	if !a.endedByResultPosted(context.Background(), "j") {
		t.Fatal("setup: the job should read as ended by its posted result")
	}
	now := a.Now
	t.Cleanup(func() { a.Now = now })
	a.Now = func() time.Time { return now().Add(-6 * time.Hour) }
	if err := a.event(context.Background(), "j", "job.reprompted", map[string]any{"pane_id": "w:p1"}); err != nil {
		t.Fatal(err)
	}

	if a.endedByResultPosted(context.Background(), "j") {
		t.Fatal("a re-prompt after the end (stamped earlier by a stepped clock) did not clear it")
	}
}

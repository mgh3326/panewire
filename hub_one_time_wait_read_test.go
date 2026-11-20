package panewire

import (
	"net/http"
	"testing"
	"time"
)

// These tests pin how an operator wait is learned after an ambiguous open. The
// row keeps the anchor until the open wait is either found by a read of
// GET /v1/waits or proven absent by a complete one. The wait is never written
// again just to learn its id: a repeat open would create a wait for a need that
// holds none.

// oneTimeRowState reads the need's row under the lock the worker writes it with.
func oneTimeRowState(h *HubServer) (oneTimeActionState, bool) {
	return h.oneTimeStateOf(oneTimeWaitKey())
}

// An open that committed but answered 5xx leaves an unconfirmed id on the row.
// A retry refused with 4xx says the retry committed nothing, not that the
// committed wait is absent: the open wait is read back and kept on the row, so
// a later close ends it. Releasing the anchor mark on the refusal would leave
// the wait open with no row to clear it.
func TestOneTimeWaitRefusedRetryKeepsCommittedWait(t *testing.T) {
	h, fake, _ := spawnNotifyHubLane(t, "", "operator-desk")
	oneTimeSentry(t, h)
	start := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	cycle := oneTimeActionClearCycle

	setFakeHk(fake, func(f *fakeHandoffkeep) { f.waitCommitStatus = http.StatusInternalServerError })
	h.noteReadinessActions("oci", oneTimeWaitDialog(start), start)
	settleOneTimeNotices(t, h)
	committed := fake.openOperatorWaits()
	if len(committed) != 1 {
		t.Fatalf("open waits=%d after the ambiguous open, want the committed wait", len(committed))
	}

	setFakeHk(fake, func(f *fakeHandoffkeep) {
		f.waitCommitStatus = 0
		f.waitStatus = http.StatusBadRequest
	})
	retry := start.Add(time.Minute)
	h.noteReadinessActions("oci", oneTimeWaitDialog(retry), retry)
	settleOneTimeNotices(t, h)
	if got := fake.openOperatorWaits(); len(got) != 1 || got[0].ID != committed[0].ID {
		t.Fatalf("open waits=%v after the refused retry, want the committed wait %d", got, committed[0].ID)
	}
	state, ok := oneTimeRowState(h)
	if !ok || state.WaitTaskID == 0 || state.WaitID != committed[0].ID {
		t.Fatalf("row=%+v after the refused retry, want the committed wait %d recorded", state, committed[0].ID)
	}

	setFakeHk(fake, func(f *fakeHandoffkeep) { f.waitStatus = 0 })
	gone := start.Add(2 * time.Minute)
	h.noteReadinessActions("oci", oneTimeWaitClean(gone), gone)
	h.noteReadinessActions("oci", oneTimeWaitClean(gone.Add(cycle)), gone.Add(cycle))
	settleOneTimeNotices(t, h)
	if got := fake.openOperatorWaits(); len(got) != 0 {
		t.Fatalf("open waits=%v after the close, want the committed wait cleared", got)
	}
}

// A close whose wait id was never confirmed reads the id back and clears it. A
// clear that fails leaves the wait open and the row kept; the retry clears the
// same wait. Exactly one wait write happens for the whole need: the open.
func TestOneTimeWaitCloseReadsBackWaitIDAfterFailedClear(t *testing.T) {
	h, fake, _ := spawnNotifyHubLane(t, "", "operator-desk")
	oneTimeSentry(t, h)
	start := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	cycle := oneTimeActionClearCycle

	setFakeHk(fake, func(f *fakeHandoffkeep) { f.waitCommitStatus = http.StatusInternalServerError })
	h.noteReadinessActions("oci", oneTimeWaitDialog(start), start)
	settleOneTimeNotices(t, h)

	setFakeHk(fake, func(f *fakeHandoffkeep) {
		f.waitCommitStatus = 0
		f.clearStatus = http.StatusInternalServerError
	})
	gone := start.Add(time.Minute)
	h.noteReadinessActions("oci", oneTimeWaitClean(gone), gone)
	h.noteReadinessActions("oci", oneTimeWaitClean(gone.Add(cycle)), gone.Add(cycle))
	settleOneTimeNotices(t, h)
	if got := fake.openOperatorWaits(); len(got) != 1 {
		t.Fatalf("open waits=%d after the failed clear, want the wait still open", len(got))
	}

	setFakeHk(fake, func(f *fakeHandoffkeep) { f.clearStatus = 0 })
	again := gone.Add(cycle + time.Minute)
	h.noteReadinessActions("oci", oneTimeWaitClean(again), again)
	settleOneTimeNotices(t, h)

	if got := fake.openOperatorWaits(); len(got) != 0 {
		t.Fatalf("open waits=%v after the retried clear, want none", got)
	}
	if waits := fake.allWaits(); len(waits) != 1 || !waits[0].Cleared {
		t.Fatalf("wait rows=%+v, want the one wait opened and cleared", waits)
	}
	if n := fake.oneTimeCallCount(http.MethodPost, "/waits"); n != 1 {
		t.Fatalf("wait writes=%d, want only the open: the close reads the id back rather than repeat the write", n)
	}
}

// While the read cannot prove the committed wait absent or present, the anchor
// mark stays. The refused retry is answered again once the read recovers, and
// then the id is learned from it.
func TestOneTimeWaitRefusedRetryKeepsRowWhileReadFails(t *testing.T) {
	h, fake, _ := spawnNotifyHubLane(t, "", "operator-desk")
	oneTimeSentry(t, h)
	start := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)

	setFakeHk(fake, func(f *fakeHandoffkeep) { f.waitCommitStatus = http.StatusInternalServerError })
	h.noteReadinessActions("oci", oneTimeWaitDialog(start), start)
	settleOneTimeNotices(t, h)
	committed := fake.openOperatorWaits()
	if len(committed) != 1 {
		t.Fatalf("open waits=%d after the ambiguous open, want the committed wait", len(committed))
	}

	setFakeHk(fake, func(f *fakeHandoffkeep) {
		f.waitCommitStatus = 0
		f.waitStatus = http.StatusBadRequest
		f.waitsStatus = http.StatusInternalServerError
	})
	retry := start.Add(time.Minute)
	h.noteReadinessActions("oci", oneTimeWaitDialog(retry), retry)
	settleOneTimeNotices(t, h)
	state, ok := oneTimeRowState(h)
	if !ok || state.WaitTaskID == 0 {
		t.Fatalf("row=%+v after a refused retry with a failed read, want the anchor mark kept", state)
	}

	setFakeHk(fake, func(f *fakeHandoffkeep) { f.waitsStatus = 0 })
	again := start.Add(2 * time.Minute)
	h.noteReadinessActions("oci", oneTimeWaitDialog(again), again)
	settleOneTimeNotices(t, h)
	state, ok = oneTimeRowState(h)
	if !ok || state.WaitID != committed[0].ID {
		t.Fatalf("row=%+v after the read recovers, want the committed wait %d recorded", state, committed[0].ID)
	}
	if got := fake.openOperatorWaits(); len(got) != 1 {
		t.Fatalf("open waits=%d, want the one committed wait", len(got))
	}
}

// The desk lists an open decision request on the anchor under needs_operator as
// a decision:<ref> row with the same task_id and on_kind operator. A close with
// an unconfirmed id reads past that row to the anchor's hand wait and clears it.
func TestOneTimeWaitCloseFindsWaitBesideDecisionRow(t *testing.T) {
	h, fake, _ := spawnNotifyHubLane(t, "", "operator-desk")
	oneTimeSentry(t, h)
	start := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	cycle := oneTimeActionClearCycle

	setFakeHk(fake, func(f *fakeHandoffkeep) { f.waitCommitStatus = http.StatusInternalServerError })
	h.noteReadinessActions("oci", oneTimeWaitDialog(start), start)
	settleOneTimeNotices(t, h)
	anchorID := liveHkAnchor(fake).ID
	setFakeHk(fake, func(f *fakeHandoffkeep) {
		f.waitCommitStatus = 0
		f.decisionRowTasks = map[int64]bool{anchorID: true}
	})

	gone := start.Add(time.Minute)
	h.noteReadinessActions("oci", oneTimeWaitClean(gone), gone)
	h.noteReadinessActions("oci", oneTimeWaitClean(gone.Add(cycle)), gone.Add(cycle))
	settleOneTimeNotices(t, h)
	if got := fake.openOperatorWaits(); len(got) != 0 {
		t.Fatalf("open waits=%v, want the anchor's wait cleared beside its decision row", got)
	}
}

// A needs_operator section can outgrow any fixed response cap. The lookup
// streams the section, so an unrelated row of 2 MiB ahead of the anchor's wait
// does not stop the close from clearing it.
func TestOneTimeWaitCloseReadsPastOversizedSection(t *testing.T) {
	h, fake, _ := spawnNotifyHubLane(t, "", "operator-desk")
	oneTimeSentry(t, h)
	start := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	cycle := oneTimeActionClearCycle

	setFakeHk(fake, func(f *fakeHandoffkeep) { f.waitCommitStatus = http.StatusInternalServerError })
	h.noteReadinessActions("oci", oneTimeWaitDialog(start), start)
	settleOneTimeNotices(t, h)
	setFakeHk(fake, func(f *fakeHandoffkeep) {
		f.waitCommitStatus = 0
		f.waitsPadBytes = 2 << 20
	})

	gone := start.Add(time.Minute)
	h.noteReadinessActions("oci", oneTimeWaitClean(gone), gone)
	h.noteReadinessActions("oci", oneTimeWaitClean(gone.Add(cycle)), gone.Add(cycle))
	settleOneTimeNotices(t, h)
	if got := fake.openOperatorWaits(); len(got) != 0 {
		t.Fatalf("open waits=%v, want the anchor's wait cleared past the oversized section", got)
	}
}

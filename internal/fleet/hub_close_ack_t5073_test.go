package fleet

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func writeBrokenEvent(t *testing.T, a *App) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(a.Jobs, "j", "events", "99999-broken.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
}

// drainCancelledGoneClose leaves job j with a gone close whose pane is still
// listed, which the rollback drain cancels as rollback_unproven.
func drainCancelledGoneClose(t *testing.T) (*App, HubCloseIntent) {
	t.Helper()
	a, _ := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpGone})
	if err := BeginCloseOpsDrain(context.Background(), a.Jobs); err != nil {
		t.Fatal(err)
	}
	settlement := HubCloseSettlement{App: a, PaneListed: func(context.Context, string, string) (bool, error) { return true, nil }}
	if state, err := settlement.Settle(context.Background(), "j"); err != nil || state != HubCloseCancelled {
		t.Fatalf("settlement=%v err=%v; want the drain to cancel the intent", state, err)
	}
	return a, in
}

func wantAckPending(t *testing.T, a *App, want []string) {
	t.Helper()
	got, err := PendingHubCloseAckJobs(a.Jobs)
	if err != nil || !slices.Equal(got, want) {
		t.Fatalf("jobs awaiting a hub ack=%v err=%v; want %v", got, err, want)
	}
}

// hk 5073: the local cancel is not the hub's durable proof. The drain keeps
// the job pending until the hub's acknowledgement is recorded, and the intent
// itself is no longer open (the node's settlement has nothing left to do).
func TestDrainCancelWaitsForTheHubAck_t5073(t *testing.T) {
	a, in := drainCancelledGoneClose(t)
	if open, err := PendingHubCloseJobs(a.Jobs); err != nil || len(open) != 0 {
		t.Fatalf("open intents=%v err=%v; the cancel closed the intent", open, err)
	}
	wantAckPending(t, a, []string{"j"})
	if err := RecordHubCloseAck(context.Background(), a.Jobs, "j", in.RequestID); err != nil {
		t.Fatal(err)
	}
	wantAckPending(t, a, nil)
}

// An ack names its request: another request's ack leaves the debt in place.
func TestHubAckMustNameTheCancelledRequest_t5073(t *testing.T) {
	a, _ := drainCancelledGoneClose(t)
	if err := RecordHubCloseAck(context.Background(), a.Jobs, "j", "other-request"); err != nil {
		t.Fatal(err)
	}
	wantAckPending(t, a, []string{"j"})
}

// A cancel written before the ack field existed, or for any other reason,
// owes nothing: an old ledger must not hold a drain forever.
func TestCancelWithoutAnAckMarkOwesNothing_t5073(t *testing.T) {
	a, _ := hubCloseFixture(t)
	for _, payload := range []map[string]any{
		{"request_id": "close-1", "reason": "rollback_unproven"},
		{"request_id": "close-2", "reason": "released"},
	} {
		if err := a.eventSynced(context.Background(), "j", hubCloseCancelKind, payload); err != nil {
			t.Fatal(err)
		}
	}
	wantAckPending(t, a, nil)
}

// A settled hub close makes close_status answer settled, not intent_cancelled,
// so the debt ends with it.
func TestSettledCloseEndsTheAckDebt_t5073(t *testing.T) {
	a, in := drainCancelledGoneClose(t)
	if err := a.eventSynced(context.Background(), "j", "job.lost", map[string]any{"reason": hubCloseLostReason, "final": true, "request_id": in.RequestID}); err != nil {
		t.Fatal(err)
	}
	wantAckPending(t, a, nil)
}

// A repeat of the request records a new intent: close_status then no longer
// answers intent_cancelled, so the old cancel owes nothing (the open intent
// is what holds the drain), and an ack given earlier does not cover a later
// cancel.
func TestRepeatedRequestRestartsTheAckDebt_t5073(t *testing.T) {
	a, in := drainCancelledGoneClose(t)
	recordHubClose(t, a, in)
	wantAckPending(t, a, nil)
	if err := a.eventSynced(context.Background(), "j", hubCloseCancelKind, map[string]any{"request_id": in.RequestID, "reason": hubCloseRollbackUnproven, hubCloseAckField: true}); err != nil {
		t.Fatal(err)
	}
	wantAckPending(t, a, []string{"j"})
	if err := RecordHubCloseAck(context.Background(), a.Jobs, "j", in.RequestID); err != nil {
		t.Fatal(err)
	}
	recordHubClose(t, a, in)
	if err := a.eventSynced(context.Background(), "j", hubCloseCancelKind, map[string]any{"request_id": in.RequestID, "reason": hubCloseRollbackUnproven, hubCloseAckField: true}); err != nil {
		t.Fatal(err)
	}
	wantAckPending(t, a, []string{"j"})
}

// An unreadable ledger cannot prove the hub acknowledged anything.
func TestAckScanFailsClosedOnAnUnreadableLedger_t5073(t *testing.T) {
	a, _ := hubCloseFixture(t)
	writeBrokenEvent(t, a)
	wantAckPending(t, a, []string{"j"})
}

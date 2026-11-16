package fleet

import (
	"context"
	"slices"
	"testing"
)

func eventKinds(t *testing.T, a *App) []string {
	t.Helper()
	evs, e := a.events("j")
	if e != nil {
		t.Fatal(e)
	}
	var kinds []string
	for _, ev := range evs {
		kinds = append(kinds, ev.Kind)
	}
	return kinds
}

// #572 r4576225999: the node journals closed, then the owner revokes the job
// locally before settlement. The intent stays eligible, and settlement writes
// the intended hk record, releases the lease and records the final job.lost.
// When the revoke closes the intent, the job is never settled.
func TestSettlementFinishesAClosedOpAfterALocalRevoke_t4632(t *testing.T) {
	a, h := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpClosed})
	if e := a.event(context.Background(), "j", "job.revoked", map[string]any{"reason": "owner"}); e != nil {
		t.Fatal(e)
	}
	pending, e := PendingHubCloseJobs(a.Jobs)
	if e != nil || !slices.Equal(pending, []string{"j"}) {
		t.Fatalf("pending %v, %v; want [j]", pending, e)
	}
	state, e := (HubCloseSettlement{App: a}).Settle(context.Background(), "j")
	if e != nil || state != HubCloseSettled {
		t.Fatalf("settle %v, %v; want settled", state, e)
	}
	wantHubCloseFinished(t, a, h)
	if pending, _ := PendingHubCloseJobs(a.Jobs); len(pending) != 0 {
		t.Fatalf("still pending after settlement: %v", pending)
	}
}

// A competing terminal hk record written before the settlement is kept.
func TestSettlementAfterALocalEndKeepsAnotherTerminalRecord_t4632(t *testing.T) {
	a, h := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpClosed})
	if e := a.event(context.Background(), "j", "job.revoked", map[string]any{"reason": "owner"}); e != nil {
		t.Fatal(e)
	}
	h.mu.Lock()
	h.records["j"]["state"] = "completed"
	h.mu.Unlock()
	if state, e := (HubCloseSettlement{App: a}).Settle(context.Background(), "j"); e != nil || state != HubCloseSettled {
		t.Fatalf("settle %v, %v; want settled", state, e)
	}
	if st := str(h.record(t, "j"), "state"); st != "completed" {
		t.Fatalf("hk state %s, want the competing completed kept", st)
	}
}

// A local end before the operation was ever registered: nothing closed, a
// repeat can no longer bind, so settlement cancels the intent.
func TestSettlementCancelsAnUnregisteredCloseAfterALocalEnd_t4632(t *testing.T) {
	a, h := hubCloseFixture(t)
	recordHubClose(t, a, liveIntent(t, a))
	if e := a.event(context.Background(), "j", "job.revoked", map[string]any{"reason": "owner"}); e != nil {
		t.Fatal(e)
	}
	if state, e := (HubCloseSettlement{App: a}).Settle(context.Background(), "j"); e != nil || state != HubCloseCancelled {
		t.Fatalf("settle %v, %v; want cancelled", state, e)
	}
	if str(h.record(t, "j"), "end_reason") == hubCloseReason {
		t.Fatal("an unregistered close wrote the intended record")
	}
	if pending, _ := PendingHubCloseJobs(a.Jobs); len(pending) != 0 {
		t.Fatalf("still pending: %v", pending)
	}
}

// #572 r4576226003: the watcher decides a re-prompt on a snapshot, the node
// settles the close before the watcher takes the end lock, and the decision is
// discarded: no job.reprompted follows the final job.lost.
func TestWatcherDropsADecisionAHubCloseSettledSince_t4632(t *testing.T) {
	a, _ := hubCloseFixture(t)
	snapshot, e := a.job("j")
	if e != nil {
		t.Fatal(e)
	}
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpClosed})
	if state, e := (HubCloseSettlement{App: a}).Settle(context.Background(), "j"); e != nil || state != HubCloseSettled {
		t.Fatalf("settle %v, %v", state, e)
	}
	ran := false
	deferred, e := a.endUnlessHubClose("j", snapshot, func(Job) error {
		ran = true
		return a.event(context.Background(), "j", "job.reprompted", map[string]any{"reason": "quota_stop_resumed"})
	})
	if e != nil || !deferred || ran {
		t.Fatalf("deferred %v, ran %v, err %v; want the stale decision dropped", deferred, ran, e)
	}
	if slices.Contains(eventKinds(t, a), "job.reprompted") {
		t.Fatal("stale job.reprompted recorded after the final job.lost")
	}
}

// Control: a decision whose snapshot is still current runs.
func TestWatcherRunsADecisionOnACurrentSnapshot_t4632(t *testing.T) {
	a, _ := hubCloseFixture(t)
	snapshot, e := a.job("j")
	if e != nil {
		t.Fatal(e)
	}
	ran := false
	deferred, e := a.endUnlessHubClose("j", snapshot, func(Job) error { ran = true; return nil })
	if e != nil || deferred || !ran {
		t.Fatalf("deferred %v, ran %v, err %v; want it run", deferred, ran, e)
	}
}

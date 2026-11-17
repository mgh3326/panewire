package fleet

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
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

// #662 review: a reaper that ran after the close was journaled closed (here
// after a local revoke) does not discard the close; settlement still writes
// the planned record.
func TestSettlementSurvivesAReap_t4632(t *testing.T) {
	a, h := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpClosed})
	for _, kind := range []string{"job.revoked", "job.reaped"} {
		if e := a.event(context.Background(), "j", kind, map[string]any{"reason": "owner"}); e != nil {
			t.Fatal(e)
		}
	}
	if pending, _ := PendingHubCloseJobs(a.Jobs); !slices.Equal(pending, []string{"j"}) {
		t.Fatalf("pending %v, want [j]", pending)
	}
	if state, e := (HubCloseSettlement{App: a}).Settle(context.Background(), "j"); e != nil || state != HubCloseSettled {
		t.Fatalf("settle %v, %v; want settled", state, e)
	}
	if rec := h.record(t, "j"); str(rec, "end_reason") != hubCloseReason {
		t.Fatalf("hk record %v, want the planned lost record", rec)
	}
}

// #662 review: a settlement already in the watcher's snapshot still drops a
// decision (the hand-off case, where the terminal check does not stop the
// watcher), and the watch itself ends on it.
func TestWatcherDropsDecisionsOnceAHubCloseSettled_t4632(t *testing.T) {
	a, _ := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpClosed})
	if state, e := (HubCloseSettlement{App: a}).Settle(context.Background(), "j"); e != nil || state != HubCloseSettled {
		t.Fatalf("settle %v, %v", state, e)
	}
	snapshot, e := a.job("j")
	if e != nil {
		t.Fatal(e)
	}
	ran := false
	deferred, e := a.endUnlessHubClose("j", snapshot, func(Job) error { ran = true; return nil })
	if e != nil || !deferred || ran {
		t.Fatalf("deferred %v, ran %v, err %v; want dropped", deferred, ran, e)
	}
}

// #662 review: the node's final job.lost carries a timestamp before the
// spawn (a clock stepped backwards). The watcher folds settlement in ledger
// order, ends, and writes no second loss.
func TestWatcherHonorsASettlementWithABackwardsClock_t4632(t *testing.T) {
	a, _ := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpClosed})
	now := a.Now
	a.Now = func() time.Time { return now().Add(-48 * time.Hour) }
	if state, e := (HubCloseSettlement{App: a}).Settle(context.Background(), "j"); e != nil || state != HubCloseSettled {
		t.Fatalf("settle %v, %v", state, e)
	}
	a.Now = now
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if lost := lostEvents(t, a); len(lost) != 1 {
		t.Fatalf("job.lost events %v, want only the settlement's", lost)
	}
}

// #662 review: a hub revocation the node records with no kind field (only
// "type", with the hub's epoch) after the intent keeps the ledger readable.
// It means the hub reassigned the job, so settlement finishes locally (lease,
// final job.lost) and leaves the shared hk record to the new owner.
func TestSettlementLeavesHKToANewOwnerAfterAHubRevocation_t4632(t *testing.T) {
	a, h := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpClosed})
	if e := os.WriteFile(filepath.Join(a.Jobs, "j", "events", "99999-job.revoked.json"), []byte(`{"type":"job.revoked","job_id":"j","epoch":2}`), 0o600); e != nil {
		t.Fatal(e)
	}
	if pending, e := PendingHubCloseJobs(a.Jobs); e != nil || !slices.Equal(pending, []string{"j"}) {
		t.Fatalf("pending %v, %v; want [j]", pending, e)
	}
	if state, e := (HubCloseSettlement{App: a}).Settle(context.Background(), "j"); e != nil || state != HubCloseSettled {
		t.Fatalf("settle %v, %v; want settled", state, e)
	}
	if st := str(h.record(t, "j"), "state"); st != "spawned" {
		t.Fatalf("hk state %s, want the new owner's record untouched", st)
	}
	if lost := lostEvents(t, a); len(lost) != 1 || !boolean(lost[0], "superseded", false) {
		t.Fatalf("job.lost %v, want one superseded settlement", lost)
	}
	if st, _ := ledgerState(t, a, "j"); st != "released" {
		t.Fatalf("quota %s, want released", st)
	}
}

// A type-only revocation without an epoch (a legacy prune record) is a local
// end: settlement still writes the planned record.
func TestSettlementAfterALegacyTypeOnlyRevocation_t4632(t *testing.T) {
	a, h := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpClosed})
	if e := os.WriteFile(filepath.Join(a.Jobs, "j", "events", "99999-job.revoked.json"), []byte(`{"type":"job.revoked","job_id":"j","source":"legacy prune"}`), 0o600); e != nil {
		t.Fatal(e)
	}
	if state, e := (HubCloseSettlement{App: a}).Settle(context.Background(), "j"); e != nil || state != HubCloseSettled {
		t.Fatalf("settle %v, %v; want settled", state, e)
	}
	wantHubCloseFinished(t, a, h)
}

// #662 review round 3: a newer close B left registered must not mask an
// older close A that already closed the pane. After a local end, settlement
// drives every open intent and finishes the job with A's planned record.
func TestSettlementReachesAnOlderClosedIntent_t4632(t *testing.T) {
	a, h := hubCloseFixture(t)
	older := liveIntent(t, a)
	recordHubClose(t, a, older)
	writeJobCloseOp(t, a, CloseOp{RequestID: older.RequestID, Attempt: older.Attempt, State: CloseOpClosed})
	newer := older
	newer.RequestID, newer.EndReason = "close-2", "quota_stop pool=other replaced_by=j3"
	recordHubClose(t, a, newer)
	writeJobCloseOp(t, a, CloseOp{RequestID: newer.RequestID, Attempt: newer.Attempt, State: CloseOpRegistered})
	if e := a.event(context.Background(), "j", "job.completed", map[string]any{}); e != nil {
		t.Fatal(e)
	}
	if state, e := (HubCloseSettlement{App: a}).Settle(context.Background(), "j"); e != nil || state != HubCloseSettled {
		t.Fatalf("settle %v, %v; want settled through the older close", state, e)
	}
	wantHubCloseFinished(t, a, h)
	if pending, _ := PendingHubCloseJobs(a.Jobs); len(pending) != 0 {
		t.Fatalf("still pending: %v", pending)
	}
}

// #662 consult (L8I4): `reap --apply` leaves a job whose hub close is still
// open to settlement: no generic hk lost lands first (settlement would keep
// it and lose the planned reason), and no job.reaped is recorded.
func TestReapLeavesAnOpenHubCloseToSettlement_t4632(t *testing.T) {
	a, h := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpClosed})
	if e := a.event(context.Background(), "j", "job.revoked", map[string]any{"reason": "owner"}); e != nil {
		t.Fatal(e)
	}
	var out strings.Builder
	a.Out = &out
	now := a.Now
	a.Now = func() time.Time { return now().Add(time.Hour) }
	if e := a.hygiene(context.Background(), "reap", []string{"--job", "j", "--grace", "1m", "--apply"}); e != nil {
		t.Logf("reap: %v", e)
	}
	a.Now = now
	if !strings.Contains(out.String(), "action=reap") {
		t.Fatalf("reap did not select the job: %s", out.String())
	}
	if slices.Contains(eventKinds(t, a), "job.reaped") {
		t.Fatal("reaped a job whose hub close is open")
	}
	if st := str(h.record(t, "j"), "state"); st == "lost" {
		t.Fatal("reap wrote a generic lost record before settlement")
	}
	if state, e := (HubCloseSettlement{App: a}).Settle(context.Background(), "j"); e != nil || state != HubCloseSettled {
		t.Fatalf("settle %v, %v", state, e)
	}
	wantHubCloseFinished(t, a, h)
}

// #662 round 4: an announced quota stop is watchable, so the fence release
// re-arms its watcher (the hand-off grace fallback needs one).
func TestFenceStateCountsAnAnnouncedQuotaStopLive_t4632(t *testing.T) {
	a, _ := hubCloseFixture(t)
	if e := a.event(context.Background(), "j", "job.lost", map[string]any{"reason": "quota_stop", "pool": "devin-free", "pane_id": "w:p1"}); e != nil {
		t.Fatal(e)
	}
	if open, live, e := HubCloseFenceState(a.Jobs, "j"); e != nil || open || !live {
		t.Fatalf("open=%v live=%v err=%v; want a watchable hand-off", open, live, e)
	}
}

// #662 round 4: a generic end holds the end lock through its end when no
// close owns the job, and fails closed on an open intent or an unreadable
// ledger.
func TestGenericEndOwnershipUnderTheEndLock_t4632(t *testing.T) {
	a, _ := hubCloseFixture(t)
	owned, unlock := a.lockUnlessHubCloseOwnsEnd(context.Background(), "j")
	if owned || unlock == nil {
		t.Fatal("owned with no intent open")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	if _, e := LockJobEndContext(ctx, filepath.Join(a.Jobs, "j")); e == nil {
		t.Fatal("end lock free while the generic end holds it")
	}
	cancel()
	unlock()
	recordHubClose(t, a, liveIntent(t, a))
	if owned, _ := a.lockUnlessHubCloseOwnsEnd(context.Background(), "j"); !owned {
		t.Fatal("not owned with an open intent")
	}
	if e := os.WriteFile(filepath.Join(a.Jobs, "j", "events", "99998-job.bad.json"), []byte("{"), 0o600); e != nil {
		t.Fatal(e)
	}
	if owned, _ := a.lockUnlessHubCloseOwnsEnd(context.Background(), "j"); !owned {
		t.Fatal("not owned with an unreadable ledger")
	}
}

// #662 round 4: the job-dir TTL keeps a dir whose hub-close intent is open.
func TestJobDirTTLKeepsAnOpenHubClose_t4632(t *testing.T) {
	a, _ := hubCloseFixture(t)
	recordHubClose(t, a, liveIntent(t, a))
	var out strings.Builder
	p := &JobDirTTL{TTL: 24 * time.Hour, Out: &out, Only: []string{"j"}}
	if _, e := p.Pass(context.Background(), a); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(out.String(), "reason=hub-close-open") {
		t.Fatalf("ttl output %q, want the dir kept as hub-close-open", out.String())
	}
}

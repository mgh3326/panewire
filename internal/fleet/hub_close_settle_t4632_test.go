package fleet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// #572 r4576225999: the node journalled closed, then `panewire job close`
// wrote job.revoked before settlement ran. The close is still settled: hk
// takes the intended lost record and the lease is released; the revoke stays
// the job's local outcome. The daemon pass lists the job.
func TestSettlementFinishesAClosedOpAfterAnIndependentEnd_t4632(t *testing.T) {
	a, h := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpClosed})
	if e := a.event(context.Background(), "j", "job.revoked", map[string]any{}); e != nil {
		t.Fatal(e)
	}
	jobs, e := PendingHubCloseJobs(a.Jobs)
	if e != nil || len(jobs) != 1 || jobs[0] != "j" {
		t.Fatalf("pending = %v, %v; want [j]", jobs, e)
	}
	settle := HubCloseSettlement{App: a}
	if st, e := settle.Settle(context.Background(), "j"); e != nil || st != HubCloseSettled {
		t.Fatalf("settle = %v, %v", st, e)
	}
	if rec := h.record(t, "j"); str(rec, "state") != "lost" || str(rec, "end_reason") != hubCloseReason {
		t.Fatalf("hk = %v, want the intended lost record", rec)
	}
	if st, _ := ledgerState(t, a, "j"); st != "released" {
		t.Fatalf("quota %s, want released", st)
	}
	if j, _ := a.job("j"); j.Ended != "job.revoked" {
		t.Fatalf("job ended %q, want the revoke kept as its outcome", j.Ended)
	}
	if st, e := settle.Settle(context.Background(), "j"); e != nil || st != HubCloseNone {
		t.Fatalf("second settle = %v, %v; want none", st, e)
	}
	if jobs, _ := PendingHubCloseJobs(a.Jobs); len(jobs) != 0 {
		t.Fatalf("pending after settle = %v", jobs)
	}
}

// An independent end over an operation that closed nothing owes nothing: no
// settlement, no hk write. (One never started is preempted instead:
// TestSettlementPreemptsARegisteredCloseAnEndOvertook_t4632.)
func TestSettlementIgnoresAnEndedIntentThatClosedNothing_t4632(t *testing.T) {
	for _, state := range []string{CloseOpRefused, CloseOpPreempted} {
		t.Run("state="+state, func(t *testing.T) {
			a, h := hubCloseFixture(t)
			in := liveIntent(t, a)
			recordHubClose(t, a, in)
			if state != "" {
				writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: state})
			}
			if e := a.event(context.Background(), "j", "job.revoked", map[string]any{}); e != nil {
				t.Fatal(e)
			}
			if st, e := (HubCloseSettlement{App: a}).Settle(context.Background(), "j"); e != nil || st != HubCloseNone {
				t.Fatalf("settle = %v, %v; want none", st, e)
			}
			if str(h.record(t, "j"), "end_reason") == hubCloseReason {
				t.Fatal("hk took the intended record for a close that closed nothing")
			}
		})
	}
}

// #572 r4576226003: the node settled the close after the watcher took its
// snapshot. The watcher's stale decision, re-checked under the end lock, is
// dropped rather than written after the final job.lost.
func TestWatcherDropsADecisionStaleAfterASettledClose_t4632(t *testing.T) {
	a, _ := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpClosed})
	if st, e := (HubCloseSettlement{App: a}).Settle(context.Background(), "j"); e != nil || st != HubCloseSettled {
		t.Fatalf("settle = %v, %v", st, e)
	}
	ran := false
	deferred, e := a.endUnlessHubClose("j", snapshot(t, a), func(Job) error { ran = true; return nil })
	if e != nil || !deferred || ran {
		t.Fatalf("endUnlessHubClose = %v, %v (ran %v); want the stale decision dropped", deferred, e, ran)
	}
}

// A cancelled intent hands the decision back: it runs.
func TestWatcherRunsADecisionAfterACancelledIntent_t4632(t *testing.T) {
	a, _ := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	if e := a.recordHubCloseCancelForTest(in.RequestID); e != nil {
		t.Fatal(e)
	}
	ran := false
	deferred, e := a.endUnlessHubClose("j", snapshot(t, a), func(Job) error { ran = true; return nil })
	if e != nil || deferred || !ran {
		t.Fatalf("endUnlessHubClose = %v, %v (ran %v); want the decision run", deferred, e, ran)
	}
}

func (a *App) recordHubCloseCancelForTest(id string) error {
	return a.eventSynced(context.Background(), "j", hubCloseCancelKind, map[string]any{"request_id": id, "reason": "released"})
}

// #593 review: a reap after the independent end does not retire an owed
// close; the settlement records job.hub_close_settled, never a terminal
// job.lost, so the revoke stays the job's local outcome.
func TestOwedCloseSurvivesAReapAndKeepsTheRevoke_t4632(t *testing.T) {
	a, h := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpClosed})
	for _, kind := range []string{"job.revoked", "job.reaped"} {
		if e := a.event(context.Background(), "j", kind, map[string]any{}); e != nil {
			t.Fatal(e)
		}
	}
	if jobs, e := PendingHubCloseJobs(a.Jobs); e != nil || len(jobs) != 1 {
		t.Fatalf("pending = %v, %v; want the reaped job", jobs, e)
	}
	if st, e := (HubCloseSettlement{App: a}).Settle(context.Background(), "j"); e != nil || st != HubCloseSettled {
		t.Fatalf("settle = %v, %v", st, e)
	}
	if rec := h.record(t, "j"); str(rec, "state") != "lost" || str(rec, "end_reason") != hubCloseReason {
		t.Fatalf("hk = %v", rec)
	}
	if st, _ := ledgerState(t, a, "j"); st != "released" {
		t.Fatalf("quota %s, want released", st)
	}
	if lost := lostEvents(t, a); len(lost) != 0 {
		t.Fatalf("job.lost %v, want none after an independent end", lost)
	}
	j, _ := a.job("j")
	if j.Ended != "job.reaped" && j.Ended != "job.revoked" {
		t.Fatalf("job ended %q, want the independent end kept", j.Ended)
	}
	if jobs, _ := PendingHubCloseJobs(a.Jobs); len(jobs) != 0 {
		t.Fatalf("pending after settle = %v", jobs)
	}
}

// #593 review: the node's own settlement of B retires an older unknown A of
// the same attempt; A is never settled into a second final job.lost.
func TestOwnSettlementRetiresTheAttemptsOtherIntents_t4632(t *testing.T) {
	a, h := hubCloseFixture(t)
	first := liveIntent(t, a)
	recordHubClose(t, a, first)
	writeJobCloseOp(t, a, CloseOp{RequestID: first.RequestID, Attempt: first.Attempt, State: CloseOpUnknown})
	second := first
	second.RequestID, second.EndReason = "close-2", hubCloseReason
	recordHubClose(t, a, second)
	writeJobCloseOp(t, a, CloseOp{RequestID: second.RequestID, Attempt: second.Attempt, State: CloseOpClosed})
	settle := HubCloseSettlement{App: a}
	if st, e := settle.Settle(context.Background(), "j"); e != nil || st != HubCloseSettled {
		t.Fatalf("settle = %v, %v", st, e)
	}
	if st, e := settle.Settle(context.Background(), "j"); e != nil || st != HubCloseNone {
		t.Fatalf("second settle = %v, %v; want none", st, e)
	}
	if lost := lostEvents(t, a); len(lost) != 1 || str(lost[0], "request_id") != "close-2" {
		t.Fatalf("job.lost %v, want one, for close-2", lost)
	}
	_ = h
}

// #593 review: a watcher whose attempt the node's settlement ended exits.
func TestWatcherExitsAfterASettledClose_t4632(t *testing.T) {
	a, _ := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpClosed})
	if st, e := (HubCloseSettlement{App: a}).Settle(context.Background(), "j"); e != nil || st != HubCloseSettled {
		t.Fatalf("settle = %v, %v", st, e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if e := a.watch(ctx, []string{"j"}); e != nil {
		t.Fatalf("watch = %v, want a clean exit", e)
	}
	if lost := lostEvents(t, a); len(lost) != 1 {
		t.Fatalf("job.lost %v, want only the settlement's", lost)
	}
}

// #593 od-4a876257a3: a node crash left the operation closing, then a revoke
// ended the attempt. No hub repeat will come; the daemon settles it once the
// pane inventory shows the pane absent, and waits while it is listed.
func TestSettlementRecoversAClosingOpAfterAnIndependentEnd_t4632(t *testing.T) {
	a, h := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpClosing})
	if e := a.event(context.Background(), "j", "job.revoked", map[string]any{}); e != nil {
		t.Fatal(e)
	}
	listed := true
	settle := HubCloseSettlement{App: a, PaneListed: func(context.Context, string, string) (bool, error) { return listed, nil }}
	if st, e := settle.Settle(context.Background(), "j"); e != nil || st != HubClosePending {
		t.Fatalf("settle with the pane listed = %v, %v; want pending", st, e)
	}
	listed = false
	if st, e := settle.Settle(context.Background(), "j"); e != nil || st != HubCloseSettled {
		t.Fatalf("settle with the pane gone = %v, %v; want settled", st, e)
	}
	if rec := h.record(t, "j"); str(rec, "state") != "lost" || str(rec, "end_reason") != hubCloseReason {
		t.Fatalf("hk = %v", rec)
	}
	if st, _ := ledgerState(t, a, "j"); st != "released" {
		t.Fatalf("quota %s, want released", st)
	}
}

// #593 review: an owed close after an independent end fences the watcher's
// decisions (no stale re-prompt revives the job), and its settlement, which
// writes job.hub_close_settled, drops them too.
func TestOwedEndedCloseFencesWatcherDecisions_t4632(t *testing.T) {
	a, _ := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpUnknown})
	if e := a.event(context.Background(), "j", "job.revoked", map[string]any{}); e != nil {
		t.Fatal(e)
	}
	ran := false
	decide := func(Job) error { ran = true; return nil }
	if deferred, e := a.endUnlessHubClose("j", snapshot(t, a), decide); e != nil || !deferred || ran {
		t.Fatalf("owed: deferred=%v err=%v ran=%v; want the decision fenced", deferred, e, ran)
	}
	if st, e := (HubCloseSettlement{App: a}).Settle(context.Background(), "j"); e != nil || st != HubCloseSettled {
		t.Fatalf("settle = %v, %v", st, e)
	}
	if deferred, e := a.endUnlessHubClose("j", snapshot(t, a), decide); e != nil || !deferred || ran {
		t.Fatalf("settled: deferred=%v err=%v ran=%v; want the stale decision dropped", deferred, e, ran)
	}
	if j, _ := a.job("j"); !foldHubClose(j).settled || j.Ended != "job.revoked" {
		t.Fatalf("settled=%v ended=%q", foldHubClose(j).settled, j.Ended)
	}
}

// snapshot is the job as the watcher's poll reads it.
func snapshot(t *testing.T, a *App) Job {
	t.Helper()
	j, e := a.job("j")
	if e != nil {
		t.Fatal(e)
	}
	return j
}

// #593 review (consult B1): a decision made on a snapshot that an end or a
// new attempt overtook is dropped under the end lock; a cancelled intent
// moves no attempt, so it does not make a decision stale.
func TestWatcherDropsADecisionAnEndOvertook_t4632(t *testing.T) {
	a, _ := hubCloseFixture(t)
	seen := snapshot(t, a)
	if e := a.event(context.Background(), "j", "job.revoked", map[string]any{}); e != nil {
		t.Fatal(e)
	}
	ran := false
	if deferred, e := a.endUnlessHubClose("j", seen, func(Job) error { ran = true; return nil }); e != nil || !deferred || ran {
		t.Fatalf("deferred=%v err=%v ran=%v; want the decision dropped", deferred, e, ran)
	}
}

// #593 review r4577077136, r4577077150: an independent end that wins before
// the close journalled closing makes the registered operation preemptible.
// Settlement, under the end lock, journals it preempted and cancels the
// intent: no hk write, no lease change, and a hub repeat replays preempted.
func TestSettlementPreemptsARegisteredCloseAnEndOvertook_t4632(t *testing.T) {
	for _, journalled := range []bool{true, false} {
		t.Run(fmt.Sprintf("journalled=%v", journalled), func(t *testing.T) {
			a, h := hubCloseFixture(t)
			in := liveIntent(t, a)
			recordHubClose(t, a, in)
			if journalled {
				writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpRegistered})
			}
			if e := a.event(context.Background(), "j", "job.revoked", map[string]any{}); e != nil {
				t.Fatal(e)
			}
			if jobs, e := PendingHubCloseJobs(a.Jobs); e != nil || len(jobs) != 1 {
				t.Fatalf("pending = %v, %v", jobs, e)
			}
			if st, e := (HubCloseSettlement{App: a}).Settle(context.Background(), "j"); e != nil || st != HubCloseCancelled {
				t.Fatalf("settle = %v, %v; want cancelled", st, e)
			}
			if journalled {
				op, found, e := a.readJobCloseOp("j", in.RequestID)
				if e != nil || !found || op.State != CloseOpPreempted {
					t.Fatalf("op = %+v found=%v err=%v; want preempted", op, found, e)
				}
			}
			if str(h.record(t, "j"), "end_reason") == hubCloseReason {
				t.Fatal("hk took the close's record")
			}
			if jobs, _ := PendingHubCloseJobs(a.Jobs); len(jobs) != 0 {
				t.Fatalf("pending after preempt = %v", jobs)
			}
		})
	}
}

// #593 review: an interrupted done (job.record_settled without job.completed)
// resumes from the report its receipt names, only while that file still has
// the settled digest; the spawn's report is the fallback for an older
// receipt; a new attempt or job.completed clears it.
func TestInterruptedCompletion_t4632(t *testing.T) {
	dir := t.TempDir()
	settled := filepath.Join(dir, "settled.md")
	other := filepath.Join(dir, "spawn-default.md")
	for path, body := range map[string]string{settled: "settled report\n", other: "another report\n"} {
		if e := os.WriteFile(path, []byte(body), 0o600); e != nil {
			t.Fatal(e)
		}
	}
	digest := reportDigest([]byte("settled report\n"))
	ev := func(seq int, kind string, payload map[string]any) Event {
		return Event{Kind: kind, name: fmt.Sprintf("%05d-%s.json", seq, kind), Payload: payload}
	}
	withPath := map[string]any{"report_sha256": digest, "report": settled}
	noPath := map[string]any{"report_sha256": digest}
	for name, test := range map[string]struct {
		spawnReport string
		events      []Event
		want        string
	}{
		"receipt names the report":        {other, []Event{ev(1, "job.spawned", nil), ev(2, eventRecordSettled, withPath)}, settled},
		"older receipt, spawn report":     {settled, []Event{ev(1, "job.spawned", nil), ev(2, eventRecordSettled, noPath)}, settled},
		"spawn report has another digest": {other, []Event{ev(1, "job.spawned", nil), ev(2, eventRecordSettled, noPath)}, ""},
		"no report known":                 {"", []Event{ev(1, "job.spawned", nil), ev(2, eventRecordSettled, noPath)}, ""},
		"notified":                        {other, []Event{ev(1, "job.spawned", nil), ev(2, eventRecordSettled, withPath), ev(3, "job.completed", nil)}, ""},
		"a new attempt":                   {other, []Event{ev(1, eventRecordSettled, withPath), ev(2, "job.reprompted", nil)}, ""},
		"never settled":                   {other, []Event{ev(1, "job.spawned", nil)}, ""},
	} {
		got, ok := interruptedCompletion(Job{ID: "j", Report: test.spawnReport, Events: test.events})
		if got != test.want || ok != (test.want != "") {
			t.Errorf("%s: %q %v, want %q", name, got, ok, test.want)
		}
	}
	if e := os.WriteFile(settled, []byte("edited after settling\n"), 0o600); e != nil {
		t.Fatal(e)
	}
	if _, ok := interruptedCompletion(Job{ID: "j", Events: []Event{ev(1, eventRecordSettled, withPath)}}); ok {
		t.Error("a report changed since settling is never published in its place")
	}
}

// #593 review: a hand-off decision on an attempt that already ended is
// dropped even when the end came before the poll's snapshot; an announced
// quota stop (the hand-over itself) is not such an end.
func TestHandOffDecisionDroppedAfterAnEarlierEnd_t4632(t *testing.T) {
	a, _ := hubCloseFixture(t)
	if e := a.event(context.Background(), "j", "job.lost", map[string]any{"reason": "quota_stop", "final": false}); e != nil {
		t.Fatal(e)
	}
	ran := false
	decide := func(Job) error { ran = true; return nil }
	if deferred, e := a.endUnlessHubClose("j", snapshot(t, a), decide); e != nil || deferred || !ran {
		t.Fatalf("announced stop: deferred=%v err=%v ran=%v; want the hand-off decision run", deferred, e, ran)
	}
	ran = false
	if e := a.event(context.Background(), "j", "job.revoked", map[string]any{}); e != nil {
		t.Fatal(e)
	}
	if deferred, e := a.endUnlessHubClose("j", snapshot(t, a), decide); e != nil || !deferred || ran {
		t.Fatalf("revoked before the snapshot: deferred=%v err=%v ran=%v; want dropped", deferred, e, ran)
	}
}

// #593 review: restore waits while a hub close of the job can still act.
func TestHubCloseInFlightFencesRestore_t4632(t *testing.T) {
	a, _ := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpClosing})
	if inFlight, e := a.hubCloseInFlight(snapshot(t, a)); e != nil || !inFlight {
		t.Fatalf("closing op: in flight=%v err=%v", inFlight, e)
	}
	if e := a.restore(context.Background(), snapshot(t, a)); e == nil || !strings.Contains(e.Error(), "in flight") {
		t.Fatalf("restore = %v, want refused while the close is in flight", e)
	}
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpRefused})
	if e := a.recordHubCloseCancelForTest(in.RequestID); e != nil {
		t.Fatal(e)
	}
	if inFlight, e := a.hubCloseInFlight(snapshot(t, a)); e != nil || inFlight {
		t.Fatalf("after the close resolved: in flight=%v err=%v", inFlight, e)
	}
}

// #593 review: `done`'s end-lock wait follows its caller's context, so an
// in-process hub job.done never holds the node's acting lock past its
// command deadline behind a held end lock.
func TestDoneEndLockWaitFollowsItsContext_t4632(t *testing.T) {
	a, _ := hubCloseFixture(t)
	report := filepath.Join(t.TempDir(), "report.md")
	if e := os.WriteFile(report, []byte("r\n"), 0o600); e != nil {
		t.Fatal(e)
	}
	unlock, e := LockJobEnd(filepath.Join(a.Jobs, "j"))
	if e != nil {
		t.Fatal(e)
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	if e := a.lifecycle(ctx, "done", []string{"j", "--report", report}); e == nil {
		t.Fatal("done ran under a held end lock")
	}
	if waited := time.Since(start); waited > 5*time.Second {
		t.Fatalf("done waited %v past its 200ms context", waited)
	}
}

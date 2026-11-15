package fleet

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// #572 r4576225999: the node journalled closed, then `panewire job close`
// wrote job.revoked before settlement ran. The close is still settled: hk
// takes the intended lost record, the lease is released, and the final
// job.lost is written once. The daemon pass lists the job.
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
	wantHubCloseFinished(t, a, h)
	if st, e := settle.Settle(context.Background(), "j"); e != nil || st != HubCloseNone {
		t.Fatalf("second settle = %v, %v; want none", st, e)
	}
	if jobs, _ := PendingHubCloseJobs(a.Jobs); len(jobs) != 0 {
		t.Fatalf("pending after settle = %v", jobs)
	}
}

// An independent end over an operation that closed nothing (or was never
// started) owes nothing: no settlement, no hk write.
func TestSettlementIgnoresAnEndedIntentThatClosedNothing_t4632(t *testing.T) {
	for _, state := range []string{"", CloseOpRegistered, CloseOpRefused, CloseOpPreempted} {
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
	deferred, e := a.endUnlessHubClose("j", func(Job) error { ran = true; return nil })
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
	deferred, e := a.endUnlessHubClose("j", func(Job) error { ran = true; return nil })
	if e != nil || deferred || !ran {
		t.Fatalf("endUnlessHubClose = %v, %v (ran %v); want the decision run", deferred, e, ran)
	}
}

func (a *App) recordHubCloseCancelForTest(id string) error {
	return a.eventSynced(context.Background(), "j", hubCloseCancelKind, map[string]any{"request_id": id, "reason": "released"})
}

// The barrier step refuses while an old-binary watcher or a spawn launcher
// runs, and otherwise records a mode-0600 marker that CloseOpsReady reads.
func TestCloseOpsBarrier_t4632(t *testing.T) {
	jobs := t.TempDir()
	if e := os.MkdirAll(filepath.Join(jobs, "j1", watchCapsDir), 0o700); e != nil {
		t.Fatal(e)
	}
	list := func(out string) ProcessLister {
		return func(context.Context) ([]byte, error) { return []byte(out), nil }
	}
	now := time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)
	ctx := context.Background()
	if CloseOpsReady(ctx, jobs, list("")) {
		t.Fatal("ready before the step")
	}
	oldWatcher := "4242 panewire _watch j1\n"
	if e := EnableCloseOps(ctx, jobs, "v1", now, list(oldWatcher)); !errors.Is(e, ErrCloseOpsNotReady) {
		t.Fatalf("enable with an old watcher = %v", e)
	}
	if e := EnableCloseOps(ctx, jobs, "v1", now, list("4343 /usr/local/bin/panewire _spawn-stdin\n")); !errors.Is(e, ErrCloseOpsNotReady) {
		t.Fatalf("enable with a launcher = %v", e)
	}
	if _, e := os.Stat(filepath.Join(jobs, CloseOpsMarker)); !errors.Is(e, os.ErrNotExist) {
		t.Fatalf("a refused step left a marker: %v", e)
	}
	// A watcher of this binary carries its token.
	if e := os.WriteFile(filepath.Join(jobs, "j1", watchCapsDir, "4242"), []byte(WatcherCapsToken+"\n"), 0o600); e != nil {
		t.Fatal(e)
	}
	self := strconv.Itoa(os.Getpid()) + " panewire _spawn-stdin\n"
	if e := EnableCloseOps(ctx, jobs, "v1", now, list(oldWatcher+self)); e != nil {
		t.Fatalf("enable = %v", e)
	}
	info, e := os.Stat(filepath.Join(jobs, CloseOpsMarker))
	if e != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("marker %v, %v; want mode 0600", info, e)
	}
	if !CloseOpsReady(ctx, jobs, list(oldWatcher)) {
		t.Fatal("not ready after the step")
	}
	// An old-binary watcher that starts later takes readiness away again.
	if CloseOpsReady(ctx, jobs, list("5151 panewire _watch j1\n")) {
		t.Fatal("ready with a token-less watcher")
	}
	if CloseOpsReady(ctx, jobs, func(context.Context) ([]byte, error) { return nil, errors.New("ps failed") }) {
		t.Fatal("ready on a failed listing")
	}
}

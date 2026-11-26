package panewire

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"
)

// The quota_ladder ledger counts a quota successor made while its pointer is
// still pending, and done when the pointer is written or the successor leaves
// pending by another path. A done counts only for a successor made that day
// (hk 6312). These tests drive the states the old rule counted every day: a
// pointer written on an earlier day, a successor that ended unsent, a stuck
// pending one, and a settled day shaped like production.

// setQuotaLedgerDay pins the hub's clock to at, so a test moves between UTC days.
func setQuotaLedgerDay(h *HubServer, at time.Time) {
	h.now = func() time.Time { return at }
}

// claimSuccessorOf claims the quota successor of old the way a replacement does,
// leaving it accepted and unsent. Only the ledger tests use it, for several
// predecessors in one fixture.
func (f *quotaReplaceFixture) claimSuccessorOf(t *testing.T, old string) hubSpawnTask {
	t.Helper()
	next, _, ok := quotaReplacementJob(old)
	if !ok {
		t.Fatalf("%q has no quota successor name", old)
	}
	request, err := quotaReplacementRequest(f.holder.Request, old, next, "claude", "claude", "reviewer")
	if err != nil {
		t.Fatal(err)
	}
	successor, _, err := f.hub.inbox.store.claimSpawnFrom(t.Context(), request, next, "parent", time.Now(), old)
	if err != nil {
		t.Fatal(err)
	}
	return successor
}

// A pointer written on an earlier day is not made again: the day it was written
// counts made and done, and the next day, whether the hub kept its settled
// verdict or restarted with an empty ledger, counts nothing for it.
func TestQuotaSuccessorWrittenOnAnEarlierDayIsNotMadeToday_t6312(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-61-p1")
	setQuotaLedgerDay(f.hub, time.Now().Add(-24*time.Hour))
	successor := f.quotaClaimedSuccessor(t, "accepted")
	f.bindSuccessor(t, successor, true)
	f.holdsClaim(t, successor.Job)
	f.paneClosed = true
	wrote := make(chan int, 1)
	go func() { wrote <- f.hub.reconcileQuotaReplacements(t.Context()) }()
	commands := f.serveCommands(t, 2) // probe (pane gone), mark_lost
	reason := endReasonOf(t, commands[1])
	if n := <-wrote; n != 1 {
		t.Fatalf("wrote %d records on the first day, want 1", n)
	}
	// hk keeps the lost record with its pointer; the fake job store does not apply
	// the node's mark_lost, so the test writes the record the way hk would.
	f.jobs.records[f.job]["state"] = "lost"
	f.jobs.records[f.job]["end_reason"] = reason
	assertQuotaLadderLedger(t, f.hub, 1, 1)

	setQuotaLedgerDay(f.hub, time.Now())
	if n := f.reconcileSendingNothing(t); n != 0 {
		t.Fatalf("the next day wrote %d records, want none", n)
	}
	assertQuotaLadderLedger(t, f.hub, 0, 0)

	// A restarted hub has an empty ledger and no settled verdicts: its first pass
	// reads the predecessor's record and still counts nothing for the day.
	f.hub.loops = nil
	if n := f.reconcileSendingNothing(t); n != 0 {
		t.Fatalf("the restarted hub wrote %d records, want none", n)
	}
	assertQuotaLadderLedger(t, f.hub, 0, 0)
}

// A successor that ends unsent (failed or given_up) counts done on the day it
// ends, and only if it was made that day. The day after it ends it counts
// nothing. One that ended before the hub ever saw it pending counts nothing.
func TestQuotaSuccessorEndedUnsentIsDoneOnlyOnTheDayItWasMade_t6312(t *testing.T) {
	for _, ended := range []string{"given_up", "failed"} {
		t.Run(ended+" after it was pending", func(t *testing.T) {
			f := newQuotaReplaceFixture(t, "reviewer", "mwr-62-p1")
			today := time.Now()
			setQuotaLedgerDay(f.hub, today)
			successor := f.quotaClaimedSuccessor(t, "accepted") // unsent, predecessor spawned: pending
			f.hub.reconcileQuotaReplacements(t.Context())
			assertQuotaLadderLedger(t, f.hub, 1, 0)

			f.setSuccessorStatus(t, successor, ended)
			f.hub.reconcileQuotaReplacements(t.Context())
			assertQuotaLadderLedger(t, f.hub, 1, 1)

			setQuotaLedgerDay(f.hub, today.Add(24*time.Hour))
			f.hub.reconcileQuotaReplacements(t.Context())
			assertQuotaLadderLedger(t, f.hub, 0, 0)
		})
	}

	t.Run("ended before the hub saw it pending", func(t *testing.T) {
		f := newQuotaReplaceFixture(t, "reviewer", "mwr-62-p2")
		setQuotaLedgerDay(f.hub, time.Now())
		successor := f.quotaClaimedSuccessor(t, "accepted")
		f.setSuccessorStatus(t, successor, "failed")
		f.hub.reconcileQuotaReplacements(t.Context())
		assertQuotaLadderLedger(t, f.hub, 0, 0)
	})
}

// A sent successor whose pointer is written in the pass that saw it pending is
// made and done on that pass.
func TestQuotaSentPendingSuccessorIsMadeAndDoneWhenItsPointerIsWritten_t6312(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-63-p1")
	setQuotaLedgerDay(f.hub, time.Now())
	successor := f.quotaClaimedSuccessor(t, "accepted")
	f.bindSuccessor(t, successor, true)
	f.holdsClaim(t, successor.Job)
	f.paneClosed = true
	wrote := make(chan int, 1)
	go func() { wrote <- f.hub.reconcileQuotaReplacements(t.Context()) }()
	f.serveCommands(t, 2) // probe (pane gone), mark_lost
	if n := <-wrote; n != 1 {
		t.Fatalf("wrote %d records, want 1", n)
	}
	assertQuotaLadderLedger(t, f.hub, 1, 1)
}

// A pending successor that stays unresolved is made once a day and never done,
// however many passes see it. Twenty such make the progress rule fire.
func TestQuotaPendingSuccessorStaysMadeNotDone_t6312(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-64-p1")
	setQuotaLedgerDay(f.hub, time.Now())
	f.quotaClaimedSuccessor(t, "accepted") // queued and unsent; predecessor still spawned
	for range 3 {
		f.hub.reconcileQuotaReplacements(t.Context())
	}
	assertQuotaLadderLedger(t, f.hub, 1, 0)
	if slices.Contains(f.hub.loopsProgressHits(), loopQuotaLadder) {
		t.Fatal("one stuck successor fired the progress rule below the floor")
	}
	for i := range 19 {
		old := fmt.Sprintf("mwr-64-old-%02d", i)
		f.jobs.records[old] = map[string]any{"state": "spawned", "pool": "claude"}
		holderRequest := quotaHolderRequest("reviewer", old)
		holderRequest.RequestID = fmt.Sprintf("d676220b-e763-4af1-aefd-%012d", 100+i)
		taskArg := slices.Index(holderRequest.Args, "--task")
		holderRequest.Args[taskArg+1] = fmt.Sprintf("%d", 100+i)
		holder, _, err := f.hub.inbox.store.claimSpawn(t.Context(), holderRequest, old, "parent", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.hub.inbox.store.completeSpawn(t.Context(), holder, hubSpawnOutcome{RC: 0, Stage: "done", Host: "machine-a", Pane: roleHeldPane}, time.Now()); err != nil {
			t.Fatal(err)
		}
		next, _, ok := quotaReplacementJob(old)
		if !ok {
			t.Fatalf("%q has no quota successor name", old)
		}
		request, err := quotaReplacementRequest(holderRequest, old, next, "claude", "claude", "reviewer")
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := f.hub.inbox.store.claimSpawnFrom(t.Context(), request, next, "parent", time.Now(), old); err != nil {
			t.Fatal(err)
		}
	}
	f.hub.reconcileQuotaReplacements(t.Context())
	assertQuotaLadderLedger(t, f.hub, 20, 0)
	if !slices.Contains(f.hub.loopsProgressHits(), loopQuotaLadder) {
		t.Fatalf("made=20 done=0 has no progress hit: %+v", f.hub.loopsProgressHits())
	}
}

// A transient predecessor-record read after a successor was made must leave
// its daily made/done verdict alone and report the read failure.
func TestQuotaSuccessorTransientPredecessorReadKeepsMadeNotDone_t6691(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-69-p1")
	setQuotaLedgerDay(f.hub, time.Now())
	f.quotaClaimedSuccessor(t, "accepted")
	f.hub.reconcileQuotaReplacements(t.Context())
	assertQuotaLadderLedger(t, f.hub, 1, 0)

	f.jobs.failGets = 1
	f.hub.reconcileQuotaReplacements(t.Context())
	assertQuotaLadderLedger(t, f.hub, 1, 0)
	if got := f.hub.loopSnapshot()[loopQuotaLadder].SilentErrors; got != 1 {
		t.Fatalf("quota_ladder silent_errors=%d, want 1", got)
	}
}

// A malformed holder payload makes latestSpawnForJob fail after the successor
// scan succeeds; that unknown read must preserve the earlier made verdict.
func TestQuotaSuccessorTransientHolderReadKeepsMadeNotDone_t6691(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-69-p2")
	setQuotaLedgerDay(f.hub, time.Now())
	f.quotaClaimedSuccessor(t, "accepted")
	f.hub.reconcileQuotaReplacements(t.Context())
	assertQuotaLadderLedger(t, f.hub, 1, 0)

	if _, err := f.hub.inbox.store.db.ExecContext(t.Context(), `UPDATE spawn_operations SET payload='{' WHERE request_id=?`, f.holder.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	f.hub.reconcileQuotaReplacements(t.Context())
	assertQuotaLadderLedger(t, f.hub, 1, 0)
	if got := f.hub.loopSnapshot()[loopQuotaLadder].SilentErrors; got != 1 {
		t.Fatalf("quota_ladder silent_errors=%d, want 1", got)
	}
}

// A succeeded successor is not pending when a permanent pointer prerequisite
// cannot be met. These cases used to be re-counted as made on every UTC day.
func TestQuotaSuccessorWithImpossiblePointerIsNotMade_t6676(t *testing.T) {
	t.Run("non-reviewer holder", func(t *testing.T) {
		f := newQuotaReplaceFixture(t, "reviewer", "mwr-66-p1")
		successor := f.claimSuccessorOf(t, f.job)
		f.setSuccessorStatus(t, successor, "succeeded")
		roleArg := slices.Index(f.holder.Request.Args, "--role")
		if roleArg < 0 {
			t.Fatal("holder has no --role")
		}
		f.holder.Request.Args[roleArg+1] = "implementer"
		if passArg := slices.Index(f.holder.Request.Args, "--review-pass"); passArg >= 0 {
			f.holder.Request.Args = slices.Delete(f.holder.Request.Args, passArg, passArg+2)
		}
		payload, err := json.Marshal(f.holder.Request)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.hub.inbox.store.db.ExecContext(t.Context(), `UPDATE spawn_operations SET payload=? WHERE request_id=?`, string(payload), f.holder.Request.RequestID); err != nil {
			t.Fatal(err)
		}
		for day := range 2 {
			setQuotaLedgerDay(f.hub, time.Now().Add(time.Duration(day)*24*time.Hour))
			f.reconcileSendingNothing(t)
			assertQuotaLadderLedger(t, f.hub, 0, 0)
		}
	})

	t.Run("missing predecessor record", func(t *testing.T) {
		f := newQuotaReplaceFixture(t, "reviewer", "mwr-66-p2")
		successor := f.claimSuccessorOf(t, f.job)
		f.setSuccessorStatus(t, successor, "succeeded")
		delete(f.jobs.records, f.job)
		f.hub.reconcileQuotaReplacements(t.Context())
		assertQuotaLadderLedger(t, f.hub, 0, 0)
	})

	t.Run("holder has not succeeded", func(t *testing.T) {
		f := newQuotaReplaceFixture(t, "reviewer", "mwr-66-p4")
		successor := f.claimSuccessorOf(t, f.job)
		f.setSuccessorStatus(t, successor, "succeeded")
		if _, err := f.hub.inbox.store.db.ExecContext(t.Context(), `UPDATE spawn_operations SET status='failed' WHERE request_id=?`, f.holder.Request.RequestID); err != nil {
			t.Fatal(err)
		}
		f.hub.reconcileQuotaReplacements(t.Context())
		assertQuotaLadderLedger(t, f.hub, 0, 0)
	})

	t.Run("lineage mismatch", func(t *testing.T) {
		f := newQuotaReplaceFixture(t, "reviewer", "mwr-66-p5")
		successor := f.claimSuccessorOf(t, f.job)
		f.setSuccessorStatus(t, successor, "succeeded")
		if _, err := f.hub.inbox.store.db.ExecContext(t.Context(), `UPDATE spawn_operations SET replaced_reason='backfill' WHERE request_id=?`, successor.Request.RequestID); err != nil {
			t.Fatal(err)
		}
		f.hub.reconcileQuotaReplacements(t.Context())
		assertQuotaLadderLedger(t, f.hub, 0, 0)
	})

	t.Run("predecessor pane remains present past replacement budget", func(t *testing.T) {
		f := newQuotaReplaceFixture(t, "reviewer", "mwr-66-p3")
		successor := f.claimSuccessorOf(t, f.job)
		f.setSuccessorStatus(t, successor, "succeeded")
		f.paneStatus = "idle"
		setQuotaLedgerDay(f.hub, time.Now().Add(quotaReplaceBudget))
		done := make(chan struct{})
		go func() {
			defer close(done)
			f.hub.reconcileQuotaReplacements(t.Context())
		}()
		f.serveCommands(t, 1) // present stopped pane is left alone without a stop sequence
		<-done
		assertQuotaLadderLedger(t, f.hub, 1, 0)
	})
}

// A day shaped like production (60 successors, none pending) counts nothing and
// does not fire: 30 succeeded whose pointers were written earlier, 15 failed and
// 15 given_up.
func TestQuotaProductionShapedDayDoesNotFire_t6312(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-65-p1")
	setQuotaLedgerDay(f.hub, time.Now())
	for i := range 60 {
		old := fmt.Sprintf("mwr-65-old-%02d", i)
		switch {
		case i < 30:
			f.jobs.records[old] = map[string]any{"state": "lost", "pool": "claude", "end_reason": "quota_stop pool=claude replaced_by=" + old + "-q1"}
			f.setSuccessorStatus(t, f.claimSuccessorOf(t, old), "succeeded")
		case i < 45:
			f.jobs.records[old] = map[string]any{"state": "spawned", "pool": "claude"}
			f.setSuccessorStatus(t, f.claimSuccessorOf(t, old), "failed")
		default:
			f.jobs.records[old] = map[string]any{"state": "spawned", "pool": "claude"}
			f.setSuccessorStatus(t, f.claimSuccessorOf(t, old), "given_up")
		}
	}
	f.hub.reconcileQuotaReplacements(t.Context())
	assertQuotaLadderLedger(t, f.hub, 0, 0)
	if slices.Contains(f.hub.loopsProgressHits(), loopQuotaLadder) {
		t.Fatal("a settled production-shaped day fired the quota_ladder progress rule")
	}
}

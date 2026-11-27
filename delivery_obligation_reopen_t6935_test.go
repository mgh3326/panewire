package panewire

// hk 6935 (the consult on #927, thread lJk7): an exhausted row is a verdict on
// the fault it held when its budget ran out. A later drop that settles that
// maybe-typed fault to a safe one reopens the row while budget remains, and the
// next pass queues the copy the exhaustion ended.

import (
	"testing"
	"time"
)

func TestSafeDropReopensExhaustedRowWithBudget(t *testing.T) {
	for name, how := range map[string]struct {
		report string
		settle t6820Drop
	}{
		"prompt rejected, inject budget settles": {
			report: relayUnconfirmedPromptFailed,
			settle: t6820Drop{drop: relayDroppedRetriesExhausted, harness: "claude"},
		},
		"unproven pane read, read failure settles": {
			report: "maybe_in_pane presend:unproven:read_failed",
			settle: t6820Drop{drop: relayDroppedPaneReadFailed, harness: "claude"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			rig := newT6384Rig(t)
			d := rig.send("lane-a", "reopen-6935")
			t6820Report(t, rig, d, t6820Drop{report: how.report})
			rig.pass(0)
			copy := rig.injected()
			// The labelled copy's own pass finds its maybe-typed fault spent.
			rig.pass(time.Minute)
			if o := rig.obligation(d.EventID); o.State != obligationExhausted || o.Spent != 2 {
				t.Fatalf("before the settling drop: %+v", o)
			}
			t6820Report(t, rig, copy, t6820Drop{drop: how.settle.drop, harness: how.settle.harness})
			t6820AwaitDropExpiry(t, rig, copy)
			if o := rig.obligation(d.EventID); o.State != obligationOwed || o.Spent != 2 || o.Fault != how.settle.drop {
				t.Fatalf("after the safe drop, want owed with budget left: %+v", o)
			}
			rig.pass(0)
			rig.injected()
			if o := rig.obligation(d.EventID); o.Spent != relayReplayMaxAttempts {
				t.Fatalf("the reopened row's copy did not spend the budget: %+v", o)
			}
		})
	}
}

// A safe drop does not reopen an exhausted row whose budget is spent: that
// verdict stands. Killed by: reopening on the fault alone, without spent<budget.
func TestSafeDropKeepsExhaustedRowWithSpentBudget(t *testing.T) {
	rig := newT6384Rig(t)
	d := rig.send("lane-a", "spent-6935")
	for _, wait := range []time.Duration{0, time.Minute} {
		t6820Report(t, rig, d, t6820ReadFailed)
		rig.pass(wait)
		d = rig.injected()
	}
	rig.pass(5 * time.Minute)
	if o := rig.obligation(d.EventID); o.State != obligationExhausted || o.Spent != relayReplayMaxAttempts {
		t.Fatalf("before the safe drop: %+v", o)
	}
	t6820Report(t, rig, d, t6820Drop{drop: relayDroppedRetriesExhausted, harness: "claude"})
	t6820AwaitDropExpiry(t, rig, d)
	// Checked before any pass: a pass would exhaust a reopened row again and hide it.
	if o := rig.obligation(d.EventID); o.State != obligationExhausted {
		t.Fatalf("spent row reopened by the safe drop: %+v", o)
	}
	rig.pass(time.Minute)
	if got := t3426Injected(rig.agent); len(got) != 0 {
		t.Fatalf("a spent budget was copied again after a safe drop: %+v", got)
	}
	if o := rig.obligation(d.EventID); o.State != obligationExhausted {
		t.Fatalf("spent row reopened: %+v", o)
	}
}

// A working-expired row ends with no budget copy by design, and a later safe
// drop that overwrites its fault does not earn copies back: only a fault
// settled from a maybe-typed verdict reopens. Killed by: reopening on the
// stored fault alone, without the maybe-typed check.
func TestSafeDropLeavesWorkingExpiredRowExhausted(t *testing.T) {
	rig := newT6384Rig(t)
	d := rig.send("lane-a", "working-6935")
	t6820Report(t, rig, d, t6820Working)
	t6820AwaitDropExpiry(t, rig, d)
	rig.pass(0)
	if o := rig.obligation(d.EventID); o.State != obligationExhausted || o.Spent >= relayReplayMaxAttempts {
		t.Fatalf("before the safe drop: %+v, want exhausted with budget left", o)
	}
	t6820Report(t, rig, d, t6820Drop{drop: relayDroppedPaneReadFailed, harness: "claude"})
	t6820AwaitDropExpiry(t, rig, d)
	if o := rig.obligation(d.EventID); o.State != obligationExhausted {
		t.Fatalf("working-expired row reopened by a safe drop: %+v", o)
	}
	rig.pass(time.Minute)
	if got := t3426Injected(rig.agent); len(got) != 0 {
		t.Fatalf("a working-expired row was copied after a safe drop: %+v", got)
	}
}

// A row exhausted on a maybe-typed verdict joins its design lane's incident, so
// it is noticed and not exhausted. A safe drop that lands while it is noticed
// reopens it, detaches it from the incident as a revive does, and its budget
// copy is queued. Killed by: reopening only an exhausted row.
func TestSafeDropReopensNoticedRow(t *testing.T) {
	rig := t6821Live(t)
	d := rig.send(t3431Worker)
	rig.report(d, t6820Drop{report: relayUnconfirmedPromptFailed})
	// The one labelled copy; its own report leaves the maybe-typed row spent.
	labelled := rig.copyOf(d, 0)
	rig.report(labelled, t6820Drop{report: relayUnconfirmedPromptFailed})
	rig.pass(time.Minute)
	if r := rig.row(d.EventID); r.State != obligationNoticed || r.Incident == 0 {
		t.Fatalf("before the safe drop: %+v, want noticed in an incident", r)
	}
	rig.report(labelled, t6820Drop{drop: relayDroppedRetriesExhausted, harness: "claude"})
	if r := rig.row(d.EventID); r.State != obligationOwed || r.Incident != 0 || r.Fault != relayDroppedRetriesExhausted {
		t.Fatalf("after the safe drop: %+v, want owed and detached", r)
	}
	if c := rig.copyOf(d, time.Minute); c.EventID != d.EventID {
		t.Fatalf("copy = %+v, want row %d", c, d.EventID)
	}
}

// A row whose incident notice is still due when a safe drop reopens it is owed
// with that notice withdrawn: due, it would be joined back into an incident
// while it is sent again. A notice already posted stays posted. Killed by:
// leaving notice_due_at out of the reopen write.
func TestSafeDropWithdrawsDueNotice(t *testing.T) {
	rig := newT6384Rig(t)
	d := rig.send("lane-a", "due-6935")
	t6820Report(t, rig, d, t6820Drop{report: relayUnconfirmedPromptFailed})
	rig.pass(0)
	labelled := rig.injected()
	t6820Report(t, rig, labelled, t6820Drop{report: relayUnconfirmedPromptFailed})
	rig.pass(time.Minute)
	read := func() (state string, incident, due, told int64) {
		t.Helper()
		if err := rig.hub.inbox.store.db.QueryRowContext(t.Context(),
			`SELECT state,incident_id,notice_due_at,told_at FROM delivery_obligations WHERE relay_id=?`, d.EventID).
			Scan(&state, &incident, &due, &told); err != nil {
			t.Fatal(err)
		}
		return state, incident, due, told
	}
	before, _, dueBefore, toldBefore := read()
	if before != obligationExhausted || dueBefore == 0 {
		t.Fatalf("before the safe drop: state=%s due=%d, want exhausted with a due notice", before, dueBefore)
	}
	t6820Report(t, rig, labelled, t6820Drop{drop: relayDroppedRetriesExhausted, harness: "claude"})
	t6820AwaitDropExpiry(t, rig, labelled)
	state, incident, due, told := read()
	if state != obligationOwed || incident != 0 || due != 0 || told != toldBefore {
		t.Fatalf("after the safe drop: state=%s incident=%d due=%d told=%d, want owed, detached, not due, told still %d", state, incident, due, told, toldBefore)
	}
	rig.pass(0)
	if got := rig.injected(); got.EventID != d.EventID {
		t.Fatalf("copy = %+v, want row %d", got, d.EventID)
	}
}

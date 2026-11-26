package panewire

// hk 6820: a store an earlier build wrote holds working-expired rows that
// build had already re-delivered, counted in labelled alone. These tests start
// this build on such a store and drive the node's frames.

import (
	"strings"
	"testing"
	"time"
)

// A row the earlier build re-delivered at an idle wake gets no second copy,
// while a row it never copied is still owed its one. The marking is not
// repeated: a row this build labels, and that then expires working, is revived
// after another restart. Killed by: no marking; marking unlabelled rows;
// marking at every start.
func TestEarlierBuildIdleCopyIsNotRevived(t *testing.T) {
	rig := newT6384Rig(t)
	expire := func(d hubRelayInjectEvent) {
		t.Helper()
		t650Send(t, rig.hub, "host-a", rig.agent, "relay.unconfirmed", relayAckPayload{JobID: d.JobID, Pane: d.Pane, OriginalEventID: d.EventID, Reason: relayDroppedWorkingExpired})
		t650Send(t, rig.hub, "host-a", rig.agent, "relay.dropped", relayDroppedPayload{JobID: d.JobID, Pane: d.Pane, Lane: d.Lane, OriginalEventID: d.EventID, Reason: relayDroppedWorkingExpired, Harness: "cmd"})
	}
	restart := func() {
		t.Helper()
		if err := rig.hub.Close(); err != nil {
			t.Fatal(err)
		}
		rig.start()
	}
	idle := func(sequence uint64) []hubRelayInjectEvent {
		rig.hub.redeliverWorkingExpiredOnIdle("host-a", t6820Wake("w1:p1", sequence))
		rig.pass(0)
		return t3426Injected(rig.agent)
	}
	counts := func(id int64) (labelled, revived int) {
		t.Helper()
		c := t6820CountsOf(t, rig, id)
		return c.labelled, c.revived
	}

	copied := rig.send("lane-a", "earlier-copied")
	expire(copied)
	waiting := rig.send("lane-a", "earlier-waiting")
	expire(waiting)
	// The older row as the earlier build leaves it after its idle copy was
	// reported maybe typed: spent and labelled, the fault unchanged, revived
	// never set. PRAGMA user_version=2 is the version that build records.
	rig.exec(`UPDATE delivery_obligations SET state='exhausted',spent=spent+1,labelled=1,revived=0,revive_requested=0 WHERE relay_id=?`, copied.EventID)
	rig.exec(`PRAGMA user_version=2`)
	restart()

	got := idle(1)
	if len(got) != 1 || got[0].EventID != waiting.EventID {
		t.Fatalf("the first wake queued %+v, want only the row never copied (%d, not %d)", got, waiting.EventID, copied.EventID)
	}
	expire(got[0])
	rig.pass(obligationParkRecheck)
	for sequence := uint64(2); sequence < 5; sequence++ {
		if again := idle(sequence); len(again) != 0 {
			t.Fatalf("wake %d queued a further copy: %+v", sequence, again)
		}
		rig.pass(obligationReviveBackoff)
	}
	if labelled, revived := counts(copied.EventID); labelled != 1 || revived != 1 {
		t.Fatalf("the earlier copy: labelled=%d revived=%d", labelled, revived)
	}

	// This build: a budget copy, which then expires working.
	later := rig.send("lane-a", "labelled-by-this-build")
	t650Send(t, rig.hub, "host-a", rig.agent, "relay.unconfirmed", relayAckPayload{JobID: later.JobID, Pane: later.Pane, OriginalEventID: later.EventID, Reason: "maybe_in_pane presend:unproven:read_failed"})
	t650Send(t, rig.hub, "host-a", rig.agent, "relay.dropped", relayDroppedPayload{JobID: later.JobID, Pane: later.Pane, Lane: later.Lane, OriginalEventID: later.EventID, Reason: relayDroppedPaneReadFailed, Harness: "claude"})
	rig.pass(0)
	budget := rig.injected()
	expire(budget)
	restart()
	rig.clock.advance(time.Minute)
	revived := idle(10)
	if len(revived) != 1 || revived[0].EventID != later.EventID || !strings.HasPrefix(revived[0].Text, relayIdleRedeliveryLabel(rig.row(later.EventID))) {
		t.Fatalf("a row this build labelled lost its revive: %+v", revived)
	}
	if labelled, n := counts(later.EventID); labelled != 2 || n != 1 {
		t.Fatalf("this build's row: labelled=%d revived=%d", labelled, n)
	}
}

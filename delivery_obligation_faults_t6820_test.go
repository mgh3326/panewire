package panewire

// hk 6820 (hk 4289 T3): each fault a node reports earns its own copies, and an
// idle wake asks for the one revived copy on the obligation instead of typing
// from hub memory. These tests drive the node's real frames, in the order the
// node sends them, and read what the hub queued and stored.

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// t6820Drop is one way a node gives up on a copy: its relay.unconfirmed report
// and the relay.dropped that follows.
type t6820Drop struct{ report, drop string }

var (
	t6820ReadFailed   = t6820Drop{"maybe_in_pane presend:unproven:read_failed", relayDroppedPaneReadFailed}
	t6820PromptFailed = t6820Drop{relayUnconfirmedPromptFailed, relayDroppedRetriesExhausted}
	t6820Maybe        = t6820Drop{"maybe_in_pane unproven:composer_empty", relayDroppedCheckPane}
	t6820Working      = t6820Drop{relayDroppedWorkingExpired, relayDroppedWorkingExpired}
)

func t6820Report(t *testing.T, rig *t6384Rig, d hubRelayInjectEvent, how t6820Drop) {
	t.Helper()
	if how.report != "" {
		t650Send(t, rig.hub, "host-a", rig.agent, "relay.unconfirmed", relayAckPayload{JobID: d.JobID, Pane: d.Pane, OriginalEventID: d.EventID, Reason: how.report})
	}
	if how.drop != "" {
		t650Send(t, rig.hub, "host-a", rig.agent, "relay.dropped", relayDroppedPayload{JobID: d.JobID, Pane: d.Pane, Lane: d.Lane, OriginalEventID: d.EventID, Reason: how.drop})
	}
}

func t6820Wake(pane string, sequence uint64) idleWakeRouteRequest {
	return idleWakeRouteRequest{Pane: pane, EventID: "idle:w1:session:" + strconv.FormatUint(sequence, 10), StateChangeSeq: sequence}
}

// t6820Idle reports pane idle and runs one pass.
func t6820Idle(rig *t6384Rig, pane string, sequence uint64) {
	rig.hub.redeliverWorkingExpiredOnIdle("host-a", t6820Wake(pane, sequence))
	rig.pass(0)
}

// t6820NoMoreCopies runs passes past every backoff, each after a new idle
// wake, and fails on any queued copy.
func t6820NoMoreCopies(t *testing.T, rig *t6384Rig, pane string, sequence uint64) {
	t.Helper()
	for i, wait := range []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute, obligationParkRecheck} {
		t6820Idle(rig, pane, sequence+uint64(i))
		rig.pass(wait)
		if got := t3426Injected(rig.agent); len(got) != 0 {
			t.Fatalf("a further copy was queued after %v: %+v", wait, got)
		}
	}
}

type t6820Counts struct {
	labelled, revived int
	requested         int64
}

func t6820CountsOf(t *testing.T, rig *t6384Rig, id int64) t6820Counts {
	t.Helper()
	var c t6820Counts
	if err := rig.hub.inbox.store.db.QueryRowContext(t.Context(), `SELECT labelled,revived,revive_requested FROM delivery_obligations WHERE relay_id=?`, id).Scan(&c.labelled, &c.revived, &c.requested); err != nil {
		t.Fatal(err)
	}
	return c
}

// t6820RequestWithoutPass records a wake's revive request while no pass can
// run, the state a hub that stops right after the wake leaves behind.
func t6820RequestWithoutPass(rig *t6384Rig, pane string, sequence uint64) {
	rig.hub.obligations.mu.Lock()
	defer rig.hub.obligations.mu.Unlock()
	rig.hub.redeliverWorkingExpiredOnIdle("host-a", t6820Wake(pane, sequence))
}

// An exhausted working_wait_expired row, its budget spent and its receipt past
// the replay window, gets exactly one labelled copy on the pass after the wake,
// across a restart between the two, and never another. Killed by: testing the
// budget before the revive; gating the revive on spent; keeping the request in
// memory; retiring the row by its receipt age.
func TestRevivedRowGetsExactlyOneCopy(t *testing.T) {
	rig := newT6384Rig(t)
	d := rig.send("lane-a", "revive-once")
	for _, wait := range []time.Duration{0, time.Minute} {
		t6820Report(t, rig, d, t6820ReadFailed)
		rig.pass(wait)
		d = rig.injected()
	}
	t6820Report(t, rig, d, t6820Working)
	rig.pass(5 * time.Minute)
	if o := rig.obligation(d.EventID); o.State != obligationExhausted || o.Spent != relayReplayMaxAttempts || o.Fault != relayDroppedWorkingExpired || len(rig.agent.relays) != 0 {
		t.Fatalf("before the wake: obligation=%+v queued=%d", o, len(rig.agent.relays))
	}
	rig.fake.mu.Lock()
	for _, row := range rig.fake.rows {
		if row.ID == d.EventID {
			row.ReceivedAt = rig.clock.Now().Add(-relayReplayMaxAge - time.Hour).Format(time.RFC3339Nano)
		}
	}
	rig.fake.mu.Unlock()

	t6820RequestWithoutPass(rig, d.Pane, 1)
	if c := t6820CountsOf(t, rig, d.EventID); c.requested == 0 || c.revived != 0 || len(rig.agent.relays) != 0 {
		t.Fatalf("after the wake: %+v queued=%d", c, len(rig.agent.relays))
	}
	if err := rig.hub.Close(); err != nil {
		t.Fatal(err)
	}
	rig.start()
	rig.hub.replayUndeliveredRelayEvents(t.Context())
	if row := rig.row(d.EventID); row.DeliveredAt != "" || len(rig.agent.relays) != 0 {
		t.Fatalf("the startup walk spent or retired the row: %+v queued=%d", row, len(rig.agent.relays))
	}
	before := t6820CountsOf(t, rig, d.EventID).labelled
	rig.pass(0)
	copy := rig.injected()
	row := rig.row(d.EventID)
	if copy.EventID != d.EventID || !strings.HasPrefix(copy.Text, relayIdleRedeliveryLabel(row)) || row.Attempts < relayReplayMaxAttempts || row.DeliveredAt != "" {
		t.Fatalf("revived copy=%+v row=%+v", copy, row)
	}
	if c, o := t6820CountsOf(t, rig, d.EventID), rig.obligation(d.EventID); c.revived != 1 || c.labelled != before+1 || o.Spent != relayReplayMaxAttempts || o.State != obligationOwed {
		t.Fatalf("after the revived copy: %+v %+v", c, o)
	}

	// The revived copy expires working too: the row has had its one.
	t6820Report(t, rig, copy, t6820Working)
	t6820NoMoreCopies(t, rig, d.Pane, 2)
	if c, o := t6820CountsOf(t, rig, d.EventID), rig.obligation(d.EventID); c.revived != 1 || o.State != obligationExhausted {
		t.Fatalf("a spent revive: %+v %+v", c, o)
	}
}

// A maybe-typed row never revives: a wake does not ask for it, and a request a
// maybe-typed report overtakes is refused. Killed by: allowing the revive;
// letting a later safe fault replace a maybe-typed one.
func TestRevivedCopyRefusedForMaybeTyped(t *testing.T) {
	t.Run("not asked", func(t *testing.T) {
		rig := newT6384Rig(t)
		d := rig.send("lane-a", "maybe-never-asked")
		t6820Report(t, rig, d, t6820Maybe)
		rig.pass(0)
		copy := rig.injected()
		// A working expiry afterwards does not make the text untyped.
		t6820Report(t, rig, copy, t6820Working)
		t6820NoMoreCopies(t, rig, d.Pane, 1)
		if c, o := t6820CountsOf(t, rig, d.EventID), rig.obligation(d.EventID); c.requested != 0 || c.revived != 0 || c.labelled != 1 || o.Fault != relayDroppedCheckPane && o.Fault != relayDroppedMaybeInPane {
			t.Fatalf("maybe-typed row: %+v %+v", c, o)
		}
	})
	t.Run("overtaken", func(t *testing.T) {
		rig := newT6384Rig(t)
		d := rig.send("lane-a", "maybe-overtakes")
		t6820Report(t, rig, d, t6820ReadFailed)
		rig.pass(0)
		copy := rig.injected()
		t6820Report(t, rig, copy, t6820Working)
		t6820RequestWithoutPass(rig, d.Pane, 1)
		if c := t6820CountsOf(t, rig, d.EventID); c.requested == 0 {
			t.Fatalf("fixture made no request: %+v", c)
		}
		// The node's last word on the delivery says the pane may hold it.
		t6820Report(t, rig, copy, t6820Drop{drop: relayDroppedCheckPane})
		t6820NoMoreCopies(t, rig, d.Pane, 2)
		if c, o := t6820CountsOf(t, rig, d.EventID), rig.obligation(d.EventID); c.revived != 0 || c.labelled != 1 || o.State != obligationExhausted {
			t.Fatalf("overtaken revive: %+v %+v", c, o)
		}
	})
}

// A maybe-typed row gets one labelled copy in its life, whatever is reported
// after it. Killed by: one copy per report; classifying the fault as safe.
func TestMaybeInPaneGetsOneLabelledCopyEver(t *testing.T) {
	for name, first := range map[string]t6820Drop{
		"check pane":       t6820Maybe,
		"previous attempt": {"maybe_in_pane previous_attempt_unproven prompt_failed", relayDroppedCheckPane},
		"unproven no drop": {report: "maybe_in_pane after_return:unproven:no_adapter"},
		"unknown reason":   {report: "return_failed"},
		"drop only":        {drop: relayDroppedCheckPane},
		"unproven read":    {"maybe_in_pane presend:unproven:read_failed", relayDroppedCheckPane},
	} {
		t.Run(name, func(t *testing.T) {
			rig := newT6384Rig(t)
			d := rig.send("lane-a", "maybe-once")
			t6820Report(t, rig, d, first)
			t6820Report(t, rig, d, first)
			if first.report == "" {
				// The drop's own goroutine stores the fault.
				eventually(t, 2*time.Second, "the drop's fault", func() bool { return rig.obligation(d.EventID).Fault != "" })
			}
			rig.pass(0)
			copy := rig.injected()
			if !strings.HasPrefix(copy.Text, relayUnconfirmedLabel(rig.row(d.EventID))) {
				t.Fatalf("copy without the duplicate warning: %q", copy.Text)
			}
			t6820Report(t, rig, copy, t6820PromptFailed)
			t6820Report(t, rig, copy, t6820Drop{drop: relayDroppedPaneReadFailed})
			t6820NoMoreCopies(t, rig, d.Pane, 1)
			if c, o := t6820CountsOf(t, rig, d.EventID), rig.obligation(d.EventID); c.labelled != 1 || c.revived != 0 || o.State != obligationExhausted {
				t.Fatalf("after one copy: %+v %+v", c, o)
			}
		})
	}
	// A copy that went out for a timeout is the one labelled copy.
	t.Run("after a timeout copy", func(t *testing.T) {
		rig := newT6384Rig(t)
		d := rig.send("lane-a", "maybe-after-timeout")
		rig.hub.expireRelayAck(relayPendingKey(d.EventID, d.JobID))
		rig.pass(0)
		copy := rig.injected()
		t6820Report(t, rig, copy, t6820Maybe)
		t6820NoMoreCopies(t, rig, d.Pane, 1)
		if c := t6820CountsOf(t, rig, d.EventID); c.labelled != 1 {
			t.Fatalf("labelled=%d", c.labelled)
		}
	})
}

// Faults that prove nothing was typed retry until the budget is spent, with the
// node's own frames. Killed by: treating pane_read_failed or
// inject_failed_max_attempts as maybe typed; losing the drop that settles an
// unproven pane read.
func TestPaneReadFailedRetriesToBudget(t *testing.T) {
	for name, how := range map[string]t6820Drop{
		"pane read failed":     t6820ReadFailed,
		"inject budget spent":  t6820PromptFailed,
		"drop without reports": {drop: relayDroppedRetriesExhausted},
	} {
		t.Run(name, func(t *testing.T) {
			rig := newT6384Rig(t)
			d := rig.send("lane-a", "safe-budget")
			for i, wait := range []time.Duration{0, time.Minute} {
				t6820Report(t, rig, d, how)
				if how.report == "" {
					eventually(t, 2*time.Second, "the drop's fault", func() bool {
						rig.hub.mu.Lock()
						defer rig.hub.mu.Unlock()
						_, open := rig.hub.relayControl.relayPending[relayPendingKey(d.EventID, d.JobID)]
						return !open
					})
				}
				rig.pass(wait)
				d = rig.injected()
				if o := rig.obligation(d.EventID); o.Spent != i+2 {
					t.Fatalf("copy %d: %+v", i+1, o)
				}
			}
			t6820Report(t, rig, d, how)
			t6820NoMoreCopies(t, rig, d.Pane, 1)
			want := how.drop
			if o := rig.obligation(d.EventID); o.State != obligationExhausted || o.Spent != relayReplayMaxAttempts || o.Fault != want || rig.row(d.EventID).DeliveredAt != "" {
				t.Fatalf("after the budget: %+v, want fault %s", o, want)
			}
		})
	}
}

// A row with no route stays owed: no copy, no spend, no design-lane notice, and
// one feed announcement per process; the route's return sends it. Killed by:
// spending on an unrouted pass; escalating it; announcing every pass.
func TestUnroutedStaysOwedNoNotice(t *testing.T) {
	rig := newT6384Rig(t)
	// worker-a has a design lane, so a notice would have somewhere to go.
	d := rig.send("worker-a", "no-route")
	rig.hub.expireRelayAck(relayPendingKey(d.EventID, d.JobID))
	withRoute, err := os.ReadFile(rig.hub.reportRelayPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rig.hub.reportRelayPath, []byte(`{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	events := r20t5Subscribe(t, rig.hub)
	rows := rig.fake.rowCount()
	for _, wait := range []time.Duration{0, time.Minute, time.Minute, time.Minute} {
		rig.pass(wait)
	}
	if o := rig.obligation(d.EventID); o.State != obligationOwed || o.Spent != 1 || o.Fault != obligationFaultUnrouted || len(rig.agent.relays) != 0 {
		t.Fatalf("unrouted row: %+v queued=%d", o, len(rig.agent.relays))
	}
	if got := rig.fake.rowCount(); got != rows {
		t.Fatalf("an unrouted row wrote %d new rows", got-rows)
	}
	if got := len(events("relay.unrouted")); got != 1 {
		t.Fatalf("relay.unrouted broadcasts=%d, want 1", got)
	}
	if err := os.WriteFile(rig.hub.reportRelayPath, withRoute, 0o600); err != nil {
		t.Fatal(err)
	}
	rig.pass(time.Minute)
	if copy := rig.injected(); copy.EventID != d.EventID || rig.obligation(d.EventID).Spent != 2 {
		t.Fatalf("the returned route did not send: %+v", copy)
	}

	// A maybe-typed fault keeps its name through a missing route, and so its
	// one copy.
	m := rig.send("worker-a", "no-route-maybe")
	t6820Report(t, rig, m, t6820Drop{report: t6820Maybe.report})
	if err := os.WriteFile(rig.hub.reportRelayPath, []byte(`{"lanes":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	rig.pass(0)
	if o := rig.obligation(m.EventID); o.Fault != relayDroppedMaybeInPane || o.Spent != 1 {
		t.Fatalf("unrouted maybe-typed row: %+v", o)
	}
}

// A row whose pane is gone stays owed: neither the walk nor a pass retires it,
// and nothing is spent while no node can take it. Killed by: retiring a
// lane.event on a missing pane.
func TestVanishedPaneStaysOwed(t *testing.T) {
	rig := newT6384Rig(t)
	d := rig.send("lane-a", "pane-vanished")
	rig.hub.expireRelayAck(relayPendingKey(d.EventID, d.JobID))
	// The lane now names a pane on a node that is not connected.
	if err := os.WriteFile(rig.hub.reportRelayPath, []byte(`{"lanes":{"lane-a":{"machine":"host-z","pane":"w9:p9"}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, wait := range []time.Duration{0, time.Hour, time.Hour} {
		rig.pass(wait)
		rig.hub.replayUndeliveredRelayEvents(t.Context())
	}
	if row, o := rig.row(d.EventID), rig.obligation(d.EventID); row.DeliveredAt != "" || o.State != obligationOwed || o.Spent != 1 || len(rig.agent.relays) != 0 {
		t.Fatalf("vanished pane: row=%+v obligation=%+v", row, o)
	}

	// A node that drops a held row because its pane is gone says nothing about
	// the text: no spend, and the row stays open.
	rig2 := newT6384Rig(t)
	g := rig2.send("lane-a", "pane-occupant-gone")
	t6820Report(t, rig2, g, t6820Drop{drop: "pane_occupant_gone"})
	rig2.pass(obligationFirstCheck)
	if row, o := rig2.row(g.EventID), rig2.obligation(g.EventID); row.DeliveredAt != "" || o.State != obligationOwed || o.Spent != 1 || o.Fault != "" || len(rig2.agent.relays) != 0 {
		t.Fatalf("stale lease drop: row=%+v obligation=%+v", row, o)
	}
}

// lane_gone retires only an idle-wake report whose pane no lane has; a
// lane.event whose lane lost its route stays open. Killed by: applying
// lane_gone to any row.
func TestLaneGoneOnlyForIdleWake(t *testing.T) {
	rig := newT6384Rig(t)
	wake := t6384Row(700, "lane-a", "idle-wake:gone", 1)
	wake.Text = idleWakeRouteText(idleWakeRouteRequest{EventID: "idle:w1:session:1", Pane: "w7:gone", State: "idle",
		ChangedAt: rig.clock.Now().Format(time.RFC3339Nano), StateChangeSeq: 1}, idleWakeOwnerResolution{Lane: "lane-a"})
	plain := t6384Row(701, "lane-removed", "plain", 1)
	for _, row := range []handoffkeepRelayEvent{wake, plain} {
		row.ReceivedAt = rig.clock.Now().Format(time.RFC3339Nano)
		rig.fake.seedUndelivered(row)
		o := newDeliveryObligation{key: relayEventDedupeKey(row.Kind, relayEventFromRecord(row)), relayID: row.ID, kind: row.Kind, lane: row.OwnerLane, attempts: 1}
		if err := rig.hub.inbox.store.flagObligationRetry(t.Context(), o, obligationAckTimeout, "", rig.clock.Now()); err != nil {
			t.Fatal(err)
		}
	}
	rig.pass(0)
	rig.hub.replayUndeliveredRelayEvents(context.Background())
	rig.pass(time.Minute)
	if got := rig.row(700).DeliveredTo; got != relayReplayRetiredMarker+"lane_gone" {
		t.Fatalf("idle wake delivered_to=%q", got)
	}
	if row, o := rig.row(701), rig.obligation(701); row.DeliveredAt != "" || o.State != obligationOwed || o.Spent != 1 {
		t.Fatalf("a lane.event without a route: row=%+v obligation=%+v", row, o)
	}
}

// One idle wake asks for one row, oldest first. The same wake again, an older
// one, and a newer one while a request is untaken ask for no second row.
// Killed by: asking for every row of the pane; ignoring the wake order; asking
// past an untaken request.
func TestIdleWakeRevivesOneRowPerWake(t *testing.T) {
	rig := newT6384Rig(t)
	var rows []hubRelayInjectEvent
	for _, id := range []string{"first", "second", "third"} {
		d := rig.send("lane-a", id)
		t6820Report(t, rig, d, t6820Working)
		rows = append(rows, d)
	}
	pane := rows[0].Pane
	rig.pass(0)
	t6820RequestWithoutPass(rig, pane, 2)
	t6820RequestWithoutPass(rig, pane, 3)
	if second := t6820CountsOf(t, rig, rows[1].EventID); second.requested != 0 {
		t.Fatalf("a wake asked past an untaken request: %+v", second)
	}
	rig.pass(0)
	if got := rig.injected(); got.EventID != rows[0].EventID {
		t.Fatalf("first wake queued row %d, want %d", got.EventID, rows[0].EventID)
	}
	for _, sequence := range []uint64{2, 1} {
		t6820Idle(rig, pane, sequence)
		if got := t3426Injected(rig.agent); len(got) != 0 {
			t.Fatalf("wake %d queued %+v", sequence, got)
		}
	}
	// Wake 3 found a request untaken and was not used up.
	t6820Idle(rig, pane, 3)
	if got := rig.injected(); got.EventID != rows[1].EventID {
		t.Fatalf("next wake queued row %d, want %d", got.EventID, rows[1].EventID)
	}
	// A request that cannot be queued holds the next row back only for a while.
	rig.hub.mu.Lock()
	rig.hub.relayHeld[rows[2].EventID] = hubRelayHeldProjection{ID: rows[2].EventID, Machine: "host-a", Pane: pane}
	rig.hub.mu.Unlock()
	t6820Idle(rig, pane, 4)
	fourth := rig.send("lane-a", "fourth")
	t6820Report(t, rig, fourth, t6820Working)
	t6820Idle(rig, pane, 5)
	if c := t6820CountsOf(t, rig, fourth.EventID); c.requested != 0 {
		t.Fatalf("a wake asked past a recent request: %+v", c)
	}
	rig.clock.advance(obligationReviveBackoff)
	t6820Idle(rig, pane, 6)
	if got := rig.injected(); got.EventID != fourth.EventID {
		t.Fatalf("queued row %d past the blocked request, want %d", got.EventID, fourth.EventID)
	}
}

// A drop past its replay window is not revived. Killed by: no age bound.
func TestReviveBoundedByDropAge(t *testing.T) {
	for _, taken := range []bool{false, true} {
		t.Run(strconv.FormatBool(taken), func(t *testing.T) {
			rig := newT6384Rig(t)
			d := rig.send("lane-a", "old-drop")
			t6820Report(t, rig, d, t6820Working)
			rig.pass(0)
			if taken {
				t6820RequestWithoutPass(rig, d.Pane, 1)
			}
			rig.pass(relayReplayMaxAge + time.Minute)
			if !taken {
				t6820Idle(rig, d.Pane, 1)
				if c := t6820CountsOf(t, rig, d.EventID); c.requested != 0 {
					t.Fatalf("an aged drop was asked for: %+v", c)
				}
			}
			// With the request made in time, the pass at the end of the window
			// still ran it: only a request left untaken that long is refused.
			want := 0
			if taken {
				want = 1
			}
			if got := len(t3426Injected(rig.agent)); got > want {
				t.Fatalf("queued %d copies", got)
			}
		})
	}
	rig := newT6384Rig(t)
	d := rig.send("lane-a", "request-outlives-window")
	t6820Report(t, rig, d, t6820Working)
	t6820RequestWithoutPass(rig, d.Pane, 1)
	rig.clock.advance(relayReplayMaxAge + time.Minute)
	rig.pass(0)
	if c := t6820CountsOf(t, rig, d.EventID); c.revived != 0 || len(rig.agent.relays) != 0 {
		t.Fatalf("a request past the window was taken: %+v", c)
	}
}

// A revived copy that was never queued gives the revive back, and the next pass
// takes it. Killed by: spending the revive on a refused queue or a failed
// attempt write.
func TestUnqueuedRevivedCopyIsTakenAgain(t *testing.T) {
	for _, refusal := range []string{"full queue", "attempt write"} {
		t.Run(refusal, func(t *testing.T) {
			rig := newT6384Rig(t)
			d := rig.send("lane-a", "revive-refused")
			// A timeout leaves handoffkeep below the ceiling.
			rig.hub.expireRelayAck(relayPendingKey(d.EventID, d.JobID))
			rig.exec(`UPDATE delivery_obligations SET fault=? WHERE relay_id=?`, relayDroppedWorkingExpired, d.EventID)
			if refusal == "full queue" {
				for range cap(rig.agent.relays) {
					rig.agent.relays <- hubRelayInjectEvent{}
				}
			} else {
				rig.fake.mu.Lock()
				rig.fake.failPostOwnerLane = "lane-a"
				rig.fake.mu.Unlock()
			}
			t6820Idle(rig, d.Pane, 1)
			if c := t6820CountsOf(t, rig, d.EventID); c.revived != 0 || c.labelled != 0 || c.requested == 0 {
				t.Fatalf("unqueued revive: %+v", c)
			}
			if refusal == "full queue" {
				for range cap(rig.agent.relays) {
					<-rig.agent.relays
				}
			}
			rig.fake.mu.Lock()
			rig.fake.failPostOwnerLane = ""
			rig.fake.mu.Unlock()
			rig.pass(5 * time.Minute)
			rig.injected()
			if c := t6820CountsOf(t, rig, d.EventID); c.revived != 1 || c.labelled != 1 {
				t.Fatalf("retaken revive: %+v", c)
			}
		})
	}
}

// A node that held a retry until the working ceiling reports it only by the
// drop, the report window being gone. That drop still earns the revive. Killed
// by: ignoring a drop with no window.
func TestWorkingExpiryAfterHeldRetryIsRevived(t *testing.T) {
	rig := newT6384Rig(t)
	d := rig.send("lane-a", "held-then-expired")
	t6820Report(t, rig, d, t6820Drop{report: relayUnconfirmedPromptFailed})
	t650Send(t, rig.hub, "host-a", rig.agent, "relay.held", relayHeldPayload{EventID: d.EventID, JobID: d.JobID, Pane: d.Pane, Lane: "lane-a",
		Reason: "retry", HeldSince: rig.clock.Now().Format(time.RFC3339Nano), DeliverPolicy: relayDeliverDirective})
	rig.pass(0)
	t6820Report(t, rig, d, t6820Working)
	rig.pass(time.Minute)
	if o := rig.obligation(d.EventID); o.Fault != relayDroppedWorkingExpired || o.State != obligationExhausted || len(rig.agent.relays) != 0 {
		t.Fatalf("after the held expiry: %+v", o)
	}
	t6820Idle(rig, d.Pane, 1)
	rig.injected()
}

// A job_ended drop ends the row's copies for good, and a walk does not type it
// again either. It stays open in handoffkeep. Killed by: leaving the row to the
// replay; retrying it.
func TestJobEndedRowIsNeverRetried(t *testing.T) {
	rig := newT6384Rig(t)
	d := rig.send("lane-a", "job-ended")
	t6820Report(t, rig, d, t6820Drop{drop: relayDroppedJobEnded})
	eventually(t, 2*time.Second, "the job_ended fault", func() bool { return rig.obligation(d.EventID).Fault == relayDroppedJobEnded })
	rig.hub.replayUndeliveredRelayEvents(t.Context())
	t6820NoMoreCopies(t, rig, d.Pane, 1)
	rig.hub.replayUndeliveredRelayEvents(t.Context())
	if row, o := rig.row(d.EventID), rig.obligation(d.EventID); row.DeliveredAt != "" || o.State != obligationExhausted || len(rig.agent.relays) != 0 {
		t.Fatalf("job_ended row: %+v %+v", row, o)
	}
}

// A lane that became inbox-only takes no revived copy: the body is filed. Killed
// by: queueing the revive past the inbox-only gate.
func TestInboxOnlyLaneNeverRevives(t *testing.T) {
	rig := newT6384Rig(t)
	d := rig.send("lane-a", "became-inbox-only")
	t6820Report(t, rig, d, t6820Working)
	rig.designLane("lane-a")
	t6820Idle(rig, d.Pane, 1)
	rig.pass(time.Minute)
	// The inbox wake line may be queued; the row's own body may not.
	for _, got := range t3426Injected(rig.agent) {
		if strings.Contains(got.Text, "please look at became-inbox-only") {
			t.Fatalf("an inbox-only lane was typed the body: %+v", got)
		}
	}
	if c := t6820CountsOf(t, rig, d.EventID); c.revived != 0 || c.labelled != 0 {
		t.Fatalf("an inbox-only lane took the revive: %+v", c)
	}
	if got := rig.row(d.EventID).DeliveredTo; got != "hub/inbox" {
		t.Fatalf("delivered_to=%q", got)
	}
	rig.want(d.EventID, obligationSettled, "inbox")
}

// Working-expired rows waiting for a wake, one of them unreadable, neither
// starve another row's retry nor hide a late delivery among them within the
// bounded pass.
func TestWaitingWorkingRowsDoNotStarveThePass(t *testing.T) {
	rig := newT6384Rig(t)
	store := rig.hub.inbox.store
	now := rig.clock.Now()
	last := int64(100 + obligationClassifyLimit)
	for id := int64(100); id <= last; id++ {
		record := t6384Row(id, "lane-a", "waiting-"+strconv.FormatInt(id, 10), 1)
		rig.fake.seedUndelivered(record)
		o := newDeliveryObligation{key: relayEventDedupeKey(record.Kind, relayEventFromRecord(record)), relayID: id, kind: record.Kind, lane: record.OwnerLane, attempts: 1}
		if err := store.flagObligationRetry(t.Context(), o, relayDroppedWorkingExpired, "", now); err != nil {
			t.Fatal(err)
		}
	}
	ordinary := rig.send("lane-a", "healthy-retry")
	t6820Report(t, rig, ordinary, t6820Drop{report: t6820Maybe.report})
	rig.pass(0)
	rig.pass(0)
	if got := rig.injected(); got.EventID != ordinary.EventID {
		t.Fatalf("queued %+v", got)
	}
	if err := rig.client.markDelivered(t.Context(), last, "host-a", "w1:p1"); err != nil {
		t.Fatal(err)
	}
	rig.pass(obligationParkRecheck)
	rig.pass(0)
	rig.want(last, obligationConfirmed, "")
	if got := t3426Injected(rig.agent); len(got) != 0 {
		t.Fatalf("a waiting row was queued without a wake: %+v", got)
	}
}

// Each reason a node or the hub uses names a fault of the intended class.
// Killed by: moving a reason to another class.
func TestObligationFaultClasses(t *testing.T) {
	for reason, want := range map[string]struct {
		fault string
		class obligationClass
	}{
		obligationAckTimeout:                                       {obligationAckTimeout, obligationSafe},
		relayDroppedPaneReadFailed:                                 {relayDroppedPaneReadFailed, obligationSafe},
		relayDroppedRetriesExhausted:                               {relayDroppedRetriesExhausted, obligationSafe},
		"prompt_failed":                                            {relayDroppedRetriesExhausted, obligationSafe},
		relayDroppedWorkingExpired:                                 {relayDroppedWorkingExpired, obligationWorkingExpired},
		relayDroppedJobEnded:                                       {relayDroppedJobEnded, obligationNeverRetry},
		relayDroppedCheckPane:                                      {relayDroppedCheckPane, obligationMaybeTyped},
		"maybe_in_pane presend:unproven:read_failed":               {obligationFaultReadUnproven, obligationMaybeTyped},
		"maybe_in_pane stale_working:presend:unproven:read_failed": {relayDroppedMaybeInPane, obligationMaybeTyped},
		"maybe_in_pane previous_attempt_unproven prompt_failed":    {relayDroppedMaybeInPane, obligationMaybeTyped},
		"maybe_in_pane nonce_missing recent:nonce_echo":            {relayDroppedMaybeInPane, obligationMaybeTyped},
		"maybe_in_pane unproven:no_adapter":                        {relayDroppedMaybeInPane, obligationMaybeTyped},
		"unproven:no_adapter":                                      {obligationFaultUnproven, obligationMaybeTyped},
		relayUnparsedReason:                                        {obligationFaultUnproven, obligationMaybeTyped},
		"":                                                         {obligationFaultUnproven, obligationMaybeTyped},
	} {
		fault := obligationFaultFor(reason)
		if class := classifyObligationFault(fault); fault != want.fault || class != want.class {
			t.Errorf("%q = %s class %d, want %s class %d", reason, fault, class, want.fault, want.class)
		}
	}
	for _, fault := range []string{obligationFaultUnconfirmed, obligationFaultReadUnproven, relayDroppedMaybeInPane} {
		if classifyObligationFault(fault) != obligationMaybeTyped {
			t.Errorf("%s is not maybe typed", fault)
		}
	}
}

// The store's side of the fault table and the revive runs the same on every
// backend: which fault replaces which, one request, one claim, a release.
// Killed by: a dialect that reads the new fault inside the same UPDATE; a claim
// that is not a compare-and-swap.
func TestObligationFaultStoreContract(t *testing.T) {
	for _, backend := range laneInboxBackends() {
		t.Run(backend.name, func(t *testing.T) {
			s := backend.fresh(t)()
			ctx := t.Context()
			now := time.Date(2026, 11, 27, 9, 0, 0, 0, time.UTC)
			key := "lane.event/lane-a/contract"
			o := newDeliveryObligation{key: key, relayID: 7, kind: "lane.event", lane: "lane-a", machine: "host-a", pane: "w1:p1", attempts: 1}
			read := func() openObligation {
				t.Helper()
				got, err := s.retryObligationForKey(ctx, key)
				if err != nil {
					t.Fatal(err)
				}
				return got
			}
			steps := []struct{ fault, want string }{
				{obligationAckTimeout, obligationAckTimeout},
				{relayDroppedPaneReadFailed, relayDroppedPaneReadFailed},
				{obligationAckTimeout, relayDroppedPaneReadFailed},
				{obligationFaultUnconfirmed, relayDroppedPaneReadFailed},
				{relayDroppedWorkingExpired, relayDroppedWorkingExpired},
				{obligationFaultReadUnproven, obligationFaultReadUnproven},
				{relayDroppedWorkingExpired, obligationFaultReadUnproven},
				{relayDroppedPaneReadFailed, relayDroppedPaneReadFailed},
				{relayDroppedMaybeInPane, relayDroppedMaybeInPane},
				{relayDroppedPaneReadFailed, relayDroppedMaybeInPane},
				{relayDroppedCheckPane, relayDroppedMaybeInPane},
				{relayDroppedJobEnded, relayDroppedJobEnded},
				{relayDroppedMaybeInPane, relayDroppedJobEnded},
			}
			for i, step := range steps {
				at := now.Add(time.Duration(i) * time.Second)
				before := read()
				if err := s.flagObligationRetry(ctx, o, step.fault, "", at); err != nil {
					t.Fatal(err)
				}
				got := read()
				if got.fault != step.want {
					t.Fatalf("step %d: %s over %s = %s, want %s", i, step.fault, before.fault, got.fault, step.want)
				}
				if got.fault != before.fault && got.lastFaultAt != at.UnixMilli() {
					t.Fatalf("step %d: fault time %d, want %d", i, got.lastFaultAt, at.UnixMilli())
				}
				if step.fault != step.want && got.lastFaultAt != before.lastFaultAt {
					t.Fatalf("step %d: a refused fault moved its time", i)
				}
			}

			if _, err := s.db.ExecContext(ctx, s.q(`UPDATE delivery_obligations SET fault=?,state=?,next_at=?`), relayDroppedWorkingExpired, obligationExhausted, now.Add(time.Hour).UnixMilli()); err != nil {
				t.Fatal(err)
			}
			// A drop from a machine the obligation does not name changes nothing
			// unless the lane routes there now.
			if _, err := s.noteObligationDrop(ctx, 7, "lane-a", "host-other", false, relayDroppedCheckPane, now); err != nil || read().fault != relayDroppedWorkingExpired {
				t.Fatalf("a foreign drop changed the fault: %+v err=%v", read(), err)
			}
			at := read().lastFaultAt
			later := time.UnixMilli(at).Add(time.Minute)
			if marked, err := s.requestObligationRevive(ctx, []string{"lane-b"}, later); err != nil || marked {
				t.Fatalf("another lane's wake marked the row: %v %v", marked, err)
			}
			for _, want := range []bool{true, false} {
				if marked, err := s.requestObligationRevive(ctx, []string{"lane-b", "lane-a"}, later); err != nil || marked != want {
					t.Fatalf("request marked=%v err=%v want=%v", marked, err, want)
				}
			}
			requested := read()
			if !requested.revivable(later) || requested.nextAt != 0 {
				t.Fatalf("requested row: %+v", requested)
			}
			if due, err := s.noteObligationDrop(ctx, 7, "lane-a", "host-a", false, relayDroppedWorkingExpired, later); err != nil || !due {
				t.Fatalf("the drop did not make the request due: %v %v", due, err)
			}
			requested = read()
			next := later.Add(obligationReviveBackoff)
			for _, want := range []bool{true, false} {
				if got, err := s.spendObligationRevive(ctx, requested, later, next); err != nil || got != want {
					t.Fatalf("claim=%v err=%v want=%v", got, err, want)
				}
			}
			if got := read(); got.state != obligationOwed || got.revived != 1 || got.labelled != 1 || got.spent != requested.spent {
				t.Fatalf("claimed row: %+v", got)
			}
			if err := s.releaseObligationRevive(ctx, requested, next, next.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			if got := read(); got.revived != 0 || got.labelled != 0 || got.nextAt != next.Add(time.Minute).UnixMilli() || !got.revivable(later) {
				t.Fatalf("released row: %+v", got)
			}
		})
	}
}

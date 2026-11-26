package panewire

import (
	"context"
	"strings"
	"testing"
	"time"
)

func reportUnconfirmedRetry(t *testing.T, rig *t6384Rig, directive hubRelayInjectEvent) {
	t.Helper()
	t650Send(t, rig.hub, "host-a", rig.agent, "relay.unconfirmed", relayAckPayload{
		JobID: directive.JobID, Pane: directive.Pane, OriginalEventID: directive.EventID, Reason: "maybe_in_pane",
	})
}

func TestRetrySpendsBeforeQueue(t *testing.T) {
	rig := newT6384Rig(t)
	directive := rig.send("lane-a", "spend-first")
	reportUnconfirmedRetry(t, rig, directive)
	type observation struct {
		spent  int
		queued int
	}
	observed := make(chan observation, 4)
	rig.fake.mu.Lock()
	rig.fake.observe = func(method, path string) {
		if method != "POST" || path != "/v1/relay/events" {
			return
		}
		var spent int
		if err := rig.hub.inbox.store.db.QueryRow("SELECT spent FROM delivery_obligations WHERE relay_id=?", directive.EventID).Scan(&spent); err != nil {
			t.Error(err)
		}
		observed <- observation{spent, len(rig.agent.relays)}
	}
	rig.fake.mu.Unlock()
	rig.pass(0)
	rig.injected()
	close(observed)
	if len(observed) == 0 {
		t.Fatal("no attempt write to observe")
	}
	for got := range observed {
		if got.spent != 2 || got.queued != 0 {
			t.Fatalf("at the attempt write, spent=%d queued=%d, want 2 and 0", got.spent, got.queued)
		}
	}
}

func TestSpentIsAuthoritativeAcrossCrash(t *testing.T) {
	rig := newT6384Rig(t)
	directive := rig.send("lane-a", "crash-after-spend")
	reportUnconfirmedRetry(t, rig, directive)
	ctx := t.Context()
	store := rig.hub.inbox.store
	key := relayEventDedupeKey("lane.event", relayEventFromRecord(rig.row(directive.EventID)))
	o, err := store.retryObligationForKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	now := rig.clock.Now()
	spent, err := store.spendObligationRetry(ctx, o, 1, now, now.Add(time.Minute))
	if err != nil || !spent {
		t.Fatalf("spend=%v, err=%v", spent, err)
	}
	// The previous process stopped after the durable spend and before its
	// handoffkeep write or queue. Neither the walk nor Sweep may repeat it.
	if err := rig.hub.Close(); err != nil {
		t.Fatal(err)
	}
	rig.start()
	rig.hub.replayUndeliveredRelayEvents(ctx)
	rig.pass(2 * time.Minute)
	if got := rig.obligation(directive.EventID); got.Spent != 2 || got.State != obligationExhausted || len(rig.agent.relays) != 0 {
		t.Fatalf("after restart obligation=%+v, queued=%d", got, len(rig.agent.relays))
	}
}

func TestLabelledCopyRaisesHKAttemptsToCeiling(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "accepted"
		if fail {
			name = "rejected"
		}
		t.Run(name, func(t *testing.T) {
			rig := newT6384Rig(t)
			directive := rig.send("lane-a", "ceiling")
			reportUnconfirmedRetry(t, rig, directive)
			before := rig.fake.count("POST", "/v1/relay/events")
			if fail {
				rig.fake.mu.Lock()
				rig.fake.failPostOwnerLane = "lane-a"
				rig.fake.mu.Unlock()
			}
			rig.pass(0)
			if fail {
				if got := rig.fake.count("POST", "/v1/relay/events") - before; got != 4 || len(rig.agent.relays) != 0 {
					t.Fatalf("rejected attempt writes=%d, queued=%d; want 4 and 0", got, len(rig.agent.relays))
				}
				return
			}
			copy := rig.injected()
			if row := rig.row(directive.EventID); copy.EventID != row.ID || row.Attempts < relayReplayMaxAttempts ||
				!strings.HasPrefix(copy.Text, relayUnconfirmedLabel(row)) {
				t.Fatalf("copy=%+v, row=%+v", copy, row)
			}
		})
	}
}

func TestRollbackDoesNotRetypeMaybeTyped(t *testing.T) {
	rig := newT6384Rig(t)
	directive := rig.send("lane-a", "rollback")
	reportUnconfirmedRetry(t, rig, directive)
	rig.pass(0)
	rig.injected()

	// The compatibility replay takes the old binary's path and sees only
	// handoffkeep. It has no access to the obligation or to any live claim.
	old, agent := quietHub(t, "", "off", rig.client, &rig.clock.now)
	old.replayUndeliveredRelayRows(t.Context(), "", relayReplaySourceStartup, "rollback listing")
	if len(agent.relays) != 0 {
		t.Fatalf("compatibility replay queued %d copies", len(agent.relays))
	}
}

func TestNoPreRetireOnRetry(t *testing.T) {
	rig := newT6384Rig(t)
	directive := rig.send("lane-a", "late-proof-after-copy")
	reportUnconfirmedRetry(t, rig, directive)
	rig.pass(0)
	copy := rig.injected()
	if row := rig.row(directive.EventID); row.DeliveredAt != "" || row.DeliveredTo != "" {
		t.Fatalf("retry retired origin: %+v", row)
	}
	rig.delivered(copy)
	rig.pass(2 * time.Minute)
	rig.want(directive.EventID, obligationConfirmed, "")
}

func TestLegacyMarkerConvertedOnce(t *testing.T) {
	rig := newT6384Rig(t)
	directive := rig.send("lane-a", "legacy-marker")
	markID := unconfirmedMarkEventID(directive.EventID)
	marker := t6384Event(relayUnconfirmedMarkLane, markID, "legacy mark")
	stored, _, err := rig.client.appendEvent(t.Context(), rig.hub.relayEventRequest("lane.event", marker, reportRelayRoute{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := rig.hub.Close(); err != nil {
		t.Fatal(err)
	}
	rig.start()
	for range 2 {
		rig.hub.replayUndeliveredRelayEvents(t.Context())
	}
	if len(rig.agent.relays) != 0 {
		t.Fatal("the startup walk retried the origin")
	}
	rig.pass(0)
	rig.injected()
	rig.hub.replayUndeliveredRelayEvents(t.Context())
	rig.pass(2 * time.Minute)
	var linked string
	err = rig.hub.inbox.store.db.QueryRow("SELECT marker_event_id FROM delivery_obligations WHERE relay_id=?", directive.EventID).Scan(&linked)
	if err != nil || linked != markID || rig.row(stored.ID).DeliveredAt != "" || rig.obligation(directive.EventID).Spent != 2 || len(rig.agent.relays) != 0 {
		t.Fatalf("conversion linked=%q err=%v marker=%+v", linked, err, rig.row(stored.ID))
	}
}

func TestAckTimeoutSpendsRetryAndSchedules(t *testing.T) {
	rig := newT6384Rig(t)
	directive := rig.send("lane-a", "timeout")
	rig.hub.expireRelayAck(relayPendingKey(directive.EventID, directive.JobID))
	rig.pass(0)
	rig.injected()
	o := rig.obligation(directive.EventID)
	if o.Spent != 2 || o.Fault != obligationAckTimeout || o.NextAt != rig.clock.Now().Add(time.Minute).UnixMilli() {
		t.Fatalf("after timeout retry: %+v", o)
	}
}

func TestNodeUnconfirmedSpendsRetry(t *testing.T) {
	rig := newT6384Rig(t)
	directive := rig.send("lane-a", "node-report")
	reportUnconfirmedRetry(t, rig, directive)
	rig.hub.replayUndeliveredRelayEvents(context.Background())
	if len(rig.agent.relays) != 0 || rig.fake.rowCount() != 1 {
		t.Fatal("report wrote a marker or the replay walk queued the retry")
	}
	rig.pass(0)
	copy := rig.injected()
	reportUnconfirmedRetry(t, rig, copy)
	rig.pass(2 * time.Minute)
	if o := rig.obligation(directive.EventID); o.Spent != 2 || o.State != obligationExhausted || len(rig.agent.relays) != 0 {
		t.Fatalf("after reported retry obligation=%+v, queued=%d", o, len(rig.agent.relays))
	}
}

func TestT1034ObligationWriteFailureKeepsWindow(t *testing.T) {
	rig := newT6384Rig(t)
	directive := rig.send("lane-a", "store-outage")
	store := rig.hub.inbox.store
	if _, err := store.db.Exec(`CREATE TRIGGER reject_retry BEFORE UPDATE OF fault ON delivery_obligations BEGIN SELECT RAISE(FAIL, 'fixture store outage'); END`); err != nil {
		t.Fatal(err)
	}
	reportUnconfirmedRetry(t, rig, directive)
	if o := rig.obligation(directive.EventID); o.Fault != "" {
		t.Fatalf("failed write changed the fault: %+v", o)
	}
	if _, err := store.db.Exec("DROP TRIGGER reject_retry"); err != nil {
		t.Fatal(err)
	}
	reportUnconfirmedRetry(t, rig, directive)
	rig.pass(0)
	rig.injected()
}

func TestT1002OldAckTimerDoesNotExpireRetriedWindow(t *testing.T) {
	rig := newT6384Rig(t)
	directive := rig.send("lane-a", "previous-window-timer")
	key := relayPendingKey(directive.EventID, directive.JobID)
	rig.hub.mu.Lock()
	oldTimer := rig.hub.relayControl.relayPending[key].armToken
	rig.hub.mu.Unlock()
	reportUnconfirmedRetry(t, rig, directive)
	rig.pass(0)
	copy := rig.injected()
	rig.hub.expireRelayAckWindow(key, oldTimer)
	rig.hub.mu.Lock()
	_, live := rig.hub.relayControl.relayPending[key]
	rig.hub.mu.Unlock()
	if !live {
		t.Fatal("the first copy's timer expired the retried copy")
	}
	rig.delivered(copy)
	rig.pass(2 * time.Minute)
	rig.want(directive.EventID, obligationConfirmed, "")
}

func TestT1002JobObligationRetryUsesOriginalReport(t *testing.T) {
	rig := newT6384Rig(t)
	event := hubJobEventPayload{
		JobID: "job-obligation-retry", Epoch: 1, OwnerLane: "worker-a",
		EventID: "escalation", Question: "which way?", ReportPath: "report.md",
	}
	rig.hub.relayJobEventFrom("host-a", "job.escalate", event)
	directive := rig.injected()
	reportUnconfirmedRetry(t, rig, directive)

	// A duplicated producer frame cannot queue around the scheduled attempt.
	rig.hub.relayJobEventFrom("host-a", "job.escalate", event)
	if len(rig.agent.relays) != 0 {
		t.Fatal("the producer resend bypassed the obligation")
	}
	rig.fake.mu.Lock()
	rig.fake.failPostOwnerLane = "worker-a"
	rig.fake.mu.Unlock()
	rig.pass(0)
	copy := rig.injected()
	if copy.Text != directive.Text || copy.EventID != directive.EventID {
		t.Fatalf("job retry changed the report: %+v", copy)
	}
}

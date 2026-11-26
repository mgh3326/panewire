package panewire

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
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
	rig.hub.expireRelayAck(relayPendingKey(directive.EventID, directive.JobID))
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
	// The previous process stopped after the durable spend and before queueing. Neither the walk nor Sweep may repeat it.
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
			rig.hub.expireRelayAck(relayPendingKey(directive.EventID, directive.JobID))
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

	// Rollback before the first Sweep must be protected too.
	// The compatibility replay takes the old binary's path and sees only
	// handoffkeep. It has no access to the obligation or to any live claim.
	old, agent := quietHub(t, "", "off", rig.client, &rig.clock.now)
	old.replayUndeliveredRelayRows(t.Context(), "", relayReplaySourceStartup, "rollback listing")
	if len(agent.relays) != 0 {
		t.Fatalf("compatibility replay queued %d copies before the first sweep", len(agent.relays))
	}
	rig.pass(0)
	rig.injected()
	old.replayUndeliveredRelayRows(t.Context(), "", relayReplaySourceStartup, "rollback listing")
	if len(agent.relays) != 0 {
		t.Fatalf("compatibility replay queued %d copies after the retry", len(agent.relays))
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

func TestT1034FirstFaultImportsLegacyAttemptBudget(t *testing.T) {
	for _, mode := range []string{"report", "legacy-marker"} {
		t.Run(mode, func(t *testing.T) {
			rig := newT6384Rig(t)
			directive := rig.send("lane-a", "already-at-ceiling")
			rig.fake.mu.Lock()
			for _, row := range rig.fake.rows {
				if row.ID == directive.EventID {
					row.Attempts = relayReplayMaxAttempts
				}
			}
			rig.fake.mu.Unlock()
			if mode == "report" {
				reportUnconfirmedRetry(t, rig, directive)
			} else {
				marker := t6384Event(relayUnconfirmedMarkLane, unconfirmedMarkEventID(directive.EventID), "legacy mark")
				_, _, err := rig.client.appendEvent(t.Context(), rig.hub.relayEventRequest("lane.event", marker, reportRelayRoute{}))
				if err != nil {
					t.Fatal(err)
				}
				if err := rig.hub.Close(); err != nil {
					t.Fatal(err)
				}
				rig.start()
				rig.hub.replayUndeliveredRelayEvents(t.Context())
			}
			rig.pass(0)
			if o := rig.obligation(directive.EventID); o.Spent != relayReplayMaxAttempts || o.State != obligationExhausted || len(rig.agent.relays) != 0 {
				t.Fatalf("legacy budget reopened: obligation=%+v queued=%d", o, len(rig.agent.relays))
			}
		})
	}
}

func TestT1034DropRetainsWindowUntilFaultWriteSucceeds(t *testing.T) {
	rig := newT6384Rig(t)
	directive := rig.send("lane-a", "terminal-drop")
	store := rig.hub.inbox.store
	if _, err := store.db.Exec(`CREATE TRIGGER reject_drop BEFORE UPDATE OF fault ON delivery_obligations BEGIN SELECT RAISE(FAIL, 'fixture store outage'); END`); err != nil {
		t.Fatal(err)
	}
	t650Send(t, rig.hub, "host-a", rig.agent, "relay.dropped", relayDroppedPayload{
		JobID: directive.JobID, OriginalEventID: directive.EventID, Pane: directive.Pane,
		Lane: "lane-a", Reason: relayDroppedMaybeInPane,
	})
	eventually(t, 2*time.Second, "the drop fault write failed", func() bool {
		return strings.Contains(rig.logs.String(), "relay retry could not be scheduled")
	})
	if _, err := store.db.Exec("DROP TRIGGER reject_drop"); err != nil {
		t.Fatal(err)
	}
	// Drive the same persistence retry the timer invokes.
	rig.hub.expireRelayAck(relayPendingKey(directive.EventID, directive.JobID))
	rig.pass(0)
	if copy := rig.injected(); copy.EventID != directive.EventID {
		t.Fatalf("retried the wrong row: %+v", copy)
	}
}

func TestT1034SweepDoesNotDelayFirstIdleWake(t *testing.T) {
	rig := newT6384Rig(t)
	directive := rig.send("lane-a", "first-idle-window")
	t650Send(t, rig.hub, "host-a", rig.agent, "relay.unconfirmed", relayAckPayload{
		JobID: directive.JobID, OriginalEventID: directive.EventID, Pane: directive.Pane, Reason: relayDroppedWorkingExpired,
	})
	rig.pass(0)
	if len(rig.agent.relays) != 0 {
		t.Fatal("sweep queued a working-expired row before idle")
	}
	// The pass parked the row; the wake makes it due at once.
	rig.hub.redeliverWorkingExpiredOnIdle("host-a", idleWakeRouteRequest{
		Pane: directive.Pane, EventID: "idle:w1:session:1", StateChangeSeq: 1,
	})
	rig.pass(0)
	if copy := rig.injected(); !strings.HasPrefix(copy.Text, relayIdleRedeliveryLabel(rig.row(directive.EventID))) {
		t.Fatalf("idle retry did not carry the idle label: %q", copy.Text)
	}
}

func TestT1034RetryChecksAppliedGateAtQueueTime(t *testing.T) {
	rig := newT6384Rig(t)
	directive := rig.send("lane-a", "applied-during-spend")
	rig.hub.expireRelayAck(relayPendingKey(directive.EventID, directive.JobID))
	replacement := &hubAgent{relays: make(chan hubRelayInjectEvent, 4)}
	rig.fake.mu.Lock()
	rig.fake.observe = func(method, path string) {
		if method != "POST" || path != "/v1/relay/events" {
			return
		}
		rig.hub.mu.Lock()
		defer rig.hub.mu.Unlock()
		node := rig.hub.nodes["host-a"]
		node.agent, node.appliedPending = replacement, true
	}
	rig.fake.mu.Unlock()
	rig.pass(0)
	if old, next := len(rig.agent.relays), len(replacement.relays); old != 0 || next != 0 {
		t.Fatalf("retry raced with applied proof: old queue=%d replacement queue=%d", old, next)
	}
	if o := rig.obligation(directive.EventID); o.Spent != 2 {
		t.Fatalf("the registration did not change during the spend: %+v", o)
	}
}

func TestT1034IdleSweepPreservesCommittedRetrySchedule(t *testing.T) {
	rig := newT6384Rig(t)
	d := rig.send("lane-a", "idle-snapshot")
	t650Send(t, rig.hub, "host-a", rig.agent, "relay.unconfirmed", relayAckPayload{
		JobID: d.JobID, OriginalEventID: d.EventID, Pane: d.Pane, Reason: relayDroppedWorkingExpired,
	})
	record := rig.row(d.EventID)
	key := relayEventDedupeKey(record.Kind, relayEventFromRecord(record))
	store := rig.hub.inbox.store
	now := rig.clock.Now()
	if marked, err := store.requestObligationRevive(t.Context(), []string{"lane-a"}, now); err != nil || !marked {
		t.Fatalf("revive request marked=%v err=%v", marked, err)
	}
	o, err := store.retryObligationForKey(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	next := now.Add(time.Minute)
	if spent, err := store.spendObligationRevive(t.Context(), o, now, next); err != nil || !spent {
		t.Fatalf("spent=%v err=%v", spent, err)
	}
	// Sweep sampled this row before the revive was claimed.
	if err := rig.hub.classifyObligation(t.Context(), o, record, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.releaseObligationRevive(t.Context(), o, next, now.Add(5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	got, err := store.retryObligationForKey(t.Context(), key)
	if err != nil || got.labelled != 0 || got.revived != 0 || got.nextAt != now.Add(5*time.Minute).UnixMilli() {
		t.Fatalf("stale sweep prevented the release: %+v err=%v", got, err)
	}
	// A later wake does not bring the released copy forward.
	rig.hub.redeliverWorkingExpiredOnIdle("host-a", idleWakeRouteRequest{Pane: d.Pane, EventID: "idle:w1:session:1", StateChangeSeq: 1})
	rig.pass(0)
	if len(rig.agent.relays) != 0 {
		t.Fatal("an early wake bypassed the retry deadline")
	}
	rig.pass(5 * time.Minute)
	rig.injected()
}

func TestT1034TransientFinalReadLeavesLabelAvailable(t *testing.T) {
	rig := newT6384Rig(t)
	d := rig.send("lane-a", "final-read-outage")
	reportUnconfirmedRetry(t, rig, d)
	client := *rig.client.client
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	var reads atomic.Int32
	client.Transport = hubRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodGet && req.URL.Path == "/v1/relay/events" &&
			req.URL.Query().Get("after_id") == strconv.FormatInt(d.EventID-1, 10) {
			if reads.Add(1) == 2 {
				return nil, errors.New("fixture final lookup failed")
			}
		}
		return base.RoundTrip(req)
	})
	rig.client.client = &client
	rig.hub.classifyObligations(t.Context())
	if reads.Load() != 2 || len(rig.agent.relays) != 0 {
		t.Fatalf("reads=%d queued=%d", reads.Load(), len(rig.agent.relays))
	}
	rig.pass(time.Minute)
	if copy := rig.injected(); copy.EventID != d.EventID {
		t.Fatalf("wrong row retried: %+v", copy)
	}
}

func TestT1034ObservedDeferralDoesNotOverwriteFirstFault(t *testing.T) {
	rig := newT6384Rig(t)
	d := rig.send("lane-a", "fault-during-poll")
	record := rig.row(d.EventID)
	store := rig.hub.inbox.store
	key := relayEventDedupeKey(record.Kind, relayEventFromRecord(record))
	o, err := store.retryObligationForKey(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	rig.clock.advance(obligationFirstCheck)
	reportUnconfirmedRetry(t, rig, d)
	now := rig.clock.Now()
	if err := rig.hub.classifyObligation(t.Context(), o, record, now); err != nil {
		t.Fatal(err)
	}
	if got := rig.obligation(d.EventID); got.NextAt != now.UnixMilli() {
		t.Fatalf("first fault lost its immediate schedule: %+v", got)
	}
}

func TestT1034UnconfirmedDoesNotConsumeReplacementWindow(t *testing.T) {
	rig := newT6384Rig(t)
	d := rig.send("lane-a", "replacement-window")
	key := relayPendingKey(d.EventID, d.JobID)
	rig.fake.mu.Lock()
	rig.fake.observe = func(method, path string) {
		if method != "POST" || path != "/v1/relay/events" {
			return
		}
		rig.hub.mu.Lock()
		defer rig.hub.mu.Unlock()
		pending := rig.hub.relayControl.relayPending[key]
		delete(rig.hub.relayControl.relayPending, key)
		rig.hub.registerRelayAckEventLocked(pending.kind, pending.event, pending.machine, pending.pane, pending.eventID)
	}
	rig.fake.mu.Unlock()
	reportUnconfirmedRetry(t, rig, d)
	rig.hub.mu.Lock()
	_, ok := rig.hub.relayControl.relayPending[key]
	rig.hub.mu.Unlock()
	if !ok {
		t.Fatal("the old report consumed the replacement window")
	}
}

func TestT1034FoldedJobFaultPreservesProducerClaim(t *testing.T) {
	for _, mode := range []string{"report", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			rig := newT6384Rig(t)
			round := hubJobEventPayload{JobID: "job-folded", Epoch: 1, OwnerLane: "worker-a", Question: "round one?", ReportPath: "r.md", EventID: "round-1"}
			rig.hub.relayJobEventFrom("host-a", "job.escalate", round)
			first := rig.injected()
			rig.delivered(first)
			round.Question, round.EventID = "round two?", "round-2"
			rig.hub.relayJobEventFrom("host-a", "job.escalate", round)
			second := rig.injected()
			if second.EventID != first.EventID {
				t.Fatal("fixture did not fold the second round")
			}
			if mode == "report" {
				reportUnconfirmedRetry(t, rig, second)
			} else {
				rig.hub.expireRelayAck(relayPendingKey(second.EventID, second.JobID))
			}
			rig.hub.relayJobEventFrom("host-a", "job.escalate", round)
			if len(rig.agent.relays) != 0 {
				t.Fatal("producer resend injected the folded round twice")
			}
		})
	}
}

func TestT1034CanceledCeilingWriteReleasesLabel(t *testing.T) {
	rig := newT6384Rig(t)
	d := rig.send("lane-a", "canceled-ceiling")
	// A legacy marker schedules the retry without first raising the ceiling.
	marker := t6384Event(relayUnconfirmedMarkLane, unconfirmedMarkEventID(d.EventID), "legacy mark")
	if _, _, err := rig.client.appendEvent(t.Context(), rig.hub.relayEventRequest("lane.event", marker, reportRelayRoute{})); err != nil {
		t.Fatal(err)
	}
	if err := rig.hub.Close(); err != nil {
		t.Fatal(err)
	}
	rig.start()
	rig.hub.replayUndeliveredRelayEvents(t.Context())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	client := *rig.client.client
	oldClient := rig.client.client
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	client.Transport = hubRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodPost && req.URL.Path == "/v1/relay/events" {
			cancel()
			return nil, ctx.Err()
		}
		return base.RoundTrip(req)
	})
	rig.client.client = &client
	rig.hub.classifyObligations(ctx)
	rig.client.client = oldClient
	if o := rig.obligation(d.EventID); o.Spent != 2 || len(rig.agent.relays) != 0 {
		t.Fatalf("canceled write: spent=%d queued=%d", o.Spent, len(rig.agent.relays))
	}
	rig.pass(5 * time.Minute)
	rig.injected()
}

func TestT1034FullRelayQueueReleasesLabel(t *testing.T) {
	rig := newT6384Rig(t)
	d := rig.send("lane-a", "full-retry-queue")
	reportUnconfirmedRetry(t, rig, d)
	for range cap(rig.agent.relays) {
		rig.agent.relays <- hubRelayInjectEvent{}
	}
	rig.pass(0)
	for range cap(rig.agent.relays) {
		<-rig.agent.relays
	}
	if o := rig.obligation(d.EventID); o.Spent != 2 {
		t.Fatalf("queue refusal spent=%d", o.Spent)
	}
	rig.pass(time.Minute)
	rig.injected()
}

func TestT1034DropPersistenceRetainsIdleWake(t *testing.T) {
	rig := newT6384Rig(t)
	d := rig.send("lane-a", "pending-drop-wake")
	pending, ok := rig.hub.consumeRelayDropped("host-a", relayDroppedPayload{
		JobID: d.JobID, OriginalEventID: d.EventID, Pane: d.Pane, Lane: "lane-a", Reason: relayDroppedWorkingExpired,
	})
	if !ok {
		t.Fatal("drop rejected")
	}
	wake := idleWakeRouteRequest{Pane: d.Pane, EventID: "idle:w1:session:1", StateChangeSeq: 1}
	rig.hub.redeliverWorkingExpiredOnIdle("host-a", wake)
	if len(rig.agent.relays) != 0 {
		t.Fatal("queued before fault persistence")
	}
	rig.hub.expireRelayAckWindow(relayPendingKey(d.EventID, d.JobID), pending.armToken)
	eventually(t, 2*time.Second, "retained wake queued one retry", func() bool { return len(rig.agent.relays) == 1 })
	if got := rig.injected(); !strings.HasPrefix(got.Text, relayIdleRedeliveryLabel(rig.row(d.EventID))) {
		t.Fatalf("idle copy=%q", got.Text)
	}
	rig.hub.redeliverWorkingExpiredOnIdle("host-a", wake)
	if len(rig.agent.relays) != 0 {
		t.Fatal("the same idle turn queued twice")
	}
}

func TestT1034FailedWorkingReportKeepsItsFaultForExpiry(t *testing.T) {
	rig := newT6384Rig(t)
	d := rig.send("lane-a", "working-fault-write-recovery")
	store := rig.hub.inbox.store
	if _, err := store.db.Exec(`CREATE TRIGGER reject_working_fault BEFORE UPDATE OF fault ON delivery_obligations BEGIN SELECT RAISE(FAIL, 'fixture store outage'); END`); err != nil {
		t.Fatal(err)
	}
	t650Send(t, rig.hub, "host-a", rig.agent, "relay.unconfirmed", relayAckPayload{
		JobID: d.JobID, OriginalEventID: d.EventID, Pane: d.Pane, Reason: relayDroppedWorkingExpired,
	})
	if _, err := store.db.Exec("DROP TRIGGER reject_working_fault"); err != nil {
		t.Fatal(err)
	}
	rig.hub.expireRelayAck(relayPendingKey(d.EventID, d.JobID))
	rig.pass(0)
	if o := rig.obligation(d.EventID); o.Fault != relayDroppedWorkingExpired || len(rig.agent.relays) != 0 {
		t.Fatalf("expiry lost working fault: obligation=%+v queued=%d", o, len(rig.agent.relays))
	}
	rig.hub.redeliverWorkingExpiredOnIdle("host-a", idleWakeRouteRequest{
		Pane: d.Pane, EventID: "idle:w1:session:1", StateChangeSeq: 1,
	})
	rig.pass(0)
	rig.injected()
}

func TestT1034JobFinalReadFailurePreservesRetryAcrossRestart(t *testing.T) {
	rig := newT6384Rig(t)
	event := hubJobEventPayload{JobID: "job-final-read", Epoch: 1, OwnerLane: "worker-a",
		EventID: "job-final-read-event", Question: "please decide?", ReportPath: "report.md"}
	rig.hub.relayJobEventFrom("host-a", "job.escalate", event)
	d := rig.injected()
	reportUnconfirmedRetry(t, rig, d)

	oldClient := rig.client.client
	client := *oldClient
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	var reads atomic.Int32
	client.Transport = hubRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodGet && req.URL.Path == "/v1/relay/events" &&
			req.URL.Query().Get("after_id") == strconv.FormatInt(d.EventID-1, 10) && reads.Add(1) == 2 {
			return nil, errors.New("fixture final job lookup failed")
		}
		return base.RoundTrip(req)
	})
	rig.client.client = &client
	rig.hub.classifyObligations(t.Context())
	rig.client.client = oldClient
	if reads.Load() != 2 || len(rig.agent.relays) != 0 {
		t.Fatalf("reads=%d queued=%d before recovery", reads.Load(), len(rig.agent.relays))
	}
	if err := rig.hub.Close(); err != nil {
		t.Fatal(err)
	}
	rig.start()
	rig.hub.replayUndeliveredRelayEvents(t.Context())
	rig.pass(time.Minute)
	if copy := rig.injected(); copy.EventID != d.EventID || copy.Text != d.Text {
		t.Fatalf("retry changed the job report: %+v", copy)
	}
}

func TestT1034DropRedispatchesWakeBlockedByHeldProjection(t *testing.T) {
	rig := newT6384Rig(t)
	d := rig.send("lane-a", "held-projection-clears")
	rig.hub.mu.Lock()
	rig.hub.relayHeld[d.EventID] = hubRelayHeldProjection{
		ID: d.EventID, JobID: d.JobID, Lane: "lane-a", Machine: "host-a", Pane: d.Pane,
	}
	rig.hub.mu.Unlock()
	wake := idleWakeRouteRequest{Pane: d.Pane, EventID: "idle:w1:session:1", StateChangeSeq: 1}
	var once atomic.Bool
	rig.fake.mu.Lock()
	rig.fake.observe = func(method, path string) {
		if method == "POST" && path == "/v1/relay/events" && once.CompareAndSwap(false, true) {
			rig.hub.redeliverWorkingExpiredOnIdle("host-a", wake)
		}
	}
	rig.fake.mu.Unlock()
	t650Send(t, rig.hub, "host-a", rig.agent, "relay.unconfirmed", relayAckPayload{
		JobID: d.JobID, OriginalEventID: d.EventID, Pane: d.Pane, Reason: relayDroppedWorkingExpired,
	})
	// Run the saved wake while the old lease is still projected, regardless
	// of when the report handler's goroutine is scheduled.
	rig.hub.redeliverWorkingExpiredOnIdle("host-a", wake)
	if !once.Load() || len(rig.agent.relays) != 0 {
		t.Fatal("the wake was not observed during persistence, or queued while held")
	}
	drop := relayDroppedPayload{JobID: d.JobID, OriginalEventID: d.EventID, Pane: d.Pane,
		Lane: "lane-a", Reason: relayDroppedWorkingExpired}
	t650Send(t, rig.hub, "host-a", rig.agent, "relay.dropped", drop)
	eventually(t, 2*time.Second, "terminal drop dispatches the saved wake", func() bool { return len(rig.agent.relays) == 1 })
	if copy := rig.injected(); !strings.HasPrefix(copy.Text, relayIdleRedeliveryLabel(rig.row(d.EventID))) {
		t.Fatalf("idle retry=%q", copy.Text)
	}
	t650Send(t, rig.hub, "host-a", rig.agent, "relay.dropped", drop)
	if len(rig.agent.relays) != 0 {
		t.Fatal("duplicate drop queued another copy")
	}
}

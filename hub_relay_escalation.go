package panewire

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Contract: docs/relay-harness-delivery.md.
//
// hk 3276: review findings, ESC answers and orders go straight to the job's
// worker. When the worker's node gives up on one -- herdr kept rejecting the
// send, or the pane never showed the text taken -- nobody else knows the worker
// did not get it. The hub then tells the job's design lane once, naming the
// event, instead of leaving a quiet gap.

const (
	// unconfirmedEscalationPrefix starts the producer event id of the
	// escalation. It is deterministic in the durable row id, so a second drop
	// of the same row (a replay that also fails) re-posts the same lane.event
	// and the durable idempotency answers it as a duplicate.
	unconfirmedEscalationPrefix = "delivery-unconfirmed-"
	// relayDroppedUnconfirmed are the relay.dropped reasons that end a delivery
	// the node could not prove: the retry budget spent on rejected sends, and a
	// send that may already be in the pane and is never re-injected.
	relayDroppedRetriesExhausted = "inject_failed_max_attempts"
	relayDroppedMaybeInPane      = "maybe_in_pane"
	// relayDroppedWorkingExpired ends a directive held for a working pane that
	// never turned idle within max_wait (hk 3370). Nothing was typed.
	relayDroppedWorkingExpired = "working_wait_expired"
)

// unconfirmedEscalationBackoff spaces the retries of an escalation whose
// durable write failed (a handoffkeep outage). The drop was already consumed,
// so nothing else would send it; the last wait is the final try.
var unconfirmedEscalationBackoff = []time.Duration{2 * time.Second, 10 * time.Second, time.Minute, 5 * time.Minute}

func relayDroppedIsUnconfirmed(reason string) bool {
	return reason == relayDroppedRetriesExhausted || reason == relayDroppedMaybeInPane || reason == relayDroppedWorkingExpired
}

// designLaneFor names the lane that owns the job a worker lane belongs to: the
// lane its route reports to (the parent job.escalate already follows), else the
// owner lane of the live job that runs in that lane. It returns "" when neither
// is a registered route, and never the worker lane itself.
func (h *HubServer) designLaneFor(lane string) string {
	routes := loadReportRelayRoutes(h.reportRelayPath)
	if parent := routes[lane].Parent; parent != "" && parent != lane {
		if _, registered := routes[parent]; registered {
			return parent
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, job := range h.jobs {
		if job.Completed || job.Orphaned || job.AgentLabel != lane || job.OwnerLane == "" || job.OwnerLane == lane {
			continue
		}
		if _, registered := routes[job.OwnerLane]; registered {
			return job.OwnerLane
		}
	}
	return ""
}

// escalatesWhenUnconfirmed picks the rows worth a design lane's attention: a
// producer's lane.event. A job.* record has its own routing, an idle-wake
// reports one instant that a replay would only restate, a chat row belongs to
// the chat store, the unconfirmed marker is bookkeeping, and an escalation that
// is itself lost must not start another.
func escalatesWhenUnconfirmed(record handoffkeepRelayEvent) bool {
	return record.Kind == "lane.event" &&
		!isUnconfirmedMarkRecord(record) &&
		!strings.HasPrefix(record.EventID, unconfirmedEscalationPrefix) &&
		!strings.HasPrefix(record.EventID, "idle-wake:") &&
		!chatRelayEventPattern.MatchString(record.EventID)
}

// escalateUnconfirmedRelay sends the one escalation for a relay row the node
// dropped as unconfirmed. It runs after the drop was consumed, so the held
// projection and ack window are already clear. A row the pane already confirmed,
// a row escalatesWhenUnconfirmed refuses, and a lane with no design lane to
// tell are skipped. A transient handoffkeep failure, reading the row or writing
// the escalation, is retried on unconfirmedEscalationBackoff until the
// escalation is durable: the durable id keeps the retries to one escalation.
func (h *HubServer) escalateUnconfirmedRelay(machine string, dropped relayDroppedPayload) {
	h.escalateUnconfirmedRelayAttempt(machine, dropped, 0)
}

func (h *HubServer) escalateUnconfirmedRelayAttempt(machine string, dropped relayDroppedPayload, attempt int) {
	if !relayDroppedIsUnconfirmed(dropped.Reason) || dropped.OriginalEventID < 1 || dropped.Lane == "" || h.handoffkeep == nil {
		return
	}
	retry := func(why string) {
		if attempt >= len(unconfirmedEscalationBackoff) {
			h.logger.Error("relay unconfirmed escalation gave up", "event_id", dropped.OriginalEventID, "lane", dropped.Lane, "why", why)
			return
		}
		h.logger.Warn("relay unconfirmed escalation will retry", "event_id", dropped.OriginalEventID, "lane", dropped.Lane, "why", why, "attempt", attempt+1)
		time.AfterFunc(unconfirmedEscalationBackoff[attempt], func() { h.escalateUnconfirmedRelayAttempt(machine, dropped, attempt+1) })
	}
	ctx := context.Background()
	record, found, err := h.handoffkeep.relayEvent(ctx, dropped.OriginalEventID)
	if err != nil {
		retry("row_read_failed")
		return
	}
	if !found || record.DeliveredAt != "" || !escalatesWhenUnconfirmed(record) {
		return
	}
	design := h.designLaneFor(dropped.Lane)
	if design == "" {
		h.logger.Warn("relay unconfirmed escalation has no design lane", "event_id", record.ID, "lane", dropped.Lane, "reason", dropped.Reason)
		return
	}
	eventID := fmt.Sprintf("%s%d", unconfirmedEscalationPrefix, record.ID)
	event := hubJobEventPayload{
		JobID:      laneEventTransportID(design, eventID),
		Epoch:      1,
		OwnerLane:  design,
		EventID:    eventID,
		NotifyType: "automation-failed",
		Host:       "hub",
		Label:      "delivery-unconfirmed",
		Reason:     dropped.Reason,
		Text:       unconfirmedEscalationText(record, dropped, machine),
	}
	result := h.relayLaneEventContext(ctx, event, nil)
	if result.PersistFailed && (result.PersistStatus == 0 || result.PersistStatus >= 500) {
		retry("persist_failed")
		return
	}
	if result.ID == 0 || result.Duplicate {
		// A second drop of the same row posts the same event id and the
		// durable idempotency answers it: the design lane was told already.
		return
	}
	h.logger.Warn("relay delivery unconfirmed; design lane told", "event_id", record.ID, "lane", dropped.Lane, "design_lane", design, "reason", dropped.Reason)
}

// unconfirmedEscalationText is the one line the design lane reads. It carries
// the durable event id, the worker lane and pane, the producer's event id and
// the reason, so the receiver can re-send the original or look at the pane.
func unconfirmedEscalationText(record handoffkeepRelayEvent, dropped relayDroppedPayload, machine string) string {
	return fmt.Sprintf("[delivery unconfirmed] event %d (%s, producer id %s) for lane %s was not confirmed on %s pane %s: %s. The worker may not have it; read the pane or re-send. Reported once.",
		record.ID, record.Kind, truncateRelayText(record.EventID, 64), dropped.Lane, truncateRelayText(machine, 64), truncateRelayText(dropped.Pane, 64), dropped.Reason)
}

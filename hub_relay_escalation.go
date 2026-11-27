package panewire

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Contract: docs/relay-harness-delivery.md, "Escalation".
//
// hk 3276: review findings, ESC answers and orders go straight to the job's
// worker. When a delivery to the worker stays unproven -- herdr kept rejecting
// the send, or the pane never showed the text taken -- nobody else knows the
// worker did not get it. The hub then tells the job's design lane, naming an
// event, instead of leaving a quiet gap.
//
// hk 3431: only when someone can act on it, and one per host fault per
// incident. A worker lane whose hub jobs have all ended, or whose live job's hk
// task has ended, is logged instead (unconfirmedNoticeMoot). A hub with an
// inbox store keeps the incidents in it (delivery_incident_pass.go, hk 6821);
// a hub without one keeps the hk 3431 window in memory
// (hub_relay_escalation_legacy.go).

const (
	// unconfirmedEscalationPrefix starts the producer event id of the
	// escalation. The id is deterministic -- in the incident and its announced
	// count, or in the durable row id on a hub without an inbox store -- so a
	// repeated post of one notice is answered by the durable idempotency as a
	// duplicate.
	unconfirmedEscalationPrefix = "delivery-unconfirmed-"
	// relayDroppedUnconfirmed are the relay.dropped reasons that end a delivery
	// the node could not prove: the retry budget spent on rejected sends, and a
	// send that may already be in the pane and is never re-injected.
	relayDroppedRetriesExhausted = "inject_failed_max_attempts"
	relayDroppedMaybeInPane      = "maybe_in_pane"
	relayDroppedCheckPane        = "delivery_uncertain_check_pane"
	relayDroppedPaneReadFailed   = "pane_read_failed"
	// relayDroppedWorkingExpired ends a row held for a working cmd pane that
	// never turned idle within relay.cmd_hold_max (hk 3370, hk 3411). Nothing
	// was typed; the next idle wake re-delivers it once
	// (hub_relay_idle_redelivery.go).
	relayDroppedWorkingExpired = "working_wait_expired"
)

func relayDroppedIsUnconfirmed(reason string) bool {
	return reason == relayDroppedRetriesExhausted || reason == relayDroppedMaybeInPane || reason == relayDroppedCheckPane || reason == relayDroppedPaneReadFailed || reason == relayDroppedWorkingExpired
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
		!strings.HasPrefix(record.EventID, "idle-wake:")
}

// unconfirmedEscalationText is the one line the design lane reads. It carries
// the durable event id, the worker lane and pane, the producer's event id and
// the reason, so the receiver can re-send the original or look at the pane.
// mayBeTyped says the text may already be in the pane.
func unconfirmedEscalationText(record handoffkeepRelayEvent, lane, machine, pane, reason string, mayBeTyped bool) string {
	action := "The worker may not have it; read the pane or re-send."
	if mayBeTyped || reason == relayDroppedCheckPane {
		action = "Check the pane before resending; it may already contain the message."
	} else if reason == relayDroppedPaneReadFailed {
		action = "The pane could not be read before sending; restore pane access and resend."
	}
	return fmt.Sprintf("[delivery unconfirmed] event %d (%s, producer id %s) for lane %s was not confirmed on %s pane %s: %s. %s Reported once.",
		record.ID, record.Kind, truncateRelayText(record.EventID, 64), lane, truncateRelayText(machine, 64), truncateRelayText(pane, 64), reason, action)
}

// unconfirmedNoticeEvent is the lane.event that carries a notice to design.
func unconfirmedNoticeEvent(design, eventID, reason, text string) hubJobEventPayload {
	return hubJobEventPayload{
		JobID:      laneEventTransportID(design, eventID),
		Epoch:      1,
		OwnerLane:  design,
		EventID:    eventID,
		NotifyType: "automation-failed",
		Host:       "hub",
		Label:      "delivery-unconfirmed",
		Reason:     reason,
		Text:       text,
	}
}

// unconfirmedNoticeMoot names why nobody can act on a notice about the worker
// lane: "job_ended" when the hub knows jobs in that lane and every one is
// completed or orphaned, "task_ended" when every live one's hk task has ended.
// It returns "" -- send the notice -- when the hub knows no job in the lane (a
// hub restart forgets ended jobs, and a lane may run work the hub never
// placed), when a live job has no task, and when a job record or task cannot
// be read.
func (h *HubServer) unconfirmedNoticeMoot(lane string) string {
	known, live := h.noticeLaneJobs(lane)
	if !known {
		return ""
	}
	if len(live) == 0 {
		return "job_ended"
	}
	ctx, cancel := context.WithTimeout(h.closeCtx, handoffkeepTimeout)
	defer cancel()
	for _, id := range live {
		task, app, err := h.jobTaskID(ctx, id)
		if err != nil {
			h.logger.Warn("job record unreadable; unconfirmed delivery reported", "job", id, "lane", lane)
			return ""
		}
		if task == "" {
			return ""
		}
		status, err := app.TaskStatus(ctx, task)
		if err != nil {
			h.logger.Warn("task unreadable; unconfirmed delivery reported", "job", id, "task", task, "lane", lane)
			return ""
		}
		if !status.Ended() {
			return ""
		}
	}
	return "task_ended"
}

// laneJobsAllEnded is the part of unconfirmedNoticeMoot the hub's own job
// records answer: "job_ended", or "".
func (h *HubServer) laneJobsAllEnded(lane string) string {
	if known, live := h.noticeLaneJobs(lane); known && len(live) == 0 {
		return "job_ended"
	}
	return ""
}

// noticeLaneJobs reports whether the hub knows a job that runs in the lane,
// and the ids of those that have not ended.
func (h *HubServer) noticeLaneJobs(lane string) (known bool, live []string) {
	route, routed := loadReportRelayRoutes(h.reportRelayPath)[lane]
	// A takeover keeps the lane name while replacing its worker. Once the
	// report-relay row carries job identity, that binding is authoritative;
	// the predecessor's AgentLabel still names the lane but no longer names
	// the job that can act on an unconfirmed notice.
	routeBound := routed && (route.Job != "" || route.TakenOverFrom != "")
	h.mu.Lock()
	defer h.mu.Unlock()
	if routeBound {
		if route.Job != "" {
			if job := h.jobs[route.Job]; job != nil {
				known = true
				if !job.Completed && !job.Orphaned {
					live = append(live, route.Job)
				}
			}
		}
	} else {
		for id, job := range h.jobs {
			if job.AgentLabel != lane {
				continue
			}
			known = true
			if !job.Completed && !job.Orphaned {
				live = append(live, id)
			}
		}
	}
	return known, live
}

// unconfirmedNoticeSpacing is how long one stored notice speaks for its key --
// (design lane, machine, fault), or (design lane, worker lane) on a hub without
// an inbox store: a later drop inside it is folded into the next notice.
const unconfirmedNoticeSpacing = 30 * time.Minute

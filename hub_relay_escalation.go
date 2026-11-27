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
	if parent := parentDesignLane(routes, lane); parent != "" {
		return parent
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	live, _, _ := h.jobDesignLanesLocked(routes, lane)
	return live
}

// parentDesignLane is the registered lane the worker lane's route reports to,
// or "".
func parentDesignLane(routes map[string]reportRelayRoute, lane string) string {
	if parent := routes[lane].Parent; parent != "" && parent != lane {
		if _, registered := routes[parent]; registered {
			return parent
		}
	}
	return ""
}

// jobDesignLanesLocked reads the owner lanes of the jobs, not completed, that
// run in the worker lane: live is a registered owner of one that is not
// orphaned, away a registered owner of an orphaned one, and ownerAway whether
// an orphaned one names any owner. The caller holds h.mu.
func (h *HubServer) jobDesignLanesLocked(routes map[string]reportRelayRoute, lane string) (live, away string, ownerAway bool) {
	for _, job := range h.jobs {
		if job.Completed || job.AgentLabel != lane || job.OwnerLane == "" || job.OwnerLane == lane {
			continue
		}
		ownerAway = ownerAway || job.Orphaned
		if _, registered := routes[job.OwnerLane]; !registered {
			continue
		}
		if job.Orphaned {
			away = job.OwnerLane
		} else {
			live = job.OwnerLane
		}
	}
	return live, away, ownerAway
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
// be read. A hub without an inbox store asks it once, at the drop; the incident
// pass, which asks later and may ask again, holds an orphaned job's notice
// instead (delivery_incident_lane.go).
func (h *HubServer) unconfirmedNoticeMoot(lane string) string {
	routes := loadReportRelayRoutes(h.reportRelayPath)
	h.mu.Lock()
	jobs := h.noticeLaneJobsLocked(routes, lane)
	h.mu.Unlock()
	if !jobs.known {
		return ""
	}
	if len(jobs.live) == 0 {
		return "job_ended"
	}
	ctx, cancel := context.WithTimeout(h.closeCtx, handoffkeepTimeout)
	defer cancel()
	for _, id := range jobs.live {
		if ended, _ := h.noticeTaskEnded(ctx, lane, id); !ended {
			return ""
		}
	}
	return "task_ended"
}

// noticeTaskEnded reports whether the job's hk task has ended; a job with no
// task has not. readable is false, and the failure logged, when its record or
// task cannot be read.
func (h *HubServer) noticeTaskEnded(ctx context.Context, lane, id string) (ended, readable bool) {
	task, app, err := h.jobTaskID(ctx, id)
	if err != nil {
		h.logger.Warn("job record unreadable; unconfirmed delivery reported", "job", id, "lane", lane)
		return false, false
	}
	if task == "" {
		return false, true
	}
	status, err := app.TaskStatus(ctx, task)
	if err != nil {
		h.logger.Warn("task unreadable; unconfirmed delivery reported", "job", id, "task", task, "lane", lane)
		return false, false
	}
	return status.Ended(), true
}

// noticeJobSet is one reading of the jobs a notice about a worker lane counts:
// whether the hub knows one, and the ids of those neither completed nor
// orphaned and of those orphaned and not completed.
type noticeJobSet struct {
	known          bool
	live, orphaned []string
}

// noticeLaneJobsLocked reads them. The caller holds h.mu.
func (h *HubServer) noticeLaneJobsLocked(routes map[string]reportRelayRoute, lane string) (jobs noticeJobSet) {
	route, routed := routes[lane]
	// A takeover keeps the lane name while replacing its worker. Once the
	// report-relay row carries job identity, that binding is authoritative;
	// the predecessor's AgentLabel still names the lane but no longer names
	// the job that can act on an unconfirmed notice.
	routeBound := routed && (route.Job != "" || route.TakenOverFrom != "")
	count := func(id string, job *hubJobRecord) {
		jobs.known = true
		switch {
		case job.Completed:
		case job.Orphaned:
			jobs.orphaned = append(jobs.orphaned, id)
		default:
			jobs.live = append(jobs.live, id)
		}
	}
	if routeBound {
		if route.Job != "" {
			if job := h.jobs[route.Job]; job != nil {
				count(route.Job, job)
			}
		}
		return jobs
	}
	for id, job := range h.jobs {
		if job.AgentLabel == lane {
			count(id, job)
		}
	}
	return jobs
}

// unconfirmedNoticeSpacing is how long one stored notice speaks for its key --
// (design lane, machine, fault), or (design lane, worker lane) on a hub without
// an inbox store: a later drop inside it is folded into the next notice.
const unconfirmedNoticeSpacing = 30 * time.Minute

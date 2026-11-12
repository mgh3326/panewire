package panewire

// Contract: docs/quiet-delivery.md, "Records kept in hk only" and
// "Reviewer spawn failures and unconfirmed deliveries" (hk 4270, operator
// decisions 152 and 153).
//
// A design lane's inbox lists what the design session must read. Three kinds
// of record never need it, because the party that acts on them already has
// them: the lifecycle of a reviewer job a daemon started (SPAWNED,
// job.completed, job.revoked), and a job.completed that repeats the READY the
// same worker sent a moment earlier. The hub files such a record in hk as a
// relay row it marks delivered with the reason (`filtered:<reason>`), writes no
// inbox item and so wakes nobody. The hk row is the durable copy: nothing is
// dropped, and a producer's retry finds the same row.
//
// A record is filtered only when hk holds it. A hub with no hk client files
// nothing and delivers as before, and a failed hk write leaves the producer's
// outbox row unacknowledged so it is sent again.

import (
	"context"
	"strings"
	"sync"
	"time"
)

const (
	// daemonReviewerJobPrefix starts the job id of every reviewer a daemon
	// (mergewake) starts. node_worktree.go's nodeReviewerDirPattern relies on
	// the same naming.
	daemonReviewerJobPrefix = "mwr-"

	// filteredReasonReviewerLifecycle and filteredReasonRepeatsReady are the
	// reasons a record is filed in hk only. They follow filteredDeliveryPrefix
	// in the delivered row's pane field.
	filteredReasonReviewerLifecycle = "reviewer-lifecycle"
	filteredReasonRepeatsReady      = "repeats-ready"
	filteredDeliveryPrefix          = "filtered:"
	// relayFilteredMachine is the machine field of a filtered row, beside
	// relayReplayRetiredMachine and the "sink" and "hub" marks.
	relayFilteredMachine = "hub"

	// readyRepeatWindow is how long after a worker's READY its own
	// job.completed counts as the same news. Past it the completion is new
	// information (the worker went on working) and is delivered.
	readyRepeatWindow = 10 * time.Minute
	// readyJobsMax bounds the READY notes the hub remembers.
	readyJobsMax = 4096
)

// designQuietTimings are the time limits of the design-lane filters. Zero
// values keep the pre-hk-4270 behaviour (no hold, no retry, notices at once),
// so a hub built without NewHubServer behaves as before; NewHubServer sets the
// defaults below.
type designQuietTimings struct {
	// spawnFailureHold is how long a retryable reviewer-spawn failure waits
	// for its requester's replay before the design lane is told anyway.
	spawnFailureHold time.Duration
	// unconfirmedRetryAfter is the wait before an unconfirmed delivery that
	// typed nothing (pane_read_failed, working_wait_expired) is delivered again.
	unconfirmedRetryAfter time.Duration
	// unconfirmedUnreadAfter is how long a delivery may stay unconfirmed before
	// the repository's design lane is told.
	unconfirmedUnreadAfter time.Duration
	// hostFaultWindow is how long one host fault notice speaks for the drops
	// that follow it on the same host and reason.
	hostFaultWindow time.Duration
	// hostFaultLane is the lane told once per host fault; empty disables it.
	hostFaultLane string
	// hkRetryBackoff spaces the retries of an hk read or write the timers
	// depend on: the retry's row read and a host fault summary.
	hkRetryBackoff []time.Duration
}

func defaultDesignQuietTimings() designQuietTimings {
	return designQuietTimings{
		spawnFailureHold:       30 * time.Minute,
		unconfirmedRetryAfter:  2 * time.Minute,
		unconfirmedUnreadAfter: 15 * time.Minute,
		hostFaultWindow:        30 * time.Minute,
		hostFaultLane:          "panewire-design",
		hkRetryBackoff:         []time.Duration{10 * time.Second, time.Minute, 5 * time.Minute},
	}
}

// designQuietState is the hub's memory for the design-lane filters. It is
// memory only: after a restart a READY is forgotten (the completion is then
// delivered, the safe direction) and an unconfirmed row is replayed once by the
// startup replay.
type designQuietState struct {
	timings designQuietTimings
	mu      sync.Mutex
	// ready maps a worker job to its last READY into a design lane.
	ready map[string]laneReadyNote
	// unconfirmed maps a durable hk row id to a delivery the node could not
	// confirm and nobody has confirmed since.
	unconfirmed map[int64]unconfirmedRow
	// hostFaults spaces the notices to the host fault lane, keyed by
	// (machine, reason).
	hostFaults unconfirmedNoticeWindows
}

type laneReadyNote struct {
	lane string
	at   time.Time
}

// daemonStartedReviewer reports whether a spawn is a reviewer a daemon
// started: the reviewer role and the daemon's job naming. The submitter's
// `--requested-by` is not part of the test: mergewake's value is not visible
// here, and a human may name a lane the same way. The role comes from the
// saved request, which the daemon cannot misdescribe without also changing
// what the spawn does.
func daemonStartedReviewer(job string, args []string) bool {
	return strings.HasPrefix(job, daemonReviewerJobPrefix) && hubSpawnArg(args, "--role") == reviewerRole
}

// daemonStartedJob reports whether the hub's own spawn record names job as a
// reviewer a daemon started. A job the hub did not place, or an unreadable
// store, is not one: the record is delivered as before.
func (h *HubServer) daemonStartedJob(ctx context.Context, job string) bool {
	if h.inbox == nil || !strings.HasPrefix(job, daemonReviewerJobPrefix) {
		return false
	}
	task, err := h.inbox.store.latestSpawnForJob(ctx, job)
	if err != nil {
		return false
	}
	return daemonStartedReviewer(task.Job, task.Request.Args)
}

// noteWorkerReady remembers that job's worker sent a READY into lane, so the
// completion that follows is recognised as a repeat.
func (h *HubServer) noteWorkerReady(job, lane string) {
	if job == "" {
		return
	}
	now := h.now()
	h.quiet.mu.Lock()
	defer h.quiet.mu.Unlock()
	if h.quiet.ready == nil {
		h.quiet.ready = make(map[string]laneReadyNote)
	}
	if len(h.quiet.ready) >= readyJobsMax {
		for id, note := range h.quiet.ready {
			if now.Sub(note.at) > readyRepeatWindow {
				delete(h.quiet.ready, id)
			}
		}
		if len(h.quiet.ready) >= readyJobsMax {
			return
		}
	}
	h.quiet.ready[job] = laneReadyNote{lane: lane, at: now}
}

// completionRepeatsReady reports whether a completion of job into lane comes
// within readyRepeatWindow of that job's READY into the same lane. The note is
// never spent: a completion is filtered by what it is, not by how often it was
// seen, so a producer's retry after a lost acknowledgement is filed again (and
// answered by the same hk row) instead of reaching the inbox. A worker that
// finishes a turn twice inside the window is reporting the same news twice.
func (h *HubServer) completionRepeatsReady(job, lane string) bool {
	h.quiet.mu.Lock()
	defer h.quiet.mu.Unlock()
	note, ok := h.quiet.ready[job]
	if !ok {
		return false
	}
	age := h.now().Sub(note.at)
	return note.lane == lane && age >= 0 && age <= readyRepeatWindow
}

// designFilterReason names why a record for a design lane is kept in hk only,
// or "" when it is delivered as before.
func (h *HubServer) designFilterReason(kind string, event hubJobEventPayload, lane string) string {
	if h.handoffkeep == nil || event.JobID == "" || !h.inbox.policy.designLane(lane) {
		return ""
	}
	switch kind {
	case "job.completed", "job.revoked":
		ctx, cancel := context.WithTimeout(h.closeCtx, handoffkeepTimeout)
		defer cancel()
		if h.daemonStartedJob(ctx, event.JobID) {
			return filteredReasonReviewerLifecycle
		}
		if kind == "job.completed" && h.completionRepeatsReady(event.JobID, lane) {
			return filteredReasonRepeatsReady
		}
	}
	return ""
}

// fileFilteredInHK writes the record as an hk relay row delivered to
// relayFilteredMachine/`filtered:<reason>`. It reports false when hk refused
// the row or its delivery mark: the caller must then not acknowledge the
// producer, whose retry finds the same row.
func (h *HubServer) fileFilteredInHK(ctx context.Context, kind string, event hubJobEventPayload, machine, reason string) (handoffkeepRelayEvent, bool) {
	stored, _, persisted := h.persistRelayEventRecord(ctx, kind, event, reportRelayRoute{Machine: machine})
	if !persisted || stored.ID < 1 {
		return stored, false
	}
	if stored.DeliveredAt == "" {
		if err := h.handoffkeep.markDelivered(ctx, stored.ID, relayFilteredMachine, filteredDeliveryPrefix+reason); err != nil {
			h.logger.Warn("filtered record was not marked delivered", "kind", kind, "job", event.JobID, "reason", reason, "event_id", stored.ID)
			return stored, false
		}
	}
	h.logger.Info("design lane record kept in hk only", "kind", kind, "lane", event.OwnerLane, "job", event.JobID, "reason", reason, "event_id", stored.ID)
	return stored, true
}

// receiveFiltered keeps a record in hk only and answers the producer as for an
// inbox item: the node's outbox row is acknowledged once hk holds the row.
func (h *HubServer) receiveFiltered(kind string, event *hubJobEventPayload, sender *hubAgent, senderID, reason string) (relayLaneEventResult, bool) {
	stored, ok := h.fileFilteredInHK(h.closeCtx, kind, *event, senderID, reason)
	if !ok {
		return relayLaneEventResult{PersistFailed: true}, true
	}
	if sender != nil {
		if kind == "lane.event" {
			h.queueLaneRelayPersisted(*event, sender, stored.ID)
		} else {
			h.queueRelayPersisted(kind, *event, sender, stored.ID)
		}
	}
	return relayLaneEventResult{ID: stored.ID, Routed: true, Machine: filteredDeliveryPrefix + reason, AlreadyDelivered: true}, true
}

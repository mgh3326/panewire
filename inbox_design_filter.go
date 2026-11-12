package panewire

// Contract: docs/quiet-delivery.md, "Records kept in hk only", and
// docs/hub-spawn-operations.md, "Notifications and telemetry" (hk 4270,
// operator decisions 152 and 153).
//
// A design lane's inbox lists what the design session must read. The
// lifecycle of a reviewer a daemon started (SPAWNED, job.completed,
// job.revoked) needs no one in that lane: the daemon consumes it, and 469 of
// mergewake-design's 500 listed items on 2026-11-12 were of this kind. The hub
// files such a record in hk as a relay row it marks delivered with the reason
// (`filtered:reviewer-lifecycle`) and writes no inbox item, so the lane's list,
// unread count and wake never see it.
//
// A record is filtered only against an exact durable receipt:
//
//   - hk must hold the row (no hk client, or a refused write, files nothing;
//     the producer's outbox row stays unacknowledged and is sent again);
//   - the row hk returns must be this event's own. hk's job.* idempotency key
//     is (kind, job, epoch, report path, reason) and excludes the event id, so
//     a second completion that reuses them gets the first row back. That row
//     does not hold this event's contents, so the event is delivered to the
//     inbox like any other (fileFilteredInHK reports it as not filed).
//
// What the daemon-started test is: the hub's own saved spawn request for the
// job has `--role reviewer` and a job id that starts `mwr-`. The prefix is a
// naming convention, not authenticated provenance: a job the hub did not place,
// or whose record cannot be read, is delivered as before.

import (
	"context"
	"strings"
	"time"
)

const (
	// daemonReviewerJobPrefix starts the job id of every reviewer a daemon
	// (mergewake) starts. node_worktree.go's nodeReviewerDirPattern relies on
	// the same naming.
	daemonReviewerJobPrefix = "mwr-"

	// filteredReasonReviewerLifecycle follows filteredDeliveryPrefix in the
	// delivered row's pane field.
	filteredReasonReviewerLifecycle = "reviewer-lifecycle"
	filteredDeliveryPrefix          = "filtered:"
	// relayFilteredMachine is the machine field of a filtered row, beside
	// relayReplayRetiredMachine and the "sink" and "hub" marks.
	relayFilteredMachine = "hub"
)

// designQuietTimings are the time limits of the design-lane filters. A zero
// value keeps the pre-hk-4270 behavior (no hold), so a hub built without
// NewHubServer behaves as before; NewHubServer sets the default below.
type designQuietTimings struct {
	// spawnFailureHold is how long a retryable reviewer-spawn failure waits
	// for its requester's replay before the design lane is told anyway.
	spawnFailureHold time.Duration
}

func defaultDesignQuietTimings() designQuietTimings {
	return designQuietTimings{spawnFailureHold: 30 * time.Minute}
}

// daemonStartedReviewer reports whether a spawn is a reviewer a daemon started.
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

// designFilterReason names why a record for a design lane is kept in hk only,
// or "" when it is delivered as before. The decision depends only on the
// record and the hub's saved spawn request, so a retry after a restart or a
// lost acknowledgement decides the same way.
func (h *HubServer) designFilterReason(kind string, event hubJobEventPayload, lane string) string {
	if h.handoffkeep == nil || event.JobID == "" || !h.inbox.policy.designLane(lane) {
		return ""
	}
	if kind != "job.completed" && kind != "job.revoked" {
		return ""
	}
	ctx, cancel := context.WithTimeout(h.closeCtx, handoffkeepTimeout)
	defer cancel()
	if h.daemonStartedJob(ctx, event.JobID) {
		return filteredReasonReviewerLifecycle
	}
	return ""
}

// fileFilteredInHK writes the record as an hk relay row delivered to
// relayFilteredMachine/`filtered:<reason>`. filed is true only when hk holds
// this event's own row and it is marked delivered. ok is false when hk
// refused the row or its mark: the caller must then not acknowledge the
// producer, whose retry finds the same row. A row that belongs to another
// event (the idempotency key excludes the event id) is ok but not filed, and
// is left as it is: the caller delivers this event through the inbox.
func (h *HubServer) fileFilteredInHK(ctx context.Context, kind string, event hubJobEventPayload, machine, reason string) (stored handoffkeepRelayEvent, filed, ok bool) {
	stored, _, persisted := h.persistRelayEventRecord(ctx, kind, event, reportRelayRoute{Machine: machine})
	if !persisted || stored.ID < 1 {
		return stored, false, false
	}
	if stored.EventID != event.EventID || stored.Kind != kind || stored.OwnerLane != event.OwnerLane || stored.JobID != event.JobID {
		h.logger.Info("filtered record collides with another event's hk row; delivered to the inbox", "kind", kind, "job", event.JobID, "event_id", event.EventID, "stored_event_id", stored.EventID)
		return stored, false, true
	}
	if stored.DeliveredAt == "" {
		if err := h.handoffkeep.markDelivered(ctx, stored.ID, relayFilteredMachine, filteredDeliveryPrefix+reason); err != nil {
			h.logger.Warn("filtered record was not marked delivered", "kind", kind, "job", event.JobID, "reason", reason, "event_id", stored.ID)
			return stored, false, false
		}
	}
	h.logger.Info("design lane record kept in hk only", "kind", kind, "lane", event.OwnerLane, "job", event.JobID, "reason", reason, "event_id", stored.ID)
	return stored, true, true
}

// receiveFiltered keeps a record in hk only and answers the producer as for an
// inbox item: the node's outbox row is acknowledged once hk holds the row.
// handled is false when the record must take the ordinary inbox path instead.
func (h *HubServer) receiveFiltered(kind string, event *hubJobEventPayload, sender *hubAgent, senderID, reason string) (result relayLaneEventResult, handled bool) {
	stored, filed, ok := h.fileFilteredInHK(h.closeCtx, kind, *event, senderID, reason)
	if !ok {
		return relayLaneEventResult{PersistFailed: true}, true
	}
	if !filed {
		return relayLaneEventResult{}, false
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

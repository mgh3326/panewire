package panewire

// Contract: docs/hub-spawn-operations.md, "Owner notices by cause" (hk 5360,
// operator decision 433), and the root-cause packet
// handoff/2026-11-19/panewire-design-hk5360-root-cause.
//
// Invariant: a notice's delivery, settle or count decision is made once, from
// the inputs read at its first routing, and is recorded in
// owner_notice_decisions before its effect. A later routing of the same notice
// applies the recorded decision and reads nothing else. Counts and the repeat
// window are read from that table, so a hub restart keeps both.
//
// A spawn failure the requester replays by itself (replay=yes) is not sent to a
// design owner. It is counted by cause (decision count). The owner hears it only
// when the request is given up or cannot be replayed (replay=no), the same cause
// repeats for the owner (decision repeat, the third within the window), or the
// cause is on relay.spawn_notice_must_fix. --owner-notify final|never overrides
// those rules for the owner's copy only; the requester's own copy is never
// silenced, because it is what makes the requester replay.

import (
	"context"
	"regexp"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// Decisions recorded in owner_notice_decisions.decision.
const (
	ownerDecisionDeliver = "deliver"
	ownerDecisionFile    = "file"
	ownerDecisionCount   = "count"
	ownerDecisionRepeat  = "repeat"
	ownerDecisionSettle  = "settle"
)

// spawnNoticeRepeatWindow and spawnNoticeRepeatCount: once spawnNoticeRepeatCount
// occurrences of a cause for one owner fall within the window (counted since
// the last repeat), the next one is a repeat that notifies, and the window
// starts over at it.
const (
	spawnNoticeRepeatWindow = 30 * time.Minute
	spawnNoticeRepeatCount  = 3
)

// defaultSpawnNoticeMustFix is the built-in must-fix cause list: causes a
// person has to repair, never silenced for repetition or replay. The policy
// file's relay.spawn_notice_must_fix replaces it.
var defaultSpawnNoticeMustFix = []string{"workspace_unresolved", "auth_unavailable", "report_path", "brief_upload", "identity_unproven"}

// spawnNoticeCauseShape is a cause key as spawnNoticeCause produces it: a
// refusal code or a notice's own cause word, optionally with its detail after
// one colon (checkout_refused:branch-elsewhere).
var spawnNoticeCauseShape = regexp.MustCompile(`^[a-z0-9_]{1,40}(:[a-z0-9_-]{1,40})?$`)

func validSpawnNoticeCause(cause string) bool {
	return spawnNoticeCauseShape.MatchString(cause)
}

// spawnNoticeCause is the cause key of a failure's reason: the refusal code
// after spawn_failed:, or the bare reason (timeout, checkout_refused:locked).
func spawnNoticeCause(reason string) string {
	code, _, _ := strings.Cut(reason, ": ")
	return strings.TrimPrefix(code, "spawn_failed:")
}

// spawnNoticeOutcomeCause is the cause key an outcome is counted under.
func spawnNoticeOutcomeCause(result hubSpawnOutcome) string {
	if result.RC == 0 {
		if result.LaneUnregistered {
			return "lane_unregistered"
		}
		return "spawned"
	}
	return spawnNoticeCause(result.Reason)
}

// spawnNoticeStatus is the status an outbox row's event id names
// (<request>:<attempt>:<status>[:<lane>]). A row is judged by its own status,
// not the request's current one.
func spawnNoticeStatus(id string) string {
	parts := strings.SplitN(id, ":", 4)
	if len(parts) < 3 {
		return ""
	}
	return parts[2]
}

// ownerNotifyMode is the spawn's --owner-notify mode from its saved request:
// "final", "never", or "" when the flag is absent or not a valid value.
func ownerNotifyMode(args []string) string {
	mode := hubSpawnArg(args, fleet.OwnerNotifyArg)
	if !fleet.ValidOwnerNotify(mode) {
		return ""
	}
	return mode
}

// hasDistinctRequester reports whether the spawn names a submitting lane other
// than its owner. Only then does the requester hold its own copy of a failure.
func hasDistinctRequester(task hubSpawnTask) bool {
	requester := spawnSubmitterLane(task.Request.Args)
	return requester != "" && requester != task.Owner
}

// spawnNoticeSuperseded reports whether a retryable failure row has been
// overtaken: the request moved to a later attempt, or its current attempt ended
// with another outcome. Such a row is settled with no notice and no count.
func spawnNoticeSuperseded(n spawnNotification, task hubSpawnTask) bool {
	return spawnNoticeStatus(n.id) == "retryable" && (task.Attempt > spawnNoticeAttempt(n.id) || task.Status != "retryable")
}

// spawnNoticeMustFix reports whether a cause is on the policy's must-fix list.
func (p *deliveryPolicy) spawnNoticeMustFix(cause string) bool {
	for _, must := range p.Relay.SpawnNoticeMustFix {
		if must == cause {
			return true
		}
	}
	return false
}

// decideSpawnNotice returns the action for an outbox row. The first routing
// decides from its inputs and records the decision; a later routing applies the
// recorded decision. A row whose decision cannot be read or recorded is left
// pending: nothing is sent or counted without its record.
func (h *HubServer) decideSpawnNotice(ctx context.Context, n spawnNotification, result hubSpawnOutcome) spawnNoticeAction {
	store := h.inbox.store
	decision, found, err := store.ownerNoticeDecision(ctx, n.id)
	if err != nil {
		return spawnNoticeLater
	}
	if found {
		return spawnNoticeActionFor(decision)
	}
	decision, ok := h.computeSpawnNoticeDecision(ctx, n, result)
	if !ok {
		return spawnNoticeLater
	}
	recorded, err := store.recordOwnerNoticeDecision(ctx, n.id, n.owner, spawnNoticeOutcomeCause(result), decision, h.now())
	if err != nil {
		h.logger.Warn("owner notice decision not recorded; retried", "event_id", n.id, "err", err)
		return spawnNoticeLater
	}
	return spawnNoticeActionFor(recorded)
}

// spawnNoticeActionFor maps a recorded decision to its effect.
func spawnNoticeActionFor(decision string) spawnNoticeAction {
	switch decision {
	case ownerDecisionFile:
		return spawnNoticeFile
	case ownerDecisionCount:
		return spawnNoticeCount
	case ownerDecisionSettle:
		return spawnNoticeSettle
	}
	return spawnNoticeDeliver // deliver and repeat both send
}

// computeSpawnNoticeDecision reads a first routing's inputs and returns its
// decision. ok is false when an input cannot be read yet.
func (h *HubServer) computeSpawnNoticeDecision(ctx context.Context, n spawnNotification, result hubSpawnOutcome) (decision string, ok bool) {
	task, err := h.inbox.store.spawnTask(ctx, n.request)
	if err != nil {
		return "", false
	}
	if mode := ownerNotifyMode(task.Request.Args); mode != "" {
		if n.owner != task.Owner {
			return ownerDecisionDeliver, true // the requester's own copy reaches it at once
		}
		return h.ownerModeDecision(n, task, result, mode), true
	}
	if !h.inbox.policy.designLane(n.owner) {
		return ownerDecisionDeliver, true
	}
	if result.RC != 0 {
		if n.owner != task.Owner {
			return ownerDecisionDeliver, true // the requester replays from its own failure copy
		}
		return h.failureDecision(ctx, n, task, result)
	}
	if !daemonStartedReviewer(task.Job, task.Request.Args) {
		return ownerDecisionDeliver, true
	}
	if result.LaneUnregistered {
		return ownerDecisionDeliver, true // the pane's findings are not routed: someone must act
	}
	return ownerDecisionFile, true
}

// failureDecision applies the cause rules to a design owner's failure when the
// spawn names no --owner-notify mode.
func (h *HubServer) failureDecision(ctx context.Context, n spawnNotification, task hubSpawnTask, result hubSpawnOutcome) (string, bool) {
	cause := spawnNoticeCause(result.Reason)
	switch {
	case spawnNoticeSuperseded(n, task):
		return ownerDecisionSettle, true
	case !result.Retryable:
		return ownerDecisionDeliver, true // replay=no: nothing the requester can replay
	case spawnNoticeStatus(n.id) == "given_up":
		return ownerDecisionDeliver, true
	case n.owner == task.Owner && !hasDistinctRequester(task):
		return ownerDecisionDeliver, true // the owner's copy is the only notice it can act on
	case h.inbox.policy.spawnNoticeMustFix(cause):
		return ownerDecisionDeliver, true
	}
	due, err := h.inbox.store.ownerNoticeRepeatDue(ctx, n.owner, cause, h.now())
	if err != nil {
		return "", false
	}
	if due {
		return ownerDecisionRepeat, true
	}
	return ownerDecisionCount, true
}

// ownerModeDecision decides a notice for the owner lane of a spawn that names
// --owner-notify. The requester's copy is decided by the caller, not here.
func (h *HubServer) ownerModeDecision(n spawnNotification, task hubSpawnTask, result hubSpawnOutcome, mode string) string {
	if result.RC == 0 {
		// SPAWNED and LANE_UNREGISTERED are not terminal: neither mode sends them.
		return ownerDecisionCount
	}
	if spawnNoticeSuperseded(n, task) {
		return ownerDecisionSettle
	}
	if mode == fleet.OwnerNotifyNever {
		return ownerDecisionCount
	}
	// final: only a failure that cannot be replayed, or a given-up request.
	if !result.Retryable || spawnNoticeStatus(n.id) == "given_up" {
		return ownerDecisionDeliver
	}
	return ownerDecisionCount
}

// ownerNotifyJobSilenced reports whether a job event for the owner lane of a
// spawn that names --owner-notify is held back. Only job.completed is covered:
// a completion is not a terminal notice, so neither mode sends it. An event
// without an id cannot be recorded under its own key, so it fails open: it is
// delivered as before, and not silently dropped (mergewake review of PR 732).
func (h *HubServer) ownerNotifyJobSilenced(ctx context.Context, kind string, event hubJobEventPayload, lane string) bool {
	if kind != "job.completed" || event.EventID == "" {
		return false
	}
	return h.ownerNotifyWithheld(ctx, event.JobID, lane)
}

// ownerNotifyWithheld reports whether the spawn that started job names an
// --owner-notify mode and owner is that spawn's owner lane. An unreadable spawn
// record withholds nothing: the notice takes the ordinary path.
func (h *HubServer) ownerNotifyWithheld(ctx context.Context, job, owner string) bool {
	return h.ownerNotifyModeFor(ctx, job, owner) != ""
}

// ownerNotifyModeFor is the --owner-notify mode of the spawn that started job,
// when owner is that spawn's owner lane; "" otherwise (hk 5454 reads it for
// every owner info notice, and ownerNotifyWithheld for a completion).
func (h *HubServer) ownerNotifyModeFor(ctx context.Context, job, owner string) string {
	if h.inbox == nil || job == "" || owner == "" {
		return ""
	}
	task, err := h.inbox.store.latestSpawnForJob(ctx, job)
	if err != nil || owner != task.Owner {
		return ""
	}
	return ownerNotifyMode(task.Request.Args)
}

// ownerNotifyRecordReason is the reason a withheld completion is filed under.
// hk's idempotency key is (kind, job, epoch, report path, reason) and excludes
// the event id, so the event id is part of the reason: a second completion of
// the same job gets its own row.
func ownerNotifyRecordReason(eventID string) string {
	return "owner-notify:" + eventID
}

// receiveOwnerNotifySilenced files a withheld owner event as an hk row under its
// own reason, then records its count decision under key, grouped by cause. The
// producer is acknowledged only after both. An event hk does not file under
// this event is neither acknowledged nor counted, so the producer's retry finds
// it again. handled is false only when no hk client is configured, so the event
// takes the ordinary path (nothing is recorded, so nothing is silenced).
func (h *HubServer) receiveOwnerNotifySilenced(kind string, event *hubJobEventPayload, sender *hubAgent, senderID, lane, key, cause string) (result relayLaneEventResult, handled bool) {
	if h.handoffkeep == nil {
		return relayLaneEventResult{}, false
	}
	// hk's idempotency reason is the record's own Reason field, not the delivered
	// label, so the copy filed carries the event id there.
	recorded := *event
	recorded.Reason = ownerNotifyRecordReason(event.EventID)
	stored, filed, ok := h.fileFilteredInHK(h.closeCtx, kind, recorded, senderID, recorded.Reason)
	if !ok || !filed {
		h.logger.Warn("owner-notify record not filed under this event; not acknowledged", "kind", kind, "job", event.JobID, "event_id", event.EventID, "lane", lane)
		return relayLaneEventResult{PersistFailed: true}, true
	}
	if _, err := h.inbox.store.recordOwnerNoticeDecision(h.closeCtx, key, lane, cause, ownerDecisionCount, h.now()); err != nil {
		h.logger.Warn("owner-notify count not recorded; not acknowledged", "job", event.JobID, "event_id", event.EventID, "err", err)
		return relayLaneEventResult{PersistFailed: true}, true
	}
	if sender != nil {
		if kind == "lane.event" {
			h.queueLaneRelayPersisted(*event, sender, stored.ID)
		} else {
			h.queueRelayPersisted(kind, *event, sender, stored.ID)
		}
	}
	return relayLaneEventResult{ID: stored.ID, Routed: true, Machine: filteredDeliveryPrefix + recorded.Reason, AlreadyDelivered: true}, true
}

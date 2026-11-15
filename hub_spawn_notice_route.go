package panewire

// Contract: docs/hub-spawn-operations.md, "Notifications and telemetry", and
// docs/quiet-delivery.md, "Reviewer spawn failures" (hk 4270, decision 152).
//
// The outbox rows of spawn_notifications (hub_spawn_task_store.go) become lane
// events here. For a reviewer a daemon started, when the owner is a design
// lane:
//
//   - SPAWNED is a lifecycle record: kept in hk only (inbox_design_filter.go).
//   - A failure the saved request can still be replayed after (status
//     retryable) is held for the requester when one other than the owner
//     submitted it, which replays it: the requester's
//     own copy (`--requested-by`, else `--parent`) is delivered at once and
//     stays an unread item in its inbox. The design lane hears nothing for
//     the attempt unless the request is still unreplayed after
//     designQuietTimings.spawnFailureHold, so a requester that gave up cannot
//     leave a failure unreported.
//   - A later attempt supersedes the held row; the request's last outcome (a
//     terminal failure, or the SPAWNED that ends it) is what the design lane
//     is told, once.
//
// Every row stays in spawn_notifications and the operation record, which the
// status API reads; a superseded row is closed with delivered=2 → 1 and its
// text is kept in hk by the requester's copy.
//
// delivered=0 rows are not yet routed, delivered=2 are held for their
// requester, delivered=3 are being published at expiry, delivered=1 are done
// (delivered, filed in hk, or superseded).

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

const (
	spawnNoticePending = 0
	spawnNoticeDone    = 1
	spawnNoticeHeld    = 2
	// spawnNoticePublishing marks a held row whose expiry won the claim against
	// a replay of its request; a crash before it settles leaves it here and the
	// next flush publishes it again (the event id makes that idempotent).
	spawnNoticePublishing = 3
)

type spawnNotification struct {
	id, request, job, owner, raw string
	at                           int64
}

type spawnNoticeAction int

const (
	spawnNoticeDeliver spawnNoticeAction = iota
	spawnNoticeFile
	spawnNoticeHold
)

func (h *HubServer) flushSpawnNotifications(ctx context.Context) {
	if h.inbox == nil || h.handoffkeep == nil {
		return
	}
	for _, n := range h.readSpawnNotifications(ctx, spawnNoticePending) {
		h.routeSpawnNotification(ctx, n)
	}
	h.flushHeldSpawnNotifications(ctx)
}

func (h *HubServer) readSpawnNotifications(ctx context.Context, state int) []spawnNotification {
	store := h.inbox.store
	op, done := store.op(ctx)
	defer done()
	rows, err := store.db.QueryContext(op, store.q(`SELECT event_id,request_id,job,owner,result,created_ms FROM spawn_notifications WHERE delivered=? ORDER BY created_ms LIMIT 32`), state)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var batch []spawnNotification
	for rows.Next() {
		var n spawnNotification
		if rows.Scan(&n.id, &n.request, &n.job, &n.owner, &n.raw, &n.at) != nil {
			break
		}
		batch = append(batch, n)
	}
	return batch
}

func (h *HubServer) setSpawnNotificationState(ctx context.Context, id string, from, to int) {
	store := h.inbox.store
	op, done := store.op(ctx)
	defer done()
	_, _ = store.db.ExecContext(op, store.q(`UPDATE spawn_notifications SET delivered=? WHERE event_id=? AND delivered=?`), to, id, from)
}

func spawnNotificationEvent(n spawnNotification, result hubSpawnOutcome) hubJobEventPayload {
	if result.Stage == "placement_record" && result.Reason == legacyPlacementRecordConflict {
		return hubJobEventPayload{EventID: n.id, JobID: n.job, OwnerLane: n.owner, NotifyType: "automation-failed", Text: "[panewire] PLACEMENT_RECORD_HELD " + n.job + " request=" + n.request + " reason=" + legacyPlacementRecordConflict + " fix=Inspect the existing hk placement record and the retained hub trace before recovery.", Host: "hub", Epoch: 1}
	}
	return hubJobEventPayload{EventID: n.id, JobID: n.job, OwnerLane: n.owner, NotifyType: spawnNotificationType(result), Text: spawnNotificationText(n.job, n.request, result), Host: "hub", Epoch: 1}
}

// spawnNoticeAttempt is the attempt a notification's event id names:
// `<request>:<attempt>:<status>[:<lane>]`.
func spawnNoticeAttempt(id string) int {
	parts := strings.SplitN(id, ":", 4)
	if len(parts) < 3 {
		return 0
	}
	attempt, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0
	}
	return attempt
}

// spawnNoticeFor decides what a pending notification does. Anything the hub
// cannot place (no spawn record, an unreadable store) is delivered.
func (h *HubServer) spawnNoticeFor(ctx context.Context, n spawnNotification, result hubSpawnOutcome) spawnNoticeAction {
	if !h.inbox.policy.designLane(n.owner) {
		return spawnNoticeDeliver
	}
	task, err := h.inbox.store.spawnTask(ctx, n.request)
	if err != nil || !daemonStartedReviewer(task.Job, task.Request.Args) {
		return spawnNoticeDeliver
	}
	switch {
	case result.RC == 0:
		if result.LaneUnregistered {
			return spawnNoticeDeliver // the pane's findings are not routed: someone must act
		}
		return spawnNoticeFile
	case n.owner != task.Owner:
		return spawnNoticeDeliver // the requester's own copy reaches it at once
	case result.Retryable && h.quietTimings.spawnFailureHold > 0 && spawnSubmitterLane(task.Request.Args) != "" && spawnSubmitterLane(task.Request.Args) != task.Owner:
		// Held only for a requester that has its own copy (completeSpawn
		// writes one when the submitting lane differs from the owner). With
		// none, the owner row is the only notice the requester can act on.
		return spawnNoticeHold
	}
	return spawnNoticeDeliver
}

func (h *HubServer) routeSpawnNotification(ctx context.Context, n spawnNotification) {
	var result hubSpawnOutcome
	if json.Unmarshal([]byte(n.raw), &result) != nil {
		return
	}
	event := spawnNotificationEvent(n, result)
	switch h.spawnNoticeFor(ctx, n, result) {
	case spawnNoticeFile:
		switch _, filed, ok := h.fileFilteredInHK(ctx, "lane.event", event, event.Host, filteredReasonReviewerLifecycle); {
		case ok && filed:
			h.setSpawnNotificationState(ctx, n.id, spawnNoticePending, spawnNoticeDone)
		case ok:
			// hk's row is another event's: deliver this one.
			if h.relayLaneEventContext(ctx, event, nil).ID != 0 {
				h.setSpawnNotificationState(ctx, n.id, spawnNoticePending, spawnNoticeDone)
			}
		}
	case spawnNoticeHold:
		h.logger.Info("reviewer spawn failure held for its requester", "request", n.request, "job", n.job, "event_id", n.id)
		h.setSpawnNotificationState(ctx, n.id, spawnNoticePending, spawnNoticeHeld)
	default:
		// The relay owns the durable write. Persisting here first makes its own
		// POST a 200 duplicate, which it leaves to replay instead of injecting.
		if h.relayLaneEventContext(ctx, event, nil).ID != 0 {
			h.setSpawnNotificationState(ctx, n.id, spawnNoticePending, spawnNoticeDone)
		}
	}
}

// flushHeldSpawnNotifications settles the held failures, oldest first. A held
// row whose request has moved on (a later attempt, or no longer retryable) is
// superseded: the outcome that moved it on is notified on its own. One still
// unreplayed when its hold ends is published, but only after claiming it
// against the request's current state in one statement: a replay that
// advances the attempt first makes the claim fail, so only the notice of the
// request's current attempt is ever published at expiry. Rows are ordered by
// creation and share one hold, so the first row still inside it ends the pass.
// Rows a crash left publishing are published again.
func (h *HubServer) flushHeldSpawnNotifications(ctx context.Context) {
	now := h.now()
	for _, n := range h.readSpawnNotifications(ctx, spawnNoticePublishing) {
		h.publishHeldSpawnNotification(ctx, n)
	}
	for _, n := range h.readSpawnNotifications(ctx, spawnNoticeHeld) {
		task, err := h.inbox.store.spawnTask(ctx, n.request)
		if err != nil {
			continue
		}
		attempt := spawnNoticeAttempt(n.id)
		if task.Attempt > attempt || task.Status != "retryable" {
			h.logger.Info("reviewer spawn failure superseded by a later outcome", "request", n.request, "job", n.job, "event_id", n.id, "attempt", task.Attempt, "status", task.Status)
			h.setSpawnNotificationState(ctx, n.id, spawnNoticeHeld, spawnNoticeDone)
			continue
		}
		if now.Before(time.UnixMilli(n.at).Add(h.quietTimings.spawnFailureHold)) {
			return
		}
		if h.claimHeldSpawnNotification(ctx, n, attempt) {
			h.publishHeldSpawnNotification(ctx, n)
		}
	}
}

// claimHeldSpawnNotification moves a held row to publishing only while its
// request is still at the row's attempt and retryable, in one statement, so a
// concurrent replay (resumeCleanedSpawn) and the expiry cannot both win.
func (h *HubServer) claimHeldSpawnNotification(ctx context.Context, n spawnNotification, attempt int) bool {
	store := h.inbox.store
	op, done := store.op(ctx)
	defer done()
	res, err := store.db.ExecContext(op, store.q(`UPDATE spawn_notifications SET delivered=? WHERE event_id=? AND delivered=? AND EXISTS (SELECT 1 FROM spawn_operations WHERE request_id=? AND attempt=? AND status='retryable')`), spawnNoticePublishing, n.id, spawnNoticeHeld, n.request, attempt)
	if err != nil {
		return false
	}
	rows, _ := res.RowsAffected()
	return rows == 1
}

// publishHeldSpawnNotification delivers a claimed row. A failure returns it to
// held, so the next flush tries again.
func (h *HubServer) publishHeldSpawnNotification(ctx context.Context, n spawnNotification) {
	var result hubSpawnOutcome
	if json.Unmarshal([]byte(n.raw), &result) != nil {
		return
	}
	if h.relayLaneEventContext(ctx, spawnNotificationEvent(n, result), nil).ID != 0 {
		h.setSpawnNotificationState(ctx, n.id, spawnNoticePublishing, spawnNoticeDone)
		return
	}
	h.setSpawnNotificationState(ctx, n.id, spawnNoticePublishing, spawnNoticeHeld)
}

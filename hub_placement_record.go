package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// Queue-core spawns separate placement from dispatch (#473): the App that ran
// the placement and holds its candidate trace is gone before the bound node
// answers, and a hub restart can bury the whole decision. The hub therefore
// persists a placement snapshot per attempt at bind time and produces the
// immutable hk placement record when the attempt reaches a terminal result,
// delivering it through the placement spool's outbox semantics (one record
// per attempt, retried across restarts, permanent refusals quarantined as
// hk 4604 does). The dispatch scheduler (hk 4581) reads the same snapshot by
// request id and attempt through spawnPlacementSnapshot.

// spawnAttemptTerminal reports whether an operation status ends the attempt:
// the node answered (succeeded, failed) or the attempt ended so another may
// begin (retryable). reconciling and deadline_exceeded still accept a later
// node answer for the same attempt.
func spawnAttemptTerminal(status string) bool {
	return status == "succeeded" || status == "failed" || status == "retryable"
}

// placementRecordDeliveryBudget bounds one outbox delivery; the record stays
// spooled and the next flush retries it.
const placementRecordDeliveryBudget = 2 * time.Second

// saveSpawnPlacementSnapshot persists the latest placement trace for an
// attempt. A row whose record was already produced ignores later writes, so
// a stale App emission can never rewrite a delivered record's source.
func (s *laneInboxStore) saveSpawnPlacementSnapshot(parent context.Context, requestID string, attempt int, payload []byte) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`INSERT INTO spawn_placement_snapshots(request_id,attempt,payload) VALUES(?,?,?) ON CONFLICT(request_id,attempt) DO UPDATE SET payload=excluded.payload WHERE spawn_placement_snapshots.recorded=0`), requestID, attempt, string(payload))
	return err
}

// spawnPlacementSnapshot reads the persisted placement trace of one attempt
// and whether its record was already produced. The dispatch scheduler
// (hk 4581) looks attempts up by request id and attempt through this method.
func (s *laneInboxStore) spawnPlacementSnapshot(parent context.Context, requestID string, attempt int) (payload []byte, recorded bool, err error) {
	ctx, done := s.op(parent)
	defer done()
	var raw string
	var marked int
	err = s.db.QueryRowContext(ctx, s.q(`SELECT payload,recorded FROM spawn_placement_snapshots WHERE request_id=? AND attempt=?`), requestID, attempt).Scan(&raw, &marked)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	return []byte(raw), marked == 1, err
}

// markSpawnPlacementRecorded seals an attempt's snapshot after its record is
// delivered or quarantined. A spooled-but-undelivered record keeps the row
// pending so the flush keeps retrying it.
func (s *laneInboxStore) markSpawnPlacementRecorded(parent context.Context, requestID string, attempt int) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_placement_snapshots SET recorded=1 WHERE request_id=? AND attempt=?`), requestID, attempt)
	return err
}

type pendingPlacementSnapshot struct {
	RequestID      string
	Attempt        int
	Job            string
	Status         string
	CurrentAttempt int
}

// pendingPlacementSnapshots lists the attempts whose records are not yet
// sealed: awaiting a terminal result, or spooled and undelivered.
func (s *laneInboxStore) pendingPlacementSnapshots(parent context.Context) ([]pendingPlacementSnapshot, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, `SELECT p.request_id,p.attempt,o.job,o.status,o.attempt FROM spawn_placement_snapshots p JOIN spawn_operations o ON o.request_id=p.request_id WHERE p.recorded=0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []pendingPlacementSnapshot
	for rows.Next() {
		var row pendingPlacementSnapshot
		if err := rows.Scan(&row.RequestID, &row.Attempt, &row.Job, &row.Status, &row.CurrentAttempt); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// attemptOver reports whether this attempt can no longer produce a record:
// its operation reached a terminal status or a newer attempt already began.
func (p pendingPlacementSnapshot) attemptOver() bool {
	return spawnAttemptTerminal(p.Status) || p.CurrentAttempt > p.Attempt
}

// notePlacementTrace persists the latest placement trace a hub spawn's App
// emits. When the attempt's terminal result is already recorded — the App's
// final emission follows the node's answer — it delivers from this snapshot.
func (h *HubServer) notePlacementTrace(task hubSpawnTask, tr fleet.PlacementTrace) {
	if h.inbox == nil || task.Request.RequestID == "" {
		return
	}
	payload, err := json.Marshal(tr)
	if err != nil {
		return
	}
	if err := h.inbox.store.saveSpawnPlacementSnapshot(h.closeCtx, task.Request.RequestID, task.Attempt, payload); err != nil {
		h.logger.Warn("placement snapshot could not be persisted", "request_id", task.Request.RequestID, "job", task.Job, "attempt", task.Attempt)
		return
	}
	current, err := h.inbox.store.spawnTask(h.closeCtx, task.Request.RequestID)
	if err != nil || current.Attempt != task.Attempt || !spawnAttemptTerminal(current.Status) {
		return
	}
	h.deliverPlacementRecord(h.closeCtx, task.Request.RequestID, task.Attempt)
}

// deliverPlacementRecord builds the attempt's hk placement record from its
// persisted snapshot and the terminal outcome, and enqueues it exactly once.
// A missing snapshot means placement never produced a trace (a refusal
// before the plan ranked hosts), so there is nothing to record.
func (h *HubServer) deliverPlacementRecord(ctx context.Context, requestID string, attempt int) {
	if h.inbox == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, placementRecordDeliveryBudget)
	defer cancel()
	payload, recorded, err := h.inbox.store.spawnPlacementSnapshot(ctx, requestID, attempt)
	if err != nil {
		h.logger.Warn("placement snapshot could not be read", "request_id", requestID, "attempt", attempt)
		return
	}
	if recorded || len(payload) == 0 {
		return
	}
	var tr fleet.PlacementTrace
	if err := json.Unmarshal(payload, &tr); err != nil {
		h.logger.Warn("placement snapshot could not be decoded", "request_id", requestID, "attempt", attempt)
		return
	}
	task, err := h.inbox.store.spawnTask(ctx, requestID)
	if err != nil || task.Attempt != attempt {
		return
	}
	app, err := h.placementRecordApp()
	if err != nil {
		h.logger.Warn("placement record delivery unavailable; snapshot remains pending", "request_id", requestID, "attempt", attempt)
		return
	}
	delivered, err := app.EnqueuePlacementRecord(ctx, hubPlacementRecord(task, tr))
	if err != nil {
		h.logger.Warn("placement record could not be enqueued; snapshot remains pending", "request_id", requestID, "attempt", attempt, "error", err)
		return
	}
	if !delivered {
		// hk did not take it yet; the pending row keeps the flush retrying.
		return
	}
	if err := h.inbox.store.markSpawnPlacementRecorded(ctx, requestID, attempt); err != nil {
		h.logger.Warn("placement snapshot could not be marked recorded", "request_id", requestID, "attempt", attempt)
	}
}

// hubPlacementRecord combines the persisted trace with the attempt's
// terminal outcome into the immutable hk record. A trace that already
// carries an outcome is the record finishPlacement completed in the App; it
// is posted as built. Otherwise the verdict comes from the outcome: a landed
// job is placed, a refusal before the node's hk job claim is refused, and
// anything past it failed.
func hubPlacementRecord(task hubSpawnTask, tr fleet.PlacementTrace) fleet.PlacementRecord {
	o := task.Outcome
	rec := fleet.PlacementRecord{
		JobID: task.Job, Attempt: task.Attempt, TaskID: tr.TaskID,
		DecidedAt: tr.DecidedAt, ToolVersion: fleet.BuildVersion,
		Source: tr.Source, Ranking: tr.Ranking, StateSource: tr.StateSource,
		Outcome: tr.Outcome, Reason: tr.Reason,
		Candidates: tr.Candidates,
		PlacementExecution: fleet.PlacementExecution{
			Host:       firstNonEmptyString(o.Host, tr.Chosen),
			PaneID:     o.Pane,
			Profile:    firstNonEmptyString(o.Profile, tr.Profile),
			Model:      o.Model,
			Pool:       firstNonEmptyString(o.Pool, tr.Pool),
			AccountKey: firstNonEmptyString(o.AccountKey, tr.AccountKey),
			Role:       firstNonEmptyString(o.Role, tr.Role),
			Family:     tr.Family,
			Repo:       tr.Repo,
		},
	}
	if rec.Outcome != "" {
		return rec
	}
	switch {
	case o.RC == 0:
		rec.Outcome, rec.Reason = "placed", tr.StateReason
	case o.Pane != "" || !spawnStageBeforeJobClaim(o.Stage):
		rec.Outcome, rec.Reason = "failed", o.Reason
	default:
		rec.Outcome, rec.Reason = "refused", o.Reason
	}
	// The host the attempt bound is its candidate: mark it in the trace, or
	// add the contacted-host row a pre-plan trace never listed.
	marked := false
	for i := range rec.Candidates {
		if rec.Candidates[i].Host != rec.Host || rec.Host == "" {
			continue
		}
		marked = true
		if rec.Outcome == "placed" {
			rec.Candidates[i].Outcome, rec.Candidates[i].Reason = "chosen", "available"
		} else {
			rec.Candidates[i].Outcome, rec.Candidates[i].Reason = "refused", rec.Reason
		}
	}
	if !marked && rec.Host != "" {
		outcome, reason := "refused", rec.Reason
		if rec.Outcome == "placed" {
			outcome, reason = "chosen", "available"
		}
		rec.Candidates = append(rec.Candidates, fleet.PlacementCandidateRecord{Host: rec.Host, Outcome: outcome, Reason: reason})
	}
	if rec.Candidates == nil {
		rec.Candidates = []fleet.PlacementCandidateRecord{}
	}
	// Contact is not launch acceptance: a refused record names no chosen
	// host, matching the records finishPlacement produces.
	if rec.Outcome == "refused" {
		rec.Host = ""
	}
	return rec
}

// placementRecordApp returns the fleet App whose state directory holds the
// placement spool and whose hk client delivers the records. It is created on
// first use; a hub restart builds a fresh one.
func (h *HubServer) placementRecordApp() (*fleet.App, error) {
	h.placementAppMu.Lock()
	defer h.placementAppMu.Unlock()
	if h.placementRecordClient != nil {
		return h.placementRecordClient, nil
	}
	build := h.placementAppFn
	if build == nil {
		build = func() (*fleet.App, error) { return fleet.New(io.Discard, io.Discard) }
	}
	app, err := build()
	if err != nil {
		return nil, err
	}
	h.placementRecordClient = app
	return app, nil
}

// pendingPlacementRecords reports whether any attempt's snapshot still waits
// for its record. The flush checks this cheap row before paying for the
// delivery App.
func (h *HubServer) pendingPlacementRecords(ctx context.Context) bool {
	if h.inbox == nil {
		return false
	}
	var one int
	err := h.inbox.store.db.QueryRowContext(ctx, `SELECT 1 FROM spawn_placement_snapshots WHERE recorded=0 LIMIT 1`).Scan(&one)
	return err == nil
}

// flushPlacementRecords retries records still spooled from before a restart,
// on the same coordinator tick as spawn notifications. Without a pending
// snapshot there is nothing the outbox owes, so no delivery App is built.
func (h *HubServer) flushPlacementRecords(ctx context.Context) {
	if !h.pendingPlacementRecords(ctx) {
		return
	}
	app, err := h.placementRecordApp()
	if err != nil {
		return
	}
	app.FlushPlacements()
	// Reconcile pending snapshots against the spool. A file still present
	// keeps its row pending so the next tick retries delivery; a live attempt
	// has no file yet and must never be sealed. A terminal attempt with no
	// file either never enqueued (a crash between the terminal commit and the
	// enqueue, a delivery-App build failure, an enqueue error) or delivered
	// before its seal could be written: rebuild and enqueue the identical
	// record — hk dedupes the repost — and let its own mark seal it. Only an
	// attempt that is over but owes no rebuildable record (superseded, its
	// outcome already overwritten by a newer attempt) is sealed here.
	pending, err := h.inbox.store.pendingPlacementSnapshots(h.closeCtx)
	if err != nil {
		return
	}
	for _, row := range pending {
		if !row.attemptOver() || app.PlacementSpooled(row.Job, row.Attempt) {
			continue
		}
		if spawnAttemptTerminal(row.Status) && row.CurrentAttempt == row.Attempt {
			h.deliverPlacementRecord(h.closeCtx, row.RequestID, row.Attempt)
			continue
		}
		_ = h.inbox.store.markSpawnPlacementRecorded(h.closeCtx, row.RequestID, row.Attempt)
	}
}

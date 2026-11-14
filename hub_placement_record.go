package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/fleetevent"
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

// recorded is 0 for a mutable snapshot, 1 after delivery, and 2 when a
// completed attempt is frozen for delivery after the operation advances.
const placementSnapshotFrozen = 2

// saveSpawnPlacementSnapshot persists the latest placement trace for an
// attempt. A row whose record was already produced ignores later writes, so
// a stale App emission can never rewrite a delivered record's source.
func (s *laneInboxStore) saveSpawnPlacementSnapshot(parent context.Context, requestID string, attempt int, payload []byte, delivery string) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`INSERT INTO spawn_placement_snapshots(request_id,attempt,payload,delivery) VALUES(?,?,?,?) ON CONFLICT(request_id,attempt) DO UPDATE SET payload=excluded.payload,delivery=excluded.delivery WHERE spawn_placement_snapshots.recorded=0`), requestID, attempt, string(payload), delivery)
	return err
}

// spawnPlacementSnapshot reads the persisted placement trace of one attempt,
// its placing App's delivery settings, and whether its record was already
// produced. The dispatch scheduler (hk 4581) looks attempts up by request id
// and attempt through this method.
func (s *laneInboxStore) spawnPlacementSnapshot(parent context.Context, requestID string, attempt int) (payload []byte, delivery string, recorded bool, err error) {
	ctx, done := s.op(parent)
	defer done()
	var raw string
	var marked int
	err = s.db.QueryRowContext(ctx, s.q(`SELECT payload,delivery,recorded FROM spawn_placement_snapshots WHERE request_id=? AND attempt=?`), requestID, attempt).Scan(&raw, &delivery, &marked)
	if err == sql.ErrNoRows {
		return nil, "", false, nil
	}
	return []byte(raw), delivery, marked == 1, err
}

// continueSpawnPlacementTrace carries the original decision and its candidates
// into a later placement pass. A claimed-host retry is forced internally, but
// the request's original auto decision must not become a forced placement in hk.
func continueSpawnPlacementTrace(previous, current fleet.PlacementTrace, exclusions []hubSpawnExclusion) fleet.PlacementTrace {
	if previous.Source == "" {
		return current
	}
	current.DecidedAt, current.Source, current.Ranking = previous.DecidedAt, previous.Source, previous.Ranking
	current.StateSource, current.StateReason = previous.StateSource, previous.StateReason
	if current.Repo == "" {
		current.Repo = previous.Repo
	}
	candidates := append([]fleet.PlacementCandidateRecord(nil), previous.Candidates...)
	positions := make(map[string]int, len(candidates))
	for i, candidate := range candidates {
		positions[candidate.Host] = i
	}
	for _, candidate := range current.Candidates {
		if i, found := positions[candidate.Host]; found {
			candidates[i] = candidate
		} else {
			positions[candidate.Host] = len(candidates)
			candidates = append(candidates, candidate)
		}
	}
	for _, excluded := range exclusions {
		if excluded.Machine == current.Chosen {
			continue
		}
		i, found := positions[excluded.Machine]
		if !found {
			i = len(candidates)
			positions[excluded.Machine] = i
			candidates = append(candidates, fleet.PlacementCandidateRecord{Host: excluded.Machine})
		}
		reason := "hub_" + strings.TrimPrefix(excluded.Reason, "spawn_failed:")
		candidates[i].Outcome = "skipped"
		candidates[i].Reason = fleetevent.Code(fleetevent.Redact(reason))
	}
	current.Candidates = candidates
	if current.Record != nil {
		record := *current.Record
		record.DecidedAt, record.Source, record.Ranking = current.DecidedAt, current.Source, current.Ranking
		record.StateSource, record.Candidates = current.StateSource, current.Candidates
		current.Record = &record
	}
	return current
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

// freezeSpawnPlacementForRetry keeps a completed attempt self-contained before
// its operation row is reused. The caller advances the attempt in this same
// transaction, so recovery can always deliver the old record afterwards.
func (s *laneInboxStore) freezeSpawnPlacementForRetry(ctx context.Context, tx *sql.Tx, task hubSpawnTask) error {
	var raw string
	err := tx.QueryRowContext(ctx, s.q(`SELECT payload FROM spawn_placement_snapshots WHERE request_id=? AND attempt=? AND recorded=0`), task.Request.RequestID, task.Attempt).Scan(&raw)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	var trace fleet.PlacementTrace
	if err := json.Unmarshal([]byte(raw), &trace); err != nil {
		return err
	}
	if trace.Record == nil {
		record := hubPlacementRecord(task, trace, task.Attempt)
		trace.Record, trace.Outcome, trace.Reason = &record, record.Outcome, record.Reason
	}
	payload, err := json.Marshal(trace)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, s.q(`UPDATE spawn_placement_snapshots SET payload=?,recorded=? WHERE request_id=? AND attempt=? AND recorded=0`), string(payload), placementSnapshotFrozen, task.Request.RequestID, task.Attempt)
	return err
}

type pendingPlacementSnapshot struct {
	RequestID      string
	Attempt        int
	Job            string
	Status         string
	CurrentAttempt int
	Payload        []byte
	Delivery       string
}

// pendingPlacementSnapshots lists the attempts whose records are not yet
// sealed: awaiting a terminal result, or spooled and undelivered.
func (s *laneInboxStore) pendingPlacementSnapshots(parent context.Context) ([]pendingPlacementSnapshot, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, `SELECT p.request_id,p.attempt,o.job,o.status,o.attempt,p.payload,p.delivery FROM spawn_placement_snapshots p JOIN spawn_operations o ON o.request_id=p.request_id WHERE p.recorded<>1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []pendingPlacementSnapshot
	for rows.Next() {
		var row pendingPlacementSnapshot
		if err := rows.Scan(&row.RequestID, &row.Attempt, &row.Job, &row.Status, &row.CurrentAttempt, &row.Payload, &row.Delivery); err != nil {
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
// emits, together with the App's delivery settings so the record posts the
// way this attempt's own App would have. When the attempt's terminal result
// is already recorded — the App's final emission follows the node's answer —
// it delivers from this snapshot.
func (h *HubServer) notePlacementTrace(task hubSpawnTask, tr fleet.PlacementTrace, delivery string) error {
	if h.inbox == nil || task.Request.RequestID == "" {
		return errors.New("placement snapshot store unavailable")
	}
	// A quota refresh pauses this same attempt. Keep the candidate trace,
	// but let its eventual terminal result determine the immutable record.
	if tr.Reason == "quota_refresh_pending" || tr.Record != nil && tr.Record.Reason == "quota_refresh_pending" {
		tr.Record, tr.Outcome, tr.Reason = nil, "", ""
	}
	payload, err := json.Marshal(tr)
	if err != nil {
		return err
	}
	if err := h.persistPlacementTrace(task, payload, delivery); err != nil {
		current, readErr := h.inbox.store.spawnTask(h.closeCtx, task.Request.RequestID)
		if tr.Record != nil && readErr == nil && current.Attempt == task.Attempt && spawnAttemptTerminal(current.Status) {
			h.enqueueFinishedPlacementRecord(h.closeCtx, task, *tr.Record, delivery)
		}
		return err
	}
	// Even a finished App emission waits for the operation's terminal row.
	current, err := h.inbox.store.spawnTask(h.closeCtx, task.Request.RequestID)
	if err != nil || current.Attempt != task.Attempt || !spawnAttemptTerminal(current.Status) {
		return nil
	}
	if !h.wakePlacementRecords() {
		h.deliverPlacementRecord(h.closeCtx, task.Request.RequestID, task.Attempt)
	}
	return nil
}

// persistPlacementTrace serializes fresh and deferred writes for an attempt.
// A newer successful trace retires an older deferred copy before binding.
func (h *HubServer) persistPlacementTrace(task hubSpawnTask, payload []byte, delivery string) error {
	release, err := h.placementTraces.lock(h.closeCtx, task.Request.RequestID)
	if err != nil {
		return err
	}
	defer release()
	return h.persistPlacementTraceLocked(task, payload, delivery)
}

// The request's placementTraces lock must be held across this save and the
// retirement of any older deferred copy.
func (h *HubServer) persistPlacementTraceLocked(task hubSpawnTask, payload []byte, delivery string) error {
	err := h.inbox.store.saveSpawnPlacementSnapshot(h.closeCtx, task.Request.RequestID, task.Attempt, payload, delivery)
	if err != nil {
		err = h.inbox.store.saveSpawnPlacementSnapshot(h.closeCtx, task.Request.RequestID, task.Attempt, payload, delivery)
	}
	if err != nil {
		h.logger.Warn("placement snapshot could not be persisted", "request_id", task.Request.RequestID, "job", task.Job, "attempt", task.Attempt)
		h.placementAppMu.Lock()
		if e := h.loadPlacementDeferredSavesLocked(); e == nil {
			if e := h.deferPlacementTraceSaveLocked(task.Request.RequestID, task.Attempt, payload, delivery); e != nil {
				h.logger.Warn("placement deferred saves could not be persisted", "request_id", task.Request.RequestID, "attempt", task.Attempt)
			}
		}
		h.placementAppMu.Unlock()
		return err
	}
	h.placementAppMu.Lock()
	defer h.placementAppMu.Unlock()
	if err := h.loadPlacementDeferredSavesLocked(); err != nil {
		return err
	}
	old := h.placementDeferredSaves
	kept := make([]placementDeferredSave, 0, len(old))
	for _, d := range old {
		if d.RequestID != task.Request.RequestID || d.Attempt != task.Attempt {
			kept = append(kept, d)
		}
	}
	if len(kept) != len(old) {
		h.placementDeferredSaves = kept
		if err := h.writePlacementDeferredSavesLocked(); err != nil {
			h.placementDeferredSaves = old
			return err
		}
	}
	return nil
}

// lockPlacementReplay restores any deferred trace before the operation row is
// reused. Its returned unlock must be held through the store's retry commit.
func (h *HubServer) lockPlacementReplay(ctx context.Context, task hubSpawnTask) (func(), error) {
	release, err := h.placementTraces.lock(ctx, task.Request.RequestID)
	if err != nil {
		return nil, err
	}
	h.placementAppMu.Lock()
	err = h.loadPlacementDeferredSavesLocked()
	var deferred *placementDeferredSave
	if err == nil {
		for _, save := range h.placementDeferredSaves {
			if save.RequestID == task.Request.RequestID && save.Attempt == task.Attempt {
				copy := save
				deferred = &copy
				break
			}
		}
	}
	h.placementAppMu.Unlock()
	if err == nil && deferred != nil {
		err = h.persistPlacementTraceLocked(task, deferred.Payload, deferred.Delivery)
	}
	if err != nil {
		release()
		return nil, err
	}
	return release, nil
}

// wakePlacementRecords gives the outbox its own bounded worker. A hub without
// a running coordinator uses the synchronous delivery path instead.
func (h *HubServer) wakePlacementRecords() bool {
	if !h.placementAsync.Load() || h.placementKick == nil {
		return false
	}
	select {
	case h.placementKick <- struct{}{}:
	default:
	}
	return true
}

func (h *HubServer) runPlacementRecords(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	stop := context.AfterFunc(h.closeCtx, cancel)
	defer stop()
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		h.flushPlacementRecords(ctx)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-h.placementKick:
		}
	}
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
	payload, delivery, recorded, err := h.inbox.store.spawnPlacementSnapshot(ctx, requestID, attempt)
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
	if err != nil {
		return
	}
	if task.Attempt == attempt && !spawnAttemptTerminal(task.Status) {
		return
	}
	if task.Attempt != attempt && tr.Record == nil {
		// A superseded attempt whose snapshot carries no finished record
		// cannot be rebuilt: the operation's outcome now belongs to a newer
		// attempt.
		return
	}
	app, err := h.placementRecordApp(delivery)
	if err != nil {
		h.logger.Warn("placement record delivery unavailable; snapshot remains pending", "request_id", requestID, "attempt", attempt)
		return
	}
	delivered, err := app.EnqueuePlacementRecord(ctx, hubPlacementRecord(task, tr, attempt))
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
func hubPlacementRecord(task hubSpawnTask, tr fleet.PlacementTrace, attempt int) fleet.PlacementRecord {
	if tr.Record != nil {
		// The snapshot carries the record finishPlacement completed: it is
		// posted verbatim — never re-derived from the trace's lossy
		// projection or mixed with the operation's current outcome (which
		// may belong to a newer attempt). The hub owns the identity keys.
		rec := *tr.Record
		rec.JobID, rec.Attempt = task.Job, attempt
		return rec
	}
	o := task.Outcome
	if task.Attempt != attempt {
		// The row's outcome belongs to a newer attempt; a superseded
		// attempt is only rebuilt from a self-contained trace, so its
		// execution fields come from the trace alone.
		o = hubSpawnOutcome{}
	}
	rec := fleet.PlacementRecord{
		JobID: task.Job, Attempt: attempt, TaskID: tr.TaskID,
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
		// Contact is not launch acceptance here either: a refused trace
		// posts no chosen host even when the operation row names the
		// contacted one.
		if rec.Outcome == "refused" {
			rec.Host = ""
		}
		return rec
	}
	switch {
	case o.RC == 0:
		rec.Outcome, rec.Reason = "placed", tr.StateReason
	case !o.NodeResult:
		// No node answer arrived — a bound attempt that starved in the
		// queue never contacted its host, so like every other pre-claim
		// path it is a refusal, not a launch failure the host owns.
		rec.Outcome, rec.Reason = "refused", o.Reason
	case o.Pane != "" || !spawnStageBeforeJobClaim(o.Stage) && !spawnOutcomeNotStarted(o):
		rec.Outcome, rec.Reason = "failed", o.Reason
	default:
		rec.Outcome, rec.Reason = "refused", o.Reason
	}
	// The host the attempt bound is its candidate: mark it in the trace, or
	// add the contacted-host row a pre-plan trace never listed. A placed or
	// failed record keeps the host chosen (a failed verdict means the node
	// claimed the job; the launch failed after), as finishPlacement does;
	// only a refused verdict downgrades the row.
	chosen := rec.Outcome != "refused"
	marked := false
	for i := range rec.Candidates {
		if rec.Candidates[i].Host != rec.Host || rec.Host == "" {
			continue
		}
		marked = true
		if chosen {
			rec.Candidates[i].Outcome, rec.Candidates[i].Reason = "chosen", "available"
		} else {
			rec.Candidates[i].Outcome, rec.Candidates[i].Reason = "refused", rec.Reason
		}
	}
	if !marked && rec.Host != "" {
		outcome, reason := "refused", rec.Reason
		if chosen {
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

// placementDeferredSave is a snapshot write the store refused, persisted to
// the hub's deferred file and retried by the flush until a pending row
// exists — a restart between the refused write and its retry must not
// orphan the spooled record the emission produced.
type placementDeferredSave struct {
	RequestID string `json:"request_id"`
	Attempt   int    `json:"attempt"`
	Payload   []byte `json:"payload"`
	Delivery  string `json:"delivery,omitempty"`
}

// deferPlacementTraceSave keeps a failed snapshot write, in memory and in
// the deferred file, so the flush retries the row insert. The latest
// emission replaces an older deferred one for the same attempt.
func (h *HubServer) deferPlacementTraceSave(requestID string, attempt int, payload []byte, delivery string) {
	release, err := h.placementTraces.lock(h.closeCtx, requestID)
	if err != nil {
		return
	}
	defer release()
	h.placementAppMu.Lock()
	defer h.placementAppMu.Unlock()
	if err := h.loadPlacementDeferredSavesLocked(); err != nil {
		return
	}
	if err := h.deferPlacementTraceSaveLocked(requestID, attempt, payload, delivery); err != nil {
		h.logger.Warn("placement deferred saves could not be persisted", "request_id", requestID, "attempt", attempt)
	}
}

func (h *HubServer) deferPlacementTraceSaveLocked(requestID string, attempt int, payload []byte, delivery string) error {
	kept := false
	for i, d := range h.placementDeferredSaves {
		if d.RequestID == requestID && d.Attempt == attempt {
			h.placementDeferredSaves[i].Payload = payload
			h.placementDeferredSaves[i].Delivery = delivery
			kept = true
			break
		}
	}
	if !kept {
		h.placementDeferredSaves = append(h.placementDeferredSaves, placementDeferredSave{RequestID: requestID, Attempt: attempt, Payload: payload, Delivery: delivery})
	}
	return h.writePlacementDeferredSavesLocked()
}

// writePlacementDeferredSavesLocked rewrites the deferred file; an empty
// list removes it. Caller holds placementAppMu.
func (h *HubServer) writePlacementDeferredSavesLocked() error {
	path := h.inbox.deferredPath
	if len(h.placementDeferredSaves) == 0 {
		err := os.Remove(path)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	raw, err := json.Marshal(h.placementDeferredSaves)
	if err != nil {
		return err
	}
	return replaceFileAtomically(path, ".placement-deferred-*", raw, 0o600)
}

// loadPlacementDeferredSavesLocked restores deferred saves from the file,
// once per hub run — a restart must not forget a refused write. Caller
// holds placementAppMu.
func (h *HubServer) loadPlacementDeferredSavesLocked() error {
	if h.placementDeferredLoaded {
		return nil
	}
	raw, err := os.ReadFile(h.inbox.deferredPath)
	if err != nil {
		if os.IsNotExist(err) {
			h.placementDeferredLoaded = true
			return nil
		}
		h.logger.Warn("placement deferred saves could not be read")
		return err
	}
	var saves []placementDeferredSave
	if err := json.Unmarshal(raw, &saves); err != nil {
		h.logger.Warn("placement deferred saves unreadable")
		return err
	}
	h.placementDeferredSaves = saves
	h.placementDeferredLoaded = true
	return nil
}

// retryDeferredTraceSaves lands the deferred snapshot writes. Once a row
// exists, the normal reconciliation picks up the record it tracks.
func (h *HubServer) retryDeferredTraceSaves() {
	h.placementAppMu.Lock()
	if err := h.loadPlacementDeferredSavesLocked(); err != nil {
		h.placementAppMu.Unlock()
		return
	}
	deferred := append([]placementDeferredSave(nil), h.placementDeferredSaves...)
	h.placementAppMu.Unlock()
	for _, d := range deferred {
		release, err := h.placementTraces.lock(h.closeCtx, d.RequestID)
		if err != nil {
			continue
		}
		h.placementAppMu.Lock()
		index := -1
		for i, current := range h.placementDeferredSaves {
			if current.RequestID == d.RequestID && current.Attempt == d.Attempt && string(current.Payload) == string(d.Payload) && current.Delivery == d.Delivery {
				index = i
				break
			}
		}
		h.placementAppMu.Unlock()
		if index < 0 {
			release()
			continue
		}
		if err := h.inbox.store.saveSpawnPlacementSnapshot(h.closeCtx, d.RequestID, d.Attempt, d.Payload, d.Delivery); err != nil {
			release()
			continue
		}
		h.placementAppMu.Lock()
		// Entries for other requests may have moved while the store call ran.
		index = -1
		for i, current := range h.placementDeferredSaves {
			if current.RequestID == d.RequestID && current.Attempt == d.Attempt && string(current.Payload) == string(d.Payload) && current.Delivery == d.Delivery {
				index = i
				break
			}
		}
		if index < 0 {
			h.placementAppMu.Unlock()
			release()
			continue
		}
		old := h.placementDeferredSaves
		h.placementDeferredSaves = append(append([]placementDeferredSave(nil), old[:index]...), old[index+1:]...)
		if err := h.writePlacementDeferredSavesLocked(); err != nil {
			h.placementDeferredSaves = old
			h.logger.Warn("placement deferred saves could not be persisted", "request_id", d.RequestID, "attempt", d.Attempt)
		}
		h.placementAppMu.Unlock()
		release()
	}
}

// hasDeferredTraceSaves reports whether a failed save still waits for a
// row. The deferred file counts — its entries are loaded on the first retry.
func (h *HubServer) hasDeferredTraceSaves() bool {
	h.placementAppMu.Lock()
	defer h.placementAppMu.Unlock()
	if len(h.placementDeferredSaves) > 0 {
		return true
	}
	if h.placementDeferredLoaded {
		return false
	}
	_, err := os.Stat(h.inbox.deferredPath)
	return err == nil || !os.IsNotExist(err)
}

// enqueueFinishedPlacementRecord spools an already-completed record when its
// snapshot row could not be persisted: the durable file keeps the delivery
// obligation the row would have tracked.
func (h *HubServer) enqueueFinishedPlacementRecord(ctx context.Context, task hubSpawnTask, rec fleet.PlacementRecord, delivery string) {
	ctx, cancel := context.WithTimeout(ctx, placementRecordDeliveryBudget)
	defer cancel()
	app, err := h.placementRecordApp(delivery)
	if err != nil {
		h.logger.Warn("placement record delivery unavailable after snapshot write failure", "request_id", task.Request.RequestID, "attempt", task.Attempt)
		return
	}
	rec.JobID, rec.Attempt = task.Job, task.Attempt
	if _, err := app.EnqueuePlacementRecord(ctx, rec); err != nil {
		h.logger.Warn("placement record could not be enqueued after snapshot write failure", "request_id", task.Request.RequestID, "attempt", task.Attempt, "error", err)
	}
}

// placementDelivery holds the placing App's delivery-relevant settings: the
// state directory the placement spool lives in and the shared-quota override
// that points the record post at the right hk. It is persisted on the
// attempt's snapshot row, so the record posts the way its own App would
// have, even beside a concurrent spawn with different settings.
type placementDelivery struct {
	StateDir string         `json:"state_dir,omitempty"`
	Home     string         `json:"home,omitempty"`
	Quota    map[string]any `json:"quota,omitempty"`
}

// placementRecordApp returns the fleet App whose state directory holds the
// placement spool and whose hk client delivers the records, built for the
// delivery settings the placing App persisted. Clients are cached per
// delivery JSON; a different one builds a fresh App.
func (h *HubServer) placementRecordApp(delivery string) (*fleet.App, error) {
	h.placementAppMu.Lock()
	defer h.placementAppMu.Unlock()
	if h.placementRecordClient != nil && h.placementDeliveryKey == delivery {
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
	var d placementDelivery
	if json.Unmarshal([]byte(delivery), &d) == nil {
		if d.StateDir != "" {
			app.StateDir = d.StateDir
		}
		if d.Home != "" {
			app.Home = d.Home
		}
		if d.Quota != nil {
			app.Config.Quota = d.Quota
		}
	}
	h.placementRecordClient = app
	h.placementDeliveryKey = delivery
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
	err := h.inbox.store.db.QueryRowContext(ctx, `SELECT 1 FROM spawn_placement_snapshots WHERE recorded<>1 LIMIT 1`).Scan(&one)
	return err == nil
}

// flushPlacementRecords retries records still spooled from before a restart.
// Without a pending snapshot there is nothing the outbox owes, so no delivery
// App is built.
func (h *HubServer) flushPlacementRecords(ctx context.Context) {
	if h.inbox == nil {
		return
	}
	if !h.pendingPlacementRecords(ctx) && !h.hasDeferredTraceSaves() {
		return
	}
	// Deferred saves retry first: a landed row turns an orphaned spooled
	// record back into a pending one the reconciliation below can seal.
	h.retryDeferredTraceSaves()
	if !h.pendingPlacementRecords(ctx) {
		return
	}
	pending, err := h.inbox.store.pendingPlacementSnapshots(h.closeCtx)
	if err != nil || len(pending) == 0 {
		return
	}
	// Records still spooled retry delivery under the delivery settings their
	// own attempt persisted.
	flushed := map[string]bool{}
	for _, row := range pending {
		if ctx.Err() != nil {
			return
		}
		if flushed[row.Delivery] {
			continue
		}
		flushed[row.Delivery] = true
		if app, err := h.placementRecordApp(row.Delivery); err == nil {
			app.FlushPlacements()
		}
	}
	// Reconcile pending snapshots against the spool. A file still present
	// keeps its row pending so the next tick retries delivery; a live attempt
	// has no file yet and must never be sealed. A terminal attempt with no
	// file either never enqueued (a crash between the terminal commit and the
	// enqueue, a delivery-App build failure, an enqueue error) or delivered
	// before its seal could be written: rebuild and enqueue the identical
	// record — hk dedupes the repost — and let its own mark seal it. Only an
	// attempt that is over but owes no rebuildable record (superseded, its
	// outcome already overwritten by a newer attempt) is sealed here.
	for _, row := range pending {
		if ctx.Err() != nil {
			return
		}
		if !row.attemptOver() {
			continue
		}
		app, err := h.placementRecordApp(row.Delivery)
		if err != nil {
			continue
		}
		if app.PlacementSpooled(row.Job, row.Attempt) {
			continue
		}
		if spawnAttemptTerminal(row.Status) && row.CurrentAttempt == row.Attempt {
			h.deliverPlacementRecord(h.closeCtx, row.RequestID, row.Attempt)
			continue
		}
		// A superseded attempt is still owed its record when the snapshot is
		// self-contained (the trace carries its own verdict); only one whose
		// outcome a newer attempt overwrote cannot be rebuilt and is sealed.
		var tr fleet.PlacementTrace
		if json.Unmarshal(row.Payload, &tr) == nil && tr.Outcome != "" {
			h.deliverPlacementRecord(h.closeCtx, row.RequestID, row.Attempt)
			continue
		}
		_ = h.inbox.store.markSpawnPlacementRecorded(h.closeCtx, row.RequestID, row.Attempt)
	}
}

package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
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
	record := hubPlacementRecord(task, trace, task.Attempt)
	trace.Record, trace.Outcome, trace.Reason = &record, record.Outcome, record.Reason
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
	previous, previousDelivery, _, readErr := h.inbox.store.spawnPlacementSnapshot(h.closeCtx, task.Request.RequestID, task.Attempt)
	var base *placementDeferredBase
	if readErr == nil {
		base = &placementDeferredBase{Exists: len(previous) != 0, Payload: previous, Delivery: previousDelivery}
		h.rememberPlacementTraceBase(task.Request.RequestID, task.Attempt, *base, false)
	} else {
		base = h.cachedPlacementTraceBase(task.Request.RequestID, task.Attempt)
	}
	err := h.inbox.store.saveSpawnPlacementSnapshot(h.closeCtx, task.Request.RequestID, task.Attempt, payload, delivery)
	if err != nil {
		err = h.inbox.store.saveSpawnPlacementSnapshot(h.closeCtx, task.Request.RequestID, task.Attempt, payload, delivery)
	}
	if err != nil {
		h.logger.Warn("placement snapshot could not be persisted", "request_id", task.Request.RequestID, "job", task.Job, "attempt", task.Attempt)
		if base == nil {
			// Without a known base a deferred write cannot be fenced. Leave
			// placement pending and retry once the store can be read.
			return err
		}
		h.placementAppMu.Lock()
		if e := h.loadPlacementDeferredSavesLocked(); e == nil {
			if e := h.deferPlacementTraceSaveLocked(task.Request.RequestID, task.Attempt, payload, delivery, base); e != nil {
				h.logger.Warn("placement deferred saves could not be persisted", "request_id", task.Request.RequestID, "attempt", task.Attempt)
			}
		}
		h.placementAppMu.Unlock()
		return err
	}
	h.rememberPlacementTraceBase(task.Request.RequestID, task.Attempt, placementDeferredBase{Exists: true, Payload: payload, Delivery: delivery}, false)
	h.placementAppMu.Lock()
	defer h.placementAppMu.Unlock()
	if err := h.loadPlacementDeferredSavesLocked(); err != nil {
		// The snapshot is durable. Preserve the unreadable file for repair,
		// without stopping unrelated requests whose snapshots saved normally.
		return nil
	}
	_ = h.retireDeferredTraceSaveLocked(task.Request.RequestID, task.Attempt)
	return nil
}

// The caller holds placementAppMu. A failure leaves the older file in place;
// its base condition keeps a later retry from overwriting a newer snapshot.
func (h *HubServer) retireDeferredTraceSaveLocked(requestID string, attempt int) error {
	old := h.placementDeferredSaves
	kept := make([]placementDeferredSave, 0, len(old))
	for _, d := range old {
		if d.RequestID != requestID || d.Attempt != attempt {
			kept = append(kept, d)
		}
	}
	if len(kept) != len(old) {
		h.placementDeferredSaves = kept
		if err := h.writePlacementDeferredSavesLocked(); err != nil {
			h.placementDeferredSaves = old
			h.logger.Warn("placement deferred saves could not be retired", "request_id", requestID, "attempt", attempt)
			return err
		}
	}
	return nil
}

// lockPlacementTrace restores any deferred trace before another placement
// pass or a replay reads the row. Replay holds the lock through its commit.
func (h *HubServer) lockPlacementTrace(ctx context.Context, task hubSpawnTask, replay bool) (func(), error) {
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
		var resolved bool
		var observed *placementDeferredBase
		resolved, observed, err = h.inbox.store.saveDeferredPlacementSnapshot(ctx, *deferred)
		if observed != nil {
			h.rememberPlacementTraceBase(task.Request.RequestID, task.Attempt, *observed, true)
		}
		if err == nil && !resolved {
			h.logger.Warn("deferred placement trace conflicts with snapshot; replay held", "request_id", task.Request.RequestID, "attempt", task.Attempt)
			err = errors.New("deferred placement trace conflicts with snapshot")
		} else if err == nil {
			h.placementAppMu.Lock()
			err = h.retireDeferredTraceSaveLocked(task.Request.RequestID, task.Attempt)
			h.placementAppMu.Unlock()
			if !replay {
				// The fenced file may be retired later; this pass reads the
				// restored snapshot before it emits another trace.
				err = nil
			}
		}
	} else if err != nil {
		if !replay {
			// A damaged file must not prevent a fresh request from placing.
			return release, nil
		}
		// A saved snapshot can be frozen without knowing the contents of a
		// damaged deferred file. A missing one must wait for its trace.
		var payload []byte
		payload, _, _, err = h.inbox.store.spawnPlacementSnapshot(ctx, task.Request.RequestID, task.Attempt)
		if err == nil && len(payload) == 0 {
			err = errors.New("placement trace unavailable for replay")
		}
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
	release, err := h.placementTraces.lock(ctx, requestID)
	if err != nil {
		return
	}
	defer release()
	if h.deferredPlacementTracePending(requestID, attempt) {
		return
	}
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
	if task.Attempt == attempt && (task.Outcome.Reason == "timeout" || spawnOutcomeNotStarted(task.Outcome)) {
		// An in-process placement error can race the authoritative deadline or
		// not-started result. The terminal operation decides this verdict.
		tr.Record, tr.Outcome, tr.Reason = nil, "", ""
	}
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
	RequestID string                 `json:"request_id"`
	Attempt   int                    `json:"attempt"`
	Payload   []byte                 `json:"payload"`
	Delivery  string                 `json:"delivery,omitempty"`
	Base      *placementDeferredBase `json:"base,omitempty"`
}

// Base is the snapshot observed before a failed save. It fences a retry from
// overwriting a newer snapshot if retiring the deferred file failed.
type placementDeferredBase struct {
	Exists   bool   `json:"exists"`
	Payload  []byte `json:"payload,omitempty"`
	Delivery string `json:"delivery,omitempty"`
}

type placementTraceBaseKey struct {
	RequestID string
	Attempt   int
}

// The in-process base spans a placement pass: its first snapshot read is
// successful before the App starts, even if the store fails during an emission.
func (h *HubServer) rememberPlacementTraceBase(requestID string, attempt int, base placementDeferredBase, existingOnly bool) {
	h.placementAppMu.Lock()
	defer h.placementAppMu.Unlock()
	key := placementTraceBaseKey{RequestID: requestID, Attempt: attempt}
	if _, exists := h.placementTraceBases[key]; existingOnly && !exists {
		return
	}
	if h.placementTraceBases == nil {
		h.placementTraceBases = make(map[placementTraceBaseKey]placementDeferredBase)
	}
	base.Payload = append([]byte(nil), base.Payload...)
	h.placementTraceBases[key] = base
}

func (h *HubServer) cachedPlacementTraceBase(requestID string, attempt int) *placementDeferredBase {
	h.placementAppMu.Lock()
	defer h.placementAppMu.Unlock()
	base, exists := h.placementTraceBases[placementTraceBaseKey{RequestID: requestID, Attempt: attempt}]
	if !exists {
		return nil
	}
	base.Payload = append([]byte(nil), base.Payload...)
	return &base
}

func (h *HubServer) forgetPlacementTraceBase(requestID string, attempt int) {
	h.placementAppMu.Lock()
	defer h.placementAppMu.Unlock()
	delete(h.placementTraceBases, placementTraceBaseKey{RequestID: requestID, Attempt: attempt})
}

// saveDeferredPlacementSnapshot only replaces the row that a failed write
// observed. Legacy or unreadable-base saves cannot replace a different row;
// they stay on disk for this attempt instead of risking another attempt's data.
func (s *laneInboxStore) saveDeferredPlacementSnapshot(parent context.Context, d placementDeferredSave) (bool, *placementDeferredBase, error) {
	ctx, done := s.op(parent)
	defer done()
	var result sql.Result
	var err error
	if d.Base != nil && d.Base.Exists {
		result, err = s.db.ExecContext(ctx, s.q(`UPDATE spawn_placement_snapshots SET payload=?,delivery=? WHERE request_id=? AND attempt=? AND recorded=0 AND payload=? AND delivery=?`), string(d.Payload), d.Delivery, d.RequestID, d.Attempt, string(d.Base.Payload), d.Base.Delivery)
	} else {
		result, err = s.db.ExecContext(ctx, s.q(`INSERT INTO spawn_placement_snapshots(request_id,attempt,payload,delivery) VALUES(?,?,?,?) ON CONFLICT(request_id,attempt) DO NOTHING`), d.RequestID, d.Attempt, string(d.Payload), d.Delivery)
	}
	if err != nil {
		return false, nil, err
	}
	if n, err := result.RowsAffected(); err != nil || n != 0 {
		if err != nil {
			return false, nil, err
		}
		return true, &placementDeferredBase{Exists: true, Payload: d.Payload, Delivery: d.Delivery}, nil
	}
	var payload, delivery string
	var recorded int
	err = s.db.QueryRowContext(ctx, s.q(`SELECT payload,delivery,recorded FROM spawn_placement_snapshots WHERE request_id=? AND attempt=?`), d.RequestID, d.Attempt).Scan(&payload, &delivery, &recorded)
	if errors.Is(err, sql.ErrNoRows) {
		return false, &placementDeferredBase{}, nil
	}
	if err != nil {
		return false, nil, err
	}
	observed := &placementDeferredBase{Exists: true, Payload: []byte(payload), Delivery: delivery}
	if payload == string(d.Payload) && delivery == d.Delivery {
		return true, observed, nil
	}
	if recorded == placementSnapshotFrozen && d.Base != nil && d.Base.Exists && payload == string(d.Base.Payload) && delivery == d.Base.Delivery {
		// The save was deferred after this attempt had already frozen.
		return true, observed, nil
	}
	// A known base that changed was superseded by a later successful save.
	// A frozen row is held for reconciliation: it may carry the old result.
	return d.Base != nil && recorded != placementSnapshotFrozen, observed, nil
}

func samePlacementDeferredSave(a, b placementDeferredSave) bool {
	if a.RequestID != b.RequestID || a.Attempt != b.Attempt || string(a.Payload) != string(b.Payload) || a.Delivery != b.Delivery {
		return false
	}
	if a.Base == nil || b.Base == nil {
		return a.Base == nil && b.Base == nil
	}
	return a.Base.Exists == b.Base.Exists && string(a.Base.Payload) == string(b.Base.Payload) && a.Base.Delivery == b.Base.Delivery
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
	previous, previousDelivery, _, readErr := h.inbox.store.spawnPlacementSnapshot(h.closeCtx, requestID, attempt)
	var base *placementDeferredBase
	if readErr == nil {
		base = &placementDeferredBase{Exists: len(previous) != 0, Payload: previous, Delivery: previousDelivery}
	}
	h.placementAppMu.Lock()
	defer h.placementAppMu.Unlock()
	if err := h.loadPlacementDeferredSavesLocked(); err != nil {
		return
	}
	if err := h.deferPlacementTraceSaveLocked(requestID, attempt, payload, delivery, base); err != nil {
		h.logger.Warn("placement deferred saves could not be persisted", "request_id", requestID, "attempt", attempt)
	}
}

func (h *HubServer) deferPlacementTraceSaveLocked(requestID string, attempt int, payload []byte, delivery string, base *placementDeferredBase) error {
	kept := false
	for i, d := range h.placementDeferredSaves {
		if d.RequestID == requestID && d.Attempt == attempt {
			h.placementDeferredSaves[i].Payload = payload
			h.placementDeferredSaves[i].Delivery = delivery
			h.placementDeferredSaves[i].Base = base
			kept = true
			break
		}
	}
	if !kept {
		h.placementDeferredSaves = append(h.placementDeferredSaves, placementDeferredSave{RequestID: requestID, Attempt: attempt, Payload: payload, Delivery: delivery, Base: base})
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
		if err == nil {
			syncDirectoryBestEffort(filepath.Dir(path))
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
			if samePlacementDeferredSave(current, d) {
				index = i
				break
			}
		}
		h.placementAppMu.Unlock()
		if index < 0 {
			release()
			continue
		}
		resolved, observed, err := h.inbox.store.saveDeferredPlacementSnapshot(h.closeCtx, d)
		if observed != nil {
			h.rememberPlacementTraceBase(d.RequestID, d.Attempt, *observed, true)
		}
		if err != nil || !resolved {
			release()
			continue
		}
		h.placementAppMu.Lock()
		// Entries for other requests may have moved while the store call ran.
		index = -1
		for i, current := range h.placementDeferredSaves {
			if samePlacementDeferredSave(current, d) {
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

// A conflicting legacy save is retained until it can be reconciled. Do not
// publish a record built from only the other copy of this attempt's trace.
func (h *HubServer) deferredPlacementTracePending(requestID string, attempt int) bool {
	h.placementAppMu.Lock()
	defer h.placementAppMu.Unlock()
	if err := h.loadPlacementDeferredSavesLocked(); err != nil {
		return true
	}
	for _, d := range h.placementDeferredSaves {
		if d.RequestID == requestID && d.Attempt == attempt {
			return true
		}
	}
	return false
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
	// Reconcile each attempt separately, using the delivery settings saved
	// with that attempt even when several share the same spool directory.
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
		attemptCtx, cancel := context.WithTimeout(ctx, placementRecordDeliveryBudget)
		release, err := h.placementTraces.lock(attemptCtx, row.RequestID)
		if err != nil {
			cancel()
			continue
		}
		if h.deferredPlacementTracePending(row.RequestID, row.Attempt) {
			release()
			cancel()
			continue
		}
		if app.PlacementSpooled(row.Job, row.Attempt) {
			if app.FlushPlacementRecord(attemptCtx, row.Job, row.Attempt) {
				if err := h.inbox.store.markSpawnPlacementRecorded(attemptCtx, row.RequestID, row.Attempt); err != nil {
					h.logger.Warn("placement snapshot could not be marked recorded", "request_id", row.RequestID, "attempt", row.Attempt)
				}
			}
			release()
			cancel()
			continue
		}
		if spawnAttemptTerminal(row.Status) && row.CurrentAttempt == row.Attempt {
			release()
			cancel()
			h.deliverPlacementRecord(h.closeCtx, row.RequestID, row.Attempt)
			continue
		}
		// A superseded attempt is still owed its record when the snapshot is
		// self-contained (the trace carries its own verdict); only one whose
		// outcome a newer attempt overwrote cannot be rebuilt and is sealed.
		var tr fleet.PlacementTrace
		if json.Unmarshal(row.Payload, &tr) == nil && tr.Outcome != "" {
			release()
			cancel()
			h.deliverPlacementRecord(h.closeCtx, row.RequestID, row.Attempt)
			continue
		}
		_ = h.inbox.store.markSpawnPlacementRecorded(attemptCtx, row.RequestID, row.Attempt)
		release()
		cancel()
	}
}

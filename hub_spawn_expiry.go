package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// The request time limit and start conditions of the spawn queue (hk 4660;
// hk design/2026-11-13/hub-owned-spawn-queue §3 rule 2 and §4). See
// docs/hub-spawn-operations.md#time-limit-and-start-conditions.

// defaultStartConditionPoll is how often the hub reads hk for the tasks a
// waiting request names. A time condition and an expiry are judged on every
// coordinator tick without it.
const defaultStartConditionPoll = 60 * time.Second

// spawnWaitingScanLimit bounds one pass over the waiting rows. Passes
// continue from a cursor, so every waiting row is judged within
// ceil(rows/limit) passes whatever the older ones are waiting for.
const spawnWaitingScanLimit = 100

// spawnConditionReadBudget bounds the hk reads of one waiting pass.
const spawnConditionReadBudget = 30 * time.Second

// spawnAttemptEndedUnstarted reports whether an outcome ends an attempt
// whose start is proven absent, so a request with a limit renews it or gives
// up: the landing wait ran out with the hub never having sent the attempt (or
// the node attesting it, which completeSpawn decides), or the node's own
// not-started attestation (also the start proof's fence) came after the
// attempt's deadline. A not-started answer inside the deadline is a refusal
// the next-host path or the caller owns, and ends retryable as before.
func spawnAttemptEndedUnstarted(task hubSpawnTask, outcome hubSpawnOutcome, now time.Time) bool {
	if task.Expires.IsZero() {
		return false
	}
	return outcome.Reason == "timeout" || spawnOutcomeNotStarted(outcome) && !now.Before(task.Deadline)
}

// spawnAttemptDeadline is when an attempt that begins at now ends: the
// per-attempt landing wait, or the request's limit when that comes first.
func spawnAttemptDeadline(now time.Time, deadlineSeconds int, expires time.Time) time.Time {
	deadline := now.Add(time.Duration(deadlineSeconds) * time.Second)
	if !expires.IsZero() && expires.Before(deadline) {
		return expires
	}
	return deadline
}

// spawnExpiresAt is the request's limit as the status API shows it.
func spawnExpiresAt(task hubSpawnTask) string {
	if task.Expires.IsZero() {
		return ""
	}
	return task.Expires.UTC().Format(time.RFC3339)
}

// spawnEndedUnstarted reports the terminal statuses in which the hub never
// started the request: the time limit passed, or the request is no longer
// wanted.
func spawnEndedUnstarted(status string) bool {
	return status == "given_up" || status == "cancelled"
}

// spawnGivenUpOutcome is the record of a request that reached its limit with
// no attempt left that could have started.
func spawnGivenUpOutcome(task hubSpawnTask, host string) hubSpawnOutcome {
	return hubSpawnOutcome{
		RC: 84, Stage: "queue", Host: host,
		Reason: spawnOutcomeReason("expired", fmt.Sprintf("expires_at %s passed after %d attempt(s)", spawnExpiresAt(task), task.Attempt)),
		Fix:    "Nothing was started. Submit the spawn again to queue a new request; --expires raises the limit (at most 24h).",
		E6Arm:  task.Request.E6Arm,
	}
}

// waitingSpawns lists up to limit requests queued behind a start condition,
// oldest first, after the cursor (the zero cursor starts at the oldest).
func (s *laneInboxStore) waitingSpawns(parent context.Context, after spawnScanPosition, limit int) ([]hubSpawnTask, error) {
	ctx, done := s.op(parent)
	defer done()
	query := `SELECT ` + spawnTaskColumns + ` FROM spawn_operations WHERE status='waiting'`
	var args []any
	if after.valid {
		query += ` AND (created_ms>? OR (created_ms=? AND request_id>?))`
		args = append(args, after.created, after.created, after.id)
	}
	rows, err := s.db.QueryContext(ctx, s.q(query+fmt.Sprintf(` ORDER BY created_ms,request_id LIMIT %d`, limit)), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []hubSpawnTask
	for rows.Next() {
		task, err := scanSpawnTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

// releaseWaitingSpawn moves a request whose start condition holds to pending.
// Its first attempt's landing wait starts now.
//
// A row an older hub left (no limit) gets the role's default limit from its
// release, so a conditioned row with no limit exists only until then, and the
// legacy guard in executeSpawnTask never applies to a request this hub queued.
func (s *laneInboxStore) releaseWaitingSpawn(parent context.Context, task hubSpawnTask, now time.Time) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	expires := task.Expires
	if expires.IsZero() {
		spawn, err := fleet.ValidateSubmission(task.Request)
		if err != nil {
			return false, err
		}
		expires = now.Add(fleet.SpawnExpiresFor(spawn.Role, task.Request.ExpiresSeconds))
	}
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET status='accepted',deadline_ms=?,expires_ms=? WHERE request_id=? AND attempt=? AND status='waiting'`), spawnAttemptDeadline(now, task.Request.DeadlineSeconds, expires).UnixMilli(), expires.UnixMilli(), task.Request.RequestID, task.Attempt)
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	return n == 1, nil
}

// endWaitingSpawn ends a waiting request without ever starting it: given_up
// at its limit, cancelled when its condition can never hold. The outcome, the
// freed reservation and the owner's notice are one transaction.
func (s *laneInboxStore) endWaitingSpawn(parent context.Context, task hubSpawnTask, status string, outcome hubSpawnOutcome, now time.Time) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	raw, err := json.Marshal(outcome)
	if err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, s.q(`UPDATE spawn_operations SET status=?,result=? WHERE request_id=? AND attempt=? AND status='waiting'`), status, string(raw), task.Request.RequestID, task.Attempt)
	if err != nil {
		return false, err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return false, nil
	}
	if err = s.releaseEndedSpawn(ctx, tx, task, status, string(raw), now); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// endUnstartedAttempt runs inside completeSpawn for an attempt whose start is
// proven absent (the hub never sent it, or the node attested it never
// started): the attempt ended, not the request. Before the request's limit it
// goes back to pending under the same reservation and a new attempt; at the
// limit it is given up. A request whose start is unproven never reaches this
// (completeSpawn keeps it deadline_exceeded or reconciling).
func (s *laneInboxStore) endUnstartedAttempt(ctx context.Context, tx *sql.Tx, task hubSpawnTask, outcome hubSpawnOutcome, now time.Time) (bool, error) {
	if now.Before(task.Expires) {
		// The same hand-over a client's replay of a retryable request makes
		// (resumeCleanedSpawn), without the gap in the reservation.
		// The attempt's record is judged by the outcome that ended it.
		ended := task
		ended.Outcome = outcome
		if err := s.freezeSpawnPlacementForRetry(ctx, tx, ended); err != nil {
			return false, err
		}
		result, err := tx.ExecContext(ctx, s.q(`UPDATE spawn_operations SET status='accepted',deadline_ms=?,node_id=NULL,node_payload='',attempt=attempt+1,trace_recorded=0,last_run_ms=0,exclusions='[]',claimed_host=?,refresh_waits='[]' WHERE request_id=? AND status='retryable' AND attempt=?`), spawnAttemptDeadline(now, task.Request.DeadlineSeconds, task.Expires).UnixMilli(), spawnPinnedHost(task), task.Request.RequestID, task.Attempt)
		if err != nil {
			return false, err
		}
		if n, _ := result.RowsAffected(); n != 1 {
			return false, nil
		}
		return true, tx.Commit()
	}
	given := spawnGivenUpOutcome(task, outcome.Host)
	raw, err := json.Marshal(given)
	if err != nil {
		return false, err
	}
	result, err := tx.ExecContext(ctx, s.q(`UPDATE spawn_operations SET status='given_up',result=? WHERE request_id=? AND attempt=? AND status='retryable'`), string(raw), task.Request.RequestID, task.Attempt)
	if err != nil {
		return false, err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return false, nil
	}
	if err = s.releaseEndedSpawn(ctx, tx, task, "given_up", string(raw), now); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// releaseEndedSpawn frees the reservation of a request that ended without
// starting, so the job id and the task's role are free to submit again (the
// same release a retryable request gets), and queues the owner's notice.
func (s *laneInboxStore) releaseEndedSpawn(ctx context.Context, tx *sql.Tx, task hubSpawnTask, status, raw string, now time.Time) error {
	if _, err := tx.ExecContext(ctx, s.q(`DELETE FROM spawn_reservations WHERE job=? AND request_id=?`), task.Job, task.Request.RequestID); err != nil {
		return err
	}
	return queueSpawnNotifications(ctx, s, tx, task, status, raw, now)
}

// parkLegacyConditionedSpawns returns a request that carries start conditions
// but was never placed to waiting. Such a row predates the waiting state and
// the time limit (expires_ms=0: this hub gives every request it queues a
// limit, and gives a released legacy row one). Until it is parked,
// executeSpawnTask refuses to place it.
func (s *laneInboxStore) parkLegacyConditionedSpawns(parent context.Context) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, `UPDATE spawn_operations SET status='waiting' WHERE status='accepted' AND node_id IS NULL AND expires_ms=0 AND payload LIKE '%"start_conditions":%'`)
	return err
}

// checkWaitingSpawns is one pass over a page of the requests queued behind a
// start condition: past its limit a request is given up; one whose condition
// holds is released to pending; one that waits on a task hk dropped is
// cancelled. Each pass continues from the last, so every waiting row is
// judged however many are waiting. hk is read for a task at most once per
// poll interval (an unread task at once), whether or not the read answered.
func (h *HubServer) checkWaitingSpawns(ctx context.Context) {
	h.startConditionMu.Lock()
	cursor := h.waitingCursor
	h.startConditionMu.Unlock()
	rows, err := h.inbox.store.waitingSpawns(ctx, cursor, spawnWaitingScanLimit)
	if err != nil {
		return
	}
	now := h.now().UTC()
	h.startConditionMu.Lock()
	switch {
	case len(rows) < spawnWaitingScanLimit:
		// The end of the queue: start over, and forget what no row waits for.
		h.waitingCursor = spawnScanPosition{}
		if len(rows) == 0 && !cursor.valid {
			h.startConditionTasks, h.startConditionTried, h.startConditionSeen = nil, nil, nil
		}
	default:
		last := rows[len(rows)-1]
		h.waitingCursor = spawnScanPosition{created: last.Created.UnixMilli(), id: last.Request.RequestID, valid: true}
	}
	h.startConditionMu.Unlock()
	var live []hubSpawnTask
	for _, task := range rows {
		if !task.Expires.IsZero() && !now.Before(task.Expires) {
			h.endWaitingSpawn(ctx, task, "given_up", spawnGivenUpOutcome(task, ""), now)
			continue
		}
		live = append(live, task)
	}
	var stale [][]fleet.StartCondition
	h.startConditionMu.Lock()
	for _, task := range live {
		for _, id := range fleet.StartConditionTaskIDs(task.Request.StartConditions) {
			if tried, ok := h.startConditionTried[id]; !ok || now.Sub(tried) >= h.startConditionEvery() {
				stale = append(stale, task.Request.StartConditions)
				break
			}
		}
	}
	h.startConditionMu.Unlock()
	if len(stale) != 0 {
		readCtx, cancel := context.WithTimeout(ctx, spawnConditionReadBudget)
		read, attempted := fleet.ReadStartConditionTaskPass(readCtx, stale)
		cancel()
		h.startConditionMu.Lock()
		if h.startConditionTasks == nil {
			h.startConditionTasks, h.startConditionTried = map[string]fleet.TaskSnapshot{}, map[string]time.Time{}
		}
		// A read that failed keeps the last answer: a condition that held
		// stays held, and one never read fails closed (hk_unavailable).
		for id, snapshot := range read {
			h.startConditionTasks[id] = snapshot
		}
		for id := range attempted {
			h.startConditionTried[id] = now
		}
		h.startConditionMu.Unlock()
	}
	h.startConditionMu.Lock()
	snapshots := h.startConditionTasks
	if h.startConditionSeen == nil {
		h.startConditionSeen = map[string]fleet.StartConditionResult{}
	}
	h.startConditionMu.Unlock()
	for _, task := range live {
		result := fleet.EvaluateStartConditions(task.Request.StartConditions, snapshots, now)
		id := task.Request.RequestID
		switch result.Status {
		case "met":
			if released, err := h.inbox.store.releaseWaitingSpawn(ctx, task, now); err == nil && released {
				h.logger.Info("hub spawn released from its start condition", "request_id", id, "job", task.Job)
				h.kickSpawnPlacement()
			}
			h.forgetStartCondition(id)
		case "cancelled":
			outcome := hubSpawnOutcome{RC: 84, Stage: "queue", Reason: spawnOutcomeReason("start_condition_cancelled", result.Reason), Fix: "Nothing was started: a task this request waits on was dropped or closed as a duplicate. Submit the spawn again without that condition.", E6Arm: task.Request.E6Arm}
			h.endWaitingSpawn(ctx, task, "cancelled", outcome, now)
			h.forgetStartCondition(id)
		default:
			h.startConditionMu.Lock()
			h.startConditionSeen[id] = result
			h.startConditionMu.Unlock()
		}
	}
	for _, task := range rows {
		if !task.Expires.IsZero() && !now.Before(task.Expires) {
			h.forgetStartCondition(task.Request.RequestID)
		}
	}
}

func (h *HubServer) forgetStartCondition(requestID string) {
	h.startConditionMu.Lock()
	delete(h.startConditionSeen, requestID)
	h.startConditionMu.Unlock()
}

// runWaitingSpawns judges the waiting requests on its own goroutine, so an hk
// that answers slowly never holds up placement. It also retries the startup
// parking of legacy conditioned rows until it succeeds.
func (h *HubServer) runWaitingSpawns(parent context.Context) {
	if h.inbox == nil {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	stop := context.AfterFunc(h.closeCtx, cancel)
	defer stop()
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	parked := false
	for {
		if h.drain.admit() {
			if !parked {
				parked = h.inbox.store.parkLegacyConditionedSpawns(ctx) == nil
			}
			h.checkWaitingSpawns(ctx)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// evaluateHubStartCondition judges one request's conditions with a live hk
// read, for the guard on rows an older hub left (executeSpawnTask).
func evaluateHubStartCondition(ctx context.Context, h *HubServer, conditions []fleet.StartCondition) fleet.StartConditionResult {
	snapshots, err := fleet.ReadStartConditionTasks(ctx, conditions)
	if err != nil {
		return fleet.StartConditionResult{Status: "waiting", Reasons: []string{"hk_unavailable"}}
	}
	return fleet.EvaluateStartConditions(conditions, snapshots, h.now().UTC())
}

func (h *HubServer) endWaitingSpawn(ctx context.Context, task hubSpawnTask, status string, outcome hubSpawnOutcome, now time.Time) {
	changed, err := h.inbox.store.endWaitingSpawn(ctx, task, status, outcome, now)
	if err != nil || !changed {
		return
	}
	h.logger.Warn("hub spawn ended while waiting", "request_id", task.Request.RequestID, "job", task.Job, "status", status, "reason", outcome.Reason)
}

func (h *HubServer) startConditionEvery() time.Duration {
	if h.startConditionPoll > 0 {
		return h.startConditionPoll
	}
	return defaultStartConditionPoll
}

// waitingStartCondition is what the last pass found for a waiting request, or
// nil before the first one.
func (h *HubServer) waitingStartCondition(requestID string) *fleet.StartConditionResult {
	h.startConditionMu.Lock()
	defer h.startConditionMu.Unlock()
	if result, ok := h.startConditionSeen[requestID]; ok {
		return &result
	}
	return nil
}

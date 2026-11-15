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

// spawnWaitingScanLimit bounds one pass over the waiting rows.
const spawnWaitingScanLimit = 1000

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

// waitingSpawns lists the requests queued behind a start condition.
func (s *laneInboxStore) waitingSpawns(parent context.Context) ([]hubSpawnTask, error) {
	ctx, done := s.op(parent)
	defer done()
	return s.queryPendingSpawns(ctx, fmt.Sprintf(`status='waiting' ORDER BY created_ms,request_id LIMIT %d`, spawnWaitingScanLimit))
}

// releaseWaitingSpawn moves a request whose start condition holds to pending.
// Its first attempt's landing wait starts now.
func (s *laneInboxStore) releaseWaitingSpawn(parent context.Context, task hubSpawnTask, now time.Time) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET status='accepted',deadline_ms=? WHERE request_id=? AND attempt=? AND status='waiting'`), spawnAttemptDeadline(now, task.Request.DeadlineSeconds, task.Expires).UnixMilli(), task.Request.RequestID, task.Attempt)
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
// but was never placed to waiting. Such a row predates the waiting state; a
// released row is converted too and is judged again on the next pass, because
// its conditions still hold.
func (s *laneInboxStore) parkLegacyConditionedSpawns(parent context.Context) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, `UPDATE spawn_operations SET status='waiting' WHERE status='accepted' AND node_id IS NULL AND payload LIKE '%"start_conditions":%'`)
	return err
}

// checkWaitingSpawns is one pass over the requests queued behind a start
// condition: past its limit a request is given up; one whose condition holds
// is released to pending; one that waits on a task hk dropped is cancelled.
// hk is read at most once per poll interval, and sooner only for a request
// the hub has not judged yet.
func (h *HubServer) checkWaitingSpawns(ctx context.Context) {
	rows, err := h.inbox.store.waitingSpawns(ctx)
	if err != nil {
		return
	}
	now := h.now().UTC()
	var live []hubSpawnTask
	for _, task := range rows {
		if !task.Expires.IsZero() && !now.Before(task.Expires) {
			h.endWaitingSpawn(ctx, task, "given_up", spawnGivenUpOutcome(task, ""), now)
			continue
		}
		live = append(live, task)
	}
	var conditions [][]fleet.StartCondition
	unjudged := false
	h.startConditionMu.Lock()
	for _, task := range live {
		if spawnConditionsNeedHK(task.Request.StartConditions) {
			conditions = append(conditions, task.Request.StartConditions)
			if _, judged := h.startConditionSeen[task.Request.RequestID]; !judged {
				unjudged = true
			}
		}
	}
	snapshots := h.startConditionTasks
	read := len(conditions) != 0 && (unjudged || now.Sub(h.startConditionReadAt) >= h.startConditionEvery())
	h.startConditionMu.Unlock()
	if read {
		snapshots, _ = fleet.ReadStartConditionTaskBatch(ctx, conditions)
		h.startConditionMu.Lock()
		h.startConditionTasks, h.startConditionReadAt = snapshots, now
		h.startConditionMu.Unlock()
	}
	seen := make(map[string]fleet.StartConditionResult, len(live))
	for _, task := range live {
		result := fleet.EvaluateStartConditions(task.Request.StartConditions, snapshots, now)
		switch result.Status {
		case "met":
			if released, err := h.inbox.store.releaseWaitingSpawn(ctx, task, now); err == nil && released {
				h.logger.Info("hub spawn released from its start condition", "request_id", task.Request.RequestID, "job", task.Job)
			}
		case "cancelled":
			outcome := hubSpawnOutcome{RC: 84, Stage: "queue", Reason: spawnOutcomeReason("start_condition_cancelled", result.Reason), Fix: "Nothing was started: a task this request waits on was dropped or closed as a duplicate. Submit the spawn again without that condition.", E6Arm: task.Request.E6Arm}
			h.endWaitingSpawn(ctx, task, "cancelled", outcome, now)
		default:
			seen[task.Request.RequestID] = result
		}
	}
	h.startConditionMu.Lock()
	h.startConditionSeen = seen
	h.startConditionMu.Unlock()
}

func (h *HubServer) endWaitingSpawn(ctx context.Context, task hubSpawnTask, status string, outcome hubSpawnOutcome, now time.Time) {
	changed, err := h.inbox.store.endWaitingSpawn(ctx, task, status, outcome, now)
	if err != nil || !changed {
		return
	}
	h.logger.Warn("hub spawn ended while waiting", "request_id", task.Request.RequestID, "job", task.Job, "status", status, "reason", outcome.Reason)
}

// spawnConditionsNeedHK reports whether judging the conditions reads hk: a
// time condition alone does not.
func spawnConditionsNeedHK(conditions []fleet.StartCondition) bool {
	for _, condition := range conditions {
		if condition.Kind != "time" {
			return true
		}
	}
	return false
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

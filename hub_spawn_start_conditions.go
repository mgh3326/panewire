package panewire

// Start conditions through the spawn queue (hk 4736; design
// docs/design 2026-11-13/hub-owned-spawn-queue §4, operator decision 188).
//
// A request submitted with --after holds status waiting until its predicates
// hold, then returns to the ordinary pending path as accepted. One scheduler
// check reads every waiting request's task ids through handoffkeep's batched
// snapshot route, so the number of hk reads follows the ids (chunks of
// fleet.TaskStateSnapshotChunk), never the number of requests. A task id hk
// names in missing is a refusal, not an endless wait; an hk failure keeps every
// row waiting and logs once until a read answers again.
//
// See docs/hub-spawn-operations.md#start-conditions.

import (
	"context"
	"encoding/json"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// waitingSpawnPoll is how often the coordinator asks hk about waiting rows
// (design §9a: 60 s, unmeasured and replaced by a change feed in phase 3).
const waitingSpawnPoll = 60 * time.Second

// waitingSpawnPageLimit bounds one page of the waiting scan; the check gathers
// every page so one hk read covers all of them.
const waitingSpawnPageLimit = 256

// checkWaitingSpawns runs one start-condition check over every waiting row,
// rate-limited to waitingSpawnPoll by the hub's clock. A row is released to
// accepted when its conditions hold, refused when a predicate names a task hk
// does not have, cancelled when the task was dropped or duplicated, and given
// up when its time limit passed.
func (h *HubServer) checkWaitingSpawns(ctx context.Context) {
	if h.inbox == nil {
		return
	}
	now := h.now().UTC()
	if !h.beginWaitingSpawnCheck(now) {
		return
	}
	requests, err := h.inbox.store.waitingSpawnRequests(ctx)
	if err != nil {
		h.loopNoteSilentError(loopSpawnCoordinator, "hub_spawn_start_conditions.go:waitingSpawnRequests", err)
		return
	}
	h.loopClearSilent("hub_spawn_start_conditions.go:waitingSpawnRequests")
	if len(requests) == 0 {
		return
	}
	h.loopAddMade(loopSpawnCoordinator, len(requests))
	// Expiry needs no hk data: a waiting row is given up at its limit even
	// while the condition read is failing.
	conditions := make([][]fleet.StartCondition, 0, len(requests))
	active := make([]hubSpawnTask, 0, len(requests))
	for _, task := range requests {
		if !task.Expires.IsZero() && !now.Before(task.Expires) {
			if _, err := h.inbox.store.giveUpWaitingSpawn(ctx, task, now); err != nil {
				h.logger.Warn("hub spawn waiting expiry not recorded", "request_id", task.Request.RequestID, "job", task.Job, "error", err)
			} else {
				h.loopAddDone(loopSpawnCoordinator, 1)
			}
			continue
		}
		active = append(active, task)
		conditions = append(conditions, task.Request.StartConditions)
	}
	if len(active) == 0 {
		return
	}
	snapshot, err := readStartConditionSnapshot(ctx, conditions)
	if err != nil {
		h.noteWaitingSpawnReadFailure(err)
		return
	}
	h.clearWaitingSpawnReadFailure()
	released := false
	for _, task := range active {
		result := fleet.EvaluateStartConditionSnapshot(task.Request.StartConditions, snapshot, now)
		switch result.Status {
		case "met":
			changed, err := h.inbox.store.releaseWaitingSpawn(ctx, task, now)
			if err != nil {
				h.logger.Warn("hub spawn start condition not released", "request_id", task.Request.RequestID, "job", task.Job, "error", err)
			} else if changed {
				h.loopAddDone(loopSpawnCoordinator, 1)
			}
			released = released || changed
		case "waiting":
			if err := h.inbox.store.recordWaitingSpawnResult(ctx, task, result); err != nil {
				h.logger.Warn("hub spawn start condition result not recorded", "request_id", task.Request.RequestID, "job", task.Job, "error", err)
			}
		case "refused":
			h.endWaitingSpawn(ctx, task, "refused", result.Reason, 78)
			h.loopAddDone(loopSpawnCoordinator, 1)
		case "cancelled":
			h.endWaitingSpawn(ctx, task, "cancelled", result.Reason, 84)
			h.loopAddDone(loopSpawnCoordinator, 1)
		}
	}
	if released {
		h.kickSpawnPlacement()
	}
}

// readStartConditionSnapshot reads every task the predicates name, then the
// blocking tasks a blockers predicate's target reveals in that first snapshot,
// so one check settles task and blockers predicates alike. An hk failure is
// returned as is: no condition is decided from a partial answer.
func readStartConditionSnapshot(ctx context.Context, requests [][]fleet.StartCondition) (fleet.TaskStateSnapshot, error) {
	snapshot, err := fleet.ReadTaskStateSnapshot(ctx, fleet.StartConditionTaskIDs(requests))
	if err != nil {
		return fleet.TaskStateSnapshot{}, err
	}
	known := make(map[string]bool, len(snapshot.Tasks)+len(snapshot.Missing))
	for id := range snapshot.Tasks {
		known[id] = true
	}
	for _, id := range snapshot.Missing {
		known[id] = true
	}
	var blockers []string
	for _, conditions := range requests {
		for _, condition := range conditions {
			if condition.Kind != "blockers" {
				continue
			}
			for _, id := range snapshot.Tasks[condition.TargetTaskID].Blockers {
				if !known[id] {
					known[id] = true
					blockers = append(blockers, id)
				}
			}
		}
	}
	if len(blockers) == 0 {
		return snapshot, nil
	}
	extra, err := fleet.ReadTaskStateSnapshot(ctx, blockers)
	if err != nil {
		return fleet.TaskStateSnapshot{}, err
	}
	for id, state := range extra.Tasks {
		snapshot.Tasks[id] = state
	}
	snapshot.Missing = append(snapshot.Missing, extra.Missing...)
	return snapshot, nil
}

// endWaitingSpawn records a terminal start-condition outcome for one row.
func (h *HubServer) endWaitingSpawn(ctx context.Context, task hubSpawnTask, status, reason string, rc int) {
	fix := "Nothing was started. Correct the --after predicate or submit again without it."
	if status == "cancelled" {
		fix = "Nothing was started. The task an --after predicate named was dropped or duplicated; submit again without that predicate."
	}
	if _, err := h.inbox.store.endWaitingSpawn(ctx, task, status, reason, fix, rc, h.now().UTC()); err != nil {
		h.logger.Warn("hub spawn start condition outcome not recorded", "request_id", task.Request.RequestID, "job", task.Job, "status", status, "error", err)
	}
}

// beginWaitingSpawnCheck records this check's time and reports whether it may
// run; the rate limit keeps a failing hk from being asked every coordinator
// tick.
func (h *HubServer) beginWaitingSpawnCheck(now time.Time) bool {
	h.waitingMu.Lock()
	defer h.waitingMu.Unlock()
	if !h.waitingCheckedAt.IsZero() && now.Before(h.waitingCheckedAt.Add(waitingSpawnPoll)) {
		return false
	}
	h.waitingCheckedAt = now
	return true
}

// noteWaitingSpawnReadFailure logs one hk read failure per outage, so a
// waiting queue does not fill the log while hk is down.
func (h *HubServer) noteWaitingSpawnReadFailure(err error) {
	h.waitingMu.Lock()
	defer h.waitingMu.Unlock()
	if h.waitingReadFailed {
		return
	}
	h.waitingReadFailed = true
	h.logger.Warn("hub spawn start conditions unavailable; waiting requests keep waiting", "error", err)
}

func (h *HubServer) clearWaitingSpawnReadFailure() {
	h.waitingMu.Lock()
	defer h.waitingMu.Unlock()
	h.waitingReadFailed = false
}

// waitingSpawnRequests returns every durable waiting row, oldest first. Pages
// keep each read bounded; the list is gathered so one snapshot covers every
// request in the check.
func (s *laneInboxStore) waitingSpawnRequests(parent context.Context) ([]hubSpawnTask, error) {
	ctx, done := s.op(parent)
	defer done()
	var tasks []hubSpawnTask
	var afterCreated int64
	var afterID string
	paged := false
	for {
		query := `SELECT ` + spawnTaskColumns + ` FROM spawn_operations WHERE status='waiting' AND node_id IS NULL`
		var args []any
		if paged {
			query += ` AND (created_ms>? OR (created_ms=? AND request_id>?))`
			args = append(args, afterCreated, afterCreated, afterID)
		}
		query += ` ORDER BY created_ms,request_id LIMIT ?`
		args = append(args, waitingSpawnPageLimit)
		rows, err := s.db.QueryContext(ctx, s.q(query), args...)
		if err != nil {
			return nil, err
		}
		page := 0
		for rows.Next() {
			task, err := scanSpawnTask(rows)
			if err != nil {
				rows.Close()
				return nil, err
			}
			tasks = append(tasks, task)
			afterCreated, afterID = task.Created.UnixMilli(), task.Request.RequestID
			page++
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
		if page < waitingSpawnPageLimit {
			return tasks, nil
		}
		paged = true
	}
}

// releaseWaitingSpawn moves a waiting row to the ordinary pending path with a
// fresh attempt deadline; the coordinator's next scan picks it up.
func (s *laneInboxStore) releaseWaitingSpawn(parent context.Context, task hubSpawnTask, now time.Time) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	deadline := spawnAttemptDeadline(now, task.Request.DeadlineSeconds, task.Expires)
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET status='accepted',deadline_ms=?,last_run_ms=0,conditions_met_ms=?,condition_result='' WHERE request_id=? AND attempt=? AND status='waiting' AND node_id IS NULL`), deadline.UnixMilli(), now.UnixMilli(), task.Request.RequestID, task.Attempt)
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	return n == 1, nil
}

// giveUpWaitingSpawn ends a waiting row at its time limit. Its reservation and
// owner notice follow the same path as an attempt that was given up.
func (s *laneInboxStore) giveUpWaitingSpawn(parent context.Context, task hubSpawnTask, now time.Time) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	outcome := spawnGivenUpOutcome(task, spawnPinnedHost(task))
	raw, err := json.Marshal(outcome)
	if err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, s.q(`UPDATE spawn_operations SET status='given_up',result=? WHERE request_id=? AND attempt=? AND status='waiting' AND node_id IS NULL`), string(raw), task.Request.RequestID, task.Attempt)
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

// endWaitingSpawn records a refused or cancelled start-condition outcome: the
// row is terminal, its reservation is freed and its owner is told once.
func (s *laneInboxStore) endWaitingSpawn(parent context.Context, task hubSpawnTask, status, reason, fix string, rc int, now time.Time) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	outcome := hubSpawnOutcome{RC: rc, Stage: "queue", Reason: reason, Fix: fix, E6Arm: task.Request.E6Arm, Host: spawnPinnedHost(task)}
	raw, err := json.Marshal(outcome)
	if err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, s.q(`UPDATE spawn_operations SET status=?,result=? WHERE request_id=? AND attempt=? AND status='waiting' AND node_id IS NULL`), status, string(raw), task.Request.RequestID, task.Attempt)
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

// recordWaitingSpawnResult stores the last evaluation of a waiting row, so
// GET /v1/spawns can name the predicates that still block it.
func (s *laneInboxStore) recordWaitingSpawnResult(parent context.Context, task hubSpawnTask, result fleet.StartConditionResult) error {
	ctx, done := s.op(parent)
	defer done()
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET condition_result=? WHERE request_id=? AND attempt=? AND status='waiting'`), string(raw), task.Request.RequestID, task.Attempt)
	return err
}

// returnToWaiting puts a conditioned row that became executable without
// passing the queue back into it: a legacy row, a replay, a node refusal that
// unbound it, or a staged activation. A row the queue already released keeps
// its conditions_met_ms and is never moved, so this cannot re-wait a request
// whose condition holds.
func (s *laneInboxStore) returnToWaiting(parent context.Context, task hubSpawnTask) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET status='waiting' WHERE request_id=? AND attempt=? AND conditions_met_ms=0 AND node_id IS NULL AND status IN ('accepted','deadline_exceeded','deadline_exceeded_grant')`), task.Request.RequestID, task.Attempt)
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	return n == 1, nil
}

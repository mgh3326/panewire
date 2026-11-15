package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// The request time limit of the spawn queue (hk 4660; hk
// design/2026-11-13/hub-owned-spawn-queue §3 rule 2). See
// docs/hub-spawn-operations.md#time-limit. Start conditions through the queue
// are a later part.

// spawnExpiryField is the expires_ms a job.spawn frame carries: the request's
// absolute expiry when the target node advertised that it reads the field,
// else zero so an older node's strict decoder never sees it (hk 4747). The
// node writes it into the job.spawned receipt as expires_at.
func spawnExpiryField(capabilities map[string]bool, expires time.Time) int64 {
	if !capabilities[hubCapabilitySpawnExpires] || expires.IsZero() {
		return 0
	}
	return expires.UnixMilli()
}

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

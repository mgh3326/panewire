// Spawn-failure hk task comments (hk 5137).
//
// When a spawn whose request names an hk task (--task) reaches a final
// failure — failed or refused — the hub posts one comment to that hk task:
//
//	spawn failed request=<request_id> rc=<rc> stage=<stage> reason=<reason>
//
// That is the line the wake skill asks sessions to write by hand; hk parses
// the prefix, so the hub writing it keeps the stuck-work wait (hk 5128)
// accurate without a session in the loop. A retryable failure is never
// final, so it never posts: only a final outcome does, and a spawn later
// replayed to success under the same request id posts nothing.
// deadline_exceeded is not final — a late node result can still land it as
// succeeded — so it posts nothing; if the row is later finished as failed
// (finishUnownedSpawn or otherwise), the failed post fires then, once.
//
// The single hook is the sweep below, run by the spawn coordinator next to
// the notification flush. It finds final-failure rows whose mark is unset,
// posts each through the hub's handoffkeep client, and records the posted-at
// mark (task_fail_notice_ms) on the operation. The mark is what makes the
// post idempotent across restarts: a row the hub stopped after posting keeps
// its repost suppressed only once the mark write lands, so the guarantee is
// exactly-once per mark write and at-least-once across a crash (or an hk
// answer lost after hk stored the write — the comments route takes no
// idempotency key, and nothing can read back posted comments to reconcile).
// A row judged not owed (no --task, or an outcome that is no longer a final
// failure) is marked too, so the sweep never reconsiders it. Rows that turn
// final under this hub always post: rows that were already terminal when the
// mark column landed are stamped judged once at migration time and never
// post. A failed post keeps its mark unset and retries on a later sweep with
// a growing per-row delay persisted on the operation (30s doubling to 10m):
// each due row costs one hk attempt per sweep, and not-due rows are skipped
// without an hk call, so an hk outage costs one bounded sweep and cheap skips
// instead of stalling scheduling. The mark and the retry delay are written
// detached from the sweep budget, so a spent budget or a shutting-down
// coordinator cannot lose bookkeeping for a post that already succeeded. One
// warn is logged per failure streak, so an outage does not fill the log and
// is never silent. Without an hk client the sweep judges only failures older
// than the hub's start, so enabling hk later never floods old tasks.
package panewire

import (
	"context"
	"net/http"
	"strconv"
	"time"
)

// spawnTaskFailNoticeLimit bounds one sweep's hk comments, so a backlog of
// unposted failures cannot hold the coordinator tick.
const spawnTaskFailNoticeLimit = 32

// spawnTaskFailSweepBudget bounds one sweep's hk posts. A sweep that reaches
// it stops and leaves the rest pending; the per-row next-attempt delay spaces
// retries after that, so an hk outage costs one bounded sweep and cheap skips
// afterwards instead of stalling scheduling.
const spawnTaskFailSweepBudget = 30 * time.Second

// spawnTaskFailBackoffDefault is the first retry delay for a row hk did not
// take. It doubles per consecutive failure to spawnTaskFailBackoffMax.
const spawnTaskFailBackoffDefault = 30 * time.Second

// spawnTaskFailBackoffMax caps a row's retry delay.
const spawnTaskFailBackoffMax = 10 * time.Minute

// spawnTaskFailText is the hk task comment for a terminal spawn failure. The
// wake skill's hand-written line has exactly this shape, and hk parses the
// "spawn failed" prefix.
func spawnTaskFailText(requestID string, outcome hubSpawnOutcome) string {
	return "spawn failed request=" + requestID + " rc=" + strconv.Itoa(outcome.RC) + " stage=" + outcome.Stage + " reason=" + outcome.Reason
}

// pendingSpawnTaskFailNoticeIDs lists the oldest operations whose final
// failure still owes its hk task comment: a failed or refused row the sweep
// has not judged yet whose retry delay has passed. deadline_exceeded is not
// final and never appears here; if such a row is later finished as failed,
// that failed row is listed then. The due filter runs in the query, before
// LIMIT, so backed-off rows never monopolize the page and starve newer
// failures.
func (s *laneInboxStore) pendingSpawnTaskFailNoticeIDs(parent context.Context, now time.Time, limit int) ([]string, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT request_id FROM spawn_operations WHERE status IN ('failed','refused') AND task_fail_notice_ms=0 AND (task_fail_retry_ms=0 OR task_fail_retry_ms<=?) ORDER BY created_ms LIMIT ?`), now.UnixMilli(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// markSpawnTaskFailNotice records that the sweep judged a final failure:
// posted its hk task comment, or found nothing owed. It answers whether this
// call set the mark, so a concurrent sweep cannot post the same failure
// twice. The write runs under context.WithoutCancel: a mark whose post
// already succeeded must still land when the sweep budget is spent or the
// coordinator is shutting down; the store's own op timeout still bounds it.
func (s *laneInboxStore) markSpawnTaskFailNotice(parent context.Context, requestID string, now time.Time) (bool, error) {
	ctx, done := s.op(context.WithoutCancel(parent))
	defer done()
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET task_fail_notice_ms=? WHERE request_id=? AND task_fail_notice_ms=0`), now.UnixMilli(), requestID)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n == 1, err
}

// deferSpawnTaskFailNotice records a row's next attempt after hk did not take
// its comment. The sweep's due filter keeps the row out of later pages until
// then, so backed-off rows never starve newer failures. Like the mark, it
// runs under context.WithoutCancel so a spent sweep budget cannot lose it.
func (s *laneInboxStore) deferSpawnTaskFailNotice(parent context.Context, requestID string, next time.Time) (bool, error) {
	ctx, done := s.op(context.WithoutCancel(parent))
	defer done()
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET task_fail_retry_ms=? WHERE request_id=? AND task_fail_notice_ms=0`), next.UnixMilli(), requestID)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n == 1, err
}

// judgeSpawnTaskFailNoticesBefore marks unjudged final failures created
// before cutover as judged without posting, and answers how many it judged.
// The no-hk sweep calls it with the hub's start time, so enabling hk later
// posts only failures from the configured lifetime, never a backlog whose
// tasks have moved on.
func (s *laneInboxStore) judgeSpawnTaskFailNoticesBefore(parent context.Context, cutover time.Time) (int64, error) {
	ctx, done := s.op(context.WithoutCancel(parent))
	defer done()
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET task_fail_notice_ms=? WHERE task_fail_notice_ms=0 AND status IN ('failed','refused') AND created_ms<?`), cutover.UnixMilli(), cutover.UnixMilli())
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// backfillSpawnTaskFailNotices stamps failed and refused rows that predate
// the posted-at mark as judged, once per store. Without it the first sweep
// after deploy would post a spawn-failed comment for every historical
// failure. The guard row and the UPDATE commit together, so a hub that stops
// half way redoes the backfill and a finished one never rescans. Rows that
// turn final later keep mark 0 and post normally, so a restart between a
// final commit and its post never loses the comment.
func (s *laneInboxStore) backfillSpawnTaskFailNotices(parent context.Context, now time.Time) error {
	ctx, done := s.op(parent)
	defer done()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS spawn_task_fail_backfill (singleton INTEGER PRIMARY KEY CHECK(singleton=1))`); err != nil {
		return err
	}
	marked, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO spawn_task_fail_backfill(singleton) VALUES(1)`)
	if err != nil {
		return err
	}
	if n, _ := marked.RowsAffected(); n == 1 {
		if _, err := tx.ExecContext(ctx, s.q(`UPDATE spawn_operations SET task_fail_notice_ms=? WHERE task_fail_notice_ms=0 AND status IN ('failed','refused')`), now.UnixMilli()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// spawnTaskFailNoticePermanent reports whether an hk comment answer ends
// retries: the request is malformed, names no task, or is already recorded.
// Anything else — an outage, or a refusal the backoff may outlast — stays
// pending for a later sweep.
func spawnTaskFailNoticePermanent(status int) bool {
	switch status {
	case http.StatusBadRequest, http.StatusNotFound, http.StatusConflict, http.StatusGone, http.StatusUnprocessableEntity:
		return true
	}
	return false
}

// flushSpawnTaskFailNotices posts the hk task comment for every final spawn
// failure that still owes one and whose retry delay has passed. A sweep that
// finds nothing due costs one indexed status read; rows in their retry delay
// are filtered by the query before LIMIT, so backed-off rows never monopolize
// the page and starve newer failures, and an hk outage cannot hold the
// coordinator loop past one bounded sweep.
func (h *HubServer) flushSpawnTaskFailNotices(ctx context.Context) {
	if h.inbox == nil {
		return
	}
	if h.handoffkeep == nil {
		// No hk client to post through: failures from before this hub
		// started are judged without posting, so enabling hk later does not
		// flood old tasks. Failures from this hub's own lifetime stay owed
		// and post once a client exists.
		if judged, err := h.inbox.store.judgeSpawnTaskFailNoticesBefore(ctx, h.startedAt); err != nil {
			h.logger.Warn("hub spawn task comment backlog not judged", "error", err)
		} else if judged > 0 {
			h.logger.Info("hub spawn task comment backlog judged without posting: no hk client", "rows", judged)
		}
		return
	}
	ids, err := h.inbox.store.pendingSpawnTaskFailNoticeIDs(ctx, h.now().UTC(), spawnTaskFailNoticeLimit)
	if err != nil || len(ids) == 0 {
		return
	}
	sweepCtx, cancel := context.WithTimeout(ctx, spawnTaskFailSweepBudget)
	defer cancel()
	for _, id := range ids {
		if sweepCtx.Err() != nil {
			return
		}
		h.postSpawnTaskFailNotice(sweepCtx, id)
	}
}

// spawnTaskFailDelay backs a row off after hk did not take its comment. The
// next attempt is stored on the operation, so the due filter keeps it out of
// later sweeps and a hub restart respects the delay instead of retrying at
// once. The delay doubles per consecutive failure to spawnTaskFailBackoffMax.
func (h *HubServer) spawnTaskFailDelay(ctx context.Context, id string) {
	base := h.spawnTaskFailBackoffBase
	if base <= 0 {
		base = spawnTaskFailBackoffDefault
	}
	h.spawnTaskFailMu.Lock()
	if h.spawnTaskFailFails == nil {
		h.spawnTaskFailFails = map[string]int{}
	}
	h.spawnTaskFailFails[id]++
	failures := h.spawnTaskFailFails[id]
	h.spawnTaskFailMu.Unlock()
	delay := base << (failures - 1)
	if delay <= 0 || delay > spawnTaskFailBackoffMax {
		delay = spawnTaskFailBackoffMax
	}
	if _, err := h.inbox.store.deferSpawnTaskFailNotice(ctx, id, h.now().UTC().Add(delay)); err != nil {
		h.logger.Warn("hub spawn task comment retry not recorded", "request_id", id, "error", err)
	}
}

// spawnTaskFailReset clears a row's retry count after its comment posted (or
// the row was judged).
func (h *HubServer) spawnTaskFailReset(id string) {
	h.spawnTaskFailMu.Lock()
	defer h.spawnTaskFailMu.Unlock()
	delete(h.spawnTaskFailFails, id)
}

// postSpawnTaskFailNotice posts one final failure's hk task comment and
// marks the operation judged. Only a durable mark suppresses a repost: the
// sweep re-reads the row, so anything that is no longer failed or refused —
// including a deadline_exceeded row a late node result landed, which posts
// nothing — is judged without posting.
func (h *HubServer) postSpawnTaskFailNotice(ctx context.Context, requestID string) {
	// One sweep owns a request id at a time; the durable mark owns it across
	// sweeps and restarts.
	if !h.spawnActions.begin(requestID) {
		return
	}
	defer h.spawnActions.end(requestID)
	task, err := h.inbox.store.spawnTask(ctx, requestID)
	if err != nil {
		return
	}
	if task.Status != "failed" && task.Status != "refused" {
		_, _ = h.inbox.store.markSpawnTaskFailNotice(ctx, requestID, h.now().UTC())
		h.spawnTaskFailReset(requestID)
		return
	}
	// The value slot of another flag must never read as --task: a purpose of
	// "--task" followed by digits would otherwise misattribute the failure.
	target, err := strconv.ParseInt(hubSpawnFlagArg(task.Request.Args, "--task"), 10, 64)
	if err != nil || target < 1 {
		// No hk task to tell: judge the row so the sweep never reconsiders it.
		_, _ = h.inbox.store.markSpawnTaskFailNotice(ctx, requestID, h.now().UTC())
		h.spawnTaskFailReset(requestID)
		return
	}
	body := spawnTaskFailText(requestID, task.Outcome)
	// One hk attempt per sweep: the sweep's own cadence plus the persisted
	// per-row delay is the retry backoff, so a due row costs one attempt, not
	// a 10s hubretry budget of the coordinator loop. The comments route takes
	// no idempotency key: an hk answer lost after hk stored the write can
	// still duplicate on the next sweep, and a hub that stops between the
	// post and the mark reposts as well. Both windows are at-least-once by
	// necessity; the mark suppresses every repost the hub can observe.
	status, postErr := h.handoffkeep.postTaskComment(ctx, target, body)
	if postErr != nil {
		// The attempt never got an answer. Treat it like a 502/503: the row
		// stays pending and the per-row delay spaces the next sweep's retry.
		status = http.StatusServiceUnavailable
	}
	switch {
	case status == http.StatusOK || status == http.StatusCreated:
		if _, err := h.inbox.store.markSpawnTaskFailNotice(ctx, requestID, h.now().UTC()); err != nil {
			h.noteSpawnTaskFailFailure(requestID, target, err)
			h.spawnTaskFailDelay(ctx, requestID)
			return
		}
		h.spawnTaskFailReset(requestID)
		h.clearSpawnTaskFailFailure()
	case spawnTaskFailNoticePermanent(status):
		// hk will never take this comment. Drop it loudly, not silently: the
		// mark records the judgement and the warn carries the reason.
		if _, err := h.inbox.store.markSpawnTaskFailNotice(ctx, requestID, h.now().UTC()); err != nil {
			h.noteSpawnTaskFailFailure(requestID, target, err)
			h.spawnTaskFailDelay(ctx, requestID)
			return
		}
		h.spawnTaskFailReset(requestID)
		h.spawnTaskFailMu.Lock()
		h.spawnTaskFailWarned = false
		h.spawnTaskFailMu.Unlock()
		h.logger.Warn("hub spawn task comment refused by hk; not retried", "request_id", requestID, "task", target, "status", status)
	default:
		h.noteSpawnTaskFailFailure(requestID, target, errSpawnTaskFailHTTP(status))
		h.spawnTaskFailDelay(ctx, requestID)
	}
}

// errSpawnTaskFailHTTP names an hk comment answer that stays pending.
func errSpawnTaskFailHTTP(status int) error {
	return &spawnTaskFailHTTPError{status: status}
}

type spawnTaskFailHTTPError struct {
	status int
}

func (e *spawnTaskFailHTTPError) Error() string {
	return "handoffkeep comment not posted: HTTP " + strconv.Itoa(e.status)
}

// noteSpawnTaskFailFailure logs one warn per failure streak, so an hk outage
// holds the log to its first failure while every unposted comment stays
// pending for a later sweep.
func (h *HubServer) noteSpawnTaskFailFailure(requestID string, task int64, err error) {
	h.spawnTaskFailMu.Lock()
	defer h.spawnTaskFailMu.Unlock()
	if h.spawnTaskFailWarned {
		return
	}
	h.spawnTaskFailWarned = true
	h.logger.Warn("hub spawn task comment not posted; retrying", "request_id", requestID, "task", task, "error", err)
}

// clearSpawnTaskFailFailure ends the streak the next post closes, so the
// outage after it warns again.
func (h *HubServer) clearSpawnTaskFailFailure() {
	h.spawnTaskFailMu.Lock()
	defer h.spawnTaskFailMu.Unlock()
	h.spawnTaskFailWarned = false
}

// Spawn-failure hk task comments (hk 5137).
//
// When a spawn whose request names an hk task (--task) reaches a terminal
// failure — failed, refused or deadline_exceeded — the hub posts one comment
// to that hk task:
//
//	spawn failed request=<request_id> rc=<rc> stage=<stage> reason=<reason>
//
// That is the line the wake skill asks sessions to write by hand; hk parses
// the prefix, so the hub writing it keeps the stuck-work wait (hk 5128)
// accurate without a session in the loop. A retryable failure is never
// terminal, so it never posts: only a final outcome does, and a spawn later
// replayed to success under the same request id posts nothing.
//
// The single hook is the sweep below, run by the spawn coordinator next to
// the notification flush. It finds terminal-failure rows whose mark is unset,
// posts each through the hub's handoffkeep client, and records the posted-at
// mark (task_fail_notice_ms) on the operation. The mark is what makes the
// post idempotent across restarts; a row judged not owed (no --task, or an
// outcome that is no longer a terminal failure) is marked too, so the sweep
// never reconsiders it. A failed post keeps its mark unset and retries on a
// later sweep with the hub's backoff (hubretry); one warn is logged per
// failure streak, so an hk outage does not fill the log and is never silent.

package panewire

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/mgh3326/panewire/internal/hubretry"
)

// spawnTaskFailNoticeLimit bounds one sweep's hk comments, so a backlog of
// unposted failures cannot hold the coordinator tick.
const spawnTaskFailNoticeLimit = 32

// spawnTaskFailText is the hk task comment for a terminal spawn failure. The
// wake skill's hand-written line has exactly this shape, and hk parses the
// "spawn failed" prefix.
func spawnTaskFailText(requestID string, outcome hubSpawnOutcome) string {
	return "spawn failed request=" + requestID + " rc=" + strconv.Itoa(outcome.RC) + " stage=" + outcome.Stage + " reason=" + outcome.Reason
}

// pendingSpawnTaskFailNoticeIDs lists the oldest operations whose terminal
// failure still owes its hk task comment: a failed, refused or
// deadline_exceeded row the sweep has not judged yet.
func (s *laneInboxStore) pendingSpawnTaskFailNoticeIDs(parent context.Context, limit int) ([]string, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT request_id FROM spawn_operations WHERE status IN ('failed','refused','deadline_exceeded') AND task_fail_notice_ms=0 ORDER BY created_ms LIMIT ?`), limit)
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

// markSpawnTaskFailNotice records that the sweep judged a terminal failure:
// posted its hk task comment, or found nothing owed. It answers whether this
// call set the mark, so a concurrent sweep cannot post the same failure
// twice.
func (s *laneInboxStore) markSpawnTaskFailNotice(parent context.Context, requestID string, now time.Time) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET task_fail_notice_ms=? WHERE request_id=? AND task_fail_notice_ms=0`), now.UnixMilli(), requestID)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n == 1, err
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

// flushSpawnTaskFailNotices posts the hk task comment for every terminal
// spawn failure that still owes one. A sweep that finds nothing pending costs
// one indexed status read.
func (h *HubServer) flushSpawnTaskFailNotices(ctx context.Context) {
	if h.inbox == nil || h.handoffkeep == nil {
		return
	}
	ids, err := h.inbox.store.pendingSpawnTaskFailNoticeIDs(ctx, spawnTaskFailNoticeLimit)
	if err != nil || len(ids) == 0 {
		return
	}
	for _, id := range ids {
		if ctx.Err() != nil {
			return
		}
		h.postSpawnTaskFailNotice(ctx, id)
	}
}

// postSpawnTaskFailNotice posts one terminal failure's hk task comment and
// marks the operation judged. Only a durable mark suppresses a repost: the
// sweep re-reads the row, so a failure superseded since (a deadline_exceeded
// row a late node result landed, or any row that left its terminal failure)
// is judged without posting.
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
	if task.Status != "failed" && task.Status != "refused" && task.Status != "deadline_exceeded" {
		_, _ = h.inbox.store.markSpawnTaskFailNotice(ctx, requestID, h.now().UTC())
		return
	}
	target, err := strconv.ParseInt(hubSpawnArg(task.Request.Args, "--task"), 10, 64)
	if err != nil || target < 1 {
		// No hk task to tell: judge the row so the sweep never reconsiders it.
		_, _ = h.inbox.store.markSpawnTaskFailNotice(ctx, requestID, h.now().UTC())
		return
	}
	body := spawnTaskFailText(requestID, task.Outcome)
	status, _, _ := hubretry.Do(ctx, h.spawnTaskFailRetry, func(ctx context.Context) (int, int, error) {
		got, postErr := h.handoffkeep.postTaskComment(ctx, target, body)
		if postErr != nil {
			// The attempt never got an answer. Surface it as 503 so the
			// hub's backoff retries it exactly like a 502/503.
			return 0, http.StatusServiceUnavailable, nil
		}
		return got, got, nil
	}, nil)
	switch {
	case status == http.StatusOK || status == http.StatusCreated:
		if _, err := h.inbox.store.markSpawnTaskFailNotice(ctx, requestID, h.now().UTC()); err != nil {
			h.noteSpawnTaskFailFailure(requestID, target, err)
			return
		}
		h.clearSpawnTaskFailFailure()
	case spawnTaskFailNoticePermanent(status):
		// hk will never take this comment. Drop it loudly, not silently: the
		// mark records the judgement and the warn carries the reason.
		if _, err := h.inbox.store.markSpawnTaskFailNotice(ctx, requestID, h.now().UTC()); err != nil {
			h.noteSpawnTaskFailFailure(requestID, target, err)
			return
		}
		h.spawnTaskFailMu.Lock()
		h.spawnTaskFailWarned = false
		h.spawnTaskFailMu.Unlock()
		h.logger.Warn("hub spawn task comment refused by hk; not retried", "request_id", requestID, "task", target, "status", status)
	default:
		h.noteSpawnTaskFailFailure(requestID, target, errSpawnTaskFailHTTP(status))
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

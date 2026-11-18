package panewire

// A bound request past its deadline is not final: a late node result can still
// land it as succeeded, or the stale probe can finish it. Until one of those
// happens the owner is told nothing (completeSpawn). Past the stale-probe
// horizon (spawnStuckAfter, the same 30 minutes the stuck warning uses) with no
// final answer, the owner gets one unresolved SPAWN_FAILED and the hub keeps
// ownership. A landing that follows gets one correcting SPAWNED under the same
// request id. Each notice has a fixed event id, so a hub restart never sends it
// twice (hk 5267).

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/getsentry/sentry-go"
)

// spawnUnresolvedStatus is the event-id status of the unresolved notice. It is
// not a stored operation status: the row stays deadline_exceeded.
const spawnUnresolvedStatus = "deadline_unresolved"

// spawnUnresolvedReason is the reason the unresolved SPAWN_FAILED carries.
const spawnUnresolvedReason = "deadline_exceeded_unresolved"

// spawnUnresolvedEventID is the outbox event id of task's attempt's unresolved
// notice. The attempt is in it, so a later attempt gets its own notice.
func spawnUnresolvedEventID(task hubSpawnTask) string {
	return fmt.Sprintf("%s:%d:%s", task.Request.RequestID, task.Attempt, spawnUnresolvedStatus)
}

// spawnUnresolvedOutcome is the notice payload: rc 124 with ownership kept, and
// replay=no because the saved request may already be running on its node.
func spawnUnresolvedOutcome(task hubSpawnTask) hubSpawnOutcome {
	return hubSpawnOutcome{
		RC:     124,
		Stage:  "landing",
		Reason: spawnUnresolvedReason,
		Host:   spawnTaskHost(task),
		Fix:    "The outcome is unknown and the hub keeps ownership of the request. A late landing is reported afterwards under this request id; if none is, inspect the saved operation and the executing node before retrying.",
	}
}

// noteUnresolvedSpawn queues the unresolved notice for a bound request whose
// deadline passed the stale-probe horizon with no final answer. It is a no-op
// unless the row is still deadline_exceeded, so a late landing that committed
// first queues nothing here. It reports whether the notice is queued.
func (s *laneInboxStore) noteUnresolvedSpawn(parent context.Context, task hubSpawnTask, now time.Time) (bool, error) {
	if task.Node == nil {
		return false, nil
	}
	ctx, done := s.op(parent)
	defer done()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	// The row's write lock comes first, so the status check and the outbox
	// write are one decision: completeSpawn's landing UPDATE waits behind it,
	// then sees this notice and is corrected.
	result, err := tx.ExecContext(ctx, s.q(`UPDATE spawn_operations SET status=status WHERE request_id=? AND attempt=? AND node_id=? AND status IN ('deadline_exceeded','deadline_exceeded_grant')`), task.Request.RequestID, task.Attempt, task.Node.RequestID)
	if err != nil {
		return false, err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return false, nil
	}
	raw, err := json.Marshal(spawnUnresolvedOutcome(task))
	if err != nil {
		return false, err
	}
	if err = queueSpawnNoticeRows(ctx, s, tx, task, spawnUnresolvedEventID(task), string(raw), now, true); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// settleCompletedSpawn settles a bound deadline_exceeded request as succeeded
// when hk records its job completed on the request's node, with a pane, under
// the request's task id. The landing receipt never reached the hub before the
// deadline, so this completed job is the landing proof (hk 5267, second case).
// It needs no node connection: it reads the job record only. It reports whether
// the request was settled, and the landing then follows the normal SPAWNED path.
func (h *HubServer) settleCompletedSpawn(parent context.Context, task hubSpawnTask) bool {
	if task.Node == nil {
		return false
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil || app.CheckTrust() != nil {
		return false
	}
	readCtx, cancel := context.WithTimeout(parent, 5*time.Second)
	record, exists, err := app.ExistingJob(readCtx, task.Job)
	cancel()
	if err != nil || !exists || record["state"] != "completed" {
		return false
	}
	pane := spawnRecordString(record, "pane_id")
	if pane == "" || spawnRecordString(record, "host") != task.Node.Machine {
		return false
	}
	// The completed job must carry the request's task identity, or it is not
	// this request's landing (hk 5267 review).
	if !spawnRecordMatchesRequest(task, record) {
		return false
	}
	// Job ids are reusable once a request ends, and hk never rewrites a terminal
	// record, so an older completed record can carry this job's id. Only a
	// completion at or after this request's creation is its landing (mergewake
	// review, hk 5267). A missing or unparseable stamp fails closed, and is logged.
	if !spawnRecordCompletedSince(record, task.Created) {
		h.logger.Warn("hub did not settle a spawn from a completed job without a usable completion time", "request_id", task.Request.RequestID, "job", task.Job)
		return false
	}
	outcome := hubSpawnOutcome{RC: 0, Stage: "landing", Host: task.Node.Machine, Pane: pane, Profile: spawnRecordString(record, "profile"), Model: spawnRecordString(record, "model"), E6Arm: task.Request.E6Arm}
	changed, err := h.inbox.store.completeSpawn(parent, task, outcome, h.now())
	if err != nil || !changed {
		return false
	}
	h.recordSpawnTrace(task, outcome)
	h.logger.Info("hub settled a spawn from its completed job", "request_id", task.Request.RequestID, "job", task.Job, "host", task.Node.Machine, "pane", pane)
	return true
}

// spawnRecordCompletedSince reports whether the record's completion stamp is
// at or after created. A missing or unparseable stamp reports false.
func spawnRecordCompletedSince(record map[string]any, created time.Time) bool {
	at, err := time.Parse(time.RFC3339Nano, spawnRecordString(record, "completed_at"))
	return err == nil && !at.Before(created)
}

// spawnRecordMatchesRequest reports whether a completed job record is this
// request's landing. With --task, the record's task_id must equal it. A
// --parent-job request has no --task: the node claims the child under the
// parent's task (internal/fleet/spawn.go), and the child's own record names the
// parent it was spawned from (jobs.go parent_job). That field is checked,
// not the parent's record, because a parent may exist only in the node's local
// ledger (mergewake review, hk 5267). A request with neither refuses.
func spawnRecordMatchesRequest(task hubSpawnTask, record map[string]any) bool {
	if want := hubSpawnFlagArg(task.Request.Args, "--task"); want != "" {
		return spawnRecordTaskID(record) == want
	}
	parentJob := hubSpawnFlagArg(task.Request.Args, "--parent-job")
	if parentJob == "" {
		return false
	}
	return spawnRecordString(record, "parent_job") == parentJob && spawnRecordTaskID(record) != ""
}

// spawnRecordTaskID renders a job record's task id as a plain decimal. Job
// records decode JSON numbers as float64, which prints large ids in exponent
// form (1e+06) and would never match the --task argument. A number at or past
// 2^53 may already be rounded (9007199254740993 reads back as ...992), so it
// is refused rather than matched: the settle fails closed (mergewake review).
func spawnRecordTaskID(record map[string]any) string {
	switch v := record["task_id"].(type) {
	case float64:
		if v >= 1<<53 || v < 0 {
			return ""
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	case string:
		return v
	case nil:
		return ""
	default:
		return fmt.Sprint(v)
	}
}

// correctionAwaitsUnresolved reports whether pending row n is a correcting
// SPAWNED whose unresolved notice, for the same recipient, is still undelivered.
// Such a correction waits so the lane never reads the success before the
// failure it corrects (hk 5267 review). Other rows are never held.
func (h *HubServer) correctionAwaitsUnresolved(ctx context.Context, n spawnNotification) bool {
	var result hubSpawnOutcome
	if json.Unmarshal([]byte(n.raw), &result) != nil || !result.AfterUnresolved {
		return false
	}
	attempt := spawnNoticeAttempt(n.id)
	base := fmt.Sprintf("%s:%d:", n.request, attempt)
	// The correction's id is <request>:<attempt>:succeeded, plus ":<lane>" for a
	// submitter copy; the unresolved id carries the same suffix.
	suffix := strings.TrimPrefix(n.id, base+"succeeded")
	unresolved := base + spawnUnresolvedStatus + suffix
	store := h.inbox.store
	op, done := store.op(ctx)
	defer done()
	var delivered int
	err := store.db.QueryRowContext(op, store.q(`SELECT delivered FROM spawn_notifications WHERE event_id=?`), unresolved).Scan(&delivered)
	if errors.Is(err, sql.ErrNoRows) {
		return false // explicitly absent: nothing to wait for
	}
	if err != nil {
		return true // unknown: keep waiting, never release on a lookup error
	}
	return delivered != spawnNoticeDone
}

func spawnRecordString(record map[string]any, key string) string {
	s, _ := record[key].(string)
	return s
}

// recordUnresolvedSpawnError emits the SPAWN_FAILED error event for an
// unresolved notice that was just queued. It claims the notice's own
// telemetry mark, so a repeat probe emits nothing (hk 5267).
func (h *HubServer) recordUnresolvedSpawnError(task hubSpawnTask) {
	if h.spawnSentryClient() == nil || h.inbox == nil {
		return
	}
	ctx, done := h.inbox.store.op(h.closeCtx)
	defer done()
	recorded, err := h.inbox.store.db.ExecContext(ctx, h.inbox.store.q(`UPDATE spawn_notifications SET telemetry_recorded=1 WHERE event_id=? AND telemetry_recorded=0`), spawnUnresolvedEventID(task))
	if err != nil {
		return
	}
	if n, _ := recorded.RowsAffected(); n != 1 {
		return
	}
	reason := spawnTraceReason(spawnUnresolvedReason)
	hub := sentry.NewHub(h.spawnSentry, sentry.NewScope())
	hub.Scope().SetTag("job", task.Job)
	hub.CaptureEvent(&sentry.Event{Level: sentry.LevelError, Message: "SPAWN_FAILED landing/" + reason, Fingerprint: []string{"spawn", "landing", reason}, Tags: map[string]string{"stage": "landing", "reason": reason, "request": task.Request.RequestID, "attempt": strconv.Itoa(task.Attempt)}})
}

// spawnUnresolvedNoticed reports whether task's attempt already has its
// unresolved notice in the outbox, read inside completeSpawn's transaction.
func spawnUnresolvedNoticed(ctx context.Context, s *laneInboxStore, tx *sql.Tx, task hubSpawnTask) (bool, error) {
	var count int
	if err := tx.QueryRowContext(ctx, s.q(`SELECT count(*) FROM spawn_notifications WHERE event_id=?`), spawnUnresolvedEventID(task)).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

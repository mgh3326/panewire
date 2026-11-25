package panewire

import (
	"context"
	"database/sql"
	"io"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// A daemon-started reviewer (`--role reviewer`, job id `mwr-*`) that posts its
// result and stops without running `done` keeps a claimed or spawned hk record
// and a lane row. The node's ended-job pane sweep needs a terminal record, and
// lane release needs the pane gone, so neither takes it and the node's lane cap
// fills (hk 4658). This pass writes the end of such a job; it never touches the
// pane. Closing the pane belongs to the node, once the record is terminal
// (docs/pane-sweep.md). docs/hub-spawn-operations.md states the rule.
//
// The only proof is a POSTED RESULT: a non-null findings object or a report on
// disk (JSON null is absent). An idle or done pane is not proof, because an
// idle reviewer can be waiting on a background command, a wakeup or an answer.
// The hub asks the job's node to write the record (job.mark_lost with
// end_reason reviewer_result_posted); the node re-proves, under the job's
// operation lock, the reviewer role, the absence of an announced quota stop, a
// pane that is not working and a result unchanged for reviewerResultSettle
// (nodeJobMarkReviewerResultPosted). A refused or failed write is retried on
// every pass until it lands.
//
// The candidates are the persisted succeeded reviewer operations, not the
// nodes' active-job heartbeat: that view drops panes idle for 30 minutes, caps
// each node at 32 jobs and forgets a spawn after two minutes, which would miss
// exactly the long-orphaned jobs this pass exists for.
//
// The record is written lost, not completed: completing a job uploads its
// report through `panewire done`, which needs the job directory on the node
// and belongs to the job itself or to mergewake's reap.

// reviewerRole is the --role value of a review seat. A reviewer holds no
// task/role reservation (hk 4737); the name still keys the spawn args and the
// node's reviewer end proof.
const reviewerRole = "reviewer"

// reviewerEndInterval is how often the hub looks for a reviewer job that posted
// its result and was never ended.
const reviewerEndInterval = 5 * time.Minute

// runReviewerEnd runs endPostedReviewerJobs once per reviewerEndInterval until
// ctx ends.
func (h *HubServer) runReviewerEnd(ctx context.Context) {
	ticker := time.NewTicker(reviewerEndInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.endPostedReviewerJobs(ctx)
		}
	}
}

// reviewerEndWindow bounds how far back the pass looks for a succeeded
// reviewer operation. The pass lists every succeeded operation in the window: a
// limit on the newest rows would displace an older orphan from every pass.
const reviewerEndWindow = 14 * 24 * time.Hour

// reviewerEndPage is how many operations one full-row read fetches (a variable
// so a test can force a second read).
var reviewerEndPage = 500

// succeededSpawnIDs lists the request ids of the succeeded operations created
// since `since`, newest first. It reads no payload: on MySQL (index
// spawn_operations_created) and on SQLite and Postgres (spawn_operations_succeeded)
// the ids come from the index alone. It is one read, not pages: MySQL sorted the
// whole range again for every LIMIT page (hk 6394), so N rows cost N times the
// page count.
func (s *laneInboxStore) succeededSpawnIDs(parent context.Context, since time.Time) ([]string, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT request_id FROM spawn_operations WHERE status='succeeded' AND created_ms>=? ORDER BY created_ms DESC, request_id DESC`), since.UnixMilli())
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

// succeededSpawnsByID reads the full rows of the named operations that are
// still succeeded, in the order of ids. The caller keeps the reviewers: no text
// match on the stored request is a safe filter, because argv takes the role as
// `--role reviewer` or `--role=reviewer`.
func (s *laneInboxStore) succeededSpawnsByID(parent context.Context, ids []string) ([]hubSpawnTask, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	ctx, done := s.op(parent)
	defer done()
	marks := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT request_id,`+spawnTaskColumns+` FROM spawn_operations WHERE status='succeeded' AND request_id IN (`+marks+`)`), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	byID := make(map[string]hubSpawnTask, len(ids))
	for rows.Next() {
		var id string
		task, err := scanSpawnTask(requestIDScanner{rows: rows, id: &id})
		if err != nil {
			return nil, err
		}
		byID[id] = task
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	tasks := make([]hubSpawnTask, 0, len(byID))
	for _, id := range ids {
		if task, ok := byID[id]; ok {
			tasks = append(tasks, task)
		}
	}
	return tasks, nil
}

// requestIDScanner reads a leading request_id column into id and hands the
// rest to scanSpawnTask.
type requestIDScanner struct {
	rows *sql.Rows
	id   *string
}

func (r requestIDScanner) Scan(dest ...any) error {
	return r.rows.Scan(append([]any{r.id}, dest...)...)
}

// reviewerEndRead is what the last pass read from the store: the operations it
// listed by id, and the ones it then read in full (payload included).
type reviewerEndRead struct{ Listed, Full int }

// endPostedReviewerJobs ends every succeeded reviewer spawn whose job posted a
// result and whose hk record is still claimed or spawned, looking at every
// succeeded operation in the window.
//
// An operation the pass has judged needs no more reads while it stays in the
// window (reviewerEndSkip): a spawn that is not a reviewer, and a reviewer job
// found ended. Only the rest is read in full, so a pass costs one id list plus
// the live reviewers, not the 11 KB payload of every operation of 14 days.
func (h *HubServer) endPostedReviewerJobs(ctx context.Context) {
	if h.inbox == nil {
		return
	}
	since := h.now().Add(-reviewerEndWindow)
	seen := map[string]bool{}
	var app *fleet.App
	h.loopTick(loopReviewerEnd)
	ids, err := h.inbox.store.succeededSpawnIDs(ctx, since)
	if err != nil {
		h.loopNoteSilentError(loopReviewerEnd, "hub_spawn_reviewer_end.go:succeededSpawns", err)
		h.logger.Warn("reviewer end pass skipped", "reason", "store_unavailable")
		return
	}
	h.loopClearSilent("hub_spawn_reviewer_end.go:succeededSpawns")
	read := reviewerEndRead{Listed: len(ids)}
	pending := h.reviewerEndUnjudged(ids)
	for len(pending) > 0 {
		page := pending[:min(reviewerEndPage, len(pending))]
		pending = pending[len(page):]
		tasks, err := h.inbox.store.succeededSpawnsByID(ctx, page)
		if err != nil {
			h.loopNoteSilentError(loopReviewerEnd, "hub_spawn_reviewer_end.go:succeededSpawns", err)
			h.logger.Warn("reviewer end pass skipped", "reason", "store_unavailable")
			return
		}
		read.Full += len(tasks)
		for _, holder := range tasks {
			request := holder.Request.RequestID
			if holder.Status != "succeeded" || !validNodeJobName(holder.Job) {
				h.skipReviewerEnd(request)
				continue
			}
			spawn, err := fleet.ValidateSubmission(holder.Request)
			if err != nil || spawn.Role != reviewerRole {
				h.skipReviewerEnd(request)
				continue
			}
			seen[holder.Job] = true
			host := spawnHolderHost(holder)
			if host == "" || h.reviewerEndSettledJob(holder.Job) || !h.nodeConnected(host) {
				// Already ended, or the node cannot answer: nothing to read or
				// write, and the next pass looks again (an ended job no more).
				if h.reviewerEndSettledJob(holder.Job) {
					h.skipReviewerEnd(request)
				}
				continue
			}
			if app == nil {
				if app, err = h.newSpawnApp(io.Discard, io.Discard); err != nil {
					h.logger.Warn("reviewer end pass skipped", "reason", "app_unavailable")
					return
				}
			}
			h.endPostedReviewerJob(ctx, app, spawn, holder)
			if h.reviewerEndSettledJob(holder.Job) {
				h.skipReviewerEnd(request)
			}
		}
	}
	h.retainReviewerEnd(seen, ids)
	h.mu.Lock()
	h.reviewerEndLastRead = read
	h.mu.Unlock()
}

// reviewerEndUnjudged is ids without the operations the pass already judged.
func (h *HubServer) reviewerEndUnjudged(ids []string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	pending := make([]string, 0, len(ids))
	for _, id := range ids {
		if !h.reviewerEndSkip[id] {
			pending = append(pending, id)
		}
	}
	return pending
}

// skipReviewerEnd records that the operation needs no further read.
func (h *HubServer) skipReviewerEnd(request string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.reviewerEndSkip == nil {
		h.reviewerEndSkip = map[string]bool{}
	}
	h.reviewerEndSkip[request] = true
}

// nodeConnected reports that machine's node has an open connection.
func (h *HubServer) nodeConnected(machine string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	record := h.nodes[machine]
	return record != nil && record.agent != nil
}

// endPostedReviewerJob ends one reviewer job if it posted a result. Any check
// that cannot be read keeps the record live for the next pass.
func (h *HubServer) endPostedReviewerJob(ctx context.Context, app *fleet.App, spawn fleet.Spawn, holder hubSpawnTask) {
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	record, exists, err := app.ExistingJob(readCtx, holder.Job)
	cancel()
	if err != nil || !exists {
		return
	}
	name, host := heldAgentName(holder, record), spawnHolderHost(holder)
	state, _ := record["state"].(string)
	if state == "lost" && record["end_reason"] == reviewerResultPosted {
		// This end's hk write landed but its ledger event may not have (a crash
		// or failure between the two): the node finishes it, and only then is
		// the job settled. Without the event the pane sweep never sees the end.
		if validNodeJobName(name) {
			h.finishReviewerEnd(ctx, app, spawn, holder, name, host)
		}
		return
	}
	if state != "claimed" && state != "spawned" {
		// Terminal (or unknown to this pass): never read again while it stays
		// in the window.
		h.markReviewerEndSettled(holder.Job)
		return
	}
	if !validNodeJobName(name) {
		return
	}
	var ledger nodeJobResult
	if !h.nodeCall(ctx, host, nodeCommandJobResult, nodeJobResultArgs{Job: holder.Job}, spawnHeldPaneProbeWait, &ledger) {
		h.logReviewerEndOnce(holder.Job, "result_unreadable", false, "hub kept a reviewer job", "host", host)
		return
	}
	if !ledger.Found || !nodeResultPosted(ledger) {
		// The ordinary state of a reviewer still at work: nothing to log.
		return
	}
	h.finishReviewerEnd(ctx, app, spawn, holder, name, host)
}

// finishReviewerEnd asks the node to write the job's end (hk record and ledger
// event, node_job_mark_lost_result_posted.go) and logs the outcome.
func (h *HubServer) finishReviewerEnd(ctx context.Context, app *fleet.App, spawn fleet.Spawn, holder hubSpawnTask, name, host string) {
	marked, reason := h.markHolderJobLostBecause(ctx, app, holder, name, reviewerResultPosted)
	if marked {
		h.forgetReviewerEndLog(holder.Job)
		h.markReviewerEndSettled(holder.Job)
		if reason == nodeJobMarkLostCompleted {
			// Another terminal record (the job's own `done`) won; hk holds it.
			return
		}
		h.logger.Info("hub ended a reviewer job with a posted result", "end_reason", reviewerResultPosted,
			"role_key", spawn.RoleKey(), "job", holder.Job, "holder_request", holder.Request.RequestID, "holder_host", host)
		return
	}
	switch reason {
	case nodeJobMarkLostQuotaStopped:
		h.logReviewerEndOnce(holder.Job, reason, false, "hub kept a reviewer job", "host", host)
	case nodeJobMarkLostResultMissing, nodeJobMarkLostResultSettling, nodeJobMarkLostPaneWorking, nodeJobMarkLostBusy:
		// The reviewer is still finishing (its files are new or changing, its
		// pane works, or `done` holds the lock): the next pass asks again, and
		// nothing is logged.
	case nodeJobMarkLostCompleted:
		// The job completed itself, or hk holds another terminal record, between
		// the hk read and the node's check.
	default:
		h.logReviewerEndOnce(holder.Job, reason, true, "hub could not end a reviewer job", "host", host)
	}
}

// logReviewerEndOnce logs a refusal for job once per distinct reason: a pass
// that fails the same way again stays quiet, and a changed reason logs again.
func (h *HubServer) logReviewerEndOnce(job, reason string, warn bool, message string, attrs ...any) {
	h.mu.Lock()
	if h.reviewerEndLogged == nil {
		h.reviewerEndLogged = map[string]string{}
	}
	repeat := h.reviewerEndLogged[job] == reason
	h.reviewerEndLogged[job] = reason
	h.mu.Unlock()
	if repeat {
		return
	}
	attrs = append([]any{"reason", reason, "job", job}, attrs...)
	if warn {
		h.logger.Warn(message, attrs...)
		return
	}
	h.logger.Info(message, attrs...)
}

func (h *HubServer) forgetReviewerEndLog(job string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.reviewerEndLogged, job)
}

func (h *HubServer) reviewerEndSettledJob(job string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.reviewerEndSettled[job]
}

func (h *HubServer) markReviewerEndSettled(job string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.reviewerEndSettled == nil {
		h.reviewerEndSettled = map[string]bool{}
	}
	h.reviewerEndSettled[job] = true
}

// retainReviewerEnd forgets the jobs the pass no longer read and the judged
// operations that left the window, so every map stays bounded by the pass's
// window.
func (h *HubServer) retainReviewerEnd(seen map[string]bool, window []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	inWindow := make(map[string]bool, len(window))
	for _, id := range window {
		inWindow[id] = true
	}
	for id := range h.reviewerEndSkip {
		if !inWindow[id] {
			delete(h.reviewerEndSkip, id)
		}
	}
	for job := range h.reviewerEndLogged {
		if !seen[job] {
			delete(h.reviewerEndLogged, job)
		}
	}
	for job := range h.reviewerEndSettled {
		if !seen[job] {
			delete(h.reviewerEndSettled, job)
		}
	}
}

package panewire

import (
	"context"
	"io"
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

const (
	// reviewerEndWindow bounds how far back the pass looks for a succeeded
	// reviewer operation; reviewerEndLimit bounds the rows one pass reads.
	reviewerEndWindow = 14 * 24 * time.Hour
	reviewerEndLimit  = 1000
)

// succeededReviewerSpawns lists the succeeded operations created since `since`
// whose request mentions a reviewer, newest first. The role is confirmed from the
// validated request by the caller; the match only narrows the rows read.
func (s *laneInboxStore) succeededReviewerSpawns(parent context.Context, since time.Time, limit int) ([]hubSpawnTask, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+spawnTaskColumns+` FROM spawn_operations WHERE status='succeeded' AND created_ms>=? AND payload LIKE ? ORDER BY created_ms DESC LIMIT ?`),
		since.UnixMilli(), "%"+reviewerRole+"%", limit)
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

// endPostedReviewerJobs ends every succeeded reviewer spawn whose job posted a
// result and whose hk record is still claimed or spawned.
func (h *HubServer) endPostedReviewerJobs(ctx context.Context) {
	if h.inbox == nil {
		return
	}
	tasks, err := h.inbox.store.succeededReviewerSpawns(ctx, h.now().Add(-reviewerEndWindow), reviewerEndLimit)
	if err != nil {
		h.logger.Warn("reviewer end pass skipped", "reason", "store_unavailable")
		return
	}
	seen := map[string]bool{}
	var app *fleet.App
	for _, holder := range tasks {
		if holder.Status != "succeeded" || !validNodeJobName(holder.Job) {
			continue
		}
		spawn, err := fleet.ValidateSubmission(holder.Request)
		if err != nil || spawn.Role != reviewerRole {
			continue
		}
		seen[holder.Job] = true
		host := spawnHolderHost(holder)
		if host == "" || h.reviewerEndSettledJob(holder.Job) || !h.nodeConnected(host) {
			// Already seen terminal, or the node cannot answer: nothing to
			// read or write, and the next pass looks again.
			continue
		}
		if app == nil {
			if app, err = h.newSpawnApp(io.Discard, io.Discard); err != nil {
				h.logger.Warn("reviewer end pass skipped", "reason", "app_unavailable")
				return
			}
		}
		h.endPostedReviewerJob(ctx, app, spawn, holder)
	}
	h.retainReviewerEnd(seen)
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
	if state, _ := record["state"].(string); state != "claimed" && state != "spawned" {
		// Terminal (or unknown to this pass): never read again while it stays
		// in the window.
		h.markReviewerEndSettled(holder.Job)
		return
	}
	name, host := heldAgentName(holder, record), spawnHolderHost(holder)
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
	marked, reason := h.markHolderJobLostBecause(ctx, app, holder, name, reviewerResultPosted)
	if marked {
		h.forgetReviewerEndLog(holder.Job)
		h.markReviewerEndSettled(holder.Job)
		h.logger.Info("hub ended a reviewer job with a posted result", "end_reason", reviewerResultPosted,
			"role_key", spawn.RoleKey(), "job", holder.Job, "holder_request", holder.Request.RequestID, "holder_host", host,
			"findings_posted", len(ledger.Findings) > 0 && !isJSONNull(ledger.Findings), "report_posted", ledger.Report.Present)
		return
	}
	switch reason {
	case nodeJobMarkLostQuotaStopped:
		h.logReviewerEndOnce(holder.Job, reason, false, "hub kept a reviewer job", "host", host)
	case nodeJobMarkLostResultMissing, nodeJobMarkLostResultSettling, nodeJobMarkLostPaneWorking, nodeJobMarkLostBusy:
		// The reviewer is still finishing (its files are new, its pane works, or
		// `done` holds the lock): the next pass asks again, and nothing is logged.
	case nodeJobMarkLostCompleted:
		// The job completed itself between the hk read and the node's check;
		// hk holds the terminal record by the next pass.
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

// retainReviewerEnd forgets the jobs the pass no longer read, so both maps stay
// bounded by the pass's window.
func (h *HubServer) retainReviewerEnd(seen map[string]bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
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

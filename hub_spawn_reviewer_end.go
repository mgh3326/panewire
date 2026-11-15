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
// end_reason reviewer_result_posted); the node re-proves the result, the
// reviewer role and the absence of an announced quota stop from its own job
// directory (reviewerResultPostedProof). A failed write is retried on every
// pass until it lands.
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

// endPostedReviewerJobs ends every live reviewer spawn whose job posted a
// result and whose hk record is still claimed or spawned. A live operation is
// one a node still reports active, so an ended job drops out on the node's next
// heartbeat and is not visited again.
func (h *HubServer) endPostedReviewerJobs(ctx context.Context) {
	if h.inbox == nil {
		return
	}
	tasks, err := h.liveSpawnTasks(ctx)
	if err != nil {
		h.logger.Warn("reviewer end pass skipped", "reason", "store_unavailable")
		return
	}
	seen := map[string]bool{}
	var app *fleet.App
	for _, holder := range tasks {
		if holder.Status != "succeeded" || spawnHolderHost(holder) == "" || !validNodeJobName(holder.Job) {
			continue
		}
		spawn, err := fleet.ValidateSubmission(holder.Request)
		if err != nil || spawn.Role != reviewerRole {
			continue
		}
		seen[holder.Job] = true
		if app == nil {
			if app, err = h.newSpawnApp(io.Discard, io.Discard); err != nil {
				h.logger.Warn("reviewer end pass skipped", "reason", "app_unavailable")
				return
			}
		}
		h.endPostedReviewerJob(ctx, app, spawn, holder)
	}
	h.retainReviewerEndLog(seen)
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
		h.logger.Info("hub ended a reviewer job with a posted result", "end_reason", reviewerResultPosted,
			"role_key", spawn.RoleKey(), "job", holder.Job, "holder_request", holder.Request.RequestID, "holder_host", host,
			"findings_posted", len(ledger.Findings) > 0 && !isJSONNull(ledger.Findings), "report_posted", ledger.Report.Present)
		return
	}
	if reason == nodeJobMarkLostQuotaStopped {
		h.logReviewerEndOnce(holder.Job, reason, false, "hub kept a reviewer job", "host", host)
		return
	}
	h.logReviewerEndOnce(holder.Job, reason, true, "hub could not end a reviewer job", "host", host)
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

// retainReviewerEndLog forgets the jobs the pass no longer visited, so the map
// stays bounded by the live reviewers.
func (h *HubServer) retainReviewerEndLog(seen map[string]bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for job := range h.reviewerEndLogged {
		if !seen[job] {
			delete(h.reviewerEndLogged, job)
		}
	}
}

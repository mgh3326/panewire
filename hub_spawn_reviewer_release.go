package panewire

import (
	"context"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// A reviewer pane that finished, or stopped at a startup prompt, and stays open
// keeps the (task, review pass) reservation held, so every later review of the
// task is refused until the desk closes the pane by hand (hk 3315). A new
// submission for the same key releases a reviewer holder when its pane is not
// working and has stayed so past the grace.
//
// A reviewer that finishes without running `done` (the common case for a
// daemon-started `mwr-*` reviewer) otherwise leaves an idle pane, a live job
// record and a lane row behind, and the node's ended-job sweep never sees it:
// the lane cap fills and new lanes are refused `lane_quota_exceeded` (hk 4658).
// The unattended pass that ends such a reviewer on the hub's own schedule moved
// to hk 4684; this file keeps the submission path only.
//
// An idle pane is not proof that the work ended: a reviewer that asked a
// question is idle too. The close therefore carries the job id, and the node
// rules out an open question from its own lane-event records at the moment of
// the close (fleet.OpenLaneQuestion); a question the watcher has not copied yet
// cannot be raced. Implementers and consults are never released this way: an
// idle worker may be waiting on a decision.
//
// The node's agent list carries no time of the last state change, so the grace
// runs from the holder's spawn deadline, as releaseUnworkedSpawn's does. Any
// check that fails or cannot be read keeps the reservation.

const (
	reviewerRole = "reviewer"

	// The reason names double as the INFO line's branch.
	reviewerPaneFinished = "reviewer_pane_finished"
	reviewerPaneStuck    = "reviewer_pane_stuck"
)

// releaseFinishedReviewerPane closes a reviewer holder's not-working pane and
// marks its hk job lost, so the caller can release the reservation. probe is
// the caller's read of the holder's pane; only a present pane listed under the
// holder's own pane id and agent label counts. closed is false when any check kept
// the hold. When the grace alone kept it (every other check passed), remaining
// is the time until the grace ends, so the 409 can say when to retry; it is
// zero for any other hold.
func (h *HubServer) releaseFinishedReviewerPane(ctx context.Context, app *fleet.App, request fleet.Submission, spawn fleet.Spawn, holder hubSpawnTask, record map[string]any, probe heldProbe) (closed bool, branch string, remaining time.Duration) {
	if spawn.Role != reviewerRole || holder.Request.RequestID == request.RequestID {
		return false, "", 0
	}
	return h.releaseFinishedReviewerHolder(ctx, app, spawn, holder, record, probe, "submission", request.RequestID)
}

// releaseFinishedReviewerHolder is the release rule: the holder is a reviewer
// spawn in succeeded or failed state, its hk record still claimed or spawned,
// and its pane present under its own id and label with a closable status past
// the grace. source names the caller in the INFO line; submitter is the
// submitting request there, "" for a caller with none. The node refuses the
// close if the job's own records show an unanswered question.
func (h *HubServer) releaseFinishedReviewerHolder(ctx context.Context, app *fleet.App, spawn fleet.Spawn, holder hubSpawnTask, record map[string]any, probe heldProbe, source, submitter string) (closed bool, branch string, remaining time.Duration) {
	if spawn.Role != reviewerRole || probe.state != heldPanePresent {
		return false, "", 0
	}
	switch holder.Status {
	case "succeeded":
		branch = reviewerPaneFinished
	case "failed":
		branch = reviewerPaneStuck
	default:
		return false, "", 0
	}
	name, row := heldAgentName(holder, record), probe.row
	if state, _ := record["state"].(string); state != "claimed" && state != "spawned" {
		return false, "", 0
	}
	if !validNodeJobName(name) || !validNodeJobName(holder.Job) || row.PaneID != holder.Outcome.Pane || row.Name != name || !validNodeCommandPath(row.Cwd) {
		return false, "", 0
	}
	// working is never closed; a status pane.close does not accept (blank for
	// a plain shell, or one herdr adds later) cannot be proven finished.
	if !nodePaneClosableStatus[row.AgentStatus] {
		return false, "", 0
	}
	if remaining = holder.Deadline.Add(h.unworkedLandingGrace).Sub(h.now()); remaining > 0 {
		return false, "", remaining
	}
	// The end_reason records why the record was written, so a lost reviewer job
	// says whether its pane finished or stopped at a startup prompt. The job id
	// makes the node rule out an open question before it closes.
	closedPane, reason := h.closeHeldPaneAndMarkLostBecause(ctx, app, holder, name, row, branch, holder.Job)
	if !closedPane {
		switch reason {
		case "question_open":
			h.logger.Info("hub kept a reviewer pane",
				"reason", reason, "source", source, "role_key", spawn.RoleKey(),
				"job", holder.Job, "holder_host", spawnHolderHost(holder), "holder_pane", row.PaneID)
		case "capture_failed", "close_unreadable", "pane_changed":
		default:
			// The close ran and the record write did not: the pane is gone, so
			// the next submission's absent-pane check retries this write.
			h.logger.Warn("hub closed a reviewer pane but could not mark the job",
				"reason", reason, "source", source, "job", holder.Job, "holder_host", spawnHolderHost(holder))
		}
		return false, "", 0
	}
	attrs := []any{"branch", branch, "source", source, "role_key", spawn.RoleKey(), "job", holder.Job}
	if submitter != "" {
		// The submission that triggered the release; a caller with none passes "".
		attrs = append(attrs, "request", submitter)
	}
	attrs = append(attrs, "holder_request", holder.Request.RequestID, "holder_job", holder.Job, "holder_status", holder.Status,
		"holder_host", spawnHolderHost(holder), "holder_pane", row.PaneID, "pane_status", row.AgentStatus)
	h.logger.Info("hub closed a finished reviewer pane", attrs...)
	return true, branch, 0
}

// retryAfterSeconds is the whole seconds a caller should wait for a hold that
// frees after remaining, rounded up so a retry never lands inside the hold.
// It is at least 1 for any remaining above zero.
func retryAfterSeconds(remaining time.Duration) int {
	return int((remaining + time.Second - 1) / time.Second)
}

package panewire

import (
	"context"
	"io"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// A reviewer pane that finished, or stopped at a startup prompt, and stays open
// keeps the (task, review pass) reservation held, so every later review of the
// task is refused until the desk closes the pane by hand (hk 3315). Two callers
// apply one rule:
//
//   - a new submission for the same key releases a reviewer holder when its
//     pane is not working and has stayed so past the grace;
//   - the hub's own pass (reviewerReleaseInterval) does the same for every live
//     reviewer spawn whose hk job never reached a terminal state. A reviewer
//     that finishes without running `done` (the common case for a daemon-started
//     `mwr-*` reviewer) otherwise leaves an idle pane, a live job record and a
//     lane row behind, and the node's ended-job sweep never sees it: the lane
//     cap fills and new lanes are refused `lane_quota_exceeded` (hk 4658).
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

	// reviewerReleaseInterval is how often the hub looks for a finished
	// reviewer pane that no submission has reclaimed. It is unrelated to the
	// ended-job pane sweep's interval: that pass needs a terminal record, and
	// this one is what writes it.
	reviewerReleaseInterval = 5 * time.Minute
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

// releaseFinishedReviewerHolder is the rule both callers share: the holder is a
// reviewer spawn in succeeded or failed state, its hk record still claimed or
// spawned, and its pane present under its own id and label with a closable
// status past the grace. source names the caller in the INFO line; submitter is
// the submitting request there, "" for the sweep. The node refuses the close if
// the job's own records show an unanswered question.
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
		if reason == "question_open" {
			h.logger.Info("hub kept a reviewer pane",
				"reason", reason, "source", source, "role_key", spawn.RoleKey(),
				"job", holder.Job, "holder_host", spawnHolderHost(holder), "holder_pane", row.PaneID)
		}
		return false, "", 0
	}
	attrs := []any{"branch", branch, "source", source, "role_key", spawn.RoleKey(), "job", holder.Job}
	if submitter != "" {
		// The submission that triggered the release; the sweep has none.
		attrs = append(attrs, "request", submitter)
	}
	attrs = append(attrs, "holder_request", holder.Request.RequestID, "holder_job", holder.Job, "holder_status", holder.Status,
		"holder_host", spawnHolderHost(holder), "holder_pane", row.PaneID, "pane_status", row.AgentStatus)
	h.logger.Info("hub closed a finished reviewer pane", attrs...)
	return true, branch, 0
}

// runReviewerPaneRelease runs releaseFinishedReviewerSpawns once per
// reviewerReleaseInterval until ctx ends.
func (h *HubServer) runReviewerPaneRelease(ctx context.Context) {
	ticker := time.NewTicker(reviewerReleaseInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.releaseFinishedReviewerSpawns(ctx)
		}
	}
}

// releaseFinishedReviewerSpawns releases every live reviewer spawn whose pane
// finished and whose hk job is still claimed or spawned. A live operation is
// one the hub placed whose job a node still reports active, so a released
// reviewer drops out on the next heartbeat and is not visited again. The
// reservation is left in place: the next submission for the key releases it
// through the terminal-record path, exactly as for a reviewer that wrote its
// own record.
func (h *HubServer) releaseFinishedReviewerSpawns(ctx context.Context) {
	if h.inbox == nil {
		return
	}
	tasks, err := h.liveSpawnTasks(ctx)
	if err != nil {
		h.logger.Warn("reviewer pane release skipped", "reason", "store_unavailable")
		return
	}
	type reviewer struct {
		holder hubSpawnTask
		spawn  fleet.Spawn
	}
	var reviewers []reviewer
	for _, holder := range tasks {
		if holder.Status != "succeeded" || holder.Outcome.Pane == "" {
			continue
		}
		spawn, err := fleet.ValidateSubmission(holder.Request)
		if err != nil || spawn.Role != reviewerRole {
			continue
		}
		reviewers = append(reviewers, reviewer{holder: holder, spawn: spawn})
	}
	if len(reviewers) == 0 {
		return
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		h.logger.Warn("reviewer pane release skipped", "reason", "app_unavailable")
		return
	}
	for _, r := range reviewers {
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		record, exists, readErr := app.ExistingJob(readCtx, r.holder.Job)
		cancel()
		if readErr != nil || !exists {
			continue
		}
		switch state, _ := record["state"].(string); state {
		case "claimed", "spawned":
		default:
			continue
		}
		probe := h.probeHeldPane(ctx, spawnHolderHost(r.holder), r.holder.Outcome.Pane, heldAgentName(r.holder, record))
		h.releaseFinishedReviewerHolder(ctx, app, r.spawn, r.holder, record, probe, "sweep", "")
	}
}

// retryAfterSeconds is the whole seconds a caller should wait for a hold that
// frees after remaining, rounded up so a retry never lands inside the hold.
// It is at least 1 for any remaining above zero.
func retryAfterSeconds(remaining time.Duration) int {
	return int((remaining + time.Second - 1) / time.Second)
}

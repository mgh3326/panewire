package panewire

import (
	"context"

	"github.com/mgh3326/panewire/internal/fleet"
)

// A reviewer pane that finished, or stopped at a startup prompt, and stays open
// keeps the (task, review pass) reservation held, so every later review of the
// task is refused until the desk closes the pane by hand (hk 3315). A new
// submission for the same key releases a reviewer holder when its pane is not
// working and has stayed so past the grace. Workers and testers are never
// released this way: an idle worker may be waiting on an ESC answer.
//
// The node's agent list carries no time of the last state change, so the grace
// runs from the holder's spawn deadline, as releaseUnworkedLanding's does. Any
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
// the hold.
func (h *HubServer) releaseFinishedReviewerPane(ctx context.Context, app *fleet.App, request fleet.Submission, spawn fleet.Spawn, holder hubSpawnTask, record map[string]any, probe heldProbe) (closed bool, branch string) {
	if spawn.Role != reviewerRole || holder.Request.RequestID == request.RequestID || probe.state != heldPanePresent {
		return false, ""
	}
	switch holder.Status {
	case "succeeded":
		branch = reviewerPaneFinished
	case "failed":
		branch = reviewerPaneStuck
	default:
		return false, ""
	}
	name, row := heldAgentName(holder, record), probe.row
	if state, _ := record["state"].(string); state != "claimed" && state != "spawned" {
		return false, ""
	}
	if !validNodeJobName(name) || !validNodeJobName(holder.Job) || row.PaneID != holder.Outcome.Pane || row.Name != name || !validNodeCommandPath(row.Cwd) {
		return false, ""
	}
	// working is never closed; a status pane.close does not accept (blank for
	// a plain shell, or one herdr adds later) cannot be proven finished.
	if !nodePaneClosableStatus[row.AgentStatus] || h.now().Before(holder.Deadline.Add(h.unworkedLandingGrace)) {
		return false, ""
	}
	if marked, _ := h.closeHeldPaneAndMarkLost(ctx, app, holder, name, row); !marked {
		return false, ""
	}
	h.logger.Info("hub closed a finished reviewer pane",
		"branch", branch,
		"role_key", spawn.RoleKey(),
		"request", request.RequestID, "job", spawn.Job,
		"holder_request", holder.Request.RequestID, "holder_job", holder.Job, "holder_status", holder.Status,
		"holder_host", spawnHolderHost(holder), "holder_pane", row.PaneID, "pane_status", row.AgentStatus)
	return true, branch
}

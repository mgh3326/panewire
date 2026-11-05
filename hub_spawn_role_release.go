package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// A failed spawn whose pane cleanup was not verified leaves its hk job
// `claimed` (markSpawnUncertain writes nothing to hk), so the (task, role)
// reservation would stay held for good. A new submission on that task and role
// asks the prior operation's node, through herdr, whether that pane still
// exists; only a verified absence releases the reservation.

const spawnHeldPaneProbeWait = 5 * time.Second

type heldPane int

const (
	// heldPaneUnknown covers an unreachable node, a read error, a herdr that
	// cannot list panes, and a failed operation that recorded no pane.
	heldPaneUnknown heldPane = iota
	heldPanePresent
	heldPaneAbsent
)

// probeHeldPane asks machine's node for every pane herdr lists. Only a node
// that answered with a complete pane list lacking the pane proves it absent;
// anything else keeps the reservation.
func (h *HubServer) probeHeldPane(ctx context.Context, machine, pane string) heldPane {
	if machine == "" || !nodeCommandPaneIDPattern.MatchString(pane) {
		return heldPaneUnknown
	}
	pending, id, _, sent := h.sendNodeCommand(machine, nodeCommandAgentsList, json.RawMessage("{}"))
	if !sent {
		return heldPaneUnknown
	}
	defer h.forgetNodeCommand(id)
	ctx, cancel := context.WithTimeout(ctx, spawnHeldPaneProbeWait)
	defer cancel()
	select {
	case reply := <-pending.done:
		var list nodeAgentsListResult
		if reply.Error != "" || json.Unmarshal(reply.Result, &list) != nil || !list.PanesOK {
			return heldPaneUnknown
		}
		for _, row := range append(append([]nodeAgentRow(nil), list.Agents...), list.Panes...) {
			if row.PaneID == pane {
				return heldPanePresent
			}
		}
		return heldPaneAbsent
	case <-ctx.Done():
		return heldPaneUnknown
	}
}

// releaseFailedSpawnRole frees the role held by a failed operation when its
// pane is verifiably gone: it marks the still-claimed hk job lost, then drops
// the reservation. A crash between the two repeats harmlessly, because a lost
// job already releases through the completed-or-lost path.
func (h *HubServer) releaseFailedSpawnRole(ctx context.Context, app *fleet.App, holder hubSpawnTask, record map[string]any, task, role string) (released bool, pane heldPane) {
	pane = h.probeHeldPane(ctx, spawnHolderHost(holder), holder.Outcome.Pane)
	if pane != heldPaneAbsent {
		return false, pane
	}
	if state, _ := record["state"].(string); state != "claimed" {
		return false, heldPaneUnknown
	}
	if err := app.MarkJobLost(ctx, holder.Job); err != nil {
		return false, heldPaneUnknown
	}
	return h.inbox.store.releaseCompletedSpawnRole(ctx, holder.Request.RequestID, task, role) == nil, pane
}

func spawnHolderHost(holder hubSpawnTask) string {
	if holder.Outcome.Host != "" {
		return holder.Outcome.Host
	}
	return spawnTaskHost(holder)
}

// spawnRoleHeldBody names the operation that holds the (task, role)
// reservation so the submitter can act on it instead of guessing.
func spawnRoleHeldBody(holder hubSpawnTask, pane heldPane, found bool) map[string]string {
	body := map[string]string{"error": "job_already_claimed", "fix": "inspect the existing operation; do not submit a new request while ownership is live"}
	if !found {
		return body
	}
	body["request_id"], body["job"] = holder.Request.RequestID, holder.Job
	host, paneID := spawnHolderHost(holder), holder.Outcome.Pane
	if host != "" {
		body["host"] = host
	}
	if nodeCommandPaneIDPattern.MatchString(paneID) {
		body["pane"] = paneID
	}
	if holder.Status != "failed" {
		return body
	}
	switch {
	case host == "" || body["pane"] == "":
		body["fix"] = "request " + holder.Request.RequestID + " failed without a recorded pane, so the hub cannot check it; inspect job " + holder.Job + " and mark it lost, then resubmit"
	case pane == heldPanePresent:
		body["fix"] = "close pane " + body["pane"] + " on " + host + " (job " + holder.Job + ", request " + holder.Request.RequestID + "), then resubmit"
	default:
		body["fix"] = "the hub could not read panes on " + host + "; restore that node's connection, or close pane " + body["pane"] + " there (job " + holder.Job + ", request " + holder.Request.RequestID + "), then resubmit"
	}
	return body
}

// reclaimSpawnRole retries claimSpawn after a role collision when the holder is
// finished: its hk job is completed or lost, or it failed and its pane is
// verifiably gone. When it cannot, held carries the 409 body naming the holder.
func (h *HubServer) reclaimSpawnRole(ctx context.Context, app *fleet.App, request fleet.Submission, spawn fleet.Spawn, owner string) (task hubSpawnTask, held map[string]string, err error) {
	priorID, priorJob, priorStatus, readErr := h.inbox.store.spawnRoleOwner(ctx, spawn.Task, spawn.Role)
	if readErr != nil {
		return task, spawnRoleHeldBody(hubSpawnTask{}, heldPaneUnknown, false), errSpawnClaimed
	}
	holder, holderErr := h.inbox.store.spawnTask(ctx, priorID)
	pane := heldPaneUnknown
	if holderErr == nil && (priorStatus == "succeeded" || priorStatus == "failed") {
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		record, exists, readErr := app.ExistingJob(readCtx, priorJob)
		cancel()
		state, _ := record["state"].(string)
		released := false
		switch {
		case readErr == nil && !exists && priorStatus == "failed" && holder.Outcome.Pane == "":
			// Refused before any pane existed (e.g. checkout_refused at
			// worktree.prepare): no pane was recorded and the node never
			// claimed the hk job, which it does only after a pane takes the
			// brief. Nothing can still hold the role, so release it.
			released = h.inbox.store.releaseCompletedSpawnRole(ctx, priorID, spawn.Task, spawn.Role) == nil
		case readErr != nil || !exists:
		case state == "completed" || state == "lost":
			released = h.inbox.store.releaseCompletedSpawnRole(ctx, priorID, spawn.Task, spawn.Role) == nil
		case priorStatus == "failed":
			released, pane = h.releaseFailedSpawnRole(ctx, app, holder, record, spawn.Task, spawn.Role)
		}
		if released {
			task, _, err = h.inbox.store.claimSpawn(ctx, request, spawn.Job, owner, h.now().UTC())
			if !errors.Is(err, errSpawnClaimed) {
				return task, nil, err
			}
		}
	}
	return task, spawnRoleHeldBody(holder, pane, holderErr == nil), errSpawnClaimed
}

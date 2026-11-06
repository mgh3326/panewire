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
// anything else keeps the reservation. A present pane returns its row. A
// non-empty agent name also counts as presence when any listed row carries it:
// herdr gives a pane moved across workspaces a new id, so the id alone cannot
// prove a live job gone.
func (h *HubServer) probeHeldPane(ctx context.Context, machine, pane, agent string) (heldPane, nodeAgentRow) {
	if machine == "" || !nodeCommandPaneIDPattern.MatchString(pane) {
		return heldPaneUnknown, nodeAgentRow{}
	}
	pending, id, _, sent := h.sendNodeCommand(machine, nodeCommandAgentsList, json.RawMessage("{}"))
	if !sent {
		return heldPaneUnknown, nodeAgentRow{}
	}
	defer h.forgetNodeCommand(id)
	ctx, cancel := context.WithTimeout(ctx, spawnHeldPaneProbeWait)
	defer cancel()
	select {
	case reply := <-pending.done:
		var list nodeAgentsListResult
		if reply.Error != "" || json.Unmarshal(reply.Result, &list) != nil || !list.PanesOK {
			return heldPaneUnknown, nodeAgentRow{}
		}
		// herdr lists a pane in both reads; only the agent row carries the
		// status, so it is read last and wins.
		found := nodeAgentRow{}
		for _, row := range append(append([]nodeAgentRow(nil), list.Panes...), list.Agents...) {
			if row.PaneID == pane || agent != "" && row.Name == agent {
				found = row
			}
		}
		if found.PaneID == "" {
			return heldPaneAbsent, nodeAgentRow{}
		}
		return heldPanePresent, found
	case <-ctx.Done():
		return heldPaneUnknown, nodeAgentRow{}
	}
}

// releaseFailedSpawnRole frees the role held by a failed operation when its
// pane is verifiably gone, or present but proven never to have worked
// (releaseUnworkedLanding): it marks the still-claimed hk job lost, then drops
// the reservation. A crash between the two repeats harmlessly, because a lost
// job already releases through the completed-or-lost path.
func (h *HubServer) releaseFailedSpawnRole(ctx context.Context, app *fleet.App, holder hubSpawnTask, record map[string]any, task, role string) (released bool, pane heldPane) {
	pane, row := h.probeHeldPane(ctx, spawnHolderHost(holder), holder.Outcome.Pane, "")
	if pane == heldPanePresent {
		return h.releaseUnworkedLanding(ctx, app, holder, record, row) && h.inbox.store.releaseCompletedSpawnRole(ctx, holder.Request.RequestID, task, role) == nil, pane
	}
	if pane != heldPaneAbsent {
		return false, pane
	}
	if state, _ := record["state"].(string); state != "claimed" {
		return false, heldPaneUnknown
	}
	return h.markLostAndRelease(ctx, app, holder, task, role), pane
}

// markLostAndRelease writes the hk job lost, then drops the reservation. A
// crash between the two repeats harmlessly: a lost job releases through the
// completed-or-lost path.
//
// hk never rewrites a terminal record (409), so a holder that completed after
// the caller read its state keeps its completed record. A refused write then
// re-reads it: a record now completed or lost releases the role as the
// completed-or-lost path does, anything else keeps the hold.
func (h *HubServer) markLostAndRelease(ctx context.Context, app *fleet.App, holder hubSpawnTask, task, role string) bool {
	if err := app.MarkJobLost(ctx, holder.Job); err != nil {
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		record, exists, readErr := app.ExistingJob(readCtx, holder.Job)
		cancel()
		if state, _ := record["state"].(string); readErr != nil || !exists || state != "completed" && state != "lost" {
			return false
		}
	}
	return h.inbox.store.releaseCompletedSpawnRole(ctx, holder.Request.RequestID, task, role) == nil
}

// releaseFinishedSpawnRole frees the role held by a succeeded operation whose
// hk job never reached a terminal state (a job revoked and reaped by an older
// binary, or one that ended without writing its record). Only a node that
// listed every pane and lacks the recorded one proves the job gone; a present
// pane, an unknown pane state, or no recorded pane keeps the hold. A present
// idle pane is a reviewer that finished without completing, which stays held.
// A listed pane with the holder's agent name under another id (a pane herdr
// moved across workspaces) also keeps the hold.
func (h *HubServer) releaseFinishedSpawnRole(ctx context.Context, app *fleet.App, holder hubSpawnTask, record map[string]any, task, role string) (released bool, pane heldPane) {
	pane, _ = h.probeHeldPane(ctx, spawnHolderHost(holder), holder.Outcome.Pane, heldAgentName(holder, record))
	if pane != heldPaneAbsent {
		return false, pane
	}
	if state, _ := record["state"].(string); state != "claimed" && state != "spawned" {
		return false, heldPaneUnknown
	}
	return h.markLostAndRelease(ctx, app, holder, task, role), pane
}

// heldAgentName is the herdr agent name the holder's pane carries: the label hk
// recorded at landing, else the request's -l label.
func heldAgentName(holder hubSpawnTask, record map[string]any) string {
	if label, _ := record["label"].(string); label != "" {
		return label
	}
	return hubSpawnArg(holder.Request.Args, "-l")
}

// supersedeRetryableHolders marks lost the hk job of every retryable operation
// on the same (task, role) under another job id, once the new request holds
// that role. A retryable operation (verified pre-landing cleanup) holds no hub
// reservation, because completeSpawn deletes it, and its hk job stays
// `claimed`, so nothing else would ever close that record. A replay of the
// holder's own request never reaches here: it takes the replay path first.
// Best effort: a failed hk read or write leaves the record for the next
// submission.
func (h *HubServer) supersedeRetryableHolders(ctx context.Context, app *fleet.App, request fleet.Submission, spawn fleet.Spawn) {
	if spawn.Task == "" || spawn.ParentJob != "" {
		return
	}
	holders, err := h.inbox.store.retryableSpawns(ctx)
	if err != nil {
		return
	}
	for _, holder := range holders {
		prior, err := fleet.ValidateSubmission(holder.Request)
		if err != nil || prior.Task != spawn.Task || prior.Role != spawn.Role || holder.Request.RequestID == request.RequestID || holder.Job == spawn.Job {
			continue
		}
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		record, exists, readErr := app.ExistingJob(readCtx, holder.Job)
		cancel()
		if state, _ := record["state"].(string); readErr != nil || !exists || state != "claimed" {
			continue
		}
		_ = app.MarkJobLost(ctx, holder.Job)
	}
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
// finished: its hk job is completed or lost, or it failed or succeeded and its
// pane is verifiably gone. When it cannot, held carries the 409 body naming the
// holder.
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
		case priorStatus == "succeeded":
			released, pane = h.releaseFinishedSpawnRole(ctx, app, holder, record, spawn.Task, spawn.Role)
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

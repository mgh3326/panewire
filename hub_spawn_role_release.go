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

// heldProbe is a pane probe's verdict with the reason behind it, for the log
// line a refused reclaim writes.
type heldProbe struct {
	state  heldPane
	row    nodeAgentRow
	reason string
}

func (p heldPane) String() string {
	switch p {
	case heldPanePresent:
		return "present"
	case heldPaneAbsent:
		return "absent"
	}
	return "unknown"
}

// probeHeldPane asks machine's node for every pane herdr lists. Only a node
// that answered with a complete pane list lacking the pane proves it absent;
// anything else keeps the reservation. A present pane returns its row. A
// non-empty agent name also counts as presence when any listed row carries it:
// herdr gives a pane moved across workspaces a new id, so the id alone cannot
// prove a live job gone.
func (h *HubServer) probeHeldPane(ctx context.Context, machine, pane, agent string) heldProbe {
	if machine == "" {
		return heldProbe{reason: "no_host"}
	}
	if !nodeCommandPaneIDPattern.MatchString(pane) {
		return heldProbe{reason: "no_pane_id"}
	}
	pending, id, _, sent := h.sendNodeCommand(machine, nodeCommandAgentsList, json.RawMessage("{}"))
	if !sent {
		return heldProbe{reason: "node_unreachable"}
	}
	defer h.forgetNodeCommand(id)
	ctx, cancel := context.WithTimeout(ctx, spawnHeldPaneProbeWait)
	defer cancel()
	select {
	case reply := <-pending.done:
		var list nodeAgentsListResult
		switch {
		case reply.Error != "":
			return heldProbe{reason: "node_error"}
		case json.Unmarshal(reply.Result, &list) != nil:
			return heldProbe{reason: "unreadable_reply"}
		case !list.PanesOK:
			return heldProbe{reason: "panes_ok_false"}
		}
		// herdr lists a pane in both reads; only the agent row carries the
		// status, so it is read last and wins.
		found, byLabel := nodeAgentRow{}, false
		for _, row := range append(append([]nodeAgentRow(nil), list.Panes...), list.Agents...) {
			if row.PaneID == pane || agent != "" && row.Name == agent {
				found, byLabel = row, row.PaneID != pane
			}
		}
		switch {
		case found.PaneID == "":
			return heldProbe{state: heldPaneAbsent, reason: "pane_and_label_not_listed"}
		case byLabel:
			return heldProbe{state: heldPanePresent, row: found, reason: "agent_label_listed"}
		}
		return heldProbe{state: heldPanePresent, row: found, reason: "pane_listed"}
	case <-ctx.Done():
		return heldProbe{reason: "probe_timeout"}
	}
}

// releaseFailedSpawnRole frees the role held by a failed operation when its
// pane is verifiably gone, or present but proven never to have worked
// (releaseUnworkedLanding): it marks the still-claimed hk job lost, then drops
// the reservation. A crash between the two repeats harmlessly, because a lost
// job already releases through the completed-or-lost path.
func (h *HubServer) releaseFailedSpawnRole(ctx context.Context, app *fleet.App, holder hubSpawnTask, record map[string]any, roleKey string) (released bool, probe heldProbe) {
	probe = h.probeHeldPane(ctx, spawnHolderHost(holder), holder.Outcome.Pane, "")
	if probe.state == heldPanePresent {
		return h.releaseUnworkedLanding(ctx, app, holder, record, probe.row) && h.inbox.store.releaseCompletedSpawnRole(ctx, holder.Request.RequestID, roleKey) == nil, probe
	}
	if probe.state != heldPaneAbsent {
		return false, probe
	}
	if state, _ := record["state"].(string); state != "claimed" {
		return false, heldProbe{reason: "hk_state_not_claimable"}
	}
	return h.markLostAndRelease(ctx, app, holder, roleKey), probe
}

// hkTerminalState reports whether hk never rewrites a job in state (hk
// internal/store/jobs.go jobTerminal), so the role it held is free.
func hkTerminalState(state string) bool {
	return state == "completed" || state == "lost" || state == "escalated"
}

// markLostAndRelease writes the hk job lost, then drops the reservation. A
// crash between the two repeats harmlessly: a lost job releases through the
// completed-or-lost path.
//
// hk never rewrites a terminal record (409), so a holder that completed after
// the caller read its state keeps its completed record. A refused write then
// re-reads it: a record now completed or lost releases the role as the
// terminal path does, anything else keeps the hold.
func (h *HubServer) markLostAndRelease(ctx context.Context, app *fleet.App, holder hubSpawnTask, roleKey string) bool {
	if err := app.MarkJobLost(ctx, holder.Job); err != nil {
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		record, exists, readErr := app.ExistingJob(readCtx, holder.Job)
		cancel()
		if state, _ := record["state"].(string); readErr != nil || !exists || state != "completed" && state != "lost" {
			return false
		}
	}
	return h.inbox.store.releaseCompletedSpawnRole(ctx, holder.Request.RequestID, roleKey) == nil
}

// releaseFinishedSpawnRole frees the role held by a succeeded operation whose
// hk job never reached a terminal state (a job revoked and reaped by an older
// binary, or one that ended without writing its record). Only a node that
// listed every pane and lacks the recorded one proves the job gone; a present
// pane, an unknown pane state, or no recorded pane keeps the hold. A present
// idle pane is a reviewer that finished without completing, which stays held.
// A listed pane with the holder's agent name under another id (a pane herdr
// moved across workspaces) also keeps the hold.
func (h *HubServer) releaseFinishedSpawnRole(ctx context.Context, app *fleet.App, holder hubSpawnTask, record map[string]any, roleKey string) (released bool, probe heldProbe) {
	probe = h.probeHeldPane(ctx, spawnHolderHost(holder), holder.Outcome.Pane, heldAgentName(holder, record))
	if probe.state != heldPaneAbsent {
		return false, probe
	}
	if state, _ := record["state"].(string); state != "claimed" && state != "spawned" {
		return false, heldProbe{reason: "hk_state_not_claimable"}
	}
	return h.markLostAndRelease(ctx, app, holder, roleKey), probe
}

// releaseRecordlessSpawnRole frees the role held by a succeeded operation whose
// hk job has no record at all. The proof is releaseFinishedSpawnRole's: a node
// that listed every pane and lacks both the recorded pane id and the request's
// -l label. There is no record to mark lost, so it only drops the reservation.
func (h *HubServer) releaseRecordlessSpawnRole(ctx context.Context, holder hubSpawnTask, roleKey string) (released bool, probe heldProbe) {
	probe = h.probeHeldPane(ctx, spawnHolderHost(holder), holder.Outcome.Pane, heldAgentName(holder, nil))
	if probe.state != heldPaneAbsent {
		return false, probe
	}
	return h.inbox.store.releaseCompletedSpawnRole(ctx, holder.Request.RequestID, roleKey) == nil, probe
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
		if err != nil || prior.RoleKey() != spawn.RoleKey() || holder.Request.RequestID == request.RequestID || holder.Job == spawn.Job {
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
// finished: its hk job is completed, escalated or lost, or it failed or
// succeeded and its pane is verifiably gone (a succeeded holder with no hk
// record included). When it cannot, held carries the 409 body naming the
// holder, and one INFO line says which branch held.
func (h *HubServer) reclaimSpawnRole(ctx context.Context, app *fleet.App, request fleet.Submission, spawn fleet.Spawn, owner string) (task hubSpawnTask, held map[string]string, err error) {
	roleKey := spawn.RoleKey()
	trace := heldTrace{branch: "owner_read_error", hk: "not_read"}
	holder := hubSpawnTask{}
	defer func() {
		if held != nil {
			h.logHeldSpawnRole(spawn, request, holder, trace)
		}
	}()
	priorID, priorJob, priorStatus, readErr := h.inbox.store.spawnRoleOwner(ctx, roleKey)
	if readErr != nil {
		return task, spawnRoleHeldBody(hubSpawnTask{}, heldPaneUnknown, false), errSpawnClaimed
	}
	holder, holderErr := h.inbox.store.spawnTask(ctx, priorID)
	trace.branch = "holder_unreadable"
	if holderErr != nil {
		holder = hubSpawnTask{}
	}
	probe := heldProbe{reason: "not_probed"}
	if holderErr == nil {
		trace.branch = "holder_not_finished"
	}
	if holderErr == nil && (priorStatus == "succeeded" || priorStatus == "failed") {
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		record, exists, readErr := app.ExistingJob(readCtx, priorJob)
		cancel()
		state, _ := record["state"].(string)
		released := false
		switch {
		case readErr != nil:
			trace.branch, trace.hk = "hk_read_error", "error"
		case !exists:
			trace.hk = "missing"
			switch {
			case priorStatus == "failed" && holder.Outcome.Pane == "":
				// Refused before any pane existed (e.g. checkout_refused at
				// worktree.prepare): no pane was recorded and the node never
				// claimed the hk job, which it does only after a pane takes the
				// brief. Nothing can still hold the role, so release it.
				released = h.inbox.store.releaseCompletedSpawnRole(ctx, priorID, roleKey) == nil
				trace.branch = "release_write_failed"
			case priorStatus == "failed":
				trace.branch = "failed_record_missing"
			default:
				released, probe = h.releaseRecordlessSpawnRole(ctx, holder, roleKey)
				trace.branch = "recordless_pane_held"
			}
		case hkTerminalState(state):
			trace.hk = state
			released = h.inbox.store.releaseCompletedSpawnRole(ctx, priorID, roleKey) == nil
			trace.branch = "release_write_failed"
		case priorStatus == "failed":
			trace.hk, trace.branch = state, "failed_pane_held"
			released, probe = h.releaseFailedSpawnRole(ctx, app, holder, record, roleKey)
		default:
			trace.hk, trace.branch = state, "succeeded_pane_held"
			released, probe = h.releaseFinishedSpawnRole(ctx, app, holder, record, roleKey)
		}
		trace.probe = probe
		if released {
			task, _, err = h.inbox.store.claimSpawn(ctx, request, spawn.Job, owner, h.now().UTC())
			if !errors.Is(err, errSpawnClaimed) {
				return task, nil, err
			}
			trace.branch = "claim_raced"
		} else if probe.state == heldPaneAbsent {
			trace.branch = "release_write_failed"
		}
	}
	trace.probe = probe
	return task, spawnRoleHeldBody(holder, probe.state, holderErr == nil), errSpawnClaimed
}

// heldTrace is what a refused reclaim saw: the branch that held, the hk read
// ("error", "missing", or the record's state) and the pane probe.
type heldTrace struct {
	branch, hk string
	probe      heldProbe
}

// logHeldSpawnRole writes the one INFO line a refused reclaim leaves, because
// the 409 is synchronous and its body is not kept. It names ids, host, pane and
// verdicts only: no tokens, request bodies or brief text.
func (h *HubServer) logHeldSpawnRole(spawn fleet.Spawn, request fleet.Submission, holder hubSpawnTask, trace heldTrace) {
	pane := holder.Outcome.Pane
	if !nodeCommandPaneIDPattern.MatchString(pane) {
		pane = ""
	}
	if trace.probe.reason == "" {
		trace.probe.reason = "not_probed"
	}
	h.logger.Info("hub held a spawn role reservation",
		"branch", trace.branch,
		"role_key", spawn.RoleKey(),
		"request", request.RequestID, "job", spawn.Job,
		"holder_request", holder.Request.RequestID, "holder_job", holder.Job, "holder_status", holder.Status,
		"holder_host", spawnHolderHost(holder), "holder_pane", pane,
		"hk_read", trace.hk,
		"probe", trace.probe.state.String(), "probe_reason", trace.probe.reason)
}

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
// line a refused reclaim writes. write marks a refusal at the lost-record
// write that follows a proven-absent or freshly closed pane.
type heldProbe struct {
	state  heldPane
	row    nodeAgentRow
	reason string
	write  bool
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
	return h.probeNodePanes(ctx, machine, pane, agent)
}

// probeNodePanes reads machine's agents.list and finds the row with pane id
// pane (when non-empty) or agent name agent (when non-empty).
func (h *HubServer) probeNodePanes(ctx context.Context, machine, pane, agent string) heldProbe {
	ctx, cancel := context.WithTimeout(ctx, spawnHeldPaneProbeWait)
	defer cancel()
	pending, id, _, sent := h.sendNodeCommandWithContext(ctx, machine, nodeCommandAgentsList, json.RawMessage("{}"))
	if !sent {
		return heldProbe{reason: "node_unreachable"}
	}
	defer h.forgetNodeCommand(id)
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
			if pane != "" && row.PaneID == pane || agent != "" && row.Name == agent {
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
// (releaseUnworkedSpawn): it marks the still-claimed hk job lost, then drops
// the reservation. A crash between the two repeats harmlessly, because a lost
// job already releases through the completed-or-lost path. A failed operation
// that recorded no pane id is probed by its job label instead: probeHeldPane
// needs a pane id, so without this such a holder would hold for good (hk 4620).
func (h *HubServer) releaseFailedSpawnRole(ctx context.Context, app *fleet.App, holder hubSpawnTask, record map[string]any, roleKey string) (released bool, probe heldProbe) {
	host, label := spawnHolderHost(holder), heldAgentName(holder, record)
	if holder.Outcome.Pane == "" && label != "" {
		probe = h.probeLabelledPane(ctx, host, label)
	} else {
		probe = h.probeHeldPane(ctx, host, holder.Outcome.Pane, "")
	}
	if probe.state == heldPanePresent {
		released, markReason := h.releaseUnworkedSpawn(ctx, app, holder, record, probe.row)
		if markReason != "" {
			probe.write, probe.reason = true, markReason
		}
		return released && h.inbox.store.releaseCompletedSpawnRole(ctx, holder.Request.RequestID, roleKey) == nil, probe
	}
	if probe.state != heldPaneAbsent {
		return false, probe
	}
	if state, _ := record["state"].(string); state != "claimed" {
		return false, heldProbe{reason: "hk_state_not_claimable"}
	}
	released, markReason := h.markLostAndRelease(ctx, app, holder, heldAgentName(holder, record), roleKey)
	if !released && markReason == nodeJobMarkLostNoLiveAttempt && holder.Outcome.Pane == "" {
		// hk 4799: a failed holder that recorded no pane, whose pane is
		// absent, frees its reservation only when its node proved from its own
		// spawn journal and active-job list that no attempt of the job is live
		// under any label. hk is left as it is: the node still never writes
		// lost over a record it cannot check (hk 3685), so the hk job stays
		// claimed. A ledger the node cannot read, or a live attempt it cannot
		// rule out, keeps the hold (hk 4673's bare ledger_missing release let
		// a newer claim under another label through). Reviewer reservations
		// are the subject of hk 4737.
		if h.inbox.store.releaseCompletedSpawnRole(ctx, holder.Request.RequestID, roleKey) == nil {
			return true, probe
		}
	}
	if !released {
		probe.write, probe.reason = true, markReason
	}
	return released, probe
}

// hkTerminalState reports whether hk never rewrites a job in state (hk
// internal/store/jobs.go jobTerminal), so the role it held is free.
func hkTerminalState(state string) bool {
	return state == "completed" || state == "lost" || state == "escalated"
}

// markLostAndRelease has the holder's node write the hk job lost, then drops
// the reservation. A crash between the two repeats harmlessly: a lost job
// releases through the completed-or-lost path. The hub writes the record
// itself only when the holder ran on the hub's own host, where its hk client
// holds the grant; a remote host's node writes with its own credential.
func (h *HubServer) markLostAndRelease(ctx context.Context, app *fleet.App, holder hubSpawnTask, label, roleKey string) (bool, string) {
	if lost, reason := h.markHolderJobLost(ctx, app, holder, label); !lost {
		return false, reason
	}
	return h.inbox.store.releaseCompletedSpawnRole(ctx, holder.Request.RequestID, roleKey) == nil, ""
}

// markHolderJobLost asks the holder's node to write its hk job lost, because
// the hub's hk client is not granted a remote host. A node that answers
// anything but lost, or does not answer, keeps the reservation; the reason is
// what the caller logs. The hub writes the record itself only for a holder on
// its own host, whose grant it holds.
func (h *HubServer) markHolderJobLost(ctx context.Context, app *fleet.App, holder hubSpawnTask, label string) (bool, string) {
	return h.markHolderJobLostBecause(ctx, app, holder, label, "")
}

type hubLostJobApp interface {
	NodeID() (string, bool)
	MarkJobLostBecause(context.Context, string, string) error
	ExistingJob(context.Context, string) (map[string]any, bool, error)
}

// markHolderJobLostBecause is markHolderJobLost with the hk end_reason the
// lost record takes ("" writes none).
func (h *HubServer) markHolderJobLostBecause(ctx context.Context, app hubLostJobApp, holder hubSpawnTask, label, endReason string) (bool, string) {
	host, pane := spawnHolderHost(holder), holder.Outcome.Pane
	if host == "" {
		return false, "no_host"
	}
	// The hub-local write has no job ledger to check, and a posted-result end
	// rests on the node's own ledger (a role, a quota stop, the result itself),
	// so that end goes through the node even on the hub's own host.
	if hubHost, known := app.NodeID(); known && hubHost == host && endReason != reviewerResultPosted {
		state, reason := h.writeJobLost(ctx, app, holder.Job, endReason)
		if state == "lost" {
			return true, "hk_lost"
		}
		if hkTerminalState(state) {
			return true, nodeJobMarkLostCompleted
		}
		return false, reason
	}
	if pane == "" && label == "" {
		return false, "no_pane_or_label"
	}
	raw, err := json.Marshal(nodeJobMarkLostArgs{Job: holder.Job, PaneID: pane, Label: label, EndReason: endReason})
	if err != nil {
		return false, "invalid_args"
	}
	pending, requestID, state, sent := h.sendNodeCommand(host, nodeCommandJobMarkLost, raw)
	if !sent {
		return false, "node_unreachable:" + state
	}
	defer h.forgetNodeCommand(requestID)
	waitCtx, cancel := context.WithTimeout(ctx, hubNodeWait(h.nodeCommandWait, hubNodeCommandWait))
	defer cancel()
	select {
	case reply := <-pending.done:
		if reply.Error != "" {
			return false, reply.Error
		}
		var result nodeJobMarkLostResult
		if json.Unmarshal(reply.Result, &result) != nil {
			return false, "unreadable_reply"
		}
		if result.Outcome == nodeJobMarkLostLost {
			return true, result.Outcome
		}
		// A holder that completed meanwhile frees its role once hk holds the
		// terminal record, exactly as on the completed-or-lost path; a
		// completion hk has not recorded yet keeps the hold.
		if result.Outcome == nodeJobMarkLostCompleted {
			readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			record, exists, readErr := app.ExistingJob(readCtx, holder.Job)
			cancel()
			if state, _ := record["state"].(string); readErr == nil && exists && hkTerminalState(state) {
				return true, result.Outcome
			}
		}
		return false, result.Outcome
	case <-waitCtx.Done():
		return false, "mark_lost_timeout"
	}
}

// writeJobLost is the hub-local fallback. hk never rewrites a terminal record
// (409), so a refused write re-reads it: a record now completed or lost
// releases the role as the terminal path does; anything else keeps the hold.
func (h *HubServer) writeJobLost(ctx context.Context, app hubLostJobApp, job, endReason string) (state, reason string) {
	if err := app.MarkJobLostBecause(ctx, job, endReason); err != nil {
		if fleet.IsJobReassigned(err) {
			return "", "job_owner_changed"
		}
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		record, exists, readErr := app.ExistingJob(readCtx, job)
		cancel()
		state, _ = record["state"].(string)
		if readErr != nil || !exists || !hkTerminalState(state) {
			return "", "hk_write_failed"
		}
		reason, _ = record["end_reason"].(string)
		return state, reason
	}
	return "lost", endReason
}

// releaseFinishedSpawnRole frees the role held by a succeeded operation whose
// hk job never reached a terminal state (a job revoked and reaped by an older
// binary, or one that ended without writing its record). Only a node that
// listed every pane and lacks the recorded one proves the job gone; a present
// pane, an unknown pane state, or no recorded pane keeps the hold. An idle
// pane stays held: an implementer may be waiting on an answer. A reviewer
// never reaches this rule, because it holds no reservation (hk 4737). A listed
// pane with the holder's agent name under another id (a pane herdr moved across
// workspaces) also keeps the hold.
func (h *HubServer) releaseFinishedSpawnRole(ctx context.Context, app *fleet.App, holder hubSpawnTask, record map[string]any, roleKey string) (released bool, probe heldProbe) {
	unlock, free := h.lockWithoutPoolAction(ctx, holder.Job)
	if !free {
		return false, heldProbe{reason: "pool_action_pending"}
	}
	defer unlock()
	h.mu.Lock()
	job := h.jobs[holder.Job]
	moved := job != nil && (job.Node != spawnHolderHost(holder) || job.Epoch > 1)
	h.mu.Unlock()
	if moved {
		return false, heldProbe{reason: "job_owner_changed"}
	}
	label := heldAgentName(holder, record)
	probe = h.probeHeldPane(ctx, spawnHolderHost(holder), holder.Outcome.Pane, label)
	if probe.state != heldPaneAbsent {
		return false, probe
	}
	if state, _ := record["state"].(string); state != "claimed" && state != "spawned" {
		return false, heldProbe{reason: "hk_state_not_claimable"}
	}
	released, markReason := h.markLostAndRelease(ctx, app, holder, label, roleKey)
	if !released {
		probe.write, probe.reason = true, markReason
	}
	return released, probe
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
// A request that takes no slot (consult, reviewer) supersedes nothing: it
// shares the task beside the other operation, so closing that operation's
// record would end work the slot never displaced (hk 4568, hk 4737). Best
// effort: a failed hk read or write leaves the record for the next submission.
func (h *HubServer) supersedeRetryableHolders(ctx context.Context, app *fleet.App, request fleet.Submission, spawn fleet.Spawn) {
	if !spawn.HoldsTaskRole() {
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
		// The holder's own node writes the record; the hub's hk client is not
		// granted a remote host.
		_, _ = h.markHolderJobLost(ctx, app, holder, heldAgentName(holder, nil))
	}
}

func spawnHolderHost(holder hubSpawnTask) string {
	if holder.Outcome.Host != "" {
		return holder.Outcome.Host
	}
	return spawnTaskHost(holder)
}

// heldFix is the fix a submitter sees for a live hold. It names no job id:
// the refusal is about the operation holding the reservation (hk 2944).
const heldFix = "inspect the existing operation; do not submit a new request while ownership is live"

// jobHeldBody answers a claim refused because the job id is already claimed
// and no (task, role) reservation holds the request.
func jobHeldBody() map[string]string {
	return map[string]string{"error": "job_already_claimed", "fix": heldFix}
}

// spawnRoleHeldBody answers a refused claim whose reservation is held by another
// operation. code is task_role_claimed when the (task, role) is the collision,
// and job_already_claimed when the job id is: the body names the task and role
// either way, and the holder's identity follows when readable.
func spawnRoleHeldBody(code, task, role string, holder hubSpawnTask, pane heldPane, found bool) map[string]string {
	body := map[string]string{"error": code, "task": task, "role": role, "fix": heldFix}
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

// releaseSettledSpawnRole frees the role of a holder whose job the node settled
// lost (observeJobLost, hk 5351). It takes the job's pool-action lock, so a
// pool action that still owns the job keeps the hold (I1/I2), and it checks the
// settle under that lock. The holder's node must be the one that settled it.
func (h *HubServer) releaseSettledSpawnRole(ctx context.Context, holder hubSpawnTask, roleKey string) bool {
	unlock, free := h.lockWithoutPoolAction(ctx, holder.Job)
	if !free {
		return false
	}
	defer unlock()
	h.mu.Lock()
	job := h.jobs[holder.Job]
	settled := job != nil && job.LostSettled && job.Node == spawnHolderHost(holder)
	h.mu.Unlock()
	if !settled {
		return false
	}
	return h.inbox.store.releaseCompletedSpawnRole(ctx, holder.Request.RequestID, roleKey) == nil
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
		return task, jobHeldBody(), errSpawnClaimed
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
	if settled := holderErr == nil && (priorStatus == "succeeded" || priorStatus == "failed") && h.releaseSettledSpawnRole(ctx, holder, roleKey); settled {
		// The node settled the holder's job lost (hk 5351): its hk record is
		// lost and no pool action owns the job, so the role is free without
		// reading hk again.
		trace.branch, trace.hk = "job_settled", "settled"
		task, _, err = h.inbox.store.claimSpawn(ctx, request, spawn.Job, owner, h.now().UTC())
		if !errors.Is(err, errSpawnClaimed) {
			return task, nil, err
		}
		trace.branch = "claim_raced"
	} else if holderErr == nil && (priorStatus == "succeeded" || priorStatus == "failed") {
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
			unlock, free := h.lockWithoutPoolAction(ctx, priorJob)
			if free {
				released = h.inbox.store.releaseCompletedSpawnRole(ctx, priorID, roleKey) == nil
				unlock()
			}
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
		} else if probe.write {
			// The node refused the lost write (or its read failed), so no
			// reservation release was tried: name the refusal, not a write.
			trace.branch = "mark_lost_refused"
		} else if probe.state == heldPaneAbsent {
			trace.branch = "release_write_failed"
		}
	}
	trace.probe = probe
	// The job id and the role can both collide. The job-id duplicate keeps its
	// own code; only a collision on the role alone is task_role_claimed.
	jobHeld, jobErr := h.inbox.store.spawnJobReserved(ctx, spawn.Job)
	if jobErr != nil {
		return task, nil, jobErr
	}
	code := "task_role_claimed"
	if jobHeld {
		code = "job_already_claimed"
	}
	return task, spawnRoleHeldBody(code, spawn.Task, spawn.Role, holder, probe.state, holderErr == nil), errSpawnClaimed
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
	h.observeSpawnHeld(spawn.Job, spawn.RoleKey(), trace.branch, trace.probe.reason,
		"holder_job="+holder.Job+" holder_host="+spawnHolderHost(holder)+" holder_pane="+pane+" hk_read="+trace.hk+" probe="+trace.probe.state.String())
	h.logger.Info("hub held a spawn role reservation",
		"branch", trace.branch,
		"role_key", spawn.RoleKey(),
		"request", request.RequestID, "job", spawn.Job,
		"holder_request", holder.Request.RequestID, "holder_job", holder.Job, "holder_status", holder.Status,
		"holder_host", spawnHolderHost(holder), "holder_pane", pane,
		"hk_read", trace.hk,
		"probe", trace.probe.state.String(), "probe_reason", trace.probe.reason)
}

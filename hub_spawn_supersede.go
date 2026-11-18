package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// Successor-closes-parent (hk 5140): when an implementer spawn with
// --parent-job P lands, the hub ends P itself through the jobs-close reap
// path. Only implementer successors take over; reviewer and consult children
// never close their parent, and the successor must own the parent's lane.
//
//   - Reap's safety holds: never a pane that is not P's, never a job spawned
//     with --keep, and never a worktree removal (reap closes the pane only).
//   - An idle, done or blocked parent closes at landing; a working parent is
//     recorded as superseded and closed at the first sweep where it is no
//     longer working. Force is never used on a working pane: a blocked pane
//     is proven blocked first (one unforced reap) and the forced close
//     carries that proof, which the node rechecks atomically with the
//     close, so a pane that resumed working since the proof is refused
//     instead of killed.
//   - The hub writes job.superseded (successor id) on P and sends one info
//     lane.event to P's owner lane once the pane is closed. Deferred parents
//     are rebuilt from hk after a hub restart, so no recorded parent is
//     lost with the in-memory map.
//   - An unknown or already-ended parent is a no-op. A close failure never
//     fails the successor's spawn; it logs one warn and retries on the sweep.
//
// P's hk task is untouched; the successor carries it.

// supersededParentOf names the parent a landed successor replaces, or "".
func supersededParentOf(task hubSpawnTask) string {
	return hubSpawnFlagArg(task.Request.Args, "--parent-job")
}

// maybeSupersedeParent starts the successor-closes-parent close after a
// landed spawn. It never fails the spawn: every failure is one warn.
func (h *HubServer) maybeSupersedeParent(task hubSpawnTask) {
	parent := supersededParentOf(task)
	successor := task.Job
	if parent == "" || successor == "" || parent == successor {
		return
	}
	// Only an implementer successor takes over: a reviewer or consult
	// child must never close its parent's pane (docs/config.md).
	if hubSpawnFlagArg(task.Request.Args, "--role") != "implementer" {
		return
	}
	h.mu.Lock()
	job := h.jobs[parent]
	var node, owner string
	completed := true
	if job != nil {
		node, owner = job.Node, job.OwnerLane
		completed = job.Completed
	}
	h.mu.Unlock()
	if job == nil || completed || node == "" || owner == "" {
		return
	}
	// The successor may only end a parent it owns: the forced path could
	// otherwise close another lane's blocked pane, which is usually an
	// open operator decision.
	succOwner := task.Owner
	if succOwner == "" {
		succOwner = hubSpawnFlagArg(task.Request.Args, "--owner")
	}
	if succOwner != owner {
		h.logger.Warn("successor close refused: successor owner differs", "parent", parent, "successor", successor, "owner", owner, "successor_owner", succOwner)
		return
	}
	h.supersedeParent(parent, successor, owner, node)
}

// supersedeParent records P as superseded and closes its pane through reap.
// Working defers to the sweep; every other unclosed outcome that is not an
// expected no-op logs exactly one warn.
func (h *HubServer) supersedeParent(parent, successor, owner, node string) {
	if parent == "" || successor == "" || owner == "" || node == "" {
		return
	}
	h.mu.Lock()
	job := h.jobs[parent]
	completed := true
	if job != nil {
		completed = job.Completed
	}
	h.mu.Unlock()
	if job == nil || completed {
		h.clearSuperseded(parent)
		return
	}
	if h.supersededParentTerminal(parent) {
		h.clearSuperseded(parent)
		return
	}
	reaped, transportErr := h.reapParentOnce(node, parent, owner, false, "")
	if transportErr != "" {
		h.noteSuperseded(parent, successor)
		h.writeSupersededBestEffort(parent, successor)
		h.logger.Warn("successor close failed, will retry", "parent", parent, "successor", successor, "reason", transportErr)
		return
	}
	switch reaped.Outcome {
	case nodeJobReapClosed, nodeJobReapPaneGone:
		hkErr := h.writeSupersededBestEffort(parent, successor)
		notifyErr := h.notifySuperseded(parent, successor, owner, node)
		h.clearSuperseded(parent)
		if hkErr != nil || notifyErr != nil {
			h.logger.Warn("successor close failed", "parent", parent, "successor", successor)
		}
	case nodeJobReapJobWorking:
		h.noteSuperseded(parent, successor)
		if err := h.writeSupersededBestEffort(parent, successor); err != nil {
			h.logger.Warn("successor close failed, will retry", "parent", parent, "successor", successor)
		}
	case nodeJobReapPaneBlocked:
		// The forced close carries what the proof saw (blocked): the node
		// rechecks it atomically with the close, so a pane that resumed
		// working since the proof is refused instead of killed.
		forced, forcedErr := h.reapParentOnce(node, parent, owner, true, "blocked")
		if forcedErr != "" {
			h.noteSuperseded(parent, successor)
			h.writeSupersededBestEffort(parent, successor)
			h.logger.Warn("successor close failed, will retry", "parent", parent, "successor", successor, "reason", forcedErr)
			return
		}
		switch forced.Outcome {
		case nodeJobReapClosed, nodeJobReapPaneGone:
			hkErr := h.writeSupersededBestEffort(parent, successor)
			notifyErr := h.notifySuperseded(parent, successor, owner, node)
			h.clearSuperseded(parent)
			if hkErr != nil || notifyErr != nil {
				h.logger.Warn("successor close failed", "parent", parent, "successor", successor)
			}
		case nodeJobReapProtected, nodeJobReapNotAJob, nodeJobReapOwnerMismatch:
			h.clearSuperseded(parent)
		case nodeJobReapJobWorking:
			// The pane resumed after the blocked proof; wait for the sweep
			// like any other working pane.
			h.noteSuperseded(parent, successor)
			if err := h.writeSupersededBestEffort(parent, successor); err != nil {
				h.logger.Warn("successor close failed, will retry", "parent", parent, "successor", successor)
			}
		default:
			h.noteSuperseded(parent, successor)
			h.writeSupersededBestEffort(parent, successor)
			h.logger.Warn("successor close failed, will retry", "parent", parent, "successor", successor, "reason", forced.Outcome)
		}
	case nodeJobReapProtected, nodeJobReapNotAJob, nodeJobReapOwnerMismatch:
		h.clearSuperseded(parent)
	default:
		h.noteSuperseded(parent, successor)
		h.writeSupersededBestEffort(parent, successor)
		h.logger.Warn("successor close failed, will retry", "parent", parent, "successor", successor, "reason", reaped.Outcome)
	}
}

// supersededParentTerminal reports whether P is unknown or already ended in
// hk. A transport failure reads as unknown (false) so the reap still runs and
// the sweep retries; only a proven missing or terminal record stops the close.
func (h *HubServer) supersededParentTerminal(parent string) bool {
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return false
	}
	if err := app.CheckTrust(); err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	record, found, err := app.ExistingJob(ctx, parent)
	if err != nil || !found {
		return err == nil
	}
	state, _ := record["state"].(string)
	return hkTerminalState(state)
}

// reapParentOnce runs one jobs-close reap against the parent's node and
// retires the lane rows the node verified, exactly as handleJobReap does. A
// non-empty transport error means the pane was not proven anything about.
func (h *HubServer) reapParentOnce(node, job, lane string, force bool, expect string) (nodeJobReapResult, string) {
	args, _ := json.Marshal(nodeJobReapArgs{Job: job, Lane: lane, Force: force, Expect: expect})
	commandStart := h.now().UTC()
	pending, requestID, state, sent := h.sendNodeCommand(node, nodeCommandJobReap, args)
	if !sent {
		if state == "send_failed" {
			return nodeJobReapResult{}, "close_unconfirmed"
		}
		return nodeJobReapResult{}, "node_offline"
	}
	timer := time.NewTimer(hubNodeWait(h.nodeCommandWait, hubNodeCommandWait))
	defer timer.Stop()
	var result hubNodeCommandResult
	select {
	case result = <-pending.done:
	case <-timer.C:
		h.forgetNodeCommand(requestID)
		return nodeJobReapResult{}, "close_unconfirmed"
	}
	if result.Error == "node_lost" || result.Error == "node_timeout" {
		return nodeJobReapResult{}, "close_unconfirmed"
	}
	if result.Error != "" {
		return nodeJobReapResult{}, result.Error
	}
	var reaped nodeJobReapResult
	if json.Unmarshal(result.Result, &reaped) != nil {
		return nodeJobReapResult{}, "invalid_node_response"
	}
	switch reaped.Outcome {
	case nodeJobReapClosed, nodeJobReapPaneGone:
		if reaped.Pane != "" {
			h.releasePaneLanes(node, reaped.Pane, commandStart)
		}
		h.logger.Info("job reaped", "client", "successor", "lane", lane, "job", job, "machine", node, "pane", reaped.Pane, "outcome", reaped.Outcome)
		return reaped, ""
	default:
		h.logger.Info("job reap refused", "client", "successor", "lane", lane, "job", job, "machine", node, "pane", reaped.Pane, "outcome", reaped.Outcome)
		return reaped, ""
	}
}

// writeSupersededBestEffort writes job.superseded on P. A missing hk client
// or write failure is the caller's one warn, never a spawn failure.
func (h *HubServer) writeSupersededBestEffort(parent, successor string) error {
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return err
	}
	if err := app.CheckTrust(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return app.MarkJobSuperseded(ctx, parent, successor)
}

// notifySuperseded sends the one info lane.event to P's owner lane. A
// duplicate event id means the close was already told, which still counts as
// told.
func (h *HubServer) notifySuperseded(parent, successor, owner, host string) error {
	text := fmt.Sprintf("[panewire] closed %s on %s: superseded by %s", parent, host, successor)
	event := hubJobEventPayload{
		EventID:    "superseded:" + parent,
		JobID:      parent,
		OwnerLane:  owner,
		NotifyType: "info",
		Text:       utf8PrefixWithin(text, laneEventTextLimit),
		Host:       "hub",
		Epoch:      1,
	}
	result := h.relayLaneEventContext(context.Background(), event, nil)
	if result.ID == 0 || result.PersistFailed {
		return fmt.Errorf("owner not notified")
	}
	return nil
}

func (h *HubServer) noteSuperseded(parent, successor string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.supersededParents == nil {
		h.supersededParents = map[string]string{}
	}
	h.supersededParents[parent] = successor
}

func (h *HubServer) clearSuperseded(parent string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.supersededParents, parent)
}

// rebuildSupersededFromHK re-arms deferred parents recorded before a hub
// restart. job.superseded survives in hk while the in-memory map does not;
// the first sweep re-reads each live hub job once and re-notes the ones
// whose record names a successor and is not terminal.
func (h *HubServer) rebuildSupersededFromHK() {
	h.mu.Lock()
	if h.supersededRebuilt {
		h.mu.Unlock()
		return
	}
	h.supersededRebuilt = true
	type liveJob struct{ id, node, owner string }
	var jobs []liveJob
	for id, job := range h.jobs {
		if job == nil || job.Completed {
			continue
		}
		jobs = append(jobs, liveJob{id: id, node: job.Node, owner: job.OwnerLane})
	}
	h.mu.Unlock()
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return
	}
	if err := app.CheckTrust(); err != nil {
		return
	}
	for _, job := range jobs {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		record, found, err := app.ExistingJob(ctx, job.id)
		cancel()
		if err != nil || !found {
			continue
		}
		if state, _ := record["state"].(string); hkTerminalState(state) {
			continue
		}
		if succ, _ := record["superseded"].(string); succ != "" {
			h.noteSuperseded(job.id, succ)
		}
	}
}

// sweepSupersededParents retries every deferred parent close. It runs on the
// hub's Sweep: the first pass where the pane is no longer working closes it.
// The first sweep after a hub start re-arms entries recorded before the
// restart from their hk job.superseded records.
func (h *HubServer) sweepSupersededParents() {
	h.rebuildSupersededFromHK()
	h.mu.Lock()
	pending := make(map[string]string, len(h.supersededParents))
	for parent, successor := range h.supersededParents {
		pending[parent] = successor
	}
	h.mu.Unlock()
	for parent, successor := range pending {
		h.mu.Lock()
		job := h.jobs[parent]
		var node, owner string
		completed := true
		if job != nil {
			node, owner = job.Node, job.OwnerLane
			completed = job.Completed
		}
		h.mu.Unlock()
		if job == nil || completed || node == "" || owner == "" {
			h.clearSuperseded(parent)
			continue
		}
		h.supersedeParent(parent, successor, owner, node)
	}
}

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
//     longer working. Force is never aimed at a working pane: a blocked pane
//     is proven blocked first and the forced close carries that proof, which
//     the node re-reads just before closing. The recheck narrows the race to
//     the close call itself; herdr's pane close takes only the pane id, so a
//     pane answered between the recheck and the close is still closed.
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
	if !h.supersedeClose {
		return
	}
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
		h.recordSuperseded(parent, successor)
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
		if h.recordSuperseded(parent, successor) {
			h.logger.Warn("successor close failed, will retry", "parent", parent, "successor", successor)
		}
	case nodeJobReapPaneBlocked:
		// The forced close carries what the proof saw (blocked): the node
		// re-reads the pane just before closing and refuses a pane that
		// resumed working or otherwise left the proven state. The recheck
		// narrows the race to the close call itself; it cannot close the
		// gap between that read and herdr's pane close, which takes only
		// the pane id.
		forced, forcedErr := h.reapParentOnce(node, parent, owner, true, "blocked")
		if forcedErr != "" {
			h.recordSuperseded(parent, successor)
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
			// like any other working pane. The recheck narrowed the race
			// to the close call itself; it cannot close the gap between
			// that read and herdr's pane close, which takes only the pane
			// id.
			if h.recordSuperseded(parent, successor) {
				h.logger.Warn("successor close failed, will retry", "parent", parent, "successor", successor)
			}
		default:
			h.recordSuperseded(parent, successor)
			h.logger.Warn("successor close failed, will retry", "parent", parent, "successor", successor, "reason", forced.Outcome)
		}
	case nodeJobReapProtected, nodeJobReapNotAJob, nodeJobReapOwnerMismatch:
		h.clearSuperseded(parent)
	default:
		h.recordSuperseded(parent, successor)
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

// supersededEntry is one deferred parent close: who replaces the parent, and
// whether job.superseded is confirmed in hk. A failed hk write leaves
// recorded false so later ticks retry until it succeeds; a confirmed write
// suppresses repeats.
type supersededEntry struct {
	successor string
	recorded  bool
}

// noteSuperseded records parent as replaced by successor. It reports whether
// job.superseded still lacks a confirmed write: a new or changed pair, or a
// pair whose write has not succeeded yet. Callers write only then, and mark
// the write, so a working parent held across ticks costs one hk write on
// success while a failed write is retried.
func (h *HubServer) noteSuperseded(parent, successor string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.supersededParents == nil {
		h.supersededParents = map[string]supersededEntry{}
	}
	if e, ok := h.supersededParents[parent]; !ok || e.successor != successor {
		h.supersededParents[parent] = supersededEntry{successor: successor}
		return true
	}
	return !h.supersededParents[parent].recorded
}

// markSupersededWritten confirms the hk write for the pair, unless a newer
// successor replaced it meanwhile.
func (h *HubServer) markSupersededWritten(parent, successor string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if e, ok := h.supersededParents[parent]; ok && e.successor == successor {
		e.recorded = true
		h.supersededParents[parent] = e
	}
}

// recordSuperseded notes the pair and writes job.superseded unless a
// confirmed write already covers it. It reports whether the pair still lacks
// confirmation, so the caller warns only then.
func (h *HubServer) recordSuperseded(parent, successor string) bool {
	if !h.noteSuperseded(parent, successor) {
		return false
	}
	if err := h.writeSupersededBestEffort(parent, successor); err != nil {
		return true
	}
	h.markSupersededWritten(parent, successor)
	return false
}

func (h *HubServer) clearSuperseded(parent string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.supersededParents, parent)
}

// rebuildSupersededFromHK re-arms deferred parents recorded before a hub
// restart. job.superseded survives in hk while the in-memory map does not.
// Every sweep checks each live hub job it has not checked before, so a job
// whose node reports after the restart is still re-armed when it appears. A
// successful hk read marks the job checked; a missing record or a read
// failure leaves it unchecked so a later sweep retries.
func (h *HubServer) rebuildSupersededFromHK() {
	var ids []string
	h.mu.Lock()
	if h.supersededChecked == nil {
		h.supersededChecked = map[string]bool{}
	}
	for id, job := range h.jobs {
		if job == nil || job.Completed || h.supersededChecked[id] {
			continue
		}
		ids = append(ids, id)
	}
	h.mu.Unlock()
	if len(ids) == 0 {
		return
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return
	}
	if err := app.CheckTrust(); err != nil {
		return
	}
	for _, id := range ids {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		record, found, err := app.ExistingJob(ctx, id)
		cancel()
		if err != nil || !found {
			continue
		}
		if state, _ := record["state"].(string); hkTerminalState(state) {
			h.markSupersededChecked(id)
			continue
		}
		if succ, _ := record["superseded"].(string); succ != "" {
			h.noteSuperseded(id, succ)
			h.markSupersededWritten(id, succ)
		}
		h.markSupersededChecked(id)
	}
}

func (h *HubServer) markSupersededChecked(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.supersededChecked == nil {
		h.supersededChecked = map[string]bool{}
	}
	h.supersededChecked[id] = true
}

// startSupersededSweep runs the deferred-parent pass off the maintenance
// ticker, at most one pass in flight: a hung node must never hold Sweep's
// pings, failover broadcasts and notification dispatch behind a 30s reap.
func (h *HubServer) startSupersededSweep() {
	if !h.supersedeClose {
		return
	}
	if !h.supersededSweepMu.TryLock() {
		return
	}
	go func() {
		defer h.supersededSweepMu.Unlock()
		h.sweepSupersededParents()
	}()
}

// sweepSupersededParents retries every deferred parent close. It runs on the
// hub's Sweep: the first pass where the pane is no longer working closes it.
// The first sweep after a hub start re-arms entries recorded before the
// restart from their hk job.superseded records.
func (h *HubServer) sweepSupersededParents() {
	h.rebuildSupersededFromHK()
	h.mu.Lock()
	pending := make(map[string]string, len(h.supersededParents))
	for parent, entry := range h.supersededParents {
		pending[parent] = entry.successor
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

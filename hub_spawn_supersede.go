package panewire

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// Successor-closes-parent (hk 5140, hk 5195): when an implementer spawn that
// named its parent with a typed --parent-job lands, the hub ends the parent's
// pane through a durable action. Rule and rationale: docs/config.md,
// "--parent-job".
//
//   - Explicit only. The submitter's own command line carried --parent-job
//     (Submission.ParentJobExplicit). A parent filled from ARBITER_JOB, a
//     hub-built successor and an older client's request close nothing.
//   - Only an implementer successor takes over, and only a successor that
//     owns the parent's lane. A reviewer or consult child, a parent spawned
//     with --keep, and a parent this hub holds no spawn record for are never
//     closed.
//   - The intent is a pool_actions row of kind supersede_close, one per
//     parent (successor, node and epoch). The pool action reconciler
//     (hub_pool_action.go) drives it, so it survives a hub restart and a
//     failed hk write is retried.
//   - The close is the close-op path (pane.close_op) to the parent's node at
//     its epoch. The hub sends it only while the parent's pane is idle or
//     done; a working or blocked pane leaves the row pending for a later
//     pass. The node then re-checks inside the same operation that the pane
//     is still the parent's job and still in the status the hub saw, so a
//     pane that changed hands or resumed work is refused, never closed.
//   - The worktree is never removed and the parent's hk task is untouched:
//     the successor carries it. The parent's hk record ends lost with
//     end_reason superseded_by=<successor>, and the owner lane hears one
//     info line.

const (
	// poolActionKindSupersedeClose is the pool action kind of a successor
	// closing its parent.
	poolActionKindSupersedeClose = "supersede_close"
	// supersedeMaxGenerations bounds how often a close the node refused while
	// the pane was still the parent's is asked again under a new request id.
	supersedeMaxGenerations = 5
)

// poolSupersede is the kind-specific data of a supersede_close row.
type poolSupersede struct {
	// Successor is the job that replaced the parent.
	Successor string
	// Generation counts the requests the node refused for a pane that was
	// still the parent's (a close racing a state change); each starts a new
	// row, so a refusal is not final while the pane remains.
	Generation uint8 `json:",omitempty"`
	// Recorded is set once job.superseded is confirmed in hk; a failed write
	// leaves it false so the next pass retries.
	Recorded bool `json:",omitempty"`
}

// supersededParentOf names the parent a landed successor replaces, or "".
func supersededParentOf(task hubSpawnTask) string {
	return hubSpawnFlagArg(task.Request.Args, "--parent-job")
}

// supersedeEndReason is the hk end_reason of the parent's lost record.
func supersedeEndReason(successor string) string {
	return "superseded_by=" + successor
}

// supersedeCloseStatus reports whether a parent pane in this herdr status may
// be closed: only a pane that has stopped. A working pane is never forced, and
// a blocked one may be waiting for an answer.
func supersedeCloseStatus(status string) bool {
	return status == "idle" || status == "done"
}

// supersedeLandingWindow bounds how long after a spawn was accepted the hub
// keeps retrying to queue its parent's close. A request lives at most 24 hours
// before it lands, plus the run time of its attempt.
const supersedeLandingWindow = 48 * time.Hour

// supersedeScanInterval paces the retry pass over recent landings.
const supersedeScanInterval = time.Minute

// maybeSupersedeParent queues the parent's close after a landed spawn. It
// never fails the spawn. A landing that cannot be decided yet (the parent's
// node is offline, hk or the store is unavailable) is retried by
// retrySupersedeLandings from the succeeded spawn operation itself.
func (h *HubServer) maybeSupersedeParent(task hubSpawnTask) {
	h.trySupersedeParent(task)
}

// trySupersedeParent reports done when the landing is decided: its row is
// queued or it is definitively ineligible. A transient failure reports false.
func (h *HubServer) trySupersedeParent(task hubSpawnTask) (done bool) {
	if !h.supersedeClose || h.inbox == nil {
		return true
	}
	parent := supersededParentOf(task)
	successor := task.Job
	if parent == "" || successor == "" || parent == successor {
		return true
	}
	// An implicit parent (ARBITER_JOB) never closes anything: nobody asked
	// for a replacement.
	if !task.Request.ParentJobExplicit {
		return true
	}
	// Only an implementer successor takes over: a reviewer or consult child
	// must never close its parent's pane (docs/config.md).
	if hubSpawnFlagArg(task.Request.Args, "--role") != "implementer" {
		return true
	}
	succOwner := task.Owner
	if succOwner == "" {
		succOwner = hubSpawnFlagArg(task.Request.Args, "--owner")
	}
	ctx, cancel := context.WithTimeout(h.closeCtx, 30*time.Second)
	defer cancel()
	retry, err := h.createSupersedeAction(ctx, parent, successor, succOwner)
	if err != nil {
		// One warn per request: a retried landing must not repeat it.
		if _, seen := h.supersedeWarned.LoadOrStore(task.Request.RequestID, struct{}{}); !seen {
			h.logger.Warn("successor close not queued", "parent", parent, "successor", successor, "retry", retry, "reason", err.Error())
		}
	}
	return !retry
}

// retrySupersedeLandings queues the close of every recent explicit landing
// whose first attempt could not decide, so a parent whose node reconnects
// later, or a hub that restarted between the landing and the row insert, is
// not lost. It runs on the pool action loop, at most once a minute.
func (h *HubServer) retrySupersedeLandings(ctx context.Context) {
	if !h.supersedeClose || h.inbox == nil {
		return
	}
	now := h.now().UnixMilli()
	if last := h.supersedeScanned.Load(); now-last < supersedeScanInterval.Milliseconds() || !h.supersedeScanned.CompareAndSwap(last, now) {
		return
	}
	tasks, err := h.inbox.store.recentExplicitParentSpawns(ctx, h.now().Add(-supersedeLandingWindow))
	if err != nil {
		h.logger.Warn("successor close landings unavailable", "error", h.inbox.store.loggable("successor close landings", err))
		return
	}
	for _, task := range tasks {
		if ctx.Err() != nil {
			return
		}
		if _, decided := h.supersedeDecided.Load(task.Request.RequestID); decided {
			continue
		}
		if h.trySupersedeParent(task) {
			h.supersedeDecided.Store(task.Request.RequestID, struct{}{})
		}
	}
}

// createSupersedeAction stores the durable close intent for parent. retry
// reports a transient obstacle (the parent's node or hk or the store is
// unavailable, or the parent is not known yet): the landing is tried again.
// A nil error with no retry and no row is an expected no-op (ended parent,
// --keep, a row already exists); an error without retry is a definitive
// refusal. Both errors are one warn for the caller.
func (h *HubServer) createSupersedeAction(ctx context.Context, parent, successor, succOwner string) (retry bool, err error) {
	unlock, err := h.poolActionLocks.lock(ctx, parent)
	if err != nil {
		return true, err
	}
	defer unlock()
	h.mu.Lock()
	job := h.jobs[parent]
	var node, owner, pane string
	var epoch uint64
	known, ended := job != nil, job != nil && job.Completed
	if known && !ended {
		node, owner, pane, epoch = job.Node, job.OwnerLane, job.Pane, job.Epoch
	}
	record := h.nodes[node]
	closeOps := record != nil && record.agent != nil && record.capabilities[hubCapabilityCloseOps]
	orphaned := known && job.Orphaned
	h.mu.Unlock()
	if ended {
		return false, nil
	}
	if !known || orphaned || node == "" || owner == "" {
		return true, errors.New("parent job not known on a live node yet")
	}
	// The successor may only end a parent it owns: a blocked pane of another
	// lane is usually an open operator decision.
	if succOwner != owner {
		return false, fmt.Errorf("successor owner %q differs from the parent's lane %q", succOwner, owner)
	}
	if epoch == 0 || !closeOps {
		return true, errors.New("close ops unavailable on the parent's node")
	}
	// Only a parent this hub spawned is known not to be --keep: the flag
	// lives in its spawn request, not in the node's heartbeat.
	parentSpawn, err := h.inbox.store.latestSpawnForJob(ctx, parent)
	if errors.Is(err, sql.ErrNoRows) {
		return false, errors.New("parent has no spawn record on this hub")
	}
	if err != nil {
		return true, err
	}
	if hubSpawnHasBoolFlag(parentSpawn.Request.Args, "--keep") {
		return false, nil
	}
	if spawnHolderHost(parentSpawn) != node {
		return false, errors.New("parent's node differs from its spawn record")
	}
	if parentSpawn.Outcome.Pane != "" {
		pane = parentSpawn.Outcome.Pane
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return true, err
	}
	if err := app.CheckTrust(); err != nil {
		return true, err
	}
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	hkRecord, exists, err := app.ExistingJob(readCtx, parent)
	cancel()
	state, _ := hkRecord["state"].(string)
	if err != nil {
		return true, err
	}
	if !exists || (state != "claimed" && state != "spawned") {
		return false, nil
	}
	label := heldAgentName(parentSpawn, hkRecord)
	endReason := supersedeEndReason(successor)
	if !validNodeEndReason(endReason) || !validNodePaneName(label) || !nodeCommandPaneIDPattern.MatchString(pane) || !validLaneReference(owner) {
		return false, errors.New("invalid close target")
	}
	if rows, err := h.inbox.store.supersedeActionsOf(ctx, parent); err != nil {
		return true, err
	} else if len(rows) > 0 {
		return false, nil
	}
	if pending, err := h.inbox.store.pendingPoolAction(ctx, parent); err != nil || pending != "" {
		if err == nil {
			err = errors.New("parent already has a pending close action")
		}
		return true, err
	}
	id, err := fleet.NewRequestID()
	if err != nil {
		return true, err
	}
	action := poolAction{ID: id, Job: parent, Node: node, Epoch: epoch, Phase: poolActionPending, Data: poolActionData{
		Owner: owner, Pane: pane, Label: label, EndReason: endReason,
		Text: fmt.Sprintf("[panewire] closed %s on %s: superseded by %s", parent, node, successor),
		Kind: poolActionKindSupersedeClose, Supersede: &poolSupersede{Successor: successor},
	}}
	if err := h.inbox.store.insertPoolAction(ctx, action); err != nil {
		return true, err
	}
	h.poolActionObserved.Store(action.ID, struct{}{})
	h.logger.Info("successor close action created", "node", node, "job", parent, "successor", successor, "action", action.ID)
	return false, nil
}

// prepareSupersedeClose runs the supersede-specific steps of a pending row
// before the shared close protocol: a parent that ended or moved needs no
// close, and job.superseded is recorded once. It reports stop when the row is
// finished or waiting; an error leaves the row pending for the next pass.
func (h *HubServer) prepareSupersedeClose(ctx context.Context, action *poolAction) (stop bool, err error) {
	data := action.Data.Supersede
	if data == nil || data.Successor == "" {
		return true, h.refusePoolAction(ctx, *action)
	}
	if action.Data.Args != nil {
		// A close request exists: the protocol, not this check, decides.
		return false, nil
	}
	if !h.supersedeClose {
		// The off switch also cancels a row that has sent nothing yet (it
		// may have been queued before the flag was turned off, or waited on
		// a working pane). A request already dispatched is finished by the
		// protocol above, which fences it safely.
		h.logger.Info("successor close action cancelled: --supersede-close is off", "job", action.Job, "action", action.ID)
		return true, h.refusePoolAction(ctx, *action)
	}
	h.mu.Lock()
	job := h.jobs[action.Job]
	gone := job != nil && (job.Completed || job.Node != action.Node || job.Epoch != action.Epoch)
	h.mu.Unlock()
	if gone {
		// Nothing was sent, so there is nothing to release: the parent ended
		// or another owner holds it now.
		return true, h.refusePoolAction(ctx, *action)
	}
	if data.Recorded {
		return false, nil
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return true, err
	}
	if err := app.CheckTrust(); err != nil {
		return true, err
	}
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = app.MarkJobSuperseded(writeCtx, action.Job, data.Successor)
	cancel()
	if err != nil {
		return true, fmt.Errorf("job.superseded not recorded: %w", err)
	}
	data.Recorded = true
	if err := h.inbox.store.savePoolAction(ctx, *action); err != nil {
		return true, err
	}
	return false, nil
}

// refuseCloseAction ends a row whose close the node refused. A supersede row
// whose pane is still the parent's was refused by a race with a state change
// (the pane resumed work, or its composer filled), so it moves to a new
// request id instead of ending, up to supersedeMaxGenerations; a pane that
// changed hands or is gone ends the row for good.
func (h *HubServer) refuseCloseAction(ctx context.Context, action poolAction) error {
	data := action.Data.Supersede
	if action.Data.Kind != poolActionKindSupersedeClose || data == nil || int(data.Generation)+1 >= supersedeMaxGenerations {
		return h.refusePoolAction(ctx, action)
	}
	probe := h.probeHeldPane(ctx, action.Node, action.Data.Pane, action.Data.Label)
	if probe.state == heldPaneAbsent || probe.state == heldPanePresent && probe.row.PaneID != action.Data.Pane {
		return h.refusePoolAction(ctx, action)
	}
	if probe.state != heldPanePresent {
		return errors.New("pane unavailable")
	}
	id, err := fleet.NewRequestID()
	if err != nil {
		return err
	}
	next := action
	next.ID, next.Phase = id, poolActionPending
	next.Data = action.Data
	next.Data.Args, next.Data.TargetAttempt, next.Data.RetryID, next.Data.RetryAfter, next.Data.RetryCount = nil, "", "", 0, 0
	next.Data.Supersede = &poolSupersede{Successor: data.Successor, Generation: data.Generation + 1, Recorded: data.Recorded}
	action.Phase = poolActionRefused
	if err := h.inbox.store.replaceSupersedeAction(ctx, action, next); err != nil {
		return err
	}
	h.poolActionObserved.Delete(action.ID)
	h.poolActionObserved.Store(next.ID, struct{}{})
	h.loopNoteDone(loopPoolActions, action.ID)
	h.logger.Info("successor close action retried", "node", next.Node, "job", next.Job, "action", next.ID, "retry_of", action.ID, "generation", next.Data.Supersede.Generation)
	return nil
}

// supersedeEventID is the fixed id of the one info line a supersede close owes
// the parent's owner lane.
func supersedeEventID(job string) string { return "superseded:" + job }

// hubSpawnHasBoolFlag reports whether a hub spawn argv carries the boolean
// flag, read as strict flag/value pairs like hubSpawnFlagArg so a value
// cannot forge it.
func hubSpawnHasBoolFlag(args []string, flag string) bool {
	for i := 0; i < len(args); {
		switch elem := args[i]; {
		case elem == flag:
			return true
		case elem == "--keep" || elem == "--job-dup-ok" || elem == "--task-hk-bypass" || elem == "--allow-escalation":
			i++
		default:
			i += 2
		}
	}
	return false
}

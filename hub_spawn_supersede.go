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
	// Fence is the supersede fence generation the row was queued under. A hub
	// started with the switch off advances the fence, which cancels the row
	// for dispatch in every later hub (initSupersedeFence).
	Fence int64 `json:",omitempty"`
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

const (
	// supersedeLandingWindow bounds how long after a landing the hub keeps
	// retrying to queue its parent's close.
	supersedeLandingWindow = 48 * time.Hour
	// supersedeLandingInterval paces the retry pass; each pass tries at most
	// supersedeLandingBatch landings within supersedeLandingBudget.
	supersedeLandingInterval = time.Minute
	supersedeLandingBatch    = 50
	supersedeLandingBudget   = 25 * time.Second
)

// supersedeLandingOf is the durable intent of a landed successor, or false
// when the landing never owes a close: the switch is off, the parent was not
// typed explicitly, or the successor is not an implementer that names one.
func (h *HubServer) supersedeLandingOf(task hubSpawnTask) (supersedeLanding, bool) {
	if !h.supersedeClose || h.inbox == nil {
		return supersedeLanding{}, false
	}
	parent := supersededParentOf(task)
	successor := task.Job
	if parent == "" || successor == "" || parent == successor {
		return supersedeLanding{}, false
	}
	// An implicit parent (ARBITER_JOB) never closes anything: nobody asked
	// for a replacement.
	if !task.Request.ParentJobExplicit {
		return supersedeLanding{}, false
	}
	// Only an implementer successor takes over: a reviewer or consult child
	// must never close its parent's pane (docs/config.md).
	if hubSpawnFlagArg(task.Request.Args, "--role") != "implementer" {
		return supersedeLanding{}, false
	}
	owner := task.Owner
	if owner == "" {
		owner = hubSpawnFlagArg(task.Request.Args, "--owner")
	}
	return supersedeLanding{RequestID: task.Request.RequestID, Parent: parent, Successor: successor, Owner: owner, Created: h.now().UnixMilli()}, true
}

// wakeSupersedeLandings asks the landing loop for a pass now, so a landed
// successor's close is queued without waiting for the next tick. The intent is
// already stored; the wake only saves time.
func (h *HubServer) wakeSupersedeLandings() {
	select {
	case h.supersedeKick <- struct{}{}:
	default:
	}
}

// initSupersedeFence settles the switch against the durable fence before the
// hub accepts any work. A start with the switch off advances the fence and
// fails when it cannot: that is the revocation of every landing and close row
// of earlier generations, and it must not depend on a later sweep, on the
// nodes, or on the store being healthy afterwards. A start with the switch on
// only reads the fence; when it cannot, supersede dispatch stays closed.
func (h *HubServer) initSupersedeFence() error {
	if h.inbox == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(h.closeCtx, 30*time.Second)
	defer cancel()
	if !h.supersedeClose {
		if err := h.inbox.store.advanceSupersedeFence(ctx); err != nil {
			return fmt.Errorf("supersede fence not recorded: %w", err)
		}
		// Rows that sent nothing end now, in a form a pre-change binary also
		// ignores (a rollback must not dispatch a revoked row). Rows with a
		// stored request stay for the node release (docs/config.md, rolling
		// back).
		if err := h.inbox.store.refuseUnsentSupersedeActions(ctx); err != nil {
			return fmt.Errorf("supersede rows not cancelled: %w", err)
		}
		return nil
	}
	if _, err := h.inbox.store.supersedeGeneration(ctx); err != nil {
		// The configured switch stays on: the fence being unreadable is not a
		// revocation. Every dispatch and intent pass reads the fence itself
		// and withholds work (leaving it unchanged) until a read succeeds.
		h.logger.Warn("successor close withheld: the supersede fence is unreadable", "error", h.inbox.store.loggable("supersede fence", err))
	}
	return nil
}

// supersedeFenced reports whether a supersede row may no longer dispatch: the
// switch is off in this process, or the fence moved past the row's generation
// (a hub started with the switch off since). An unreadable fence is an error,
// never permission.
func (h *HubServer) supersedeFenced(ctx context.Context, action poolAction) (bool, error) {
	if action.Data.Supersede == nil || !h.supersedeClose {
		return true, nil
	}
	current, err := h.inbox.store.supersedeGeneration(ctx)
	if err != nil {
		return true, err
	}
	return action.Data.Supersede.Fence != current, nil
}

// attemptSupersedeLanding tries to queue the landing's close and settles its
// intent when the landing is decided: its row is queued or it is definitively
// ineligible. The try is recorded first, so a probe that consumes the pass
// cannot keep the same landing at the head of the line.
func (h *HubServer) attemptSupersedeLanding(ctx context.Context, landing supersedeLanding) {
	store := h.inbox.store
	if err := store.settleSupersedeLanding(ctx, landing.RequestID, false, h.now()); err != nil {
		h.logger.Warn("successor close intent not settled", "parent", landing.Parent, "error", store.loggable("successor close intent", err))
		return
	}
	retry, err := h.createSupersedeAction(ctx, landing)
	if err != nil {
		// One warn per request: a retried landing must not repeat it.
		if _, seen := h.supersedeWarned.LoadOrStore(landing.RequestID, struct{}{}); !seen {
			h.logger.Warn("successor close not queued", "parent", landing.Parent, "successor", landing.Successor, "retry", retry, "reason", err.Error())
		}
	}
	if !retry {
		if err := store.settleSupersedeLanding(ctx, landing.RequestID, true, h.now()); err != nil {
			h.logger.Warn("successor close intent not settled", "parent", landing.Parent, "error", store.loggable("successor close intent", err))
		}
	}
}

// sweepSupersedeLandings is one retry pass over the undecided intents. It
// runs on its own loop, never the pool action reconciler's: a slow hk must
// not delay a close already queued. With the switch off every intent ends; an
// intent of an earlier generation is cancelled, so enabling the switch again
// closes nothing retroactively.
func (h *HubServer) sweepSupersedeLandings(ctx context.Context) {
	if h.inbox == nil {
		return
	}
	store := h.inbox.store
	if !h.supersedeClose {
		if err := store.endSupersedeLandings(ctx, time.Time{}); err != nil {
			h.logger.Warn("successor close intents unavailable", "error", store.loggable("successor close intents", err))
		}
		return
	}
	generation, err := store.supersedeGeneration(ctx)
	if err != nil {
		h.logger.Warn("successor close intents unavailable", "error", store.loggable("successor close intents", err))
		return
	}
	if err := store.endSupersedeLandings(ctx, h.now().Add(-supersedeLandingWindow)); err != nil {
		h.logger.Warn("successor close intents unavailable", "error", store.loggable("successor close intents", err))
		return
	}
	pending, err := store.pendingSupersedeLandings(ctx, supersedeLandingBatch)
	if err != nil {
		h.logger.Warn("successor close intents unavailable", "error", store.loggable("successor close intents", err))
		return
	}
	passCtx, cancel := context.WithTimeout(ctx, supersedeLandingBudget)
	defer cancel()
	for _, landing := range pending {
		if passCtx.Err() != nil {
			return
		}
		if landing.Generation != generation {
			// Recorded before a hub started with the switch off.
			_ = store.settleSupersedeLanding(passCtx, landing.RequestID, true, h.now())
			continue
		}
		h.attemptSupersedeLanding(passCtx, landing)
	}
}

// runSupersedeLandings drives sweepSupersedeLandings until ctx ends.
func (h *HubServer) runSupersedeLandings(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	stop := context.AfterFunc(h.closeCtx, cancel)
	defer stop()
	ticker := time.NewTicker(supersedeLandingInterval)
	defer ticker.Stop()
	for {
		h.sweepSupersedeLandings(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-h.supersedeKick:
		}
	}
}

// createSupersedeAction stores the durable close intent for parent. retry
// reports a transient obstacle (the parent's node or hk or the store is
// unavailable, or the parent is not known yet): the landing is tried again.
// A nil error with no retry and no row is an expected no-op (ended parent,
// --keep, a row already exists); an error without retry is a definitive
// refusal. Both errors are one warn for the caller.
func (h *HubServer) createSupersedeAction(ctx context.Context, landing supersedeLanding) (retry bool, err error) {
	parent, successor, succOwner := landing.Parent, landing.Successor, landing.Owner
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
		Kind: poolActionKindSupersedeClose, Supersede: &poolSupersede{Successor: successor, Fence: landing.Generation},
	}}
	if err := h.inbox.store.insertSupersedeAction(ctx, action, landing.Generation); err != nil {
		// A fenced landing is cancelled, never retried: a hub started with the
		// switch off revoked it between the sweep's read and this insert.
		return errors.Is(err, errSupersedeFenced) == false, err
	}
	h.poolActionObserved.Store(action.ID, struct{}{})
	h.logger.Info("successor close action created", "node", node, "job", parent, "successor", successor, "action", action.ID)
	return false, nil
}

// prepareSupersedeClose runs the supersede-specific steps of a pending row
// before the shared close protocol: a parent that ended or moved needs no
// close. It reports stop when the row is
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
	fenced, err := h.supersedeFenced(ctx, *action)
	if err != nil {
		return true, err
	}
	if fenced {
		// The switch is off or the fence moved on: a row that sent nothing
		// ends, with no node I/O. A stored request is fenced by the shared
		// protocol (reconcilePoolAction).
		h.logger.Info("successor close action cancelled: --supersede-close was off", "job", action.Job, "action", action.ID)
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
	if fenced, err := h.supersedeFenced(ctx, action); err != nil || fenced {
		// A cancelled row asks for nothing again.
		if err != nil {
			return err
		}
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
	// The retry stays bound to the attempt the refused request named: a
	// close that the node refused because the job was re-prompted into a new
	// attempt on the same pane must not close that new session (the shared
	// protocol releases a request whose attempt moved).
	next.Data.TargetAttempt = action.Data.TargetAttempt
	if action.Data.Args != nil {
		next.Data.TargetAttempt = action.Data.Args.Attempt
	}
	next.Data.Args, next.Data.RetryID, next.Data.RetryAfter, next.Data.RetryCount = nil, "", 0, 0
	next.Data.Supersede = &poolSupersede{Successor: data.Successor, Generation: data.Generation + 1, Fence: data.Fence}
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

// supersedeMayRetry reports whether an unknown supersede request may mint a
// successor: only while no other request of the parent is live and the shared
// budget of supersedeMaxGenerations is not spent.
func (h *HubServer) supersedeMayRetry(ctx context.Context, action poolAction) (bool, error) {
	data := action.Data.Supersede
	if data == nil || int(data.Generation)+1 >= supersedeMaxGenerations {
		return false, nil
	}
	rows, err := h.inbox.store.supersedeActionsOf(ctx, action.Job)
	if err != nil {
		return false, err
	}
	for _, row := range rows {
		if row.ID != action.ID && row.Phase != poolActionRefused && row.Phase != poolActionDone {
			return false, nil
		}
	}
	return true, nil
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

package panewire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// Pool actions hold reassignment from durable intent through the node's
// settled proof. See docs/node-commands.md, "Hub close protocol".
func (h *HubServer) createPoolAction(ctx context.Context, exhaustion poolExhaustion, task hubSpawnTask, app *fleet.App) error {
	unlock, err := h.poolActionLocks.lock(ctx, task.Job)
	if err != nil {
		return err
	}
	defer unlock()
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	record, exists, err := app.ExistingJob(readCtx, task.Job)
	cancel()
	state, _ := record["state"].(string)
	if err != nil || !exists || (state != "claimed" && state != "spawned") {
		return errors.New("job is no longer live")
	}
	if pool, _ := record["pool"].(string); pool != "" && pool != exhaustion.Pool {
		return errors.New("job pool changed")
	}
	id, err := fleet.NewRequestID()
	if err != nil {
		return err
	}
	owner := task.Owner
	if strings.HasPrefix(task.Job, "mwr-") {
		owner = "mergewake"
	}
	endReason := fleet.QuotaStopEndReason(exhaustion.Pool, time.Time{}, "", "")
	label := heldAgentName(task, record)
	if !validNodeEndReason(endReason) || !validNodePaneName(label) || !nodeCommandPaneIDPattern.MatchString(task.Outcome.Pane) || !validLaneReference(owner) {
		return errors.New("invalid close target")
	}
	action := poolAction{ID: id, Job: task.Job, Phase: poolActionPending, Data: poolActionData{
		Owner: owner, Text: fmt.Sprintf("Pool %s is exhausted; job %s ended. Respawn it on another pool.", exhaustion.Pool, task.Job),
		SpawnRequest: task.Request.RequestID, Pane: task.Outcome.Pane, Label: label, EndReason: endReason,
	}}
	// The per-job lock also guards reassignment; mu is needed only to take the
	// ownership snapshot, not while waiting for the durable store.
	h.mu.Lock()
	job := h.jobs[task.Job]
	if job == nil || job.Completed || job.Orphaned || job.Node != spawnHolderHost(task) || job.Epoch == 0 {
		h.mu.Unlock()
		return errors.New("job ownership unavailable")
	}
	node := h.nodes[job.Node]
	if node == nil || node.agent == nil || !node.capabilities[hubCapabilityCloseOps] {
		h.mu.Unlock()
		return errors.New("close ops unavailable")
	}
	action.Node, action.Epoch = job.Node, job.Epoch
	h.mu.Unlock()
	if pending, err := h.inbox.store.pendingPoolAction(ctx, task.Job); err != nil || pending != "" {
		return errors.New("job already has a pending close action")
	}
	if err := h.inbox.store.insertPoolAction(ctx, action); err != nil {
		return err
	}
	h.poolActionObserved.Store(action.ID, struct{}{})
	h.logger.Info("pool close action created", "node", action.Node, "job", action.Job, "action", action.ID)
	return nil
}

const poolActionInterval = 10 * time.Second

func (h *HubServer) startPoolActions(ctx context.Context) {
	h.closeMu.Lock()
	defer h.closeMu.Unlock()
	if h.closed || h.inbox == nil {
		return
	}
	h.poolActionOnce.Do(func() {
		h.spawnWorkers.Add(1)
		go func() { defer h.spawnWorkers.Done(); h.runPoolActions(ctx) }()
	})
}

func (h *HubServer) runPoolActions(parent context.Context) {
	if h.inbox == nil {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	stop := context.AfterFunc(h.closeCtx, cancel)
	defer stop()
	ticker := time.NewTicker(poolActionInterval)
	defer ticker.Stop()
	for {
		h.reconcilePoolActions(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (h *HubServer) reconcilePoolActions(ctx context.Context) {
	if h.inbox == nil || !h.poolActionMu.TryLock() {
		return
	}
	defer h.poolActionMu.Unlock()
	actions, err := h.inbox.store.activePoolActions(ctx)
	if err != nil {
		h.logger.Warn("pool actions unavailable", "error", h.inbox.store.loggable("pool actions", err))
		return
	}
	for _, action := range actions {
		if ctx.Err() != nil {
			return
		}
		h.logObservedPoolAction(action)
		if err := h.reconcilePoolAction(ctx, action); err != nil {
			h.logger.Warn("pool action pending", "job", action.Job, "action", action.ID, "error", h.inbox.store.loggable("pool action", err))
		}
	}
}

func (h *HubServer) reconcilePoolAction(ctx context.Context, action poolAction) error {
	if action.Phase == poolActionSettled {
		return h.finishPoolAction(ctx, action)
	}
	if action.Phase == poolActionGuarded {
		return h.reconcileGuardedPoolAction(ctx, action)
	}
	ref := nodeCloseOpRefArgs{RequestID: action.ID, Job: action.Job}
	var status nodeCloseOpStatusResult
	if !h.nodeCall(ctx, action.Node, nodeCommandPaneCloseStatus, ref, hubNodeCommandWait, &status) {
		return errors.New("close status unavailable")
	}
	if status.Settled {
		action.Phase = poolActionSettled
		return h.saveFinalPoolAction(ctx, action, "settled")
	}
	if (status.State == nodeCloseOpNone || status.State == fleet.CloseOpRegistered) && !h.nodeHasCapability(action.Node, hubCapabilityCloseOps) {
		// Withdrawal can precede a binary rollback. Fence a request that has
		// not started closing rather than strand it on the withdrawn node.
		return h.releasePoolAction(ctx, action)
	}
	switch status.State {
	case fleet.CloseOpRefused, fleet.CloseOpPreempted:
		return h.refusePoolAction(ctx, action)
	case nodeCloseOpSuperseded:
		if action.Data.Args == nil || status.Attempt == action.Data.Args.Attempt {
			return errors.New("invalid superseded close proof")
		}
		return h.refusePoolAction(ctx, action)
	case nodeCloseIntentCancelled:
		action.Phase = poolActionGuarded
		return h.saveFinalPoolAction(ctx, action, "guarded")
	case fleet.CloseOpCancelled:
		if status.Released {
			return h.refusePoolAction(ctx, action)
		}
		return nil
	case fleet.CloseOpUnknown:
		if !h.nodeHasCapability(action.Node, hubCapabilityCloseOps) {
			return nil
		}
		if action.Data.Args == nil || status.Attempt == "" || status.Attempt != action.Data.Args.Attempt {
			return nil
		}
		return h.retryUnknownPoolAction(ctx, action)
	case fleet.CloseOpClosed, fleet.CloseOpGone:
		// An hk lost read is not settlement: the node may still retry the write.
		return nil
	case nodeCloseOpNone:
		if action.Data.Args == nil {
			if status.Attempt == "" {
				return h.releasePoolAction(ctx, action)
			}
			if action.Data.TargetAttempt != "" && status.Attempt != action.Data.TargetAttempt {
				return h.releasePoolAction(ctx, action)
			}
			probe := h.probeHeldPane(ctx, action.Node, action.Data.Pane, action.Data.Label)
			if probe.state == heldPaneAbsent || probe.state == heldPanePresent && probe.row.PaneID != action.Data.Pane {
				// No command was sent before Args was saved. A tombstone
				// releases this intent even if the pane vanished meanwhile.
				return h.releasePoolAction(ctx, action)
			}
			if probe.state != heldPanePresent {
				return errors.New("pane unavailable")
			}
			args := &nodePaneCloseOpArgs{RequestID: action.ID, Job: action.Job, Epoch: action.Epoch, Attempt: status.Attempt,
				PaneID: action.Data.Pane, Name: action.Data.Label, AgentStatus: probe.row.AgentStatus, Cwd: probe.row.Cwd,
				AllowUnnamed: probe.row.Name == "", EndReason: action.Data.EndReason}
			if !validNodePaneCloseOpArgs(*args) {
				return h.releasePoolAction(ctx, action)
			}
			action.Data.Args = args
			if err := h.inbox.store.savePoolAction(ctx, action); err != nil {
				return err
			}
		}
	case fleet.CloseOpRegistered, fleet.CloseOpClosing:
		if action.Data.Args == nil {
			return errors.New("close arguments unavailable")
		}
	default:
		return errors.New("unknown close status")
	}
	args := action.Data.Args
	if args.RequestID != action.ID || args.Job != action.Job || args.Epoch != action.Epoch {
		return errors.New("close arguments do not match ownership")
	}
	// Once the action is stored, I1 prevents its ownership changing. An absent
	// in-memory job after hub restart does not prevent replay of this binding.
	allowed := func() bool {
		job, node := h.jobs[action.Job], h.nodes[action.Node]
		return (job == nil || job.Node == action.Node && job.Epoch == action.Epoch) && node != nil && node.agent != nil && node.capabilities[hubCapabilityCloseOps]
	}
	var result nodePaneCloseOpResult
	if !h.nodeCallWhere(ctx, action.Node, nodeCommandPaneCloseOp, *args, hubNodeCommandWait, &result, allowed) {
		return errors.New("close ownership, capability or answer unavailable")
	}
	switch result.Outcome {
	case fleet.CloseOpRefused, fleet.CloseOpPreempted, fleet.CloseOpCancelled:
		return h.refusePoolAction(ctx, action)
	case nodeCloseOpMismatch, nodeCaptureNotAJob:
		return h.releasePoolAction(ctx, action)
	}
	return nil
}

func (h *HubServer) retryUnknownPoolAction(ctx context.Context, action poolAction) error {
	guarded, err := h.inbox.store.guardedPoolAction(ctx, action.Job)
	if err != nil || guarded {
		return err
	}
	now := h.now().UTC()
	ready, err := h.inbox.store.poolRetryReady(ctx, action, now)
	if err != nil || !ready {
		return err
	}
	probe := h.probeHeldPane(ctx, action.Node, action.Data.Pane, action.Data.Label)
	if probe.state != heldPanePresent || probe.row.PaneID != action.Data.Pane || !nodePaneClosableStatus[probe.row.AgentStatus] || !validNodeCommandPath(probe.row.Cwd) {
		return nil
	}
	unlock, err := h.poolActionLocks.lock(ctx, action.Job)
	if err != nil {
		return err
	}
	defer unlock()
	id, err := fleet.NewRequestID()
	if err != nil {
		return err
	}
	retry := action
	retry.ID = id
	retry.Data.TargetAttempt = action.Data.Args.Attempt
	retry.Data.Args = nil
	retry.Data.RetryID = ""
	inserted, err := h.inbox.store.retryPoolAction(ctx, action, retry, now)
	if err != nil {
		return err
	}
	if inserted {
		h.poolActionObserved.Store(retry.ID, struct{}{})
		h.logger.Info("pool close action retry", "node", retry.Node, "job", retry.Job, "action", retry.ID, "retry_of", action.ID)
	}
	return nil
}

// The close request is finished, but its pane or watcher may still write hk.
// Keep I1 until the record is terminal; no close, notice or role release is
// issued while this guard remains.
func (h *HubServer) reconcileGuardedPoolAction(ctx context.Context, action poolAction) error {
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	exists := false
	if err == nil {
		var record map[string]any
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		record, exists, err = app.ExistingJob(readCtx, action.Job)
		cancel()
		state, _ := record["state"].(string)
		if err == nil && exists && hkTerminalState(state) {
			return h.refusePoolAction(ctx, action)
		}
	}
	// A node may also prove the old request's final job.lost was synced. A
	// withdrawn or rolled-back node may not answer; never block the sweep on it.
	nodeCtx, cancel := context.WithTimeout(ctx, time.Second)
	var status nodeCloseOpStatusResult
	answered := h.nodeCall(nodeCtx, action.Node, nodeCommandPaneCloseStatus, nodeCloseOpRefArgs{RequestID: action.ID, Job: action.Job}, time.Second, &status)
	cancel()
	if answered && status.Settled {
		return h.refusePoolAction(ctx, action)
	}
	if err != nil || !exists {
		return errors.New("guarded job record unavailable")
	}
	return nil
}

// A mismatch itself is not final: release must first leave a tombstone for
// the request so a delayed close cannot register after reassignment.
func (h *HubServer) releasePoolAction(ctx context.Context, action poolAction) error {
	var status nodeCloseOpStatusResult
	ref := nodeCloseOpRefArgs{RequestID: action.ID, Job: action.Job}
	if !h.nodeCall(ctx, action.Node, nodeCommandPaneCloseRelease, ref, hubNodeCommandWait, &status) || status.State != fleet.CloseOpCancelled || !status.Released {
		return errors.New("release not proved")
	}
	return h.refusePoolAction(ctx, action)
}

func (h *HubServer) refusePoolAction(ctx context.Context, action poolAction) error {
	action.Phase = poolActionRefused
	if err := h.saveFinalPoolAction(ctx, action, "refused"); err != nil {
		return err
	}
	h.poolSweep.release(action.Job)
	h.poolActionObserved.Delete(action.ID)
	return nil
}

func (h *HubServer) logObservedPoolAction(action poolAction) {
	if _, seen := h.poolActionObserved.LoadOrStore(action.ID, struct{}{}); seen {
		return
	}
	if action.Phase == poolActionPending {
		h.logger.Info("pool close action pending", "node", action.Node, "job", action.Job, "action", action.ID)
		return
	}
	h.logger.Info("pool close action finalized", "node", action.Node, "job", action.Job, "action", action.ID, "outcome", action.Phase)
}

func (h *HubServer) saveFinalPoolAction(ctx context.Context, action poolAction, outcome string) error {
	if err := h.inbox.store.savePoolAction(ctx, action); err != nil {
		return err
	}
	h.poolActionObserved.Store(action.ID, struct{}{})
	h.logger.Info("pool close action finalized", "node", action.Node, "job", action.Job, "action", action.ID, "outcome", outcome)
	return nil
}

func (h *HubServer) finishPoolAction(ctx context.Context, action poolAction) error {
	unlock, free := h.lockWithoutPoolAction(ctx, action.Job)
	if !free {
		return errors.New("another close action is pending")
	}
	defer unlock()
	h.mu.Lock()
	job := h.jobs[action.Job]
	moved := job != nil && (job.Node != action.Node || job.Epoch != action.Epoch)
	h.mu.Unlock()
	if moved {
		// Settlement permits reassignment. Once another owner has the job,
		// this old action must not release its role or announce its end.
		action.Phase = poolActionDone
		if err := h.inbox.store.savePoolAction(ctx, action); err != nil {
			return err
		}
		h.poolActionObserved.Delete(action.ID)
		return nil
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return err
	}
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	record, exists, err := app.ExistingJob(readCtx, action.Job)
	cancel()
	state, _ := record["state"].(string)
	reason, _ := record["end_reason"].(string)
	if err != nil || !exists || !hkTerminalState(state) {
		return errors.New("terminal job record unavailable")
	}
	if err := h.inbox.store.releaseCompletedSpawnRoleOf(ctx, action.Data.SpawnRequest); err != nil {
		return err
	}
	if state == "lost" && reason == action.Data.EndReason {
		eventID := "pool-closed:" + action.Job
		owned, err := h.inbox.store.poolNoticeOwned(ctx, action.Data.Owner, eventID)
		if err != nil {
			return err
		}
		if !owned {
			result := h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: eventID, JobID: laneEventTransportID(action.Data.Owner, eventID), OwnerLane: action.Data.Owner, NotifyType: "info", Text: action.Data.Text, Host: "hub", Epoch: 1}, nil)
			if result.ID == 0 || result.PersistFailed {
				return errors.New("owner notice unavailable")
			}
		}
	}
	action.Phase = poolActionDone
	if err := h.inbox.store.savePoolAction(ctx, action); err != nil {
		return err
	}
	h.poolActionObserved.Delete(action.ID)
	return nil
}

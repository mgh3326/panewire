package panewire

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// Pool actions hold reassignment from durable intent through the node's
// settled proof. See docs/node-commands.md, "Hub close protocol".
// A non-nil handover makes the row an implementer hand-over (hk 4633,
// hub_pool_handover.go): its close writes the old record with the successor
// pointer, and settlement submits the stored successor.
func (h *HubServer) createPoolAction(ctx context.Context, exhaustion poolExhaustion, task hubSpawnTask, app *fleet.App, handover *poolHandover) error {
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
	if handover != nil {
		endReason = handoverEndReason(record, exhaustion, handover.Job)
	}
	label := heldAgentName(task, record)
	if !validNodeEndReason(endReason) || !validNodePaneName(label) || !nodeCommandPaneIDPattern.MatchString(task.Outcome.Pane) || !validLaneReference(owner) {
		return errors.New("invalid close target")
	}
	text := fmt.Sprintf("Pool %s is exhausted; job %s ended. Respawn it on another pool.", exhaustion.Pool, task.Job)
	if exhaustion.accountCredit() {
		// Only a provider credit rejection speaks about the account's credits;
		// a free-limit or window exhaustion also names an account.
		text = fmt.Sprintf("Account %s has no provider credit left; job %s ended. Respawn it on another account.", exhaustion.Account, task.Job)
	}
	action := poolAction{ID: id, Job: task.Job, Phase: poolActionPending, Data: poolActionData{
		Owner: owner, Text: text,
		SpawnRequest: task.Request.RequestID, Pane: task.Outcome.Pane, Label: label, EndReason: endReason,
	}}
	if handover != nil {
		action.Data.Kind, action.Data.Handover = poolActionKindHandover, handover
		action.Data.Text = poolExhaustionText(exhaustion, task.Job)
	}
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
	// One row per job: a settled hand-over still owns it.
	if owner, err := h.inbox.store.poolActionOwns(ctx, task.Job); err != nil || owner != "" {
		return errors.New("job already has a pending close action")
	}
	if handover != nil {
		if _, err := h.inbox.store.latestSpawnForJob(ctx, handover.Job); !errors.Is(err, sql.ErrNoRows) {
			// Only a definite absence admits the row; an unreadable store
			// is not one.
			return errors.New("successor already exists or unreadable")
		}
	}
	if err := h.inbox.store.insertPoolAction(ctx, action); err != nil {
		return err
	}
	h.poolActionObserved.Store(action.ID, struct{}{})
	h.logger.Info("pool close action created", "node", action.Node, "job", action.Job, "action", action.ID)
	return nil
}

const poolActionInterval = 10 * time.Second

var (
	errCloseAttemptUnavailable = errors.New("close attempt unavailable")
	errPoolReleaseNotProved    = errors.New("release not proved")
)

func (h *HubServer) startPoolActions(ctx context.Context) {
	h.closeMu.Lock()
	defer h.closeMu.Unlock()
	if h.closed || h.inbox == nil {
		return
	}
	h.poolActionOnce.Do(func() {
		h.spawnWorkers.Add(2)
		go func() { defer h.spawnWorkers.Done(); h.runPoolActions(ctx) }()
		go func() { defer h.spawnWorkers.Done(); h.runSupersedeLandings(ctx) }()
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
	h.loopTick(loopPoolActions)
	started := h.now().UTC()
	actions, err := h.inbox.store.activePoolActions(ctx)
	if err != nil {
		h.loopNoteSilentError(loopPoolActions, "hub_pool_action.go:activePoolActions", err)
		h.logger.Warn("pool actions unavailable", "error", h.inbox.store.loggable("pool actions", err))
		return
	}
	h.loopClearSilent("hub_pool_action.go:activePoolActions")
	for _, action := range actions {
		h.loopNoteMade(loopPoolActions, action.ID)
	}
	// Inventory the whole snapshot before any potentially slow node call. The
	// count lets a rollback operator detect missing log entries; see the runbook.
	inventory, err := fleet.NewRequestID()
	if err != nil {
		h.logger.Warn("pool action inventory unavailable", "error", err)
	} else {
		for _, action := range actions {
			h.logger.Info("pool close action inventory entry", "inventory", inventory, "node", action.Node, "job", action.Job, "action", action.ID, "phase", action.Phase)
		}
		h.logger.Info("pool close action inventory complete", "inventory", inventory, "started_at", started, "count", len(actions))
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
	err := h.reconcilePoolActionStep(ctx, action)
	stuck := errors.Is(err, errCloseAttemptUnavailable) || errors.Is(err, errPoolReleaseNotProved)
	if !stuck && action.Data.StuckSince == 0 {
		return err
	}
	now := h.now().UTC()
	current, observeErr := h.inbox.store.observePoolActionStuck(ctx, action.ID, stuck, now)
	if observeErr != nil {
		return errors.Join(err, observeErr)
	}
	if !stuck || current.Phase != poolActionPending || current.Data.StuckSince == 0 || now.Sub(time.UnixMilli(current.Data.StuckSince)) < 30*time.Minute {
		return err
	}
	if noticeErr := h.notifyStuckPoolAction(ctx, current, err.Error()); noticeErr != nil {
		return errors.Join(err, noticeErr)
	}
	return err
}

func (h *HubServer) notifyStuckPoolAction(ctx context.Context, action poolAction, reason string) error {
	eventID := "pool-action-stuck:" + action.ID
	owned, err := h.inbox.store.poolNoticeOwned(ctx, action.Data.Owner, eventID)
	if err != nil || owned {
		return err
	}
	text := fmt.Sprintf("[panewire] The hub holds job %s on host %s: %s (row %s). Check the host; you can hand the job over by hand after checking it.", action.Job, action.Node, reason, action.ID)
	result := h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: eventID, JobID: laneEventTransportID(action.Data.Owner, eventID), OwnerLane: action.Data.Owner, NotifyType: "info", Text: utf8PrefixWithin(text, laneEventTextLimit), Host: "hub", Epoch: 1}, nil)
	if result.ID == 0 || result.PersistFailed {
		return errors.New("stuck pool action notice unavailable")
	}
	return nil
}

func (h *HubServer) reconcilePoolActionStep(ctx context.Context, action poolAction) error {
	if action.Phase == poolActionSettled {
		return h.finishPoolAction(ctx, action)
	}
	if action.Phase == poolActionGuarded {
		return h.reconcileGuardedPoolAction(ctx, action)
	}
	if action.Data.Kind == poolActionKindSupersedeClose {
		if stop, err := h.prepareSupersedeClose(ctx, &action); stop || err != nil {
			return err
		}
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
	if action.Data.Kind == poolActionKindSupersedeClose {
		fenced, err := h.supersedeFenced(ctx, action)
		if err != nil {
			return err
		}
		if fenced && status.State == nodeCloseOpNone && status.Attempt == "" && action.Data.Args != nil {
			// The node holds no attempt this request could bind to (its job
			// directory is gone, or it never recorded one), so a close op for
			// it can only answer not-a-job or mismatch: nothing is left to
			// fence, and no tombstone can be written for a job the node no
			// longer has.
			return h.refusePoolAction(ctx, action)
		}
		if fenced && (status.State == nodeCloseOpNone || status.State == fleet.CloseOpRegistered) {
			// The switch is off, or a hub started with it off moved the
			// fence since this row was queued: a request stored but not
			// begun is released, and only the release tombstone retires it,
			// so an unanswered node leaves the row cancelled-pending across
			// restarts. A request the node began is finished below.
			return h.releasePoolAction(ctx, action)
		}
		if fenced && status.State == fleet.CloseOpUnknown {
			// A cancelled row creates no replacement request.
			return nil
		}
	}
	if (status.State == nodeCloseOpNone || status.State == fleet.CloseOpRegistered) && !h.nodeHasCapability(action.Node, hubCapabilityCloseOps) {
		// Withdrawal can precede a binary rollback. Fence a request that has
		// not started closing rather than strand it on the withdrawn node.
		return h.releasePoolAction(ctx, action)
	}
	switch status.State {
	case fleet.CloseOpRefused, fleet.CloseOpPreempted:
		if action.Data.Kind == poolActionKindHandover && action.Data.Handover != nil {
			action.Data.Handover.Reason = status.Reason
		}
		return h.refuseCloseAction(ctx, action)
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
		if action.Data.Kind == poolActionKindSupersedeClose {
			// One retry chain per parent, with one budget shared with the
			// refused-close retries (refuseCloseAction).
			if may, err := h.supersedeMayRetry(ctx, action); err != nil || !may {
				return err
			}
		}
		return h.retryUnknownPoolAction(ctx, action)
	case fleet.CloseOpClosed, fleet.CloseOpGone:
		// An hk lost read is not settlement: the node may still retry the write.
		return nil
	case nodeCloseOpNone:
		if action.Data.Kind == poolActionKindHandover && action.Data.Args != nil && status.Attempt == "" {
			// A transiently unreadable attempt is not proof that the saved
			// close may bypass the pre-close checks after a hub restart.
			return errCloseAttemptUnavailable
		}
		if action.Data.Kind == poolActionKindHandover && status.Attempt != "" {
			// The node has registered no close yet, even when Args were saved
			// before a restart: the last point the hand-over can still be
			// withdrawn without a trace. The job's lock is held through the
			// close command, so the checks bind the close (hk 4633).
			unlock, err := h.poolActionLocks.lock(ctx, action.Job)
			if err != nil {
				return err
			}
			defer unlock()
			due, err := h.handoverBeforeClose(ctx, &action)
			if err != nil {
				return err
			}
			if !due {
				return h.releasePoolAction(ctx, action)
			}
		}
		if action.Data.Args == nil {
			if status.Attempt == "" {
				return h.releasePoolAction(ctx, action)
			}
			if action.Data.TargetAttempt != "" && status.Attempt != action.Data.TargetAttempt {
				return h.releasePoolAction(ctx, action)
			}
			if action.Data.Kind == poolActionKindSupersedeClose && action.Data.TargetAttempt == "" {
				// Bind the parent's attempt at the first look, before a
				// working pane defers the close: a parent re-prompted into
				// a new attempt while it works is released above, never
				// closed by this intent.
				action.Data.TargetAttempt = status.Attempt
				if err := h.inbox.store.savePoolAction(ctx, action); err != nil {
					return err
				}
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
			if handover := action.Data.Handover; action.Data.Kind == poolActionKindHandover && handover != nil && probe.row.Cwd != handover.Cwd {
				// The close must act on the worktree that was checked.
				return h.releasePoolAction(ctx, action)
			}
			if action.Data.Kind == poolActionKindSupersedeClose && !supersedeCloseStatus(probe.row.AgentStatus) {
				// A working (or blocked) parent is never closed: the row
				// stays pending and the next pass looks again.
				return nil
			}
			args := &nodePaneCloseOpArgs{RequestID: action.ID, Job: action.Job, Epoch: action.Epoch, Attempt: status.Attempt,
				PaneID: action.Data.Pane, Name: action.Data.Label, AgentStatus: probe.row.AgentStatus, Cwd: probe.row.Cwd,
				AllowUnnamed: probe.row.Name == "", EndReason: action.Data.EndReason}
			if handover := action.Data.Handover; action.Data.Kind == poolActionKindHandover && handover != nil {
				checkout := handover.Successor.Repository.Checkout
				if checkout == nil {
					return errors.New("handover checkout unavailable")
				}
				args.Handover = &nodeHandoverCloseGuard{Origin: checkout.Origin, Branch: handover.Branch, Commit: checkout.Sha}
			}
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
	if action.Data.Kind == poolActionKindHandover && args.Handover == nil {
		return errors.New("handover close guard unavailable")
	}
	// Once the action is stored, I1 prevents its ownership changing. An absent
	// in-memory job after hub restart does not prevent replay of this binding.
	allowed := func() bool {
		job, node := h.jobs[action.Job], h.nodes[action.Node]
		return (job == nil || job.Node == action.Node && job.Epoch == action.Epoch) && node != nil && node.agent != nil && node.capabilities[hubCapabilityCloseOps] && (action.Data.Kind != poolActionKindHandover || node.capabilities[hubCapabilityHandoverCheck])
	}
	var result nodePaneCloseOpResult
	if !h.nodeCallWhere(ctx, action.Node, nodeCommandPaneCloseOp, *args, hubNodeCommandWait, &result, allowed) {
		return errors.New("close ownership, capability or answer unavailable")
	}
	switch result.Outcome {
	case fleet.CloseOpRefused, fleet.CloseOpPreempted:
		if action.Data.Kind == poolActionKindHandover && action.Data.Handover != nil {
			action.Data.Handover.Reason = result.Reason
		}
		return h.refuseCloseAction(ctx, action)
	case fleet.CloseOpCancelled:
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
	if retry.Data.Supersede != nil {
		next := *retry.Data.Supersede
		next.Generation++
		retry.Data.Supersede = &next
	}
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
	// The action is stored guarded, so the node may now be told it can stop
	// owing the intent_cancelled answer (hk 5073): its rollback drain waits
	// for this ack. It is repeated every pass until the node confirms it, and
	// the action is not finished before that, so a failed or lost ack is
	// retried even once the hk record is terminal. Only a node that rejects
	// the argument (invalid_args: it predates the ack, so it never owed one)
	// is let go without it. A node that does not answer is retried; the
	// fence it holds is the safe side.
	acked := action.Data.Acked
	var rejected bool
	askNode := func() (status nodeCloseOpStatusResult, answered bool) {
		nodeCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		ref := nodeCloseOpRefArgs{RequestID: action.ID, Job: action.Job, Ack: !acked}
		var code string
		answered, code = h.nodeCallCode(nodeCtx, action.Node, nodeCommandPaneCloseStatus, ref, time.Second, &status, nil)
		rejected = !answered && code == "invalid_args"
		if answered && !acked && status.State == nodeCloseIntentCancelled {
			// The node answered the ack: no later pass repeats it.
			acked = true
			action.Data.Acked = true
			if err := h.inbox.store.savePoolAction(ctx, action); err != nil {
				h.logger.Warn("pool action ack not saved", "job", action.Job, "action", action.ID, "error", h.inbox.store.loggable("pool action", err))
			}
		}
		return status, answered
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	exists := false
	if err == nil {
		var record map[string]any
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		record, exists, err = app.ExistingJob(readCtx, action.Job)
		cancel()
		state, _ := record["state"].(string)
		if err == nil && exists && hkTerminalState(state) {
			if !acked {
				if _, answered := askNode(); !answered && !rejected {
					return errors.New("guard ack unconfirmed")
				}
			}
			return h.refusePoolAction(ctx, action)
		}
	}
	// A node may also prove the old request's final job.lost was synced. A
	// withdrawn or rolled-back node may not answer; never block the sweep on it.
	if status, answered := askNode(); answered && status.Settled {
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
		return errPoolReleaseNotProved
	}
	return h.refusePoolAction(ctx, action)
}

func (h *HubServer) refusePoolAction(ctx context.Context, action poolAction) error {
	if action.Data.Kind == poolActionKindHandover && action.Data.Handover != nil && action.Data.Handover.Reason != "recovered" {
		handover := action.Data.Handover
		if handover.Reason == "" {
			handover.Reason = "close_refused"
		}
		handover.Outcome = handoverFailed
		// Save the reason before publishing: a lost acknowledgement or a
		// restart must replay the same notice without closing the old pane.
		if err := h.inbox.store.savePoolAction(ctx, action); err != nil {
			return err
		}
		eventID := handoverNoticeID(action)
		owned, err := h.inbox.store.poolNoticeOwned(ctx, action.Data.Owner, eventID)
		if err != nil {
			return err
		}
		if !owned {
			text := fmt.Sprintf("[panewire] hand-over of %s failed (%s): %s", action.Job, handover.Reason, action.Data.Text)
			result := h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: eventID, JobID: laneEventTransportID(action.Data.Owner, eventID), OwnerLane: action.Data.Owner, NotifyType: "info", Text: utf8PrefixWithin(text, laneEventTextLimit), Host: "hub", Epoch: 1}, nil)
			if result.ID == 0 || result.PersistFailed {
				return errors.New("owner notice unavailable")
			}
		}
	}
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
	// The durable terminal transition is the action's exit: settled,
	// guarded or refused outcomes all land here.
	h.loopNoteDone(loopPoolActions, action.ID)
	h.poolActionObserved.Store(action.ID, struct{}{})
	h.logger.Info("pool close action finalized", "node", action.Node, "job", action.Job, "action", action.ID, "outcome", outcome)
	return nil
}

func (h *HubServer) finishPoolAction(ctx context.Context, action poolAction) error {
	unlock, err := h.poolActionLocks.lock(ctx, action.Job)
	if err != nil {
		return err
	}
	defer unlock()
	if owner, err := h.inbox.store.poolActionOwns(ctx, action.Job); err != nil || owner != "" && owner != action.ID {
		return errors.New("another close action is pending")
	}
	h.mu.Lock()
	job := h.jobs[action.Job]
	moved := job != nil && (job.Node != action.Node || job.Epoch != action.Epoch)
	h.mu.Unlock()
	if moved && action.Data.Kind == poolActionKindHandover {
		// Reassignment is fenced while the row is active, so this is a move
		// the fence did not see. A successor already claimed (even when the
		// flag was not yet saved) is tracked to its own outcome; one never
		// claimed is not placed beside the moved job, and the row ends
		// handover_failed with the owner's notice (hk 4633).
		handover := action.Data.Handover
		if handover != nil && handover.Outcome == "" && !handover.Submitted {
			if _, err := h.inbox.store.spawnTask(ctx, handover.Successor.RequestID); err == nil {
				handover.Submitted = true
				if err := h.inbox.store.savePoolAction(ctx, action); err != nil {
					return err
				}
			} else if !errors.Is(err, sql.ErrNoRows) {
				return err
			} else {
				handover.Outcome, handover.Reason = handoverFailed, "old job moved to another owner"
			}
		}
		return h.finishHandover(ctx, action, "lost", action.Data.EndReason)
	}
	if moved {
		// Settlement permits reassignment. Once another owner has the job,
		// this old action must not release its role or announce its end.
		action.Phase = poolActionDone
		if err := h.inbox.store.savePoolAction(ctx, action); err != nil {
			return err
		}
		h.poolActionObserved.Delete(action.ID)
		h.loopNoteDone(loopPoolActions, action.ID)
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
	if action.Data.Kind == poolActionKindHandover {
		// The old job's task role passes to the successor in its claim, or
		// is released when the hand-over fails (finishHandover).
		return h.finishHandover(ctx, action, state, reason)
	}
	if err := h.inbox.store.releaseCompletedSpawnRoleOf(ctx, action.Data.SpawnRequest); err != nil {
		return err
	}
	if state == "lost" && reason == action.Data.EndReason {
		eventID := "pool-closed:" + action.Job
		if action.Data.Kind == poolActionKindSupersedeClose {
			eventID = supersedeEventID(action.Job)
		}
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
	h.loopNoteDone(loopPoolActions, action.ID)
	return nil
}

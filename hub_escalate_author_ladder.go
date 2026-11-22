package panewire

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mgh3326/panewire/internal/fleet"
)

// The hub half of the ladder close (hk 5865, step 3 of the hk 5676 split;
// docs/escalate-author.md). A node that advertises ladder_close_v1 closes the
// author's pane with pane.ladder_close under a K the staged row froze before
// anything was sent; any other node keeps pane.close plus the close marker.
//
// Every caller (the handler, the replay of a lost author's step, the expiry
// pass and a cancellation) reads the node's verdict through the one reconciler
// below (hk 5866, step 4).

// ladderVerdict is the node's answer to a ladder close. answered is false when
// no readable answer arrived.
type ladderVerdict struct {
	state, reason string
	answered      bool
}

// stagedLadderClose is the stage's frozen close binding, nil when it has none.
func stagedLadderClose(stage hubSpawnTask) *fleet.LadderCloseBinding {
	if stage.Request.LadderStep == nil {
		return nil
	}
	return stage.Request.LadderStep.Close
}

// ladderCloseCandidate is the binding a stage with none takes: the marker
// protocol for a node without the capability, else K read from the hub's grant
// and the node's current attempt. nil means K cannot be built now; nothing has
// been sent and the step retries.
func (h *HubServer) ladderCloseCandidate(ctx context.Context, holder hubSpawnTask, name string, row nodeAgentRow) *fleet.LadderCloseBinding {
	machine := spawnHolderHost(holder)
	if !h.nodeHasCapability(machine, hubCapabilityLadderClose) {
		return &fleet.LadderCloseBinding{Protocol: fleet.LadderCloseProtocolMarker}
	}
	h.mu.Lock()
	var epoch uint64
	if job := h.jobs[holder.Job]; job != nil && job.Node == machine {
		epoch = job.Epoch
	}
	h.mu.Unlock()
	statusID, idErr := fleet.NewRequestID()
	requestID, requestErr := fleet.NewRequestID()
	var status nodeCloseOpStatusResult
	if epoch == 0 || idErr != nil || requestErr != nil || !h.nodeCall(ctx, machine, nodeCommandPaneCloseStatus, nodeCloseOpRefArgs{RequestID: statusID, Job: holder.Job}, spawnHeldPaneActWait, &status) {
		return nil
	}
	bound := &fleet.LadderCloseBinding{Protocol: fleet.LadderCloseProtocolV1, RequestID: requestID, Node: machine, Epoch: epoch, Attempt: status.Attempt,
		Pane: row.PaneID, Name: name, AgentStatus: row.AgentStatus, Cwd: row.Cwd, AllowUnnamed: row.Name == ""}
	if !validNodeLadderCloseArgs(ladderCloseArgs(holder.Job, *bound)) {
		return nil
	}
	return bound
}

func ladderCloseArgs(job string, b fleet.LadderCloseBinding) nodeLadderCloseArgs {
	return nodeLadderCloseArgs{RequestID: b.RequestID, Job: job, Node: b.Node, Epoch: b.Epoch, Attempt: b.Attempt, PaneID: b.Pane,
		Name: b.Name, AgentStatus: b.AgentStatus, Cwd: b.Cwd, AllowUnnamed: b.AllowUnnamed}
}

// askLadderClose sends pane.ladder_close with the frozen K. The node answers a
// repeat from its durable record, so every caller may ask again.
func (h *HubServer) askLadderClose(ctx context.Context, job string, b fleet.LadderCloseBinding) ladderVerdict {
	var result nodeLadderCloseResult
	if !h.nodeCall(ctx, b.Node, nodeCommandLadderClose, ladderCloseArgs(job, b), spawnHeldPaneActWait, &result) || !validNodeLadderCloseResult(result) {
		return ladderVerdict{}
	}
	return ladderVerdict{state: result.State, reason: result.Reason, answered: true}
}

// closeStepLadder saves the screen, freezes the binding on the stage and only
// then sends the close. A failure before the send closed nothing: it is a
// refusal (the screen) or a retry (the binding could not be written).
func (h *HubServer) closeStepLadder(ctx context.Context, holder hubSpawnTask, name string, row nodeAgentRow, stage hubSpawnTask, bound fleet.LadderCloseBinding) (verdict ladderVerdict, retry bool) {
	if !h.captureHeldPane(ctx, bound.Node, name, row) {
		return ladderVerdict{state: nodeLadderRefused, reason: "capture_failed", answered: true}, false
	}
	stored, err := h.inbox.store.bindStagedClose(ctx, stage.Request.RequestID, bound)
	if err != nil || stored.Protocol != fleet.LadderCloseProtocolV1 {
		return ladderVerdict{}, true
	}
	return h.askLadderClose(ctx, holder.Job, stored), false
}

// unresolved reports a verdict that does not say the pane is closed or kept.
func (v ladderVerdict) unresolved() bool {
	return !v.answered || v.state == nodeLadderUnknown || v.state == nodeLadderClosing
}

// escalateAuthorUnresolved keeps the stage and tells the owner once: the
// event id is the one the expiry pass files, so both paths share one event.
func (h *HubServer) escalateAuthorUnresolved(ctx context.Context, id *fleet.LadderStepIdentity, owner string) {
	text := fmt.Sprintf("[panewire] ESC | authorless?: ladder step %s (round %d, %s): the pane close was asked for and its outcome is unresolved; the staged successor is kept. Decide whether %s still has an author.", id.Parent, id.Round, id.PR, id.PR)
	h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: fmt.Sprintf("ladder-step:%s:%d:unresolved:esc", id.Parent, id.Round), JobID: id.Parent, OwnerLane: owner, NotifyType: "decision", Text: utf8PrefixWithin(text, laneEventTextLimit), Host: "hub", Epoch: 1}, nil)
}

// fenceLadderClose asks the node to fence the stage's K: it answers the record
// that exists (closed, gone, unknown) and otherwise writes a tombstone, so a
// close for this K never runs afterwards. It is the one read of a K that sends
// no close.
func (h *HubServer) fenceLadderClose(ctx context.Context, stage hubSpawnTask, bound fleet.LadderCloseBinding) ladderVerdict {
	fence := nodeLadderFenceArgs{RequestID: bound.RequestID, Job: stage.Request.LadderStep.Parent, Node: bound.Node, Epoch: bound.Epoch, Attempt: bound.Attempt, PaneID: bound.Pane}
	var result nodeLadderCloseResult
	if !h.nodeCall(ctx, bound.Node, nodeCommandLadderFence, fence, spawnHeldPaneActWait, &result) || !validNodeLadderCloseResult(result) {
		return ladderVerdict{}
	}
	return ladderVerdict{state: result.State, reason: result.Reason, answered: true}
}

// stagedCloseVerdict reads what the step's close did to the predecessor's pane.
// A marker stage reads the close marker (closed or refused). A ladder stage
// reads the node's record through the fence, never through pane.ladder_close,
// which would run a close the node has no record of: a fenced K means nothing
// was or will be closed.
func (h *HubServer) stagedCloseVerdict(ctx context.Context, stage hubSpawnTask) (ladderVerdict, error) {
	bound := stagedLadderClose(stage)
	if bound != nil && bound.Protocol == fleet.LadderCloseProtocolV1 {
		return h.fenceLadderClose(ctx, stage, *bound), nil
	}
	marked, err := h.inbox.store.stagedCloseMarked(ctx, stage.Request.RequestID)
	if marked {
		return ladderVerdict{state: nodeLadderClosed, answered: true}, err
	}
	return ladderVerdict{state: nodeLadderRefused, answered: true}, err
}

// ladderAction is what the reconciler decided for a staged step.
type ladderAction int

const (
	// ladderKeep: the outcome is ambiguous (unknown, closing or no readable
	// answer). The stage stays and the owner has one decision event.
	ladderKeep ladderAction = iota
	// ladderRelease: nothing closed, the predecessor ended on its own, or the
	// caller cancels. The stage is released.
	ladderRelease
	// ladderActivate: the pane is closed. The caller marks a live predecessor
	// lost and activates the stage if the task still names the job.
	ladderActivate
)

// decideLadder is the table (docs/escalate-author.md):
//
//	unknown, closing, no answer  keep the stage, one decision event
//	closed                       activate, marking a live predecessor lost
//	gone                         activate a live predecessor, release a completed one
//	refused, fenced              release
//
// cancel is a reason the caller wants the stage gone (the task moved or closed,
// the stage expired, the pane is busy). It never overrides an ambiguous
// verdict: a close that may have run is never released on a guess.
func decideLadder(v ladderVerdict, predecessorLive, cancel bool) ladderAction {
	switch {
	case v.unresolved():
		return ladderKeep
	case cancel:
		return ladderRelease
	case v.state == nodeLadderClosed, v.state == nodeLadderGone && predecessorLive:
		return ladderActivate
	}
	return ladderRelease
}

// ladderStageCall is one reconciliation. verdict is the node's answer when the
// caller already holds it (the close the handler just sent); nil reads it from
// the node's record without sending a close.
type ladderStageCall struct {
	stage           hubSpawnTask
	verdict         *ladderVerdict
	predecessorLive bool
	cancel          bool
}

// reconcileLadderStage is the one place a staged step's close verdict becomes an
// action, for the handler, the replay, the expiry pass and every cancellation.
// An ambiguous verdict files the one decision event here; the caller applies a
// release or an activation.
func (h *HubServer) reconcileLadderStage(ctx context.Context, c ladderStageCall) (ladderAction, error) {
	var verdict ladderVerdict
	switch bound := stagedLadderClose(c.stage); {
	case c.verdict != nil:
		verdict = *c.verdict
	case c.cancel && (bound == nil || bound.Protocol != fleet.LadderCloseProtocolV1):
		// No close was sent to a node: there is nothing to read.
		return ladderRelease, nil
	default:
		var err error
		if verdict, err = h.stagedCloseVerdict(ctx, c.stage); err != nil {
			return ladderKeep, err
		}
	}
	action := decideLadder(verdict, c.predecessorLive, c.cancel)
	if action == ladderKeep {
		h.escalateAuthorUnresolved(ctx, c.stage.Request.LadderStep, c.stage.Owner)
	}
	return action, nil
}

// ladderStageKept is the answer of a step whose stage the reconciler kept.
var ladderStageKept = hubEscalateAuthorResult{Code: "pending", Reason: "the pane close outcome is unresolved; the stage is kept"}

// withdrawStage cancels a stage: a stage whose close was sent to a capable node
// is read through the fence first (which also fences an absent K), so a close
// delayed past the release never runs. kept reports an ambiguous K: the stage
// stays, the owner has its decision event, and the caller answers pending.
// previousID is the holder whose role is restored, or "" for a discard.
func (h *HubServer) withdrawStage(ctx context.Context, id, previousID, roleKey string) (kept bool, err error) {
	stage, err := h.inbox.store.spawnTask(ctx, id)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	if err == nil {
		action, err := h.reconcileLadderStage(ctx, ladderStageCall{stage: stage, cancel: true})
		if err != nil || action == ladderKeep {
			return err == nil, err
		}
	}
	return false, h.releaseStagedRow(ctx, id, previousID, roleKey)
}

// withdrawStageResult is withdrawStage for a handler step: stop is true when the
// step must answer now, with the kept stage's pending result or the error.
func (h *HubServer) withdrawStageResult(ctx context.Context, id, previousID, roleKey string) (result hubEscalateAuthorResult, stop bool, err error) {
	kept, err := h.withdrawStage(ctx, id, previousID, roleKey)
	if kept {
		return ladderStageKept, true, nil
	}
	return hubEscalateAuthorResult{}, err != nil, err
}

// releaseStagedRow is the store release without a fence: for a stage whose
// close was never sent, or whose node answered that nothing closed.
func (h *HubServer) releaseStagedRow(ctx context.Context, id, previousID, roleKey string) error {
	if previousID == "" {
		return h.inbox.store.discardStagedSpawn(ctx, id, roleKey)
	}
	return h.inbox.store.cancelStagedSpawn(ctx, id, previousID, roleKey)
}

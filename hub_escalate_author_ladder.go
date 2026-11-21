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
// The verdict mapping is the smallest one that keeps today's outcomes (step 4,
// hk 5866, replaces it with one reconciler):
//
//	closed, gone       today's closed path
//	refused, fenced    the node closed nothing: release the stage, today's refusal
//	unknown, no answer keep the stage, never activate, one decision event
var errLadderFenceHeld = errors.New("the ladder close is not fenced")

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

// stagedCloseOutcome says whether the step's close took the predecessor's pane,
// for a stage whose pane is gone. A ladder stage asks the node (its record is
// the proof); any other reads the close marker. unresolved keeps the stage.
func (h *HubServer) stagedCloseOutcome(ctx context.Context, stage hubSpawnTask) (marked, unresolved bool, err error) {
	bound := stagedLadderClose(stage)
	if bound == nil || bound.Protocol != fleet.LadderCloseProtocolV1 {
		marked, err = h.inbox.store.stagedCloseMarked(ctx, stage.Request.RequestID)
		return marked, false, err
	}
	verdict := h.askLadderClose(ctx, stage.Request.LadderStep.Parent, *bound)
	return verdict.state == nodeLadderClosed || verdict.state == nodeLadderGone, verdict.unresolved(), nil
}

// withdrawStage releases a stage. A stage whose close was sent to a capable
// node is fenced first, so a close delayed past the release never runs; only a
// fenced or already settled K may be released: an unknown one stays. previousID is the holder whose role is restored,
// or "" for a discard.
func (h *HubServer) withdrawStage(ctx context.Context, id, previousID, roleKey string) error {
	stage, err := h.inbox.store.spawnTask(ctx, id)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if bound := stagedLadderClose(stage); err == nil && bound != nil && bound.Protocol == fleet.LadderCloseProtocolV1 {
		fence := nodeLadderFenceArgs{RequestID: bound.RequestID, Job: stage.Request.LadderStep.Parent, Node: bound.Node, Epoch: bound.Epoch, Attempt: bound.Attempt, PaneID: bound.Pane}
		var result nodeLadderCloseResult
		if !h.nodeCall(ctx, bound.Node, nodeCommandLadderFence, fence, spawnHeldPaneActWait, &result) || result.State != nodeLadderFenced && result.State != nodeLadderClosed && result.State != nodeLadderGone {
			return errLadderFenceHeld
		}
	}
	return h.releaseStagedRow(ctx, id, previousID, roleKey)
}

// releaseStagedRow is the store release without a fence: for a stage whose
// close was never sent, or whose node answered that nothing closed.
func (h *HubServer) releaseStagedRow(ctx context.Context, id, previousID, roleKey string) error {
	if previousID == "" {
		return h.inbox.store.discardStagedSpawn(ctx, id, roleKey)
	}
	return h.inbox.store.cancelStagedSpawn(ctx, id, previousID, roleKey)
}

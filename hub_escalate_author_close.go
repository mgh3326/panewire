package panewire

import (
	"context"
	"errors"
	"fmt"
)

// The hub half of the ladder close operation (hk 5676; the node half is
// node_pane_close_ladder.go, the contract docs/escalate-author.md "The close
// operation"). On a node that advertises hubCapabilityLadderClose the step
// closes the author's pane through one durable node operation bound to the
// staged successor, and reads the node's stored result instead of inferring a
// close from the pane being gone. A node that does not keeps the marker path
// (closeHeldPaneAfter), unchanged.

// ladderClose is the hub's reading of the node's answer.
type ladderClose int

const (
	// ladderCloseRetry: no answer was read. The close may or may not have run.
	ladderCloseRetry ladderClose = iota
	// ladderClosed: this step's operation closed the pane.
	ladderClosed
	// ladderPaneGone: the pane was absent before the operation acted, so it
	// ended on its own.
	ladderPaneGone
	// ladderRefused: the node's guards kept the pane open, or the request was
	// fenced before it acted. Nothing was closed.
	ladderRefused
	// ladderAmbiguous: a close may have run and the node holds no result.
	ladderAmbiguous
)

// errLadderFencePending stops a stage release whose node operation could not
// be fenced: the stage stays, and the next pass or replay tries again.
var errLadderFencePending = errors.New("the node close operation could not be fenced; retry")

func readLadderClose(result nodeLadderCloseResult) (ladderClose, string) {
	switch result.State {
	case nodeLadderClosed:
		return ladderClosed, ""
	case nodeLadderGone:
		return ladderPaneGone, ""
	case nodeLadderRefused:
		return ladderRefused, result.Reason
	case nodeLadderFenced:
		return ladderRefused, "fenced"
	case nodeLadderClosing, nodeLadderUnknown:
		return ladderAmbiguous, ""
	}
	// none: the node holds no record, so nothing it did can be read.
	return ladderCloseRetry, ""
}

// ladderCloseArgs binds the operation to the staged request, the predecessor
// job, the node, the ownership epoch, the attempt and the pane. The epoch is the
// hub's current ownership of the job; a node that never assigned one (a job
// the hub has not indexed) starts at 1, as pane.close_op's first request does.
// The attempt is the predecessor's spawn attempt.
func (h *HubServer) ladderCloseArgs(holder hubSpawnTask, name string, row nodeAgentRow, stageID string) (nodeLadderCloseArgs, bool) {
	machine, epoch := spawnHolderHost(holder), uint64(1)
	h.mu.Lock()
	if job := h.jobs[holder.Job]; job != nil {
		if job.Node != machine {
			h.mu.Unlock()
			return nodeLadderCloseArgs{}, false
		}
		if job.Epoch > 0 {
			epoch = job.Epoch
		}
	}
	h.mu.Unlock()
	args := nodeLadderCloseArgs{
		RequestID: stageID, Job: holder.Job, Node: machine, Epoch: epoch,
		Attempt: fmt.Sprintf("spawn-%d", max(holder.Attempt, 1)),
		PaneID:  row.PaneID, Name: name, AgentStatus: row.AgentStatus, Cwd: row.Cwd, AllowUnnamed: row.Name == "",
	}
	return args, validNodeLadderCloseArgs(args)
}

// captureHeldPane saves the holder's pane screen through its node.
func (h *HubServer) captureHeldPane(ctx context.Context, machine, name string, row nodeAgentRow) bool {
	var captured nodePaneCaptureResult
	capture := nodePaneCaptureArgs{PaneID: row.PaneID, Name: name, Cwd: row.Cwd, AllowUnnamed: row.Name == "", MergedAfter: 0, Write: true}
	return h.nodeCall(ctx, machine, nodeCommandPaneCapture, capture, spawnHeldPaneActWait, &captured) && captured.Outcome == nodeCaptureScreen
}

// closeHeldPaneLadder saves the screen, marks the staged row as bound to a node
// operation, and asks the node to close the pane under it. The mark precedes
// the command, so a stage that was ever sent one is fenced before its release.
func (h *HubServer) closeHeldPaneLadder(ctx context.Context, holder hubSpawnTask, name string, row nodeAgentRow, stageID string) (ladderClose, string) {
	machine := spawnHolderHost(holder)
	args, ok := h.ladderCloseArgs(holder, name, row, stageID)
	if !ok {
		return ladderCloseRetry, "ownership_unavailable"
	}
	if !h.captureHeldPane(ctx, machine, name, row) {
		return ladderCloseRetry, "capture_failed"
	}
	if h.inbox.store.markStagedCloseOp(ctx, stageID, h.now().UTC()) != nil {
		return ladderCloseRetry, "intent_unwritten"
	}
	var result nodeLadderCloseResult
	if !h.nodeCall(ctx, machine, nodeCommandLadderClose, args, spawnHeldPaneActWait, &result) {
		return ladderCloseRetry, "close_unreadable"
	}
	return readLadderClose(result)
}

// ladderCloseFence ends the stage's operation on the node (or reads its final
// result) and reports what the node holds. ladderCloseRetry means the node did
// not answer: the operation may still act.
func (h *HubServer) ladderCloseFence(ctx context.Context, machine, stageID, job string) (ladderClose, string) {
	if !h.nodeHasCapability(machine, hubCapabilityLadderClose) {
		return ladderCloseRetry, ""
	}
	var result nodeLadderCloseResult
	if !h.nodeCall(ctx, machine, nodeCommandLadderCloseFence, nodeLadderRefArgs{RequestID: stageID, Job: job}, spawnHeldPaneActWait, &result) {
		return ladderCloseRetry, ""
	}
	return readLadderClose(result)
}

// fenceLadderStage reports that the staged request's node operation, if the
// step ever sent one, can no longer act, so its stage may be released. A stage
// with no operation needs nothing. An unreadable stage or a node that does not
// answer the fence keeps the stage.
func (h *HubServer) fenceLadderStage(ctx context.Context, stageID, job string) bool {
	kind, err := h.inbox.store.stagedCloseKind(ctx, stageID)
	if err != nil {
		return false
	}
	if kind != stagedCloseNodeOp {
		return true
	}
	stage, err := h.inbox.store.spawnTask(ctx, stageID)
	if err != nil {
		return false
	}
	verdict, _ := h.ladderCloseFence(ctx, hubSpawnArg(stage.Request.Args, "--host"), stageID, job)
	return verdict != ladderCloseRetry
}

// releaseLadderStage releases a staged successor after fencing its close
// operation: a delayed close command can never close the author's pane once the
// stage is gone. liveHolder names the predecessor to hand its reservation back
// to, or "" to discard the stage.
func (h *HubServer) releaseLadderStage(ctx context.Context, stageID, job, roleKey, liveHolder string) error {
	if !h.fenceLadderStage(ctx, stageID, job) {
		return errLadderFencePending
	}
	if liveHolder != "" {
		return h.inbox.store.cancelStagedSpawn(ctx, stageID, liveHolder, roleKey)
	}
	return h.inbox.store.discardStagedSpawn(ctx, stageID, roleKey)
}

// releaseFailed is the handler's answer to a failed stage release: a fence the
// node did not answer is a retry, anything else a hub error.
func releaseFailed(err error) (hubEscalateAuthorResult, error) {
	if errors.Is(err, errLadderFencePending) {
		return hubEscalateAuthorResult{Code: "pending", Reason: err.Error()}, nil
	}
	return hubEscalateAuthorResult{}, fmt.Errorf("cannot release staged step: %w", err)
}

// escalateAuthorAmbiguousClose keeps the stage and tells the owner once that
// the pane close has no recorded outcome. The event id is the step's, so the
// handler, a replay and the expiry pass raise one decision between them.
func (h *HubServer) escalateAuthorAmbiguousClose(ctx context.Context, parent string, round int, pr, owner string) hubEscalateAuthorResult {
	text := fmt.Sprintf("[panewire] ESC | authorless?: ladder step %s (round %d, %s): the pane close was asked for and its outcome is unresolved; the staged successor is kept. Decide whether %s still has an author.", parent, round, pr, pr)
	h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: fmt.Sprintf("ladder-step:%s:%d:unresolved:esc", parent, round), JobID: parent, OwnerLane: owner, NotifyType: "decision", Text: utf8PrefixWithin(text, laneEventTextLimit), Host: "hub", Epoch: 1}, nil)
	return hubEscalateAuthorResult{Code: "pending", Reason: "the pane close outcome is unresolved; the staged successor is kept and the owner was asked"}
}

// closeAuthorPaneByNode is the step's pane close on a node with the ladder close
// operation. It stops the step (stop true, or an error) unless the node's
// result says this step's operation closed the pane, or that the pane was
// already gone and the author is still wanted replaced (a live record). cancel
// releases the stage with a refusal.
//
//   - closed: go on to the lost mark and activation.
//   - pane gone before the operation acted: a completed author ended on its
//     own (the stage is released); a live record's author is still replaced.
//   - refused: the guards kept the pane open, so the stage is cancelled.
//   - ambiguous: a close may have run and the node holds no result. The stage
//     stays and the owner decides, once.
//   - no answer: the pane decides as it does on the marker path, after the
//     request is fenced so it cannot run later.
func (h *HubServer) closeAuthorPaneByNode(ctx context.Context, request hubEscalateAuthorRequest, owner string, holder hubSpawnTask, name string, row nodeAgentRow, stageID, roleKey string, finished bool, cancel func(code, why string) (hubEscalateAuthorResult, error)) (hubEscalateAuthorResult, bool, error) {
	machine := spawnHolderHost(holder)
	verdict, why := h.closeHeldPaneLadder(ctx, holder, name, row, stageID)
	if verdict == ladderCloseRetry && h.probeHeldPane(ctx, machine, holder.Outcome.Pane, name).state == heldPanePresent {
		// No result was read and the pane is still there. End the request so it
		// cannot close the pane after the stage is released, and read what it
		// stored: a close that ran after all is not cancelled.
		if verdict, why = h.ladderCloseFence(ctx, machine, stageID, request.Job); verdict == ladderCloseRetry {
			return hubEscalateAuthorResult{Code: "pending", Reason: "the node's close request could not be fenced; retry"}, true, nil
		}
		if verdict == ladderPaneGone {
			verdict = ladderRefused
		}
	}
	switch verdict {
	case ladderClosed:
		return hubEscalateAuthorResult{}, false, nil
	case ladderPaneGone:
		if !finished {
			return hubEscalateAuthorResult{}, false, nil
		}
		if err := h.releaseLadderStage(ctx, stageID, request.Job, roleKey, ""); err != nil {
			result, err := releaseFailed(err)
			return result, true, err
		}
		return h.escalateAuthorParentEnded(ctx, request, owner), true, nil
	case ladderRefused:
		result, err := cancel("pane_not_closed", "the author pane could not be closed: "+firstNonempty(why, "unknown"))
		return result, true, err
	case ladderAmbiguous:
		return h.escalateAuthorAmbiguousClose(ctx, request.Job, request.Round, request.PR, owner), true, nil
	}
	return hubEscalateAuthorResult{Code: "pending", Reason: "pane close outcome is unknown; retry"}, true, nil
}

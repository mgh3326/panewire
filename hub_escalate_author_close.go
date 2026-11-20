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

var (
	// errLadderFencePending stops a stage release whose node operation could not
	// be fenced: the stage stays, and the next pass or replay tries again.
	errLadderFencePending = errors.New("the node close operation could not be fenced; retry")
	// errLadderCloseRan stops a release made on the ground that the author
	// remains: the fence found the node's close had run, so the author is gone
	// and the step goes on to activation on its next pass.
	errLadderCloseRan = errors.New("the node close operation closed the pane; retry")
	// errLadderCloseAmbiguous stops the same release when the fence found a close
	// that may have run with no result recorded: the stage stays and the owner
	// decides.
	errLadderCloseAmbiguous = errors.New("the node close operation is unresolved")
)

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
// The attempt is the one the node's status named: its own ledger's current
// attempt for the job, or the one the request was first bound to.
func (h *HubServer) ladderCloseArgs(holder hubSpawnTask, name string, row nodeAgentRow, stageID, attempt string) (nodeLadderCloseArgs, bool) {
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
		Attempt: firstNonempty(attempt, nodeLadderNoAttempt),
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
	var status nodeLadderCloseResult
	if !h.nodeCall(ctx, machine, nodeCommandLadderCloseStatus, nodeLadderRefArgs{RequestID: stageID, Job: holder.Job}, spawnHeldPaneActWait, &status) {
		return ladderCloseRetry, "status_unreadable"
	}
	args, ok := h.ladderCloseArgs(holder, name, row, stageID, status.Attempt)
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

// ladderCloseStatus reads the stage's operation without ending it.
func (h *HubServer) ladderCloseStatus(ctx context.Context, machine, stageID, job string) (ladderClose, string) {
	if !h.nodeHasCapability(machine, hubCapabilityLadderClose) {
		return ladderCloseRetry, ""
	}
	var result nodeLadderCloseResult
	if !h.nodeCall(ctx, machine, nodeCommandLadderCloseStatus, nodeLadderRefArgs{RequestID: stageID, Job: job}, spawnHeldPaneActWait, &result) {
		return ladderCloseRetry, ""
	}
	return readLadderClose(result)
}

// fenceLadderStage ends the staged request's node operation, if the step ever
// sent one, and reports what the node held. ok is false when it could not be
// ended (an unreadable stage, or a node that does not answer): the stage must
// then be kept. A stage with no operation answers ladderCloseRetry, ok true.
func (h *HubServer) fenceLadderStage(ctx context.Context, stageID, job string) (verdict ladderClose, ok bool) {
	kind, err := h.inbox.store.stagedCloseKind(ctx, stageID)
	if err != nil {
		return ladderCloseRetry, false
	}
	if kind != stagedCloseNodeOp {
		return ladderCloseRetry, true
	}
	stage, err := h.inbox.store.spawnTask(ctx, stageID)
	if err != nil {
		return ladderCloseRetry, false
	}
	verdict, _ = h.ladderCloseFence(ctx, hubSpawnArg(stage.Request.Args, "--host"), stageID, job)
	return verdict, verdict != ladderCloseRetry
}

// releaseLadderStage releases a staged successor after fencing its close
// operation: a delayed close command can never close the author's pane once the
// stage is gone. liveHolder names the predecessor to hand its reservation back
// to, or "" to discard the stage.
//
// authorRemains is true for a release made because the author is still there
// (a refusal, an expiry). The fence's verdict then decides: a close that ran
// means the author is gone and the stage is still needed (errLadderCloseRan),
// and one that may have run keeps the stage for the owner
// (errLadderCloseAmbiguous). A release for another reason (the task moved on,
// the predecessor ended) needs only the fence.
func (h *HubServer) releaseLadderStage(ctx context.Context, stageID, job, roleKey, liveHolder string, authorRemains bool) error {
	verdict, ok := h.fenceLadderStage(ctx, stageID, job)
	if !ok {
		return errLadderFencePending
	}
	if authorRemains {
		switch verdict {
		case ladderClosed:
			return errLadderCloseRan
		case ladderAmbiguous:
			return errLadderCloseAmbiguous
		}
	}
	if liveHolder != "" {
		return h.inbox.store.cancelStagedSpawn(ctx, stageID, liveHolder, roleKey)
	}
	return h.inbox.store.discardStagedSpawn(ctx, stageID, roleKey)
}

// releaseFailed is the handler's answer to a failed stage release: a fence the
// node did not answer, or a close that ran, is a retry; one that may have run
// asks the owner once; anything else is a hub error.
func (h *HubServer) releaseFailed(ctx context.Context, err error, parent string, round int, pr, owner string) (hubEscalateAuthorResult, error) {
	switch {
	case errors.Is(err, errLadderFencePending), errors.Is(err, errLadderCloseRan):
		return hubEscalateAuthorResult{Code: "pending", Reason: err.Error()}, nil
	case errors.Is(err, errLadderCloseAmbiguous):
		return h.escalateAuthorAmbiguousClose(ctx, parent, round, pr, owner), nil
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
		if err := h.releaseLadderStage(ctx, stageID, request.Job, roleKey, "", false); err != nil {
			result, err := h.releaseFailed(ctx, err, request.Job, request.Round, request.PR, owner)
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

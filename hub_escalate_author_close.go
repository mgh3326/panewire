package panewire

import (
	"context"
	"errors"
	"fmt"

	"github.com/mgh3326/panewire/internal/fleet"
)

// The hub half of the ladder close operation (hk 5676; the node half is
// node_pane_close_ladder.go, the contract docs/escalate-author.md "The close
// operation"). On a node that advertises hubCapabilityLadderClose the step
// closes the author's pane through one durable node operation bound to the
// staged successor, and reads the node's stored result instead of inferring a
// close from the pane being gone. A node that does not keeps the marker path
// (closeHeldPaneAfter), unchanged.
//
// Every path that can settle an operation-bound stage - the handler's close,
// its replay, the expiry pass, a cancellation - reads the node's verdict and
// asks reconcileLadderStage what to do. Ambiguity wins over every cancellation
// reason.

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
	// ladderMarkerPath: the node holds no provenance for the job, so the stage
	// keeps the marker path. Only an unmarked stage can take it.
	ladderMarkerPath
)

var (
	// errLadderFencePending stops a stage release whose node operation could not
	// be fenced: the stage stays, and the next pass or replay tries again.
	errLadderFencePending = errors.New("the node close operation could not be fenced; retry")
	// errLadderCloseRan stops a release made on the ground that the author
	// remains: the fence found the node's close had run, so the author is gone
	// and the step goes on to activation on its next pass.
	errLadderCloseRan = errors.New("the node close operation closed the pane; retry")
	// errLadderCloseAmbiguous stops any release when the fence found a close that
	// may have run with no result recorded: the stage stays and the owner
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
	return ladderCloseRetry, ""
}

// ladderCloseArgs binds the operation to the staged request, the predecessor
// job, the node, the ownership epoch, the attempt and the pane. The epoch is the
// hub's current ownership of the job; a node that never assigned one (a job
// the hub has not indexed) starts at 1, as pane.close_op's first request does.
// The attempt is the token pane.capture reported. A binding that differs from
// the one the stage's request first named is the node's to answer as unknown.
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
		RequestID: stageID, Job: holder.Job, Node: machine, Epoch: epoch, Attempt: attempt,
		PaneID: row.PaneID, Name: name, AgentStatus: row.AgentStatus, Cwd: row.Cwd, AllowUnnamed: row.Name == "",
	}
	return args, validNodeLadderCloseArgs(args)
}

// captureHeldPane saves the holder's pane screen through its node and returns
// the attempt token the node reported (empty when it holds no provenance).
func (h *HubServer) captureHeldPane(ctx context.Context, machine, name string, row nodeAgentRow) (attempt string, ok bool) {
	var captured nodePaneCaptureResult
	capture := nodePaneCaptureArgs{PaneID: row.PaneID, Name: name, Cwd: row.Cwd, AllowUnnamed: row.Name == "", MergedAfter: 0, Write: true}
	if !h.nodeCall(ctx, machine, nodeCommandPaneCapture, capture, spawnHeldPaneActWait, &captured) || captured.Outcome != nodeCaptureScreen {
		return "", false
	}
	return captured.Attempt, true
}

// closeHeldPaneLadder saves the screen, marks the staged row as bound to a node
// operation, and asks the node to close the pane under it. The mark precedes
// the command, so a stage that was ever sent one is fenced before its release.
// A node with no provenance for an unmarked stage answers ladderMarkerPath
// before the mark is written.
func (h *HubServer) closeHeldPaneLadder(ctx context.Context, holder hubSpawnTask, name string, row nodeAgentRow, stageID string, kind stagedClose) (ladderClose, string) {
	machine := spawnHolderHost(holder)
	attempt, ok := h.captureHeldPane(ctx, machine, name, row)
	if !ok {
		return ladderCloseRetry, "capture_failed"
	}
	if attempt == "" {
		if kind == stagedCloseNone {
			return ladderMarkerPath, ""
		}
		return ladderCloseRetry, "no_provenance"
	}
	args, ok := h.ladderCloseArgs(holder, name, row, stageID, attempt)
	if !ok {
		return ladderCloseRetry, "ownership_unavailable"
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

// ladderCloseFence ends the stage's operation on the node (or reads its stored
// result) and reports what the node holds. ladderCloseRetry means the node did
// not answer: the operation may still act.
func (h *HubServer) ladderCloseFence(ctx context.Context, machine, stageID, job string) (ladderClose, string) {
	if !h.nodeHasCapability(machine, hubCapabilityLadderClose) {
		return ladderCloseRetry, ""
	}
	var result nodeLadderCloseResult
	if !h.nodeCall(ctx, machine, nodeCommandLadderCloseFence, nodeLadderFenceArgs{RequestID: stageID, Job: job}, spawnHeldPaneActWait, &result) {
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
// Ambiguity wins over every reason to release: a close that may have run keeps
// the stage for the owner (errLadderCloseAmbiguous). authorRemains is true for
// a release made because the author is still there; a close that ran then means
// the author is gone and the stage is still needed (errLadderCloseRan).
func (h *HubServer) releaseLadderStage(ctx context.Context, stageID, job, roleKey, liveHolder string, authorRemains bool) error {
	verdict, ok := h.fenceLadderStage(ctx, stageID, job)
	if !ok {
		return errLadderFencePending
	}
	switch {
	case verdict == ladderAmbiguous:
		return errLadderCloseAmbiguous
	case verdict == ladderClosed && authorRemains:
		return errLadderCloseRan
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
// the pane close has no recorded outcome. The event id is the step's, so every
// path raises one decision between them.
func (h *HubServer) escalateAuthorAmbiguousClose(ctx context.Context, parent string, round int, pr, owner string) hubEscalateAuthorResult {
	text := fmt.Sprintf("[panewire] ESC | authorless?: ladder step %s (round %d, %s): the pane close was asked for and its outcome is unresolved; the staged successor is kept. Decide whether %s still has an author.", parent, round, pr, pr)
	h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: fmt.Sprintf("ladder-step:%s:%d:unresolved:esc", parent, round), JobID: parent, OwnerLane: owner, NotifyType: "decision", Text: utf8PrefixWithin(text, laneEventTextLimit), Host: "hub", Epoch: 1}, nil)
	return hubEscalateAuthorResult{Code: "pending", Reason: "the pane close outcome is unresolved; the staged successor is kept and the owner was asked"}
}

// ladderPane is what the caller saw of the predecessor's pane.
type ladderPane int

const (
	ladderPaneAbsent ladderPane = iota
	ladderPanePresent
	ladderPaneUnreadable
)

// ladderStage is what the reconciler needs of one operation-bound stage.
type ladderStage struct {
	request hubEscalateAuthorRequest // Job, Round, PR
	owner   string
	stageID string
	roleKey string
	// finished: the predecessor's hk record completed, so its stage is
	// discarded, never handed back, and no lost mark is written.
	finished bool
}

type ladderAct int

const (
	// ladderProceed: the author is gone. A live record is marked lost, the task
	// binding is read again, and the stage is activated.
	ladderProceed ladderAct = iota
	// ladderStop: the result is the answer. The stage is kept (pending, or one
	// owner decision) or was released here.
	ladderStop
	// ladderCancel: the pane is still there and the node closed nothing. The
	// caller releases the stage with its own refusal.
	ladderCancel
)

// reconcileLadderStage is the one reader of a node verdict for an operation-
// bound stage. The handler's close, its replay, the expiry pass and every
// cancellation call it with the verdict they read and what they saw of the pane.
//
//	retry (no answer)         keep the stage, retry
//	ambiguous                 keep the stage, one owner decision
//	closed                    proceed
//	pane gone                 completed predecessor: release (parent_ended);
//	                          live record: proceed (its author is replaced)
//	refused / fenced          pane present: cancel; pane absent: as pane gone;
//	                          pane unreadable: keep
func (h *HubServer) reconcileLadderStage(ctx context.Context, st ladderStage, verdict ladderClose, pane ladderPane) (ladderAct, hubEscalateAuthorResult) {
	request := st.request
	switch verdict {
	case ladderAmbiguous:
		return ladderStop, h.escalateAuthorAmbiguousClose(ctx, request.Job, request.Round, request.PR, st.owner)
	case ladderClosed:
		return ladderProceed, hubEscalateAuthorResult{}
	case ladderRefused:
		switch pane {
		case ladderPanePresent:
			return ladderCancel, hubEscalateAuthorResult{}
		case ladderPaneUnreadable:
			return ladderStop, hubEscalateAuthorResult{Code: "pending", Reason: "the pane cannot be read; retry"}
		}
	case ladderPaneGone:
	default:
		return ladderStop, hubEscalateAuthorResult{Code: "pending", Reason: "the node's close result is unreadable; retry"}
	}
	// The pane ended on its own.
	if !st.finished {
		return ladderProceed, hubEscalateAuthorResult{}
	}
	if err := h.releaseLadderStage(ctx, st.stageID, request.Job, st.roleKey, "", false); err != nil {
		result, err := h.releaseFailed(ctx, err, request.Job, request.Round, request.PR, st.owner)
		if err != nil {
			return ladderStop, hubEscalateAuthorResult{Code: "pending", Reason: err.Error()}
		}
		return ladderStop, result
	}
	return ladderStop, h.escalateAuthorParentEnded(ctx, request, st.owner)
}

// expireLadderStage settles an expired, operation-bound stage of a completed or
// live predecessor through the reconciler. It reports true when the stage is
// settled (released or activated); a stage kept for the owner or for a retry
// is not.
func (h *HubServer) expireLadderStage(ctx context.Context, app *fleet.App, task, holder hubSpawnTask, record map[string]any, taskID, roleKey string, finished bool) bool {
	id := task.Request.LadderStep
	request := hubEscalateAuthorRequest{Job: id.Parent, Round: id.Round, PR: id.PR}
	st := ladderStage{request: request, owner: task.Owner, stageID: task.Request.RequestID, roleKey: roleKey, finished: finished}
	machine := spawnHolderHost(holder)
	pane := ladderPaneUnreadable
	switch h.probeHeldPane(ctx, machine, holder.Outcome.Pane, heldAgentName(holder, record)).state {
	case heldPanePresent:
		pane = ladderPanePresent
	case heldPaneAbsent:
		pane = ladderPaneAbsent
	}
	verdict, _ := h.ladderCloseFence(ctx, machine, st.stageID, id.Parent)
	act, result := h.reconcileLadderStage(ctx, st, verdict, pane)
	switch act {
	case ladderCancel:
		liveHolder := holder.Request.RequestID
		if finished {
			liveHolder = ""
		}
		if err := h.releaseLadderStage(ctx, st.stageID, id.Parent, roleKey, liveHolder, true); err != nil {
			if errors.Is(err, errLadderCloseAmbiguous) {
				h.escalateAuthorAmbiguousClose(ctx, id.Parent, id.Round, id.PR, task.Owner)
			}
			return false
		}
		_, _ = h.escalateAuthorRefusal(ctx, request, task.Owner, "staged_expired", "the staged step expired while the author remained present", false)
		return true
	case ladderStop:
		// parent_ended is the one stop that settles the stage.
		return result.Code == "parent_ended"
	}
	if !finished {
		endReason := fleet.LadderStepEndReason(id.Round, task.Job)
		lost, why := h.markHolderJobLostBecause(ctx, app, holder, heldAgentName(holder, record), endReason)
		if !lost {
			return false
		}
		if why == nodeJobMarkLostCompleted || why == "hk_lost" {
			// The record may have completed while the close ran: hk keeps it
			// terminal, and the node's stored close still stands.
			current, exists, err := app.ExistingJob(ctx, id.Parent)
			if err != nil || !exists {
				return false
			}
			if state, reason := escalateAuthorStr(current, "state"), escalateAuthorStr(current, "end_reason"); state != "completed" && !(state == "lost" && reason == endReason) {
				return false
			}
		}
	}
	return h.activateExpiredLadderStage(ctx, app, task, id.Parent, taskID, roleKey)
}

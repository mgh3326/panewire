package panewire

import (
	"context"
	"errors"
	"os"

	"github.com/mgh3326/panewire/internal/fleet"
)

// Close operations (hk 4594; docs/node-commands.md, "Close operations") are
// pane.close with a hub-chosen request id and a durable journal in the job's
// directory (internal/fleet/close_journal.go). The hub issues them itself, so
// the public node-command endpoint refuses all three ops.
//
// pane.close_op closes like pane.close and answers one final outcome per
// request id; a repeat of the same id answers the recorded outcome without
// touching herdr (a lost acknowledgement), or finishes an attempt a node
// restart interrupted. pane.close_status reads an operation; pane.close_release
// marks a final one done, which hands the job's ending back to its watcher.
const (
	nodeCommandPaneCloseOp      = "pane.close_op"
	nodeCommandPaneCloseStatus  = "pane.close_status"
	nodeCommandPaneCloseRelease = "pane.close_release"

	// nodeCloseOpNone: no operation under that id (or no such job directory).
	nodeCloseOpNone = "none"
)

type nodePaneCloseOpArgs struct {
	RequestID    string `json:"request_id"`
	PaneID       string `json:"pane_id"`
	Name         string `json:"name"`
	AgentStatus  string `json:"agent_status"`
	Cwd          string `json:"cwd"`
	AllowUnnamed bool   `json:"allow_unnamed"`
}

// nodePaneCloseOpResult.Outcome is a final fleet.CloseOp state, or
// nodeCaptureNotAJob when the node has no job directory for the name (no
// journal can be kept, so nothing was closed).
type nodePaneCloseOpResult struct {
	Outcome  string `json:"outcome"`
	Replayed bool   `json:"replayed"`
}

type nodeCloseOpRefArgs struct {
	RequestID string `json:"request_id"`
	Name      string `json:"name"`
}

type nodeCloseOpStatusResult struct {
	State    string `json:"state"`
	Released bool   `json:"released"`
}

func validNodePaneCloseOpArgs(args nodePaneCloseOpArgs) bool {
	return fleet.ValidCloseOpID(args.RequestID) && nodeCommandPaneIDPattern.MatchString(args.PaneID) && validNodeJobName(args.Name) && nodePaneClosableStatus[args.AgentStatus] && validNodeCommandPath(args.Cwd)
}

func validNodeCloseOpRefArgs(args nodeCloseOpRefArgs) bool {
	return fleet.ValidCloseOpID(args.RequestID) && validNodeJobName(args.Name)
}

func validNodePaneCloseOpResult(result nodePaneCloseOpResult) bool {
	switch result.Outcome {
	case fleet.CloseOpClosed, fleet.CloseOpRefused, fleet.CloseOpGone, fleet.CloseOpUnknown:
		return true
	case nodeCaptureNotAJob:
		return !result.Replayed
	}
	return false
}

func validNodeCloseOpStatusResult(result nodeCloseOpStatusResult) bool {
	switch result.State {
	case nodeCloseOpNone:
		return !result.Released
	case fleet.CloseOpRegistered, fleet.CloseOpClosing:
		// Only a final operation can be released.
		return !result.Released
	case fleet.CloseOpClosed, fleet.CloseOpRefused, fleet.CloseOpGone, fleet.CloseOpUnknown:
		return true
	}
	return false
}

// openCloseOpJob opens the job directory a close operation is journalled in.
func (client *HubClient) openCloseOpJob(name string) (*os.Root, bool, string) {
	job, _, found, err := client.openNodeJobDir(name)
	switch {
	case errors.Is(err, errNodeJobsRootUnset):
		return nil, false, "jobs_root_unset"
	case err != nil:
		return nil, false, "journal_failed"
	}
	return job, found, ""
}

// nodePaneCloseOp runs one close operation. The caller holds nodeCommandMu.
//
// The journal moves registered → closing → final. registered is written before
// any herdr read, closing immediately before `herdr pane close`, so a pane found
// absent tells the cases apart: absent before closing was ever recorded is
// gone (it vanished on its own), absent after closing with no recorded result
// is unknown (a node restart or a failed close between the two). A pane that is
// still present after an interrupted attempt is closed again under the same
// guards pane.close applies.
func (client *HubClient) nodePaneCloseOp(ctx context.Context, args nodePaneCloseOpArgs) (any, string) {
	job, found, code := client.openCloseOpJob(args.Name)
	if code != "" {
		return nil, code
	}
	if !found {
		return nodePaneCloseOpResult{Outcome: nodeCaptureNotAJob}, ""
	}
	defer job.Close()
	op, found, err := fleet.ReadCloseOp(job, args.RequestID)
	if err != nil {
		return nil, "journal_failed"
	}
	if found {
		if op.PaneID != args.PaneID {
			return nil, "invalid_args"
		}
		if op.Final() {
			return nodePaneCloseOpResult{Outcome: op.State, Replayed: true}, ""
		}
	} else {
		now := client.nowUTC()
		op = fleet.CloseOp{RequestID: args.RequestID, PaneID: args.PaneID, State: fleet.CloseOpRegistered, Created: now, Updated: now}
		if fleet.WriteCloseOp(job, op) != nil {
			return nil, "journal_failed"
		}
	}
	closeStarted := op.State == fleet.CloseOpClosing
	if closeStarted {
		outcome, _, err := client.nodePaneMatchWithUnnamed(ctx, args.PaneID, args.Name, "", args.Cwd, false, args.AllowUnnamed)
		if err != nil {
			return nil, "herdr_failed"
		}
		if outcome == nodePaneGone {
			return client.finishCloseOp(job, op, fleet.CloseOpUnknown)
		}
	}
	closeArgs := nodePaneCloseArgs{PaneID: args.PaneID, Name: args.Name, AgentStatus: args.AgentStatus, Cwd: args.Cwd, AllowUnnamed: args.AllowUnnamed}
	result, code := client.nodePaneCloseWith(ctx, closeArgs, func() error {
		if op.State == fleet.CloseOpClosing {
			return nil
		}
		op.State, op.Updated = fleet.CloseOpClosing, client.nowUTC()
		return fleet.WriteCloseOp(job, op)
	})
	if code != "" {
		// The journal keeps registered or closing; a repeat of this request
		// id resolves it.
		return nil, code
	}
	switch result.Outcome {
	case nodePaneClosed:
		return client.finishCloseOp(job, op, fleet.CloseOpClosed)
	case nodePaneGone:
		if closeStarted {
			return client.finishCloseOp(job, op, fleet.CloseOpUnknown)
		}
		return client.finishCloseOp(job, op, fleet.CloseOpGone)
	default:
		return client.finishCloseOp(job, op, fleet.CloseOpRefused)
	}
}

func (client *HubClient) finishCloseOp(job *os.Root, op fleet.CloseOp, state string) (any, string) {
	op.State, op.Updated = state, client.nowUTC()
	if fleet.WriteCloseOp(job, op) != nil {
		return nil, "journal_failed"
	}
	return nodePaneCloseOpResult{Outcome: state}, ""
}

// nodePaneCloseStatus reads one close operation.
func (client *HubClient) nodePaneCloseStatus(args nodeCloseOpRefArgs) (any, string) {
	return client.nodeCloseOpRef(args, false)
}

// nodePaneCloseRelease marks a final close operation released. An operation
// still in progress is answered unreleased: the hub repeats pane.close_op to
// resolve it first. The caller holds nodeCommandMu.
func (client *HubClient) nodePaneCloseRelease(args nodeCloseOpRefArgs) (any, string) {
	return client.nodeCloseOpRef(args, true)
}

func (client *HubClient) nodeCloseOpRef(args nodeCloseOpRefArgs, release bool) (any, string) {
	job, found, code := client.openCloseOpJob(args.Name)
	if code != "" {
		return nil, code
	}
	if !found {
		return nodeCloseOpStatusResult{State: nodeCloseOpNone}, ""
	}
	defer job.Close()
	op, found, err := fleet.ReadCloseOp(job, args.RequestID)
	if err != nil {
		return nil, "journal_failed"
	}
	if !found {
		return nodeCloseOpStatusResult{State: nodeCloseOpNone}, ""
	}
	if release && op.Final() && op.Released == nil {
		now := client.nowUTC()
		op.Released, op.Updated = &now, now
		if fleet.WriteCloseOp(job, op) != nil {
			return nil, "journal_failed"
		}
	}
	return nodeCloseOpStatusResult{State: op.State, Released: op.Released != nil}, ""
}

// nodeCloseOpReleaseArgs is pane.close_release's decoded arguments, a distinct
// type so the dispatcher can tell release from status.
type nodeCloseOpReleaseArgs nodeCloseOpRefArgs

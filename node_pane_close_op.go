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
// touching herdr (a lost acknowledgement), or settles an attempt a node
// restart interrupted. pane.close_status reads an operation; pane.close_release
// marks a final one done, which hands the job's ending back to its watcher.
//
// job names the job directory the journal lives in (and the watcher reads);
// name is the herdr agent label the close guards match, which a spawn may set
// apart from the job id.
const (
	nodeCommandPaneCloseOp      = "pane.close_op"
	nodeCommandPaneCloseStatus  = "pane.close_status"
	nodeCommandPaneCloseRelease = "pane.close_release"

	// nodeCloseOpNone: no operation under that id (or no such job directory).
	nodeCloseOpNone = "none"
	// nodeCloseOpUnsupported: the job's watcher predates the journal, so it
	// would not wait for the hub; no operation was opened and nothing closed.
	nodeCloseOpUnsupported = "unsupported"

	// hubCapabilityCloseOps is the hello capability of a node that serves
	// the three close-operation ops; the hub sends them to no other node.
	hubCapabilityCloseOps = "close_ops_v1"
)

type nodePaneCloseOpArgs struct {
	RequestID    string `json:"request_id"`
	Job          string `json:"job"`
	PaneID       string `json:"pane_id"`
	Name         string `json:"name"`
	AgentStatus  string `json:"agent_status"`
	Cwd          string `json:"cwd"`
	AllowUnnamed bool   `json:"allow_unnamed"`
}

// nodePaneCloseOpResult.Outcome is a final fleet.CloseOp state, or
// nodeCaptureNotAJob (no job directory) or nodeCloseOpUnsupported; for those
// two no journal entry exists and nothing was closed.
type nodePaneCloseOpResult struct {
	Outcome  string `json:"outcome"`
	Replayed bool   `json:"replayed"`
}

type nodeCloseOpRefArgs struct {
	RequestID string `json:"request_id"`
	Job       string `json:"job"`
}

// nodeCloseOpReleaseArgs is pane.close_release's decoded arguments, a distinct
// type so the dispatcher can tell release from status.
type nodeCloseOpReleaseArgs nodeCloseOpRefArgs

type nodeCloseOpStatusResult struct {
	State    string `json:"state"`
	Released bool   `json:"released"`
}

func validNodePaneCloseOpArgs(args nodePaneCloseOpArgs) bool {
	return fleet.ValidCloseOpID(args.RequestID) && validNodeJobName(args.Job) && nodeCommandPaneIDPattern.MatchString(args.PaneID) && validNodeJobName(args.Name) && nodePaneClosableStatus[args.AgentStatus] && validNodeCommandPath(args.Cwd)
}

func validNodeCloseOpRefArgs(args nodeCloseOpRefArgs) bool {
	return fleet.ValidCloseOpID(args.RequestID) && validNodeJobName(args.Job)
}

func validNodePaneCloseOpResult(result nodePaneCloseOpResult) bool {
	switch result.Outcome {
	case fleet.CloseOpClosed, fleet.CloseOpRefused, fleet.CloseOpGone, fleet.CloseOpUnknown, fleet.CloseOpCancelled, fleet.CloseOpPreempted:
		return true
	case nodeCaptureNotAJob, nodeCloseOpUnsupported:
		return !result.Replayed
	}
	return false
}

func validNodeCloseOpStatusResult(result nodeCloseOpStatusResult) bool {
	switch result.State {
	case nodeCloseOpNone, fleet.CloseOpRegistered, fleet.CloseOpClosing:
		// Only a final operation can be released.
		return !result.Released
	case fleet.CloseOpCancelled:
		return result.Released
	case fleet.CloseOpClosed, fleet.CloseOpRefused, fleet.CloseOpGone, fleet.CloseOpUnknown, fleet.CloseOpPreempted:
		return true
	}
	return false
}

// openCloseOpJob opens the job directory a close operation is journalled in.
func (client *HubClient) openCloseOpJob(job string) (*os.Root, bool, string) {
	root, _, found, err := client.openNodeJobDir(job)
	switch {
	case errors.Is(err, errNodeJobsRootUnset):
		return nil, false, "jobs_root_unset"
	case err != nil:
		return nil, false, "journal_failed"
	}
	return root, found, ""
}

// nodePaneCloseOp runs one close operation. The caller holds nodeCommandMu.
//
// The journal moves registered → closing → final. registered is written under
// the journal lock before any herdr read; there the node also finds whether the
// job's watcher already committed its own ending of the attempt (preempted).
// closing is written after the first guarded look and the settle, before the
// final look that precedes `herdr pane close`, so a slow journal write never
// sits between that look and the close. A pane absent before closing was
// recorded is gone; an operation found closing on replay is unknown, without
// touching herdr: its close may have run, and a pane under the same id now may
// be another one.
func (client *HubClient) nodePaneCloseOp(ctx context.Context, args nodePaneCloseOpArgs) (any, string) {
	job, found, code := client.openCloseOpJob(args.Job)
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
	switch {
	case found && op.PaneID != args.PaneID:
		return nil, "invalid_args"
	case found && op.Final():
		return nodePaneCloseOpResult{Outcome: op.State, Replayed: true}, ""
	case found && op.State == fleet.CloseOpClosing:
		return client.finishCloseOp(job, op, fleet.CloseOpUnknown)
	case !found:
		if !fleet.JournalAwareWatcher(job) {
			return nodePaneCloseOpResult{Outcome: nodeCloseOpUnsupported}, ""
		}
		op, code = client.registerCloseOp(job, args)
		if code != "" {
			return nil, code
		}
		if op.State == fleet.CloseOpPreempted {
			return nodePaneCloseOpResult{Outcome: op.State}, ""
		}
	}
	closeArgs := nodePaneCloseArgs{PaneID: args.PaneID, Name: args.Name, AgentStatus: args.AgentStatus, Cwd: args.Cwd, AllowUnnamed: args.AllowUnnamed}
	result, code := client.nodePaneCloseWith(ctx, closeArgs, func() error {
		op.State, op.Updated = fleet.CloseOpClosing, client.nowUTC()
		return fleet.WriteCloseOp(job, op)
	})
	if code != "" {
		// The journal keeps registered or closing; a repeat of this request
		// id settles it.
		return nil, code
	}
	switch result.Outcome {
	case nodePaneClosed:
		return client.finishCloseOp(job, op, fleet.CloseOpClosed)
	case nodePaneGone:
		return client.finishCloseOp(job, op, fleet.CloseOpGone)
	default:
		return client.finishCloseOp(job, op, fleet.CloseOpRefused)
	}
}

// registerCloseOp records a new operation under the journal lock, against the
// job's current attempt; an attempt its watcher is already ending is recorded
// preempted instead.
func (client *HubClient) registerCloseOp(job *os.Root, args nodePaneCloseOpArgs) (fleet.CloseOp, string) {
	unlock, err := fleet.LockCloseOps(job)
	if err != nil {
		return fleet.CloseOp{}, "journal_failed"
	}
	defer unlock()
	attempt, err := fleet.JobDirAttemptKey(job)
	if err != nil {
		return fleet.CloseOp{}, "journal_failed"
	}
	ended, err := fleet.WatcherEnded(job, args.PaneID, attempt)
	if err != nil {
		return fleet.CloseOp{}, "journal_failed"
	}
	now := client.nowUTC()
	op := fleet.CloseOp{RequestID: args.RequestID, PaneID: args.PaneID, Attempt: attempt, State: fleet.CloseOpRegistered, Created: now, Updated: now}
	if ended {
		op.State = fleet.CloseOpPreempted
	}
	if fleet.WriteCloseOp(job, op) != nil {
		return fleet.CloseOp{}, "journal_failed"
	}
	return op, ""
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

// nodePaneCloseRelease marks a final close operation released, and cancels
// one still registered (no close was started, so the pane was never touched).
// One left closing is answered unreleased: only a repeated pane.close_op can
// settle it. The caller holds nodeCommandMu.
func (client *HubClient) nodePaneCloseRelease(args nodeCloseOpRefArgs) (any, string) {
	return client.nodeCloseOpRef(args, true)
}

func (client *HubClient) nodeCloseOpRef(args nodeCloseOpRefArgs, release bool) (any, string) {
	job, found, code := client.openCloseOpJob(args.Job)
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
	if release && (op.Final() || op.State == fleet.CloseOpRegistered) && op.Released == nil {
		now := client.nowUTC()
		if op.State == fleet.CloseOpRegistered {
			op.State = fleet.CloseOpCancelled
		}
		op.Released, op.Updated = &now, now
		if fleet.WriteCloseOp(job, op) != nil {
			return nil, "journal_failed"
		}
	}
	return nodeCloseOpStatusResult{State: op.State, Released: op.Released != nil}, ""
}

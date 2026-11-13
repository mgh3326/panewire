package panewire

import (
	"encoding/json"
	"errors"
	"os"

	"github.com/mgh3326/panewire/internal/durable"
	"github.com/mgh3326/panewire/internal/fleet"
)

// job.pool_action registers, in the holder node's own job directory, that the
// hub owns this attempt's ending on an exhausted pool: which action it is and
// the terminal reason it will write. The node watcher reads the marker and does
// not write its own terminal record while it is open, so the hub's record (with
// its quota reason and successor pointer) is the one hk keeps, and a job the hub
// closed is not raced into a plain `lost` by the node's missing-pane fallback.
const nodeCommandJobPoolAction = "job.pool_action"

const (
	nodePoolActionRegister = "register"
	nodePoolActionClear    = "clear"
	nodePoolActionFinish   = "finish"

	// nodePoolActionFile is the marker, beside the job's events/ ledger.
	nodePoolActionFile = "pool-action.json"
)

type nodeJobPoolActionArgs struct {
	Job    string `json:"job"`
	PaneID string `json:"pane_id"`
	Label  string `json:"label"`
	// ActionID identifies the hub's durable action (request id and attempt).
	ActionID string `json:"action_id"`
	// Op is "register" or "clear".
	Op string `json:"op"`
	// EndReason is the record the hub will write; empty on clear.
	EndReason string `json:"end_reason"`
}

type nodeJobPoolActionResult struct {
	Outcome string `json:"outcome"`
}

const (
	nodePoolActionRegistered = "registered"
	nodePoolActionCleared    = "cleared"
	nodePoolActionFinished   = "finished"
	// nodePoolActionMoved: the attempt is over or has a newer claim, so the
	// marker would speak about another attempt.
	nodePoolActionMoved = "attempt-moved"
	// nodePoolActionNoLedger: the job has no local ledger to hold the marker.
	nodePoolActionNoLedger = "ledger-missing"
)

func validNodeJobPoolActionArgs(args nodeJobPoolActionArgs) bool {
	if !validNodeJobName(args.Job) || (args.PaneID == "" && args.Label == "") {
		return false
	}
	if args.PaneID != "" && !nodeCommandPaneIDPattern.MatchString(args.PaneID) {
		return false
	}
	if args.Label != "" && !hubAgentLabelPattern.MatchString(args.Label) {
		return false
	}
	if !nodePoolActionIDPattern.MatchString(args.ActionID) {
		return false
	}
	switch args.Op {
	case nodePoolActionRegister, nodePoolActionFinish:
		return validNodeEndReason(args.EndReason)
	case nodePoolActionClear:
		return args.EndReason == ""
	}
	return false
}

func validNodeJobPoolActionResult(result nodeJobPoolActionResult) bool {
	switch result.Outcome {
	case nodePoolActionRegistered, nodePoolActionCleared, nodePoolActionFinished, nodePoolActionMoved, nodePoolActionNoLedger:
		return true
	}
	return false
}

// nodeJobPoolAction writes or removes the holder's marker for the attempt named
// by pane or label. It refuses an attempt that completed or was reclaimed after
// it, exactly as job.mark_lost does, so a marker can never outlive its attempt.
func (client *HubClient) nodeJobPoolAction(args nodeJobPoolActionArgs) (any, string) {
	job, _, found, err := client.openNodeJobDir(args.Job)
	if errors.Is(err, errNodeJobsRootUnset) {
		return nil, "jobs_root_unset"
	}
	if err != nil {
		return nil, "pool_action_failed"
	}
	if !found {
		return nodeJobPoolActionResult{Outcome: nodePoolActionNoLedger}, ""
	}
	defer job.Close()
	unlock, err := fleet.LockPoolAction(job)
	if err != nil {
		return nil, "pool_action_failed"
	}
	defer unlock()
	ledger, err := readNodeJobLedger(job, args.PaneID, args.Label)
	if err != nil {
		return nil, "pool_action_failed"
	}
	if args.Op != nodePoolActionClear && (ledger.newerClaim || args.Op == nodePoolActionRegister && (ledger.completed || ledger.lost)) {
		return nodeJobPoolActionResult{Outcome: nodePoolActionMoved}, ""
	}
	if args.Op == nodePoolActionRegister {
		raw, err := job.ReadFile(nodePoolActionFile)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, "pool_action_failed"
		}
		if err == nil {
			var marker struct {
				ActionID string `json:"action_id"`
				Done     bool   `json:"done"`
			}
			if json.Unmarshal(raw, &marker) != nil || marker.ActionID == "" {
				return nil, "pool_action_failed"
			}
			if marker.ActionID != args.ActionID && !marker.Done {
				return nodeJobPoolActionResult{Outcome: nodePoolActionMoved}, ""
			}
		}
	}
	if args.Op == nodePoolActionFinish || args.Op == nodePoolActionClear {
		raw, err := job.ReadFile(nodePoolActionFile)
		if errors.Is(err, os.ErrNotExist) && args.Op == nodePoolActionClear {
			return nodeJobPoolActionResult{Outcome: nodePoolActionCleared}, ""
		}
		var marker struct {
			ActionID string `json:"action_id"`
		}
		if err != nil || json.Unmarshal(raw, &marker) != nil || marker.ActionID != args.ActionID {
			return nodeJobPoolActionResult{Outcome: nodePoolActionMoved}, ""
		}
	}
	if args.Op == nodePoolActionClear {
		if err := job.Remove(nodePoolActionFile); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, "pool_action_failed"
		}
		return nodeJobPoolActionResult{Outcome: nodePoolActionCleared}, ""
	}
	body, err := json.Marshal(map[string]any{"action_id": args.ActionID, "end_reason": args.EndReason, "at": client.nowUTC(), "done": args.Op == nodePoolActionFinish})
	if err != nil {
		return nil, "pool_action_failed"
	}
	if err := durable.ReplaceFileAtomicallyInRoot(job, nodePoolActionFile, body, 0o600); err != nil {
		return nil, "pool_action_failed"
	}
	if args.Op == nodePoolActionFinish {
		return nodeJobPoolActionResult{Outcome: nodePoolActionFinished}, ""
	}
	return nodeJobPoolActionResult{Outcome: nodePoolActionRegistered}, ""
}

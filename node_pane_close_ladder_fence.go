package panewire

import (
	"context"
	"errors"
	"path/filepath"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/jobowner"
)

// The ladder close's ownership fence and the fence op (hk 5864;
// docs/escalate-author.md).
const (
	nodeCommandLadderFence = "pane.ladder_close_fence"

	// Refusals of the composite check: known facts, nothing was closed.
	ladderGrantChanged = "grant_changed"
	ladderRevoked      = "revoked"
	ladderUnbound      = "receipt_unbound"
	// Failures of the check: it could not say, so the result is unknown.
	ladderOwnerBusy   = "owner_busy"
	ladderFenceUnread = "fence_unreadable"
)

// ladderFenceCheck is the close's last check, run after the pane guards and
// right before `herdr pane close`. It tries the job's ownership lock once (no
// retry: contention is unknown), stores the release in held so the caller keeps
// the lock through the close, and makes ONE composite check under it:
//
//   - the job's grant equals the request's epoch exactly;
//   - the hub has not revoked that grant (a revocation leaves the grant file
//     as it was, so the grant alone cannot say);
//   - the spawn receipt of the attempt binds this pane and herdr session, and
//     was written at this epoch (a receipt with no epoch predates epochs and
//     is epoch 1), so a reassignment that returns the job at a later epoch
//     does not make an earlier attempt's receipt pass.
//
// Anything that cannot be read is unknown; missing provenance is refused.
func (client *HubClient) ladderFenceCheck(args nodeLadderCloseArgs, held *func()) func() error {
	return func() error {
		jobs := filepath.Join(client.jobsInboxRoot, "jobs")
		job, _, found, code := client.openCloseOpJobDir(args.Job)
		if code != "" {
			return nodeCloseHookStop{code: code}
		}
		if !found {
			return nodeCloseHookStop{outcome: ladderUnbound}
		}
		defer job.Close()
		release, ok, err := jobowner.TryLock(jobs, args.Job)
		switch {
		case err != nil:
			return nodeCloseHookStop{code: ladderFenceUnread}
		case !ok:
			return nodeCloseHookStop{code: ladderOwnerBusy}
		}
		*held = release
		grant, grantErr := jobowner.Epoch(jobs, args.Job)
		revoked, revokedErr := jobowner.Revoked(jobs, args.Job)
		target, targetErr := readNodeCloseTarget(job)
		if grantErr != nil || revokedErr != nil || targetErr != nil {
			return nodeCloseHookStop{code: ladderFenceUnread}
		}
		receiptEpoch := target.receiptEpoch
		if receiptEpoch == 0 {
			receiptEpoch = 1
		}
		switch {
		case grant != args.Epoch:
			return nodeCloseHookStop{outcome: ladderGrantChanged}
		case revoked > grant:
			return nodeCloseHookStop{outcome: ladderRevoked}
		case !target.receipt || target.attempt != args.Attempt || target.pane != args.PaneID || target.session == "" ||
			target.session != client.herdrSessionFor(args.Name, args.PaneID) || receiptEpoch != args.Epoch:
			return nodeCloseHookStop{outcome: ladderUnbound}
		}
		return nil
	}
}

// nodeLadderFenceArgs names the close to fence by request id alone (the
// request id's K is read from the index) or with its K, for a close the node
// never received; the K fields travel together.
type nodeLadderFenceArgs struct {
	RequestID string `json:"request_id"`
	Job       string `json:"job"`
	Node      string `json:"node"`
	Epoch     uint64 `json:"epoch"`
	Attempt   string `json:"attempt"`
	PaneID    string `json:"pane_id"`
}

func validNodeLadderFenceArgs(args nodeLadderFenceArgs) bool {
	if args.Job == "" && args.Node == "" && args.Epoch == 0 && args.Attempt == "" && args.PaneID == "" {
		return fleet.ValidCloseOpID(args.RequestID)
	}
	return fleet.ValidCloseOpID(args.RequestID) && validLadderKey(args.Job, args.Node, args.Epoch, args.Attempt, args.PaneID)
}

// nodeLadderFence settles a K: it answers the record that exists (a stored
// result, or unknown for one left closing), and otherwise writes a fenced
// tombstone, so a close delayed past this call finds it and never runs. The
// caller holds nodeCommandMu, as the close does.
func (client *HubClient) nodeLadderFence(_ context.Context, args nodeLadderFenceArgs) (any, string) {
	if client.machineID != "" && args.Node != "" && args.Node != client.machineID {
		return nil, "invalid_args"
	}
	store, err := client.openLadderStore()
	switch {
	case errors.Is(err, errNodeJobsRootUnset):
		return nil, "jobs_root_unset"
	case err != nil:
		return nil, "journal_failed"
	}
	defer store.close()
	var binding nodeLadderRecord
	if args.Job == "" {
		found, err := store.read(ladderRequestName(args.RequestID), &binding)
		switch {
		case err != nil:
			return nil, "journal_failed"
		case !found:
			// No K to fence: the request id was never seen and names none.
			return nil, "invalid_args"
		}
	} else {
		binding = nodeLadderRecord{Job: args.Job, Node: args.Node, Epoch: args.Epoch, Attempt: args.Attempt, PaneID: args.PaneID}
		if same, err := store.bindRequest(args.RequestID, binding); err != nil {
			return nil, "journal_failed"
		} else if !same {
			return nil, "invalid_args"
		}
	}
	name := ladderKName(ladderKey(nodeLadderCloseArgs{Job: binding.Job, Node: binding.Node, Epoch: binding.Epoch, Attempt: binding.Attempt, PaneID: binding.PaneID}))
	var record nodeLadderRecord
	found, err := store.read(name, &record)
	if err != nil {
		return nil, "journal_failed"
	}
	if found {
		if record, err = store.settle(name, record); err != nil {
			return nil, "journal_failed"
		}
		return answerLadder(record.State, "", true), ""
	}
	binding.State = nodeLadderFenced
	if store.write(name, binding) != nil {
		return nil, "journal_failed"
	}
	return answerLadder(nodeLadderFenced, "", false), ""
}

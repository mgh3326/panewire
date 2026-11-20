package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/mgh3326/panewire/internal/durable"
	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/jobowner"
)

// The ladder close operation (hk 5676; docs/escalate-author.md, "The close
// operation"). The escalate-author step closes the author's pane and then
// activates a staged successor, and the hub's staged row only says the close
// was asked for. This op makes the node's answer a durable fact: the node
// writes its result before it replies, so a lost reply or a hub restart reads
// the same result back instead of guessing from a vanished pane.
//
// It is not the close journal of pane.close_op (node_pane_close_op.go): that
// journal lives in the job's ledger and binds an attempt that has not ended,
// while a ladder step also replaces an author whose hk record completed. This
// op is keyed by the staged successor's request id and bound to (predecessor
// job, node, ownership epoch, attempt, pane); it runs pane.close's own guards
// (idle status, empty composer, open question) and stores one record per id.
//
// Ops: pane.ladder_close closes once and answers the stored result on every
// repeat; pane.ladder_close_status reads it; pane.ladder_close_fence ends a
// request nobody will repeat, so a delayed command can never close the pane
// after the hub released its stage. All three are hub-issued: the public
// node-command endpoint refuses them.
const (
	nodeCommandLadderClose       = "pane.ladder_close"
	nodeCommandLadderCloseStatus = "pane.ladder_close_status"
	nodeCommandLadderCloseFence  = "pane.ladder_close_fence"

	// hubCapabilityLadderClose is advertised by a node that runs the ladder
	// close op. A hub never sends the three ops, and never relies on their
	// stored results, to a node that did not advertise it.
	hubCapabilityLadderClose = "ladder_close_v1"

	// States of a ladder close record. closing is the intent written before
	// the pane close is asked for; every other state is final.
	nodeLadderNone    = "none"
	nodeLadderClosing = "closing"
	// nodeLadderClosed: this operation closed the pane.
	nodeLadderClosed = "closed"
	// nodeLadderGone: the pane was absent when the operation looked, so it
	// ended before this operation could close it.
	nodeLadderGone = "gone"
	// nodeLadderRefused: the guards kept the pane open; reason says which.
	nodeLadderRefused = "refused"
	// nodeLadderUnknown: a close may have run and no result was recorded (the
	// node stopped, or the record could not be written, after the intent).
	nodeLadderUnknown = "unknown"
	// nodeLadderFenced: the request was ended before it acted; it never will.
	nodeLadderFenced = "fenced"

	ladderCloseDir = "ladder-close"

	// nodeLadderNoAttempt is the attempt token of a job the node holds no
	// ledger for (its directory was removed, or never had a claim): the close
	// is then bound by its pane guards alone.
	nodeLadderNoAttempt = "none"
)

type nodeLadderCloseArgs struct {
	RequestID string `json:"request_id"`
	Job       string `json:"job"`
	Node      string `json:"node"`
	Epoch     uint64 `json:"epoch"`
	// Attempt names the predecessor attempt the hub decided to close.
	Attempt      string `json:"attempt"`
	PaneID       string `json:"pane_id"`
	Name         string `json:"name"`
	AgentStatus  string `json:"agent_status"`
	Cwd          string `json:"cwd"`
	AllowUnnamed bool   `json:"allow_unnamed"`
}

type nodeLadderRefArgs struct {
	RequestID string `json:"request_id"`
	Job       string `json:"job"`
}

// nodeLadderFenceArgs is pane.ladder_close_fence's decoded arguments, a
// distinct type so the dispatcher can tell it from status.
type nodeLadderFenceArgs nodeLadderRefArgs

type nodeLadderCloseResult struct {
	State string `json:"state"`
	// Replayed is true when a close answers a record an earlier call wrote.
	Replayed bool   `json:"replayed,omitempty"`
	Reason   string `json:"reason,omitempty"`
	// Attempt, on pane.ladder_close_status only, is the attempt token a close
	// for the job must name: the stored record's, or else the job's current
	// attempt from its own ledger (nodeLadderNoAttempt when it has none).
	Attempt string `json:"attempt,omitempty"`
}

func validNodeLadderCloseArgs(args nodeLadderCloseArgs) bool {
	return fleet.ValidCloseOpID(args.RequestID) && fleet.ValidJobID(args.Job) && machineIDPattern.MatchString(args.Node) && args.Epoch > 0 && fleet.ValidJobID(args.Attempt) &&
		nodeCommandPaneIDPattern.MatchString(args.PaneID) && validNodePaneName(args.Name) && nodePaneClosableStatus[args.AgentStatus] && validNodeCommandPath(args.Cwd)
}

func validNodeLadderRefArgs(args nodeLadderRefArgs) bool {
	return fleet.ValidCloseOpID(args.RequestID) && fleet.ValidJobID(args.Job)
}

func validNodeLadderCloseResult(result nodeLadderCloseResult) bool {
	if result.Reason != "" && result.State != nodeLadderRefused || result.Attempt != "" && !fleet.ValidJobID(result.Attempt) {
		return false
	}
	switch result.State {
	case nodeLadderNone, nodeLadderClosing, nodeLadderClosed, nodeLadderGone, nodeLadderRefused, nodeLadderUnknown, nodeLadderFenced:
		return !result.Replayed || result.State != nodeLadderNone
	}
	return false
}

// nodeLadderRecord is one stored request. The binding fields are written with
// the first record and compared on every repeat.
type nodeLadderRecord struct {
	RequestID string `json:"request_id"`
	Job       string `json:"job"`
	Node      string `json:"node"`
	Epoch     uint64 `json:"epoch"`
	Attempt   string `json:"attempt"`
	PaneID    string `json:"pane_id"`
	State     string `json:"state"`
	Reason    string `json:"reason,omitempty"`
}

func (r nodeLadderRecord) binds(args nodeLadderCloseArgs) bool {
	return r.Job == args.Job && r.Node == args.Node && r.Epoch == args.Epoch && r.Attempt == args.Attempt && r.PaneID == args.PaneID
}

func (r nodeLadderRecord) final() bool { return r.State != nodeLadderClosing }

// ladderCloseRoot opens the directory the records live in, creating it.
func (client *HubClient) ladderCloseRoot() (*os.Root, error) {
	if client.jobsInboxRoot == "" {
		return nil, errNodeJobsRootUnset
	}
	dir := filepath.Join(client.jobsInboxRoot, ladderCloseDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	// The journal directory entry and the directory itself are made durable
	// before any record in it is read or relied on: a host crash must not lose
	// an acknowledged result, or a fence, together with a lazily created
	// directory. Syncing on every open also makes a record whose own write
	// reported a failed sync durable before a retry acknowledges it.
	for _, path := range []string{dir, client.jobsInboxRoot} {
		if err := syncNodeDirectory(path); err != nil {
			return nil, err
		}
	}
	return os.OpenRoot(dir)
}

func syncNodeDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

// ladderAttempt is the attempt token a close of job must name: the job's
// current attempt in its own ledger. A job with no directory or no claim has
// none, and a close of it rests on the pane guards.
func (client *HubClient) ladderAttempt(job string) (string, error) {
	root, _, found, err := client.openNodeJobDir(job)
	if err != nil || !found {
		return nodeLadderNoAttempt, err
	}
	defer root.Close()
	target, err := readNodeCloseTarget(root)
	if err != nil || target.attempt == "" {
		return nodeLadderNoAttempt, err
	}
	return target.attempt, nil
}

// ladderOwnerStale reports why the request no longer holds the job: the node
// durably recorded a higher grant (job.assigned), or a hub revocation at or
// above the request's epoch (job.revoked, which leaves the grant file as it
// was). Empty means the request still owns the job.
func (client *HubClient) ladderOwnerStale(args nodeLadderCloseArgs) (string, error) {
	jobs := filepath.Join(client.jobsInboxRoot, "jobs")
	stored, err := jobowner.Epoch(jobs, args.Job)
	if err != nil {
		return "", err
	}
	if stored > args.Epoch {
		return "epoch_changed", nil
	}
	revoked, err := jobowner.Revoked(jobs, args.Job)
	if err != nil {
		return "", err
	}
	// A grant at or above the revocation's epoch supersedes it (jobowner.Revoked).
	if revoked > stored && revoked >= args.Epoch {
		return "revoked", nil
	}
	return "", nil
}

// ladderBound reports that the job's current ledger still binds the request:
// the attempt is the one the hub decided on, and a spawn receipt, when the
// attempt has one, names this pane and label. An ended attempt is allowed: an
// author's first READY completes its record and the pane goes on working.
func (client *HubClient) ladderBound(args nodeLadderCloseArgs) (bool, error) {
	root, _, found, err := client.openNodeJobDir(args.Job)
	if err != nil {
		return false, err
	}
	if !found {
		return args.Attempt == nodeLadderNoAttempt, nil
	}
	defer root.Close()
	target, err := readNodeCloseTarget(root)
	if err != nil {
		return false, err
	}
	attempt := firstNonempty(target.attempt, nodeLadderNoAttempt)
	if args.Attempt != attempt {
		return false, nil
	}
	return !target.receipt || target.pane == args.PaneID && target.label == args.Name, nil
}

func readNodeLadderRecord(root *os.Root, id string) (nodeLadderRecord, bool, error) {
	file, err := root.Open(id + ".json")
	if errors.Is(err, os.ErrNotExist) {
		return nodeLadderRecord{}, false, nil
	}
	if err != nil {
		return nodeLadderRecord{}, false, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, 64<<10))
	if err != nil {
		return nodeLadderRecord{}, false, err
	}
	var record nodeLadderRecord
	if err := json.Unmarshal(raw, &record); err != nil || record.RequestID != id {
		return nodeLadderRecord{}, false, errors.New("unreadable ladder close record")
	}
	return record, true, nil
}

// writeNodeLadderRecord makes the record durable before it returns: the
// final state is the node's answer, and the answer is sent only after it.
func writeNodeLadderRecord(root *os.Root, record nodeLadderRecord) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return durable.ReplaceFileAtomicallyInRootSynced(root, record.RequestID+".json", raw, 0o600)
}

func nodeLadderAnswer(record nodeLadderRecord, replayed bool) nodeLadderCloseResult {
	return nodeLadderCloseResult{State: record.State, Replayed: replayed, Reason: record.Reason}
}

// settleNodeLadderRecord ends an interrupted request: a record left closing
// means a close may have run and its result was never recorded.
func settleNodeLadderRecord(root *os.Root, record nodeLadderRecord) (nodeLadderRecord, error) {
	if record.State == nodeLadderClosing {
		record.State = nodeLadderUnknown
		if err := writeNodeLadderRecord(root, record); err != nil {
			return record, err
		}
	}
	return record, nil
}

// nodeLadderClose closes the predecessor's pane once. The caller holds
// nodeCommandMu, so no other acting command interleaves with the guards.
func (client *HubClient) nodeLadderClose(ctx context.Context, args nodeLadderCloseArgs) (any, string) {
	if client.machineID != "" && args.Node != client.machineID {
		return nil, "invalid_args"
	}
	root, err := client.ladderCloseRoot()
	switch {
	case errors.Is(err, errNodeJobsRootUnset):
		return nil, "jobs_root_unset"
	case err != nil:
		return nil, "journal_failed"
	}
	defer root.Close()
	record, found, err := readNodeLadderRecord(root, args.RequestID)
	if err != nil {
		return nil, "journal_failed"
	}
	if found {
		// A fenced request never acts, whatever binding the late command names.
		if record.State == nodeLadderFenced && record.Job == args.Job {
			return nodeLadderAnswer(record, true), ""
		}
		// A repeat names the binding it was registered with; another job,
		// epoch, attempt or pane never reads this request's result.
		if !record.binds(args) {
			return nil, "invalid_args"
		}
		record, err = settleNodeLadderRecord(root, record)
		if err != nil {
			return nil, "journal_failed"
		}
		return nodeLadderAnswer(record, true), ""
	}
	// The ownership epoch is the larger of the node's in-memory assignment and
	// the one a job.assigned durably recorded, which a restarted node has not
	// loaded yet (as pane.close_op reads it).
	client.assignmentMu.Lock()
	owned := client.assignedJobs[args.Job]
	client.assignmentMu.Unlock()
	if stored, err := jobowner.Epoch(filepath.Join(client.jobsInboxRoot, "jobs"), args.Job); err != nil {
		return nil, "epoch_mismatch"
	} else if stored > owned {
		owned = stored
	}
	if owned > args.Epoch {
		return nil, "epoch_mismatch"
	}
	if reason, err := client.ladderOwnerStale(args); err != nil || reason != "" {
		return nil, "epoch_mismatch"
	}
	record = nodeLadderRecord{RequestID: args.RequestID, Job: args.Job, Node: args.Node, Epoch: args.Epoch, Attempt: args.Attempt, PaneID: args.PaneID, State: nodeLadderClosing}
	closeArgs := nodePaneCloseArgs{PaneID: args.PaneID, Name: args.Name, AgentStatus: args.AgentStatus, Cwd: args.Cwd, AllowUnnamed: args.AllowUnnamed, QuestionJob: args.Job}
	// The intent is written after the guarded look and before the last look
	// that precedes `herdr pane close`: a pane that was never going to be
	// closed leaves no intent, and one that may have been leaves one.
	stale := ""
	releaseOwner := func() {}
	result, code := client.nodePaneCloseWith(ctx, closeArgs, func() error {
		// Read the ledger again after the first look and the settle, immediately
		// before the intent: an attempt that moved on, or another pane, is not
		// closed. The epoch is read again too.
		if reason, err := client.ladderOwnerStale(args); err != nil || reason != "" {
			return nodeCloseHookStop{code: "epoch_mismatch"}
		}
		bound, err := client.ladderBound(args)
		if err != nil {
			return err
		}
		if !bound {
			return nodeCloseHookStop{outcome: nodePaneChanged}
		}
		return writeNodeLadderRecord(root, record)
	}, func() error {
		// The last check before `herdr pane close`: a grant a job.assigned
		// recorded, a hub revocation, or an attempt a re-prompt started while the
		// final look ran leaves the pane open and the refusal stored. The job's
		// ownership lock is taken first and held until the close returns, so a
		// grant or revocation lands before this check or after the close, never
		// between them. A job with no directory has no ownership ledger to race.
		if _, _, found, err := client.openNodeJobDir(args.Job); err != nil {
			return err
		} else if found {
			unlock, err := jobowner.Lock(filepath.Join(client.jobsInboxRoot, "jobs"), args.Job)
			if err != nil {
				return err
			}
			releaseOwner = unlock
		}
		if reason, err := client.ladderOwnerStale(args); err != nil {
			return err
		} else if reason != "" {
			stale = reason
			return nodeCloseHookStop{outcome: nodePaneChanged}
		}
		bound, err := client.ladderBound(args)
		if err != nil {
			return err
		}
		if !bound {
			stale = "attempt_changed"
			return nodeCloseHookStop{outcome: nodePaneChanged}
		}
		return nil
	})
	releaseOwner()
	if code != "" {
		// A failed command leaves whatever the journal holds: no record, or
		// the closing intent a repeat settles as unknown.
		return nil, code
	}
	switch result.Outcome {
	case nodePaneClosed:
		record.State = nodeLadderClosed
	case nodePaneGone:
		record.State = nodeLadderGone
	case nodePaneQuestionOpen:
		record.State, record.Reason = nodeLadderRefused, nodePaneQuestionOpen
	default:
		record.State, record.Reason = nodeLadderRefused, firstNonempty(stale, nodePaneChanged)
	}
	if err := writeNodeLadderRecord(root, record); err != nil {
		return nil, "journal_failed"
	}
	return nodeLadderAnswer(record, false), ""
}

// nodeLadderRef reads a request (fence false) or ends it (fence true). Fencing
// a request with no record writes a fenced one under its id, so a close
// delayed in transit finds it and touches no pane; fencing one the node left
// closing settles it as unknown; a final record is answered as stored. The
// caller holds nodeCommandMu, so a fence never lands inside a running close.
func (client *HubClient) nodeLadderRef(args nodeLadderRefArgs, fence bool) (any, string) {
	root, err := client.ladderCloseRoot()
	switch {
	case errors.Is(err, errNodeJobsRootUnset):
		return nil, "jobs_root_unset"
	case err != nil:
		return nil, "journal_failed"
	}
	defer root.Close()
	record, found, err := readNodeLadderRecord(root, args.RequestID)
	if err != nil {
		return nil, "journal_failed"
	}
	if found && record.Job != args.Job {
		return nil, "invalid_args"
	}
	if !found {
		if !fence {
			attempt, err := client.ladderAttempt(args.Job)
			if err != nil {
				return nil, "journal_failed"
			}
			return nodeLadderCloseResult{State: nodeLadderNone, Attempt: attempt}, ""
		}
		record = nodeLadderRecord{RequestID: args.RequestID, Job: args.Job, State: nodeLadderFenced}
		if err := writeNodeLadderRecord(root, record); err != nil {
			return nil, "journal_failed"
		}
		return nodeLadderAnswer(record, false), ""
	}
	if fence {
		if record, err = settleNodeLadderRecord(root, record); err != nil {
			return nil, "journal_failed"
		}
	}
	answer := nodeLadderAnswer(record, false)
	if !fence {
		answer.Attempt = record.Attempt
	}
	return answer, ""
}

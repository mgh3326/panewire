package panewire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/mgh3326/panewire/internal/durable"
	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/jobowner"
)

// The ladder close operation (hk 5676; docs/escalate-author.md, "The close
// operation"). The escalate-author step closes the author's pane and then
// activates a staged successor. This op makes the node's answer a durable
// fact: the node writes its result before it replies, so a lost reply or a hub
// restart reads the same result back instead of guessing from a vanished pane.
//
// One durable record per K = (job, node, epoch, attempt, pane), shared by every
// request id that targets it. A request id has its own small index file that
// names its K, so a fence can find the record without the binding. Order, under
// the node's acting lock:
//
//  1. replay: a final record is answered as stored; one left `closing` becomes
//     `unknown` and never runs again;
//  2. publish `closing` (fsynced) - nothing is closed without a durable intent;
//  3. run pane.close's guards (idle status, empty composer, open question);
//  4. try the job's ownership lock ONCE and make one composite check under it:
//     the grant equals the request's epoch, no hub revocation stands, and the
//     attempt's spawn receipt, written after the grant, names this pane;
//  5. hold that lock through the single `herdr pane close`;
//  6. store the final result (fsynced) before replying.
//
// A known refusal (a guard, a stale binding) closed nothing and leaves no
// record, so the next request for the same K is judged afresh. Contention, a
// read or write failure, or a failure after the intent all end as `unknown`:
// the node cannot say the pane was not closed, and the hub keeps the stage and
// asks the owner. The record is final only after the result is durable; a
// failed final write leaves `closing`, which never runs again.
//
// Ops: pane.ladder_close and pane.ladder_close_fence. Both are hub-issued; the
// public node-command endpoint refuses them. The fence ends a request nobody
// will repeat, so a delayed close can never act after the hub released its
// stage.
const (
	nodeCommandLadderClose      = "pane.ladder_close"
	nodeCommandLadderCloseFence = "pane.ladder_close_fence"

	// hubCapabilityLadderClose is advertised by a node that runs the ladder
	// close op. A hub never sends the ops, and never relies on their stored
	// results, to a node that did not advertise it.
	hubCapabilityLadderClose = "ladder_close_v1"

	// States of a ladder close record. closing is the intent written before the
	// pane close is asked for; every other state is final.
	nodeLadderClosing = "closing"
	// nodeLadderClosed: this operation closed the pane.
	nodeLadderClosed = "closed"
	// nodeLadderGone: the pane was absent when the operation looked, so it ended
	// before this operation could close it.
	nodeLadderGone = "gone"
	// nodeLadderRefused: a guard or the composite check kept the pane open;
	// reason says which. A refusal is never stored.
	nodeLadderRefused = "refused"
	// nodeLadderUnknown: a close may have run and no result was recorded.
	nodeLadderUnknown = "unknown"
	// nodeLadderFenced: the request was ended before it acted; it never will.
	nodeLadderFenced = "fenced"

	ladderCloseDir = "ladder-close"
)

type nodeLadderCloseArgs struct {
	RequestID string `json:"request_id"`
	Job       string `json:"job"`
	Node      string `json:"node"`
	Epoch     uint64 `json:"epoch"`
	// Attempt is the predecessor's attempt token as pane.capture reported it:
	// the spawn receipt's event name.
	Attempt      string `json:"attempt"`
	PaneID       string `json:"pane_id"`
	Name         string `json:"name"`
	AgentStatus  string `json:"agent_status"`
	Cwd          string `json:"cwd"`
	AllowUnnamed bool   `json:"allow_unnamed"`
}

type nodeLadderFenceArgs struct {
	RequestID string `json:"request_id"`
	Job       string `json:"job"`
}

type nodeLadderCloseResult struct {
	State string `json:"state"`
	// Replayed is true when a close answers a record an earlier call wrote.
	Replayed bool   `json:"replayed,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

func validNodeLadderCloseArgs(args nodeLadderCloseArgs) bool {
	return fleet.ValidCloseOpID(args.RequestID) && fleet.ValidJobID(args.Job) && machineIDPattern.MatchString(args.Node) && args.Epoch > 0 && fleet.ValidJobID(args.Attempt) &&
		nodeCommandPaneIDPattern.MatchString(args.PaneID) && validNodePaneName(args.Name) && nodePaneClosableStatus[args.AgentStatus] && validNodeCommandPath(args.Cwd)
}

func validNodeLadderFenceArgs(args nodeLadderFenceArgs) bool {
	return fleet.ValidCloseOpID(args.RequestID) && fleet.ValidJobID(args.Job)
}

func validNodeLadderCloseResult(result nodeLadderCloseResult) bool {
	if result.Reason != "" && result.State != nodeLadderRefused && result.State != nodeLadderUnknown {
		return false
	}
	switch result.State {
	case nodeLadderClosing, nodeLadderClosed, nodeLadderGone, nodeLadderRefused, nodeLadderUnknown, nodeLadderFenced:
		return true
	}
	return false
}

// nodeLadderRecord is the K record. Its binding fields are the K it is stored
// under.
type nodeLadderRecord struct {
	Job     string `json:"job"`
	Node    string `json:"node"`
	Epoch   uint64 `json:"epoch"`
	Attempt string `json:"attempt"`
	PaneID  string `json:"pane_id"`
	State   string `json:"state"`
}

// nodeLadderRequest is a request id's index: the K it named, or a fence.
type nodeLadderRequest struct {
	RequestID string `json:"request_id"`
	Job       string `json:"job"`
	K         string `json:"k,omitempty"`
	Fenced    bool   `json:"fenced,omitempty"`
}

func ladderKey(args nodeLadderCloseArgs) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d\x00%s\x00%s", args.Job, args.Node, args.Epoch, args.Attempt, args.PaneID)))
	return hex.EncodeToString(sum[:])
}

// ladderStore is the journal directory, opened per command.
type ladderStore struct {
	root *os.Root
	dir  string
}

func (client *HubClient) openLadderStore() (*ladderStore, error) {
	if client.jobsInboxRoot == "" {
		return nil, errNodeJobsRootUnset
	}
	dir := filepath.Join(client.jobsInboxRoot, ladderCloseDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	// The journal directory entry and the directory itself are fsynced before
	// any record in it is read or relied on, so a created entry, and a record
	// whose own write reported a failed sync, are durable before a result or a
	// fence is acknowledged.
	for _, path := range []string{dir, client.jobsInboxRoot} {
		if err := syncNodeDirectory(path); err != nil {
			return nil, err
		}
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	return &ladderStore{root: root, dir: dir}, nil
}

func syncNodeDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func (s *ladderStore) close() { s.root.Close() }

func (s *ladderStore) read(name string, into any) (bool, error) {
	file, err := s.root.Open(name)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, 64<<10))
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(raw, into); err != nil {
		return false, errors.New("unreadable ladder close record")
	}
	return true, nil
}

// write makes the record durable before it returns; an error does not mean the
// record is absent (the rename precedes the directory sync).
func (s *ladderStore) write(name string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return durable.ReplaceFileAtomicallyInRootSynced(s.root, name, raw, 0o600)
}

func (s *ladderStore) remove(name string) error {
	if err := s.root.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncNodeDirectory(s.dir)
}

func ladderKName(k string) string        { return "k-" + k + ".json" }
func ladderRequestName(id string) string { return "r-" + id + ".json" }

func answerLadder(state, reason string, replayed bool) nodeLadderCloseResult {
	return nodeLadderCloseResult{State: state, Replayed: replayed, Reason: reason}
}

// settle ends an interrupted record: one left closing means a close may have
// run and its result was never recorded.
func (s *ladderStore) settle(name string, record nodeLadderRecord) (nodeLadderRecord, error) {
	if record.State == nodeLadderClosing {
		record.State = nodeLadderUnknown
		if err := s.write(name, record); err != nil {
			return record, err
		}
	}
	return record, nil
}

// nodeLadderClose closes the predecessor's pane at most once per K. The caller
// holds nodeCommandMu.
func (client *HubClient) nodeLadderClose(ctx context.Context, args nodeLadderCloseArgs) (any, string) {
	if client.machineID != "" && args.Node != client.machineID {
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
	k := ladderKey(args)
	var request nodeLadderRequest
	haveRequest, err := store.read(ladderRequestName(args.RequestID), &request)
	if err != nil {
		return nil, "journal_failed"
	}
	if haveRequest {
		if request.Fenced {
			return answerLadder(nodeLadderFenced, "", true), ""
		}
		if request.K != k {
			// The request id was first bound to another K: some close under that
			// binding may have run, so this one does not act.
			return answerLadder(nodeLadderUnknown, "binding_changed", true), ""
		}
	}
	var record nodeLadderRecord
	found, err := store.read(ladderKName(k), &record)
	if err != nil {
		return nil, "journal_failed"
	}
	index := nodeLadderRequest{RequestID: args.RequestID, Job: args.Job, K: k}
	if found {
		if record, err = store.settle(ladderKName(k), record); err != nil {
			return nil, "journal_failed"
		}
		if !haveRequest && store.write(ladderRequestName(args.RequestID), index) != nil {
			return nil, "journal_failed"
		}
		return answerLadder(record.State, "", true), ""
	}
	if !haveRequest {
		if err := store.write(ladderRequestName(args.RequestID), index); err != nil {
			return nil, "journal_failed"
		}
	}
	// The intent is durable before anything is looked at or closed.
	record = nodeLadderRecord{Job: args.Job, Node: args.Node, Epoch: args.Epoch, Attempt: args.Attempt, PaneID: args.PaneID, State: nodeLadderClosing}
	if err := store.write(ladderKName(k), record); err != nil {
		return nil, "journal_failed"
	}
	return client.runLadderClose(ctx, store, k, record, args)
}

// runLadderClose runs steps 3-6 for a record whose intent is durable.
func (client *HubClient) runLadderClose(ctx context.Context, store *ladderStore, k string, record nodeLadderRecord, args nodeLadderCloseArgs) (any, string) {
	finish := func(state, reason string) (any, string) {
		record.State = state
		if store.write(ladderKName(k), record) != nil {
			// The record may or may not be on disk; either way this reply is not
			// its result, and a repeat settles it as unknown.
			return nil, "journal_failed"
		}
		return answerLadder(state, reason, false), ""
	}
	// refuse closes nothing and stores nothing, so the next request for the same
	// K is judged afresh.
	refuse := func(reason string) (any, string) {
		if store.remove(ladderKName(k)) != nil {
			return nil, "journal_failed"
		}
		return answerLadder(nodeLadderRefused, reason, false), ""
	}
	jobs := filepath.Join(client.jobsInboxRoot, "jobs")
	closeArgs := nodePaneCloseArgs{PaneID: args.PaneID, Name: args.Name, AgentStatus: args.AgentStatus, Cwd: args.Cwd, AllowUnnamed: args.AllowUnnamed, QuestionJob: args.Job}
	reason, contended := "", false
	releaseOwner := func() {}
	result, code := client.nodePaneCloseWith(ctx, closeArgs, nil, func() error {
		// The last check before `herdr pane close`. The ownership lock (the one
		// job.assigned and job.revoked take) is tried once, after the pane
		// checks, and held until the close returns: a grant or revocation lands
		// before the check or after the close. A job event writer holding it is
		// contention, which ends as unknown.
		unlock, err := jobowner.TryLock(jobs, args.Job)
		if errors.Is(err, jobowner.ErrLockBusy) {
			contended = true
			return nodeCloseHookStop{outcome: nodePaneChanged}
		} else if err != nil {
			return err
		}
		releaseOwner = unlock
		stale, err := client.ladderFence(jobs, args)
		if err != nil {
			return err
		}
		if stale != "" {
			reason = stale
			return nodeCloseHookStop{outcome: nodePaneChanged}
		}
		return nil
	})
	releaseOwner()
	switch {
	case contended:
		return finish(nodeLadderUnknown, "lock_busy")
	case code != "":
		// A failure after the intent: a close may have run.
		return finish(nodeLadderUnknown, code)
	}
	switch result.Outcome {
	case nodePaneClosed:
		return finish(nodeLadderClosed, "")
	case nodePaneGone:
		return finish(nodeLadderGone, "")
	case nodePaneQuestionOpen:
		return refuse(nodePaneQuestionOpen)
	}
	return refuse(firstNonempty(reason, nodePaneChanged))
}

// ladderFence is the one composite check, made under the ownership lock: the
// node's grant equals the request's epoch, no hub revocation stands above it,
// and the attempt is the one the request names - its spawn receipt names this
// pane and label, no hub revocation came after it, and it was written after the
// grant (a revocation marker a reassignment deleted leaves A/1 -> B/2 -> A/3
// with the epoch-1 receipt, which only its age shows). A job with no ledger has
// no provenance, and the close fails closed. The empty string means the close
// may go on.
func (client *HubClient) ladderFence(jobs string, args nodeLadderCloseArgs) (string, error) {
	grant, err := jobowner.Epoch(jobs, args.Job)
	if err != nil {
		return "", err
	}
	if grant != args.Epoch {
		return "epoch_changed", nil
	}
	revoked, err := jobowner.Revoked(jobs, args.Job)
	if err != nil {
		return "", err
	}
	if revoked > grant {
		return "revoked", nil
	}
	root, _, found, err := client.openNodeJobDir(args.Job)
	if err != nil {
		return "", err
	}
	if !found {
		return "no_provenance", nil
	}
	defer root.Close()
	target, err := readNodeCloseTarget(root)
	if errors.Is(err, os.ErrNotExist) {
		return "no_provenance", nil
	}
	if err != nil {
		return "", err
	}
	// An ended attempt is allowed: an author's first READY completes its record
	// and the pane goes on working. A hub revocation is not an end.
	if target.hubRevoked || !target.receipt || target.attempt != args.Attempt || target.pane != args.PaneID || target.label != args.Name {
		return "attempt_changed", nil
	}
	if grantAt, ok, err := jobowner.GrantTime(jobs, args.Job); err != nil {
		return "", err
	} else if ok {
		info, err := root.Stat(filepath.Join("events", target.attempt))
		if err != nil {
			return "", err
		}
		if info.ModTime().Before(grantAt) {
			return "stale_receipt", nil
		}
	}
	return "", nil
}

// ladderAttempt is the attempt token a close of job must name: its current
// spawn receipt's event name. Empty means the node holds no provenance for the
// job, and the hub keeps the marker path.
func (client *HubClient) ladderAttempt(job string) string {
	root, _, found, err := client.openNodeJobDir(job)
	if err != nil || !found {
		return ""
	}
	defer root.Close()
	target, err := readNodeCloseTarget(root)
	if err != nil || !target.receipt {
		return ""
	}
	return target.attempt
}

// nodeLadderFence ends a request. A request that never reached the intent is
// fenced for good: a close delayed in transit finds it and acts on no pane. One
// that did is answered from its K record (a record left closing becomes
// unknown). The caller holds nodeCommandMu, so a fence never lands inside a
// running close.
func (client *HubClient) nodeLadderFence(args nodeLadderFenceArgs) (any, string) {
	store, err := client.openLadderStore()
	switch {
	case errors.Is(err, errNodeJobsRootUnset):
		return nil, "jobs_root_unset"
	case err != nil:
		return nil, "journal_failed"
	}
	defer store.close()
	var request nodeLadderRequest
	haveRequest, err := store.read(ladderRequestName(args.RequestID), &request)
	if err != nil {
		return nil, "journal_failed"
	}
	if haveRequest && request.Job != args.Job {
		return nil, "invalid_args"
	}
	if haveRequest && request.Fenced {
		return answerLadder(nodeLadderFenced, "", false), ""
	}
	if haveRequest {
		var record nodeLadderRecord
		found, err := store.read(ladderKName(request.K), &record)
		if err != nil {
			return nil, "journal_failed"
		}
		if found {
			if record, err = store.settle(ladderKName(request.K), record); err != nil {
				return nil, "journal_failed"
			}
			return answerLadder(record.State, "", false), ""
		}
	}
	// No record: nothing this request did stands, and nothing it does later will.
	if err := store.write(ladderRequestName(args.RequestID), nodeLadderRequest{RequestID: args.RequestID, Job: args.Job, Fenced: true}); err != nil {
		return nil, "journal_failed"
	}
	return answerLadder(nodeLadderFenced, "", false), ""
}

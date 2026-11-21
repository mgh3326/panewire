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
)

// The ladder close operation (hk 5863, step 1 of the hk 5676 split;
// docs/escalate-author.md). The escalate-author step closes the author's pane
// and then activates a staged successor. This op makes the node's answer a
// durable fact: the node writes its result before it replies, so a lost reply
// or a hub restart reads the same result back instead of guessing from a
// vanished pane.
//
// One durable record per K = (job, node, epoch, attempt, pane), shared by
// every request id that targets it. Order, under the node's acting lock:
//
//  1. replay: a final record is answered as stored; one left `closing`
//     becomes `unknown` and never runs again;
//  2. publish `closing` (fsynced) - nothing is closed without a durable
//     intent, and a failed intent write closes nothing;
//  3. run pane.close's guards (pane match, empty composer, open question);
//  4. one `herdr pane close`;
//  5. store the final result (fsynced) before replying.
//
// Step 2 (hk 5864) adds, between the guards and the close, the job's ownership
// lock (tried once; contention is `unknown`) and one composite fence check made
// under it (ladderFenceCheck), held through the close; and the
// pane.ladder_close_fence op (node_pane_close_ladder_fence.go) with the
// request-id index it finds a K by. The hub calls them only on a node that
// advertises ladder_close_v1 (step 3, hk 5865, hub_escalate_author_ladder.go);
// they are admitted only as hub-issued commands.
//
// A known refusal (a guard) closed nothing and leaves no record, so the next
// request for the same K is judged afresh. A failure after the intent — a
// read, or the close itself — ends as `unknown`: the node cannot say the pane
// was not closed. The record is final only after the result is durable; a
// failed final write leaves `closing`, which never runs again.
const (
	nodeCommandLadderClose = "pane.ladder_close"

	// hubCapabilityLadderClose: the node answers pane.ladder_close and
	// pane.ladder_close_fence (hk 5865). The hub calls them only on a node that
	// advertises it; any other node keeps pane.close plus the close marker.
	hubCapabilityLadderClose = fleet.LadderCloseProtocolV1

	// States of a ladder close record. closing is the intent written before
	// the pane close is asked for; every other state is final.
	nodeLadderClosing = "closing"
	// nodeLadderClosed: this operation closed the pane.
	nodeLadderClosed = "closed"
	// nodeLadderGone: the pane was absent when the operation looked, so it
	// ended before this operation could close it.
	nodeLadderGone = "gone"
	// nodeLadderRefused: a guard kept the pane open; reason says which. A
	// refusal is never stored.
	nodeLadderRefused = "refused"
	// nodeLadderUnknown: a close may have run and no result was recorded.
	nodeLadderUnknown = "unknown"
	// nodeLadderFenced: a fence was recorded before any close ran, so a close
	// for this K never runs.
	nodeLadderFenced = "fenced"

	ladderCloseDir = "ladder-close"
)

type nodeLadderCloseArgs struct {
	RequestID string `json:"request_id"`
	Job       string `json:"job"`
	Node      string `json:"node"`
	Epoch     uint64 `json:"epoch"`
	// Attempt is the predecessor's attempt token: the spawn receipt's event
	// name. A request with no epoch or attempt binding has no provenance and
	// is refused before anything is looked at (there is no permissive
	// fallback).
	Attempt      string `json:"attempt"`
	PaneID       string `json:"pane_id"`
	Name         string `json:"name"`
	AgentStatus  string `json:"agent_status"`
	Cwd          string `json:"cwd"`
	AllowUnnamed bool   `json:"allow_unnamed"`
}

type nodeLadderCloseResult struct {
	State string `json:"state"`
	// Replayed is true when a close answers a record an earlier call wrote.
	Replayed bool   `json:"replayed,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

func validNodeLadderCloseArgs(args nodeLadderCloseArgs) bool {
	return fleet.ValidCloseOpID(args.RequestID) && validLadderKey(args.Job, args.Node, args.Epoch, args.Attempt, args.PaneID) && validNodePaneName(args.Name) && nodePaneClosableStatus[args.AgentStatus] && validNodeCommandPath(args.Cwd)
}

func validLadderKey(job, node string, epoch uint64, attempt, pane string) bool {
	return fleet.ValidJobID(job) && machineIDPattern.MatchString(node) && epoch > 0 && fleet.ValidJobID(attempt) && nodeCommandPaneIDPattern.MatchString(pane)
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
	// whose own write reported a failed sync, are durable before a result is
	// acknowledged.
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

// write makes the record durable before it returns; an error does not mean
// the record is absent (the rename precedes the directory sync).
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

func ladderKName(k string) string { return "k-" + k + ".json" }

// ladderRequestName is the request-id index entry: it holds the K binding the
// request id was first used for, so one request id never names two Ks and a
// fence by request id finds its K.
func ladderRequestName(id string) string { return "r-" + id + ".json" }

// bindRequest records which K a request id names, or reports that it already
// names another one.
func (s *ladderStore) bindRequest(id string, binding nodeLadderRecord) (ok bool, err error) {
	var stored nodeLadderRecord
	found, err := s.read(ladderRequestName(id), &stored)
	if err != nil {
		return false, err
	}
	if found {
		return stored == binding, nil
	}
	return true, s.write(ladderRequestName(id), binding)
}

func answerLadder(state, reason string, replayed bool) nodeLadderCloseResult {
	return nodeLadderCloseResult{State: state, Replayed: replayed, Reason: reason}
}

// settle ends an interrupted record: one left closing means a close may have
// run and its result was never recorded. A record that cannot be settled
// stays closing and is reported as a failure, never as a result.
func (s *ladderStore) settle(name string, record nodeLadderRecord) (nodeLadderRecord, error) {
	if record.State == nodeLadderClosing {
		record.State = nodeLadderUnknown
		if err := s.write(name, record); err != nil {
			return record, err
		}
	}
	return record, nil
}

// nodeLadderClose closes the predecessor's pane at most once per K. The
// caller holds nodeCommandMu.
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
	binding := nodeLadderRecord{Job: args.Job, Node: args.Node, Epoch: args.Epoch, Attempt: args.Attempt, PaneID: args.PaneID}
	if same, err := store.bindRequest(args.RequestID, binding); err != nil {
		return nil, "journal_failed"
	} else if !same {
		return nil, "invalid_args"
	}
	var record nodeLadderRecord
	found, err := store.read(ladderKName(k), &record)
	if err != nil {
		return nil, "journal_failed"
	}
	if found {
		if record, err = store.settle(ladderKName(k), record); err != nil {
			return nil, "journal_failed"
		}
		return answerLadder(record.State, "", true), ""
	}
	// The intent is durable before anything is looked at or closed; a failed
	// write closes nothing.
	record = binding
	record.State = nodeLadderClosing
	if err := store.write(ladderKName(k), record); err != nil {
		return nil, "journal_failed"
	}
	return client.runLadderClose(ctx, store, k, record, args)
}

// runLadderClose runs the guarded close for a record whose intent is durable.
func (client *HubClient) runLadderClose(ctx context.Context, store *ladderStore, k string, record nodeLadderRecord, args nodeLadderCloseArgs) (any, string) {
	finish := func(state, reason string) (any, string) {
		record.State = state
		if store.write(ladderKName(k), record) != nil {
			// The record may or may not be on disk; either way this reply is
			// not its result, and a repeat settles it as unknown.
			return nil, "journal_failed"
		}
		return answerLadder(state, reason, false), ""
	}
	// refuse closes nothing and stores nothing, so the next request for the
	// same K is judged afresh.
	refuse := func(reason string) (any, string) {
		if store.remove(ladderKName(k)) != nil {
			return nil, "journal_failed"
		}
		return answerLadder(nodeLadderRefused, reason, false), ""
	}
	closeArgs := nodePaneCloseArgs{PaneID: args.PaneID, Name: args.Name, AgentStatus: args.AgentStatus, Cwd: args.Cwd, AllowUnnamed: args.AllowUnnamed, QuestionJob: args.Job}
	// The lock the fence check takes is held until the close call has returned.
	var release func()
	defer func() {
		if release != nil {
			release()
		}
	}()
	result, code := client.nodePaneCloseWith(ctx, closeArgs, nil, client.ladderFenceCheck(args, &release))
	if code != "" {
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
	return refuse(result.Outcome)
}

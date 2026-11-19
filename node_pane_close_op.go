package panewire

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/jobowner"
)

// hubCapabilityCloseOps names the close operations in hello. A node
// advertises it only while fleet.CloseOpsReady holds (hk 4632): the migration
// barrier `panewire node close-ops enable` was recorded under the jobs root,
// and no older-binary watcher or launcher runs. The node re-checks on every
// hello and while connected, and registers again when the answer changes
// (closeOpsRecheckInterval).
const hubCapabilityCloseOps = "close_ops_v1"

// hubCapabilityCloseMissing names the missing-job answer of pane.close_status
// (hk 5296, docs/node-commands.md "A missing job"). A node advertises it with
// close_ops_v1; the hub sends the status `epoch` argument, and accepts the
// answer, only for a node that does.
const hubCapabilityCloseMissing = "close_missing_v1"

// closeOpsRecheckInterval paces the connected node's re-check of close-op
// readiness (a process listing); a change sends a fresh hello.
const closeOpsRecheckInterval = 30 * time.Second

// closeOpsReady reports whether this node may advertise and admit close ops
// now. A node without a jobs root (and no fixture ledger) never may.
func (client *HubClient) closeOpsReady(ctx context.Context) bool {
	if client.nodeHubClose == nil && client.jobsInboxRoot == "" {
		return false
	}
	if client.nodeHubClose == nil && !client.closeOpsRecovered.Load() {
		return false
	}
	return client.hubCloseLedger().CloseOpsReady(ctx)
}

// spawnHelloCapabilities is hubSpawnCapabilities plus close_ops_v1 when the
// node is ready for it and checkout_sparse_v1 when its git can run a sparse
// checkout, read fresh; it records what it decided about close ops so the
// connected re-check can tell a change.
func (client *HubClient) spawnHelloCapabilities(ctx context.Context) []string {
	ready := client.closeOpsReady(ctx)
	client.closeOpsAdvertised.Store(ready)
	capabilities := slices.Clone(hubSpawnCapabilities)
	if ready {
		capabilities = append(capabilities, hubCapabilityCloseOps, hubCapabilityHandoverCheck, hubCapabilityCloseMissing, hubCapabilityHandoverSnapshot)
	}
	if client.checkoutSparseReady(ctx) {
		capabilities = append(capabilities, hubCapabilityCheckoutSparse)
	}
	return capabilities
}

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
	// nodeCloseOpMismatch: the job's ledger does not bind its current attempt
	// to this pane and label, or the attempt has ended; no operation was
	// opened and nothing closed.
	nodeCloseOpMismatch = "mismatch"
	// nodeCloseOpSuperseded proves an operation's attempt is no longer current;
	// its journal retains the original outcome.
	nodeCloseOpSuperseded = "superseded"
	// nodeCloseIntentCancelled proves the request's close intent ended; it
	// does not prove the pane or its watcher can no longer write to hk.
	nodeCloseIntentCancelled = "intent_cancelled"
)

type nodePaneCloseOpArgs struct {
	RequestID string `json:"request_id"`
	Job       string `json:"job"`
	Epoch     uint64 `json:"epoch"`
	// Attempt is the job attempt the hub decided to close: the opaque token
	// pane.close_status reported for the job when it decided. The node acts
	// only while it is still the job's current attempt.
	Attempt      string `json:"attempt"`
	PaneID       string `json:"pane_id"`
	Name         string `json:"name"`
	AgentStatus  string `json:"agent_status"`
	Cwd          string `json:"cwd"`
	AllowUnnamed bool   `json:"allow_unnamed"`
	// EndReason is the hk end_reason of the lost record that ends the job
	// once the close is final (hk 4652): recorded in the job's ledger as a
	// hub-close intent before the operation is registered, so the job's
	// watcher writes no competing record and finishes with exactly this one.
	EndReason string `json:"end_reason"`
	// Handover requires the old worktree still to match the saved checkout.
	Handover *nodeHandoverCloseGuard `json:"handover,omitempty"`
}

type nodeHandoverCloseGuard struct {
	Origin string `json:"origin"`
	Branch string `json:"branch"`
	Commit string `json:"commit"`
}

// nodePaneCloseOpResult.Outcome is a final fleet.CloseOp state, or
// nodeCaptureNotAJob (no job directory) or nodeCloseOpMismatch; for those two
// no journal entry exists and nothing was closed.
type nodePaneCloseOpResult struct {
	Outcome  string `json:"outcome"`
	Replayed bool   `json:"replayed"`
	Reason   string `json:"reason,omitempty"`
}

type nodeCloseOpRefArgs struct {
	RequestID string `json:"request_id"`
	Job       string `json:"job"`
	// Ack, on pane.close_status only, is the hub's acknowledgement that it
	// stored the intent_cancelled answer (hk 5073): the node records it
	// durably, which lets a rollback drain finish. The hub sends it only
	// after its guarded action is saved.
	Ack bool `json:"ack,omitempty"`
	// Epoch, on pane.close_status only, is the ownership epoch of the hub's
	// action (hk 5296). It asks the node for the missing-job answer when the
	// job directory is gone; the hub sends it only to a node that advertises
	// close_missing_v1, since an older node refuses an unknown argument.
	Epoch uint64 `json:"epoch,omitempty"`
}

// nodeCloseOpReleaseArgs is pane.close_release's decoded arguments, a distinct
// type so the dispatcher can tell release from status.
type nodeCloseOpReleaseArgs nodeCloseOpRefArgs

type nodeCloseOpStatusResult struct {
	State    string `json:"state"`
	Released bool   `json:"released"`
	// Settled certifies the synced final job.lost for this request, not just
	// the hk write that can precede it (docs/node-commands.md).
	Settled bool `json:"settled"`
	// Attempt is the job's current attempt token, the value a pane.close_op
	// for it must name; empty when the job has no readable attempt.
	Attempt string `json:"attempt"`
	Reason  string `json:"reason,omitempty"`
	// MissingEpoch is the missing-job proof (hk 5296): the job's directory is
	// gone, and the node holds a durable tombstone for this request that
	// fences it, so a delayed pane.close_op under the id can never act on a
	// directory recreated later. It is the epoch the request was issued at.
	// Only a status asked with an epoch is answered with it; zero is no
	// proof, which is also what an older node answers.
	MissingEpoch uint64 `json:"missing_epoch,omitempty"`
}

func validNodePaneCloseOpArgs(args nodePaneCloseOpArgs) bool {
	guard := args.Handover
	return fleet.ValidCloseOpID(args.RequestID) && fleet.ValidJobID(args.Job) && args.Epoch > 0 && validNodeAttemptToken(args.Attempt) && nodeCommandPaneIDPattern.MatchString(args.PaneID) && validNodePaneName(args.Name) && nodePaneClosableStatus[args.AgentStatus] && validNodeCommandPath(args.Cwd) && validNodeEndReason(args.EndReason) && args.EndReason != "" && (guard == nil || guard.Origin != "" && validNodeRefName(guard.Branch) && nodeShaPattern.MatchString(guard.Commit))
}

func validHandoverCloseReason(reason string) bool {
	switch reason {
	case "", handoverDirty, handoverUnpushed, handoverBranchMismatch, handoverGitFailed:
		return true
	}
	return false
}

// validNodeAttemptToken bounds an attempt token: a ledger event file name.
func validNodeAttemptToken(token string) bool { return fleet.ValidJobID(token) }

func validNodeCloseOpRefArgs(args nodeCloseOpRefArgs) bool {
	return fleet.ValidCloseOpID(args.RequestID) && fleet.ValidJobID(args.Job)
}

func validNodePaneCloseOpResult(result nodePaneCloseOpResult) bool {
	if !validHandoverCloseReason(result.Reason) || result.Reason != "" && result.Outcome != fleet.CloseOpRefused {
		return false
	}
	switch result.Outcome {
	case fleet.CloseOpClosed, fleet.CloseOpRefused, fleet.CloseOpGone, fleet.CloseOpUnknown, fleet.CloseOpCancelled, fleet.CloseOpPreempted:
		return true
	case nodeCaptureNotAJob, nodeCloseOpMismatch:
		return !result.Replayed
	}
	return false
}

func validNodeCloseOpStatusResult(result nodeCloseOpStatusResult) bool {
	if result.MissingEpoch != 0 && (result.State != nodeCloseOpNone || result.Attempt != "" || result.Released || result.Settled || result.Reason != "") {
		return false
	}
	if !validHandoverCloseReason(result.Reason) || result.Reason != "" && result.State != fleet.CloseOpRefused {
		return false
	}
	if result.Attempt != "" && !validNodeAttemptToken(result.Attempt) {
		return false
	}
	if result.Settled && result.State != fleet.CloseOpClosed && result.State != fleet.CloseOpGone && result.State != fleet.CloseOpUnknown {
		return false
	}
	switch result.State {
	case nodeCloseOpNone, fleet.CloseOpRegistered, fleet.CloseOpClosing:
		// Only a final operation can be released.
		return !result.Released
	case fleet.CloseOpCancelled:
		return result.Released
	case nodeCloseOpSuperseded, nodeCloseIntentCancelled:
		return result.Attempt != ""
	case fleet.CloseOpClosed, fleet.CloseOpRefused, fleet.CloseOpGone, fleet.CloseOpUnknown, fleet.CloseOpPreempted:
		return true
	}
	return false
}

// openCloseOpJob opens the job directory a close operation is journalled in.
func (client *HubClient) openCloseOpJob(job string) (*os.Root, bool, string) {
	root, _, found, code := client.openCloseOpJobDir(job)
	return root, found, code
}

func (client *HubClient) openCloseOpJobDir(job string) (*os.Root, string, bool, string) {
	root, dir, found, err := client.openNodeJobDir(job)
	switch {
	case errors.Is(err, errNodeJobsRootUnset):
		return nil, "", false, "jobs_root_unset"
	case err != nil:
		return nil, "", false, "journal_failed"
	}
	return root, dir, found, ""
}

// nodePaneCloseOp runs one close operation. The caller holds nodeCommandMu.
//
// The journal moves registered → closing → final. registered is written,
// against the job's current attempt, before any herdr read. closing is written after the first guarded look and the settle, before the
// final look that precedes `herdr pane close`, so a slow journal write never
// sits between that look and the close. A pane absent before closing was
// recorded is gone; an operation found closing on replay is unknown, without
// touching herdr: its close may have run, and a pane under the same id now may
// be another one.
func (client *HubClient) nodePaneCloseOp(ctx context.Context, args nodePaneCloseOpArgs) (any, string) {
	client.assignmentMu.Lock()
	epoch := client.assignedJobs[args.Job]
	client.assignmentMu.Unlock()
	if client.jobsInboxRoot != "" {
		stored, err := jobowner.Epoch(filepath.Join(client.jobsInboxRoot, "jobs"), args.Job)
		if err != nil {
			return nil, "epoch_mismatch"
		}
		if stored > epoch {
			epoch = stored
		}
	}
	if epoch > args.Epoch {
		return nil, "epoch_mismatch"
	}
	if client.jobsInboxRoot != "" {
		unlock, err := fleet.LockCloseOpsAdmission(ctx, filepath.Join(client.jobsInboxRoot, "jobs"))
		if err != nil {
			if errors.Is(err, fleet.ErrCloseOpsDraining) {
				return nil, "close_ops_not_ready"
			}
			return nil, "journal_failed"
		}
		defer unlock()
	}
	needsProof := epoch != args.Epoch && !(epoch == 0 && args.Epoch == 1)
	result, code := client.runPaneCloseOp(ctx, args, needsProof)
	// A close that is now final (or was already, on a replay) is settled by
	// the node at once, inside the command's own deadline; a failed command or
	// step is left to runHubCloseSettle.
	if code == "" {
		step, cancel := context.WithTimeout(ctx, hubCloseCommandSettleBudget)
		client.settleHubClose(step, args.Job)
		cancel()
	}
	return result, code
}

func (client *HubClient) runPaneCloseOp(ctx context.Context, args nodePaneCloseOpArgs, requireEpochProof bool) (any, string) {
	if fenced, code := client.missingJobFence(args); code != "" || fenced {
		if code != "" {
			return nil, code
		}
		// The request was fenced while its job directory was gone (hk 5296):
		// whatever stands under the job's name now belongs to a later attempt,
		// and a replay of a released request touches no pane.
		return nodePaneCloseOpResult{Outcome: fleet.CloseOpCancelled, Replayed: true}, ""
	}
	job, jobDir, found, code := client.openCloseOpJobDir(args.Job)
	if code != "" {
		return nil, code
	}
	if !found {
		return nodePaneCloseOpResult{Outcome: nodeCaptureNotAJob}, ""
	}
	defer job.Close()
	events, err := readNodeJobLedgerEvents(job)
	if err != nil {
		return nil, "journal_failed"
	}
	for _, event := range events {
		if event.hubRevokedEpoch >= args.Epoch {
			return nil, "epoch_mismatch"
		}
	}
	op, found, err := fleet.ReadCloseOp(job, args.RequestID)
	if err != nil || (found && fleet.SyncCloseOps(job) != nil) {
		return nil, "journal_failed"
	}
	if requireEpochProof && (!found || op.Epoch != args.Epoch) {
		// A node restart loses its in-memory assignment. A synced, still-open
		// intent can recover the epoch if the node crashed before registering
		// the journal entry. A new request still waits for assignment.
		open := false
		for _, event := range events {
			switch {
			case nodeJobClaimKind(event.kind), event.kind == "job.spawned", event.kind == "job.reprompted":
				open = false
			case event.kind == "job.hub_close_intent" && event.request == args.RequestID && event.attempt == args.Attempt && event.pane == args.PaneID && event.label == args.Name && event.closeIntentEpoch == args.Epoch:
				open = true
			case event.kind == "job.hub_close_cancelled" && event.request == args.RequestID:
				open = false
			case event.kind == "job.lost" && event.reason == "hub_close" && event.final:
				open = false
			}
		}
		if !open {
			return nil, "epoch_mismatch"
		}
		dir, err := job.Open("events")
		if err != nil {
			return nil, "journal_failed"
		}
		err = dir.Sync()
		dir.Close()
		if err != nil {
			return nil, "journal_failed"
		}
	}
	switch {
	// A repeat names the pane and attempt it was registered for, before any
	// replay or settlement: another attempt's outcome is never returned.
	case found && (op.PaneID != args.PaneID || op.Attempt != args.Attempt || op.Epoch != args.Epoch && !(op.Epoch == 0 && op.State == fleet.CloseOpCancelled)):
		return nil, "invalid_args"
	case found && op.Final():
		return nodePaneCloseOpResult{Outcome: op.State, Replayed: true, Reason: op.Reason}, ""
	case found && op.State == fleet.CloseOpClosing:
		return client.finishCloseOp(job, op, fleet.CloseOpUnknown)
	}
	// Registering, or resuming one left registered: bind the request to the
	// job's current attempt before any herdr call.
	op, target, outcome, code := client.bindCloseOp(ctx, job, jobDir, op, found, args)
	if code != "" {
		return nil, code
	}
	if outcome != "" {
		return nodePaneCloseOpResult{Outcome: outcome}, ""
	}
	closeArgs := nodePaneCloseArgs{PaneID: args.PaneID, Name: args.Name, AgentStatus: args.AgentStatus, Cwd: args.Cwd, AllowUnnamed: args.AllowUnnamed, target: target}
	if target.role == "implementer" {
		// An idle implementer may be waiting on an ESC answer: the hub's
		// hand-over close keeps the open-question guard pane.close has
		// (hk 4633).
		closeArgs.QuestionJob = args.Job
	}
	var prepared nodeHandoverPrepared
	checkEpoch := func() error {
		if client.jobsInboxRoot != "" {
			epoch, err := jobowner.Epoch(filepath.Join(client.jobsInboxRoot, "jobs"), args.Job)
			if err != nil || epoch > args.Epoch {
				return nodeCloseHookStop{code: "epoch_mismatch"}
			}
		}
		return nil
	}
	result, code := client.nodePaneCloseWith(ctx, closeArgs, func() error {
		if err := checkEpoch(); err != nil {
			return err
		}
		// The proof is read again after the settle: the job's watcher, a
		// separate process, may have recorded a re-prompt or an end of the
		// attempt meanwhile.
		again, err := readNodeCloseTarget(job)
		if err != nil {
			return err
		}
		if bound, code := client.closeTargetBound(ctx, args, again, op.Attempt); code != "" {
			return nodeCloseHookStop{code: code}
		} else if !bound {
			op.State, op.Updated = fleet.CloseOpPreempted, client.nowUTC()
			if err := fleet.WriteCloseOp(job, op); err != nil {
				return err
			}
			return nodeCloseHookStop{outcome: fleet.CloseOpPreempted}
		}
		if guard := args.Handover; guard != nil {
			if again.role != "implementer" {
				op.Reason = handoverGitFailed
				return nodeCloseHookStop{outcome: fleet.CloseOpRefused}
			}
			var reason string
			prepared, reason = client.handoverPrepare(ctx, nodeHandoverCheckArgs{Job: args.Job, Cwd: args.Cwd, Origin: guard.Origin, Branch: guard.Branch}, &again)
			if reason == "" && prepared.tip != guard.Commit {
				reason = handoverUnpushed
			}
			if reason != "" {
				op.Reason = reason
				return nodeCloseHookStop{outcome: fleet.CloseOpRefused}
			}
		}
		op.State, op.Updated = fleet.CloseOpClosing, client.nowUTC()
		return fleet.WriteCloseOp(job, op)
	}, func() error {
		if args.Handover != nil {
			localCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			check := client.handoverCheckLocal(localCtx, prepared)
			cancel()
			if check.Outcome != handoverCheckOK {
				op.Reason = check.Outcome
				return nodeCloseHookStop{outcome: fleet.CloseOpRefused}
			}
		}
		if err := checkEpoch(); err != nil {
			op.State, op.Updated = fleet.CloseOpPreempted, client.nowUTC()
			if writeErr := fleet.WriteCloseOp(job, op); writeErr != nil {
				return writeErr
			}
			return err
		}
		return nil
	})
	if code != "" {
		// The journal retains its latest state; a repeat of this request id
		// settles or replays it.
		return nil, code
	}
	switch result.Outcome {
	case fleet.CloseOpPreempted:
		return nodePaneCloseOpResult{Outcome: fleet.CloseOpPreempted}, ""
	case nodePaneClosed:
		return client.finishCloseOp(job, op, fleet.CloseOpClosed)
	case nodePaneGone:
		return client.finishCloseOp(job, op, fleet.CloseOpGone)
	default:
		return client.finishCloseOp(job, op, fleet.CloseOpRefused)
	}
}

// nodeCloseTarget is the one proof a close operation acts on, read from the
// job's own ledger in ledger (file-name) order: the current attempt, the
// spawn receipt belonging to it, and what that receipt names. A claim or
// reclaim starts an attempt with no receipt until its own job.spawned; a
// re-prompt starts an attempt on the same receipt. Any end of the attempt —
// completion, join, revoke, hand-off, escalation, reap, or a loss other than
// a quota stop announced for the hub's hand-over — marks it ended.
// nodeCloseHookStop is a close hook's decision not to close: answered as the
// outcome, or as the error code when one is set.
type nodeCloseHookStop struct{ outcome, code string }

func (stop nodeCloseHookStop) Error() string { return "close stopped: " + stop.outcome }

type nodeCloseTarget struct {
	attempt                         string
	receipt                         bool
	pane, label, role, session, cwd string
	ended                           bool
	// announced: the attempt's newest end-like event is a quota stop
	// announced for the hub's hand-over, or a reviewer's quota-stop intent
	// is still open; whether the job already ended is in hk, not the ledger.
	announced, intent bool
	// hubClose: a hub-close intent was recorded on this attempt; it keeps
	// intent set for the rest of the attempt.
	hubClose bool
}

func readNodeCloseTarget(job *os.Root) (nodeCloseTarget, error) {
	events, err := readNodeJobLedgerEvents(job)
	if err != nil {
		return nodeCloseTarget{}, err
	}
	var target nodeCloseTarget
	for _, event := range events {
		switch {
		case nodeJobClaimKind(event.kind):
			target = nodeCloseTarget{attempt: event.name, role: event.role}
		case event.kind == "job.spawned":
			target.attempt, target.receipt, target.ended, target.announced, target.intent, target.hubClose = event.name, true, false, false, false, false
			target.pane, target.label, target.session, target.cwd = event.pane, event.label, event.session, event.cwd
			if event.role != "" {
				target.role = event.role
			}
		case event.kind == "job.reprompted":
			target.attempt, target.ended, target.announced, target.intent, target.hubClose = event.name, false, false, false, false
		case event.kind == "job.hub_close_intent":
			// A hub close was opened on this attempt: whether something else
			// already ended the job in hk (the watcher then keeps that record)
			// is in hk, not the ledger, so every later close asks hk.
			target.intent, target.hubClose = true, true
		case event.kind == "job.quota_stop_intent":
			// The watcher writes hk lost before its final job.lost: a crash
			// between the two leaves the intent open over an ended job.
			target.intent = true
		case event.kind == "job.quota_stop_cancelled":
			// "superseded": hk already held another terminal record, and the
			// watcher ended without a local terminal event. That is an end.
			if event.reason == "superseded" {
				target.ended = true
			}
			target.intent = target.hubClose
		case event.kind == "job.lost":
			if event.reason == "quota_stop" && !event.final {
				target.announced = true
			} else {
				target.ended = true
			}
		case event.kind == "job.completed", event.kind == "job.completion", event.kind == "job.record_settled",
			event.kind == "job.joined", event.kind == "job.revoked",
			event.kind == "job.handed_off", event.kind == "job.escalate", event.kind == "job.reaped":
			target.ended = true
		}
	}
	return target, nil
}

// closeTargetBound reports whether target still binds attempt to the
// requested pane and label and has not ended. For an attempt whose quota stop
// was announced for the hub's hand-over, the hk record decides whether the
// hand-over grace already ended the job: a terminal record is an end, and an
// unreadable one answers record_unavailable rather than guessing.
func (client *HubClient) closeTargetBound(ctx context.Context, args nodePaneCloseOpArgs, target nodeCloseTarget, attempt string) (bool, string) {
	// A receipt with no recorded session cannot say where the pane lives.
	if !target.receipt || target.ended || target.session == "" || target.pane != args.PaneID || target.label != args.Name || target.attempt != attempt {
		return false, ""
	}
	if !target.announced && !target.intent {
		return true, ""
	}
	store, err := client.nodeJobStoreForCommand()
	if err != nil {
		return false, "record_unavailable"
	}
	record, err := store.Get(ctx, args.Job)
	if err != nil {
		return false, "record_unavailable"
	}
	switch record["state"] {
	case "completed", "lost", "escalated":
		return false, ""
	}
	return true, ""
}

// bindCloseOp checks the job's ledger proof (readNodeCloseTarget): its
// current attempt has its own receipt naming the requested pane and label,
// and has not ended. A new request that fails opens nothing (mismatch). A
// resumed registered operation whose attempt moved on or ended is settled
// preempted without touching herdr. Otherwise the operation is (still)
// registered and the proof is returned for the guarded close. The caller
// holds nodeCommandMu.
func (client *HubClient) bindCloseOp(ctx context.Context, job *os.Root, jobDir string, op fleet.CloseOp, resuming bool, args nodePaneCloseOpArgs) (fleet.CloseOp, *nodeCloseTarget, string, string) {
	// The job's end lock, which its watcher takes around every terminal
	// decision, spans the binding check and the intent: no watcher end can
	// land between them (hk 4652).
	unlock, err := client.hubCloseLedger().LockEnd(ctx, jobDir)
	if err != nil {
		return op, nil, "", "journal_failed"
	}
	defer unlock()
	return client.bindCloseOpLocked(ctx, job, jobDir, op, resuming, args)
}

func (client *HubClient) bindCloseOpLocked(ctx context.Context, job *os.Root, jobDir string, op fleet.CloseOp, resuming bool, args nodePaneCloseOpArgs) (fleet.CloseOp, *nodeCloseTarget, string, string) {
	target, err := readNodeCloseTarget(job)
	if err != nil {
		return op, nil, "", "journal_failed"
	}
	// The attempt the hub decided on; a resumed operation keeps the one it
	// was registered for, which a repeat must name.
	attempt := args.Attempt
	if resuming && op.Attempt != args.Attempt {
		return op, nil, "", "invalid_args"
	}
	bound, code := client.closeTargetBound(ctx, args, target, attempt)
	if code != "" {
		return op, nil, "", code
	}
	now := client.nowUTC()
	switch {
	case resuming && !bound:
		op.State, op.Updated = fleet.CloseOpPreempted, now
		if fleet.WriteCloseOp(job, op) != nil {
			return op, nil, "", "journal_failed"
		}
		return op, nil, op.State, ""
	case resuming:
		// A resumed operation closes only while no older-binary watcher,
		// which would not defer, runs for the job.
		if code, _ := client.admitHubCloseWatcher(ctx, args.Job, jobDir, true); code != "" {
			return op, nil, "", code
		}
		return op, &target, "", ""
	case !bound:
		return op, nil, nodeCloseOpMismatch, ""
	}
	// One open operation per attempt: a second request id while another is
	// registered or closing would leave two intents the watcher must settle.
	if open, err := fleet.OpenCloseOps(job, attempt); err != nil {
		return op, nil, "", "journal_failed"
	} else if len(open) > 0 {
		return op, nil, "", "close_in_progress"
	}
	// A watcher of an older binary would not defer to the intent. No
	// watcher at all is fine: the node itself settles the close.
	code, fenced := client.admitHubCloseWatcher(ctx, args.Job, jobDir, false)
	if code != "" {
		return op, nil, "", code
	}
	// The intent precedes the registration: a crash between the two leaves
	// an intent with no operation, which the watcher defers to until the
	// hub's repeat registers it or its release cancels it.
	if client.hubCloseLedger().RecordIntent(ctx, args.Job, fleet.HubCloseIntent{RequestID: args.RequestID, Attempt: attempt, Epoch: args.Epoch, Pane: args.PaneID, Label: args.Name, EndReason: args.EndReason}) != nil {
		// The write may have failed after the intent became visible (its
		// directory sync), and an older intent may be open: under the end
		// lock admission still holds, the fence is released only when the
		// ledger shows none open (#662 review round 3).
		if open, live, err := client.hubCloseLedger().FenceState(args.Job); fenced && err == nil && !open {
			client.releaseWatcherFence(args.Job)
			if live {
				// A watcher started since admission met the fence and exited.
				client.rearmAfterFence(args.Job)
			}
		}
		return op, nil, "", "journal_failed"
	}
	op = fleet.CloseOp{RequestID: args.RequestID, PaneID: args.PaneID, Epoch: args.Epoch, Attempt: attempt, State: fleet.CloseOpRegistered, Created: now, Updated: now}
	if fleet.WriteCloseOp(job, op) != nil {
		return op, nil, "", "journal_failed"
	}
	return op, &target, "", ""
}

// nodeHubCloseLedger is what a hub close needs from the job's ledger and
// watcher (internal/fleet/watch_hub_close.go).
type nodeHubCloseLedger interface {
	// LockEnd takes the job's end lock, shared with its watcher's terminal
	// decisions.
	// decisions. It waits at most until ctx ends, so a close command never
	// holds the node's acting lock past its own deadline.
	LockEnd(ctx context.Context, jobDir string) (func(), error)
	Watcher(jobDir string) (fleet.HubCloseWatcher, error)
	// Fence takes the job's watch lock for the node (fleet.FenceWatchers).
	Fence(jobDir string) (release func(), ok bool, err error)
	// Borrow receives an aware watcher's own locked watch-lock description
	// (fleet.BorrowWatchLock), which keeps the lock held after it exits.
	Borrow(ctx context.Context, jobDir string) (release func(), err error)
	// FenceState reads whether any hub-close intent of the job is open and
	// whether its attempt is live (fleet.HubCloseFenceState). The caller
	// holds the job's end lock.
	FenceState(job string) (open, live bool, err error)
	// CloseOpsReady is the node-wide precondition: the migration barrier
	// was recorded and no older watcher or launcher runs (fleet.CloseOpsReady).
	CloseOpsReady(ctx context.Context) bool
	// SyncEvents makes the ledger's observed event names durable.
	SyncEvents(job *os.Root) error
	RecordIntent(ctx context.Context, job string, in fleet.HubCloseIntent) error
	// CancelIntent closes the intent of a request the hub released before
	// the node registered it.
	CancelIntent(ctx context.Context, job, requestID string) error
	// AckIntentCancel records, durably, the hub's acknowledgement of the
	// request's intent_cancelled answer (fleet.RecordHubCloseAck). The
	// caller holds the job's end lock.
	AckIntentCancel(ctx context.Context, job, requestID string) error
	// Settle runs one step of the node's settlement of the job's hub close
	// (fleet.HubCloseSettlement).
	Settle(ctx context.Context, job string) (fleet.HubCloseState, error)
}

// nodeFleetHubClose is the production ledger: the node's jobs root, its own
// hk JobStore and its herdr runner.
type nodeFleetHubClose struct {
	jobs   string
	client *HubClient
}

func (l nodeFleetHubClose) LockEnd(ctx context.Context, jobDir string) (func(), error) {
	return fleet.LockJobEndContext(ctx, jobDir)
}
func (l nodeFleetHubClose) CloseOpsReady(ctx context.Context) bool {
	return fleet.CloseOpsReady(ctx, l.jobs, nil)
}

func syncNodeCloseEvents(job *os.Root) error {
	dir, err := job.Open("events")
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (l nodeFleetHubClose) SyncEvents(job *os.Root) error {
	return syncNodeCloseEvents(job)
}

func (l nodeFleetHubClose) Fence(jobDir string) (func(), bool, error) {
	return fleet.FenceWatchers(jobDir)
}

func (l nodeFleetHubClose) Borrow(ctx context.Context, jobDir string) (func(), error) {
	return fleet.BorrowWatchLock(ctx, jobDir)
}

func (l nodeFleetHubClose) FenceState(job string) (bool, bool, error) {
	return fleet.HubCloseFenceState(l.jobs, job)
}

func (l nodeFleetHubClose) Watcher(jobDir string) (fleet.HubCloseWatcher, error) {
	return fleet.ProbeHubCloseWatcher(jobDir)
}
func (l nodeFleetHubClose) RecordIntent(ctx context.Context, job string, in fleet.HubCloseIntent) error {
	return fleet.RecordHubCloseIntent(ctx, l.jobs, job, in)
}
func (l nodeFleetHubClose) CancelIntent(ctx context.Context, job, requestID string) error {
	return fleet.RecordHubCloseCancel(ctx, l.jobs, job, requestID, "released")
}

func (l nodeFleetHubClose) AckIntentCancel(ctx context.Context, job, requestID string) error {
	return fleet.RecordHubCloseAck(ctx, l.jobs, job, requestID)
}

// Settle settles with the node's own hk JobStore (spawn.json
// handoffkeep_env, nodeJobStoreForCommand) and reads the pane inventory in
// the receipt's herdr session with the node's runner. The lease release and
// the final job.lost are local ledger work.
func (l nodeFleetHubClose) Settle(ctx context.Context, job string) (fleet.HubCloseState, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return fleet.HubClosePending, err
	}
	listed := func(ctx context.Context, session, pane string) (bool, error) {
		out, err := l.client.nodeHerdrRunnerForSession(session)(ctx, "pane", "list")
		if err != nil {
			return false, err
		}
		return fleet.PaneListedIn(out, pane)
	}
	// The hk store is resolved only by a step that reads or writes hk: a
	// close that closed nothing is cancelled locally even when the node has
	// no handoffkeep_env.
	settlement := fleet.NewHubCloseSettlement(l.jobs, home, l.client.nodeJobStoreForCommand, l.client.nowUTC, listed, slogWarnWriter{})
	settlement.App.JobOwnerNode = l.client.machineID
	settlement.App.JobFenceCapability = l.client.noteJobFenceSupport
	return settlement.Settle(ctx, job)
}

func (client *HubClient) hubCloseLedger() nodeHubCloseLedger {
	if client.nodeHubClose != nil {
		return client.nodeHubClose
	}
	return nodeFleetHubClose{jobs: filepath.Join(client.jobsInboxRoot, "jobs"), client: client}
}

// admitHubCloseWatcher refuses a new close while the node is not ready for
// close ops (close_ops_not_ready: the barrier is not recorded, or an older
// watcher runs on the host); an accepted operation that resumes is not held
// to readiness again, so it can still finish. Every close is refused while
// the job's watcher is one that does not defer (watcher_incompatible: the
// watch lock is held by anything but an aware watcher). Both are "not
// closed, retry later". A job with no watcher is fenced (took true): the
// node holds its watch lock while any hub-close intent of the job is open
// (reconcileWatcherFence), so no watcher of any binary, from any launcher,
// runs meanwhile. The caller holds the job's end lock.
func (client *HubClient) admitHubCloseWatcher(ctx context.Context, job, jobDir string, resuming bool) (code string, took bool) {
	if !resuming && !client.closeOpsReady(ctx) {
		return "close_ops_not_ready", false
	}
	if client.holdsWatcherFence(job) {
		return "", false
	}
	watcher, err := client.hubCloseLedger().Watcher(jobDir)
	switch {
	case err != nil:
		return "journal_failed", false
	case watcher == fleet.HubCloseWatcherIncompatible:
		return "watcher_incompatible", false
	case watcher == fleet.HubCloseWatcherAware:
		// The node holds the watcher's own lock description for the close,
		// so the watcher exiting cannot free the lock (#662 consult NCxM).
		if !client.borrowWatcherFence(ctx, job, jobDir) {
			return "watcher_incompatible", false
		}
		return "", true
	}
	if code := client.takeWatcherFence(job, jobDir); code != "" {
		return code, false
	}
	return "", true
}

// borrowWatcherFence keeps an aware watcher's lent watch lock as the job's
// fence; false when the watcher did not lend it (retry later).
func (client *HubClient) borrowWatcherFence(ctx context.Context, job, jobDir string) bool {
	release, err := client.hubCloseLedger().Borrow(ctx, jobDir)
	if err != nil {
		return false
	}
	client.storeWatcherFence(job, release)
	return true
}

// takeWatcherFence takes and keeps the job's watch lock for the node; a
// watcher that started since the probe answers watcher_incompatible (retry).
func (client *HubClient) takeWatcherFence(job, jobDir string) string {
	release, ok, err := client.hubCloseLedger().Fence(jobDir)
	switch {
	case err != nil:
		return "journal_failed"
	case !ok:
		return "watcher_incompatible"
	}
	client.storeWatcherFence(job, release)
	return ""
}

func (client *HubClient) storeWatcherFence(job string, release func()) {
	client.closeFenceMu.Lock()
	defer client.closeFenceMu.Unlock()
	if client.closeFences == nil {
		client.closeFences = map[string]func(){}
	}
	delete(client.closeRearmed, job)
	if old := client.closeFences[job]; old != nil {
		old()
	}
	client.closeFences[job] = release
}

func (client *HubClient) holdsWatcherFence(job string) bool {
	client.closeFenceMu.Lock()
	defer client.closeFenceMu.Unlock()
	return client.closeFences[job] != nil
}

func (client *HubClient) releaseWatcherFence(job string) {
	client.closeFenceMu.Lock()
	defer client.closeFenceMu.Unlock()
	if release := client.closeFences[job]; release != nil {
		release()
		delete(client.closeFences, job)
	}
}

func (client *HubClient) markWatcherRearmed(job string) {
	client.closeFenceMu.Lock()
	defer client.closeFenceMu.Unlock()
	if client.closeRearmed == nil {
		client.closeRearmed = map[string]bool{}
	}
	client.closeRearmed[job] = true
}

func (client *HubClient) isWatcherRearmed(job string) bool {
	client.closeFenceMu.Lock()
	defer client.closeFenceMu.Unlock()
	return client.closeRearmed != nil && client.closeRearmed[job]
}

func (client *HubClient) clearWatcherRearmed(job string) {
	client.closeFenceMu.Lock()
	defer client.closeFenceMu.Unlock()
	delete(client.closeRearmed, job)
}

// settleHubClose drives one job's hub close one step and reports the step's
// state. The daemon pass counts a done edge in the ledger's hub_close_settle
// flow only for a terminal state: a pending state with a nil error (an
// unreadable close operation waits) is still open, and counting it done
// would hide a stuck close behind balanced made and done. The fence is
// reconciled under the job's end lock before the step (a step runs only while
// no watcher that ignores the close can run) and after it (released once no
// intent is open).
func (client *HubClient) settleHubClose(ctx context.Context, job string) fleet.HubCloseState {
	if !client.reconcileWatcherFence(ctx, job) {
		slog.Warn("hub close settlement waits: the job's watch lock is held by a watcher that does not defer, or unreadable", "job", job)
		return fleet.HubClosePending
	}
	state, err := client.hubCloseLedger().Settle(ctx, job)
	if err != nil {
		slog.Warn("hub close settlement pending", "job", job, "error", err)
		state = fleet.HubClosePending
	}
	client.reconcileWatcherFence(ctx, job)
	return state
}

// hubCloseStateIsTerminal reports whether a HubCloseState is terminal: only
// terminal states count as a done edge in the hub_close_settle flow. A
// pending state (even with a nil error) means settlement is still open.
func hubCloseStateIsTerminal(state fleet.HubCloseState) bool {
	switch state {
	case fleet.HubCloseNone, fleet.HubCloseSettled, fleet.HubCloseCancelled:
		return true
	default:
		return false
	}
}

// reconcileWatcherFence makes the node's fence on the job's watch lock match
// its ledger, under the job's end lock, which admission also holds: so a
// release can never drop the fence of a close admitted meanwhile. While any
// intent is open the node holds the lock: its own when no watcher runs, or
// the aware watcher's lent description (so that watcher exiting cannot free
// it); after a node restart it is taken or borrowed again before settling. Once none
// is open it is released, and a live attempt gets its watcher re-armed: a
// watcher that met the fence exited. safe reports that no watcher which
// ignores hub closes can run for the job now.
func (client *HubClient) reconcileWatcherFence(ctx context.Context, job string) (safe bool) {
	ledger := client.hubCloseLedger()
	dir := filepath.Join(client.jobsInboxRoot, "jobs", job)
	unlock, err := ledger.LockEnd(ctx, dir)
	if err != nil {
		return false
	}
	rearm := false
	defer func() {
		unlock()
		if rearm {
			client.rearmAfterFence(job)
		}
	}()
	open, live, err := ledger.FenceState(job)
	if err != nil {
		return client.holdsWatcherFence(job)
	}
	if open {
		client.clearWatcherRearmed(job)
	}
	held := client.holdsWatcherFence(job)
	if !open {
		if held {
			client.releaseWatcherFence(job)
			rearm = live
		} else if watcher, err := ledger.Watcher(dir); err == nil && watcher == fleet.HubCloseWatcherNone && !client.isWatcherRearmed(job) {
			rearm = live
		}
		return true
	}
	if held {
		return true
	}
	switch watcher, err := ledger.Watcher(dir); {
	case err != nil:
		return false
	case watcher == fleet.HubCloseWatcherAware:
		return client.borrowWatcherFence(ctx, job, dir)
	case watcher == fleet.HubCloseWatcherNone:
		return client.takeWatcherFence(job, dir) == ""
	}
	return false
}

// rearmAfterFence starts the watchers of live jobs that have none, after a
// fence that may have turned one away was released.
func (client *HubClient) rearmAfterFence(job string) {
	client.markWatcherRearmed(job)
	if client.fenceRearm != nil {
		client.fenceRearm(job)
		return
	}
	go client.rearmWatchers()
}

// fencedJobs lists the jobs whose watch lock the node holds.
func (client *HubClient) fencedJobs() []string {
	client.closeFenceMu.Lock()
	defer client.closeFenceMu.Unlock()
	return slices.Collect(maps.Keys(client.closeFences))
}

// hubCloseSettleInterval paces the daemon's retry of unfinished hub-close
// settlements; the first pass runs at node start.
const hubCloseSettleInterval = 30 * time.Second

// Bound concurrent job steps so one held job lock cannot stall other jobs.
const hubCloseSettleConcurrency = 4

// runHubCloseSettle settles, at node start and then on every interval, every
// job whose ledger holds an open hub-close intent: an accepted close is
// finished by the node even when no hub command and no watcher come back.
func (client *HubClient) runHubCloseSettle(ctx context.Context) {
	if client.jobsInboxRoot == "" || ctx.Err() != nil {
		return
	}
	jobsRoot := filepath.Join(client.jobsInboxRoot, "jobs")
	recoveryCtx, stopRecovery := context.WithCancel(ctx)
	recoveryDone := make(chan struct{})
	go func() {
		defer close(recoveryDone)
		for !client.closeOpsRecovered.Load() {
			err := fleet.RecoverLegacyCloseOps(recoveryCtx, jobsRoot, client.nowUTC())
			if err == nil {
				client.closeOpsRecovered.Store(true)
				return
			}
			if recoveryCtx.Err() != nil {
				return
			}
			slog.Warn("hub close legacy recovery waits", "error", err)
			client.nodeLoopNoteError(nodeHubCloseSettle)
			select {
			case <-recoveryCtx.Done():
				return
			case <-time.After(hubCloseSettleInterval):
			}
		}
	}()
	defer func() { stopRecovery(); <-recoveryDone }()
	// open is the jobs the last full pass found with an open intent; the
	// fence is reconciled for them, and for every fenced job, on every
	// short tick, so an aware watcher that exits mid-close is replaced by
	// the fence within hubCloseFenceInterval.
	var open []string
	pass := func() {
		// The ledger ticks at the top of every full pass, not on the fence
		// ticks between them: the tick is the settlement liveness signal.
		client.nodeLoopTick(nodeHubCloseSettle)
		jobs, err := fleet.PendingHubCloseJobs(jobsRoot)
		if err != nil {
			client.nodeLoopNoteSilentError(nodeHubCloseSettle, "node_pane_close_op.go:pendingHubCloseJobs", err)
			return
		}
		client.nodeLoopClearSilent("node_pane_close_op.go:pendingHubCloseJobs")
		open = jobs
		visited := unionJobs(jobs, client.fencedJobs())
		// Flow edges: one item per job with an open hub-close intent or fence.
		// A job enters when the pass visits it; a settled or released close exits.
		for _, job := range visited {
			client.nodeLoopNoteMade(nodeHubCloseSettle, job)
		}
		// Each step is bounded and runs outside the node's acting lock: the
		// job's end lock alone serializes it with that job's close ops. A
		// fenced job whose intent went stale is visited too, to release it.
		var running sync.WaitGroup
		defer running.Wait()
		slots := make(chan struct{}, hubCloseSettleConcurrency)
		for _, job := range visited {
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				return
			}
			running.Add(1)
			go func(job string) {
				defer running.Done()
				defer func() { <-slots }()
				step, cancel := context.WithTimeout(ctx, hubCloseSettleBudget)
				defer cancel()
				// A done edge needs a terminal state: HubClosePending with a
				// nil error is still open, and counting it would balance made
				// and done for a stuck close.
				if hubCloseStateIsTerminal(client.settleHubClose(step, job)) {
					client.nodeLoopNoteDone(nodeHubCloseSettle, job)
				}
			}(job)
		}
	}
	fence := func() {
		for _, job := range unionJobs(open, client.fencedJobs()) {
			step, cancel := context.WithTimeout(ctx, hubCloseSettleBudget)
			client.reconcileWatcherFence(step, job)
			cancel()
		}
	}
	pass()
	ticker := time.NewTicker(hubCloseFenceInterval)
	defer ticker.Stop()
	for tick := 1; ; tick++ {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if tick%int(hubCloseSettleInterval/hubCloseFenceInterval) == 0 {
				pass()
			} else {
				fence()
			}
		}
	}
}

// hubCloseFenceInterval paces the fence reconciliation of jobs with an open
// close between full settlement passes.
const hubCloseFenceInterval = 2 * time.Second

func unionJobs(a, b []string) []string {
	out := slices.Concat(a, b)
	slices.Sort(out)
	return slices.Compact(out)
}

func (client *HubClient) finishCloseOp(job *os.Root, op fleet.CloseOp, state string) (any, string) {
	op.State, op.Updated = state, client.nowUTC()
	if fleet.WriteCloseOp(job, op) != nil {
		return nil, "journal_failed"
	}
	return nodePaneCloseOpResult{Outcome: state, Reason: op.Reason}, ""
}

// nodePaneCloseStatus reads one close operation. The caller holds
// nodeCommandMu.
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

// hubCloseLockBudget bounds the end-lock wait of a release, inside the hub's
// command deadline.
const hubCloseLockBudget = 10 * time.Second

// hubCloseSettleBudget bounds one settlement step of the daemon pass.
const hubCloseSettleBudget = 30 * time.Second

// hubCloseCommandSettleBudget bounds the settlement step a status or release
// runs inline, under the acting lock: with hubCloseLockBudget it stays inside
// the hub's command deadline. The daemon pass (runHubCloseSettle) finishes
// whatever an inline step leaves pending.
const hubCloseCommandSettleBudget = 5 * time.Second

func (client *HubClient) nodeCloseOpRef(args nodeCloseOpRefArgs, release bool) (any, string) {
	result, code := client.runCloseOpRef(args, release)
	// A failed command settles nothing inline (hk 4632, #572 r4576225996,
	// r4576228677): a release whose end-lock wait expired would wait on the
	// same lock again while holding the acting lock. The daemon pass retries.
	if code == "" {
		ctx, cancel := context.WithTimeout(context.Background(), hubCloseCommandSettleBudget)
		client.settleHubClose(ctx, args.Job)
		cancel()
	}
	return result, code
}

func (client *HubClient) runCloseOpRef(args nodeCloseOpRefArgs, release bool) (any, string) {
	job, jobDir, found, code := client.openCloseOpJobDir(args.Job)
	if code != "" {
		return nil, code
	}
	if !found {
		if !release && args.Epoch > 0 {
			return client.missingJobAnswer(args)
		}
		return nodeCloseOpStatusResult{State: nodeCloseOpNone}, ""
	}
	defer job.Close()
	op, found, err := fleet.ReadCloseOp(job, args.RequestID)
	if err != nil || (found && fleet.SyncCloseOps(job) != nil) {
		return nil, "journal_failed"
	}
	if !found {
		if release {
			// A released request the node never registered (its intent was
			// recorded, then journal_failed) will not
			// run: its intent is closed, under the end lock, so the watcher
			// stops waiting on it.
			if code := client.cancelUnregisteredIntent(job, jobDir, args); code != "" {
				return nil, code
			}
			return nodeCloseOpStatusResult{State: fleet.CloseOpCancelled, Released: true, Attempt: currentAttempt(job)}, ""
		}
		return nodeCloseOpStatusResult{State: nodeCloseOpNone, Attempt: currentAttempt(job)}, ""
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
	if op.State == fleet.CloseOpUnknown || op.State == fleet.CloseOpGone || op.State == fleet.CloseOpClosed {
		ctx, cancel := context.WithTimeout(context.Background(), hubCloseLockBudget)
		unlock, err := client.hubCloseLedger().LockEnd(ctx, jobDir)
		cancel()
		if err != nil {
			return nil, "journal_failed"
		}
		defer unlock()
		target, err := readNodeCloseTarget(job)
		if err != nil {
			return nil, "journal_failed"
		}
		events, err := readNodeJobLedgerEvents(job)
		if err != nil {
			return nil, "journal_failed"
		}
		matching, cancelled, laterSettled, acked := false, false, false, false
		var attempt string
		for _, event := range events {
			switch {
			case nodeJobClaimKind(event.kind), event.kind == "job.spawned", event.kind == "job.reprompted":
				attempt = event.name
			case event.kind == "job.hub_close_intent" && event.request == args.RequestID:
				matching = event.attempt == op.Attempt && event.pane == op.PaneID
				cancelled, laterSettled, acked = false, false, false
			case event.kind == "job.hub_close_cancelled" && event.request == args.RequestID && matching:
				cancelled = true
			case event.kind == "job.hub_close_acked" && event.request == args.RequestID:
				acked = true
			case event.kind == "job.lost" && event.reason == "hub_close" && event.final && cancelled && attempt == op.Attempt:
				laterSettled = true
			}
		}
		if laterSettled {
			if err := client.hubCloseLedger().SyncEvents(job); err != nil {
				return nil, "journal_failed"
			}
			return nodeCloseOpStatusResult{State: op.State, Released: op.Released != nil, Settled: true, Attempt: target.attempt}, ""
		}
		if cancelled {
			if err := client.hubCloseLedger().SyncEvents(job); err != nil {
				return nil, "journal_failed"
			}
			if args.Ack && !release && !acked {
				ackCtx, cancel := context.WithTimeout(context.Background(), hubCloseLockBudget)
				err := client.hubCloseLedger().AckIntentCancel(ackCtx, args.Job, args.RequestID)
				cancel()
				if err != nil {
					return nil, "journal_failed"
				}
			}
			return nodeCloseOpStatusResult{State: nodeCloseIntentCancelled, Released: op.Released != nil, Attempt: target.attempt}, ""
		}
		if target.attempt != "" && target.attempt != op.Attempt {
			if err := client.hubCloseLedger().SyncEvents(job); err != nil {
				return nil, "journal_failed"
			}
			return nodeCloseOpStatusResult{State: nodeCloseOpSuperseded, Released: op.Released != nil, Attempt: target.attempt}, ""
		}
	}
	settled := false
	if op.State == fleet.CloseOpClosed || op.State == fleet.CloseOpGone || op.State == fleet.CloseOpUnknown {
		events, err := readNodeJobLedgerEvents(job)
		if err != nil {
			return nil, "journal_failed"
		}
		open := false
		var attempt, pane, label string
		for _, event := range events {
			switch {
			case nodeJobClaimKind(event.kind):
				attempt, pane, label, open = event.name, "", "", false
			case event.kind == "job.spawned":
				attempt, pane, label, open = event.name, event.pane, event.label, false
			case event.kind == "job.reprompted":
				attempt, open = event.name, false
			case event.kind == "job.hub_close_intent" && event.request == args.RequestID &&
				event.attempt == attempt && attempt == op.Attempt && event.pane == pane && pane == op.PaneID && event.label == label:
				open = true
			case event.kind == "job.hub_close_cancelled" && event.request == args.RequestID:
				open = false
			case event.kind == "job.lost" && event.reason == "hub_close" && event.final:
				// The final event closes every still-open intent on this
				// attempt, including an older unknown request retried under a
				// new id (fleet.openHubCloseIntents).
				if event.request == args.RequestID || open {
					settled = true
				}
				open = false
			}
		}
		if settled {
			if err := client.hubCloseLedger().SyncEvents(job); err != nil {
				return nil, "journal_failed"
			}
		}
	}
	return nodeCloseOpStatusResult{State: op.State, Released: op.Released != nil, Settled: settled, Attempt: currentAttempt(job), Reason: op.Reason}, ""
}

// missingJobAnswer answers a pane.close_status that carries the hub's epoch
// for a job whose directory is not there (hk 5296). Only a proved absence is
// answered as missing: the node fences the request with a durable tombstone
// outside every job directory first, then reports the epoch it holds. A
// directory it cannot prove gone (an unreadable or missing jobs root, an
// entry that is not a directory, no identity record, a failed write) is the
// plain none answer, or journal_failed when a read or write failed, and the
// hub keeps its hold. The caller holds nodeCommandMu.
func (client *HubClient) missingJobAnswer(args nodeCloseOpRefArgs) (any, string) {
	jobsRoot := filepath.Join(client.jobsInboxRoot, "jobs")
	missing, err := fleet.JobDirMissingProof(jobsRoot, args.Job)
	if err != nil {
		return nil, "journal_failed"
	}
	if !missing {
		return nodeCloseOpStatusResult{State: nodeCloseOpNone}, ""
	}
	tombstone, err := fleet.WriteMissingJobTombstone(jobsRoot, fleet.MissingJobTombstone{RequestID: args.RequestID, Job: args.Job, Epoch: args.Epoch, Created: client.nowUTC()})
	if err != nil {
		return nil, "journal_failed"
	}
	return nodeCloseOpStatusResult{State: nodeCloseOpNone, MissingEpoch: tombstone.Epoch}, ""
}

// missingJobFence reports whether the node fenced this request while its job
// directory was gone. A tombstone that names another job, or an older epoch
// than the request's, answers invalid_args like any other reuse of an id.
func (client *HubClient) missingJobFence(args nodePaneCloseOpArgs) (bool, string) {
	if client.jobsInboxRoot == "" {
		return false, ""
	}
	tombstone, found, err := fleet.ReadMissingJobTombstone(filepath.Join(client.jobsInboxRoot, "jobs"), args.RequestID)
	switch {
	case err != nil:
		return false, "journal_failed"
	case !found:
		return false, ""
	case tombstone.Job != args.Job || args.Epoch > tombstone.Epoch:
		return false, "invalid_args"
	}
	return true, ""
}

// cancelUnregisteredIntent fences a request the hub released before the node
// registered it, under the job's end lock: it journals a released cancelled
// tombstone under the request id, so a delayed duplicate pane.close_op replays
// cancelled (or is invalid_args) and touches no pane, then durably cancels the
// request's intent, if one was recorded, so the watcher stops waiting on it.
// The tombstone names the intent's pane and attempt; with no intent it names
// none, and any later close_op under that id is invalid_args.
func (client *HubClient) cancelUnregisteredIntent(job *os.Root, jobDir string, args nodeCloseOpRefArgs) string {
	ledger := client.hubCloseLedger()
	// Inside the hub's command deadline: a held end lock answers a
	// retryable journal_failed instead of holding the acting lock.
	ctx, cancel := context.WithTimeout(context.Background(), hubCloseLockBudget)
	defer cancel()
	unlock, err := ledger.LockEnd(ctx, jobDir)
	if err != nil {
		return "journal_failed"
	}
	defer unlock()
	if op, found, err := fleet.ReadCloseOp(job, args.RequestID); err != nil {
		return "journal_failed"
	} else if found && op.State != fleet.CloseOpCancelled {
		return ""
	}
	events, err := readNodeJobLedgerEvents(job)
	if err != nil {
		return "journal_failed"
	}
	var intent *nodeJobLedgerEvent
	for i := range events {
		if events[i].kind == "job.hub_close_intent" && events[i].request == args.RequestID {
			intent = &events[i]
		}
	}
	now := client.nowUTC()
	tombstone := fleet.CloseOp{RequestID: args.RequestID, State: fleet.CloseOpCancelled, Created: now, Updated: now, Released: &now}
	if intent != nil {
		tombstone.PaneID, tombstone.Attempt = intent.pane, intent.attempt
	}
	if fleet.WriteCloseOp(job, tombstone) != nil {
		return "journal_failed"
	}
	if intent != nil && ledger.CancelIntent(ctx, args.Job, args.RequestID) != nil {
		return "journal_failed"
	}
	return ""
}

// currentAttempt is the job's current attempt token, or empty when its
// ledger cannot be read.
func currentAttempt(job *os.Root) string {
	target, err := readNodeCloseTarget(job)
	if err != nil {
		return ""
	}
	return target.attempt
}

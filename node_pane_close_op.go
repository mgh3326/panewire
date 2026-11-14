package panewire

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hubCapabilityCloseOps is advertised in hello once a node both runs the close
// operations and records the hub-close intent its job watchers defer to (hk
// 4652): a hub issues pane.close_op only to a node that advertises it.
const hubCapabilityCloseOps = "close_ops_v1"

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
)

type nodePaneCloseOpArgs struct {
	RequestID string `json:"request_id"`
	Job       string `json:"job"`
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
}

// nodePaneCloseOpResult.Outcome is a final fleet.CloseOp state, or
// nodeCaptureNotAJob (no job directory) or nodeCloseOpMismatch; for those two
// no journal entry exists and nothing was closed.
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
	// Attempt is the job's current attempt token, the value a pane.close_op
	// for it must name; empty when the job has no readable attempt.
	Attempt string `json:"attempt"`
}

func validNodePaneCloseOpArgs(args nodePaneCloseOpArgs) bool {
	return fleet.ValidCloseOpID(args.RequestID) && fleet.ValidJobID(args.Job) && validNodeAttemptToken(args.Attempt) && nodeCommandPaneIDPattern.MatchString(args.PaneID) && validNodeJobName(args.Name) && nodePaneClosableStatus[args.AgentStatus] && validNodeCommandPath(args.Cwd) && validNodeEndReason(args.EndReason) && args.EndReason != ""
}

// validNodeAttemptToken bounds an attempt token: a ledger event file name.
func validNodeAttemptToken(token string) bool { return fleet.ValidJobID(token) }

func validNodeCloseOpRefArgs(args nodeCloseOpRefArgs) bool {
	return fleet.ValidCloseOpID(args.RequestID) && fleet.ValidJobID(args.Job)
}

func validNodePaneCloseOpResult(result nodePaneCloseOpResult) bool {
	switch result.Outcome {
	case fleet.CloseOpClosed, fleet.CloseOpRefused, fleet.CloseOpGone, fleet.CloseOpUnknown, fleet.CloseOpCancelled, fleet.CloseOpPreempted:
		return true
	case nodeCaptureNotAJob, nodeCloseOpMismatch:
		return !result.Replayed
	}
	return false
}

func validNodeCloseOpStatusResult(result nodeCloseOpStatusResult) bool {
	if result.Attempt != "" && !validNodeAttemptToken(result.Attempt) {
		return false
	}
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
// The journal moves registered → closing → final. registered is written,
// against the job's current attempt, before any herdr read. closing is written after the first guarded look and the settle, before the
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
	if err != nil || (found && fleet.SyncCloseOps(job) != nil) {
		return nil, "journal_failed"
	}
	switch {
	// A repeat names the pane and attempt it was registered for, before any
	// replay or settlement: another attempt's outcome is never returned.
	case found && (op.PaneID != args.PaneID || op.Attempt != args.Attempt):
		return nil, "invalid_args"
	case found && op.Final():
		return nodePaneCloseOpResult{Outcome: op.State, Replayed: true}, ""
	case found && op.State == fleet.CloseOpClosing:
		return client.finishCloseOp(job, op, fleet.CloseOpUnknown)
	}
	// Registering, or resuming one left registered: bind the request to the
	// job's current attempt before any herdr call.
	op, target, outcome, code := client.bindCloseOp(ctx, job, op, found, args)
	if code != "" {
		return nil, code
	}
	if outcome != "" {
		return nodePaneCloseOpResult{Outcome: outcome}, ""
	}
	closeArgs := nodePaneCloseArgs{PaneID: args.PaneID, Name: args.Name, AgentStatus: args.AgentStatus, Cwd: args.Cwd, AllowUnnamed: args.AllowUnnamed, target: target}
	result, code := client.nodePaneCloseWith(ctx, closeArgs, func() error {
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
		op.State, op.Updated = fleet.CloseOpClosing, client.nowUTC()
		return fleet.WriteCloseOp(job, op)
	})
	if code != "" {
		// The journal keeps registered or closing; a repeat of this request
		// id settles it.
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
	attempt                    string
	receipt                    bool
	pane, label, role, session string
	ended                      bool
	// announced: the attempt's newest end-like event is a quota stop
	// announced for the hub's hand-over, or a reviewer's quota-stop intent
	// is still open; whether the job already ended is in hk, not the ledger.
	announced, intent bool
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
			target.attempt, target.receipt, target.ended, target.announced, target.intent = event.name, true, false, false, false
			target.pane, target.label, target.session = event.pane, event.label, event.session
			if event.role != "" {
				target.role = event.role
			}
		case event.kind == "job.reprompted":
			target.attempt, target.ended, target.announced, target.intent = event.name, false, false, false
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
			target.intent = false
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
func (client *HubClient) bindCloseOp(ctx context.Context, job *os.Root, op fleet.CloseOp, resuming bool, args nodePaneCloseOpArgs) (fleet.CloseOp, *nodeCloseTarget, string, string) {
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
		return op, &target, "", ""
	case !bound:
		return op, nil, nodeCloseOpMismatch, ""
	}
	// The intent precedes the registration: a crash between the two leaves
	// an intent with no operation, which the watcher waits on until the
	// hub's repeat of this request registers and settles it.
	if client.recordHubCloseIntent(ctx, args.Job, fleet.HubCloseIntent{RequestID: args.RequestID, Attempt: attempt, Pane: args.PaneID, Label: args.Name, EndReason: args.EndReason}) != nil {
		return op, nil, "", "journal_failed"
	}
	op = fleet.CloseOp{RequestID: args.RequestID, PaneID: args.PaneID, Attempt: attempt, State: fleet.CloseOpRegistered, Created: now, Updated: now}
	if fleet.WriteCloseOp(job, op) != nil {
		return op, nil, "", "journal_failed"
	}
	return op, &target, "", ""
}

func (client *HubClient) recordHubCloseIntent(ctx context.Context, job string, in fleet.HubCloseIntent) error {
	if client.nodeHubCloseIntent != nil {
		return client.nodeHubCloseIntent(ctx, job, in)
	}
	return fleet.RecordHubCloseIntent(ctx, filepath.Join(client.jobsInboxRoot, "jobs"), job, in)
}

func (client *HubClient) finishCloseOp(job *os.Root, op fleet.CloseOp, state string) (any, string) {
	op.State, op.Updated = state, client.nowUTC()
	if fleet.WriteCloseOp(job, op) != nil {
		return nil, "journal_failed"
	}
	return nodePaneCloseOpResult{Outcome: state}, ""
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
	if err != nil || (found && fleet.SyncCloseOps(job) != nil) {
		return nil, "journal_failed"
	}
	if !found {
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
	return nodeCloseOpStatusResult{State: op.State, Released: op.Released != nil, Attempt: currentAttempt(job)}, ""
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

package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/spawnargs"
)

// The hub hands an implementer on a spent pool over itself (hk 4633;
// docs/quota-stop.md "An exhausted pool"). It decides by the §Implementer
// rule; a hand-over is a pool action row of kind handover that stores the
// whole successor submission, with a fixed request id, before anything is
// closed. The reconciler then closes the old pane through the close-op path
// (the node writes the old record lost with replaced_by=<new job>), submits
// the stored successor with the same request id on every retry, and ends the
// row only once the successor settled: placed, or handover_failed with the
// owner's fallback notice.

const (
	poolActionKindHandover = "handover"
	// poolActionKindWait is a final row recording that a stop was waited
	// out, so a later stop with another reset counts as the second one.
	poolActionKindWait = "wait"

	// poolWaitReason is quotaDecisionJob's one wait reason.
	poolWaitReason = "reset_within_expected"

	handoverPlaced = "placed"
	handoverFailed = "handover_failed"
)

// poolHandover is the hand-over half of a pool action row.
type poolHandover struct {
	// Successor is the complete submission, request id included; it is
	// stored before the close and never rebuilt.
	Successor fleet.Submission
	Job       string
	Pool      string
	Cause     string
	// Submitted: the successor's claim committed. The claim is idempotent by
	// request id, so a restart before this flag is saved claims nothing new.
	Submitted bool
	Outcome   string `json:",omitempty"`
	Reason    string `json:",omitempty"`
	// Reset is the stop's reset (unix seconds) and Account its account: the
	// wait row's identity, and for a hand-over the exhaustion the reconciler
	// checks again before the close.
	Reset   int64  `json:",omitempty"`
	Account string `json:",omitempty"`
	// Cwd, Branch and Digest bind the saved patch to the worktree: just before
	// the close the hub captures again and releases the row on a change.
	Cwd, Branch, Digest string `json:",omitempty"`
}

func (ho *poolHandover) exhaustion() poolExhaustion {
	var reset time.Time
	if ho.Reset != 0 {
		reset = time.Unix(ho.Reset, 0).UTC()
	}
	return poolExhaustion{Pool: ho.Pool, Account: ho.Account, Cause: ho.Cause, Reset: reset}
}

// handoverInputs is what the §Implementer rule reads from hk: the quota
// policy and the task's priority and expected time.
type handoverInputs struct {
	policy   quotaPolicy
	policyOK bool
	priority int
	expected time.Duration
	// bound is the job the task's refs.job_id names; empty when unread.
	bound string
}

func (h *HubServer) readHandoverInputs(ctx context.Context, task string) handoverInputs {
	if h.handoverInputs != nil {
		return h.handoverInputs(ctx, task)
	}
	var in handoverInputs
	if h.handoffkeep == nil {
		return in
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if status, raw, err := h.handoffkeep.do(ctx, http.MethodGet, h.handoffkeep.endpoint("/v1/documents/"+quotaStopPolicyKey), nil); err == nil && status == http.StatusOK {
		var doc struct {
			Body string `json:"body"`
		}
		if json.Unmarshal(raw, &doc) == nil {
			in.policy, in.policyOK = parseQuotaPolicy(doc.Body)
		}
	}
	if _, err := strconv.ParseInt(task, 10, 64); err != nil {
		return in
	}
	if status, raw, err := h.handoffkeep.do(ctx, http.MethodGet, h.handoffkeep.endpoint("/v1/tasks/"+task), nil); err == nil && status == http.StatusOK {
		var t struct {
			Priority any     `json:"priority"`
			Expected *string `json:"expected"`
			Refs     struct {
				Job string `json:"job_id"`
			} `json:"refs"`
		}
		if json.Unmarshal(raw, &t) == nil {
			in.bound = t.Refs.Job
			in.priority, _ = quotaTaskPriority(t.Priority)
			if t.Expected != nil {
				in.expected, _ = quotaDuration(*t.Expected)
			}
		}
	}
	return in
}

// sweepImplementer decides for one implementer on a spent pool: wait (one
// info line naming the reset) or hand over (a handover row). The caller has
// checked the old node offers close ops and the patch command. Past the
// replacement limit, or when no successor can be built, the owner gets
// today's hand-over notice instead.
func (h *HubServer) sweepImplementer(ctx context.Context, exhaustion poolExhaustion, task hubSpawnTask, prior fleet.Spawn, app *fleet.App, record map[string]any) poolJobResult {
	const role = "implementer"
	if h.spawnGrantDrain || h.spawnRollbackDrain {
		// No successor may be created while grant rows drain; a row opened
		// now would close the job and then never place (hk 4661).
		return poolJobSkip(role, "draining")
	}
	if active, err := h.inbox.store.activePoolActionFor(ctx, task.Job); err != nil || active {
		// One row per job: an existing non-final row owns it.
		return poolJobSkip(role, "action_exists")
	}
	resets, err := h.inbox.store.poolWaitResets(ctx, task.Job)
	if err != nil {
		return poolJobSkip(role, "store_unavailable")
	}
	waits := 0
	for _, reset := range resets {
		if reset == exhaustion.Reset.Unix() {
			// This stop is the one already waited out; its notice is replayed
			// until the inbox owns it.
			return h.notifyPoolWait(ctx, exhaustion, task, "waiting")
		}
		waits++
	}
	// The effective task: a --parent-job successor inherits it, so the job
	// record names it, not the submission.
	taskID := prior.Task
	if id := hkRecordTaskID(record); id != "" {
		taskID = id
	}
	in := h.readHandoverInputs(ctx, taskID)
	if in.bound != "" && in.bound != task.Job {
		// Another job holds the task now; replacing this one would start a
		// second implementer beside it.
		return poolJobSkip(role, "task_rebound")
	}
	started := task.Created
	h.mu.Lock()
	if job := h.jobs[task.Job]; job != nil {
		if at, err := time.Parse(time.RFC3339, job.StartedAt); err == nil {
			started = at
		}
	}
	h.mu.Unlock()
	decision, why := quotaDecisionJob(in.policy, in.policyOK, in.priority, in.expected, started, waits, quotaStop{Reset: exhaustion.Reset}, h.now())
	if decision == "wait" {
		return h.waitPoolStop(ctx, exhaustion, task, why)
	}
	newJob, n, ok := quotaReplacementJob(task.Job)
	if !ok {
		h.logger.Info("pool hand-over past the replacement limit", "job", task.Job, "replacements", n-1)
		return h.notifyPoolExhausted(ctx, exhaustion, task, role, "replacement_limit")
	}
	handover, refusal := h.prepareHandover(ctx, exhaustion, task, prior, record, newJob)
	if refusal != "" {
		h.logger.Info("pool hand-over not prepared", "job", task.Job, "reason", refusal)
		if refusal == "already_replaced" {
			return poolJobSkip(role, refusal)
		}
		return h.notifyPoolExhausted(ctx, exhaustion, task, role, refusal)
	}
	if err := h.createPoolAction(ctx, exhaustion, task, app, handover); err != nil {
		return poolJobSkip(role, "handover_not_queued")
	}
	return poolJobResult{action: poolJobQueued, role: role, why: why}
}

// notifyPoolExhausted is today's owner notice, now the exception.
func (h *HubServer) notifyPoolExhausted(ctx context.Context, exhaustion poolExhaustion, task hubSpawnTask, role, why string) poolJobResult {
	owner := task.Owner
	if strings.HasPrefix(task.Job, "mwr-") {
		owner = "mergewake"
	}
	eventID := "pool-exhausted:" + task.Job
	result := h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: eventID, JobID: laneEventTransportID(owner, eventID), OwnerLane: owner, NotifyType: "info", Text: poolExhaustionText(exhaustion, task.Job), Host: "hub", Epoch: 1}, nil)
	if result.ID == 0 || result.PersistFailed {
		return poolJobSkip(role, "owner_not_notified")
	}
	return poolJobResult{action: poolJobNotified, role: role, why: why}
}

// waitPoolStop records the wait as a final row and tells the owner once.
func (h *HubServer) waitPoolStop(ctx context.Context, exhaustion poolExhaustion, task hubSpawnTask, why string) poolJobResult {
	const role = "implementer"
	id, err := fleet.NewRequestID()
	if err != nil {
		return poolJobSkip(role, "request_id")
	}
	h.mu.Lock()
	epoch := uint64(1)
	if job := h.jobs[task.Job]; job != nil && job.Epoch > 0 {
		epoch = job.Epoch
	}
	h.mu.Unlock()
	row := poolAction{ID: id, Job: task.Job, Node: spawnHolderHost(task), Epoch: epoch, Phase: poolActionDone, Data: poolActionData{
		Owner: task.Owner, Kind: poolActionKindWait,
		Handover: &poolHandover{Pool: exhaustion.Pool, Cause: exhaustion.Cause, Reset: exhaustion.Reset.Unix()},
	}}
	if err := h.inbox.store.insertPoolAction(ctx, row); err != nil {
		return poolJobSkip(role, "store_unavailable")
	}
	return h.notifyPoolWait(ctx, exhaustion, task, why)
}

// notifyPoolWait publishes the wait's fixed-id notice unless the inbox
// already owns it, so a crash after the wait row is stored is repaired by the
// next pass.
func (h *HubServer) notifyPoolWait(ctx context.Context, exhaustion poolExhaustion, task hubSpawnTask, why string) poolJobResult {
	const role = "implementer"
	eventID := fmt.Sprintf("pool-wait:%s:%d", task.Job, exhaustion.Reset.Unix())
	if owned, err := h.inbox.store.poolNoticeOwned(ctx, task.Owner, eventID); err != nil {
		return poolJobSkip(role, "store_unavailable")
	} else if owned {
		return poolJobSkip(role, why)
	}
	text := fmt.Sprintf("[panewire] %s waits for the %s reset at %s (%s %s); a later stop hands it over.", task.Job, exhaustion.Pool, exhaustion.Reset.UTC().Format(time.RFC3339), exhaustion.Pool, exhaustion.Cause)
	result := h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: eventID, JobID: laneEventTransportID(task.Owner, eventID), OwnerLane: task.Owner, NotifyType: "info", Text: utf8PrefixWithin(text, laneEventTextLimit), Host: "hub", Epoch: 1}, nil)
	if result.ID == 0 || result.PersistFailed {
		return poolJobSkip(role, "owner_not_notified")
	}
	return poolJobResult{action: poolJobNotified, role: role, why: why}
}

// prepareHandover saves the old worktree's patch through its node and builds
// the successor submission. It changes nothing the next pass cannot redo.
func (h *HubServer) prepareHandover(ctx context.Context, exhaustion poolExhaustion, task hubSpawnTask, prior fleet.Spawn, record map[string]any, newJob string) (*poolHandover, string) {
	if _, err := h.inbox.store.latestSpawnForJob(ctx, newJob); err == nil {
		// A quota-stop replacement already placed this job's successor.
		return nil, "already_replaced"
	}
	old := task.Request
	if prior.Task == "" && prior.ParentJob == "" {
		return nil, "not_replaceable"
	}
	if prior.Profile != "" && prior.Profile != "auto" || prior.Host != "" && prior.Host != "auto" {
		// A pinned profile or host cannot move off the spent pool or the old
		// host's worktree; the owner decides (the quota replacement refuses
		// the same).
		return nil, "pinned"
	}
	if !validNodeEndReason(handoverEndReason(record, exhaustion, newJob)) {
		return nil, "end_reason_too_long"
	}
	checkout := old.Repository.Checkout
	if checkout == nil || checkout.Branch == "" || checkout.Origin == "" {
		return nil, "no_branch"
	}
	host, label := spawnHolderHost(task), heldAgentName(task, record)
	probe := h.probeHeldPane(ctx, host, task.Outcome.Pane, label)
	if probe.state != heldPanePresent || !validNodeCommandPath(probe.row.Cwd) {
		return nil, "pane_unavailable"
	}
	if !nodePaneIdleStatus[probe.row.AgentStatus] {
		// The patch must be the pane's final work: capture only an idle pane.
		return nil, "pane_not_idle"
	}
	var patch nodeHandoverPatchResult
	if !h.nodeCall(ctx, host, nodeCommandHandoverPatch, nodeHandoverPatchArgs{Job: task.Job, Cwd: probe.row.Cwd, Branch: checkout.Branch}, hubNodeCommandWait, &patch) {
		return nil, "patch_unavailable"
	}
	if patch.Outcome != nodeHandoverPatchSaved {
		return nil, "patch_" + patch.Outcome
	}
	request, err := handoverSuccessorRequest(old, task.Job, newJob, handoverExclude(exhaustion), exhaustion, patch)
	if err != nil {
		return nil, "invalid_successor"
	}
	if next, err := fleet.ValidateSubmission(request); err != nil || !validLaneReference(next.Owner) {
		return nil, "invalid_successor"
	}
	if !fleet.ValidAgentName(hubSpawnArg(request.Args, "-l")) {
		// herdr's 32-character name rule, which the hub's own claim does not
		// run through the submit preflight.
		return nil, "label_too_long"
	}
	reset := int64(0)
	if !exhaustion.Reset.IsZero() {
		reset = exhaustion.Reset.Unix()
	}
	return &poolHandover{Successor: request, Job: newJob, Pool: exhaustion.Pool, Cause: exhaustion.Cause, Reset: reset, Account: exhaustion.Account,
		Cwd: probe.row.Cwd, Branch: checkout.Branch, Digest: patch.Digest}, ""
}

// handoverExclude is the pool the successor excludes: the band a quota
// reading spent (a Devin free limit is devin-free, a paid window the paid
// band), the whole pool for a pool-wide cause, and none for a Command Code
// account credit stop, whose account mark keeps the successor off the spent
// account (hk 5138).
func handoverExclude(exhaustion poolExhaustion) string {
	switch {
	case exhaustion.accountCredit():
		return ""
	case exhaustion.Cause == poolCauseFreeLimit:
		return fleet.BandKey(exhaustion.Pool + "-free")
	case exhaustion.fromQuota():
		return fleet.BandKey(exhaustion.Pool)
	}
	return exhaustion.Pool
}

// hkRecordTaskID is the task an hk job record names.
func hkRecordTaskID(record map[string]any) string {
	switch task := record["task_id"].(type) {
	case float64:
		if task > 0 {
			return strconv.FormatInt(int64(task), 10)
		}
	case string:
		return strings.TrimSpace(task)
	}
	return ""
}

// handoverSuccessorRequest is the stored successor: the quota replacement's
// submission (same role, owner and brief, job <old>-q<n>, the spent pool
// excluded) as a spec checkout of the branch at its last pushed commit, with
// --parent-job <old> in place of --task (the two are exclusive; the task is
// inherited) and a continuation note.
func handoverSuccessorRequest(old fleet.Submission, oldJob, newJob, exclude string, exhaustion poolExhaustion, patch nodeHandoverPatchResult) (fleet.Submission, error) {
	next, err := quotaReplacementRequest(old, oldJob, newJob, exhaustion.Pool, exclude, "implementer")
	if err != nil {
		return fleet.Submission{}, err
	}
	// The successor holds no local checkout and inherits the task: drop -c,
	// --task and any earlier --parent-job, so exactly one names oldJob.
	for _, flag := range []string{"-c", "--task", "--parent-job"} {
		for i := slices.Index(next.Args, flag); i >= 0 && i+1 < len(next.Args); i = slices.Index(next.Args, flag) {
			next.Args = slices.Delete(next.Args, i, i+2)
		}
	}
	next.Args = append(next.Args, "--parent-job", oldJob)
	if hubSpawnArg(next.Args, "--report-relative") == "" {
		// A --parent-job spawn requires a report; it lives in the successor's
		// own job directory.
		next.Args = append(next.Args, "--report-relative", newJob+"/report.md")
	}
	// The old job already passed its start gate; its conditions name the task
	// the successor no longer carries.
	next.StartConditions = nil
	branch := old.Repository.Checkout.Branch
	dir := fleet.CheckoutWorktreeName(newJob)
	checkout := &fleet.HubCheckout{Origin: old.Repository.Checkout.Origin, Mode: fleet.HubCheckoutSuccessor, Branch: branch, Sha: patch.Pushed, Dir: dir}
	if patch.Pushed == "" {
		// Nothing was pushed: start from the branch's base; the patch is the
		// whole work.
		checkout = &fleet.HubCheckout{Origin: old.Repository.Checkout.Origin, Mode: "detached", Sha: patch.Base, Fetch: patch.BaseRef, Dir: dir}
	}
	next.Repository = fleet.Repository{Name: old.Repository.Name, Origin: checkout.Origin, Worktree: dir, Checkout: checkout}
	next.Brief = strings.TrimRight(old.Brief, "\n") + "\n\n" + handoverNote(oldJob, branch, exhaustion, patch) + "\n"
	return next, nil
}

// handoverNote is the continuation note appended to the successor's brief.
func handoverNote(oldJob, branch string, exhaustion poolExhaustion, patch nodeHandoverPatchResult) string {
	var b strings.Builder
	fmt.Fprintf(&b, "## Hand-over from %s\nThe hub handed job %s over to you: its pool %s ran out (%s). ", oldJob, oldJob, exhaustion.Pool, exhaustion.Cause)
	if patch.Pushed != "" {
		fmt.Fprintf(&b, "Continue branch %s from its last pushed commit %s; this worktree is that branch.", branch, patch.Pushed)
	} else {
		fmt.Fprintf(&b, "Branch %s was never pushed: this worktree is detached at its base %s (%s). Create branch %s here, apply the patch below, and push it.", branch, patch.Base, strings.TrimPrefix(patch.BaseRef, "refs/heads/"), branch)
	}
	if patch.Dirty {
		fmt.Fprintf(&b, " The old worktree's uncommitted and unpushed work, untracked files included, is saved as hk evidence %s; apply everything under its patch heading with git apply.", patch.Doc)
	} else {
		b.WriteString(" The old worktree held no other work.")
	}
	return b.String()
}

// finishHandover is a settled handover row: the old pane is closed and its
// record is terminal. It submits the stored successor (same request id on
// every retry) and ends the row once the successor settled.
func (h *HubServer) finishHandover(ctx context.Context, action poolAction, state, reason string) error {
	handover := action.Data.Handover
	if handover == nil {
		return errors.New("handover row without its successor")
	}
	if handover.Outcome == "" {
		switch {
		case state != "lost" || reason != action.Data.EndReason:
			// The old job ended another way (it completed, or a watcher wrote
			// its own end): no successor may name it.
			handover.Outcome, handover.Reason = handoverFailed, "old record ended "+state
		case !handover.Submitted:
			if _, _, err := h.inbox.store.claimSpawn(ctx, handover.Successor, handover.Job, action.Data.Owner, h.now().UTC()); err != nil {
				if !errors.Is(err, errSpawnClaimed) {
					return err
				}
				handover.Outcome, handover.Reason = handoverFailed, "claim_refused"
				break
			}
			handover.Submitted = true
			if err := h.inbox.store.savePoolAction(ctx, action); err != nil {
				return err
			}
			select {
			case h.spawnKick <- struct{}{}:
			default:
			}
		}
	}
	if handover.Outcome == "" {
		task, err := h.inbox.store.spawnTask(ctx, handover.Successor.RequestID)
		if err != nil {
			return err
		}
		if spawnStatusActive(task.Status) {
			// I2: the row stays non-final until the successor settled.
			return nil
		}
		if task.Status == "succeeded" && task.Outcome.RC == 0 {
			handover.Outcome = handoverPlaced
			handover.Reason = task.Outcome.Host + "/" + task.Outcome.Profile
		} else {
			handover.Outcome, handover.Reason = handoverFailed, fmt.Sprintf("successor %s rc=%d %s", task.Status, task.Outcome.RC, task.Outcome.Reason)
		}
		if err := h.inbox.store.savePoolAction(ctx, action); err != nil {
			return err
		}
	}
	text := fmt.Sprintf("[panewire] handed %s over to %s on %s (%s %s)", action.Job, handover.Job, handover.Reason, handover.Pool, handover.Cause)
	if handover.Outcome == handoverFailed {
		text = fmt.Sprintf("[panewire] hand-over of %s failed (%s): %s", action.Job, strings.TrimSpace(handover.Reason), action.Data.Text)
	}
	eventID := "pool-handover:" + action.Job
	owned, err := h.inbox.store.poolNoticeOwned(ctx, action.Data.Owner, eventID)
	if err != nil {
		return err
	}
	if !owned {
		result := h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: eventID, JobID: laneEventTransportID(action.Data.Owner, eventID), OwnerLane: action.Data.Owner, NotifyType: "info", Text: utf8PrefixWithin(text, laneEventTextLimit), Host: "hub", Epoch: 1}, nil)
		if result.ID == 0 || result.PersistFailed {
			return errors.New("owner notice unavailable")
		}
	}
	h.logger.Info("pool hand-over finished", "job", action.Job, "successor", handover.Job, "outcome", handover.Outcome, "action", action.ID)
	action.Phase = poolActionDone
	if err := h.inbox.store.savePoolAction(ctx, action); err != nil {
		return err
	}
	h.poolActionObserved.Delete(action.ID)
	h.loopNoteDone(loopPoolActions, action.ID)
	return nil
}

// handoverEndReason is the old record's pinned end_reason
// (TestQuotaStopEndReasonFormatIsPinned).
func handoverEndReason(record map[string]any, exhaustion poolExhaustion, newJob string) string {
	pool, _ := record["pool"].(string)
	if !spawnargs.ValuePattern.MatchString(pool) {
		pool = exhaustion.Pool
	}
	return fleet.QuotaStopEndReason(pool, time.Time{}, "replaced_by", newJob)
}

// handoverStillDue is checked before the first close command of a handover
// row. The stop must still hold, and the worktree must be the one whose
// patch the successor names: the node captures again and the digest must
// match. A drain holds the row (no close while no successor can be claimed).
func (h *HubServer) handoverStillDue(ctx context.Context, action poolAction) (bool, error) {
	if h.spawnGrantDrain || h.spawnRollbackDrain {
		return false, errors.New("hand-over held while grant rows drain")
	}
	handover := action.Data.Handover
	if handover == nil {
		return false, nil
	}
	if h.poolEventGone(handover.exhaustion(), h.now()) {
		h.logger.Info("pool hand-over withdrawn: the stop no longer holds", "job", action.Job, "action", action.ID)
		return false, nil
	}
	var patch nodeHandoverPatchResult
	if !h.nodeCall(ctx, action.Node, nodeCommandHandoverPatch, nodeHandoverPatchArgs{Job: action.Job, Cwd: handover.Cwd, Branch: handover.Branch}, hubNodeCommandWait, &patch) {
		return false, errors.New("hand-over patch unavailable")
	}
	if patch.Outcome != nodeHandoverPatchSaved || patch.Digest != handover.Digest {
		h.logger.Info("pool hand-over withdrawn: the worktree changed since its patch", "job", action.Job, "action", action.ID, "outcome", patch.Outcome)
		return false, nil
	}
	return true, nil
}

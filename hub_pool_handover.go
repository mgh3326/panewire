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
	// Reset is the wait row's reset (unix seconds).
	Reset int64 `json:",omitempty"`
}

// handoverInputs is what the §Implementer rule reads from hk: the quota
// policy and the task's priority and expected time.
type handoverInputs struct {
	policy   quotaPolicy
	policyOK bool
	priority int
	expected time.Duration
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
		}
		if json.Unmarshal(raw, &t) == nil {
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
			// This stop is the one already waited out.
			return poolJobSkip(role, "waiting")
		}
		waits++
	}
	in := h.readHandoverInputs(ctx, prior.Task)
	decision, why := quotaDecisionJob(in.policy, in.policyOK, in.priority, in.expected, task.Created, waits, quotaStop{Reset: exhaustion.Reset}, h.now())
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
	text := fmt.Sprintf("[panewire] %s waits for the %s reset at %s (%s %s); a later stop hands it over.", task.Job, exhaustion.Pool, exhaustion.Reset.UTC().Format(time.RFC3339), exhaustion.Pool, exhaustion.Cause)
	eventID := fmt.Sprintf("pool-wait:%s:%d", task.Job, exhaustion.Reset.Unix())
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
	checkout := old.Repository.Checkout
	if checkout == nil || checkout.Branch == "" || checkout.Origin == "" {
		return nil, "no_branch"
	}
	host, label := spawnHolderHost(task), heldAgentName(task, record)
	probe := h.probeHeldPane(ctx, host, task.Outcome.Pane, label)
	if probe.state != heldPanePresent || !validNodeCommandPath(probe.row.Cwd) {
		return nil, "pane_unavailable"
	}
	var patch nodeHandoverPatchResult
	if !h.nodeCall(ctx, host, nodeCommandHandoverPatch, nodeHandoverPatchArgs{Job: task.Job, Cwd: probe.row.Cwd, Branch: checkout.Branch}, hubNodeCommandWait, &patch) || patch.Outcome != nodeHandoverPatchSaved {
		return nil, "patch_unavailable"
	}
	exclude := exhaustion.Pool
	if exhaustion.accountCredit() {
		// The account mark keeps the successor off the spent account while
		// the pool's other accounts stay placeable (hk 5138).
		exclude = ""
	}
	request, err := handoverSuccessorRequest(old, task.Job, newJob, exclude, exhaustion, patch)
	if err != nil {
		return nil, "invalid_successor"
	}
	if next, err := fleet.ValidateSubmission(request); err != nil || !validLaneReference(next.Owner) {
		return nil, "invalid_successor"
	}
	return &poolHandover{Successor: request, Job: newJob, Pool: exhaustion.Pool, Cause: exhaustion.Cause}, ""
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
	// The successor holds no local checkout: drop -c and --task.
	for _, flag := range []string{"-c", "--task"} {
		if i := slices.Index(next.Args, flag); i >= 0 && i+1 < len(next.Args) {
			next.Args = slices.Delete(next.Args, i, i+2)
		}
	}
	next.Args = append(next.Args, "--parent-job", oldJob)
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
	switch {
	case !patch.Dirty:
		b.WriteString(" The old worktree held no other work.")
	case patch.Doc != "":
		fmt.Fprintf(&b, " The old worktree's uncommitted and unpushed work is saved as hk evidence %s (git status, untracked files, git diff --binary); apply it with git apply.", patch.Doc)
	default:
		fmt.Fprintf(&b, " The old worktree's uncommitted and unpushed work is saved on the old host at %s (the hk upload failed).", patch.Saved)
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

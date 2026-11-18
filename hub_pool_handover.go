package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
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
// owner's fallback notice. A non-final handover row owns the old job
// (poolActionOwns): no reassignment, quota replacement, sweep or role release
// touches it, and its task role passes to the successor in the successor's
// own claim. Just before the close, with the pane idle, the old node
// snapshots the worktree into a commit pushed to
// refs/panewire/handover/<job>, and the stored note is rewritten to name it.

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
	// Task is the effective task the old job holds: the close is withdrawn
	// once hk binds it to another job.
	Task string `json:",omitempty"`
	// Cwd and Branch bind the close to the worktree the successor continues;
	// Brief is the old brief the continuation note is appended to.
	Cwd, Branch, Brief string `json:",omitempty"`
	// Snapshot is the old node's snapshot taken at the close.
	Snapshot *nodeHandoverSnapshotResult `json:",omitempty"`
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
	if owner, err := h.inbox.store.poolActionOwns(ctx, task.Job); err != nil || owner != "" {
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
			return h.notifyPoolWait(ctx, exhaustion, task, poolWaitReason)
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
	handover, refusal := h.prepareHandover(ctx, exhaustion, task, prior, record, newJob, taskID)
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
		return poolJobSkip(role, "waiting")
	}
	text := fmt.Sprintf("[panewire] %s waits for the %s reset at %s (%s %s); a later stop hands it over.", task.Job, exhaustion.Pool, exhaustion.Reset.UTC().Format(time.RFC3339), exhaustion.Pool, exhaustion.Cause)
	result := h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: eventID, JobID: laneEventTransportID(task.Owner, eventID), OwnerLane: task.Owner, NotifyType: "info", Text: utf8PrefixWithin(text, laneEventTextLimit), Host: "hub", Epoch: 1}, nil)
	if result.ID == 0 || result.PersistFailed {
		return poolJobSkip(role, "owner_not_notified")
	}
	return poolJobResult{action: poolJobNotified, role: role, why: why}
}

// prepareHandover reads the old worktree's branch and start point through
// its node and builds the successor submission. It changes nothing: the
// snapshot is taken only at the close (handoverBeforeClose).
func (h *HubServer) prepareHandover(ctx context.Context, exhaustion poolExhaustion, task hubSpawnTask, prior fleet.Spawn, record map[string]any, newJob, taskID string) (*poolHandover, string) {
	if _, err := h.inbox.store.latestSpawnForJob(ctx, newJob); err == nil {
		// A quota-stop replacement already placed this job's successor.
		return nil, "already_replaced"
	} else if !errors.Is(err, sql.ErrNoRows) {
		// Only a definite absence lets a hand-over start.
		return nil, "already_replaced"
	}
	old := task.Request
	if prior.Task == "" && prior.ParentJob == "" {
		return nil, "not_replaceable"
	}
	if prior.Host != "" && prior.Host != "auto" {
		// A pinned host cannot move off the old host; the owner decides. A
		// pinned profile is dropped instead (handoverSuccessorRequest): the
		// successor is placed automatically, off the spent pool.
		return nil, "pinned"
	}
	if !validNodeEndReason(handoverEndReason(record, exhaustion, newJob)) {
		return nil, "end_reason_too_long"
	}
	checkout := old.Repository.Checkout
	// A successor that started detached (a never-pushed branch) has no
	// branch in its checkout; the node then reports the branch it made.
	if checkout == nil || checkout.Origin == "" || checkout.Branch == "" && checkout.Mode != "detached" {
		return nil, "no_branch"
	}
	host, label := spawnHolderHost(task), heldAgentName(task, record)
	probe := h.probeHeldPane(ctx, host, task.Outcome.Pane, label)
	if probe.state != heldPanePresent || !validNodeCommandPath(probe.row.Cwd) {
		return nil, "pane_unavailable"
	}
	var read nodeHandoverSnapshotResult
	if !h.nodeCall(ctx, host, nodeCommandHandoverSnapshot, nodeHandoverSnapshotArgs{Job: task.Job, Cwd: probe.row.Cwd, Branch: checkout.Branch}, hubNodeCommandWait, &read) {
		return nil, "snapshot_unavailable"
	}
	if read.Outcome != nodeHandoverSnapshotSaved {
		return nil, "snapshot_" + read.Outcome
	}
	if base := filepath.Base(read.Top); base != checkout.Dir && base != old.Repository.Worktree {
		// The pane sits in another checkout than the job's own.
		return nil, "worktree_mismatch"
	}
	request, err := handoverSuccessorRequest(old, task.Job, newJob, handoverExclude(exhaustion), exhaustion, read)
	if err != nil {
		return nil, "invalid_successor"
	}
	if next, err := fleet.ValidateSubmission(request); err != nil || !validLaneReference(next.Owner) {
		return nil, "invalid_successor"
	}
	if label := hubSpawnArg(request.Args, "-l"); label != "" && !fleet.ValidAgentName(label) {
		return nil, "invalid_label"
	}
	reset := int64(0)
	if !exhaustion.Reset.IsZero() {
		reset = exhaustion.Reset.Unix()
	}
	return &poolHandover{Successor: request, Job: newJob, Pool: exhaustion.Pool, Cause: exhaustion.Cause, Reset: reset, Account: exhaustion.Account,
		Task: taskID, Cwd: probe.row.Cwd, Branch: read.Branch, Brief: old.Brief}, ""
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

// herdrAgentNameMax is herdr's agent name limit (fleet.ValidAgentName).
const herdrAgentNameMax = 32

// handoverSuccessorRequest is the stored successor: the quota replacement's
// submission (same role, owner and brief, job <old>-q<n>, the spent pool
// excluded) as a spec checkout of the branch at its last pushed commit, with
// --parent-job <old> in place of --task (the two are exclusive; the task is
// inherited) and a continuation note.
func handoverSuccessorRequest(old fleet.Submission, oldJob, newJob, exclude string, exhaustion poolExhaustion, read nodeHandoverSnapshotResult) (fleet.Submission, error) {
	next, err := quotaReplacementRequest(old, oldJob, newJob, exhaustion.Pool, exclude, "implementer")
	if err != nil {
		return fleet.Submission{}, err
	}
	// The successor holds no local checkout, inherits the task and is placed
	// automatically: drop -c, --task, a pinned -m and any earlier
	// --parent-job, so exactly one names oldJob.
	for _, flag := range []string{"-c", "--task", "--parent-job", "-m"} {
		for i := slices.Index(next.Args, flag); i >= 0 && i+1 < len(next.Args); i = slices.Index(next.Args, flag) {
			next.Args = slices.Delete(next.Args, i, i+2)
		}
	}
	next.Args = append(next.Args, "--parent-job", oldJob)
	if !strings.HasPrefix(hubSpawnArg(next.Args, "--report-relative"), newJob+"/") {
		// A --parent-job spawn requires a report under its own job or its
		// parent's: it lives in the successor's own job directory, whatever
		// the stopped job inherited.
		for i := slices.Index(next.Args, "--report-relative"); i >= 0 && i+1 < len(next.Args); i = slices.Index(next.Args, "--report-relative") {
			next.Args = slices.Delete(next.Args, i, i+2)
		}
		next.Args = append(next.Args, "--report-relative", newJob+"/report.md")
	}
	if i := slices.Index(next.Args, "-l"); i >= 0 && i+1 < len(next.Args) && len(next.Args[i+1]) > herdrAgentNameMax {
		// herdr's 32-character name rule: cut the inherited label and keep
		// the -q<n> suffix that tells the successor apart.
		suffix := strings.TrimPrefix(newJob, oldJob)
		base := strings.TrimSuffix(next.Args[i+1], suffix)
		next.Args[i+1] = strings.TrimRight(utf8PrefixWithin(base, herdrAgentNameMax-len(suffix)), "-_") + suffix
	}
	// The old job already passed its start gate; its conditions name the task
	// the successor no longer carries.
	next.StartConditions = nil
	dir := fleet.CheckoutWorktreeName(newJob)
	checkout := &fleet.HubCheckout{Origin: old.Repository.Checkout.Origin, Mode: fleet.HubCheckoutSuccessor, Branch: read.Branch, Sha: read.Pushed, Dir: dir}
	if read.Pushed == "" {
		// Nothing was pushed: start from the branch's base; the snapshot is
		// the whole work.
		checkout = &fleet.HubCheckout{Origin: old.Repository.Checkout.Origin, Mode: "detached", Sha: read.Base, Fetch: read.BaseRef, Dir: dir}
	}
	next.Repository = fleet.Repository{Name: old.Repository.Name, Origin: checkout.Origin, Worktree: dir, Checkout: checkout}
	next.Brief = handoverBrief(old.Brief, oldJob, "", exhaustion, read)
	return next, nil
}

// handoverBrief is the old brief with the continuation note appended. host
// is the old node; empty before the close, when no snapshot exists yet.
func handoverBrief(brief, oldJob, host string, exhaustion poolExhaustion, snap nodeHandoverSnapshotResult) string {
	return strings.TrimRight(brief, "\n") + "\n\n" + handoverNote(oldJob, host, exhaustion, snap) + "\n"
}

// handoverNote is the continuation note appended to the successor's brief.
func handoverNote(oldJob, host string, exhaustion poolExhaustion, snap nodeHandoverSnapshotResult) string {
	var b strings.Builder
	branch := snap.Branch
	fmt.Fprintf(&b, "## Hand-over from %s\nThe hub handed job %s over to you: its pool %s ran out (%s). ", oldJob, oldJob, exhaustion.Pool, exhaustion.Cause)
	if snap.Pushed != "" {
		fmt.Fprintf(&b, "Continue branch %s from its last pushed commit %s; this worktree is that branch.", branch, snap.Pushed)
	} else {
		fmt.Fprintf(&b, "Branch %s was never pushed: this worktree is detached at its base %s (%s); create branch %s from the snapshot below and push it.", branch, snap.Base, strings.TrimPrefix(snap.BaseRef, "refs/heads/"), branch)
	}
	switch {
	case host == "":
		b.WriteString(" The hub snapshots the old worktree when it closes the old pane and names the snapshot here.")
	case snap.Ref != "":
		fmt.Fprintf(&b, " The old worktree's unpushed commits and uncommitted changes to tracked files are commit %s at %s on origin. Restore them first: git fetch origin %s && git checkout -B %s FETCH_HEAD && git reset --mixed HEAD~1 (the branch keeps the old commits, the work comes back uncommitted, and history stays linear).", snap.Commit, snap.Ref, shellWord(snap.Ref), shellWord(branch))
	case !snap.Dirty:
		b.WriteString(" The old worktree held no other work.")
	}
	if len(snap.Untracked) > 0 || snap.UntrackedMore > 0 {
		b.WriteString(" Untracked files are never pushed; these stay only in the old worktree: ")
		b.WriteString(handoverUntrackedList(snap.Untracked, snap.UntrackedMore))
		b.WriteString(".")
	}
	if snap.Kept {
		if snap.Ref == "" && snap.Dirty {
			b.WriteString(" The snapshot could not be pushed.")
		}
		if snap.Submodule {
			b.WriteString(" A submodule held changes the snapshot does not carry.")
		}
		fmt.Fprintf(&b, " The old worktree is kept at %s:%s; copy from there what the snapshot lacks (ignored files, Git LFS objects, clean-filtered files, nested repositories, anything written after the snapshot).", host, snap.Top)
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
		case handover.Snapshot == nil:
			// Closed without a snapshot: its successor would not know where
			// the old work is.
			handover.Outcome, handover.Reason = handoverFailed, "no snapshot"
		case !handover.Submitted:
			// The stored request id makes the claim idempotent: after a crash
			// between the claim and the flag the replay finds the same row.
			// The old job's task role passes to the successor in the same
			// transaction, so no other spawn takes the task between them.
			if _, _, err := h.inbox.store.claimHandoverSuccessor(ctx, handover.Successor, handover.Job, action.Data.Owner, action.Data.SpawnRequest, h.now().UTC()); err != nil {
				if _, invalid := fleet.ValidateSubmission(handover.Successor); invalid != nil {
					handover.Outcome, handover.Reason = handoverFailed, "invalid_successor"
					break
				}
				if !errors.Is(err, errSpawnClaimed) {
					return err
				}
				// A sibling row (a close retry copies the row and its request
				// id) may have claimed the same successor first: that is this
				// hand-over's successor, tracked to its own outcome.
				if _, known := h.inbox.store.spawnTask(ctx, handover.Successor.RequestID); known != nil {
					handover.Outcome, handover.Reason = handoverFailed, "claim_refused"
					break
				}
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
	if handover.Outcome == handoverFailed {
		// No successor holds the task: free the role wherever it sits, the
		// old job's (never transferred) or a successor's that failed. A
		// successor that succeeded (a sibling row placed it) keeps its role.
		ids := []string{action.Data.SpawnRequest}
		if task, err := h.inbox.store.spawnTask(ctx, handover.Successor.RequestID); err == nil && task.Status != "succeeded" {
			ids = append(ids, handover.Successor.RequestID)
		}
		for _, id := range ids {
			if err := h.inbox.store.releaseCompletedSpawnRoleOf(ctx, id); err != nil {
				return err
			}
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

// handoverBeforeClose runs under the job's pool-action lock just before the
// first close command of a handover row, while the node has registered no
// close. A drain holds the row. The stop must still hold (else the row ends
// "recovered"), hk must still bind the task to the old job, and the pane must
// be the captured one and idle. Then the old node snapshots the worktree, the
// start point must be the stored successor's, and the successor's note is
// rewritten to name the snapshot before anything closes.
func (h *HubServer) handoverBeforeClose(ctx context.Context, action *poolAction) (bool, error) {
	if h.spawnGrantDrain || h.spawnRollbackDrain || h.inbox.store.spawnFrozen {
		return false, errors.New("hand-over held while spawn rows drain")
	}
	handover := action.Data.Handover
	if handover == nil || handover.Successor.Repository.Checkout == nil {
		return false, nil
	}
	withdraw := func(reason string) (bool, error) {
		handover.Reason = reason
		h.logger.Info("pool hand-over withdrawn before the close", "job", action.Job, "action", action.ID, "reason", reason)
		return false, nil
	}
	exhaustion := handover.exhaustion()
	if h.poolEventGone(exhaustion, h.now()) || exhaustion.poolWide() && !h.poolSweep.held(exhaustion.key()) {
		// A pool-wide cause is held until the pool comes back (or a hub
		// restart forgets it; the next sweep then decides afresh).
		return withdraw("recovered")
	}
	if in := h.readHandoverInputs(ctx, handover.Task); in.bound != "" && in.bound != action.Job {
		return withdraw("task_rebound")
	}
	probe := h.probeHeldPane(ctx, action.Node, action.Data.Pane, action.Data.Label)
	switch {
	case probe.state == heldPaneAbsent || probe.state == heldPanePresent && (probe.row.PaneID != action.Data.Pane || probe.row.Cwd != handover.Cwd):
		return withdraw("pane_changed")
	case probe.state != heldPanePresent:
		return false, errors.New("pane unavailable")
	case !nodePaneIdleStatus[probe.row.AgentStatus]:
		// The snapshot must be the pane's final work: wait for it to idle.
		return false, errors.New("hand-over held until the pane is idle")
	}
	var snap nodeHandoverSnapshotResult
	if !h.nodeCall(ctx, action.Node, nodeCommandHandoverSnapshot, nodeHandoverSnapshotArgs{Job: action.Job, Cwd: handover.Cwd, Branch: handover.Branch, Snapshot: true}, hubNodeCommandWait, &snap) {
		return false, errors.New("hand-over snapshot unavailable")
	}
	switch {
	case snap.Outcome == nodeHandoverSnapshotGitError:
		return false, errors.New("hand-over snapshot failed")
	case snap.Outcome != nodeHandoverSnapshotSaved:
		return withdraw("snapshot_" + snap.Outcome)
	case handoverStartsAt(snap) != handover.Successor.Repository.Checkout.Sha:
		return withdraw("branch_moved")
	}
	successor := handover.Successor
	successor.Brief = handoverBrief(handover.Brief, action.Job, action.Node, exhaustion, snap)
	if _, err := fleet.ValidateSubmission(successor); err != nil {
		// The final note must still fit the submission (the brief cap):
		// nothing closes for a successor that could never be claimed.
		return withdraw("invalid_successor")
	}
	handover.Snapshot, handover.Successor = &snap, successor
	if err := h.inbox.store.savePoolAction(ctx, *action); err != nil {
		return false, err
	}
	return true, nil
}

// handoverStartsAt is the commit a successor starts from: the pushed commit,
// or the base when nothing was pushed.
func handoverStartsAt(snap nodeHandoverSnapshotResult) string {
	if snap.Pushed != "" {
		return snap.Pushed
	}
	return snap.Base
}

// shellWord quotes s as one POSIX shell word: a branch name may hold shell
// metacharacters the successor must not run.
func shellWord(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// handoverNoteListBytes bounds the untracked list in the note, so the brief
// keeps its room under the submission cap.
const handoverNoteListBytes = 2048

// handoverUntrackedList joins paths, quoted, up to handoverNoteListBytes and
// counts the rest.
func handoverUntrackedList(paths []string, more int) string {
	var b strings.Builder
	for i, path := range paths {
		item := strconv.Quote(path)
		if b.Len()+len(item)+2 > handoverNoteListBytes {
			more += len(paths) - i
			break
		}
		if b.Len() > 0 {
			b.WriteString(", ")
		}
		b.WriteString(item)
	}
	if more > 0 {
		fmt.Fprintf(&b, " and %d more", more)
	}
	return b.String()
}

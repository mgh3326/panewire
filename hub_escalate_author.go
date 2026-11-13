package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/grade"
)

// `POST /v1/jobs/escalate-author` is the hub half of the review-round rule
// (hk:doc policy/model-families §The escalation ladder, operator decisions
// 137, 146 and 148): at round 5 mergewake's `panewire escalate-author` call
// asks the hub to start the author again one ladder step up. The CLI resolved
// the rung and the consult seat (it has the catalog and the selection core);
// the hub owns what only it has — the replaced job's stored submission, its
// checkout and its pane — so the successor runs on the same worktree and
// branch and one job holds the task.
//
// The hub validates the successor and prepares its worktree first, then closes
// the old pane, marks the old record lost with the successor's pointer,
// releases the old role, claims the successor as an inherited (--parent-job)
// spawn and waits for its placement. A successor that does not place is a
// quota_refused refusal that says the author is gone. The design lane gets one
// info event for every outcome, never an interrupt.

const (
	// escalateAuthorWait is how long the hub waits for the successor's
	// placement. It bounds the HTTP call the CLI makes.
	escalateAuthorWait = 150 * time.Second
	// escalateAuthorMaxStep is the number of ladder steps one job chain takes.
	// The policy's round 7 is the design session's hold, not another step.
	escalateAuthorMaxStep = 1
	// escalateAuthorFindingsMax mirrors the CLI's findings bound.
	escalateAuthorFindingsMax = 32 << 10
	// ladderStepReasonPrefix opens the override reason a ladder-step spawn
	// carries (internal/fleet/outcome_ladder.go writes the rep row from it).
	ladderStepReasonPrefix = "ladder step "
)

var escalateAuthorPRPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+#[1-9][0-9]*$`)

type hubEscalateAuthorRequest struct {
	Task        string `json:"task"`
	Job         string `json:"job"`
	PR          string `json:"pr"`
	Round       int    `json:"round"`
	Findings    string `json:"findings"`
	Grade       string `json:"grade"`
	FromProfile string `json:"from_profile"`
	FromEffort  string `json:"from_effort"`
	Profile     string `json:"profile"`
	Effort      string `json:"effort"`
	Step        string `json:"step"`
	ConsultSeat string `json:"consult_seat"`
}

type hubEscalateAuthorResult struct {
	Job    string `json:"job,omitempty"`
	Rung   string `json:"rung,omitempty"`
	Code   string `json:"code,omitempty"`
	Reason string `json:"reason,omitempty"`
	// AuthorEnded marks a refusal after the old pane was closed: no author
	// runs on the task, whatever the code says.
	AuthorEnded bool `json:"author_ended,omitempty"`
}

func (h *HubServer) handleEscalateAuthor(w http.ResponseWriter, r *http.Request) {
	if h.authenticate(r).kind != hubCallerOperator {
		hubUnauthorized(w)
		return
	}
	var request hubEscalateAuthorRequest
	if !decodeHubJSON(w, r, hubSpawnMaxBodyBytes, &request) {
		return
	}
	if reason := request.refusal(); reason != "" {
		writeJSON(w, http.StatusBadRequest, hubEscalateAuthorResult{Code: "invalid_request", Reason: reason})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), escalateAuthorWait+30*time.Second)
	defer cancel()
	result, err := h.escalateAuthor(ctx, request)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, hubEscalateAuthorResult{Code: "hub_error", Reason: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// refusal validates the request shape. The findings text is already bounded by
// the CLI; the hub bounds it again because the route is reachable directly.
func (r hubEscalateAuthorRequest) refusal() string {
	switch {
	case !escalateAuthorNumeric(r.Task):
		return "task must be a task id"
	case !hubJobIDPattern.MatchString(r.Job):
		return "job must be a job id"
	case !escalateAuthorPRPattern.MatchString(r.PR):
		return "pr must be owner/repo#N"
	case r.Round < 1:
		return "round must be at least 1"
	case r.Findings == "" || len(r.Findings) > escalateAuthorFindingsMax:
		return "findings must be a non-empty JSON array within the bound"
	case json.Unmarshal([]byte(r.Findings), &[]json.RawMessage{}) != nil:
		return "findings must be a JSON array"
	case r.Grade == "":
		return "grade is required"
	}
	return ""
}

// escalateAuthor starts the successor or returns a refusal. Everything that
// can be checked is checked before the old pane is touched: the successor's
// submission, the rung, the pane and the worktree. A refusal before the pane
// close changes nothing and the PR goes on to its next round. A refusal after
// it (a successor that does not place, a claim the store refuses, a record
// that cannot be written) has ended the author; it carries AuthorEnded, and a
// replay of the same (job, round) finishes a step that stopped half way.
func (h *HubServer) escalateAuthor(ctx context.Context, request hubEscalateAuthorRequest) (hubEscalateAuthorResult, error) {
	holder, err := h.inbox.store.latestSpawnForJob(ctx, request.Job)
	if err != nil {
		return h.escalateAuthorRefusal(ctx, request, "", "job_unknown", "no spawn operation for "+request.Job, false)
	}
	spawn, err := fleet.ValidateSubmission(holder.Request)
	if err != nil {
		return h.escalateAuthorRefusal(ctx, request, "", "job_unreadable", err.Error(), false)
	}
	if spawn.Job != request.Job || spawn.Task != request.Task || spawn.Role != "worker" || spawn.ParentJob != "" {
		return h.escalateAuthorRefusal(ctx, request, "", "job_mismatch", fmt.Sprintf("job %s is %s for task %s, not a live worker on task %s", request.Job, spawn.Role, spawn.Task, request.Task), false)
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return hubEscalateAuthorResult{}, err
	}
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	record, exists, readErr := app.ExistingJob(readCtx, request.Job)
	cancel()
	if readErr != nil || !exists {
		return h.escalateAuthorRefusal(ctx, request, "", "job_unreadable", "the job's hk record is unreadable", false)
	}
	ownerLane := firstNonempty(escalateAuthorStr(record, "owner_lane"), holder.Owner)
	newJob, _, ok := escalateAuthorJob(request.Job)
	if !ok {
		return h.escalateAuthorRefusal(ctx, request, ownerLane, "no_rung", "the ladder step limit is reached", false)
	}
	// A replay after a lost answer finds its own successor, before the old
	// record's state is read: a step that got as far as the successor has
	// already ended the old record. A placed successor is the step, a failed
	// one is its refusal, and one still running is the step it will report
	// when it settles.
	if prior, err := h.inbox.store.latestSpawnForJob(ctx, newJob); err == nil {
		rung := firstNonempty(stepValue(prior.Request.Args, "-m"), request.Profile) + "@" + firstNonempty(stepValue(prior.Request.Args, "--effort"), "default")
		if prior.Status == "failed" || prior.Status == "retryable" {
			return h.escalateAuthorRefusal(ctx, request, ownerLane, "quota_refused", firstNonempty(prior.Outcome.Reason, "the successor did not place"), true)
		}
		return hubEscalateAuthorResult{Job: newJob, Rung: rung}, nil
	}
	state, _ := record["state"].(string)
	// A record this step already ended, with no successor claimed, is a step
	// the hub stopped in the middle (a restart between the record write and
	// the claim): the pane is closed and only the claim is left.
	resume := state == "lost" && escalateAuthorStr(record, "end_reason") == fleet.LadderStepEndReason(request.Round, newJob)
	if state != "claimed" && state != "spawned" && !resume {
		return h.escalateAuthorRefusal(ctx, request, ownerLane, "job_not_live", "the job's record is "+state, false)
	}
	if profile := escalateAuthorStr(record, "profile"); profile != "" && profile != request.FromProfile {
		return h.escalateAuthorRefusal(ctx, request, ownerLane, "job_changed", fmt.Sprintf("the job now runs %s, not %s", profile, request.FromProfile), false)
	}
	if effort := escalateAuthorStr(record, "effort"); effort != "" && request.FromEffort != "" && effort != request.FromEffort {
		return h.escalateAuthorRefusal(ctx, request, ownerLane, "job_changed", fmt.Sprintf("the job now runs at %s, not %s", effort, request.FromEffort), false)
	}
	if request.Profile == "" || request.Step == "" {
		return h.escalateAuthorRefusal(ctx, request, ownerLane, "no_rung", "the policy has no step left for this seat", false)
	}
	// The rung the CLI named must still be a live, ordinary or escalation rung
	// that covers the task grade. The CLI resolved it from the same catalog,
	// so a miss is a stale request.
	if app.Quota == nil {
		return hubEscalateAuthorResult{}, fmt.Errorf("the hub has no quota catalog")
	}
	rows, err := app.Quota.Catalog(ctx)
	if err != nil {
		return hubEscalateAuthorResult{}, err
	}
	if !escalateAuthorRung(rows, request) {
		return h.escalateAuthorRefusal(ctx, request, ownerLane, "no_rung", fmt.Sprintf("%s@%s is not a live rung covering grade %s", request.Profile, request.Effort, request.Grade), false)
	}
	// The successor is built and validated the way the claim will validate it,
	// before anything is closed: a submission the store would refuse must not
	// cost the author its pane.
	next, err := escalateAuthorSubmission(holder.Request, request, newJob)
	if err != nil {
		return hubEscalateAuthorResult{}, err
	}
	if _, err := fleet.ValidateSubmission(next); err != nil {
		return h.escalateAuthorRefusal(ctx, request, ownerLane, "successor_invalid", err.Error(), false)
	}
	name := heldAgentName(holder, record)
	if !resume {
		probe := h.probeHeldPane(ctx, spawnHolderHost(holder), holder.Outcome.Pane, name)
		if probe.state == heldPaneUnknown {
			return h.escalateAuthorRefusal(ctx, request, ownerLane, "pane_unavailable", "the job's pane is "+probe.state.String()+" ("+probe.reason+")", false)
		}
		if probe.state == heldPanePresent && (!nodePaneIdleStatus[probe.row.AgentStatus] || !validNodeCommandPath(probe.row.Cwd)) {
			return h.escalateAuthorRefusal(ctx, request, ownerLane, "pane_busy", "the job's pane is "+probe.row.AgentStatus, false)
		}
		// The successor reuses the worktree: refuse here, with the author's
		// pane open, anything its spawn would refuse once the pane is gone.
		if code, why := h.escalateAuthorWorktreeUsable(ctx, holder, next.Repository.Checkout); code != "" {
			return h.escalateAuthorRefusal(ctx, request, ownerLane, code, why, false)
		}
		endReason := fleet.LadderStepEndReason(request.Round, newJob)
		if len(endReason) > 128 || !validNodeEndReason(endReason) {
			return hubEscalateAuthorResult{}, fmt.Errorf("ladder-step end reason is invalid")
		}
		// Close the old pane before the successor exists, so one job holds the
		// task. A pane that changed since the probe is refused by pane.close
		// and nothing is placed. A pane already absent (an earlier attempt
		// closed it and stopped before the record write) needs no close.
		if probe.state == heldPanePresent && !h.closeHeldPane(ctx, holder, name, probe.row) {
			return h.escalateAuthorRefusal(ctx, request, ownerLane, "pane_not_closed", "the job's pane could not be closed", false)
		}
		if lost, why := h.markHolderJobLostBecause(ctx, app, holder, name, endReason); !lost {
			return h.escalateAuthorRefusal(ctx, request, ownerLane, "job_not_marked", "the old record was not written lost: "+why, true)
		}
	}
	if requestID, _, _, err := h.inbox.store.spawnRoleOwner(ctx, spawn.RoleKey()); err == nil && requestID == holder.Request.RequestID {
		_ = h.inbox.store.releaseCompletedSpawnRole(ctx, requestID, spawn.RoleKey())
	}
	if _, _, err := h.inbox.store.claimSpawn(ctx, next, newJob, ownerLane, h.now().UTC()); err != nil {
		return h.escalateAuthorRefusal(ctx, request, ownerLane, "claim_refused", err.Error(), true)
	}
	select {
	case h.spawnKick <- struct{}{}:
	default:
	}
	task, settled := h.awaitSpawnOutcome(ctx, next.RequestID, escalateAuthorWait)
	if settled && task.Outcome.RC != 0 {
		return h.escalateAuthorRefusal(ctx, request, ownerLane, "quota_refused", firstNonempty(task.Outcome.Reason, fmt.Sprintf("placement exited %d", task.Outcome.RC)), true)
	}
	if !settled {
		h.logger.Warn("ladder step undecided; its successor may still place", "job", request.Job, "successor", newJob, "status", task.Status)
	}
	rung := request.Profile + "@" + firstNonempty(request.Effort, "default")
	text := fmt.Sprintf("ladder step %s -> %s on %s (round %d, %s)", request.Job, newJob, rung, request.Round, request.PR)
	h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: "ladder-step:" + newJob, JobID: newJob, OwnerLane: ownerLane, NotifyType: "info", Text: utf8PrefixWithin(text, laneEventTextLimit), Host: "hub", Epoch: 1}, nil)
	return hubEscalateAuthorResult{Job: newJob, Rung: rung}, nil
}

// escalateAuthorWorktreeUsable asks the holder's node to prepare the
// successor's checkout while the author's pane is still open. worktree.prepare
// applies the very refusals the successor's spawn would meet after the pane is
// gone (a dirty or locked worktree, a branch checked out elsewhere, a HEAD with
// commits origin does not have), and reusing an idle, clean worktree changes
// nothing the successor's spawn would not change. A job with no checkout spec
// ran in a canonical checkout and is not checked.
func (h *HubServer) escalateAuthorWorktreeUsable(ctx context.Context, holder hubSpawnTask, checkout *fleet.HubCheckout) (code, reason string) {
	if checkout == nil {
		return "", ""
	}
	var prepared nodeWorktreePrepareResult
	args := nodeWorktreePrepareArgs{Origin: checkout.Origin, Mode: checkout.Mode, Branch: checkout.Branch, Sha: checkout.Sha, Fetch: checkout.Fetch, Dir: checkout.Dir}
	if !h.nodeCall(ctx, spawnHolderHost(holder), nodeCommandWorktreePrepare, args, spawnHeldPaneActWait, &prepared) {
		return "worktree_unreadable", "the holder's node did not answer worktree.prepare"
	}
	switch prepared.Outcome {
	case nodePreparePrepared, nodePrepareReused:
		return "", ""
	case nodeOutcomeDirty:
		return "worktree_dirty", "the job's worktree has uncommitted changes"
	case nodeRemoveLocked:
		return "worktree_locked", "the job's worktree is locked"
	case nodePrepareBranchElsewhere:
		return "worktree_unpushed", firstNonempty(prepared.Reason, "the job's branch has commits origin does not have, or is checked out elsewhere")
	}
	return "worktree_unreadable", "worktree.prepare answered " + prepared.Outcome
}

// escalateAuthorRefusal records the refusal as one info event for the design
// lane and answers the CLI with its code. ended marks a refusal that follows
// the pane close: the old author is gone and the PR has none, which the info
// and the CLI say, because mergewake reads rc 2 either way.
func (h *HubServer) escalateAuthorRefusal(ctx context.Context, request hubEscalateAuthorRequest, owner, code, reason string, ended bool) (hubEscalateAuthorResult, error) {
	consequence := "the PR goes on to the next round"
	if ended {
		consequence = "the author's pane is closed and its record ended; no author runs on this PR now"
	}
	text := fmt.Sprintf("ladder step %s: %s (%s) (round %d, %s); %s", request.Job, code, reason, request.Round, request.PR, consequence)
	h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: fmt.Sprintf("ladder-step:%s:%d:%s:%t", request.Job, request.Round, code, ended), JobID: request.Job, OwnerLane: owner, NotifyType: "info", Text: utf8PrefixWithin(text, laneEventTextLimit), Host: "hub", Epoch: 1}, nil)
	return hubEscalateAuthorResult{Code: code, Reason: reason, AuthorEnded: ended}, nil
}

// escalateAuthorChain is the ladder-step and quota-stop suffix of a job id: the
// ladder step is one `-l<n>` followed by any quota-stop replacements `-q<n>`.
var escalateAuthorChain = regexp.MustCompile(`-l[0-9]+(-q[0-9]+)*$`)

// escalateAuthorJob names the successor of old: "<old>-l1". One ladder step per
// job chain is the policy's limit, so a job whose id already carries a ladder
// step (with or without later quota-stop replacements) has none left.
func escalateAuthorJob(old string) (job string, step int, ok bool) {
	step = 1
	if escalateAuthorChain.MatchString(old) {
		step = 2
	}
	if step > escalateAuthorMaxStep {
		return "", step, false
	}
	job = fmt.Sprintf("%s-l%d", old, step)
	return job, step, len(job) <= 128 && hubJobIDPattern.MatchString(job)
}

// escalateAuthorRung reports whether profile@effort is a live catalog rung
// with an ordinary or escalation gate whose grade covers the task.
func escalateAuthorRung(rows []fleet.CatalogRow, request hubEscalateAuthorRequest) bool {
	for _, row := range rows {
		if row.Profile != request.Profile || row.Effort != request.Effort || row.RetiredAt != nil {
			continue
		}
		if row.Gate != "default" && row.Gate != "escalation" {
			continue
		}
		if row.Grade == "unrated" || row.Grade == "" {
			continue
		}
		if gradeRank(row.Grade) < gradeRank(request.Grade) {
			continue
		}
		return true
	}
	return false
}

// gradeRank orders the task grades; an unknown grade ranks below every known
// one, so it never covers a task.
func gradeRank(value string) int {
	for rank, known := range grade.Grades() {
		if strings.EqualFold(known, value) {
			return rank + 1
		}
	}
	return 0
}

// escalateAuthorStr reads a string field from an hk job record.
func escalateAuthorStr(record map[string]any, key string) string {
	value, _ := record[key].(string)
	return value
}

// escalateAuthorNumeric reports a decimal task id.
func escalateAuthorNumeric(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// escalateAuthorSubmission rewrites the replaced job's submission for its
// successor: the new job, label, pinned rung, ladder-step reason and report
// path, the parent job in place of the task, and a brief that carries the old
// brief, the findings and the consult seat. The repository and checkout stay
// the same worktree and branch; a branch checkout drops its pinned sha, because
// the author has committed since that sha was taken and worktree.prepare would
// refuse a HEAD ahead of it: with no sha the node prepares against the fetched
// origin tip, which a pushed author's HEAD equals.
func escalateAuthorSubmission(old fleet.Submission, request hubEscalateAuthorRequest, newJob string) (fleet.Submission, error) {
	id, err := fleet.NewRequestID()
	if err != nil {
		return fleet.Submission{}, err
	}
	next := old
	next.RequestID = id
	next.RetryHost = ""
	if next.DeadlineSeconds < 1 || next.DeadlineSeconds > int(escalateAuthorWait/time.Second) {
		next.DeadlineSeconds = int(escalateAuthorWait / time.Second)
	}
	next.Args = escalateAuthorArgs(old.Args, request, newJob)
	next.Brief = escalateAuthorBrief(old.Brief, request)
	if old.Repository.Checkout != nil && old.Repository.Checkout.Mode == "branch" {
		checkout := *old.Repository.Checkout
		checkout.Sha = ""
		next.Repository.Checkout = &checkout
	}
	if arm := old.E6Arm; arm != "" && arm != request.Profile+"@" && arm != request.Profile+"@"+request.Effort {
		next.E6Arm = ""
	}
	if !validHubSpawnRequestID(next.RequestID) {
		return fleet.Submission{}, fmt.Errorf("invalid successor request id")
	}
	return next, nil
}

// escalateAuthorArgs rewrites the replaced job's spawn arguments for its
// successor.
func escalateAuthorArgs(old []string, request hubEscalateAuthorRequest, newJob string) []string {
	args := slices.Clone(old)
	set := func(flag, value string) {
		for i := 0; i+1 < len(args); i++ {
			if args[i] == flag {
				args[i+1] = value
				return
			}
		}
		args = append(args, flag, value)
	}
	remove := func(flag string) {
		for i := 0; i+1 < len(args); i++ {
			if args[i] == flag {
				args = append(args[:i], args[i+2:]...)
				return
			}
		}
	}
	has := func(flag string) bool { return slices.Contains(args, flag) }
	set("--job", newJob)
	if label := hubSpawnArg(old, "-l"); label != "" {
		set("-l", label+strings.TrimPrefix(newJob, request.Job))
	}
	set("-m", request.Profile)
	if request.Effort != "" {
		set("--effort", request.Effort)
	} else {
		remove("--effort")
	}
	set("--override-reason", fmt.Sprintf("%s%s@%s -> %s@%s, round %d, %s", ladderStepReasonPrefix, request.FromProfile, firstNonempty(request.FromEffort, "default"), request.Profile, firstNonempty(request.Effort, "default"), request.Round, request.PR))
	// The successor replaces the old job on its task, so it names the parent
	// rather than the task: the node rebinds the task once the old record is
	// lost, and the ladder-step rep row is written from --parent-job.
	remove("--task")
	set("--parent-job", request.Job)
	if report := hubSpawnArg(old, "--report-relative"); strings.HasPrefix(report, request.Job+"/") {
		set("--report-relative", newJob+strings.TrimPrefix(report, request.Job))
	}
	// A pinned rung needs the containment an auto spawn did not have.
	if !has("--containment") && !has("--operator-request") {
		set("--containment", "hk:"+request.Task)
	}
	// An experiment arm is not an arg; escalateAuthorSubmission settles it.
	return args
}

// escalateAuthorBrief is the successor's brief: the original brief, the
// findings so far and the consult seat the hub resolved. The findings and the
// consult line are kept whole; the original brief gives way if the whole would
// pass the hub's brief bound.
func escalateAuthorBrief(old string, request hubEscalateAuthorRequest) string {
	tail := fmt.Sprintf("\n\n## Ladder step (round %d, %s)\n\nThis PR has not passed after %d review rounds. You replace job %s at a higher rung: %s@%s -> %s@%s. You run on the same worktree and branch; continue from the branch as it stands, do not start over.\n\nFindings so far (JSON: ref, severity, path, line, title, body):\n%s\n", request.Round, request.PR, request.Round, request.Job, request.FromProfile, firstNonempty(request.FromEffort, "default"), request.Profile, firstNonempty(request.Effort, "default"), request.Findings)
	if request.ConsultSeat != "" {
		tail += "\nConsult seat: " + request.ConsultSeat + " (one grade above your seat, another family). Ask it for the root cause and give it every finding above.\n"
	}
	if len(tail) >= hubSpawnMaxBriefBytes {
		return utf8PrefixWithin(tail, hubSpawnMaxBriefBytes)
	}
	room := hubSpawnMaxBriefBytes - len(tail)
	return utf8PrefixWithin(old, room) + tail
}

// stepValue reads a flag's value from a spawn argument list, in either form.
func stepValue(args []string, flag string) string {
	for i, arg := range args {
		if value, ok := strings.CutPrefix(arg, flag+"="); ok {
			return value
		}
		if arg == flag && i+1 < len(args) {
			return args[i+1]
		}
	}
	return ""
}

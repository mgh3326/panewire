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
// The hub closes the old pane, marks the old record lost with the successor's
// pointer, releases the old role, claims the successor as an inherited
// (--parent-job) spawn and waits for its placement. A successor that does not
// place is a quota_refused refusal; nothing is left running. The design lane
// gets one info event for every outcome, never an interrupt.

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

// escalateAuthor starts the successor or returns a refusal. A refusal before
// the pane probe touches nothing; a refusal after it has closed the old pane
// and ended the old record lost (a quota_refused successor), so the PR goes on
// to its next round with one job still holding the task.
func (h *HubServer) escalateAuthor(ctx context.Context, request hubEscalateAuthorRequest) (hubEscalateAuthorResult, error) {
	holder, err := h.inbox.store.latestSpawnForJob(ctx, request.Job)
	if err != nil {
		return h.escalateAuthorRefusal(ctx, request, "", "job_unknown", "no spawn operation for "+request.Job)
	}
	spawn, err := fleet.ValidateSubmission(holder.Request)
	if err != nil {
		return h.escalateAuthorRefusal(ctx, request, "", "job_unreadable", err.Error())
	}
	if spawn.Job != request.Job || spawn.Task != request.Task || spawn.Role != "worker" || spawn.ParentJob != "" {
		return h.escalateAuthorRefusal(ctx, request, "", "job_mismatch", fmt.Sprintf("job %s is %s for task %s, not a live worker on task %s", request.Job, spawn.Role, spawn.Task, request.Task))
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return hubEscalateAuthorResult{}, err
	}
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	record, exists, readErr := app.ExistingJob(readCtx, request.Job)
	cancel()
	if readErr != nil || !exists {
		return h.escalateAuthorRefusal(ctx, request, "", "job_unreadable", "the job's hk record is unreadable")
	}
	ownerLane := firstNonempty(escalateAuthorStr(record, "owner_lane"), holder.Owner)
	if state, _ := record["state"].(string); state != "claimed" && state != "spawned" {
		return h.escalateAuthorRefusal(ctx, request, ownerLane, "job_not_live", "the job's record is "+state)
	}
	if profile := escalateAuthorStr(record, "profile"); profile != "" && profile != request.FromProfile {
		return h.escalateAuthorRefusal(ctx, request, ownerLane, "job_changed", fmt.Sprintf("the job now runs %s, not %s", profile, request.FromProfile))
	}
	if effort := escalateAuthorStr(record, "effort"); effort != "" && request.FromEffort != "" && effort != request.FromEffort {
		return h.escalateAuthorRefusal(ctx, request, ownerLane, "job_changed", fmt.Sprintf("the job now runs at %s, not %s", effort, request.FromEffort))
	}
	newJob, _, ok := escalateAuthorJob(request.Job)
	if !ok {
		return h.escalateAuthorRefusal(ctx, request, ownerLane, "no_rung", "the ladder step limit is reached")
	}
	// A replay after a lost answer finds its own successor: a placed one is the
	// step, a failed one is its refusal, and one still running is the step it
	// will report when it settles.
	if prior, err := h.inbox.store.latestSpawnForJob(ctx, newJob); err == nil {
		rung := firstNonempty(stepValue(prior.Request.Args, "-m"), request.Profile) + "@" + firstNonempty(stepValue(prior.Request.Args, "--effort"), "default")
		if prior.Status == "failed" || prior.Status == "retryable" {
			return h.escalateAuthorRefusal(ctx, request, ownerLane, "quota_refused", firstNonempty(prior.Outcome.Reason, "the successor did not place"))
		}
		return hubEscalateAuthorResult{Job: newJob, Rung: rung}, nil
	}
	if request.Profile == "" || request.Step == "" {
		return h.escalateAuthorRefusal(ctx, request, ownerLane, "no_rung", "the policy has no step left for this seat")
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
		return h.escalateAuthorRefusal(ctx, request, ownerLane, "no_rung", fmt.Sprintf("%s@%s is not a live rung covering grade %s", request.Profile, request.Effort, request.Grade))
	}
	name := heldAgentName(holder, record)
	probe := h.probeHeldPane(ctx, spawnHolderHost(holder), holder.Outcome.Pane, name)
	if probe.state != heldPanePresent {
		return h.escalateAuthorRefusal(ctx, request, ownerLane, "pane_unavailable", "the job's pane is "+probe.state.String()+" ("+probe.reason+")")
	}
	if !nodePaneIdleStatus[probe.row.AgentStatus] || !validNodeCommandPath(probe.row.Cwd) {
		return h.escalateAuthorRefusal(ctx, request, ownerLane, "pane_busy", "the job's pane is "+probe.row.AgentStatus)
	}
	next, err := escalateAuthorSubmission(holder.Request, request, newJob)
	if err != nil {
		return hubEscalateAuthorResult{}, err
	}
	// Close the old pane before the successor exists, so one job holds the
	// task. A pane that changed since the probe is refused by pane.close and
	// nothing is placed.
	if !h.closeHeldPane(ctx, holder, name, probe.row) {
		return h.escalateAuthorRefusal(ctx, request, ownerLane, "pane_not_closed", "the job's pane could not be closed")
	}
	endReason := escalateAuthorEndReason(request, newJob)
	if len(endReason) > 128 || !validNodeEndReason(endReason) {
		return hubEscalateAuthorResult{}, fmt.Errorf("ladder-step end reason is invalid")
	}
	if lost, why := h.markHolderJobLostBecause(ctx, app, holder, name, endReason); !lost {
		return h.escalateAuthorRefusal(ctx, request, ownerLane, "job_not_marked", "the old record was not written lost: "+why)
	}
	if requestID, _, _, err := h.inbox.store.spawnRoleOwner(ctx, spawn.RoleKey()); err == nil && requestID == holder.Request.RequestID {
		_ = h.inbox.store.releaseCompletedSpawnRole(ctx, requestID, spawn.RoleKey())
	}
	if _, _, err := h.inbox.store.claimSpawn(ctx, next, newJob, ownerLane, h.now().UTC()); err != nil {
		return h.escalateAuthorRefusal(ctx, request, ownerLane, "claim_refused", err.Error())
	}
	select {
	case h.spawnKick <- struct{}{}:
	default:
	}
	task, settled := h.awaitSpawnOutcome(ctx, next.RequestID, escalateAuthorWait)
	if settled && task.Outcome.RC != 0 {
		return h.escalateAuthorRefusal(ctx, request, ownerLane, "quota_refused", firstNonempty(task.Outcome.Reason, fmt.Sprintf("placement exited %d", task.Outcome.RC)))
	}
	if !settled {
		h.logger.Warn("ladder step undecided; its successor may still place", "job", request.Job, "successor", newJob, "status", task.Status)
	}
	rung := request.Profile + "@" + firstNonempty(request.Effort, "default")
	text := fmt.Sprintf("ladder step %s -> %s on %s (round %d, %s)", request.Job, newJob, rung, request.Round, request.PR)
	h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: "ladder-step:" + newJob, JobID: newJob, OwnerLane: ownerLane, NotifyType: "info", Text: utf8PrefixWithin(text, laneEventTextLimit), Host: "hub", Epoch: 1}, nil)
	return hubEscalateAuthorResult{Job: newJob, Rung: rung}, nil
}

// escalateAuthorRefusal records the refusal as one info event for the design
// lane and answers the CLI with its code. A refusal that follows the pane
// close has already ended the old job; one before it changed nothing.
func (h *HubServer) escalateAuthorRefusal(ctx context.Context, request hubEscalateAuthorRequest, owner, code, reason string) (hubEscalateAuthorResult, error) {
	text := fmt.Sprintf("ladder step %s: %s (%s) (round %d, %s); the PR goes on to the next round", request.Job, code, reason, request.Round, request.PR)
	h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: fmt.Sprintf("ladder-step:%s:%d", request.Job, request.Round), JobID: request.Job, OwnerLane: owner, NotifyType: "info", Text: utf8PrefixWithin(text, laneEventTextLimit), Host: "hub", Epoch: 1}, nil)
	return hubEscalateAuthorResult{Code: code, Reason: reason}, nil
}

// escalateAuthorJob names the successor of old: "<old>-l<n>", where n counts
// the ladder steps already in old's id. One step is the policy's limit.
func escalateAuthorJob(old string) (job string, step int, ok bool) {
	step = 1
	if strings.HasSuffix(old, "-l1") {
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
// brief, the findings and the consult seat. The repository and checkout are
// kept exactly, which is what puts the successor on the same worktree.
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
	// An experiment arm names one canon@effort; it does not follow a rung.
	if arm := hubSpawnArg(old, "--e6-arm"); arm != "" && arm != request.Profile+"@" && arm != request.Profile+"@"+request.Effort {
		remove("--e6-arm")
	}
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

// escalateAuthorEndReason is the old record's end_reason: the successor's
// pointer is the last key=value token, as mergewake reads it.
func escalateAuthorEndReason(request hubEscalateAuthorRequest, newJob string) string {
	return fmt.Sprintf("ladder step round=%d replaced_by=%s", request.Round, newJob)
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

package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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
// The hub stages a pinned successor and transfers the role, checks the live
// checkout without mutation, then closes the pane, marks the job lost and
// activates placement. Replay completes the staged handoff; success also
// requires the task binding. See docs/escalate-author.md.

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
	Task                 string `json:"task"`
	Job                  string `json:"job"`
	PR                   string `json:"pr"`
	Round                int    `json:"round"`
	Findings             string `json:"findings"`
	Grade                string `json:"grade"`
	FromProfile          string `json:"from_profile"`
	FromEffort           string `json:"from_effort"`
	Profile              string `json:"profile"`
	Effort               string `json:"effort"`
	Step                 string `json:"step"`
	ConsultSeat          string `json:"consult_seat"`
	SelectionRefused     bool   `json:"selection_refused,omitempty"`
	SelectionUnavailable bool   `json:"selection_unavailable,omitempty"`
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
		writeJSON(w, http.StatusBadRequest, hubEscalateAuthorResult{Code: "invalid_request", Reason: "request body is invalid"})
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
	h.ladderMu.Lock()
	defer h.ladderMu.Unlock()
	holder, err := h.inbox.store.latestSpawnForJob(ctx, request.Job)
	if err != nil {
		return h.escalateAuthorRefusal(ctx, request, "", "job_unknown", "no spawn operation for "+request.Job, false)
	}
	if hubSpawnArg(holder.Request.Args, "--role") == "worker" && hubSpawnArg(holder.Request.Args, "--task") == request.Task {
		text := fmt.Sprintf("[panewire] ESC | legacy_worker_submission: job %s uses the retired worker role; its ladder successor was not started. Decide how to continue task %s.", request.Job, request.Task)
		h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: fmt.Sprintf("ladder-step:%s:%d:legacy_worker_submission", request.Job, request.Round), JobID: request.Job, OwnerLane: holder.Owner, NotifyType: "decision", Text: utf8PrefixWithin(text, laneEventTextLimit), Host: "hub", Epoch: 1}, nil)
		return hubEscalateAuthorResult{Code: "legacy_worker_submission", Reason: "the stored worker role is retired; the owner lane was notified"}, nil
	}
	spawn, err := fleet.ValidateSubmission(holder.Request)
	if err != nil {
		return h.escalateAuthorRefusal(ctx, request, "", "job_unreadable", err.Error(), false)
	}
	if spawn.Job != request.Job || spawn.Task != request.Task || spawn.Role != "implementer" || spawn.ParentJob != "" {
		return h.escalateAuthorRefusal(ctx, request, "", "job_mismatch", "the job is not the task's original implementer", false)
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return hubEscalateAuthorResult{}, err
	}
	owner := holder.Owner
	newJob, _, ok := escalateAuthorJob(request.Job)
	if !ok {
		return h.escalateAuthorRefusal(ctx, request, owner, "no_rung", "the ladder step limit is reached", false)
	}
	endReason := fleet.LadderStepEndReason(request.Round, newJob)
	if len(endReason) > 128 || !validNodeEndReason(endReason) {
		return h.escalateAuthorRefusal(ctx, request, owner, "successor_invalid", "invalid ladder end reason", false)
	}
	prior, priorErr := h.inbox.store.latestSpawnForJob(ctx, newJob)
	if priorErr != nil && !errors.Is(priorErr, sql.ErrNoRows) {
		return hubEscalateAuthorResult{}, priorErr
	}
	staged := priorErr == nil
	record, exists, err := app.ExistingJob(ctx, request.Job)
	if err != nil || !exists {
		if staged {
			return hubEscalateAuthorResult{Code: "pending", Reason: "the author's record is unreadable; retry"}, nil
		}
		return h.escalateAuthorRefusal(ctx, request, owner, "job_unreadable", "the job's hk record is unreadable", false)
	}
	owner = firstNonempty(escalateAuthorStr(record, "owner_lane"), owner)
	if staged {
		identity := prior.Request.LadderStep
		if identity == nil || identity.Parent != request.Job || identity.Round != request.Round || identity.PR != request.PR {
			if escalateAuthorStr(record, "state") == "lost" {
				return h.escalateAuthorRefusal(ctx, request, owner, "job_changed", "a different ladder step already owns the successor", true)
			}
			if prior.Status != "staged" {
				return hubEscalateAuthorResult{Code: "pending", Reason: "another ladder step owns the successor; retry its original request"}, nil
			}
			probe := h.probeHeldPane(ctx, spawnHolderHost(holder), holder.Outcome.Pane, heldAgentName(holder, record))
			if probe.state == heldPaneUnknown {
				return hubEscalateAuthorResult{Code: "pending", Reason: "author pane state is unknown; retry"}, nil
			}
			return h.escalateAuthorRefusal(ctx, request, owner, "job_changed", "a different ladder step already owns the successor", probe.state == heldPaneAbsent)
		}
		if prior.Status != "staged" {
			return h.escalateAuthorOutcome(ctx, app, request, owner, newJob, prior)
		}
	}
	state := escalateAuthorStr(record, "state")
	resume := state == "lost" && escalateAuthorStr(record, "end_reason") == endReason
	if state != "claimed" && state != "spawned" && !resume {
		if staged && hkTerminalState(state) {
			if err := h.inbox.store.discardStagedSpawn(ctx, prior.Request.RequestID, spawn.RoleKey()); err != nil {
				return hubEscalateAuthorResult{}, err
			}
			return h.escalateAuthorParentEnded(ctx, request, owner), nil
		}
		return h.escalateAuthorRefusal(ctx, request, owner, "job_not_live", "the job's record is "+state, false)
	}
	if !resume && (escalateAuthorStr(record, "profile") != "" && escalateAuthorStr(record, "profile") != request.FromProfile || escalateAuthorStr(record, "effort") != "" && request.FromEffort != "" && escalateAuthorStr(record, "effort") != request.FromEffort) {
		return h.escalateAuthorRefusal(ctx, request, owner, "job_changed", "the author rung changed", false)
	}
	var next fleet.Submission
	var probe heldProbe
	name := heldAgentName(holder, record)
	if !resume {
		probe = h.probeHeldPane(ctx, spawnHolderHost(holder), holder.Outcome.Pane, name)
		if probe.state == heldPaneUnknown {
			return hubEscalateAuthorResult{Code: "pending", Reason: "the author pane state is unknown; retry"}, nil
		}
	}
	if staged {
		next = prior.Request
	} else {
		if resume || probe.state != heldPanePresent {
			return h.escalateAuthorRefusal(ctx, request, owner, "job_not_marked", "the author is gone without a staged successor", true)
		}
		if !nodePaneIdleStatus[probe.row.AgentStatus] || !validNodeCommandPath(probe.row.Cwd) {
			return h.escalateAuthorRefusal(ctx, request, owner, "pane_busy", "the author pane is busy", false)
		}
		if request.Profile == "" || request.Step == "" {
			if request.SelectionUnavailable {
				return hubEscalateAuthorResult{}, fmt.Errorf("the successor selection is unavailable; retry")
			}
			if request.SelectionRefused {
				return h.escalateAuthorRefusal(ctx, request, owner, "quota_refused", "the selection quota gate refused the successor", false)
			}
			return h.escalateAuthorRefusal(ctx, request, owner, "no_rung", "the policy has no step left for this seat", false)
		}
		if app.Quota == nil {
			return hubEscalateAuthorResult{}, fmt.Errorf("the hub has no quota catalog")
		}
		rows, err := app.Quota.Catalog(ctx)
		if err != nil {
			return hubEscalateAuthorResult{}, err
		}
		if !escalateAuthorRung(rows, request) {
			return h.escalateAuthorRefusal(ctx, request, owner, "no_rung", "the rung is no longer admitted", false)
		}
		next, err = escalateAuthorSubmission(holder.Request, request, newJob)
		if err != nil {
			return hubEscalateAuthorResult{}, err
		}
		host := spawnHolderHost(holder)
		if host == "" {
			return h.escalateAuthorRefusal(ctx, request, owner, "pane_unavailable", "the holder host is unknown", false)
		}
		setEscalateAuthorHost(&next, host)
		if next.Repository.Checkout == nil || next.Repository.Checkout.Mode != "branch" {
			return h.escalateAuthorRefusal(ctx, request, owner, "worktree_unreadable", "the author has no branch checkout", false)
		}
		var initial nodeWorktreeInspectResult
		if !h.nodeCall(ctx, host, nodeCommandWorktreeInspect, nodeWorktreeInspectArgs{Cwd: probe.row.Cwd, ExpectedDir: next.Repository.Checkout.Dir}, spawnHeldPaneActWait, &initial) || !escalateCheckoutMatches(initial.Worktree, next.Repository.Checkout) || !nodeShaPattern.MatchString(initial.Worktree.Head) {
			return h.escalateAuthorRefusal(ctx, request, owner, "worktree_unreadable", "the pane is not in the expected branch checkout", false)
		}
		next.Repository.Checkout.Sha = initial.Worktree.Head
		next.Repository.Checkout.VerifyReuse = true
		next.LadderStep = &fleet.LadderStepIdentity{Parent: request.Job, Round: request.Round, PR: request.PR, Profile: request.Profile, Effort: request.Effort}
		if _, err := fleet.ValidateSubmission(next); err != nil {
			return h.escalateAuthorRefusal(ctx, request, owner, "successor_invalid", err.Error(), false)
		}
		prior, _, err = h.inbox.store.claimSpawnReplacing(ctx, next, newJob, owner, h.now().UTC(), holder.Request.RequestID, spawn.RoleKey(), "staged")
		if err != nil {
			return h.escalateAuthorRefusal(ctx, request, owner, "claim_refused", err.Error(), false)
		}
		staged = true
	}
	if !resume && probe.state == heldPanePresent {
		cancel := func(code, why string) (hubEscalateAuthorResult, error) {
			if err := h.inbox.store.cancelStagedSpawn(ctx, prior.Request.RequestID, holder.Request.RequestID, spawn.RoleKey()); err != nil {
				return hubEscalateAuthorResult{}, fmt.Errorf("cannot cancel staged step: %w", err)
			}
			return h.escalateAuthorRefusal(ctx, request, owner, code, why, false)
		}
		if h.now().After(prior.Deadline) {
			return cancel("staged_expired", "the staged step expired while the author remained present")
		}
		if !nodePaneIdleStatus[probe.row.AgentStatus] || !validNodeCommandPath(probe.row.Cwd) {
			return cancel("pane_busy", "the author pane is busy")
		}
		if code, why := h.escalateAuthorWorktreeUsable(ctx, holder, next.Repository.Checkout, probe.row.Cwd); code != "" {
			return cancel(code, why)
		}
		if err := h.planLadderSuccessor(ctx, next); err != nil {
			code, why := escalateAuthorPlanRefusal(err)
			return cancel(code, why)
		}
		if !h.closeHeldPane(ctx, holder, name, probe.row) {
			again := h.probeHeldPane(ctx, spawnHolderHost(holder), holder.Outcome.Pane, name)
			if again.state == heldPanePresent {
				return cancel("pane_not_closed", "the author pane could not be closed")
			}
			return hubEscalateAuthorResult{Code: "pending", Reason: "pane close outcome is unknown; retry"}, nil
		}
	}
	if !resume {
		lost, why := h.markHolderJobLostBecause(ctx, app, holder, name, endReason)
		if !lost {
			return h.escalateAuthorRefusal(ctx, request, owner, "job_not_marked", why, true)
		}
		if why == nodeJobMarkLostCompleted || why == "hk_lost" {
			current, exists, readErr := app.ExistingJob(ctx, request.Job)
			if readErr != nil || !exists {
				return hubEscalateAuthorResult{Code: "pending", Reason: "the predecessor's end reason is unconfirmed; retry"}, nil
			}
			if hkTerminalState(escalateAuthorStr(current, "state")) && !(escalateAuthorStr(current, "state") == "lost" && escalateAuthorStr(current, "end_reason") == endReason) {
				if err := h.inbox.store.discardStagedSpawn(ctx, prior.Request.RequestID, spawn.RoleKey()); err != nil {
					return hubEscalateAuthorResult{}, err
				}
				return h.escalateAuthorParentEnded(ctx, request, owner), nil
			}
			if escalateAuthorStr(current, "state") != "lost" || escalateAuthorStr(current, "end_reason") != endReason {
				return hubEscalateAuthorResult{Code: "pending", Reason: "the predecessor's end reason is unconfirmed; retry"}, nil
			}
		}
	}
	if err := h.inbox.store.activateStagedSpawn(ctx, next.RequestID, h.now().UTC(), next.DeadlineSeconds); err != nil {
		if current, readErr := h.inbox.store.spawnTask(ctx, next.RequestID); readErr == nil && current.Status != "staged" {
			return h.escalateAuthorOutcome(ctx, app, request, owner, newJob, current)
		}
		return h.escalateAuthorRefusal(ctx, request, owner, "claim_refused", err.Error(), true)
	}
	select {
	case h.spawnKick <- struct{}{}:
	default:
	}
	prior, err = h.inbox.store.spawnTask(ctx, next.RequestID)
	if err != nil {
		return hubEscalateAuthorResult{Code: "pending", Reason: "successor operation unavailable; retry"}, nil
	}
	return h.escalateAuthorOutcome(ctx, app, request, owner, newJob, prior)
}

// planLadderSuccessor runs the read-only placement plan for the successor and
// returns nil when it admits the request. The caller reports a refusal with
// escalateAuthorPlanRefusal, which keeps the plan's own code and text.
func (h *HubServer) planLadderSuccessor(ctx context.Context, request fleet.Submission) error {
	if h.ladderPlan != nil {
		return h.ladderPlan(ctx, request)
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return err
	}
	h.bindPlacement(app, planOnlyTransport{})
	planCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	plan, err := app.PlanSubmission(planCtx, request)
	if err != nil {
		return err
	}
	if plan.Host != hubSpawnArg(request.Args, "--host") {
		return fmt.Errorf("the plan selected host %q instead of the pinned host %q", plan.Host, hubSpawnArg(request.Args, "--host"))
	}
	return nil
}

// escalateAuthorPlanRefusal names a refused plan from the refusal table: the
// closed-set code the spawn route reports, and the plan's own text. A seat or
// host refusal (NO_ROLE_CANDIDATE at rc 77, an unknown host) is not a quota
// reading, so it is not reported as quota_refused.
func escalateAuthorPlanRefusal(err error) (code, reason string) {
	return firstNonempty(fleet.SpawnRefusalCode(err.Error(), fleet.ErrorCode(err)), "placement"), err.Error()
}

func (h *HubServer) escalateAuthorOutcome(ctx context.Context, app *fleet.App, request hubEscalateAuthorRequest, owner, job string, task hubSpawnTask) (hubEscalateAuthorResult, error) {
	if spawnStatusActive(task.Status) {
		var settled bool
		task, settled = h.awaitSpawnOutcome(ctx, task.Request.RequestID, escalateAuthorWait)
		if !settled {
			return hubEscalateAuthorResult{Code: "pending", Reason: "successor placement is pending; retry"}, nil
		}
	}
	if task.Status != "succeeded" || task.Outcome.RC != 0 {
		return h.escalateAuthorRefusal(ctx, request, owner, "quota_refused", firstNonempty(task.Outcome.Reason, "the successor did not place"), true)
	}
	if err := app.ConfirmLadderTaskBinding(ctx, request.Task, request.Job, job, hubSpawnArg(task.Request.Args, "-l")); err != nil {
		text := fmt.Sprintf("ladder step %s: successor %s placed, but its task binding is unconfirmed (round %d, %s)", request.Job, job, request.Round, request.PR)
		h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: fmt.Sprintf("ladder-step:%s:%d:placed:rebind_unconfirmed", request.Job, request.Round), JobID: job, OwnerLane: owner, NotifyType: "info", Text: utf8PrefixWithin(text, laneEventTextLimit), Host: "hub", Epoch: 1}, nil)
		return hubEscalateAuthorResult{Code: "rebind_unconfirmed", Reason: "successor placed but task binding is unconfirmed"}, nil
	}
	rung := stepValue(task.Request.Args, "-m") + "@" + firstNonempty(stepValue(task.Request.Args, "--effort"), "default")
	text := fmt.Sprintf("ladder step %s -> %s on %s (round %d, %s)", request.Job, job, rung, request.Round, request.PR)
	h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: "ladder-step:" + job, JobID: job, OwnerLane: owner, NotifyType: "info", Text: utf8PrefixWithin(text, laneEventTextLimit), Host: "hub", Epoch: 1}, nil)
	return hubEscalateAuthorResult{Job: job, Rung: rung}, nil
}

func setEscalateAuthorHost(next *fleet.Submission, host string) {
	for i := 0; i+1 < len(next.Args); i++ {
		if next.Args[i] == "--host" {
			next.Args[i+1] = host
			return
		}
	}
	next.Args = append(next.Args, "--host", host)
}

// An expired stage is cancelled only with fresh evidence that the old author
// still exists. A missing or unreadable pane may mean close already succeeded.
func (h *HubServer) expireStagedLadderSteps(ctx context.Context) {
	h.ladderMu.Lock()
	defer h.ladderMu.Unlock()
	tasks, err := h.inbox.store.expiredStagedSpawns(ctx, h.now().UTC())
	if err != nil {
		return
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return
	}
	for _, task := range tasks {
		id := task.Request.LadderStep
		if id == nil {
			continue
		}
		holder, err := h.inbox.store.latestSpawnForJob(ctx, id.Parent)
		if err != nil {
			continue
		}
		spawn, err := fleet.ValidateSubmission(holder.Request)
		if err != nil {
			continue
		}
		record, exists, err := app.ExistingJob(ctx, id.Parent)
		if err != nil || !exists {
			continue
		}
		state := escalateAuthorStr(record, "state")
		if state == "lost" && escalateAuthorStr(record, "end_reason") == fleet.LadderStepEndReason(id.Round, task.Job) {
			if h.inbox.store.activateStagedSpawn(ctx, task.Request.RequestID, h.now().UTC(), task.Request.DeadlineSeconds) == nil {
				select {
				case h.spawnKick <- struct{}{}:
				default:
				}
			}
			continue
		}
		if hkTerminalState(state) {
			if h.inbox.store.discardStagedSpawn(ctx, task.Request.RequestID, spawn.RoleKey()) == nil {
				request := hubEscalateAuthorRequest{Job: id.Parent, Round: id.Round, PR: id.PR}
				h.escalateAuthorParentEnded(ctx, request, task.Owner)
			}
			continue
		}
		if state != "claimed" && state != "spawned" {
			continue
		}
		if h.probeHeldPane(ctx, spawnHolderHost(holder), holder.Outcome.Pane, heldAgentName(holder, record)).state != heldPanePresent {
			continue
		}
		if err := h.inbox.store.cancelStagedSpawn(ctx, task.Request.RequestID, holder.Request.RequestID, spawn.RoleKey()); err != nil {
			continue
		}
		request := hubEscalateAuthorRequest{Job: id.Parent, Round: id.Round, PR: id.PR}
		_, _ = h.escalateAuthorRefusal(ctx, request, task.Owner, "staged_expired", "the staged step expired while the author remained present", false)
	}
}

func (h *HubServer) escalateAuthorParentEnded(ctx context.Context, request hubEscalateAuthorRequest, owner string) hubEscalateAuthorResult {
	text := fmt.Sprintf("ladder step %s: the predecessor ended independently; the staged successor and its reservation were released (round %d, %s)", request.Job, request.Round, request.PR)
	h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: fmt.Sprintf("ladder-step:%s:%d:ended:parent_ended", request.Job, request.Round), JobID: request.Job, OwnerLane: owner, NotifyType: "info", Text: utf8PrefixWithin(text, laneEventTextLimit), Host: "hub", Epoch: 1}, nil)
	return hubEscalateAuthorResult{Code: "parent_ended", Reason: "the predecessor ended independently; its staged successor was released"}
}

func escalateCheckoutMatches(tree *nodeWorktree, checkout *fleet.HubCheckout) bool {
	if tree == nil || checkout == nil || !spawnReceiptCheckoutPath(tree.Path, checkout.Dir, "") || tree.Branch != checkout.Branch {
		return false
	}
	want, ok := nodeOriginRepo(checkout.Origin)
	got, found := nodeCloneRemoteRepo(tree.OriginURL)
	return ok && found && strings.EqualFold(want, got)
}

// escalateAuthorWorktreeUsable verifies the frozen checkout without changing it.
func (h *HubServer) escalateAuthorWorktreeUsable(ctx context.Context, holder hubSpawnTask, checkout *fleet.HubCheckout, cwd string) (code, reason string) {
	if checkout == nil || cwd == "" {
		return "worktree_unreadable", "the checkout cannot be inspected"
	}
	var result nodeWorktreeInspectResult
	if !h.nodeCall(ctx, spawnHolderHost(holder), nodeCommandWorktreeInspect, nodeWorktreeInspectArgs{Cwd: cwd, ExpectedDir: checkout.Dir, VerifyBranch: true}, spawnHeldPaneActWait, &result) || result.Worktree == nil {
		return "worktree_unreadable", "the checkout cannot be inspected"
	}
	if !escalateCheckoutMatches(result.Worktree, checkout) || !strings.EqualFold(result.Worktree.Head, checkout.Sha) {
		return "worktree_unreadable", "the staged checkout changed"
	}
	switch result.Check {
	case "ok":
		return "", ""
	case "dirty":
		return "worktree_dirty", "the worktree has uncommitted changes"
	case "locked":
		return "worktree_locked", "the worktree is locked"
	case "unpushed":
		return "worktree_unpushed", "the branch does not contain the staged head"
	default:
		return "worktree_unreadable", "the checkout verification failed"
	}
}

// escalateAuthorRefusal records the refusal as one lane event for the design
// lane and answers the CLI with its code. ended marks a refusal that follows
// the pane close: the old author is gone and the PR has none, which the CLI
// reports and the lane gets as an ESC naming the PR authorless. Panewire never
// starts a successor on its own here: only the owner lane's reply does.
func (h *HubServer) escalateAuthorRefusal(ctx context.Context, request hubEscalateAuthorRequest, owner, code, reason string, ended bool) (hubEscalateAuthorResult, error) {
	eventID := fmt.Sprintf("ladder-step:%s:%d:preflight:%s", request.Job, request.Round, code)
	notify := "info"
	text := fmt.Sprintf("ladder step %s: %s (%s) (round %d, %s); the PR goes on to the next round", request.Job, code, reason, request.Round, request.PR)
	if ended {
		// The :esc suffix is its own identity. A row this (job, round, code)
		// filed before the ESC existed is stored as info under the shorter id,
		// and lane-event dedupe is first-writer-wins: without the suffix the
		// replay would find that row and never file the wake the lane needs.
			// Accepted trade-off: a build between #565 and this one filed ended
			// refusals as decision under the shorter id. A replay of such a
			// (job, round, code) after the upgrade files one duplicate ESC under
			// :esc. It is a one-time, operator-visible duplicate, not a lost wake;
			// checking the legacy row before filing was not worth the extra read.
		eventID = fmt.Sprintf("ladder-step:%s:%d:ended:%s:esc", request.Job, request.Round, code)
		notify = "decision"
		text = fmt.Sprintf("[panewire] ESC | authorless: ladder step %s: %s (%s) (round %d); %s has no author: the pane is closed and no successor was confirmed. Decide how to continue; panewire does not start a successor automatically.", request.Job, code, reason, request.Round, request.PR)
	}
	h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: eventID, JobID: request.Job, OwnerLane: owner, NotifyType: notify, Text: utf8PrefixWithin(text, laneEventTextLimit), Host: "hub", Epoch: 1}, nil)
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
// the same worktree and branch. The hub pins the currently inspected HEAD
// before staging the successor.
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

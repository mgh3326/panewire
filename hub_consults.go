package panewire

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/mgh3326/panewire/internal/fleet"
)

// Consult on request (hk 4552 phase 1, operator decision 250,
// docs/consult-on-request.md). A caller asks the hub for a consult; the hub
// picks the seat, runs one ordinary hub spawn, and records the stored answer.
//
// `POST /v1/consults` creates or returns the consult for one idempotency key
// (task, repo#pr, head). `GET /v1/consults/{id}` answers the state, the seat,
// the job and the answer document. A lane requester gets one ready lane event;
// a tool requester polls.

const (
	// consultToolLane owns a consult a tool asked for. A lane requester owns
	// its own consult. The lane is only the spawn's owner (completion routing);
	// the tool reads GET /v1/consults/{id}.
	consultToolLane = "operator-desk"
	// consultReasonLimit bounds a refusal reason carried in a row.
	consultReasonLimit = 512
	// consultPollInterval is how often the coordinator reads active rows.
	consultPollInterval = 5 * time.Second
)

var (
	consultRequesterPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/#-]{0,199}$`)
	consultFamilyPattern    = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)
)

type hubConsultRequest struct {
	Task           string `json:"task_id"`
	Requester      string `json:"requester"`
	QuestionDoc    string `json:"question_doc"`
	AuthorJob      string `json:"author_job,omitempty"`
	AuthorFamily   string `json:"author_family,omitempty"`
	PR             string `json:"pr,omitempty"`
	Head           string `json:"head,omitempty"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

type hubConsultSeat struct {
	Profile string `json:"profile,omitempty"`
	Family  string `json:"family,omitempty"`
	Grade   string `json:"grade,omitempty"`
}

type hubConsultView struct {
	ID         string          `json:"id"`
	State      string          `json:"state"`
	Seat       *hubConsultSeat `json:"seat,omitempty"`
	Job        string          `json:"job,omitempty"`
	AnswerDoc  string          `json:"answer_doc,omitempty"`
	Reason     string          `json:"reason,omitempty"`
	RequestID  string          `json:"request_id,omitempty"`
	Requester  string          `json:"requester,omitempty"`
	CreatedAt  string          `json:"created_at"`
	AnsweredAt string          `json:"answered_at,omitempty"`
}

// consultAccess admits the operator, the calling node's own credential, and a
// service holding the consult scope. It refuses an unauthenticated caller
// (401) and a service without the scope (403).
func (h *HubServer) consultAccess(request *http.Request) hubAccess {
	caller := h.authenticate(request)
	switch caller.kind {
	case hubCallerOperator, hubCallerNode:
		return hubAccessGranted
	case hubCallerService:
		if hubGrantsAllow(caller.grants, hubScopeConsult, "") {
			return hubAccessGranted
		}
		h.logger.Warn("hub scope refused", "client", caller.id, "scope", hubScopeConsult, "resource", "")
		return hubAccessForbidden
	}
	return hubAccessDenied
}

func (h *HubServer) handleConsultRequest(w http.ResponseWriter, r *http.Request) {
	if access := h.consultAccess(r); access != hubAccessGranted {
		writeHubAccessDenied(w, access)
		return
	}
	if h.inbox == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "durable_store_required", "fix": "configure the hub inbox database before accepting consults"})
		return
	}
	var request hubConsultRequest
	if !decodeHubJSON(w, r, hubSpawnMaxBodyBytes, &request) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_request", "reason": "request body is invalid"})
		return
	}
	if reason := request.refusal(); reason != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_request", "reason": reason})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	key := consultIdemKey(request.Task, request.PR, request.Head)
	if existing, err := h.inbox.store.consultByKey(ctx, key); err == nil {
		writeJSON(w, http.StatusOK, consultView(existing))
		return
	} else if !errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "store_unavailable"})
		return
	}

	now := h.now().UTC()
	c := hubConsult{
		ID: uuid.NewString(), IdemKey: key, Task: request.Task, PR: request.PR, Head: request.Head,
		Requester: request.Requester, QuestionDoc: request.QuestionDoc, AuthorJob: request.AuthorJob,
		AuthorFamily: request.AuthorFamily, State: consultQueued, Created: now, Deadline: now.Add(h.consultTimeout),
	}
	plan, reason := h.planConsultRequest(ctx, request)
	if reason != "" {
		c.State = consultRefused
		c.Reason = reason
		stored, _, err := h.inbox.store.insertConsult(ctx, c)
		if err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "store_unavailable"})
			return
		}
		writeJSON(w, http.StatusCreated, consultView(stored))
		return
	}
	c.SeatProfile, c.SeatFamily, c.SeatGrade = plan.Seat.Profile, plan.Seat.Family, plan.Seat.Grade
	c.Job, c.RequestID = plan.Job, plan.Request.RequestID
	stored, fresh, err := h.inbox.store.insertConsult(ctx, c)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "store_unavailable"})
		return
	}
	if !fresh {
		// Another caller won the same key between the read and the insert.
		writeJSON(w, http.StatusOK, consultView(stored))
		return
	}
	if err := h.claimConsultSpawn(ctx, plan, c.Requester); err != nil {
		h.failConsult(ctx, c, consultReason(err))
		writeJSON(w, http.StatusCreated, consultView(h.consultOrEmpty(ctx, c)))
		return
	}
	select {
	case h.spawnKick <- struct{}{}:
	default:
	}
	writeJSON(w, http.StatusCreated, consultView(c))
}

func (h *HubServer) handleConsultGet(w http.ResponseWriter, r *http.Request) {
	if access := h.consultAccess(r); access != hubAccessGranted {
		writeHubAccessDenied(w, access)
		return
	}
	if h.inbox == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "durable_store_required"})
		return
	}
	id := r.PathValue("id")
	if !hubSpawnRequestIDPattern.MatchString(id) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found"})
		return
	}
	c, err := h.inbox.store.consultByID(r.Context(), id)
	if errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "store_unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, consultView(c))
}

// refusal validates the request shape. The hub bounds every field before it
// stores or spawns anything.
func (r hubConsultRequest) refusal() string {
	switch {
	case !escalateAuthorNumeric(r.Task):
		return "task_id must be a task id"
	case !consultRequesterPattern.MatchString(r.Requester):
		return "requester must be a lane or tool id"
	case !strings.HasPrefix(r.QuestionDoc, "hk:doc/") || len(r.QuestionDoc) > 300:
		return "question_doc must be hk:doc/KEY"
	case r.AuthorJob != "" && !hubJobIDPattern.MatchString(r.AuthorJob):
		return "author_job must be a job id"
	case r.AuthorFamily != "" && !consultFamilyPattern.MatchString(r.AuthorFamily):
		return "author_family is invalid"
	case r.PR != "" && !escalateAuthorPRPattern.MatchString(r.PR):
		return "pr must be owner/repo#N"
	case r.Head != "" && !nodeShaPattern.MatchString(r.Head):
		return "head must be a 40-character commit id"
	}
	return ""
}

func consultIdemKey(task, pr, head string) string {
	return task + "|" + pr + "|" + head
}

func consultOwner(requester string) string {
	if validLaneReference(requester) {
		return requester
	}
	return consultToolLane
}

func consultReason(err error) string {
	return utf8PrefixWithin(err.Error(), consultReasonLimit)
}

// planConsultRequest resolves the repository evidence and the seat. A refusal
// names why the consult stays unspawned.
func (h *HubServer) planConsultRequest(ctx context.Context, request hubConsultRequest) (fleet.ConsultPlan, string) {
	repo, reason := h.consultRepository(ctx, request)
	if reason != "" {
		return fleet.ConsultPlan{}, reason
	}
	plan, err := h.resolveConsultPlan(ctx, fleet.ConsultPlanInput{
		Task: request.Task, Question: request.QuestionDoc, AuthorJob: request.AuthorJob,
		AuthorFamily: request.AuthorFamily, PR: request.PR, Head: request.Head,
		Requester: request.Requester, Owner: consultOwner(request.Requester), Repository: repo,
	})
	if err != nil {
		return fleet.ConsultPlan{}, consultReason(err)
	}
	return plan, ""
}

func (h *HubServer) resolveConsultPlan(ctx context.Context, in fleet.ConsultPlanInput) (fleet.ConsultPlan, error) {
	if h.consultPlan != nil {
		return h.consultPlan(ctx, in)
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return fleet.ConsultPlan{}, err
	}
	if err := app.CheckTrust(); err != nil {
		return fleet.ConsultPlan{}, err
	}
	return app.PlanConsult(ctx, in)
}

// consultRepository reads the author job's stored submission, else the job
// the task is bound to. A consult changes no code but still runs in a checkout
// of the requested head.
func (h *HubServer) consultRepository(ctx context.Context, request hubConsultRequest) (*fleet.Repository, string) {
	job := request.AuthorJob
	if job == "" {
		bound, err := fleet.TaskBinding(ctx, request.Task)
		if err != nil {
			return nil, "task_unreadable: " + consultReason(err)
		}
		job = bound
	}
	if job == "" {
		return nil, "author_job_required: no author job and the task is bound to none"
	}
	holder, err := h.inbox.store.latestSpawnForJob(ctx, job)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, "author_job_unknown: no spawn operation for " + job
	}
	if err != nil {
		return nil, "author_job_unreadable: " + consultReason(err)
	}
	repo := holder.Request.Repository
	return &repo, ""
}

func (h *HubServer) claimConsultSpawn(ctx context.Context, plan fleet.ConsultPlan, owner string) error {
	_, fresh, err := h.inbox.store.claimSpawn(ctx, plan.Request, plan.Job, owner, h.now().UTC())
	if err != nil {
		return err
	}
	if !fresh {
		return errors.New("job_already_claimed")
	}
	return nil
}

func (h *HubServer) consultOrEmpty(ctx context.Context, c hubConsult) hubConsult {
	if stored, err := h.inbox.store.consultByID(ctx, c.ID); err == nil {
		return stored
	}
	return c
}

func consultView(c hubConsult) hubConsultView {
	view := hubConsultView{
		ID: c.ID, State: c.State, Job: c.Job, AnswerDoc: c.AnswerDoc, Reason: c.Reason,
		RequestID: c.RequestID, Requester: c.Requester, CreatedAt: c.Created.UTC().Format(time.RFC3339),
	}
	if c.SeatProfile != "" {
		view.Seat = &hubConsultSeat{Profile: c.SeatProfile, Family: c.SeatFamily, Grade: c.SeatGrade}
	}
	if !c.Answered.IsZero() {
		view.AnsweredAt = c.Answered.UTC().Format(time.RFC3339)
	}
	return view
}

// startConsultCoordinator keeps active consults moving after a restart: it
// reads them from the durable rows, so nothing lives only in memory.
func (h *HubServer) startConsultCoordinator(ctx context.Context) {
	if h.inbox == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(consultPollInterval)
		defer ticker.Stop()
		for {
			h.reconcileConsults(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (h *HubServer) reconcileConsults(ctx context.Context) {
	if h.inbox == nil {
		return
	}
	active, err := h.inbox.store.activeConsults(ctx)
	if err != nil {
		return
	}
	var app *fleet.App
	for _, c := range active {
		if ctx.Err() != nil {
			return
		}
		if !h.now().Before(c.Deadline) {
			h.failConsult(ctx, c, "timeout: the consult passed its time limit")
			continue
		}
		task, err := h.inbox.store.spawnTask(ctx, c.RequestID)
		if errors.Is(err, sql.ErrNoRows) {
			if c.State == consultRunning {
				if c.Respawns == 0 {
					h.respawnConsult(ctx, c, "spawn_operation_missing")
				} else {
					h.failConsult(ctx, c, "spawn_operation_missing")
				}
			}
			continue
		}
		if err != nil {
			continue
		}
		if c.State == consultQueued && (task.Node != nil || !task.Placed.IsZero() || task.Status == "succeeded") {
			c.State = consultRunning
			_ = h.inbox.store.saveConsult(ctx, c)
		}
		if app == nil {
			app, _ = h.newSpawnApp(io.Discard, io.Discard)
		}
		if h.settleConsult(ctx, app, c) {
			continue
		}
		switch task.Status {
		case "failed", "retryable":
			h.consultJobFailed(ctx, c, firstNonEmptyString(task.Outcome.Reason, task.Status))
		case "reconciling", "deadline_exceeded", "accepted", "bound", "dispatching", "succeeded":
			// Ownership or the job itself is still live; the row's deadline
			// ends the wait.
		default:
			h.consultJobFailed(ctx, c, firstNonEmptyString(task.Outcome.Reason, task.Status))
		}
	}
}

// settleConsult records the answer when the job has completed and its report
// is stored. It reports false when the consult is not settled, so the caller
// can judge the spawn's own state.
func (h *HubServer) settleConsult(ctx context.Context, app *fleet.App, c hubConsult) bool {
	if app == nil || c.Job == "" {
		return false
	}
	record, found, err := app.ExistingJob(ctx, c.Job)
	if err != nil || !found {
		return false
	}
	switch recordState(record) {
	case "lost":
		h.consultJobFailed(ctx, c, "job_lost")
		return true
	case "spawned", "running", "claimed", "developing", "escalated":
		// The job is live; a spawn failure after its claim does not make a
		// second consult safe.
		return true
	case "completed":
		doc, _ := record["report_doc"].(string)
		if doc == "" {
			// The record completed before its report document landed; the
			// next tick reads it again.
			return true
		}
		problem, err := fleet.CheckConsultAnswerDocument(ctx, doc)
		if err != nil {
			return true
		}
		if problem != "" {
			h.failConsult(ctx, c, problem)
			return true
		}
		c.AnswerDoc, c.Answered, c.Reason, c.State = doc, h.now().UTC(), "", consultAnswered
		if err := h.inbox.store.saveConsult(ctx, c); err != nil {
			return true
		}
		h.notifyConsultAnswered(ctx, c)
		return true
	}
	return false
}

func recordState(record map[string]any) string {
	state, _ := record["state"].(string)
	return state
}

func (h *HubServer) consultJobFailed(ctx context.Context, c hubConsult, reason string) {
	if c.Respawns == 0 {
		h.respawnConsult(ctx, c, reason)
		return
	}
	h.failConsult(ctx, c, firstNonEmptyString(reason, "job_failed"))
}

// respawnConsult runs the single allowed second attempt on the next eligible
// seat: still at the consult floor and in another family, never the seat that
// failed.
func (h *HubServer) respawnConsult(ctx context.Context, c hubConsult, reason string) {
	request := hubConsultRequest{
		Task: c.Task, Requester: c.Requester, QuestionDoc: c.QuestionDoc, AuthorJob: c.AuthorJob,
		AuthorFamily: c.AuthorFamily, PR: c.PR, Head: c.Head,
	}
	repo, why := h.consultRepository(ctx, request)
	if why != "" {
		h.failConsult(ctx, c, "no_respawn_seat after "+reason+": "+why)
		return
	}
	plan, err := h.resolveConsultPlan(ctx, fleet.ConsultPlanInput{
		Task: request.Task, Question: request.QuestionDoc, AuthorJob: request.AuthorJob,
		AuthorFamily: request.AuthorFamily, PR: request.PR, Head: request.Head,
		Requester: request.Requester, Owner: consultOwner(request.Requester), Repository: repo,
		ExcludeProfiles: []string{c.SeatProfile},
	})
	if err != nil {
		h.failConsult(ctx, c, "no_respawn_seat after "+reason+": "+consultReason(err))
		return
	}
	if err := h.claimConsultSpawn(ctx, plan, consultOwner(request.Requester)); err != nil {
		h.failConsult(ctx, c, "respawn_unavailable after "+reason+": "+consultReason(err))
		return
	}
	h.logger.Warn("consult respawned on the next seat", "consult", c.ID, "task", c.Task, "reason", reason, "job", plan.Job, "seat", plan.Seat.Profile)
	c.State, c.Job, c.RequestID = consultQueued, plan.Job, plan.Request.RequestID
	c.SeatProfile, c.SeatFamily, c.SeatGrade = plan.Seat.Profile, plan.Seat.Family, plan.Seat.Grade
	c.Respawns = c.Respawns + 1
	c.Reason = ""
	if err := h.inbox.store.saveConsult(ctx, c); err != nil {
		return
	}
	select {
	case h.spawnKick <- struct{}{}:
	default:
	}
}

func (h *HubServer) failConsult(ctx context.Context, c hubConsult, reason string) {
	c.State = consultFailed
	c.Reason = utf8PrefixWithin(reason, consultReasonLimit)
	_ = h.inbox.store.saveConsult(ctx, c)
}

// notifyConsultAnswered sends the one ready lane event a lane requester gets.
// A tool requester polls GET /v1/consults/{id} instead.
func (h *HubServer) notifyConsultAnswered(ctx context.Context, c hubConsult) {
	if c.Notified || !validLaneReference(c.Requester) {
		return
	}
	text := fmt.Sprintf("consult %s answered for task %s: apply %s", c.ID, c.Task, c.AnswerDoc)
	event := hubJobEventPayload{
		EventID: "consult:" + c.ID + ":ready", JobID: c.Job, OwnerLane: c.Requester,
		NotifyType: "ready", Text: utf8PrefixWithin(text, laneEventTextLimit), Host: "hub", Epoch: 1,
	}
	h.relayLaneEventContext(ctx, event, nil)
	c.Notified = true
	_ = h.inbox.store.saveConsult(ctx, c)
}

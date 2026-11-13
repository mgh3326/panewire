package panewire

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"time"
)

// A lane's own jobs: `panewire jobs mine` and `panewire jobs close` reach a
// lane's jobs on every machine through the hub and the executing node's own
// credential, so a session that spawned panes on other hosts can see and close
// them without the operator token (docs/jobs-own.md). The hub answers only for
// the caller's own lane and lets the node that runs the job close its pane.

// hubOwnJobsMax bounds one lane's listing. The hub keeps every job record for
// the life of the process, and a long-lived lane accumulates completed ones.
const hubOwnJobsMax = 200

// hubOwnJob is one row of `jobs mine`: what a lane needs to see its jobs
// across machines.
type hubOwnJob struct {
	JobID    string `json:"job_id"`
	Machine  string `json:"machine"`
	Pane     string `json:"pane,omitempty"`
	State    string `json:"state"`
	Task     string `json:"task,omitempty"`
	LastSeen string `json:"last_seen,omitempty"`
}

// ownJobsDenial is the lane rule a node credential already follows for its
// inbox: the lane must be routed to the caller's own machine. The operator
// keeps every lane; a service is not a lane and is refused. It answers the
// status and the reason code to write, or 0 when the caller may act for lane.
func (h *HubServer) ownJobsDenial(request *http.Request, lane string) (int, string) {
	switch caller := h.authenticate(request); caller.kind {
	case hubCallerOperator:
		return 0, ""
	case hubCallerNode:
		if route, exists := loadReportRelayRoutes(h.reportRelayPath)[lane]; exists && route.Machine == caller.id {
			return 0, ""
		}
		return http.StatusForbidden, "lane_not_routed_to_caller"
	case hubCallerService:
		return http.StatusForbidden, "scope_forbidden"
	}
	return http.StatusUnauthorized, ""
}

// refuseOwnJobs writes a denial. An unauthenticated caller keeps the plain 401
// every route answers; an authenticated one gets the reason it was refused.
func refuseOwnJobs(writer http.ResponseWriter, status int, reason string) {
	if status == http.StatusUnauthorized {
		hubUnauthorized(writer)
		return
	}
	writeJSON(writer, status, map[string]string{"error": reason})
}

// ownJobs lists the lane's jobs across every machine, newest observation
// first. Completed and orphaned records stay visible: a lane that cannot see
// a finished job cannot ask for its pane and lane row to be retired.
func (h *HubServer) ownJobs(lane string) []hubOwnJob {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := h.now()
	jobs := make([]hubOwnJob, 0)
	for _, job := range h.jobs {
		if job.OwnerLane != lane || h.jobRecordStaleLocked(job, now) {
			continue
		}
		lastSeen := ""
		if !job.LastSeen.IsZero() {
			lastSeen = job.LastSeen.UTC().Format(time.RFC3339)
		}
		jobs = append(jobs, hubOwnJob{JobID: job.JobID, Machine: job.Node, Pane: job.Pane, State: hubJobState(job), LastSeen: lastSeen})
	}
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].LastSeen != jobs[j].LastSeen {
			return jobs[i].LastSeen > jobs[j].LastSeen
		}
		return jobs[i].JobID < jobs[j].JobID
	})
	if len(jobs) > hubOwnJobsMax {
		jobs = jobs[:hubOwnJobsMax]
	}
	return jobs
}

// fillOwnJobTasks names each job's task from the hub's own spawn operation,
// which carries the spawn's arguments. The task is best effort: a job whose
// operation is not in this hub's store (or a hub with the inbox disabled)
// lists with no task rather than failing the listing.
func (h *HubServer) fillOwnJobTasks(ctx context.Context, jobs []hubOwnJob) {
	if h.inbox == nil || h.inbox.store == nil {
		return
	}
	for index := range jobs {
		task, err := h.inbox.store.latestSpawnForJob(ctx, jobs[index].JobID)
		if err != nil {
			continue
		}
		jobs[index].Task = hubSpawnArg(task.Request.Args, "--task")
	}
}

// handleMyJobs answers GET /v1/jobs/mine?lane=L for the caller's own lane.
func (h *HubServer) handleMyJobs(writer http.ResponseWriter, request *http.Request) {
	lane := request.URL.Query().Get("lane")
	if h.authenticate(request).kind == hubCallerNone {
		hubUnauthorized(writer)
		return
	}
	if !validHistoricalLaneResource(lane) {
		writeJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_lane"})
		return
	}
	if status, reason := h.ownJobsDenial(request, lane); status != 0 {
		refuseOwnJobs(writer, status, reason)
		return
	}
	jobs := h.ownJobs(lane)
	h.fillOwnJobTasks(request.Context(), jobs)
	writeJSON(writer, http.StatusOK, struct {
		Jobs []hubOwnJob `json:"jobs"`
	}{Jobs: jobs})
}

// hubJobReapRequest is `jobs close`: the hub has the job's executing node
// close its pane and retires the lane rows that pointed at it.
type hubJobReapRequest struct {
	Lane  string `json:"lane"`
	Job   string `json:"job"`
	Force bool   `json:"force"`
}

// hubJobReapResult names the machine and pane the hub acted on, the outcome
// the node reported, the reason a refusal carries, and the lane rows the hub
// retired. Refusal is a closed reason code, never free text.
type hubJobReapResult struct {
	Lane     string   `json:"lane,omitempty"`
	Job      string   `json:"job,omitempty"`
	Machine  string   `json:"machine,omitempty"`
	Pane     string   `json:"pane,omitempty"`
	Outcome  string   `json:"outcome,omitempty"`
	Refusal  string   `json:"refusal,omitempty"`
	Failure  string   `json:"failure,omitempty"`
	Released []string `json:"released,omitempty"`
}

// handleJobReap answers POST /v1/jobs/reap. The hub authorizes the caller's
// lane, checks that the job's owner lane is that lane, and forwards the close
// to the node the job runs on; the node re-checks the job's own ledger and
// herdr before it closes anything (docs/node-commands.md, job.reap).
func (h *HubServer) handleJobReap(writer http.ResponseWriter, request *http.Request) {
	caller := h.authenticate(request)
	if caller.kind == hubCallerNone {
		hubUnauthorized(writer)
		return
	}
	var body hubJobReapRequest
	if !decodeHubJSON(writer, request, hubNodeCommandMaxBytes, &body) {
		writeJSON(writer, http.StatusBadRequest, hubJobReapResult{Refusal: "invalid_request"})
		return
	}
	if !validHistoricalLaneResource(body.Lane) || !hubJobIDPattern.MatchString(body.Job) {
		writeJSON(writer, http.StatusBadRequest, hubJobReapResult{Refusal: "invalid_request"})
		return
	}
	if status, reason := h.ownJobsDenial(request, body.Lane); status != 0 {
		refuseOwnJobs(writer, status, reason)
		return
	}
	h.mu.Lock()
	job := h.jobs[body.Job]
	var node, owner string
	if job != nil {
		node, owner = job.Node, job.OwnerLane
	}
	h.mu.Unlock()
	if job == nil {
		writeJSON(writer, http.StatusNotFound, hubJobReapResult{Refusal: "job_unknown"})
		return
	}
	if owner != body.Lane {
		writeJSON(writer, http.StatusForbidden, hubJobReapResult{Refusal: "owner_mismatch"})
		return
	}
	args, _ := json.Marshal(nodeJobReapArgs{Job: body.Job, Lane: body.Lane, Force: body.Force})
	commandStart := h.now().UTC()
	pending, requestID, state, sent := h.sendNodeCommand(node, nodeCommandJobReap, args)
	if !sent {
		refusal := "node_offline"
		if state == "send_failed" {
			refusal = "close_unconfirmed"
		}
		writeJSON(writer, http.StatusServiceUnavailable, hubJobReapResult{Refusal: refusal})
		return
	}
	timer := time.NewTimer(hubNodeWait(h.nodeCommandWait, hubNodeCommandWait))
	defer timer.Stop()
	result := h.awaitNodeCommand(request, requestID, pending, timer.C)
	if result.Error == "node_lost" || result.Error == "node_timeout" {
		writeJSON(writer, http.StatusServiceUnavailable, hubJobReapResult{Refusal: "close_unconfirmed"})
		return
	}
	if result.Error != "" {
		writeJSON(writer, http.StatusServiceUnavailable, hubJobReapResult{Failure: result.Error})
		return
	}
	var reaped nodeJobReapResult
	if json.Unmarshal(result.Result, &reaped) != nil {
		writeJSON(writer, http.StatusBadGateway, hubJobReapResult{Refusal: "invalid_node_response"})
		return
	}
	// The node names the pane it verified, and only that pane is released. A
	// job whose ledger names no pane proves nothing about the hub record's
	// pane: that id may have been reused by a live unrelated pane since, and
	// retiring its lane row would cut a route nobody asked to close.
	answer := hubJobReapResult{Lane: body.Lane, Job: body.Job, Machine: node, Pane: reaped.Pane, Outcome: reaped.Outcome}
	switch reaped.Outcome {
	case nodeJobReapClosed, nodeJobReapPaneGone:
		if reaped.Pane != "" {
			answer.Released = h.releasePaneLanes(node, reaped.Pane, commandStart)
		}
		h.logger.Info("job reaped", "client", caller.id, "lane", body.Lane, "job", body.Job, "machine", node, "pane", answer.Pane, "outcome", reaped.Outcome, "released", answer.Released)
		writeJSON(writer, http.StatusOK, answer)
	default:
		answer.Refusal = reaped.Outcome
		h.logger.Info("job reap refused", "client", caller.id, "lane", body.Lane, "job", body.Job, "machine", node, "pane", answer.Pane, "outcome", reaped.Outcome)
		writeJSON(writer, http.StatusConflict, answer)
	}
}

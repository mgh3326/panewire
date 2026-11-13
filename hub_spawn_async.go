package panewire

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/spawnargs"
)

func (h *HubServer) newSpawnApp(out, errout io.Writer) (*fleet.App, error) {
	if h.spawnNewApp != nil {
		return h.spawnNewApp(out, errout)
	}
	app, err := fleet.New(out, errout)
	if app != nil {
		app.LiveSpawnProfiles = h.liveSpawnProfiles
	}
	return app, err
}

func (h *HubServer) handleSpawnFeatures(w http.ResponseWriter, r *http.Request) {
	if h.authenticate(r).kind != hubCallerOperator {
		hubUnauthorized(w)
		return
	}
	// Start conditions are deliberately not advertised until the queue core
	// owns their lifecycle and expiry semantics.
	writeJSON(w, http.StatusOK, map[string][]string{"features": {}})
}

// decodeSpawnRequest authenticates and validates a spawn request for the submit
// and plan routes. When it returns false it has already written the refusal.
func (h *HubServer) decodeSpawnRequest(w http.ResponseWriter, r *http.Request) (fleet.Submission, fleet.Spawn, bool) {
	// Placement chooses a resource after acceptance; only the unscoped operator
	// may grant that authority. Per-host service tokens cannot escape their host.
	if h.authenticate(r).kind != hubCallerOperator {
		hubUnauthorized(w)
		return fleet.Submission{}, fleet.Spawn{}, false
	}
	var request fleet.Submission
	if !decodeHubJSON(w, r, hubSpawnMaxBodyBytes, &request) {
		writeJSON(w, 400, map[string]string{"error": "invalid_request"})
		return fleet.Submission{}, fleet.Spawn{}, false
	}
	spawn, err := fleet.ValidateSubmission(request)
	if err == nil {
		raw, _ := json.Marshal(request.Repository.Checkout)
		var spec hubSpawnCheckout
		if request.Repository.Checkout == nil || json.Unmarshal(raw, &spec) != nil || !validHubSpawnCheckout(spec) || spec.Dir != request.Repository.Worktree || spec.Sha == "" || spec.Origin != request.Repository.Origin {
			err = errors.New("invalid pinned checkout")
		}
	}
	if err != nil || !validHubSpawnRequestID(request.RequestID) || !hubSpawnMachinePattern.MatchString(request.OriginMachine) {
		writeJSON(w, 400, map[string]string{"error": "invalid_request", "fix": "rebuild the request with panewire spawn"})
		return fleet.Submission{}, fleet.Spawn{}, false
	}
	if len(request.StartConditions) != 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "start_conditions_unsupported", "reason": "queue_core_required", "fix": "start conditions are not enabled on this hub; upgrade after queue-core support is deployed"})
		return fleet.Submission{}, fleet.Spawn{}, false
	}
	// The node applies this same rule to the args it receives; refusing here
	// keeps a submitter whose own check is older from binding an operation the
	// node would drop.
	if reason := spawnargs.Refusal(request.Args, true); reason != "" {
		writeJSON(w, 400, map[string]string{"error": "invalid_request", "reason": reason, "fix": "correct the named flag; the node would refuse this request"})
		return fleet.Submission{}, fleet.Spawn{}, false
	}
	return request, spawn, true
}

// bindPlacement gives a placement app the hub's view of the fleet: the host list
// and the hub state it reads. The spawn executor and the dry-run plan both place
// through it, so a plan sees the hosts a real spawn would.
func (h *HubServer) bindPlacement(app *fleet.App, transport fleet.HubTransport) {
	app.Hub = transport
	app.State = directSpawnState{h}
	app.PlacementHosts = func(ctx context.Context, cwd string) ([]fleet.HostView, error) {
		result := h.placementFresh(ctx, "worker", cwd)
		raw, err := json.Marshal(result)
		if err != nil {
			return nil, err
		}
		views, err := fleet.ParseHubHosts(string(raw))
		for _, machine := range result.slotFull {
			views = append(views, fleet.HostView{Name: machine, Connected: true, SlotFull: true})
		}
		return views, err
	}
}

func (h *HubServer) handleSpawnSubmit(w http.ResponseWriter, r *http.Request) {
	request, spawn, ok := h.decodeSpawnRequest(w, r)
	if !ok {
		return
	}
	if h.inbox == nil {
		writeJSON(w, 503, map[string]string{"error": "durable_store_required", "fix": "configure the hub inbox database before accepting spawns"})
		return
	}
	owner := spawn.Owner
	if !validLaneReference(owner) {
		writeJSON(w, 400, map[string]string{"error": "invalid_owner", "fix": "supply an existing lane name"})
		return
	}
	// A replay does not need placement, quota or hk to be available again.
	if old, err := h.inbox.store.spawnTask(r.Context(), request.RequestID); err == nil {
		a, _ := json.Marshal(old.Request)
		b, _ := json.Marshal(request)
		if old.Owner != owner || !bytes.Equal(a, b) {
			writeJSON(w, 409, map[string]string{"error": "idempotency_payload_conflict", "fix": "replay the original request unchanged or use a new job id"})
			return
		}
		if old.Status == "retryable" {
			old, err = h.inbox.store.resumeCleanedSpawn(r.Context(), old, h.now().UTC())
			if err != nil {
				writeJSON(w, 409, map[string]string{"error": "job_already_claimed"})
				return
			}
		}
		h.acceptSpawnResponse(w, old)
		select {
		case h.spawnKick <- struct{}{}:
		default:
		}
		return
	} else if !errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, 503, map[string]string{"error": "store_unavailable"})
		return
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil || app.CheckTrust() != nil {
		writeJSON(w, 503, map[string]string{"error": "spawn_config_unavailable", "fix": "configure hub fleet policy and trust before retrying this request"})
		return
	}
	// Input the saved request can never fix is refused here, in the
	// submitter's terminal, not by a SPAWN_FAILED event after acceptance
	// (hk 4270; internal/fleet/submit_preflight.go).
	if !fleet.ValidAgentName(spawn.Label) {
		writeJSON(w, 400, map[string]string{"error": "invalid_request", "reason": "label_invalid", "fix": "pass -l LABEL as a herdr agent name: a lowercase letter first, then lowercase letters, digits, - or _, at most 32 characters"})
		return
	}
	if err := app.SubmitHostRefusal(spawn); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid_request", "reason": "no_eligible_host", "fix": spawnRefusalLine(err.Error())})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	existing, found, err := app.ExistingJob(ctx, spawn.Job)
	if err != nil {
		writeJSON(w, 503, map[string]string{"error": "job_store_unavailable", "fix": "restore handoffkeep access and retry this same request"})
		return
	}
	if found {
		// hk holds no retry marker (it takes a result only with state=completed),
		// so the job's newest hub operation says whether cleanup was verified.
		latest, latestErr := h.inbox.store.latestSpawnForJob(r.Context(), spawn.Job)
		if latestErr != nil && !errors.Is(latestErr, sql.ErrNoRows) {
			writeJSON(w, 503, map[string]string{"error": "store_unavailable"})
			return
		}
		if existing["state"] != "claimed" || latestErr != nil || latest.Status != "retryable" {
			writeJSON(w, 409, map[string]string{"error": "job_already_claimed", "fix": "inspect the existing job; use a new job id for independent work"})
			return
		}
		// HK claims are immutable host/task bindings. Safe retries stay on that host.
		if host, _ := existing["host"].(string); host != "" && spawn.Host != host {
			writeJSON(w, 409, map[string]string{"error": "retry_host_required", "fix": "retry with --host " + host + " --override-reason verified-cleanup-retry"})
			return
		}
	}
	task, _, err := h.inbox.store.claimSpawn(r.Context(), request, spawn.Job, owner, h.now().UTC())
	held := map[string]string{"error": "job_already_claimed", "fix": "inspect the existing operation; do not submit a new request while ownership is live"}
	retryAfterS := 0
	if errors.Is(err, errSpawnClaimed) && spawn.ParentJob == "" {
		task, held, retryAfterS, err = h.reclaimSpawnRole(r.Context(), app, request, spawn, owner)
	}
	if errors.Is(err, errSpawnClaimed) {
		writeSpawnHeld(w, held, retryAfterS)
		return
	}
	if err != nil {
		writeJSON(w, 503, map[string]string{"error": "store_unavailable", "fix": "restore the durable store and retry this same request"})
		return
	}
	h.supersedeRetryableHolders(r.Context(), app, request, spawn)
	h.acceptSpawnResponse(w, task)
	select {
	case h.spawnKick <- struct{}{}:
	default:
	}
}

// writeSpawnHeld answers a held reservation with 409. retryAfterS above zero
// means the hold frees on its own after that many seconds: the body carries it
// as retry_after_s and the Retry-After header repeats it. Every other hold has
// neither, because nothing time-based will free it.
func writeSpawnHeld(w http.ResponseWriter, held map[string]string, retryAfterS int) {
	if retryAfterS < 1 {
		writeJSON(w, 409, held)
		return
	}
	body := make(map[string]any, len(held)+1)
	for key, value := range held {
		body[key] = value
	}
	body["retry_after_s"] = retryAfterS
	w.Header().Set("Retry-After", strconv.Itoa(retryAfterS))
	writeJSON(w, 409, body)
}

func (h *HubServer) acceptSpawnResponse(w http.ResponseWriter, task hubSpawnTask) {
	writeJSON(w, 202, map[string]string{"job_id": task.Job, "request_id": task.Request.RequestID, "status": task.Status, "operation_url": "/v1/spawns/" + task.Request.RequestID, "claim": "durable_hub_reservation"})
}
func (h *HubServer) handleSpawnOperation(w http.ResponseWriter, r *http.Request) {
	if h.authenticate(r).kind != hubCallerOperator {
		hubUnauthorized(w)
		return
	}
	if h.inbox == nil {
		writeJSON(w, 503, map[string]string{"error": "durable_store_required"})
		return
	}
	task, err := h.inbox.store.spawnTask(r.Context(), r.PathValue("request_id"))
	if errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, 404, map[string]string{"error": "not_found"})
		return
	}
	if err != nil {
		writeJSON(w, 503, map[string]string{"error": "store_unavailable"})
		return
	}
	var startCondition *fleet.StartConditionResult
	if len(task.Request.StartConditions) != 0 && spawnPending(task.Status) {
		result := evaluateHubStartCondition(r.Context(), h, task.Request.StartConditions)
		startCondition = &result
	}
	// Never expose the persisted brief, argv or submitting paths through status
	// reads. A reason's refusal text may name a path on the executing node.
	writeJSON(w, 200, struct {
		Job            string                      `json:"job_id"`
		Request        string                      `json:"request_id"`
		Status         string                      `json:"status"`
		Result         hubSpawnOutcome             `json:"result"`
		StartCondition *fleet.StartConditionResult `json:"start_condition,omitempty"`
	}{task.Job, task.Request.RequestID, task.Status, task.Outcome, startCondition})
}

func evaluateHubStartCondition(ctx context.Context, h *HubServer, conditions []fleet.StartCondition) fleet.StartConditionResult {
	snapshots, err := fleet.ReadStartConditionTasks(ctx, conditions)
	if err != nil {
		return fleet.StartConditionResult{Status: "waiting", Reasons: []string{"hk_unavailable"}}
	}
	return fleet.EvaluateStartConditions(conditions, snapshots, h.now().UTC())
}

// hubSpawnWorkerCount bounds how many accepted spawns the hub executes at once.
// Each worker holds its request for the whole remote landing, so a slow or
// restarting node keeps workers busy; with four, a reviewer burst left requests
// queued past their 600 s deadline (hk 4348, 4368). Eight is a stopgap until
// one handler per node replaces the global pool (hk 4358).
const hubSpawnWorkerCount = 8

func (h *HubServer) runSpawnCoordinator(parent context.Context) {
	if h.inbox == nil {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	stop := context.AfterFunc(h.closeCtx, cancel)
	defer stop()
	work := make(chan hubSpawnTask, hubSpawnWorkerCount)
	var mu sync.Mutex
	active := map[string]bool{}
	for range hubSpawnWorkerCount {
		h.spawnWorkers.Add(1)
		go func() {
			defer h.spawnWorkers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case task := <-work:
					// A draining hub starts nothing new; the durable row stays
					// pending for the next hub (hub_drain.go).
					if h.drain.enter() {
						h.inbox.store.touchSpawn(ctx, task.Request.RequestID, h.now().UTC())
						h.executeSpawnTask(ctx, task)
						h.drain.leave()
					}
					mu.Lock()
					delete(active, task.Request.RequestID)
					mu.Unlock()
				}
			}
		}()
	}
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		now := h.now().UTC()
		tasks, err := h.inbox.store.pendingSpawns(ctx, now)
		if err != nil {
			tasks = nil
		}
		conditioned, conditionErr := h.inbox.store.pendingStartConditionSpawns(ctx, now)
		if conditionErr == nil {
			tasks = append(tasks, conditioned...)
		}
		if len(tasks) != 0 && h.drain.admit() {
			for _, task := range tasks {
				mu.Lock()
				busy := active[task.Request.RequestID]
				if !busy {
					active[task.Request.RequestID] = true
				}
				mu.Unlock()
				if busy {
					continue
				}
				select {
				case work <- task:
				default:
					mu.Lock()
					delete(active, task.Request.RequestID)
					mu.Unlock()
				}
			}
		}
		h.flushSpawnNotifications(ctx)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-h.spawnKick:
		}
	}
}
func (h *HubServer) executeSpawnTask(parent context.Context, task hubSpawnTask) {
	defer func() {
		if recover() != nil {
			h.finishSpawnOutcome(task, hubSpawnOutcome{RC: 1, Stage: "launch", Reason: "unknown"})
		}
	}()
	if len(task.Request.StartConditions) != 0 {
		if task.Node == nil {
			h.finishSpawnOutcome(task, hubSpawnOutcome{
				RC: 78, Stage: "placement", Reason: "start_conditions_unsupported",
				Fix: "start conditions are disabled until queue-core support is deployed; submit again after the hub advertises start_conditions_v1",
			})
			return
		}
		if h.now().Before(task.Deadline) && evaluateHubStartCondition(parent, h, task.Request.StartConditions).Status != "met" {
			// A bound legacy row may already have reached the node. While the
			// original deadline is live, do not replay an unmet condition because
			// the node treats an unknown request id as a new spawn. At the
			// deadline, the normal replay carries an expired deadline, which lets
			// the node return a journal result or refuse an unstarted request.
			return
		}
	}
	if !h.now().Before(task.Deadline) {
		if task.Status == "deadline_exceeded" && task.Node != nil {
			h.reprobeStaleSpawn(parent, task)
			return
		}
		h.logger.Warn("hub spawn starved in the queue", "request_id", task.Request.RequestID, "job", task.Job, "host", firstNonEmptyString(spawnTaskHost(task), "none"), "queue_wait_s", spawnQueueWaitSeconds(task, h.now()))
		h.finishSpawnOutcome(task, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Host: spawnTaskHost(task), Retryable: task.Node == nil, Fix: spawnQueueWaitFix(task, h.now())})
		if task.Node == nil {
			return
		}
		ctx, cancel := context.WithTimeout(parent, 5*time.Second)
		defer cancel()
		transport := directSpawnTransport{h: h, task: task}
		_, _ = transport.dispatch(ctx, *task.Node)
		h.noteStuckSpawn(task)
		return
	}
	if task.Node == nil && task.Status == "reconciling" {
		var err error
		task, err = h.inbox.store.resumeUndispatchedSpawn(parent, task)
		if err != nil {
			return
		}
	}
	ctx, cancel := context.WithDeadline(parent, task.Deadline)
	defer cancel()
	transport := &directSpawnTransport{h: h, task: task, started: time.Now(), probeWait: spawnNotStartedProbe}
	if task.Node != nil {
		_, _ = transport.dispatch(ctx, *task.Node)
		return
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		h.finishSpawnOutcome(task, hubSpawnOutcome{RC: 78, Stage: "placement", Reason: "configuration", Retryable: true})
		return
	}
	if ctx.Err() != nil {
		h.finishSpawnOutcome(task, hubSpawnOutcome{RC: 124, Stage: "placement", Reason: "timeout", Retryable: true})
		return
	}
	outcome := hubSpawnOutcome{Stage: "placement", StageMS: map[string]int64{}}
	h.bindPlacement(app, transport)
	app.PlacementTry = func(host string) { h.countPlacement(task.Job, host) }
	app.PlacementGate = h.placementGate.acquire
	app.ObserveStage = func(stage string, d time.Duration) { outcome.Stage = stage; outcome.StageMS[stage] += d.Milliseconds() }
	app.ObserveReceipt = func(r fleet.ExecutionReceipt) {
		outcome.Host = r.Host
		outcome.Pane = r.Pane
		outcome.Profile = r.Profile
		outcome.Model = r.Model
		outcome.Role = r.Role
	}
	request := task.Request
	if task.Outcome.Retryable && task.Outcome.Host != "" && !spawnRefusalTriesNextHost(task.Outcome.Reason) {
		request.Args = append([]string(nil), request.Args...)
		for i := 0; i+1 < len(request.Args); i++ {
			if request.Args[i] == "--host" {
				request.Args[i+1] = task.Outcome.Host
				request.RetryHost = task.Outcome.Host
			}
		}
		found := false
		for i := 0; i+1 < len(request.Args); i++ {
			if request.Args[i] == "--override-reason" {
				request.Args[i+1] = "verified-cleanup-retry"
				found = true
			}
		}
		if !found {
			request.Args = append(request.Args, "--override-reason", "verified-cleanup-retry")
		}
	}
	err = app.ExecuteSubmission(ctx, request)
	if transport.bound {
		return
	} // The node result owns completion and cleanup facts.
	if parent.Err() != nil {
		return
	} // Restart resumes durable accepted ownership.
	outcome.RC = fleet.ErrorCode(err)
	outcome.Reason = fleet.AdmissionRefusalReason(err)
	if err != nil {
		outcome.Reason = spawnOutcomeReason(outcome.Reason, spawnRefusalLine(err.Error()))
		outcome.Fix = utf8PrefixWithin(spawnRefusalLine(fleet.SpawnRefusalFix(err)), 512)
	}
	if err != nil && strings.Contains(err.Error(), "hub_spawn_capability_required") {
		outcome.Reason = "node_upgrade_required"
		outcome.Fix = "Upgrade the selected node panewire to advertise spawn_ops_v1, then replay the saved request."
	}
	outcome.Retryable = true
	outcome.E6Arm = request.E6Arm
	if ctx.Err() != nil {
		outcome.RC = 124
		outcome.Reason = "timeout"
	}
	if changed, _ := h.completeSpawn(parent, task, outcome); changed {
		h.recordSpawnTrace(task, outcome)
	}
}
func firstNonEmptyString(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}
func spawnTaskHost(task hubSpawnTask) string {
	if task.Node != nil {
		return task.Node.Machine
	}
	return ""
}

type directSpawnState struct{ h *HubServer }

// Snapshot is the hub's fleet state as a spawn decision reads it. The load
// values in it are recomputed first, so a decision never ranks hosts on a
// value published up to the freshness window ago, and so it sees every job
// already counted onto a host (placement_count.go).
func (s directSpawnState) Snapshot(ctx context.Context) (fleet.FleetSnapshot, error) {
	s.h.refreshFleetLoad(ctx)
	raw, err := json.Marshal(s.h.fleetSnapshot(s.h.now()))
	var snap fleet.FleetSnapshot
	if err == nil {
		err = json.Unmarshal(raw, &snap)
	}
	return snap, err
}

// spawnQueueWaitFix names the step that ran out of time when the coordinator
// finds a request already past its deadline: the hub's own spawn queue. The
// stage this branch reports is the last of the closed set, which reads as a
// node landing that never ran, so the fix text carries the wait instead.
func spawnQueueWaitFix(task hubSpawnTask, now time.Time) string {
	return fmt.Sprintf("the hub did not start this request within its %ds deadline (about %ds in the spawn queue, host=%s); no node ran its landing — check hub spawn worker occupancy, then resubmit", task.Request.DeadlineSeconds, spawnQueueWaitSeconds(task, now), firstNonEmptyString(spawnTaskHost(task), "none"))
}

// spawnQueueWaitSeconds is how long a request waited for a coordinator worker:
// its whole deadline, plus however far past the deadline the worker picked it.
func spawnQueueWaitSeconds(task hubSpawnTask, now time.Time) int64 {
	waited := int64(task.Request.DeadlineSeconds)
	if !task.Deadline.IsZero() {
		waited += int64(now.Sub(task.Deadline) / time.Second)
	}
	if waited < 0 {
		waited = 0
	}
	return waited
}

type directSpawnTransport struct {
	h       *HubServer
	task    hubSpawnTask
	bound   bool
	placing bool // inside the candidate loop, which can try another host
	started time.Time
	// probeWait is how long dispatch keeps asking its node after the landing
	// wait ran out, before it settles the request as a timeout. Zero skips the
	// probe (the coordinator's replay pass, whose own 5 s window is the probe).
	probeWait time.Duration
}

// spawnNotStartedProbe bounds the question dispatch puts to a node once the
// request's deadline has passed: did you ever start this? Past the deadline a
// node never launches a request, so asking again cannot start a second one.
const spawnNotStartedProbe = 5 * time.Second

// spawnNotStarted reports a node answer that proves the request never began:
// the node claimed it only after its deadline, so it holds no executing or
// completed journal row and ran nothing (runClaimedHubSpawn).
func spawnNotStarted(result hubSpawnResult) bool {
	return result.RC == 124 && result.Stage == "claim" && result.Error == "deadline_not_started" && result.Retryable
}

// spawnNotStartedFix tells the caller what a not-started refusal means.
const spawnNotStartedFix = "the node never started this request: it reached the node after its deadline. Replay the same command; the replay starts a new attempt and placement can pick another host."

// nextHostRefusals are the refusals that say only that this host cannot run the
// request now. cwd_unmapped is answered before any checkout or engine call, and
// the hub cannot see a node's spawn.json to rule the node out first. The checkout
// refusals identify host-local state or a missing branch; the other entries are
// first-run refusals a person clears once on the host. The reason names the
// engine's code as spawnResultReason writes it. docs/placement.md, "Readiness".
var nextHostRefusals = map[string]bool{
	"cwd_unmapped":                      true,
	"checkout_refused:dirty":            true,
	"checkout_refused:sha-missing":      true,
	"checkout_refused:branch-elsewhere": true,
	"spawn_failed:hooks_review":         true,
	"spawn_failed:trust_prompt":         true,
	"spawn_failed:omp_setup_wizard":     true,
	"spawn_failed:auth_unavailable":     true,
	"launch_proof_failed":               true,
}

// spawnRefusalTriesNextHost names a node refusal that moves an auto placement
// to the next candidate instead of ending the request.
func spawnRefusalTriesNextHost(reason string) bool {
	code, _, _ := strings.Cut(reason, ": ")
	return nextHostRefusals[code]
}

func (t *directSpawnTransport) Spawn(ctx context.Context, req fleet.HubSpawnRequest) (fleet.HubSpawnReply, error) {
	t.h.mu.Lock()
	record := t.h.nodes[req.Machine]
	oldNode := record != nil && record.agent != nil && record.state == "connected" && !record.capabilities["spawn_ops_v1"]
	t.h.mu.Unlock()
	if oldNode {
		return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_capability_required"}
	}
	if t.bound {
		// A definitive node refusal already completed this attempt. An engine
		// busy retry must not bind a second request or replay the old journal id.
		return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_pending"}
	}
	raw, err := json.Marshal(req)
	if err != nil {
		return fleet.HubSpawnReply{}, err
	}
	var node hubSpawnRequest
	if err = json.Unmarshal(raw, &node); err != nil {
		return fleet.HubSpawnReply{}, err
	}
	if !t.started.IsZero() {
		node.PlacementMS = time.Since(t.started).Milliseconds()
	}
	node.DeadlineMS = t.task.Deadline.UnixMilli()
	node.WaitSeconds = max(1, min(3600, int(time.Until(t.task.Deadline).Seconds())+1))
	// The bind can commit even when its response is lost. From this point a
	// placement caller must not release ownership or fall through to a host.
	t.bound = true
	task, err := t.h.inbox.store.bindSpawn(ctx, t.task.Request.RequestID, t.task.Attempt, node)
	if err != nil {
		return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_pending"}
	}
	t.task = task
	t.placing = true
	return t.dispatch(ctx, *task.Node)
}

// nextHostAfterRefusal hands an auto placement back to its candidate loop. A
// pinned host is the operator's choice, so its refusal stays the answer.
func (t *directSpawnTransport) nextHostAfterRefusal(ctx context.Context, node hubSpawnRequest, result hubSpawnResult) bool {
	// A dialog refusal comes after the engine launched a pane, so only a
	// verified cleanup (Retryable) lets another host take the job id.
	// A request the node never started also moves on, while the request's own
	// deadline still has room for another host.
	movesOn := spawnRefusalTriesNextHost(spawnResultReason(result)) || spawnNotStarted(result) && t.h.now().Before(t.task.Deadline)
	if !t.placing || result.RC == 0 || !result.Retryable || !movesOn || firstNonEmptyString(hubSpawnArg(t.task.Request.Args, "--host"), "auto") != "auto" {
		return false
	}
	if result.Error == "launch_proof_failed" && result.RC != 70 {
		return false
	}
	task, err := t.h.inbox.store.unbindRefusedSpawn(ctx, t.task.Request.RequestID, t.task.Attempt, node.RequestID)
	if err != nil {
		return false
	}
	t.task, t.bound = task, false
	// The refused host no longer holds this job; the next candidate counts it.
	t.h.releasePlacementCount(task.Job)
	reason := spawnResultReason(result)
	t.h.observeLaunchOutcome(node.Machine, hubSpawnArg(node.Args, "-m"), result.RC, reason, t.h.now().UTC())
	t.h.logger.Warn("hub spawn refused by node; trying next candidate", "request_id", task.Request.RequestID, "node_request_id", node.RequestID, "job", task.Job, "host", node.Machine, "profile", hubSpawnArg(node.Args, "-m"), "rc", result.RC, "reason", reason)
	return true
}
func (t *directSpawnTransport) dispatch(ctx context.Context, node hubSpawnRequest) (fleet.HubSpawnReply, error) {
	h := t.h
	h.mu.Lock()
	record := h.nodes[node.Machine]
	if record == nil || record.agent == nil || record.state != "connected" || !record.capabilities["spawn_ops_v1"] {
		h.mu.Unlock()
		return t.unknown(ctx, "disconnected")
	}
	agent := record.agent
	entry := &hubSpawnRecord{request: node, status: "pending", created: h.now(), waiter: make(chan hubSpawnResult, 1)}
	h.spawnRecords[node.RequestID] = entry
	h.mu.Unlock()
	message := struct {
		OperatorAuthorized bool              `json:"operator_authorized"`
		Type               string            `json:"type"`
		RequestID          string            `json:"request_id"`
		CWDKey             string            `json:"cwd_key"`
		Brief              hubSpawnBrief     `json:"brief"`
		Checkout           *hubSpawnCheckout `json:"checkout,omitempty"`
		Args               []string          `json:"args"`
		WaitSeconds        int               `json:"wait_seconds"`
		DeadlineMS         int64             `json:"deadline_ms,omitempty"`
	}{true, "job.spawn", node.RequestID, node.CWDKey, node.Brief, node.Checkout, node.Args, node.WaitSeconds, node.DeadlineMS}
	if agent.writeJSON(message) != nil {
		return t.unknown(ctx, "disconnected")
	}
	select {
	case result := <-entry.waiter:
		return t.settleNodeResult(ctx, node, result)
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.Canceled) {
			return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_pending"}
		}
		if t.probeWait > 0 {
			if result, answered := t.probeNode(agent, entry, message); answered {
				probeCtx, cancel := context.WithTimeout(h.closeCtx, t.probeWait)
				defer cancel()
				return t.settleNodeResult(probeCtx, node, result)
			}
		}
		return t.unknown(h.closeCtx, "timeout")
	}
}

// probeNode asks the node once more after the landing wait ran out, by
// replaying the same request id, and waits at most probeWait for a definite
// answer. Silence or an uncertain answer (still executing, restarted, journal
// trouble) returns false: the caller keeps ownership as a timeout, never
// guessing from silence. An answer to the original frame counts too.
func (t *directSpawnTransport) probeNode(agent *hubAgent, entry *hubSpawnRecord, message any) (hubSpawnResult, bool) {
	if agent.writeJSON(message) != nil {
		return hubSpawnResult{}, false
	}
	timer := time.NewTimer(t.probeWait)
	defer timer.Stop()
	select {
	case result := <-entry.waiter:
		uncertain := result.CompletedAt == nil || result.Error == "reconciling" || result.Error == "unknown" || result.Error == "journal_unavailable" || result.Error == "journal_conflict"
		return result, !uncertain
	case <-timer.C:
		return hubSpawnResult{}, false
	case <-t.h.closeCtx.Done():
		return hubSpawnResult{}, false
	}
}

// settleNodeResult applies one answer from the bound node.
func (t *directSpawnTransport) settleNodeResult(ctx context.Context, node hubSpawnRequest, result hubSpawnResult) (fleet.HubSpawnReply, error) {
	h := t.h
	// unknown: the node holds no live execution of this request. Past the
	// deadline nothing will finish it, so a node without a labelled pane
	// makes it orphaned (hub_spawn_orphan.go).
	if result.CompletedAt != nil && result.Error == "unknown" && !h.now().Before(t.task.Deadline) && h.resolveOrphanedSpawn(t.task) {
		return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_pending"}
	}
	if result.CompletedAt == nil || result.Error == "reconciling" || result.Error == "unknown" || result.Error == "journal_unavailable" || result.Error == "journal_conflict" {
		return t.unknown(ctx, "unknown")
	}
	// The authenticated node's completed journal is necessary, and a current
	// exact pane read is additionally required before accepting late success.
	if result.RC == 0 && (node.Checkout != nil && (result.Receipt == nil || !spawnReceiptCheckoutPath(result.Receipt.CWD, node.Checkout.Dir, hubSpawnArg(node.Args, "-L"))) || !h.verifySpawnPane(ctx, node.Machine, result, node.Args)) {
		return t.unknown(ctx, "unknown")
	}
	h.noteOneTimeActionRefusal(node.Machine, hubSpawnArg(node.Args, "-m"), result)
	if t.nextHostAfterRefusal(ctx, node, result) {
		return fleet.HubSpawnReply{RequestID: result.RequestID, RC: result.RC, JobID: result.JobID, StdoutTail: result.StdoutTail, Error: spawnResultReason(result), NextHost: true}, nil
	}
	stage := result.Stage
	if stage == "" {
		stage = "placement"
	}
	outcome := hubSpawnOutcome{RC: result.RC, Stage: stage, Reason: result.Error, Host: node.Machine, Pane: result.Pane, Retryable: result.Retryable, NodeResult: true, StageMS: result.StageMS, Role: hubSpawnArg(node.Args, "--role"), Profile: hubSpawnArg(node.Args, "-m"), E6Arm: t.task.Request.E6Arm}
	if outcome.StageMS == nil {
		outcome.StageMS = map[string]int64{}
	}
	outcome.StageMS["placement"] = min(int64(3600000), outcome.StageMS["placement"]+node.PlacementMS)
	outcome.Fix = spawnResultFix(result.StdoutTail)
	if outcome.Fix == "" && spawnNotStarted(result) {
		outcome.Fix = spawnNotStartedFix
	}
	if result.Receipt != nil {
		outcome.Profile = result.Receipt.Profile
		outcome.Model = result.Receipt.Model
		outcome.Pool = result.Receipt.Pool
		outcome.AccountKey = result.Receipt.AccountKey
		outcome.Role = result.Receipt.Role
		outcome.LaneUnregistered = result.Receipt.LaneUnregistered
	}
	if outcome.RC != 0 {
		outcome.Reason = spawnOutcomeReason(spawnResultReason(result), spawnResultRefusal(result.StdoutTail))
	}
	changed, err := h.completeSpawn(h.closeCtx, t.task, outcome)
	if err != nil {
		return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_pending"}
	}
	if changed {
		h.recordSpawnTrace(t.task, outcome)
		// The first completion of a request is the launch outcome; a
		// replayed completion must not count twice.
		h.observeLaunchOutcome(node.Machine, firstNonEmptyString(hubSpawnArg(node.Args, "-m"), outcome.Profile), result.RC, outcome.Reason, h.now().UTC())
	}
	return fleet.HubSpawnReply{RequestID: result.RequestID, RC: result.RC, JobID: result.JobID, Pane: result.Pane, StdoutTail: result.StdoutTail, Error: result.Error}, nil
}
func (t *directSpawnTransport) unknown(ctx context.Context, reason string) (fleet.HubSpawnReply, error) {
	outcome := hubSpawnOutcome{RC: 75, Stage: "landing", Reason: reason, Host: spawnTaskHost(t.task), E6Arm: t.task.Request.E6Arm}
	if reason == "timeout" {
		outcome.RC = 124
	}
	if changed, _ := t.h.completeSpawn(ctx, t.task, outcome); changed {
		t.h.recordSpawnTrace(t.task, outcome)
	}
	return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_pending"}
}
func (t *directSpawnTransport) Refresh(ctx context.Context, machine, pool string) error {
	args, _ := json.Marshal(map[string]string{"pool": pool})
	pending, id, _, sent := t.h.sendNodeCommand(machine, "quota.refresh", args)
	if !sent {
		return errors.New("quota refresh node unavailable")
	}
	defer t.h.forgetNodeCommand(id)
	select {
	case result := <-pending.done:
		if result.Error != "" {
			return errors.New("quota refresh failed")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (h *HubServer) verifySpawnPane(ctx context.Context, machine string, result hubSpawnResult, args []string) bool {
	if result.JobID != hubSpawnArg(args, "--job") || result.Pane == "" || result.Receipt == nil || result.Receipt.Job != result.JobID || result.Receipt.Pane != result.Pane || result.Receipt.Label != hubSpawnArg(args, "-l") || result.Receipt.CWD == "" || result.Receipt.Kind == "" {
		return false
	}
	raw, _ := json.Marshal(map[string]any{"pane_id": result.Pane, "name": hubSpawnArg(args, "-l"), "tail_lines": 0, "cwd": result.Receipt.CWD, "kind": result.Receipt.Kind})
	pending, id, _, sent := h.sendNodeCommand(machine, "pane.read", raw)
	if !sent {
		return false
	}
	defer h.forgetNodeCommand(id)
	select {
	case reply := <-pending.done:
		var pane PaneReadResult
		if reply.Error != "" || json.Unmarshal(reply.Result, &pane) != nil {
			return false
		}
		if pane.Outcome == nodePaneReadOK {
			return true
		}
		if pane.Outcome != nodePaneGone {
			return false
		}
		// A completed job can legitimately have been reaped while the hub was
		// disconnected. Require its immutable host and exact historical receipt,
		// in addition to this authenticated node's durable landing result.
		app, err := h.newSpawnApp(io.Discard, io.Discard)
		if err != nil || app.CheckTrust() != nil {
			return false
		}
		record, found, err := app.ExistingJob(ctx, result.JobID)
		if err != nil || !found || record["state"] != "completed" || record["host"] != machine || record["pane_id"] != result.Pane || record["label"] != result.Receipt.Label || record["profile"] != result.Receipt.Profile {
			return false
		}
		task := hubSpawnArg(args, "--task")
		return task == "" || fmt.Sprint(record["task_id"]) == task
	case <-ctx.Done():
		return false
	}
}

// spawnNotificationType keeps a failed spawn out of the owner's digest: the
// submitter only saw ACCEPTED, so nothing else tells it the work did not start.
func spawnNotificationType(result hubSpawnOutcome) string {
	if result.RC != 0 || result.LaneUnregistered {
		return "automation-failed"
	}
	return "report"
}

func spawnNotificationText(job, request string, result hubSpawnOutcome) string {
	if result.RC == 0 {
		text := fmt.Sprintf("[panewire] SPAWNED %s host=%s profile=%s pane=%s model=%s", job, result.Host, result.Profile, result.Pane, result.Model)
		if result.LaneUnregistered {
			text += " LANE_UNREGISTERED: hub lane registration failed after one retry; this pane's findings are not routed"
		}
		return text
	}
	fix := result.Fix
	if fix == "" {
		fix = "Inspect the saved operation and executing node before retrying; preserve unknown ownership."
	}
	replay := "no"
	if result.Retryable {
		replay = "yes"
	}
	text := fmt.Sprintf("[panewire] SPAWN_FAILED %s request=%s host=%s profile=%s rc=%d stage=%s replay=%s reason=%s fix=%s", job, request, firstNonEmptyString(result.Host, "none"), firstNonEmptyString(result.Profile, "none"), result.RC, result.Stage, replay, spawnRefusalLine(result.Reason), spawnRefusalLine(fix))
	return utf8PrefixWithin(text, laneEventTextLimit)
}

// Node results stay in the node's private journal indefinitely, so ACK loss and
// prolonged offline periods never erase the only receipt. Automatic journal GC
// is an explicit non-goal. A pane a node crash left behind is settled by the
// restarted node from herdr (hub_spawn_settle.go), not adopted by the hub.

func (h *HubServer) startSpawnCoordinator(ctx context.Context) {
	h.closeMu.Lock()
	defer h.closeMu.Unlock()
	if h.closed {
		return
	}
	h.spawnCoordinatorOnce.Do(func() {
		h.spawnWorkers.Add(2)
		go func() { defer h.spawnWorkers.Done(); h.runSpawnCoordinator(ctx) }()
		go func() { defer h.spawnWorkers.Done(); h.runQuotaReconcile(ctx) }()
	})
}

func (h *HubServer) finishSpawnOutcome(task hubSpawnTask, outcome hubSpawnOutcome) {
	outcome.E6Arm = task.Request.E6Arm
	if changed, _ := h.completeSpawn(h.closeCtx, task, outcome); changed {
		h.recordSpawnTrace(task, outcome)
	}
}

func spawnReceiptCheckoutPath(cwd, dir, lane string) bool {
	if filepath.Base(cwd) == dir {
		return true
	}
	return (lane == "live" || lane == "mock") && filepath.Base(cwd) == lane && filepath.Base(filepath.Dir(cwd)) == dir
}

// hubSpawnReasonLimit bounds an outcome reason: it is read back through the
// status API and quoted in the owner lane's SPAWN_FAILED event.
const hubSpawnReasonLimit = 768

// spawnOutcomeReason appends the refusal text to a stable reason code. The
// status codes completeSpawn and spawnOutcomeStatus compare stay bare.
func spawnOutcomeReason(code, refusal string) string {
	switch code {
	case "timeout", "disconnected", "unknown":
		return code
	}
	if refusal == "" {
		return code
	}
	return utf8PrefixWithin(code+": "+refusal, hubSpawnReasonLimit)
}

// spawnResultReason is the stable reason code of a failed node result: the
// wire error, with the engine's refusal code appended to a bare spawn_failed.
func spawnResultReason(result hubSpawnResult) string {
	reason := result.Error
	if reason == "" {
		reason = "spawn_failed"
	}
	if code := spawnFailureCode(result); reason == "spawn_failed" && code != "" {
		reason += ":" + code
	}
	return reason
}

// spawnFailureCode is the closed-set code for a spawn_failed node result, or ""
// when it is unclassified. The wire stays bare spawn_failed: nodes and the hub
// deploy at different times, so each derives the code from the same tail and rc.
func spawnFailureCode(result hubSpawnResult) string {
	return fleet.SpawnRefusalCode(spawnResultRefusal(result.StdoutTail), result.RC)
}

// spawnResultRefusal reads what the fleet App refused with from a node result
// tail ("<stdout>\nstderr: <lines>\nFix: <fix>"; see hubSpawnTailWithStderr).
// A config refusal carries its rule in stdout and only the fix on stderr. The
// tail is bounded from the front, so the error survives and warnings go first.
func spawnResultRefusal(tail string) string {
	lines := strings.Split(tail, "\n")
	start := -1
	for i, line := range lines {
		if strings.HasPrefix(line, "stderr: ") {
			start = i
		}
	}
	if start < 0 {
		return ""
	}
	section := append([]string{strings.TrimPrefix(lines[start], "stderr: ")}, lines[start+1:]...)
	for i := len(section) - 1; i >= 0; i-- {
		if strings.HasPrefix(section[i], "Fix: ") {
			section = section[:i]
			break
		}
	}
	text := strings.TrimSpace(strings.Join(section, "\n"))
	if text == "" && start > 0 {
		text = lines[start-1]
	}
	return spawnRefusalLine(text)
}

// spawnRefusalLine makes refusal text safe to store and relay: redacted, on one
// line (handoffkeep rejects control characters) and bounded from its tail.
func spawnRefusalLine(text string) string {
	lines := strings.FieldsFunc(fleetEventRedact(text), func(r rune) bool { return r == '\n' || r == '\r' })
	for i, line := range lines {
		lines[i] = strings.TrimSpace(strings.Map(func(r rune) rune {
			if r < 32 || r >= 0x7f && r <= 0x9f {
				return ' '
			}
			return r
		}, line))
	}
	return utf8SuffixWithin(strings.Join(lines, "; "), hubSpawnReasonLimit-64)
}

func spawnResultFix(tail string) string {
	index := strings.LastIndex(tail, "Fix: ")
	if index < 0 {
		return ""
	}
	fix := strings.SplitN(tail[index+5:], "\n", 2)[0]
	fix = strings.Map(func(r rune) rune {
		if r < 32 || r == 127 {
			return ' '
		}
		return r
	}, fleetEventRedact(fix))
	return utf8PrefixWithin(strings.TrimSpace(fix), 512)
}

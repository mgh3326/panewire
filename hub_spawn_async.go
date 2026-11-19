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
		app.LivePlacedByAccount = h.liveProfilesByAccount
		app.LiveAccountJobs = h.liveAccountJobs
		// Each placed selection's dropped SWE-2 candidates count into the daily
		// loops row (docs/loops.md, selection_drops).
		app.OnSelectionDrops = h.noteSelectionDrops
		// The hub owns the durable account credit marks: its in-process
		// selection reads them directly, in addition to the snapshot (hk 5138).
		app.AccountCreditMarks = func(context.Context) (map[string]time.Time, error) {
			return h.snapshotAccountCreditMarks(h.now().UTC())
		}
	}
	return app, err
}

func (h *HubServer) handleSpawnFeatures(w http.ResponseWriter, r *http.Request) {
	if h.authenticate(r).kind != hubCallerOperator {
		hubUnauthorized(w)
		return
	}
	// spawn_expires_v1: the request carries expires_seconds (hk 4660).
	// checkout_spec_v1: the request names its worktree with
	// --checkout-origin/--checkout-commit/--checkout-ref and carries no -c
	// (hk 4738).
	// start_conditions_v1: --after conditions are queued as waiting and
	// checked in one batched hk snapshot per scheduler pass (hk 4736).
	// checkout_sparse_v1: the request's checkout spec carries exclude[] and
	// include[], which the hub forwards to a node whose hello advertises it
	// and drops for every other node (hk 5098).
	// checkout_branch_v1: an implementer spec spawn may ask for a local branch
	// at its pinned commit instead of a detached HEAD (hk 5118).
	writeJSON(w, http.StatusOK, map[string][]string{"features": {"spawn_expires_v1", "start_conditions_v1", "checkout_spec_v1", "checkout_sparse_v1", "checkout_branch_v1"}})
}

// hubCheckoutSparseRefusal names the sparse list entry a request refuses, or ""
// when the request may proceed (or names no checkout spec). The http refusal
// repeats it so the submitter reads the entry index rather than a generic
// invalid_request (hk 5098).
func hubCheckoutSparseRefusal(checkout *fleet.HubCheckout) string {
	if checkout == nil {
		return ""
	}
	return spawnargs.SparseSpecRefusal(checkout.Mode, checkout.Exclude, checkout.Include)
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
	if request.LadderStep != nil {
		err = errors.New("ladder step metadata is hub-internal")
	}
	// A sparse list's refusal names the bad entry index (hk 5098); the
	// generic invalid_request would hide which one.
	sparseReason := hubCheckoutSparseRefusal(request.Repository.Checkout)
	if err == nil {
		raw, _ := json.Marshal(request.Repository.Checkout)
		var spec hubSpawnCheckout
		if request.Repository.Checkout == nil || json.Unmarshal(raw, &spec) != nil || !validHubSpawnCheckout(spec) || spec.Dir != request.Repository.Worktree || spec.Sha == "" || spec.Origin != request.Repository.Origin {
			err = errors.New("invalid pinned checkout")
		}
	}
	if err != nil || !validHubSpawnRequestID(request.RequestID) || !hubSpawnMachinePattern.MatchString(request.OriginMachine) {
		refusal := map[string]string{"error": "invalid_request", "fix": "rebuild the request with panewire spawn"}
		if sparseReason != "" {
			refusal["reason"] = sparseReason
			refusal["fix"] = "correct the named flag; the node would refuse this request"
		}
		writeJSON(w, 400, refusal)
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
	if h.spawnRollbackDrain {
		writeJSON(w, 503, map[string]string{"error": "spawn_rollback_drain", "fix": "the hub is draining its spawn queue for a rollback; submit again after it restarts without --spawn-rollback-drain"})
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
			release, lockErr := h.lockPlacementTrace(r.Context(), old, true)
			if lockErr != nil {
				writeJSON(w, 503, map[string]string{"error": "store_unavailable"})
				return
			}
			old, err = h.inbox.store.resumeCleanedSpawn(r.Context(), old, h.now().UTC())
			release()
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
	if errors.Is(err, errSpawnClaimed) && spawn.ParentJob == "" {
		task, held, err = h.reclaimSpawnRole(r.Context(), app, request, spawn, owner)
	}
	if errors.Is(err, errSpawnClaimed) {
		writeJSON(w, http.StatusConflict, held)
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

func (h *HubServer) acceptSpawnResponse(w http.ResponseWriter, task hubSpawnTask) {
	body := map[string]string{"job_id": task.Job, "request_id": task.Request.RequestID, "status": spawnPublicStatus(task.Status), "operation_url": "/v1/spawns/" + task.Request.RequestID, "claim": "durable_hub_reservation"}
	if expires := spawnExpiresAt(task); expires != "" {
		body["expires_at"] = expires
	}
	writeJSON(w, 202, body)
}

// Older submitters understand dispatching as pending but not the new bound
// state. Its conservative wire projection does not alter the durable proof.
func spawnPublicStatus(status string) string {
	if status == "bound" {
		return "dispatching"
	}
	return status
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
	// A parked row is an accepted one no candidate can take before its next
	// check (hk 4661): it reads parked, with the reason and the check.
	status, reason, nextCheckAt := spawnPublicStatus(task.Status), "", ""
	if spawnParked(task, h.now()) {
		status, reason, nextCheckAt = "parked", spawnParkPrimary(task.ParkReason), task.NextCheck.UTC().Format(time.RFC3339)
	}
	// Never expose the persisted brief, argv or submitting paths through status
	// reads. A reason's refusal text may name a path on the executing node.
	var startCondition *fleet.StartConditionResult
	if task.Status == "waiting" && task.ConditionResult.Status != "" {
		startCondition = &task.ConditionResult
	}
	writeJSON(w, 200, struct {
		Job            string                      `json:"job_id"`
		Request        string                      `json:"request_id"`
		Status         string                      `json:"status"`
		Reason         string                      `json:"reason,omitempty"`
		NextCheckAt    string                      `json:"next_check_at,omitempty"`
		ExpiresAt      string                      `json:"expires_at,omitempty"`
		Result         hubSpawnOutcome             `json:"result"`
		StartCondition *fleet.StartConditionResult `json:"start_condition,omitempty"`
	}{task.Job, task.Request.RequestID, status, reason, nextCheckAt, spawnExpiresAt(task), task.Outcome, startCondition})
}

// hubSpawnWorkerCount bounds placement work. Node I/O runs in a separate slot
// for each node, so a slow landing cannot consume a placement worker.
const hubSpawnWorkerCount = 8

// hubSpawnDrainMarkTTL bounds a node's drain mark (runSpawnCoordinator) past
// the last scan that saw its waiting request; scans run every 2s.
const hubSpawnDrainMarkTTL = 10 * time.Second

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
	// nodeActive holds each node's open dispatches by overlap key. A node gets
	// a second one only when it advertised the slots and every open dispatch
	// and the candidate are checkout spawns into distinct dirs, the overlap the
	// node admits at once, so a dispatched request never waits on the node
	// past its start window (hk 4578). A node that advertises none gets one.
	nodeActive := map[string]map[string]int{}
	// drainFor marks a node where a request without a checkout is queued
	// behind open dispatches: no new dispatch opens there until it has run, so
	// it is not starved by later checkouts. The mark is refreshed by each scan
	// that still finds the request waiting and lapses when no scan has for
	// hubSpawnDrainMarkTTL (the request ended some other way).
	drainFor := map[string]time.Time{}
	for range hubSpawnWorkerCount {
		h.spawnWorkers.Add(1)
		go func() {
			defer h.spawnWorkers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case task := <-work:
					refreshCount := h.quotaRefresh.completedCount.Load()
					// A draining hub starts nothing new; the durable row stays
					// pending for the next hub (hub_drain.go).
					if h.drain.enter() {
						current, err := h.inbox.store.spawnTask(ctx, task.Request.RequestID)
						if err == nil && current.Attempt == task.Attempt && current.Node == nil && spawnPending(current.Status) {
							h.inbox.store.touchSpawn(ctx, task.Request.RequestID, h.now().UTC())
							h.executeSpawnTask(ctx, current)
						}
						h.drain.leave()
					}
					mu.Lock()
					delete(active, task.Request.RequestID)
					mu.Unlock()
					current, err := h.inbox.store.spawnTask(ctx, task.Request.RequestID)
					if err == nil && (current.Node != nil || current.Status != "accepted") {
						h.kickSpawnPlacement()
					}
					// A refresh may finish while this task is still marked active;
					// its wake can then be consumed before the task is eligible.
					if h.quotaRefresh.completedCount.Load() != refreshCount {
						h.kickSpawnPlacement()
					}
				}
			}
		}()
	}
	schedule := func(task hubSpawnTask) bool {
		if task.Status == "bound" && task.Node != nil && h.handleQueuedBinding(ctx, task) {
			return true
		}
		slots := 1
		if task.Node != nil {
			h.mu.Lock()
			if record := h.nodes[task.Node.Machine]; record != nil {
				slots = hubNodeSpawnSlots(record.capabilities)
			}
			h.mu.Unlock()
		}
		mu.Lock()
		busy := active[task.Request.RequestID]
		var key string
		if task.Node != nil {
			key = hubSpawnOverlapKey(task.Node)
			machine := task.Node.Machine
			open := nodeActive[machine]
			if len(open) > 0 {
				busy = busy || len(open) >= slots || key == "" || open[""] > 0
				for held := range open {
					busy = busy || spawnOverlapConflicts(held, key)
				}
			}
			if mark, marked := drainFor[machine]; marked && time.Since(mark) > hubSpawnDrainMarkTTL {
				delete(drainFor, machine)
			}
			switch {
			case key == "" && busy && !active[task.Request.RequestID]:
				drainFor[machine] = time.Now()
			case key == "" && !busy:
				delete(drainFor, machine)
			case key != "":
				_, draining := drainFor[machine]
				busy = busy || draining
			}
		}
		if !busy {
			active[task.Request.RequestID] = true
			if task.Node != nil {
				if nodeActive[task.Node.Machine] == nil {
					nodeActive[task.Node.Machine] = map[string]int{}
				}
				nodeActive[task.Node.Machine][key]++
			}
		}
		mu.Unlock()
		if busy {
			return false
		}
		if task.Node != nil {
			h.spawnWorkers.Add(1)
			go func(task hubSpawnTask) {
				defer h.spawnWorkers.Done()
				if h.drain.enter() {
					current, err := h.inbox.store.spawnTask(ctx, task.Request.RequestID)
					if err == nil && current.Attempt == task.Attempt && current.Node != nil && current.Node.RequestID == task.Node.RequestID && current.Node.Machine == task.Node.Machine && spawnPending(current.Status) {
						h.inbox.store.touchSpawn(ctx, task.Request.RequestID, h.now().UTC())
						h.executeSpawnTask(ctx, current)
					}
					h.drain.leave()
				}
				mu.Lock()
				delete(active, task.Request.RequestID)
				if open := nodeActive[task.Node.Machine]; open != nil {
					if open[key]--; open[key] <= 0 {
						delete(open, key)
					}
					if len(open) == 0 {
						delete(nodeActive, task.Node.Machine)
					}
				}
				mu.Unlock()
				current, err := h.inbox.store.spawnTask(ctx, task.Request.RequestID)
				if err == nil && (current.Node == nil || !spawnPending(current.Status) || current.Node.RequestID != task.Node.RequestID) {
					h.kickSpawnPlacement()
				}
			}(task)
			return true
		}
		select {
		case work <- task:
			return true
		default:
			mu.Lock()
			delete(active, task.Request.RequestID)
			mu.Unlock()
			return false
		}
	}
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	refreshSeen := h.quotaRefresh.completedCount.Load()
	ticked := true
	// Under a rollback drain the gate count is logged at once and each
	// minute (wall clock: an elapsed interval).
	var rollbackLogged time.Time
	for {
		// A finished quota refresh is a quota change for parked requests.
		if count := h.quotaRefresh.completedCount.Load(); count != refreshSeen {
			refreshSeen = count
			// Noted, not kicked: this pass wakes it now.
			h.spawnWakeMu.Lock()
			h.markSpawnWakeLocked(true, spawnParkQuota)
			h.spawnWakeMu.Unlock()
		}
		h.wakeParkedSpawns(ctx, ticked)
		ticked = false
		if h.spawnRollbackDrain && time.Since(rollbackLogged) >= time.Minute {
			rollbackLogged = time.Now()
			h.logSpawnRollbackRows(ctx)
		}
		h.stepSpawnCoordinator(ctx, schedule)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			ticked = true
		case <-h.spawnKick:
			// The kick is a write announcing work for the queue (a submission, a
			// wake, a finished dispatch): the scan reads on this wake.
			h.inbox.store.readGate(readSpawnScan).touch()
		case <-h.inbox.store.readGate(readSpawnScan).wake:
		case <-h.inbox.store.readGate(readSpawnNotice).wake:
		case <-h.inbox.store.readGate(readSpawnTaskFail).wake:
		}
	}
}

// stepSpawnCoordinator is one iteration of the coordinator's reads. Each read
// touches the store only when a write kicked it, its earliest due time passed,
// or the backstop lapsed (read_gate.go): an idle coordinator sends no query
// between backstops (hk 5302).
func (h *HubServer) stepSpawnCoordinator(ctx context.Context, schedule func(hubSpawnTask) bool) {
	if h.drain.admit() {
		h.loopTick(loopSpawnCoordinator)
		h.gatedRead(readSpawnScan, "", func() readResult { return h.spawnScanPass(ctx, schedule) })
	}
	h.gatedRead(readSpawnNotice, "", func() readResult { return h.flushSpawnNotifications(ctx) })
	// Terminal spawn failures with --task owe their hk task one comment (hk
	// 5137); the sweep posts each once and retries failures with backoff.
	h.gatedRead(readSpawnTaskFail, "", func() readResult { return h.flushSpawnTaskFailNotices(ctx) })
}

// spawnScanRetry is how soon the coordinator looks at the queue again while a
// scan found work: the cadence it always had (hk 5302).
const spawnScanRetry = 2 * time.Second

// spawnScanPass is one gated pass over the queue: start conditions, the parked
// notices, and the runnable page. A pass that found nothing to run asks the
// queue when to look next, so an idle coordinator wakes for a parked request's
// next_check_ms, a waiting row's poll or a stale probe, and for nothing else.
func (h *HubServer) spawnScanPass(ctx context.Context, schedule func(hubSpawnTask) bool) readResult {
	// Start conditions are checked first so a row released in this pass is
	// scheduled by the scan below rather than the next one.
	h.checkWaitingSpawns(ctx)
	// A request parked too long tells its owner once (hk 4662); the sweep is
	// rate-limited to spawnParkNoticePoll.
	h.checkParkedSpawnNotices(ctx)
	found, failed := h.spawnCoordinatorScan(ctx, schedule)
	if failed {
		return readResult{failed: true}
	}
	if found {
		// Rows are in flight or waiting for a slot: read again at the old cadence.
		return readResult{found: true, retry: spawnScanRetry}
	}
	now := h.now().UTC()
	wake, err := h.inbox.store.spawnQueueWake(ctx, now)
	if err != nil {
		h.loopNoteSilentError(loopSpawnCoordinator, "hub_spawn_async.go:spawnQueueWake", err)
		return readResult{failed: true}
	}
	h.loopClearSilent("hub_spawn_async.go:spawnQueueWake")
	due := wake.next
	for _, task := range wake.stale {
		at := spawnStaleProbeAt(task, now)
		if !at.After(now) {
			// Due already (a row never probed has no last run): the probe
			// interval grew since the scan, so read again at the usual cadence.
			at = now.Add(spawnScanRetry)
		}
		due = earliestDue(due, at.UnixMilli())
	}
	if wake.waiting > 0 {
		due = earliestDue(due, h.waitingSpawnPollAt(now).UnixMilli())
	}
	if wake.parked > 0 {
		due = earliestDue(due, h.parkedNoticePollAt(now).UnixMilli())
	}
	return readResult{due: due}
}

// spawnCoordinatorScan pages the pending spawn rows and hands each new row to
// schedule, recording made/done in the loop ledger. A store failure counts a
// silent ledger error (one log line per streak) and keeps the pass's other
// half: a failed page read still tries the ordinary pending read below.
//
// It reports whether it handed any row to schedule, and whether a read failed.
func (h *HubServer) spawnCoordinatorScan(ctx context.Context, schedule func(hubSpawnTask) bool) (found, failed bool) {
	// Work claimed during a scan may move later in the ordering. Do not
	// schedule the same row twice in one pass, even if it reappears.
	seen := map[string]bool{}
	position := spawnScanPosition{}
	scanFailed := false
	for {
		tasks, next, err := h.inbox.store.pendingSpawnPage(ctx, position, h.now().UTC())
		if err != nil {
			h.loopNoteSilentError(loopSpawnCoordinator, "hub_spawn_async.go:pendingSpawnPage", err)
			scanFailed = true
			break
		}
		for _, task := range tasks {
			if !seen[task.Request.RequestID] {
				seen[task.Request.RequestID] = true
				h.loopNoteMade(loopSpawnCoordinator, task.Request.RequestID)
				if schedule(task) {
					h.loopNoteDone(loopSpawnCoordinator, task.Request.RequestID)
				}
			}
		}
		found = found || len(tasks) > 0
		if len(tasks) < 32 {
			break
		}
		position = next
	}
	if !scanFailed {
		h.loopClearSilent("hub_spawn_async.go:pendingSpawnPage")
	}
	// Deadline-exceeded replays retain their existing bounded backoff.
	now := h.now().UTC()
	ordinary, err := h.inbox.store.pendingSpawns(ctx, now)
	if err != nil {
		h.loopNoteSilentError(loopSpawnCoordinator, "hub_spawn_async.go:pendingSpawns", err)
		scanFailed = true
	} else {
		found = found || len(ordinary) > 0
		h.loopClearSilent("hub_spawn_async.go:pendingSpawns")
		var oldest time.Time
		for _, task := range ordinary {
			if oldest.IsZero() || task.Created.Before(oldest) {
				oldest = task.Created
			}
			if task.Status == "deadline_exceeded" {
				h.loopNoteMade(loopSpawnCoordinator, task.Request.RequestID)
				if schedule(task) {
					h.loopNoteDone(loopSpawnCoordinator, task.Request.RequestID)
				}
			}
		}
		if !oldest.IsZero() {
			h.loopSetOldestPending(loopSpawnCoordinator, now.Sub(oldest))
		}
	}
	return found, scanFailed
}

// handleQueuedBinding expires or unbinds an unsent row independently of the
// node slot. Its durable CAS decides whether a concurrent send won.
func (h *HubServer) handleQueuedBinding(ctx context.Context, task hubSpawnTask) bool {
	if !h.drain.enter() {
		return true
	}
	defer h.drain.leave()
	if !h.now().Before(task.Deadline) {
		h.executeSpawnTask(ctx, task)
		return true
	}
	h.mu.Lock()
	record := h.nodes[task.Node.Machine]
	admitted := record != nil && record.agent != nil && record.state == "connected" && record.capabilities["spawn_ops_v1"] && h.acceptingEffectiveLocked(task.Node.Machine, record.accepting)
	h.mu.Unlock()
	if admitted {
		return false
	}
	if _, err := h.inbox.store.unbindUnsentSpawn(ctx, task.Request.RequestID, task.Attempt, task.Node.RequestID); err == nil {
		h.releasePlacementCount(task.Job)
		h.kickSpawnPlacement()
	}
	return true
}

func (h *HubServer) executeSpawnTask(parent context.Context, task hubSpawnTask) {
	defer func() {
		if recover() != nil {
			h.finishSpawnOutcome(task, hubSpawnOutcome{RC: 1, Stage: "launch", Reason: "unknown"})
		}
	}()
	if len(task.Request.StartConditions) != 0 && task.ConditionsMet.IsZero() {
		if task.Node == nil {
			// A conditioned row that became executable without passing the
			// queue (a legacy row, a replay, a node refusal that unbound it,
			// a staged activation) goes back to waiting instead of launching
			// with its --after predicates unchecked. The queue checks it and
			// releases it to accepted once they hold.
			_, _ = h.inbox.store.returnToWaiting(parent, task)
			return
		}
		if h.now().Before(task.Deadline) {
			// A row bound before the queue owned start conditions (hk 4736)
			// keeps the guard it had: while its original deadline is live, do
			// not replay an unmet condition because the node treats an unknown
			// request id as a new spawn.
			snapshot, err := readStartConditionSnapshot(parent, [][]fleet.StartCondition{task.Request.StartConditions})
			if err != nil || fleet.EvaluateStartConditionSnapshot(task.Request.StartConditions, snapshot, h.now().UTC()).Status != "met" {
				return
			}
		}
	}
	if !h.now().Before(task.Deadline) {
		if task.Status == "deadline_exceeded" && task.Node != nil {
			h.reprobeStaleSpawn(parent, task)
			return
		}
		unsent := task.Node == nil || task.Status == "bound"
		if task.Status == "bound" {
			if !h.spawnActions.begin(task.Request.RequestID) {
				return
			}
			defer h.spawnActions.end(task.Request.RequestID)
		}
		wait, failure, refreshWait := h.spawnRefreshWaitOutcome(task)
		if refreshWait {
			refreshReason := "none"
			if failure != nil {
				refreshReason = failure.Reason
			}
			h.logger.Warn("hub spawn timed out awaiting a quota refresh", "request_id", task.Request.RequestID, "job", task.Job, "machine", wait.Machine, "pool", wait.Pool, "refresh_reason", refreshReason, "queue_wait_s", spawnQueueWaitSeconds(task, h.now()))
		} else if unsent {
			h.logger.Warn("hub spawn starved in the queue", "request_id", task.Request.RequestID, "job", task.Job, "host", firstNonEmptyString(spawnTaskHost(task), "none"), "queue_wait_s", spawnQueueWaitSeconds(task, h.now()))
		} else {
			h.logger.Warn("hub spawn deadline elapsed with ownership unresolved", "request_id", task.Request.RequestID, "job", task.Job, "host", spawnTaskHost(task))
		}
		outcome := hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Host: spawnTaskHost(task), Retryable: unsent}
		if refreshWait {
			outcome.QuotaRefresh = failure
			outcome.Fix = spawnRefreshWaitFix(task, wait, failure)
		} else {
			outcome.Fix = spawnQueueWaitFix(task, h.now())
		}
		h.finishSpawnOutcome(task, outcome)
		// A parked pass emitted no refusal event (App.Parks): its end at the
		// limit is the one terminal spawn event (hk 4661).
		if task.ParkReason != "" {
			if ended, err := h.inbox.store.spawnTask(parent, task.Request.RequestID); err == nil && ended.Status == "given_up" {
				h.emitFleet(fleetEvent{Kind: "spawn", Job: task.Job, RequestID: task.Request.RequestID, Outcome: "refused", Reason: "given_up", Detail: "park_reason=" + task.ParkReason})
			}
		}
		if unsent {
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
	// This pass now decides placement: drop the link the previous pass left so
	// a pass that ends for another reason cannot name a stale machine/pool.
	h.clearSpawnRefreshWaits(task)
	ctx, cancel := context.WithDeadline(parent, task.Deadline)
	defer cancel()
	transport := &directSpawnTransport{h: h, task: task, started: time.Now(), probeWait: spawnNotStartedProbe, requireSnapshot: task.Node == nil}
	if task.Node != nil {
		_, _ = transport.dispatch(ctx, *task.Node)
		return
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		h.finishSpawnOutcome(task, hubSpawnOutcome{RC: 78, Stage: "placement", Reason: "configuration", Retryable: true})
		return
	}
	// The queue core's outbox owns this spawn's hk placement record: persist
	// every trace the App emits so the record survives the App and a restart.
	app.PlacementOutbox = true
	var previous fleet.PlacementTrace
	releaseTrace, err := h.lockPlacementTrace(parent, task, false)
	if err != nil {
		h.logger.Warn("placement trace recovery delayed", "request_id", task.Request.RequestID, "attempt", task.Attempt, "error", err)
		return
	}
	payload, previousDelivery, _, err := h.inbox.store.spawnPlacementSnapshot(parent, task.Request.RequestID, task.Attempt)
	if err != nil {
		releaseTrace()
		return
	}
	if len(payload) != 0 && json.Unmarshal(payload, &previous) != nil {
		releaseTrace()
		h.logger.Warn("placement snapshot could not be decoded", "request_id", task.Request.RequestID, "attempt", task.Attempt)
		return
	}
	h.rememberPlacementTraceBase(task.Request.RequestID, task.Attempt, placementDeferredBase{Exists: len(payload) != 0, Payload: payload, Delivery: previousDelivery}, false)
	releaseTrace()
	defer h.forgetPlacementTraceBase(task.Request.RequestID, task.Attempt)
	delivery, _ := json.Marshal(placementDelivery{StateDir: app.StateDir, Home: app.Home, Quota: app.Config.Quota})
	app.PlacementTrace = func(tr fleet.PlacementTrace) {
		tr = continueSpawnPlacementTrace(previous, tr, task.Exclusions, h.now())
		if task.ClaimedHost != "" && len(task.Exclusions) != 0 && tr.Record != nil {
			// The retry's in-process admission error is not the claimed node's
			// result. Its final outcome still belongs to the operation row.
			tr.Record, tr.Outcome, tr.Reason = nil, "", ""
		}
		err := h.notePlacementTrace(task, tr, string(delivery))
		if tr.Chosen != "" {
			transport.snapshotHost = tr.Chosen
			transport.snapshotProfile = tr.Profile
			transport.snapshotReady = false
			// The bind writes the chosen account in its own UPDATE: stash it
			// here so the bind sees the seat this pass counted (hk 5124).
			transport.snapshotPool = tr.Pool
			transport.snapshotAccountKey = tr.AccountKey
			if err == nil {
				payload, _, recorded, readErr := h.inbox.store.spawnPlacementSnapshot(parent, task.Request.RequestID, task.Attempt)
				var saved fleet.PlacementTrace
				transport.snapshotReady = readErr == nil && !recorded && json.Unmarshal(payload, &saved) == nil && saved.Chosen == tr.Chosen && saved.Profile == tr.Profile
			}
		}
	}
	if ctx.Err() != nil {
		h.finishSpawnOutcome(task, hubSpawnOutcome{RC: 124, Stage: "placement", Reason: "timeout", Retryable: true})
		return
	}
	outcome := hubSpawnOutcome{Stage: "placement", Host: spawnPinnedHost(task), StageMS: map[string]int64{}}
	h.bindPlacement(app, transport)
	// The shared scheduler is not this request's: collect the keys this pass
	// asks it to refresh so a timeout can name the machine and pool.
	var refreshMu sync.Mutex
	var refreshWaits []hubSpawnRefreshWait
	rememberRefreshWait := func(machine, pool string) {
		refreshMu.Lock()
		defer refreshMu.Unlock()
		for _, wait := range refreshWaits {
			if wait.Machine == machine && wait.Pool == pool {
				return
			}
		}
		refreshWaits = append(refreshWaits, hubSpawnRefreshWait{Machine: machine, Pool: pool})
	}
	app.ScheduleQuotaRefresh = func(machine, pool string, timeout time.Duration) {
		rememberRefreshWait(machine, pool)
		h.scheduleQuotaRefresh(machine, pool, timeout)
	}
	app.PlacementTry = func(host string) { h.countPlacement(task.Job, host, hubSpawnArg(task.Request.Args, "-l")) }
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
	if pinnedHost := spawnPinnedHost(task); pinnedHost != "" {
		request.Args = append([]string(nil), request.Args...)
		foundHost := false
		for i := 0; i+1 < len(request.Args); i++ {
			if request.Args[i] == "--host" {
				request.Args[i+1] = pinnedHost
				foundHost = true
			}
		}
		if !foundHost {
			request.Args = append(request.Args, "--host", pinnedHost)
		}
		request.RetryHost = pinnedHost
		for _, excluded := range liveExclusions(task.Exclusions, h.now()) {
			if excluded.Machine != pinnedHost {
				continue
			}
			seat, ok := app.Policy.Profiles[excluded.Profile]
			if !ok || seat.Kind == "" {
				h.finishSpawnOutcome(task, hubSpawnOutcome{RC: 78, Stage: "placement", Reason: "configuration", Host: pinnedHost, Retryable: true})
				return
			}
			request.RetryHarnesses = append(request.RetryHarnesses, seat.Kind)
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
	// The same test as the park below, so a parked pass emits no terminal
	// refusal event on every recheck.
	parks := func(err error) bool {
		// A claimed request may park on capacity too: at its limit the
		// timeout ends it with the saved claim refusal (h.completeSpawn).
		if err == nil || ctx.Err() != nil || task.Expires.IsZero() {
			return false
		}
		if spawnClaimedAnswer(task, err) {
			return false
		}
		return spawnParkReason(fleet.ErrorCode(err), err.Error()) != ""
	}
	app.Parks = parks
	wakeBefore := h.spawnWakeSnapshot()
	err = app.ExecuteSubmission(ctx, request)
	if transport.bound {
		return
	} // The node result owns completion and cleanup facts.
	if errors.Is(err, fleet.ErrHubPlacementDeferred) {
		h.releasePlacementCount(task.Job)
		return
	}
	if parent.Err() != nil {
		return
	} // Restart resumes durable accepted ownership.
	if errors.Is(err, fleet.ErrQuotaRefreshPending) {
		refreshMu.Lock()
		waits := append([]hubSpawnRefreshWait(nil), refreshWaits...)
		refreshMu.Unlock()
		task.RefreshWaits = h.noteSpawnRefreshWaits(task, waits)
		if ctx.Err() == nil {
			h.releasePlacementCount(task.Job)
			return
		}
	}
	// If every eligible harness was already refused, the node's last
	// cleanup-confirmed refusal is the answer, not a selection error caused by
	// excluding that same harness. Other placement errors retain their detail.
	claimedAnswer := err != nil && ctx.Err() == nil && spawnClaimedAnswer(task, err)
	// No candidate can take it now: a request with a limit is parked, not
	// ended, and is placed again on an event or its next check (hk 4661).
	if !claimedAnswer && parks(err) {
		if reason := spawnParkReason(fleet.ErrorCode(err), err.Error()); reason != "" {
			// A recognised no-candidate refusal never ends the request: a
			// failed or ambiguous park write leaves it accepted for the next
			// pass to park again.
			if _, parkErr := h.parkSpawnTask(parent, task, reason, spawnRefusalLine(err.Error()), wakeBefore); parkErr != nil {
				h.logger.Warn("hub spawn park not stored; retried next pass", "request_id", task.Request.RequestID, "reason", reason, "error", parkErr)
			}
			h.releasePlacementCount(task.Job)
			return
		}
	}
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
		// The deadline can expire inside the pass that parked the request on
		// a refresh; that timeout names the refresh too, not the queue.
		if errors.Is(err, fleet.ErrQuotaRefreshPending) {
			if wait, failure, waiting := h.spawnRefreshWaitOutcome(task); waiting {
				outcome.QuotaRefresh = failure
				outcome.Fix = spawnRefreshWaitFix(task, wait, failure)
			}
		}
	}
	if outcome.Host == "" {
		outcome.Host = spawnPinnedHost(task)
	}
	if claimedAnswer {
		outcome = task.Outcome
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
	return spawnPinnedHost(task)
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

// noteSpawnRefreshWaits stores the refresh keys this placement pass parked on
// and returns that link. The durable write is the one a timeout reads after a
// restart; a pass with no keys leaves the link the start of the pass cleared.
func (h *HubServer) noteSpawnRefreshWaits(task hubSpawnTask, waits []hubSpawnRefreshWait) []hubSpawnRefreshWait {
	if len(waits) != 0 && h.inbox != nil {
		if err := h.inbox.store.noteSpawnRefreshWaits(h.closeCtx, task.Request.RequestID, task.Attempt, waits); err != nil {
			h.logger.Warn("spawn quota refresh link not recorded", "request_id", task.Request.RequestID, "attempt", task.Attempt, "error", err)
		}
	}
	return waits
}

// clearSpawnRefreshWaits drops the link a previous pass recorded before this
// pass decides anything, so a pass that ends without parking on a refresh (a
// deferred placement, say) cannot leave a stale machine/pool for the timeout.
func (h *HubServer) clearSpawnRefreshWaits(task hubSpawnTask) {
	if task.Node != nil || len(task.RefreshWaits) == 0 || h.inbox == nil {
		return
	}
	if err := h.inbox.store.clearSpawnRefreshWaits(h.closeCtx, task.Request.RequestID, task.Attempt); err != nil {
		h.logger.Warn("spawn quota refresh link not cleared", "request_id", task.Request.RequestID, "attempt", task.Attempt, "error", err)
	}
}

// spawnRefreshWaitOutcome reports whether an unbound request's timeout expired
// while it waited on a shared quota refresh. It names the wait whose last
// attempt failed most recently, or, when nothing failed, the wait whose last
// attempt succeeded most recently, or the first wait when no attempt finished.
func (h *HubServer) spawnRefreshWaitOutcome(task hubSpawnTask) (hubSpawnRefreshWait, *hubSpawnRefreshOutcome, bool) {
	if task.Node != nil || len(task.RefreshWaits) == 0 {
		return hubSpawnRefreshWait{}, nil, false
	}
	wait := task.RefreshWaits[0]
	if h.inbox == nil {
		return wait, nil, true
	}
	now := h.now()
	var failure, success *hubSpawnRefreshOutcome
	var failedAt, succeededAt time.Time
	for _, candidate := range task.RefreshWaits {
		outcome, seen, found, err := h.inbox.store.quotaRefreshAttempt(h.closeCtx, candidate.Machine, candidate.Pool)
		if err != nil {
			h.logger.Warn("spawn quota refresh attempt not read", "machine", candidate.Machine, "pool", candidate.Pool, "error", err)
			continue
		}
		if !found {
			continue
		}
		age := now.Sub(seen) / time.Second
		if age < 0 {
			age = 0
		}
		record := hubSpawnRefreshOutcome{Machine: candidate.Machine, Pool: candidate.Pool}
		if outcome != "" {
			record.Reason, record.FailedAgoS = outcome, int64(age)
			if failure == nil || seen.After(failedAt) {
				failure, failedAt = &record, seen
			}
			continue
		}
		record.SucceededAgoS = int64(age)
		if success == nil || seen.After(succeededAt) {
			success, succeededAt = &record, seen
		}
	}
	if failure != nil {
		return hubSpawnRefreshWait{Machine: failure.Machine, Pool: failure.Pool}, failure, true
	}
	if success != nil {
		return hubSpawnRefreshWait{Machine: success.Machine, Pool: success.Pool}, success, true
	}
	return wait, nil, true
}

// spawnRefreshFailureText is the operator sentence for a recorded refresh
// failure (refreshQuotaFromNode's reason codes).
func spawnRefreshFailureText(reason string) string {
	switch reason {
	case "node_timeout":
		return "the node did not answer within the refresh deadline"
	case "node_unavailable":
		return "the node was not connected"
	case "node_lost":
		return "the node's connection dropped"
	case "refresh_failed":
		return "the node's measurement failed"
	default:
		return "the refresh failed"
	}
}

// spawnRefreshWaitFix explains a timeout that expired while the request waited
// for a shared quota refresh. It names the machine, pool and last finished
// attempt instead of the spawn queue, which the request never occupied, and
// never blames a measurement the hub did not see fail.
func spawnRefreshWaitFix(task hubSpawnTask, wait hubSpawnRefreshWait, refresh *hubSpawnRefreshOutcome) string {
	head := fmt.Sprintf("the hub did not start this request within its %ds deadline: it waited for a quota refresh on %s (pool %s), and no node ran its landing", task.Request.DeadlineSeconds, wait.Machine, wait.Pool)
	switch {
	case refresh == nil:
		return utf8PrefixWithin(fmt.Sprintf("%s; the hub had not finished a refresh attempt for it, so the shared reading stayed stale — confirm that host can measure the pool with panewire quota refresh %s, then resubmit", head, wait.Pool), 512)
	case refresh.Reason != "":
		return utf8PrefixWithin(fmt.Sprintf("%s; the last attempt failed %ds ago — %s (%s). Fix that measurement or restore the connection (panewire quota refresh %s), then resubmit", head, refresh.FailedAgoS, spawnRefreshFailureText(refresh.Reason), refresh.Reason, wait.Pool), 512)
	default:
		return utf8PrefixWithin(fmt.Sprintf("%s; the last attempt finished %ds ago, yet the placement still could not confirm the reading — check the account %s measures for pool %s, then resubmit", head, refresh.SucceededAgoS, wait.Machine, wait.Pool), 512)
	}
}

// spawnQueueWaitFix distinguishes a never-sent queue timeout from a request
// whose committed send may have reached its node.
func spawnQueueWaitFix(task hubSpawnTask, now time.Time) string {
	if task.Node != nil && task.Status != "bound" {
		return fmt.Sprintf("the node %s may have started this request; inspect its saved operation and executing node before retrying", task.Node.Machine)
	}
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
	h               *HubServer
	task            hubSpawnTask
	bound           bool
	started         time.Time
	requireSnapshot bool
	snapshotReady   bool
	snapshotHost    string
	snapshotProfile string
	// snapshotPool and snapshotAccountKey are the seat the placement pass
	// chose: the bind stores them in the placed columns, holding the
	// per-account lock across the re-check and the bind (hk 5124).
	snapshotPool       string
	snapshotAccountKey string
	// snapshotWeeklyUsed is the chosen account's 7d used share, carried from
	// the selection that chose it. Nil when the selection held no reading:
	// like the gate, an unknown reading is not a nearly spent one.
	snapshotWeeklyUsed *float64
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
// the node attests that it did not start the request before its deadline.
func spawnNotStarted(result hubSpawnResult) bool {
	return result.RC == 124 && result.Stage == "claim" && result.Error == "deadline_not_started" && result.Retryable
}

// spawnOutcomeNotStarted recognizes the same attestation after persistence.
func spawnOutcomeNotStarted(outcome hubSpawnOutcome) bool {
	reason, _, _ := strings.Cut(outcome.Reason, ": ")
	return outcome.NodeResult && spawnNotStarted(hubSpawnResult{RC: outcome.RC, Stage: outcome.Stage, Error: reason, Retryable: outcome.Retryable})
}

// spawnLandingPaneUnconfirmed reports a landing refusal whose cleanup could
// not prove the pane the node launched (fleet.FailedSpawnUnconfirmed,
// internal/fleet/spawn_cleanup.go). The pane is named, so the hub can ask the
// node whether a pane with the job's name and cwd is still there.
func spawnLandingPaneUnconfirmed(result hubSpawnResult) bool {
	return result.RC == 70 && result.Stage == "landing" && result.Pane != "" && strings.Contains(result.StdoutTail, fleet.FailedSpawnUnconfirmed)
}

// spawnNotStartedFix tells the caller what a not-started refusal means.
const spawnNotStartedFix = "the node never started this request before its deadline. Replay the same command; the new attempt can choose another host only if no earlier attempt claimed the job."

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
	// A successor spec whose branch another worktree of this host's clone
	// holds refuses here, and another host prepares the same branch (hk 5118).
	"checkout_refused:branch_in_use": true,
	"spawn_failed:hooks_review":      true,
	"spawn_failed:trust_prompt":      true,
	"spawn_failed:omp_setup_wizard":  true,
	"spawn_failed:auth_unavailable":  true,
	"launch_proof_failed":            true,
}

// spawnRefusalTriesNextHost names a node refusal that moves an auto placement
// to the next candidate instead of ending the request.
func spawnRefusalTriesNextHost(reason string) bool {
	code, _, _ := strings.Cut(reason, ": ")
	return nextHostRefusals[code]
}

// spawnStageBeforeJobClaim reports a node result stage that ends before the
// node's hk job claim: the refusal came from placement, the checkout step or an
// early check (an empty stage), so no hk record names the refusing node. From
// the claim on, hk holds the job id for that node's host and answers another
// host's claim with 409 job_id_taken (rc 74, hk 4614), so a later candidate can
// never take the job. An unknown stage counts as claimed. The node's own
// not-started answer is a different case: spawnNotStarted.
func spawnStageBeforeJobClaim(stage string) bool {
	switch stage {
	case "", "placement", "checkout":
		return true
	}
	return false
}

// spawnReplayPinsHost reports whether a replay of a saved retryable refusal
// runs on the host that refused it. A refusal that left an hk claim there pins
// the replay, because another host's claim of the job id is a 409.
func spawnReplayPinsHost(outcome hubSpawnOutcome) bool {
	if !outcome.Retryable || outcome.Host == "" {
		return false
	}
	// A completed node attestation of not-started is proof this attempt made
	// no claim, even though its stage is claim. An earlier claim stays pinned
	// independently in hubSpawnTask.ClaimedHost.
	if spawnOutcomeNotStarted(outcome) {
		return false
	}
	// A timeout the hub wrote for an unsent attempt, or a node timeout before
	// the claim stage, made no claim: it pins nothing (hk 4661).
	if code, _, _ := strings.Cut(outcome.Reason, ": "); code == "timeout" && (!outcome.NodeResult || spawnStageBeforeJobClaim(outcome.Stage)) {
		return false
	}
	return !spawnRefusalMovesOn(outcome.Reason, outcome.Stage)
}

// spawnPinnedHost also reads outcomes written before claimed_host existed.
// resumeCleanedSpawn carries that legacy pin into the durable column.
func spawnPinnedHost(task hubSpawnTask) string {
	if task.ClaimedHost != "" {
		return task.ClaimedHost
	}
	if spawnReplayPinsHost(task.Outcome) {
		return task.Outcome.Host
	}
	return ""
}

// spawnRefusalMovesOn is spawnRefusalTriesNextHost for a refusal that has a
// stage: only a refusal that left no hk claim behind hands the job to another
// host. Past the claim the request stays with the refusing host, where a
// verified-cleanup retry replays the identical claim.
func spawnRefusalMovesOn(reason, stage string) bool {
	return spawnRefusalTriesNextHost(reason) && spawnStageBeforeJobClaim(stage)
}

func (t *directSpawnTransport) Spawn(ctx context.Context, req fleet.HubSpawnRequest) (fleet.HubSpawnReply, error) {
	if t.bound {
		// An engine busy retry must not bind a second request or replay the old
		// journal id after this transport has published a binding.
		return fleet.HubSpawnReply{}, fleet.ErrHubPlacementBound
	}
	// Reapply saved refusals through the existing candidate loop. In particular,
	// a launch-proof refusal first tries another harness on this same host;
	// other refusals rule out the host for the rest of this request (hk 4661: across its attempts).
	for _, excluded := range liveExclusions(t.task.Exclusions, t.h.now()) {
		profileOnly := excluded.Reason == "launch_proof_failed" || t.task.ClaimedHost == excluded.Machine
		if excluded.Machine == req.Machine && (!profileOnly || excluded.Profile == hubSpawnArg(req.Args, "-m")) {
			t.h.releasePlacementCount(t.task.Job)
			return fleet.HubSpawnReply{RC: excluded.RC, Error: excluded.Reason, NextHost: true}, nil
		}
	}
	if t.requireSnapshot && (!t.snapshotReady || t.snapshotHost != req.Machine || t.snapshotProfile != hubSpawnArg(req.Args, "-m")) {
		return fleet.HubSpawnReply{}, fleet.ErrHubPlacementDeferred
	}
	t.h.mu.Lock()
	record := t.h.nodes[req.Machine]
	oldNode := record != nil && record.agent != nil && record.state == "connected" && !record.capabilities["spawn_ops_v1"]
	// A draining hub binds no new grant-required attempt; placement goes on
	// and sends without a grant, as to a node without the capability.
	grantRequired := !t.h.spawnGrantDrain && record != nil && record.capabilities[hubCapabilitySpawnGrant]
	t.h.mu.Unlock()
	if oldNode {
		return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_capability_required"}
	}
	// The planned consult's stamp is an extra: a node that predates it refuses
	// the unknown flag and drops the whole spawn, so it goes without (hk 4875).
	if record == nil || !record.capabilities[hubCapabilityConsultStamp] {
		req.Args = withoutSpawnFlag(req.Args, fleet.ConsultStampArg)
	}
	// --owner-notify is the same kind of extra (hk 5360): the saved request
	// keeps it, and the hub applies the mode there.
	if record == nil || !record.capabilities[hubCapabilityOwnerNotify] {
		req.Args = withoutSpawnFlag(req.Args, fleet.OwnerNotifyArg)
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
	node.GrantRequired = grantRequired
	node.WaitSeconds = max(1, min(3600, int(time.Until(t.task.Deadline).Seconds())+1))
	// The request carries the selection's 7d reading for the chosen account.
	t.snapshotWeeklyUsed = req.AccountWeeklyUsed
	// The bind can commit even when its response is lost. From this point a
	// placement caller must not release ownership or fall through to a host.
	// A commandcode seat binds under its account's lock: the busy re-check and
	// the bind are one critical section, so two concurrent admissions on one
	// nearly spent account admit exactly one (hk 5124). Every pool's bind
	// records its account in the placed columns; only commandcode re-checks
	// under the lock.
	accountKey := t.snapshotAccountKey
	if t.snapshotPool == commandcodePoolName && accountKey != "" {
		release, lockErr := t.h.commandcodeSeatLocks.lock(ctx, accountKey)
		if lockErr != nil {
			return fleet.HubSpawnReply{}, &fleet.HubError{Kind: accountBusyUnknown}
		}
		defer release()
		if busyErr := t.refuseBusyCommandcodeSeat(ctx); busyErr != nil {
			return fleet.HubSpawnReply{}, busyErr
		}
	}
	t.bound = true
	task, err := t.h.inbox.store.bindSpawnPlaced(ctx, t.task.Request.RequestID, t.task.Attempt, node, accountKey, t.h.now().UTC())
	if err != nil {
		return fleet.HubSpawnReply{}, fleet.ErrHubPlacementBound
	}
	t.task = task
	t.h.kickSpawnPlacement()
	return fleet.HubSpawnReply{}, fleet.ErrHubPlacementBound
}

// nextHostAfterRefusal hands an auto placement back to its candidate loop. A
// pinned host is the operator's choice, so its refusal stays the answer.
func (t *directSpawnTransport) nextHostAfterRefusal(ctx context.Context, node hubSpawnRequest, result hubSpawnResult) (bool, error) {
	// A refusal that comes after the node's hk job claim (a dialog or login
	// screen after the pane launched) stays with that host: hk answers another
	// host's claim of the job id with 409 job_id_taken, rc 74 (hk 4614). Only a
	// refusal before the claim moves on, and only after verified cleanup.
	// A request the node never started also moves on, while the request's own
	// deadline still has room for another host.
	// A cleanup-confirmed replay can already carry a host-bound claim from its
	// prior attempt, even when this attempt refuses before making a new claim.
	movesOn := spawnRefusalMovesOn(spawnResultReason(result), result.Stage) || spawnNotStarted(result)
	if result.RC == 0 || !result.Retryable || !movesOn || !t.h.now().Before(t.task.Deadline) || spawnPinnedHost(t.task) != "" || firstNonEmptyString(hubSpawnArg(t.task.Request.Args, "--host"), "auto") != "auto" {
		return false, nil
	}
	if result.Error == "launch_proof_failed" && result.RC != 70 {
		return false, nil
	}
	reason := spawnResultReason(result)
	exclusion := hubSpawnExclusion{Machine: node.Machine, Profile: hubSpawnArg(node.Args, "-m"), Reason: reason, RC: result.RC}
	if spawnRefusalTriesNextHost(reason) {
		// A host-local refusal rules the host out for the launch backoff only:
		// the state that caused it can be cleaned up in minutes, and a host
		// excluded for the request's life would never be asked again (hk 5214).
		exclusion.ExpiresMS = t.h.now().Add(t.h.hostRefusalBackoff()).UnixMilli()
	}
	task, err := t.h.inbox.store.unbindRefusedSpawn(ctx, t.task, exclusion, nil, t.h.now().UTC())
	if err != nil {
		return true, err
	}
	t.task, t.bound = task, false
	// The refused host no longer holds this job; the next candidate counts it.
	t.h.releasePlacementCount(task.Job)
	t.h.observeLaunchOutcome(node.Machine, hubSpawnArg(node.Args, "-m"), result.RC, reason, t.h.now().UTC())
	t.h.logger.Warn("hub spawn refused by node; trying next candidate", "request_id", task.Request.RequestID, "node_request_id", node.RequestID, "job", task.Job, "host", node.Machine, "profile", hubSpawnArg(node.Args, "-m"), "rc", result.RC, "reason", reason)
	t.h.kickSpawnPlacement()
	return true, nil
}

// retryHarnessAfterClaim keeps the host-bound hk claim while retrying an
// automatically selected harness whose launch was cleaned up. The host pin,
// failed profile and unbinding are one durable transition.
func (t *directSpawnTransport) retryHarnessAfterClaim(ctx context.Context, node hubSpawnRequest, result hubSpawnResult, outcome hubSpawnOutcome) (bool, error) {
	reason := spawnResultReason(result)
	switch reason {
	case "launch_proof_failed", "spawn_failed:hooks_review", "spawn_failed:trust_prompt", "spawn_failed:omp_setup_wizard", "spawn_failed:auth_unavailable":
	default:
		return false, nil
	}
	postLaunch := result.Stage == "launch" || result.Stage == "landing"
	previousClaim := spawnPinnedHost(t.task) == node.Machine && spawnStageBeforeJobClaim(result.Stage)
	if result.RC == 0 || !result.Retryable || !postLaunch && !previousClaim || !t.h.now().Before(t.task.Deadline) || reason == "launch_proof_failed" && result.RC != 70 {
		return false, nil
	}
	spawn, err := fleet.ValidateSubmission(t.task.Request)
	profile := hubSpawnArg(node.Args, "-m")
	if err != nil || profile == "" || spawn.Delegated || spawn.Profile != "" && spawn.Profile != "auto" {
		return false, nil
	}
	exclusion := hubSpawnExclusion{Machine: node.Machine, Profile: profile, Reason: reason, RC: result.RC}
	task, err := t.h.inbox.store.unbindRefusedSpawn(ctx, t.task, exclusion, &outcome, t.h.now().UTC())
	if err != nil {
		return true, err
	}
	t.task, t.bound = task, false
	t.h.releasePlacementCount(task.Job)
	t.h.observeLaunchOutcome(node.Machine, profile, result.RC, reason, t.h.now().UTC())
	t.h.logger.Warn("hub spawn harness refused; trying another on claimed host", "request_id", task.Request.RequestID, "node_request_id", node.RequestID, "job", task.Job, "host", node.Machine, "profile", profile, "rc", result.RC, "reason", reason)
	t.h.kickSpawnPlacement()
	return true, nil
}
func (t *directSpawnTransport) dispatch(ctx context.Context, node hubSpawnRequest) (fleet.HubSpawnReply, error) {
	h := t.h
	id := t.task.Request.RequestID
	if !h.spawnActions.begin(id) {
		return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_pending"}
	}
	defer h.spawnActions.end(id)
	current, err := h.inbox.store.spawnTask(ctx, id)
	if err != nil || current.Attempt != t.task.Attempt || current.Node == nil || current.Node.RequestID != node.RequestID || current.Node.Machine != node.Machine || !spawnPending(current.Status) {
		return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_pending"}
	}
	t.task, node = current, *current.Node
	// A sent attempt whose fence committed before a hub stop was never
	// granted, so never started: settle it now instead of sending it again.
	// One whose latest start_by plus the margin has passed is fenced here,
	// before any resend, so a replay with a short context (a recovery
	// reprobe) still fences an ungranted attempt.
	if node.GrantRequired && current.Status != "bound" {
		fenced, err := h.inbox.store.spawnAttemptFenced(ctx, node.RequestID)
		if err != nil {
			return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_pending"}
		}
		if !fenced {
			startBy, err := h.inbox.store.spawnAttemptStartBy(ctx, node.RequestID)
			if err != nil {
				return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_pending"}
			}
			if startBy != 0 && time.Now().After(time.UnixMilli(startBy).Add(h.spawnStartByMargin)) {
				if fenced, err = h.inbox.store.fenceSpawnAttempt(h.closeCtx, id, node.RequestID, h.now().UTC()); err != nil {
					return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_pending"}
				}
				if fenced {
					h.logger.Warn("hub spawn attempt fenced before its start grant", "request_id", id, "node_request_id", node.RequestID, "host", node.Machine)
				}
			}
		}
		if fenced {
			return t.settleNodeResult(ctx, node, spawnFencedResult(node, h.now().UTC()), false)
		}
	}
	if current.Status == "bound" && !h.now().Before(current.Deadline) {
		h.finishSpawnOutcome(current, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Host: node.Machine, Retryable: true, Fix: spawnQueueWaitFix(current, h.now())})
		return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_pending"}
	}
	releaseAdmission := func() {}
	if current.Status == "bound" {
		releaseAdmission, err = h.spawnAdmissions.lock(ctx, node.Machine)
		if err != nil {
			return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_pending"}
		}
		if ctx.Err() != nil {
			releaseAdmission()
			return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_pending"}
		}
		if !h.now().Before(current.Deadline) {
			releaseAdmission()
			h.finishSpawnOutcome(current, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Host: node.Machine, Retryable: true, Fix: spawnQueueWaitFix(current, h.now())})
			return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_pending"}
		}
	}
	h.mu.Lock()
	record := h.nodes[node.Machine]
	// A grant-required attempt goes only to a node that asks for its grant.
	connected := record != nil && record.agent != nil && record.state == "connected" && record.capabilities["spawn_ops_v1"] && (!node.GrantRequired || record.capabilities[hubCapabilitySpawnGrant])
	if current.Status == "bound" && (!connected || !h.acceptingEffectiveLocked(node.Machine, record.accepting)) {
		h.mu.Unlock()
		releaseAdmission()
		if _, err := h.inbox.store.unbindUnsentSpawn(ctx, id, current.Attempt, node.RequestID); err == nil {
			h.releasePlacementCount(current.Job)
			select {
			case h.spawnKick <- struct{}{}:
			default:
			}
		}
		return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_pending"}
	}
	if !connected {
		h.mu.Unlock()
		releaseAdmission()
		return t.unknown(ctx, "disconnected")
	}
	agent := record.agent
	h.mu.Unlock()
	// Fresh sends serialize with that node's accepting override without
	// holding h.mu across the durable CAS. Replays remain allowed when the
	// node no longer accepts new work.
	// The start_by this send carries is recorded before the frame can leave;
	// without the record nothing is sent.
	var startByMS int64
	if node.GrantRequired {
		startByMS = time.Now().Add(h.spawnStartBy).UnixMilli()
		if err := h.inbox.store.recordSpawnStartBy(ctx, node.RequestID, startByMS); err != nil {
			releaseAdmission()
			return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_pending"}
		}
	}
	unlock, err := h.poolActionLocks.lock(ctx, current.Job)
	if err != nil {
		releaseAdmission()
		return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_pending"}
	}
	err = h.saveOwnerAssignment(hubOwnerAssignment{Job: current.Job, Node: node.Machine, Epoch: 1}, current.Status == "bound")
	unlock()
	if err != nil {
		releaseAdmission()
		return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_pending"}
	}
	started, err := h.inbox.store.beginSpawnSend(ctx, current)
	if err != nil || !started {
		releaseAdmission()
		return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_pending"}
	}
	if current.Status == "bound" {
		t.task.Status = "dispatching"
	}
	h.mu.Lock()
	record = h.nodes[node.Machine]
	if record == nil || record.agent != agent || record.state != "connected" || !record.capabilities["spawn_ops_v1"] || node.GrantRequired && !record.capabilities[hubCapabilitySpawnGrant] || current.Status == "bound" && !h.acceptingEffectiveLocked(node.Machine, record.accepting) {
		h.mu.Unlock()
		releaseAdmission()
		return t.unknown(ctx, "disconnected")
	}
	// The request's absolute expiry goes only to a node that reads it (hk
	// 4747): an older node's strict frame decoder refuses the unknown field.
	expiresMS := spawnExpiryField(record.capabilities, current.Expires)
	// The binding was filtered for the node's capability when it was made, but a
	// node may reconnect with a build that no longer accepts the stamp before the
	// first send. Its argument rule would drop an unknown flag whole, so the send
	// applies the same filter again (hk 4875).
	if !record.capabilities[hubCapabilityConsultStamp] {
		node.Args = withoutSpawnFlag(node.Args, fleet.ConsultStampArg)
	}
	if !record.capabilities[hubCapabilityOwnerNotify] {
		node.Args = withoutSpawnFlag(node.Args, fleet.OwnerNotifyArg)
	}
	// Likewise the sparse lists: only a node that advertises
	// checkout_sparse_v1 reads exclude/include, and every other node gets the
	// full checkout (hk 5098).
	unfiltered := node.Checkout
	node.Checkout = sparseCheckoutForNode(node.Checkout, record.capabilities)
	entry := &hubSpawnRecord{request: node, status: "pending", created: h.now(), waiter: make(chan hubSpawnResult, 1), checkoutStripped: node.Checkout != unfiltered}
	h.spawnRecords[node.RequestID] = entry
	h.mu.Unlock()
	releaseAdmission()
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
		ExpiresMS          int64             `json:"expires_ms,omitempty"`
		StartByMS          int64             `json:"start_by_ms,omitempty"`
	}{true, "job.spawn", node.RequestID, node.CWDKey, node.Brief, node.Checkout, node.Args, node.WaitSeconds, node.DeadlineMS, expiresMS, 0}
	// Every send of a grant-required attempt, a replay after a hub restart
	// included, gives the node a fresh start_by and arms the fence after it.
	// start_by only bounds the node's wait; the grant/fence row is the proof.
	if node.GrantRequired {
		message.StartByMS = startByMS
	}
	writeCtx, cancelWrite := context.WithCancel(ctx)
	stopConnection := func() bool { return false }
	if agent.lifetime != nil {
		stopConnection = context.AfterFunc(agent.lifetime, cancelWrite)
	}
	writeErr := agent.writeJSONContext(writeCtx, message)
	stopConnection()
	cancelWrite()
	if writeErr != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_pending"}
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return t.afterSpawnDeadline(agent, entry, message)
		}
		return t.unknown(ctx, "disconnected")
	}
	// The fence is armed once the frame is written, so time spent waiting
	// for the writer never shortens the margin after start_by.
	var fence <-chan time.Time
	var fenceTimer *time.Timer
	fenceRetry := time.Second
	if node.GrantRequired {
		fenceTimer = time.NewTimer(time.Until(time.UnixMilli(message.StartByMS)) + h.spawnStartByMargin)
		defer fenceTimer.Stop()
		fence = fenceTimer.C
	}
	for {
		select {
		case result := <-entry.waiter:
			return t.settleNodeResult(ctx, node, result, false)
		case <-fence:
			fenced, err := h.inbox.store.fenceSpawnAttempt(h.closeCtx, id, node.RequestID, h.now().UTC())
			if err != nil {
				// Undecided (a commit error may even have landed): try again
				// with bounded backoff; the INSERT is idempotent.
				h.logger.Warn("hub spawn fence not decided", "request_id", id, "node_request_id", node.RequestID, "err", err)
				fenceTimer.Reset(fenceRetry)
				fenceRetry = min(2*fenceRetry, 30*time.Second)
				continue
			}
			if !fenced {
				// A lost fence means the node holds a grant: keep waiting, and
				// past the landing bound keep ownership as before.
				fence = nil
				continue
			}
			h.mu.Lock()
			if entry.status == "pending" {
				entry.status = "fenced" // a late node answer for this id is ignored
			}
			h.mu.Unlock()
			h.logger.Warn("hub spawn attempt fenced before its start grant", "request_id", id, "node_request_id", node.RequestID, "host", node.Machine)
			return t.settleNodeResult(ctx, node, spawnFencedResult(node, h.now().UTC()), false)
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.Canceled) {
				return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_pending"}
			}
			return t.afterSpawnDeadline(agent, entry, message)
		}
	}
}

func (t *directSpawnTransport) afterSpawnDeadline(agent *hubAgent, entry *hubSpawnRecord, message any) (fleet.HubSpawnReply, error) {
	if t.probeWait > 0 {
		if result, answered := t.probeNode(agent, entry, message); answered {
			probeCtx, cancel := context.WithTimeout(t.h.closeCtx, t.probeWait)
			defer cancel()
			return t.settleNodeResult(probeCtx, entry.request, result, true)
		}
	}
	return t.unknown(t.h.closeCtx, "timeout")
}

// probeNode asks the node once more after the landing wait ran out, by
// replaying the same request id, and waits at most probeWait for a definite
// answer. Silence or an uncertain answer (still executing, restarted, journal
// trouble) returns false: the caller keeps ownership as a timeout, never
// guessing from silence. An answer to the original frame counts too.
func (t *directSpawnTransport) probeNode(agent *hubAgent, entry *hubSpawnRecord, message any) (hubSpawnResult, bool) {
	ctx, cancel := context.WithTimeout(t.h.closeCtx, t.probeWait)
	defer cancel()
	if agent.writeJSONContext(ctx, message) != nil {
		return hubSpawnResult{}, false
	}
	select {
	case result := <-entry.waiter:
		uncertain := result.CompletedAt == nil || result.Error == "reconciling" || result.Error == "unknown" || result.Error == "journal_unavailable" || result.Error == "journal_conflict"
		return result, !uncertain
	case <-ctx.Done():
		return hubSpawnResult{}, false
	}
}

// settleNodeResult applies one answer from the bound node.
func (t *directSpawnTransport) settleNodeResult(ctx context.Context, node hubSpawnRequest, result hubSpawnResult, afterDeadline bool) (fleet.HubSpawnReply, error) {
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
	// A landing refused only because the node could not confirm its pane may
	// still have the job's pane running the brief. The same exact pane read
	// that gates late success re-checks it: a pane found there settles the
	// request as landed, and nothing is launched again.
	rechecked := spawnLandingPaneUnconfirmed(result) && h.verifySpawnPane(ctx, node.Machine, result, node.Args)
	if rechecked {
		result.RC, result.Error, result.Retryable = 0, "", false
	}
	// The authenticated node's completed journal is necessary, and a current
	// exact pane read is additionally required before accepting late success.
	if result.RC == 0 && (node.Checkout != nil && (result.Receipt == nil || !spawnReceiptCheckoutPath(result.Receipt.CWD, node.Checkout.Dir, hubSpawnFlagArg(node.Args, "-L"))) || !rechecked && !h.verifySpawnPane(ctx, node.Machine, result, node.Args)) {
		return t.unknown(ctx, "unknown")
	}
	h.noteOneTimeActionRefusal(node.Machine, hubSpawnArg(node.Args, "-m"), result)
	outcome := spawnNodeOutcome(t.task, node, result)
	if !afterDeadline || !spawnNotStarted(result) {
		if handled, err := t.retryHarnessAfterClaim(ctx, node, result, outcome); handled {
			if err != nil {
				return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_pending"}
			}
			return fleet.HubSpawnReply{RequestID: result.RequestID, RC: result.RC, JobID: result.JobID, StdoutTail: result.StdoutTail, Error: spawnResultReason(result), CheckoutMode: result.CheckoutMode}, nil
		}
		if handled, err := t.nextHostAfterRefusal(ctx, node, result); handled {
			if err != nil {
				return fleet.HubSpawnReply{}, &fleet.HubError{Kind: "hub_spawn_pending"}
			}
			return fleet.HubSpawnReply{RequestID: result.RequestID, RC: result.RC, JobID: result.JobID, StdoutTail: result.StdoutTail, Error: spawnResultReason(result), NextHost: true, CheckoutMode: result.CheckoutMode}, nil
		}
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
	if result.RC == 0 {
		h.seedLandedOwner(t.task.Job, node.Machine)
	}
	return fleet.HubSpawnReply{RequestID: result.RequestID, RC: result.RC, JobID: result.JobID, Pane: result.Pane, StdoutTail: result.StdoutTail, Error: result.Error, CheckoutMode: result.CheckoutMode}, nil
}

func spawnNodeOutcome(task hubSpawnTask, node hubSpawnRequest, result hubSpawnResult) hubSpawnOutcome {
	stage := result.Stage
	if stage == "" {
		stage = "placement"
	}
	outcome := hubSpawnOutcome{RC: result.RC, Stage: stage, Reason: result.Error, Host: node.Machine, Pane: result.Pane, Retryable: result.Retryable, NodeResult: true, StageMS: result.StageMS, Role: hubSpawnArg(node.Args, "--role"), Profile: hubSpawnArg(node.Args, "-m"), E6Arm: task.Request.E6Arm, CheckoutMode: result.CheckoutMode}
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
	return outcome
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
		if result.AfterUnresolved {
			text += " request=" + request + " (after an earlier unresolved notice)"
		}
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
		h.placementAsync.Store(true)
		h.spawnWorkers.Add(3)
		go func() { defer h.spawnWorkers.Done(); h.runSpawnCoordinator(ctx) }()
		go func() { defer h.spawnWorkers.Done(); h.runQuotaReconcile(ctx) }()
		go func() {
			defer h.spawnWorkers.Done()
			defer h.placementAsync.Store(false)
			h.runPlacementRecords(ctx)
		}()
	})
}

func (h *HubServer) finishSpawnOutcome(task hubSpawnTask, outcome hubSpawnOutcome) {
	outcome.E6Arm = task.Request.E6Arm
	if outcome.Host == "" {
		outcome.Host = spawnPinnedHost(task)
	}
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

// spawnClaimedAnswer reports a role_no_candidate on a claimed host whose
// harnesses were all refused: the node's last refusal is the answer. A pool
// at its concurrency cap is capacity, not refusal, and parks instead.
func spawnClaimedAnswer(task hubSpawnTask, err error) bool {
	code, _, _ := strings.Cut(err.Error(), ":")
	return task.ClaimedHost != "" && len(task.Exclusions) > 0 && task.Outcome.NodeResult && code == "role_no_candidate" && !strings.Contains(err.Error(), "concurrency cap")
}

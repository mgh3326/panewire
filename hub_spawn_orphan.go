package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// A request bound to a node stays reconciling or deadline_exceeded until its
// node answers. When the node restarted mid-spawn, its journal keeps the entry
// executing for good and every replay answered reconciling, so the request and
// its (task, role) reservation were held forever (hk 3423). The node now
// answers unknown for an entry no live execution owns; past the deadline, with
// no pane bearing the job label on that node, the hub finishes the request as
// failed and releases the reservation. docs/hub-spawn-operations.md, "Stranded
// requests".

const (
	spawnOrphanedReason  = "orphaned_after_restart"
	spawnAbandonedReason = "operator_abandoned"
	// spawnOrphanProbeEvery bounds the pane probe an unknown answer triggers;
	// the late-result replay itself keeps its own cadence.
	spawnOrphanProbeEvery = time.Minute
	// spawnStuckAfter is how long past its deadline a pending request runs
	// before the hub warns once that its ownership is stuck.
	spawnStuckAfter = 30 * time.Minute
	// spawnStaleProbeMin and spawnStaleProbeMax bound the back-off between
	// replays of a deadline_exceeded row: a quarter of its age past the
	// deadline, so a row that stays silent costs fewer probes the longer it
	// waits.
	spawnStaleProbeMin = 30 * time.Second
	spawnStaleProbeMax = time.Hour
	// spawnStaleBatch caps the stale rows one coordinator pass hands to the
	// workers, so they never take every worker from fresh requests.
	spawnStaleBatch = 2
	// spawnAbandonReasonLimit bounds the operator's --reason text.
	spawnAbandonReasonLimit = 256
)

// spawnPending reports whether status still holds ownership unresolved.
func spawnPending(status string) bool {
	switch status {
	case "accepted", "bound", "dispatching", "reconciling", "deadline_exceeded":
		return true
	}
	return false
}

// spawnStaleProbeDue reports whether a deadline_exceeded row is due for its
// next replay at now. The back-off lives in the row's last_run_ms, so it
// survives a hub restart.
func spawnStaleProbeDue(task hubSpawnTask, now time.Time) bool {
	return !now.Before(spawnStaleProbeAt(task, now))
}

// spawnStaleProbeAt is when a deadline_exceeded row's next replay falls due,
// by the back-off its age at now implies. The interval grows with the age, so
// the coordinator's gate asks again when this time arrives (hk 5302).
func spawnStaleProbeAt(task hubSpawnTask, now time.Time) time.Time {
	every := min(max(now.Sub(task.Deadline)/4, spawnStaleProbeMin), spawnStaleProbeMax)
	return task.LastRun.Add(every)
}

// reprobeStaleSpawn runs one back-off probe of a bound request that is past
// its deadline. It replays the request so a completed node journal settles it
// as usual (settleNodeResult). A node that stays silent, restarted without
// answering unknown, or still reports reconciling never reaches the orphan
// rule through that replay, so a request stuck spawnStuckAfter past its
// deadline also gets the pane check directly: no pane bearing the job label on
// its node finishes it as orphaned and frees its reservation. A past-deadline
// node never launches a request, so asking again cannot start a second one,
// and any pane or unreadable node keeps ownership.
func (h *HubServer) reprobeStaleSpawn(parent context.Context, task hubSpawnTask) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	transport := directSpawnTransport{h: h, task: task}
	_, _ = transport.dispatch(ctx, *task.Node)
	// A completed job is the landing proof even when no receipt reached the
	// hub (hk 5267). It settles at any probe, before the horizon.
	if h.settleCompletedSpawn(parent, task) {
		return
	}
	h.noteStuckSpawn(task)
	if h.now().Sub(task.Deadline) < spawnStuckAfter {
		return
	}
	if !h.spawnActions.begin(task.Request.RequestID) {
		return
	}
	defer h.spawnActions.end(task.Request.RequestID)
	current, err := h.inbox.store.spawnTask(parent, task.Request.RequestID)
	if err != nil || current.Status != "deadline_exceeded" || current.Attempt != task.Attempt {
		return
	}
	outcome := hubSpawnOutcome{RC: 75, Stage: "landing", Reason: spawnOrphanedReason, Host: spawnTaskHost(task), NodeResult: true}
	if released, why := h.finishUnownedSpawn(parent, current, outcome); !released {
		h.logger.Info("hub kept a stale spawn", "request_id", task.Request.RequestID, "job", task.Job, "host", spawnTaskHost(task), "reason", why)
		// No final answer at the horizon: the owner hears it once, as unresolved
		// (hk 5267). A landing that comes later is corrected, not repeated.
		queued, err := h.inbox.store.noteUnresolvedSpawn(parent, current, h.now())
		if err != nil {
			h.logger.Warn("hub could not queue the unresolved spawn notice", "request_id", task.Request.RequestID, "job", task.Job, "error", err)
		} else if queued {
			h.recordUnresolvedSpawnError(current)
		}
	}
}

// resolveOrphanedSpawn runs when task's node answered unknown after the
// deadline. It reports whether the request is now finished.
func (h *HubServer) resolveOrphanedSpawn(task hubSpawnTask) bool {
	h.mu.Lock()
	if h.spawnOrphanProbed == nil {
		h.spawnOrphanProbed = map[string]time.Time{}
	}
	if last, ok := h.spawnOrphanProbed[task.Request.RequestID]; ok && time.Since(last) < spawnOrphanProbeEvery {
		h.mu.Unlock()
		return false
	}
	h.spawnOrphanProbed[task.Request.RequestID] = time.Now()
	h.mu.Unlock()
	outcome := hubSpawnOutcome{RC: 75, Stage: "landing", Reason: spawnOrphanedReason, Host: spawnTaskHost(task), NodeResult: true}
	released, why := h.finishUnownedSpawn(h.closeCtx, task, outcome)
	if !released {
		h.logger.Info("hub kept a stranded spawn", "request_id", task.Request.RequestID, "job", task.Job, "host", spawnTaskHost(task), "reason", why)
	}
	return released
}

// finishUnownedSpawn finishes a pending request as failed with outcome. A
// possibly sent request must first have no pane bearing its job label; an
// unsent binding needs no pane probe. It marks a live hk job lost, completes
// the request and releases its (task, role) reservation. Anything it cannot
// prove keeps the request pending; why names the check that held it. A crash
// after the lost write converges on the next claim.
func (h *HubServer) finishUnownedSpawn(ctx context.Context, task hubSpawnTask, outcome hubSpawnOutcome) (released bool, why string) {
	if task.Node == nil {
		return false, "not_bound"
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil || app.CheckTrust() != nil {
		return false, "spawn_config_unavailable"
	}
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	record, exists, err := app.ExistingJob(readCtx, task.Job)
	cancel()
	if err != nil {
		return false, "hk_read_error"
	}
	label := heldAgentName(task, record)
	if task.Status != "bound" {
		if label == "" {
			return false, "no_label"
		}
		probe := h.probeLabelledPane(ctx, task.Node.Machine, label)
		switch probe.state {
		case heldPanePresent:
			return false, "pane_present:" + probe.row.PaneID
		case heldPaneUnknown:
			return false, probe.reason
		}
	}
	code, _, _ := strings.Cut(outcome.Reason, ": ")
	if state, _ := record["state"].(string); exists && !hkTerminalState(state) {
		if label == "" {
			return false, "no_label"
		}
		if lost, reason := h.markHolderJobLostBecause(ctx, app, task, label, code); !lost {
			return false, "mark_lost:" + reason
		}
	}
	changed, err := h.completeSpawn(ctx, task, outcome)
	if err != nil {
		return false, "store_unavailable"
	}
	if !changed {
		return false, "not_pending"
	}
	h.recordSpawnTrace(task, outcome)
	h.mu.Lock()
	delete(h.spawnOrphanProbed, task.Request.RequestID)
	h.mu.Unlock()
	if task.Status != "bound" {
		if err = h.releaseSpawnRole(ctx, task); err != nil {
			return false, "release_write_failed"
		}
	}
	return true, ""
}

// releaseSpawnRole frees the (task, role) slot a finished request holds. A
// child, task-less or consult request holds none. A stored request this build
// cannot parse (one written under a role name the rename retired) still holds
// at most the one slot its request id reserved, so it is freed by that id alone
// and never stays stuck behind a role it no longer names.
func (h *HubServer) releaseSpawnRole(ctx context.Context, task hubSpawnTask) error {
	spawn, err := fleet.ValidateSubmission(task.Request)
	if err != nil {
		return h.inbox.store.releaseCompletedSpawnRoleOf(ctx, task.Request.RequestID)
	}
	if !spawn.HoldsTaskRole() {
		return nil
	}
	return h.inbox.store.releaseCompletedSpawnRole(ctx, task.Request.RequestID, spawn.RoleKey())
}

// probeLabelledPane asks machine's node for every pane herdr lists and looks
// for one bearing label, the way probeHeldPane does for a recorded pane. A
// stranded request recorded no pane, so the label is the only identity.
func (h *HubServer) probeLabelledPane(ctx context.Context, machine, label string) heldProbe {
	if machine == "" {
		return heldProbe{reason: "no_host"}
	}
	return h.probeNodePanes(ctx, machine, "", label)
}

// handleSpawnAbandon is POST /v1/spawns/{request_id}/abandon: the operator
// finishes a pending request as failed and frees its reservation, once its
// node lists no pane bearing the job label. An unbound request needs no probe,
// and its owner lane may withdraw it too (endUnboundSpawn, hk 5214).
func (h *HubServer) handleSpawnAbandon(w http.ResponseWriter, r *http.Request) {
	// A node may call this only to withdraw an unbound request its own lane
	// owns (endUnboundSpawn). Every other abandon is the operator's.
	caller := h.authenticate(r)
	if caller.kind != hubCallerOperator && caller.kind != hubCallerNode {
		hubUnauthorized(w)
		return
	}
	if h.inbox == nil {
		writeJSON(w, 503, map[string]string{"error": "durable_store_required"})
		return
	}
	var body struct {
		Reason string `json:"reason"`
	}
	id := r.PathValue("request_id")
	if !validHubSpawnRequestID(id) || !decodeHubJSON(w, r, 4096, &body) || !validSpawnAbandonReason(body.Reason) {
		writeJSON(w, 400, map[string]string{"error": "invalid_request", "fix": "name a request id and a one-line --reason"})
		return
	}
	if !h.spawnActions.begin(id) {
		writeJSON(w, 409, map[string]string{"error": "spawn_abandon_refused", "reason": "sending_or_abandoning", "request_id": id})
		return
	}
	defer h.spawnActions.end(id)
	task, err := h.inbox.store.spawnTask(r.Context(), id)
	if errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, 404, map[string]string{"error": "not_found"})
		return
	}
	if err != nil {
		writeJSON(w, 503, map[string]string{"error": "store_unavailable"})
		return
	}
	if !spawnPending(task.Status) {
		writeJSON(w, 409, map[string]string{"error": "spawn_not_pending", "status": task.Status})
		return
	}
	h.endPendingSpawn(w, r, caller, task, spawnAbandonEnd, body.Reason)
}

// spawnEnd is one operator-side way to finish a pending request: the reason
// code its outcome carries (the code decides the stored status, see
// spawnOutcomeStatus), the status the answer reports, the fix line the
// submitter reads, and the name of its hub log line.
type spawnEnd struct {
	code, status, fix, warning string
	// unboundFix is the fix line for a request no node bound. It names the
	// actor that ended it, so the owner lane is not told the operator did it
	// when its own node credential withdrew it (hk 5214).
	unboundFix string
}

// spawnAbandonEnd is the operator's end: the request fails (hk 3423, hk 5214).
var spawnAbandonEnd = spawnEnd{code: spawnAbandonedReason, status: "failed", fix: "the operator abandoned this request; submit the work again under a new job id", unboundFix: "the request was withdrawn before any node took it; submit the work again under a new job id", warning: "hub spawn abandon"}

// endPendingSpawn finishes task as end says, with reason as its text. It is the
// one release path for an operator-side end (abandon and requester withdrawal,
// hk 5429): a request no node has bound ends with no pane probe, a bound one
// only when its node lists no labelled pane (finishUnownedSpawn). The caller
// has checked that task is pending.
func (h *HubServer) endPendingSpawn(w http.ResponseWriter, r *http.Request, caller hubCaller, task hubSpawnTask, end spawnEnd, reason string) {
	if task.Node == nil {
		h.endUnboundSpawn(w, r, caller, task, end, reason)
		return
	}
	if caller.kind != hubCallerOperator {
		hubUnauthorized(w)
		return
	}
	outcome := hubSpawnOutcome{RC: 75, Stage: "landing", Reason: spawnOutcomeReason(end.code, spawnRefusalLine(reason)), Host: task.Node.Machine, Fix: end.fix}
	released, why := h.finishUnownedSpawn(r.Context(), task, outcome)
	h.logger.Warn(end.warning, "request_id", task.Request.RequestID, "job", task.Job, "host", task.Node.Machine, "status", task.Status, "released", released, "held_by", why, "reason", spawnRefusalLine(reason))
	if !released {
		writeJSON(w, 409, map[string]string{"error": "spawn_abandon_refused", "reason": why, "request_id": task.Request.RequestID, "job": task.Job, "host": task.Node.Machine})
		return
	}
	writeJSON(w, 200, map[string]string{"request_id": task.Request.RequestID, "job_id": task.Job, "status": end.status, "reason": outcome.Reason})
}

// endUnboundSpawn ends a pending request no node has bound (hk 5214). It never
// reached a node, so there is no pane to probe and nothing to stop: the request
// ends with end's code, its role is released in the same transaction as the
// outcome, and its owner lane gets one SPAWN_FAILED (or cancelled) notice. A
// node may end only a request whose owner lane it routes. A request that holds
// an hk job claim stays with its claimed host.
func (h *HubServer) endUnboundSpawn(w http.ResponseWriter, r *http.Request, caller hubCaller, task hubSpawnTask, end spawnEnd, reason string) {
	if caller.kind == hubCallerNode {
		if route, exists := loadReportRelayRoutes(h.reportRelayPath)[task.Owner]; !exists || route.Machine != caller.id {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "lane_not_routed_to_caller"})
			return
		}
	}
	if task.Status != "accepted" {
		writeJSON(w, 409, map[string]string{"error": "spawn_not_bound", "status": task.Status, "fix": "an unbound request releases itself at its deadline"})
		return
	}
	if spawnHoldsClaim(task) {
		writeJSON(w, 409, map[string]string{"error": "spawn_abandon_refused", "reason": "claimed_host", "request_id": task.Request.RequestID, "job": task.Job, "host": spawnPinnedHost(task)})
		return
	}
	outcome := hubSpawnOutcome{RC: 75, Stage: "placement", Reason: spawnOutcomeReason(end.code, spawnRefusalLine(reason)), Fix: end.unboundFix}
	changed, err := h.completeSpawn(r.Context(), task, outcome)
	if err != nil || !changed {
		why := "not_pending"
		if err != nil {
			why = "store_unavailable"
		}
		h.logger.Info("hub kept an unbound spawn", "request_id", task.Request.RequestID, "job", task.Job, "owner", task.Owner, "reason", why)
		writeJSON(w, 409, map[string]string{"error": "spawn_abandon_refused", "reason": why, "request_id": task.Request.RequestID, "job": task.Job})
		return
	}
	h.recordSpawnTrace(task, outcome)
	h.logger.Warn(end.warning, "request_id", task.Request.RequestID, "job", task.Job, "owner", task.Owner, "status", task.Status, "released", true, "reason", spawnRefusalLine(reason))
	writeJSON(w, 200, map[string]string{"request_id": task.Request.RequestID, "job_id": task.Job, "status": end.status, "reason": outcome.Reason})
}

// validSpawnAbandonReason is one non-empty printable line within the limit.
func validSpawnAbandonReason(reason string) bool {
	if reason == "" || len(reason) > spawnAbandonReasonLimit {
		return false
	}
	for _, r := range reason {
		if r < ' ' || r == 0x7f {
			return false
		}
	}
	return true
}

// noteStuckSpawn warns once when task is still pending spawnStuckAfter past
// its deadline: a Sentry warning fingerprinted spawn-ownership-stuck and a
// fleet event kind spawn, outcome stuck. An outbox row under
// <request>:<attempt>:stuck, marked recorded after the warning is emitted, is
// the durable once-marker: a restart does not warn again, and a stop between
// the row and the warning still warns on the next pass.
func (h *HubServer) noteStuckSpawn(task hubSpawnTask) {
	now := h.now().UTC()
	if now.Sub(task.Deadline) < spawnStuckAfter {
		return
	}
	current, err := h.inbox.store.spawnTask(h.closeCtx, task.Request.RequestID)
	if err != nil || !spawnPending(current.Status) || current.Attempt != task.Attempt {
		return
	}
	eventID, pendingWarning, err := h.inbox.store.pendingSpawnStuck(h.closeCtx, current, now)
	if err != nil || !pendingWarning {
		return
	}
	host := spawnTaskHost(current)
	pending := int64(now.Sub(current.Deadline) / time.Second)
	h.logger.Warn("hub spawn ownership stuck", "request_id", current.Request.RequestID, "job", current.Job, "host", host, "status", current.Status, "past_deadline_s", pending)
	h.emitFleet(fleetEvent{Kind: "spawn", Job: current.Job, RequestID: current.Request.RequestID, Outcome: "stuck", Reason: current.Status, Detail: fmt.Sprintf("host=%s past_deadline_s=%d", firstNonEmptyString(host, "none"), pending)})
	h.captureWarning([]string{"spawn-ownership-stuck"}, "SPAWN_OWNERSHIP_STUCK "+current.Status, map[string]string{"job": current.Job, "host": firstNonEmptyString(host, "none"), "status": current.Status, "request": current.Request.RequestID, "attempt": strconv.Itoa(current.Attempt)})
	h.inbox.store.recordSpawnStuck(h.closeCtx, eventID)
}

// pendingSpawnStuck makes sure task's attempt has a stuck-warning outbox row
// and reports whether its warning is still to be emitted. The row is
// delivered=1 so lane delivery never relays it; telemetry_recorded turns 1
// only after the warning was emitted, so a hub that stops in between warns
// on the next pass instead of never (at least once, never lost).
func (s *laneInboxStore) pendingSpawnStuck(parent context.Context, task hubSpawnTask, now time.Time) (eventID string, pending bool, err error) {
	ctx, done := s.op(parent)
	defer done()
	raw, err := json.Marshal(map[string]string{"status": task.Status})
	if err != nil {
		return "", false, err
	}
	eventID = fmt.Sprintf("%s:%d:stuck", task.Request.RequestID, task.Attempt)
	if _, err = s.db.ExecContext(ctx, s.q(s.dialect.insertIgnore(`INSERT INTO spawn_notifications(event_id,request_id,job,owner,result,created_ms,delivered,telemetry_recorded) VALUES(?,?,?,?,?,?,1,0)`, "")), eventID, task.Request.RequestID, task.Job, task.Owner, string(raw), now.UnixMilli()); err != nil {
		return "", false, err
	}
	var recorded int
	if err = s.db.QueryRowContext(ctx, s.q(`SELECT telemetry_recorded FROM spawn_notifications WHERE event_id=?`), eventID).Scan(&recorded); err != nil {
		return "", false, err
	}
	return eventID, recorded == 0, nil
}

// recordSpawnStuck marks an emitted stuck warning; a failed write only
// repeats the warning on a later pass.
func (s *laneInboxStore) recordSpawnStuck(parent context.Context, eventID string) {
	ctx, done := s.op(parent)
	defer done()
	_, _ = s.db.ExecContext(ctx, s.q(`UPDATE spawn_notifications SET telemetry_recorded=1 WHERE event_id=?`), eventID)
}

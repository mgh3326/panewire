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

	"github.com/getsentry/sentry-go"

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
	// spawnAbandonReasonLimit bounds the operator's --reason text.
	spawnAbandonReasonLimit = 256
)

// spawnPending reports whether status still holds ownership unresolved.
func spawnPending(status string) bool {
	switch status {
	case "accepted", "dispatching", "reconciling", "deadline_exceeded":
		return true
	}
	return false
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
		return false
	}
	h.mu.Lock()
	delete(h.spawnOrphanProbed, task.Request.RequestID)
	h.mu.Unlock()
	return true
}

// finishUnownedSpawn finishes a bound pending request as failed with outcome
// once the node lists no pane bearing its job label: it marks a live hk job
// lost through that node, completes the request and releases its (task, role)
// reservation. Anything it cannot prove keeps the request pending; why names
// the check that held it. A crash after the lost write converges: a failed
// holder whose hk job is lost releases on the next claim.
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
	code, _, _ := strings.Cut(outcome.Reason, ": ")
	if state, _ := record["state"].(string); exists && !hkTerminalState(state) {
		if lost, reason := h.markHolderJobLostBecause(ctx, app, task, label, code); !lost {
			return false, "mark_lost:" + reason
		}
	}
	changed, err := h.inbox.store.completeSpawn(ctx, task, outcome, h.now().UTC())
	if err != nil {
		return false, "store_unavailable"
	}
	if !changed {
		return false, "not_pending"
	}
	h.recordSpawnTrace(task, outcome)
	if err = h.releaseSpawnRole(ctx, task); err != nil {
		return false, "release_write_failed"
	}
	return true, ""
}

// releaseSpawnRole frees the (task, role) slot a finished request holds. A
// child or task-less request holds none.
func (h *HubServer) releaseSpawnRole(ctx context.Context, task hubSpawnTask) error {
	spawn, err := fleet.ValidateSubmission(task.Request)
	if err != nil || spawn.Task == "" || spawn.ParentJob != "" {
		return err
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
// node lists no pane bearing the job label.
func (h *HubServer) handleSpawnAbandon(w http.ResponseWriter, r *http.Request) {
	if h.authenticate(r).kind != hubCallerOperator {
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
	if task.Node == nil {
		writeJSON(w, 409, map[string]string{"error": "spawn_not_bound", "status": task.Status, "fix": "an unbound request releases itself at its deadline"})
		return
	}
	outcome := hubSpawnOutcome{RC: 75, Stage: "landing", Reason: spawnOutcomeReason(spawnAbandonedReason, spawnRefusalLine(body.Reason)), Host: task.Node.Machine, Fix: "the operator abandoned this request; submit the work again under a new job id"}
	released, why := h.finishUnownedSpawn(r.Context(), task, outcome)
	h.logger.Warn("hub spawn abandon", "request_id", id, "job", task.Job, "host", task.Node.Machine, "status", task.Status, "released", released, "held_by", why, "reason", spawnRefusalLine(body.Reason))
	if !released {
		writeJSON(w, 409, map[string]string{"error": "spawn_abandon_refused", "reason": why, "request_id": id, "job": task.Job, "host": task.Node.Machine})
		return
	}
	writeJSON(w, 200, map[string]string{"request_id": id, "job_id": task.Job, "status": "failed", "reason": outcome.Reason})
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
// fleet event kind spawn, outcome stuck. A delivered outbox row under
// <request>:<attempt>:stuck is the durable once-marker, so a restart does not
// warn again.
func (h *HubServer) noteStuckSpawn(task hubSpawnTask) {
	now := h.now().UTC()
	if now.Sub(task.Deadline) < spawnStuckAfter {
		return
	}
	current, err := h.inbox.store.spawnTask(h.closeCtx, task.Request.RequestID)
	if err != nil || !spawnPending(current.Status) || current.Attempt != task.Attempt {
		return
	}
	if first, err := h.inbox.store.markSpawnStuck(h.closeCtx, current, now); err != nil || !first {
		return
	}
	host := spawnTaskHost(current)
	pending := int64(now.Sub(current.Deadline) / time.Second)
	h.logger.Warn("hub spawn ownership stuck", "request_id", current.Request.RequestID, "job", current.Job, "host", host, "status", current.Status, "past_deadline_s", pending)
	write := emitFleetEvent
	if h.fleetEvents != nil {
		write = h.fleetEvents
	}
	write(now, fleetEvent{Kind: "spawn", Job: current.Job, RequestID: current.Request.RequestID, Outcome: "stuck", Reason: current.Status, Detail: fmt.Sprintf("host=%s past_deadline_s=%d", firstNonEmptyString(host, "none"), pending)})
	if client := h.spawnSentryClient(); client != nil {
		hub := sentry.NewHub(client, sentry.NewScope())
		hub.CaptureEvent(&sentry.Event{Level: sentry.LevelWarning, Message: "SPAWN_OWNERSHIP_STUCK " + current.Status, Fingerprint: []string{"spawn-ownership-stuck"}, Tags: map[string]string{"job": current.Job, "host": firstNonEmptyString(host, "none"), "status": current.Status, "request": current.Request.RequestID, "attempt": strconv.Itoa(current.Attempt)}})
	}
}

// markSpawnStuck records the stuck warning for task's attempt and reports
// whether this call recorded it first.
func (s *laneInboxStore) markSpawnStuck(parent context.Context, task hubSpawnTask, now time.Time) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	raw, err := json.Marshal(map[string]string{"status": task.Status})
	if err != nil {
		return false, err
	}
	result, err := s.db.ExecContext(ctx, s.q(`INSERT INTO spawn_notifications(event_id,request_id,job,owner,result,created_ms,delivered,telemetry_recorded) VALUES(?,?,?,?,?,?,1,1) ON CONFLICT DO NOTHING`), fmt.Sprintf("%s:%d:stuck", task.Request.RequestID, task.Attempt), task.Request.RequestID, task.Job, task.Owner, string(raw), now.UnixMilli())
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	return n == 1, nil
}

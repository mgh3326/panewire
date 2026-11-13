package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// A pool action is the hub's durable record of one job's quota-stop ending on
// an exhausted pool: the decision to end or hand over the job, and how far the
// ending got. The sweep writes it before it closes anything, and a later pass,
// an independent reconciler or a restarted hub finishes it from there, without
// another quota publish and without the job's pane. That is the part that makes
// the action recoverable: once a pane is closed the job stops being listed by
// the node heartbeat, and hk never rewrites a terminal record, so the progress
// of an ending cannot be inferred from either.
//
// The stages, in order:
//
//	intent    the decision and every input needed to finish it (hk end reason,
//	          owner notice identity, and for a hand-over the successor's job and
//	          exact submission) are persisted; nothing has been touched.
//	closing   eligibility was rechecked and a close was durably authorized.
//	closed    the job's pane was closed, or proven absent after authorization.
//	terminal  hk holds the job's terminal record.
//	released  the role reservation, when the job held one, is dropped.
//	claimed   (hand-over only) the successor operation exists.
//	notified  (end only) the owner's job.lost was made durable.
//	done      every obligation the action had is complete.
//
// A cancelled action is one the hub stopped before touching anything (the reset
// passed or a newer reading cleared the exhaustion) or one whose job ended on
// its own: there is nothing left to do, and no notice to send.
const (
	poolActionEnd      = "end"
	poolActionHandOver = "hand_over"

	poolActionIntent   = "intent"
	poolActionClosing  = "closing"
	poolActionClosed   = "closed"
	poolActionTerminal = "terminal"
	poolActionReleased = "released"
	poolActionClaimed  = "claimed"
	poolActionNotified = "notified"
	poolActionDone     = "done"
	poolActionCanceled = "cancelled"
)

// poolActionStages is the order stages advance in. End actions skip claimed and
// hand-over actions skip notified, so "at least" only ever compares against a
// stage the action kind passes through.
var poolActionStages = []string{
	poolActionIntent, poolActionClosing, poolActionClosed, poolActionTerminal, poolActionReleased, poolActionClaimed, poolActionNotified, poolActionDone,
}

// poolActionStageAtLeast reports whether stage has reached want.
func poolActionStageAtLeast(stage, want string) bool {
	return poolActionStageIndex(stage) >= poolActionStageIndex(want)
}

func poolActionStageIndex(stage string) int {
	for i, known := range poolActionStages {
		if known == stage {
			return i
		}
	}
	return -1
}

// poolAction is one row of pool_exhausted_actions.
type poolAction struct {
	Request, Job, Owner, Role, Kind string
	Attempt                         int
	Epoch                           uint64
	Pool, Account, Cause            string
	Reset                           time.Time
	EndReason, EventID              string
	// Successor and SuccessorRequest are a hand-over's successor job id and
	// its exact submission, both fixed at intent so a retry replays the same
	// operation instead of generating a new request id.
	Successor, SuccessorRequest string
	Stage                       string
	// Registered is when the holder node acknowledged the action's marker
	// (zero before that, and forever on a node with no job ledger).
	Registered       time.Time
	Created, Updated time.Time
}

// poolActionLookback bounds the sweep's search for jobs it has not acted on
// yet. It is not a recovery bound: an action that exists is driven to done
// whatever its age. A job older than this that no pass has seen is left alone,
// which keeps the per-publish cost proportional to recent spawns.
const poolActionLookback = 7 * 24 * time.Hour

// errPoolActionBusy means another operation holds the job's guard; the action
// stays pending for the next pass.
var errPoolActionBusy = errors.New("job busy")

// savePoolAction writes the action row. The intent and every later stage are
// written with the same row, so a lost save only means the step is repeated,
// which every step allows.
func (s *laneInboxStore) savePoolAction(parent context.Context, action poolAction) error {
	ctx, done := s.op(parent)
	defer done()
	if action.Epoch == 0 {
		action.Epoch = 1
	}
	now := action.Updated
	if now.IsZero() {
		now = time.Now()
	}
	_, err := s.db.ExecContext(ctx, s.q(`INSERT INTO pool_exhausted_actions(request_id,attempt,job,owner,role,kind,pool,account,cause,reset_ms,end_reason,event_id,epoch,successor,successor_request,stage,registered_ms,created_ms,updated_ms)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(request_id,attempt) DO UPDATE SET stage=excluded.stage,updated_ms=excluded.updated_ms,end_reason=excluded.end_reason,successor=excluded.successor,successor_request=excluded.successor_request,registered_ms=excluded.registered_ms`),
		action.Request, action.Attempt, action.Job, action.Owner, action.Role, action.Kind, action.Pool, action.Account, action.Cause,
		action.Reset.UTC().UnixMilli(), action.EndReason, action.EventID, action.Epoch, action.Successor, action.SuccessorRequest, action.Stage,
		action.Registered.UTC().UnixMilli(), action.Created.UTC().UnixMilli(), now.UTC().UnixMilli())
	return err
}

// poolAction reads one action row; exists is false when it has none.
func (s *laneInboxStore) poolAction(parent context.Context, requestID string, attempt int) (poolAction, bool, error) {
	ctx, done := s.op(parent)
	defer done()
	return scanPoolAction(s.db.QueryRowContext(ctx, s.q(`SELECT `+poolActionColumns+` FROM pool_exhausted_actions WHERE request_id=? AND attempt=?`), requestID, attempt))
}

// pendingPoolActions lists the actions that still owe a step, least recently
// attempted first, so an action that keeps being skipped goes to the back and
// cannot starve later ones.
func (s *laneInboxStore) pendingPoolActions(parent context.Context, limit int) ([]poolAction, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+poolActionColumns+` FROM pool_exhausted_actions WHERE stage NOT IN (?,?) ORDER BY updated_ms,created_ms,request_id LIMIT ?`), poolActionDone, poolActionCanceled, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var actions []poolAction
	for rows.Next() {
		action, _, err := scanPoolAction(rows)
		if err != nil {
			return nil, err
		}
		actions = append(actions, action)
	}
	return actions, rows.Err()
}

// poolActionPage scans all pending actions using an immutable cursor so an
// indefinitely blocked action cannot starve later work.
func (s *laneInboxStore) poolActionPage(parent context.Context, after poolAction, limit int) ([]poolAction, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+poolActionColumns+` FROM pool_exhausted_actions WHERE stage NOT IN (?,?) AND (created_ms>? OR (created_ms=? AND request_id>?) OR (created_ms=? AND request_id=? AND attempt>?)) ORDER BY created_ms,request_id,attempt LIMIT ?`), poolActionDone, poolActionCanceled, after.Created.UnixMilli(), after.Created.UnixMilli(), after.Request, after.Created.UnixMilli(), after.Request, after.Attempt, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var actions []poolAction
	for rows.Next() {
		action, _, err := scanPoolAction(rows)
		if err != nil {
			return nil, err
		}
		actions = append(actions, action)
	}
	return actions, rows.Err()
}

// deletePoolAction removes an action that never became an effect.
func (s *laneInboxStore) deletePoolAction(parent context.Context, requestID string, attempt int) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`DELETE FROM pool_exhausted_actions WHERE request_id=? AND attempt=?`), requestID, attempt)
	return err
}

// succeededSpawnsSince lists the operations that succeeded after since. The
// sweep reads its candidates from here rather than from the node heartbeats:
// the heartbeat stops naming a job once its pane is closed, and a job whose
// ending is half done must still be found.
func (s *laneInboxStore) succeededSpawnsSince(parent context.Context, since time.Time) ([]hubSpawnTask, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+spawnTaskColumns+` FROM spawn_operations WHERE status='succeeded' AND created_ms>=? ORDER BY created_ms,request_id`), since.UnixMilli())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []hubSpawnTask
	for rows.Next() {
		task, err := scanSpawnTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

const poolActionColumns = `request_id,attempt,job,owner,role,kind,pool,account,cause,reset_ms,end_reason,event_id,epoch,successor,successor_request,stage,registered_ms,created_ms,updated_ms`

func scanPoolAction(row interface{ Scan(...any) error }) (poolAction, bool, error) {
	var action poolAction
	var resetMS, registeredMS, createdMS, updatedMS int64
	err := row.Scan(&action.Request, &action.Attempt, &action.Job, &action.Owner, &action.Role, &action.Kind, &action.Pool, &action.Account, &action.Cause,
		&resetMS, &action.EndReason, &action.EventID, &action.Epoch, &action.Successor, &action.SuccessorRequest, &action.Stage, &registeredMS, &createdMS, &updatedMS)
	if errors.Is(err, sql.ErrNoRows) {
		return poolAction{}, false, nil
	}
	if err != nil {
		return poolAction{}, false, err
	}
	action.Reset = time.UnixMilli(resetMS).UTC()
	if registeredMS > 0 {
		action.Registered = time.UnixMilli(registeredMS).UTC()
	}
	action.Created = time.UnixMilli(createdMS).UTC()
	action.Updated = time.UnixMilli(updatedMS).UTC()
	return action, true, nil
}

// poolActionBudget bounds one drive of one action.
func (h *HubServer) poolActionBudget() time.Duration {
	if h.quotaReplaceBudget > 0 {
		return h.quotaReplaceBudget
	}
	return quotaReplaceBudget
}

// drivePoolAction carries one action forward as far as it can go, under the
// job's guard so a screen-triggered replacement and a concurrent pass never
// race on the same job. A step that cannot finish leaves the row at the stage
// it reached and the next pass resumes there; every step is written to be
// repeatable.
func (h *HubServer) drivePoolAction(ctx context.Context, action poolAction) poolJobResult {
	role := action.Role
	skip := func(why string, kv ...any) poolJobResult {
		h.logger.Info("pool exhausted action pending", append([]any{"job", action.Job, "pool", action.Pool, "stage", action.Stage, "reason", why}, kv...)...)
		return poolJobSkip(role, why)
	}
	if action.Stage == poolActionDone || action.Stage == poolActionCanceled {
		return poolJobSkip(role, "in_progress_or_done")
	}
	ctx, cancel := context.WithTimeout(ctx, h.poolActionBudget())
	defer cancel()
	release, held := h.jobGuard.acquire(ctx, action.Job)
	if !held {
		return skip("job_busy")
	}
	defer release()
	// Another pass may have finished some of the steps while this one waited
	// for the guard; the row, not the copy read before the wait, says where the
	// action is.
	refreshed, exists, err := h.inbox.store.poolAction(ctx, action.Request, action.Attempt)
	if err != nil {
		return skip("action_unreadable")
	}
	if exists {
		action = refreshed
	}
	if action.Stage == poolActionDone || action.Stage == poolActionCanceled {
		return poolJobSkip(role, "in_progress_or_done")
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		return skip("spawn_config_unavailable")
	}
	holder, err := h.inbox.store.latestSpawnForJob(ctx, action.Job)
	if err != nil || holder.Request.RequestID != action.Request {
		return skip("holder_unreadable", "error", fmt.Sprintf("%v", err))
	}
	prior, err := fleet.ValidateSubmission(holder.Request)
	if err != nil {
		return skip("unreadable_request")
	}
	record, exists, err := h.readPoolJobRecord(ctx, app, action.Job)
	state, _ := record["state"].(string)
	if err != nil || !exists {
		return skip("job_record", "state", state)
	}
	name := heldAgentName(holder, record)
	if pool, _ := record["pool"].(string); pool != "" && pool != action.Pool {
		if poolActionStageAtLeast(action.Stage, poolActionClosing) {
			if !h.finishPoolActionMarker(ctx, holder, name, action) {
				return skip("marker_not_finished")
			}
		} else if !h.clearPoolActionMarker(ctx, holder, name, action) {
			return skip("marker_not_cleared")
		}
		h.cancelPoolAction(ctx, action, "pool_mismatch")
		return poolJobSkip(role, "pool_mismatch")
	}
	cancelTerminal := func(reason string) bool {
		if !h.finishPoolActionMarker(ctx, holder, name, action) {
			return false
		}
		h.cancelPoolAction(ctx, action, reason)
		return true
	}
	switch state {
	case "completed", "escalated":
		// The job ended on its own. hk's record is the truth, and an owner
		// notice for a quota stop would be false, so the action ends here.
		if !cancelTerminal("hk_" + state) {
			return skip("marker_not_finished")
		}
		h.logger.Info("pool exhausted action cancelled: the job ended on its own", "job", action.Job, "state", state)
		return poolJobSkip(role, "completed")
	case "lost":
		if reason, _ := record["end_reason"].(string); reason != action.EndReason {
			if !cancelTerminal("unrelated_loss") {
				return skip("marker_not_finished")
			}
			return poolJobSkip(role, "unrelated_loss")
		}
		// A record that is already terminal is not progress unless this action
		// made an effect: an old loss for an unrelated reason is not a quota
		// stop, and reviving it would hand over or notify again.
		if !poolActionStageAtLeast(action.Stage, poolActionClosed) {
			if !cancelTerminal("already_lost") {
				return skip("marker_not_finished")
			}
			return poolJobSkip(role, "already_lost")
		}
		// The holder's watcher (or a retry of this action) wrote it. hk never
		// rewrites a terminal record; the remaining duties (role release, owner
		// notice) are still this action's to finish.
		if !poolActionStageAtLeast(action.Stage, poolActionTerminal) {
			action.Stage = poolActionTerminal
			h.savePoolActionStage(ctx, action)
		}
	}
	if !poolActionStageAtLeast(action.Stage, poolActionClosed) {
		probe := h.probeHeldPane(ctx, spawnHolderHost(holder), holder.Outcome.Pane, name)
		if probe.state == heldPaneAbsent {
			if !poolActionStageAtLeast(action.Stage, poolActionClosing) {
				if !h.clearPoolActionMarker(ctx, holder, name, action) {
					return skip("marker_not_cleared")
				}
				h.cancelPoolAction(ctx, action, "pane_disappeared_before_close")
				return poolJobSkip(role, "pane_disappeared_before_close")
			}
			// The pane is already gone: an earlier attempt closed it (or the
			// node lost it) after registering its marker, and only the stage
			// write was lost. That is an effect, so the intent may no longer be
			// cancelled for an expired reading; finish the ending.
			action.Stage = poolActionClosed
			if err := h.savePoolActionStage(ctx, action); err != nil {
				return skip("closed_stage_not_saved")
			}
		} else {
			if probe.state != heldPanePresent || !nodePaneIdleStatus[probe.row.AgentStatus] || !validNodeCommandPath(probe.row.Cwd) {
				return skip("pane_not_idle", "probe", probe.reason, "status", probe.row.AgentStatus)
			}
			// The holder's own watcher must know the action before its pane
			// goes: it reads this marker and leaves the terminal record to the
			// action instead of writing a plain lost record the hub could not
			// complete. It is written only for a pane about to be closed.
			if action.Registered.IsZero() {
				registered, why := h.setPoolActionOnHolder(ctx, holder, name, action, true)
				switch {
				case registered:
					action.Registered = h.now()
					if err := h.savePoolActionStage(ctx, action); err != nil {
						return skip("registration_not_saved")
					}
				case why == nodePoolActionMoved:
					return skip("attempt_moved", "action", action.Request)
				default:
					return skip("action_not_registered", "reason", why)
				}
			}
			// A watcher may have won its terminal decision just before it
			// acknowledged the marker. Confirm hk again after registration.
			fresh, found, err := h.readPoolJobRecord(ctx, app, action.Job)
			if err != nil || !found {
				return skip("job_record")
			}
			if freshState, _ := fresh["state"].(string); freshState == "completed" || freshState == "escalated" || freshState == "lost" {
				if !cancelTerminal("hk_" + freshState) {
					return skip("marker_not_finished")
				}
				return poolJobSkip(role, "already_terminal")
			}
			// An intent is only allowed to become an effect while the
			// exhaustion still holds; this is the last check before the close.
			if !h.now().Before(action.Reset) {
				if !h.clearPoolActionMarker(ctx, holder, name, action) {
					return skip("marker_not_cleared")
				}
				h.cancelPoolAction(ctx, action, "reset_passed")
				return poolJobSkip(role, "reset_passed")
			}
			exhaustion := poolExhaustion{Pool: action.Pool, Account: action.Account, Cause: action.Cause, Reset: action.Reset}
			if h.exhaustionSuperseded(exhaustion, h.now()) {
				if !h.clearPoolActionMarker(ctx, holder, name, action) {
					return skip("marker_not_cleared")
				}
				h.cancelPoolAction(ctx, action, "superseded")
				return poolJobSkip(role, "superseded")
			}
			if !h.closeHeldPaneIf(ctx, holder, name, probe.row, func() bool {
				now := h.now()
				if !now.Before(action.Reset) || h.exhaustionSuperseded(exhaustion, now) {
					return false
				}
				action.Stage = poolActionClosing
				return h.savePoolActionStage(ctx, action) == nil
			}) {
				return skip("pane_not_closed", "pane", holder.Outcome.Pane)
			}
			action.Stage = poolActionClosed
			if err := h.savePoolActionStage(ctx, action); err != nil {
				return skip("closed_stage_not_saved")
			}
		}
	}
	if !poolActionStageAtLeast(action.Stage, poolActionTerminal) {
		lost, reason := h.markHolderLostWithRetry(ctx, app, holder, name, action.EndReason)
		if !lost {
			h.logger.Warn("pool exhausted action: the job's record was not written", "job", action.Job, "reason", reason)
		}
		// Read the record back: a completion can win the write, and the node's
		// mark-lost path reports success for it. The state, not the boolean, is
		// what decides whether the job ended on its own.
		after, afterExists, readErr := h.readPoolJobRecord(ctx, app, action.Job)
		afterState, _ := after["state"].(string)
		switch {
		case readErr == nil && afterExists && (afterState == "completed" || afterState == "escalated"):
			if !cancelTerminal("hk_" + afterState) {
				return skip("marker_not_finished")
			}
			return poolJobSkip(role, "completed")
		case readErr == nil && afterExists && afterState == "lost":
			endReason, _ := after["end_reason"].(string)
			if endReason != action.EndReason {
				if !cancelTerminal("unrelated_loss") {
					return skip("marker_not_finished")
				}
				return poolJobSkip(role, "unrelated_loss")
			}
			lost = true
		default:
			lost = false
		}
		if !lost {
			return skip("record_not_written", "write", reason)
		}
		action.Stage = poolActionTerminal
		h.savePoolActionStage(ctx, action)
	}
	if action.Kind != poolActionHandOver && !poolActionStageAtLeast(action.Stage, poolActionReleased) {
		if err := h.releasePoolActionRole(ctx, holder, prior); err != nil {
			h.logger.Warn("pool exhausted action: the role reservation was not released", "job", action.Job, "reason", err.Error())
			return skip("role_release_failed")
		}
		action.Stage = poolActionReleased
		h.savePoolActionStage(ctx, action)
	}
	if action.Kind == poolActionHandOver {
		if !poolActionStageAtLeast(action.Stage, poolActionClaimed) {
			var request fleet.Submission
			if err := json.Unmarshal([]byte(action.SuccessorRequest), &request); err != nil {
				h.cancelPoolAction(ctx, action, "successor_unreadable")
				return poolJobSkip(role, "successor_unreadable")
			}
			next, err := fleet.ValidateSubmission(request)
			if err != nil {
				h.cancelPoolAction(ctx, action, "invalid_replacement")
				return poolJobSkip(role, "invalid_replacement")
			}
			previousID, roleKey := "", ""
			if prior.Task != "" && prior.ParentJob == "" {
				previousID, roleKey = holder.Request.RequestID, prior.RoleKey()
			}
			if _, _, err := h.inbox.store.claimSpawnReplacing(ctx, request, action.Successor, next.Owner, h.now().UTC(), previousID, roleKey, "accepted"); err != nil {
				return skip("claim_refused", "error", err.Error())
			}
			h.kickSpawn()
			action.Stage = poolActionClaimed
			h.savePoolActionStage(ctx, action)
			text := fmt.Sprintf("handed over %s (%s quota) to %s on the successor's placement", action.Job, action.Pool, action.Successor)
			h.relayLaneEventContext(ctx, hubJobEventPayload{EventID: "quota-replace:" + action.Successor, JobID: action.Successor, OwnerLane: next.Owner, NotifyType: "info", Text: utf8PrefixWithin(text, laneEventTextLimit), Host: "hub", Epoch: 1}, nil)
		}
		if !h.finishPoolActionMarker(ctx, holder, name, action) {
			return skip("marker_not_finished")
		}
		action.Stage = poolActionDone
		h.savePoolActionStage(ctx, action)
		return poolJobResult{action: poolJobHandedOver, role: role, successor: action.Successor}
	}
	if !poolActionStageAtLeast(action.Stage, poolActionNotified) {
		// The owner is told only once the record is terminal: a notice sent
		// before it could tell the owner a job is lost while it still runs.
		if !h.relayJobEventFromContext(ctx, "", "job.lost", hubJobEventPayload{
			JobID: action.Job, Epoch: action.Epoch, OwnerLane: action.Owner, AgentLabel: name, PaneID: holder.Outcome.Pane,
			EventID: action.EventID, Reason: action.EndReason,
		}) {
			return skip("owner_not_notified", "owner", action.Owner)
		}
		action.Stage = poolActionNotified
		h.savePoolActionStage(ctx, action)
	}
	if !h.finishPoolActionMarker(ctx, holder, name, action) {
		return skip("marker_not_finished")
	}
	action.Stage = poolActionDone
	h.savePoolActionStage(ctx, action)
	return poolJobResult{action: poolJobEnded, role: role}
}

// setPoolActionOnHolder writes (register) or removes (clear) the action marker
// in the holder node's job directory, through the holder's own connection. The
// marker is what keeps the node's watcher from writing its own terminal record
// for an attempt the hub is ending. why is "" on success or the node's outcome.
func (h *HubServer) setPoolActionOnHolder(ctx context.Context, holder hubSpawnTask, name string, action poolAction, register bool) (bool, string) {
	op, endReason := nodePoolActionClear, ""
	if register {
		op, endReason = nodePoolActionRegister, action.EndReason
	}
	args := nodeJobPoolActionArgs{
		Job: action.Job, PaneID: holder.Outcome.Pane, Label: name,
		ActionID: fmt.Sprintf("%s:%d", action.Request, action.Attempt), Op: op, EndReason: endReason,
	}
	var result nodeJobPoolActionResult
	if !h.nodeCall(ctx, spawnHolderHost(holder), nodeCommandJobPoolAction, args, spawnHeldPaneActWait, &result) {
		return false, "node_unreachable"
	}
	switch {
	case register && result.Outcome == nodePoolActionRegistered:
		return true, ""
	case !register && (result.Outcome == nodePoolActionCleared || result.Outcome == nodePoolActionMoved):
		return true, ""
	}
	return false, result.Outcome
}

// finishPoolActionMarker tells the holder watcher that the hub owns the
// completed ending, so it exits without emitting a second loss event.
func (h *HubServer) finishPoolActionMarker(ctx context.Context, holder hubSpawnTask, name string, action poolAction) bool {
	args := nodeJobPoolActionArgs{Job: action.Job, PaneID: holder.Outcome.Pane, Label: name,
		ActionID: fmt.Sprintf("%s:%d", action.Request, action.Attempt), Op: nodePoolActionFinish, EndReason: action.EndReason}
	var result nodeJobPoolActionResult
	if !h.nodeCall(ctx, spawnHolderHost(holder), nodeCommandJobPoolAction, args, spawnHeldPaneActWait, &result) {
		return false
	}
	if result.Outcome == nodePoolActionMoved {
		return h.clearPoolActionMarker(ctx, holder, name, action)
	}
	return result.Outcome == nodePoolActionFinished || result.Outcome == nodePoolActionNoLedger
}

// clearPoolActionMarker must be acknowledged before a pre-close cancellation.
// Registration may have succeeded even when saving that acknowledgement failed,
// so cancellation sends the clear even when Registered is zero.
func (h *HubServer) clearPoolActionMarker(ctx context.Context, holder hubSpawnTask, name string, action poolAction) bool {
	if ok, why := h.setPoolActionOnHolder(ctx, holder, name, action, false); !ok {
		h.logger.Warn("pool exhausted action: the holder's marker was not cleared", "job", action.Job, "reason", why)
		return why == nodePoolActionNoLedger
	}
	return true
}

// readPoolJobRecord reads the hk record of a job.
func (h *HubServer) readPoolJobRecord(ctx context.Context, app *fleet.App, job string) (map[string]any, bool, error) {
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return app.ExistingJob(readCtx, job)
}

// releasePoolActionRole drops the role reservation the job holds, if it owns
// one. A reservation held by another request is not ours to drop, so that is a
// successful no-op.
func (h *HubServer) releasePoolActionRole(ctx context.Context, holder hubSpawnTask, prior fleet.Spawn) error {
	if prior.Task == "" || prior.ParentJob != "" {
		return nil
	}
	priorID, _, _, err := h.inbox.store.spawnRoleOwner(ctx, prior.RoleKey())
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if priorID != holder.Request.RequestID {
		return nil
	}
	return h.inbox.store.releaseCompletedSpawnRole(ctx, priorID, prior.RoleKey())
}

// savePoolActionStage persists progress and reports failed writes so a caller
// can stop before a dependent irreversible effect.
func (h *HubServer) savePoolActionStage(ctx context.Context, action poolAction) error {
	action.Updated = h.now()
	if err := h.inbox.store.savePoolAction(ctx, action); err != nil {
		h.logger.Warn("pool exhausted action: progress was not saved", "job", action.Job, "stage", action.Stage, "error", err)
		return err
	}
	return nil
}

// cancelPoolAction records that nothing more is owed for the job. An intent
// that never became an effect is deleted instead: the same live attempt can
// exhaust again later, and a cancelled row keyed by (request, attempt) would
// otherwise block the new exhaustion from ever acting on it.
func (h *HubServer) cancelPoolAction(ctx context.Context, action poolAction, reason string) {
	h.logger.Info("pool exhausted action cancelled", "job", action.Job, "role", action.Role, "pool", action.Pool, "reason", reason)
	if !poolActionStageAtLeast(action.Stage, poolActionClosed) {
		if err := h.inbox.store.deletePoolAction(ctx, action.Request, action.Attempt); err != nil {
			h.logger.Warn("pool exhausted action: cancellation was not saved", "job", action.Job, "reason", reason, "error", err)
		}
		return
	}
	action.Stage = poolActionCanceled
	action.Updated = h.now()
	if err := h.inbox.store.savePoolAction(ctx, action); err != nil {
		h.logger.Warn("pool exhausted action: cancellation was not saved", "job", action.Job, "reason", reason, "error", err)
	}
}

// kickSpawn wakes the spawn coordinator after an action claimed a successor.
func (h *HubServer) kickSpawn() {
	select {
	case h.spawnKick <- struct{}{}:
	default:
	}
}

// reconcilePoolActions pages through every action that still owes a step. It runs on
// the quota replacement's schedule, so a hub restarted between a close and its
// record, or between a record and its notice, finishes the work without another
// quota publish; it also delivers a notice whose relay failed earlier.
func (h *HubServer) reconcilePoolActions(ctx context.Context) int {
	if h.inbox == nil {
		return 0
	}
	finished := 0
	cursor := poolAction{Created: time.UnixMilli(0)}
	for {
		actions, err := h.inbox.store.poolActionPage(ctx, cursor, 256)
		if err != nil {
			return finished
		}
		if len(actions) == 0 {
			break
		}
		for _, action := range actions {
			cursor = action
			if result := h.drivePoolAction(ctx, action); result.acted() {
				finished++
				h.logger.Info("pool exhausted action reconciled", "job", action.Job, "role", result.role, "pool", action.Pool, "account", action.Account, "action", result.action, "successor", result.successor)
			}
		}
	}

	return finished
}

// poolActionOwnsHolder fences competing terminal writers and role admissions.
func (h *HubServer) poolActionOwnsHolder(ctx context.Context, holder hubSpawnTask) (bool, error) {
	action, exists, err := h.inbox.store.poolAction(ctx, holder.Request.RequestID, holder.Attempt)
	if err != nil {
		return false, err
	}
	return exists && action.Stage != poolActionDone && action.Stage != poolActionCanceled, nil
}

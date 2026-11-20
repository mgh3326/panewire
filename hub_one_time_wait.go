package panewire

import (
	"context"
	"errors"
	"fmt"
	"strconv"
)

// A told one-time action also holds one durable handoffkeep operator wait, so
// the need reaches the desk's needs_operator section instead of only Discord.
// The contract with handoffkeep-design (hk 5250, decision
// pw-onetime-wait-5250-decision-1):
//
//   - One idempotent intake anchor per need: POST /v1/intake with
//     (source, source_ref) = ("panewire",
//     "one-time-action/<machine>/<kind>/<need>"). A repeat returns the same
//     triage task, so a retry or a hub restart reuses the anchor. The task is
//     only a wait holder — its body says the desk must not schedule work on
//     it — and it sits on lane operator-desk because handoffkeep has no
//     "operator" lane.
//   - One task-scoped operator wait on that anchor: on_kind "operator",
//     on_ref "", reason naming the action. hk dedupes an open wait on
//     (task_id, on_kind, on_ref), so re-asserting updates the reason and
//     returns the same id; it can never stack a second open wait.
//   - The wait opens only after the need is durably told (a Sentry 2xx put it
//     in oneTimeActions), and closes only after anti-flap clearing confirms
//     the need stayed gone for one full readiness cycle — the same moment the
//     row would have been dropped before hk 5250. The confirmed row then only
//     carries the wait-close obligation: a need that returns starts a new
//     episode on the same wait, and the pending close is cancelled.
//   - A declined or otherwise settled anchor cannot hold the wait: intake
//     keeps answering the same (closed) task for the same source_ref, and hk
//     tasks cannot be reopened. panewire therefore asks again under a new
//     source_ref suffixed "/d<dead task id>" — deterministic, so a restart
//     walks the same chain to the live anchor.
//
// Every write is idempotent and every failure leaves the durable row
// untouched, so the next sighting or report retries. hk errors never change
// Sentry/Discord behavior, and with no handoffkeep client configured none of
// this runs. docs/placement.md, "Readiness".

const (
	// oneTimeWaitSource is the intake source name panewire anchors needs under.
	oneTimeWaitSource = "panewire"
	// oneTimeWaitLane is the desk lane the anchors land on; handoffkeep has no
	// "operator" lane, and operator-desk is the lane the desk's lane check
	// watches (handoffkeep-design, hk 5250 decision answer).
	oneTimeWaitLane = "operator-desk"
	// oneTimeWaitRefPrefix starts every anchor's source_ref; the need key
	// follows it so the dedupe identity is (machine, kind, need).
	oneTimeWaitRefPrefix = "one-time-action/"
	// oneTimeWaitAnchorHops bounds the dead-anchor chain a single sync walks.
	// Each hop is one extra intake; a longer chain means something is declining
	// anchors faster than they are used, so the sync fails and retries later.
	oneTimeWaitAnchorHops = 8
)

// oneTimeWaitDeadAnchorStates are the handoffkeep task states whose category
// is completed or canceled (internal/store/states.go in handoffkeep): the
// desk has settled the anchor, so the wait must move to a fresh one. Triage,
// backlog, unstarted and the started states all still hold the wait.
var oneTimeWaitDeadAnchorStates = map[string]bool{
	"merged":    true,
	"deployed":  true,
	"completed": true,
	"dropped":   true,
	"duplicate": true,
	"icebox":    true,
}

// oneTimeWaitSourceRef is the intake dedupe key of the need's first anchor.
func oneTimeWaitSourceRef(key oneTimeActionKey) string {
	return oneTimeWaitRefPrefix + key.machine + "/" + key.kind + "/" + key.need
}

func oneTimeWaitTitle(key oneTimeActionKey, action string) string {
	return fmt.Sprintf("one-time action: %s %s — %s", key.machine, key.kind, action)
}

func oneTimeWaitBody(key oneTimeActionKey, action string) string {
	return fmt.Sprintf(`panewire one-time-action operator wait. This task is only the anchor for the wait; the desk should not schedule work on it.

Need: %s %s %s — %s

While the need stands, panewire keeps one operator wait open here and closes it after anti-flap clearing confirms the need stayed gone for a full readiness cycle. If the need returns, a new wait opens on this anchor.`, key.machine, key.kind, key.need, action)
}

func oneTimeWaitReason(key oneTimeActionKey, action string) string {
	return fmt.Sprintf("%s %s needs a one-time action: %s", key.machine, key.kind, action)
}

// oneTimeWaitAnchor resolves the need's live anchor task. It intakes the
// base source_ref, and while hk answers a settled task it follows the
// "/d<dead task id>" chain to the next generation, so the same walk repeats
// deterministically after a restart.
func (h *HubServer) oneTimeWaitAnchor(ctx context.Context, key oneTimeActionKey, action string) (handoffkeepTaskRef, error) {
	ref := oneTimeWaitSourceRef(key)
	for attempt := 0; attempt < oneTimeWaitAnchorHops; attempt++ {
		task, err := h.handoffkeep.intakeTask(ctx, oneTimeWaitSource, ref, oneTimeWaitTitle(key, action), oneTimeWaitBody(key, action), oneTimeWaitLane)
		if err != nil {
			return handoffkeepTaskRef{}, err
		}
		if !oneTimeWaitDeadAnchorStates[task.State] {
			return task, nil
		}
		ref = ref + "/d" + strconv.FormatInt(task.ID, 10)
	}
	return handoffkeepTaskRef{}, errors.New("handoffkeep kept answering settled one-time action anchors")
}

// syncOneTimeWaits reconciles the handoffkeep operator waits of the needs one
// notice-worker batch touched. It runs after the batch's Sentry work on the
// worker goroutine, so a slow or failing hk never holds oneTimeMu and never
// delays a notice; every hk write is idempotent and a later observation
// retries whatever failed.
func (h *HubServer) syncOneTimeWaits(ctx context.Context, touched map[oneTimeActionKey]bool, closing []oneTimeActionKey) {
	if h.handoffkeep == nil {
		return
	}
	var opens []oneTimeActionKey
	h.oneTimeMu.Lock()
	for key := range touched {
		if state, ok := h.oneTimeActions[key]; ok && state.GoneSince.IsZero() {
			opens = append(opens, key)
		}
	}
	h.oneTimeMu.Unlock()
	for _, key := range opens {
		h.syncOneTimeWaitOpen(ctx, key)
	}
	for _, key := range closing {
		h.syncOneTimeWaitClose(ctx, key)
	}
}

// syncOneTimeWaitOpen ensures the standing need's anchor holds one open
// operator wait and records the ids on the durable row. A need that still
// shows re-asserts its wait every batch, which also heals a wait the desk
// cleared by hand; a failed write leaves the row untouched for the next
// observation. When the anchor moved — the desk settled the old one — the
// superseded wait is closed first: handoffkeep does not clear a settled
// task's waits itself, so without this the need would hold two open waits.
func (h *HubServer) syncOneTimeWaitOpen(ctx context.Context, key oneTimeActionKey) {
	action := oneTimeActionFor(key.kind, key.need)
	if action == "" {
		return
	}
	anchor, err := h.oneTimeWaitAnchor(ctx, key, action)
	if err != nil {
		h.logOneTimeWait("open", key, err)
		return
	}
	waitID, err := h.handoffkeep.addTaskWait(ctx, anchor.ID, "operator", "", oneTimeWaitReason(key, action))
	if err != nil {
		h.logOneTimeWait("open", key, err)
		return
	}
	h.oneTimeMu.Lock()
	state, ok := h.oneTimeActions[key]
	oldTaskID, oldWaitID := int64(0), int64(0)
	if ok {
		oldTaskID, oldWaitID = state.WaitTaskID, state.WaitID
	}
	h.oneTimeMu.Unlock()
	if oldWaitID > 0 && (oldTaskID != anchor.ID || oldWaitID != waitID) {
		// The recorded wait is not the live one: it sits on a settled anchor
		// or was replaced. Close it before the new ids are stored, so a
		// failed close retries the whole sync instead of orphaning it.
		if err := h.handoffkeep.clearTaskWait(ctx, oldTaskID, oldWaitID, "panewire: one-time action anchor superseded"); err != nil && !errors.Is(err, errHandoffkeepWaitGone) {
			h.logOneTimeWait("open", key, err)
			return
		}
	}
	h.oneTimeMu.Lock()
	if state, ok := h.oneTimeActions[key]; ok && (state.WaitTaskID != anchor.ID || state.WaitID != waitID) {
		state.WaitTaskID, state.WaitID = anchor.ID, waitID
		h.oneTimeActions[key] = state
		h.putOneTimeActionLocked(key, state)
	}
	h.oneTimeMu.Unlock()
}

// syncOneTimeWaitClose clears the need's operator wait after anti-flap
// clearing confirmed it, then drops the durable row. The need re-showing
// since the confirm cancels the close: the wait stays and the row keeps its
// ids for the new episode. A clear whose wait or anchor is already gone
// counts as done.
func (h *HubServer) syncOneTimeWaitClose(ctx context.Context, key oneTimeActionKey) {
	h.oneTimeMu.Lock()
	state, ok := h.oneTimeActions[key]
	if !ok || state.ConfirmedAt.IsZero() {
		h.oneTimeMu.Unlock()
		return
	}
	taskID, waitID := state.WaitTaskID, state.WaitID
	h.oneTimeMu.Unlock()
	if waitID < 1 {
		// The open write never landed, or its answer was lost: resolve the
		// ids through the same idempotent path so an orphaned wait is found
		// and closed rather than left open in handoffkeep.
		action := oneTimeActionFor(key.kind, key.need)
		anchor, err := h.oneTimeWaitAnchor(ctx, key, action)
		if err != nil {
			h.logOneTimeWait("close", key, err)
			return
		}
		waitID, err = h.handoffkeep.addTaskWait(ctx, anchor.ID, "operator", "", oneTimeWaitReason(key, action))
		if err != nil {
			h.logOneTimeWait("close", key, err)
			return
		}
		taskID = anchor.ID
	}
	err := h.handoffkeep.clearTaskWait(ctx, taskID, waitID, "panewire: one-time action cleared")
	if err != nil && !errors.Is(err, errHandoffkeepWaitGone) {
		h.logOneTimeWait("close", key, err)
		return
	}
	h.oneTimeMu.Lock()
	if _, ok := h.oneTimeActions[key]; ok {
		delete(h.oneTimeActions, key)
		h.deleteOneTimeActionLocked(key)
	}
	h.oneTimeMu.Unlock()
}

func (h *HubServer) logOneTimeWait(op string, key oneTimeActionKey, err error) {
	h.logger.Warn("one-time action handoffkeep wait "+op+" failed; the next observation retries", "host", key.machine, "kind", key.kind, "need", key.need, "error", err)
}

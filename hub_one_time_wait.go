package panewire

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"
)

// A told one-time action also holds one durable handoffkeep operator wait, so
// the need reaches the desk's needs_operator section instead of only Discord.
// The contract is panewire-design's decision on hk 5250 (2026-11-20), built on
// the task-scoped wait routes live on handoffkeep:
//
//   - One idempotent intake anchor per need: POST /v1/intake with
//     (source, source_ref) = ("panewire", "one-time-action/<machine>/<kind>/<need>").
//     A repeat returns the same triage task, so a retry or a hub restart reuses
//     it. A settled anchor (the desk closed it) moves the walk to the next
//     generation "<ref>/d<dead task id>"; the walk's position is kept on the row.
//   - One task-scoped operator wait on the anchor: POST /v1/tasks/{id}/waits with
//     on_kind "operator", on_ref "". hk dedupes an open wait on that triple, so
//     a repeat answers the same id and can never stack a second wait.
//   - The anchor is written to the row before the wait write (write-ahead), and
//     the wait id after it. A 4xx refusal means hk committed nothing, so the
//     anchor mark goes; a 5xx or transport error may have committed, so the mark
//     stays and the next pass re-asserts the same deduped wait.
//   - A wait the row holds on a superseded anchor is cleared before the new
//     anchor's wait opens, so the need never holds two open waits.
//   - The close runs only after anti-flap clearing confirmed the need gone
//     (ConfirmedAt). A need that returns before the close lands keeps the same
//     wait; a need that is gone for good closes it.
//
// Every hk write is retried until it settles: by the next readiness report that
// touches the need, and by a timer, so a confirmed close does not wait on a
// report. The wait worker is separate from the Sentry notice worker
// (hub_one_time_notice.go), so a slow or failing handoffkeep never holds a
// notice. With no hub store nothing is written, and with no handoffkeep client
// nothing is sent and the rows keep their wait ids. docs/placement.md,
// "Readiness".

const (
	// oneTimeWaitSource is the intake source name panewire anchors needs under.
	oneTimeWaitSource = "panewire"
	// oneTimeWaitLane is the desk lane the anchors land on; handoffkeep has no
	// "operator" lane, and operator-desk is the lane the desk's lane check
	// watches.
	oneTimeWaitLane = "operator-desk"
	// oneTimeWaitRefPrefix starts every anchor's source_ref; the need key
	// follows it so the dedupe identity is (machine, kind, need).
	oneTimeWaitRefPrefix = "one-time-action/"
	// oneTimeWaitOperator is the wait kind the desk lists as needs_operator.
	oneTimeWaitOperator = "operator"
	// oneTimeWaitAnchorHops bounds the intakes one pass makes along the settled
	// anchor chain. The walk's position is stored, so a longer chain finishes on
	// later passes rather than never.
	oneTimeWaitAnchorHops = 8
	// oneTimeWaitRetry is how long a failed write waits before it is tried again
	// without a new report.
	oneTimeWaitRetry = 30 * time.Second
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

// errOneTimeRowGone is a pass that found its need's row already dropped: the
// need has nothing left to keep, so the pass stops without a write.
var errOneTimeRowGone = errors.New("one-time action row is gone")

// oneTimeWaitItem is one queued need, or a barrier the worker closes once
// everything queued before it has been reconciled (tests settle on it).
type oneTimeWaitItem struct {
	key  oneTimeActionKey
	done chan struct{}
}

// oneTimeWaitQueue feeds the wait worker. pending coalesces repeat requests for
// one need while it waits in the queue. The worker starts on the first request
// and exits when closeOneTimeWaits stops it.
type oneTimeWaitQueue struct {
	mu      sync.Mutex
	queue   []oneTimeWaitItem
	pending map[oneTimeActionKey]bool
	closed  bool
	wake    chan struct{} // capacity 1: the queue is non-empty
	done    chan struct{} // closed when the worker has exited; nil before start
	stop    context.CancelFunc
}

// enqueueOneTimeWaits asks the wait worker to reconcile each need. It returns at
// once. A request after close is dropped.
func (h *HubServer) enqueueOneTimeWaits(keys ...oneTimeActionKey) {
	h.addOneTimeWaitItems(keys, nil)
}

func (h *HubServer) addOneTimeWaitItems(keys []oneTimeActionKey, barrier chan struct{}) {
	q := &h.oneTimeWaitQueue
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		if barrier != nil {
			close(barrier)
		}
		return
	}
	if q.wake == nil {
		h.startOneTimeWaitWorkerLocked(q)
	}
	if q.pending == nil {
		q.pending = map[oneTimeActionKey]bool{}
	}
	for _, key := range keys {
		if q.pending[key] {
			continue
		}
		q.pending[key] = true
		q.queue = append(q.queue, oneTimeWaitItem{key: key})
	}
	if barrier != nil {
		q.queue = append(q.queue, oneTimeWaitItem{done: barrier})
	}
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// startOneTimeWaitWorkerLocked starts the wait worker under the hub's shutdown
// context. The caller holds q.mu.
func (h *HubServer) startOneTimeWaitWorkerLocked(q *oneTimeWaitQueue) {
	base := h.closeCtx
	if base == nil {
		base = context.Background()
	}
	ctx, stop := context.WithCancel(base)
	q.wake = make(chan struct{}, 1)
	q.done = make(chan struct{})
	q.stop = stop
	go h.runOneTimeWaitWorker(ctx, q, q.wake, q.done)
}

// closeOneTimeWaits stops accepting requests, cancels the worker's handoffkeep
// calls and joins it. Close calls it after closeOneTimeNotices and before the
// store closes, because the worker's last writes go to the store.
func (h *HubServer) closeOneTimeWaits() {
	q := &h.oneTimeWaitQueue
	q.mu.Lock()
	q.closed = true
	done, stop := q.done, q.stop
	q.mu.Unlock()
	if stop != nil {
		stop()
	}
	if done != nil {
		<-done
	}
}

// runOneTimeWaitWorker reconciles queued needs in order until ctx is done.
func (h *HubServer) runOneTimeWaitWorker(ctx context.Context, q *oneTimeWaitQueue, wake <-chan struct{}, done chan struct{}) {
	defer close(done)
	for {
		select {
		case <-ctx.Done():
			q.mu.Lock()
			rest := q.queue
			q.queue = nil
			q.mu.Unlock()
			releaseOneTimeWaitBarriers(rest)
			return
		case <-wake:
		}
		q.mu.Lock()
		batch := q.queue
		q.queue = nil
		q.mu.Unlock()
		for i, item := range batch {
			if item.done != nil {
				close(item.done)
				continue
			}
			q.mu.Lock()
			delete(q.pending, item.key)
			q.mu.Unlock()
			if ctx.Err() != nil {
				releaseOneTimeWaitBarriers(batch[i:])
				return
			}
			if err := h.reconcileOneTimeWait(ctx, item.key); err != nil {
				h.logOneTimeWait(item.key, err)
				h.retryOneTimeWaitLater(item.key)
			}
		}
	}
}

func releaseOneTimeWaitBarriers(items []oneTimeWaitItem) {
	for _, item := range items {
		if item.done != nil {
			close(item.done)
		}
	}
}

// retryOneTimeWaitLater asks for key again after oneTimeWaitRetry. Once the
// queue is closed the request is dropped, so a timer never outlives the hub.
func (h *HubServer) retryOneTimeWaitLater(key oneTimeActionKey) {
	time.AfterFunc(oneTimeWaitRetry, func() { h.enqueueOneTimeWaits(key) })
}

// reconcileOneTimeWait brings one need's handoffkeep wait to what its row says:
// closed once the clear is confirmed, open while the need stands. Without a
// hub store it does nothing, because the row that would make a wait durable is
// not kept. Without a handoffkeep client it does nothing and the row keeps its
// wait ids, so the close is still owed when the client returns.
func (h *HubServer) reconcileOneTimeWait(ctx context.Context, key oneTimeActionKey) error {
	if h.inbox == nil || h.handoffkeep == nil {
		return nil
	}
	state, ok := h.oneTimeStateOf(key)
	if !ok {
		return nil
	}
	var err error
	switch {
	case !state.ConfirmedAt.IsZero():
		err = h.closeOneTimeWait(ctx, key)
	case state.GoneSince.IsZero() && (!state.ToldAt.IsZero() || state.holdsWait()):
		err = h.openOneTimeWait(ctx, key)
	}
	if errors.Is(err, errOneTimeRowGone) {
		return nil
	}
	return err
}

// openOneTimeWait ensures the standing need's anchor holds one open operator
// wait and records it on the row. The order is the contract: move the walk to
// the live anchor, clear a wait on any other anchor, write the anchor mark,
// write the wait, then record its id. A pass that fails at any step leaves the
// row in a state the next pass resumes from.
func (h *HubServer) openOneTimeWait(ctx context.Context, key oneTimeActionKey) error {
	action := oneTimeActionFor(key.kind, key.need)
	if action == "" {
		return nil
	}
	anchor, err := h.walkOneTimeAnchor(ctx, key, action)
	if err != nil {
		return err
	}
	state, ok := h.oneTimeStateOf(key)
	if !ok {
		return errOneTimeRowGone
	}
	if state.WaitTaskID != 0 && state.WaitTaskID != anchor.ID {
		// The recorded wait sits on a settled anchor. Clear it before the new
		// anchor's wait opens: a failed clear returns here and keeps the old ids.
		if err := h.clearOneTimeWaitOn(ctx, key, action, state.WaitTaskID, state.WaitID, "panewire: one-time action anchor superseded"); err != nil {
			return err
		}
	}
	// Write-ahead: the anchor is durable before the wait write, so an ambiguous
	// write is found again on the row.
	applied, err := h.mutateOneTime(key, func(s *oneTimeActionState) {
		if s.WaitTaskID != anchor.ID {
			s.WaitTaskID = anchor.ID
			s.WaitID = 0
		}
	})
	if err != nil {
		return err
	}
	if !applied {
		return errOneTimeRowGone
	}
	waitID, err := h.handoffkeep.addTaskWait(ctx, anchor.ID, oneTimeWaitOperator, "", oneTimeWaitReason(key, action))
	if err != nil {
		if handoffkeepRefused(err) {
			// hk committed nothing on this anchor: the mark goes.
			if rerr := h.releaseOneTimeWait(key, anchor.ID); rerr != nil {
				h.logOneTimeWait(key, rerr)
			}
		}
		return err
	}
	// The confirmed id is saved on every pass, including one where it matches
	// memory: the store may have lost the earlier write.
	_, err = h.mutateOneTime(key, func(s *oneTimeActionState) {
		s.WaitTaskID = anchor.ID
		s.WaitID = waitID
	})
	return err
}

// closeOneTimeWait clears the need's wait once the clear is confirmed and drops
// the row when nothing is left (oneTimeRowKept). A row that holds no wait has
// nothing to close, and no wait is created to find one: a wait is only ever
// opened by openOneTimeWait.
func (h *HubServer) closeOneTimeWait(ctx context.Context, key oneTimeActionKey) error {
	state, ok := h.oneTimeStateOf(key)
	if !ok {
		return errOneTimeRowGone
	}
	if state.holdsWait() {
		action := oneTimeActionFor(key.kind, key.need)
		if err := h.clearOneTimeWaitOn(ctx, key, action, state.WaitTaskID, state.WaitID, "panewire: one-time action cleared"); err != nil {
			return err
		}
	}
	_, err := h.mutateOneTime(key, func(*oneTimeActionState) {})
	return err
}

// clearOneTimeWaitOn ends the wait on taskID. When the wait id was never
// confirmed, the same deduped write names it first: a 4xx refusal means no wait
// was ever written there, so the mark just goes; a 5xx or transport error keeps
// the mark for the next pass. A wait or task already gone counts as done.
func (h *HubServer) clearOneTimeWaitOn(ctx context.Context, key oneTimeActionKey, action string, taskID, waitID int64, note string) error {
	if waitID == 0 {
		id, err := h.handoffkeep.addTaskWait(ctx, taskID, oneTimeWaitOperator, "", oneTimeWaitReason(key, action))
		switch {
		case err == nil:
			waitID = id
		case handoffkeepRefused(err):
			return h.releaseOneTimeWait(key, taskID)
		default:
			return err
		}
	}
	err := h.handoffkeep.clearTaskWait(ctx, taskID, waitID, note)
	if err != nil && !errors.Is(err, errHandoffkeepWaitGone) {
		return err
	}
	return h.releaseOneTimeWait(key, taskID)
}

// releaseOneTimeWait forgets the wait the row holds on taskID, once it is known
// closed or never written, and saves the row (which drops it when nothing is
// left).
func (h *HubServer) releaseOneTimeWait(key oneTimeActionKey, taskID int64) error {
	_, err := h.mutateOneTime(key, func(s *oneTimeActionState) {
		if s.WaitTaskID == taskID {
			s.WaitTaskID = 0
			s.WaitID = 0
		}
	})
	return err
}

// walkOneTimeAnchor resolves the need's live anchor. It intakes the stored
// position and, while hk answers a settled task, follows the "/d<id>" chain one
// step at a time, storing each step before the next intake. A pass stops after
// oneTimeWaitAnchorHops intakes; the next pass resumes from the stored position.
func (h *HubServer) walkOneTimeAnchor(ctx context.Context, key oneTimeActionKey, action string) (handoffkeepTaskRef, error) {
	state, ok := h.oneTimeStateOf(key)
	if !ok {
		return handoffkeepTaskRef{}, errOneTimeRowGone
	}
	ref := state.AnchorRef
	if ref == "" {
		ref = oneTimeWaitSourceRef(key)
	}
	for hop := 0; hop < oneTimeWaitAnchorHops; hop++ {
		task, err := h.handoffkeep.intakeTask(ctx, oneTimeWaitSource, ref, oneTimeWaitTitle(key, action), oneTimeWaitBody(key, action), oneTimeWaitLane)
		if err != nil {
			return handoffkeepTaskRef{}, err
		}
		if !oneTimeWaitDeadAnchorStates[task.State] {
			if err := h.setOneTimeAnchorRef(key, ref); err != nil {
				return handoffkeepTaskRef{}, err
			}
			return task, nil
		}
		ref = ref + "/d" + strconv.FormatInt(task.ID, 10)
		if err := h.setOneTimeAnchorRef(key, ref); err != nil {
			return handoffkeepTaskRef{}, err
		}
	}
	return handoffkeepTaskRef{}, errors.New("handoffkeep kept answering settled one-time action anchors; the walk resumes on the next pass")
}

// setOneTimeAnchorRef stores the walk's position when it moved.
func (h *HubServer) setOneTimeAnchorRef(key oneTimeActionKey, ref string) error {
	state, ok := h.oneTimeStateOf(key)
	if !ok {
		return errOneTimeRowGone
	}
	if state.AnchorRef == ref {
		return nil
	}
	applied, err := h.mutateOneTime(key, func(s *oneTimeActionState) { s.AnchorRef = ref })
	if err != nil {
		return err
	}
	if !applied {
		return errOneTimeRowGone
	}
	return nil
}

// oneTimeStateOf reads one need's row.
func (h *HubServer) oneTimeStateOf(key oneTimeActionKey) (oneTimeActionState, bool) {
	h.oneTimeMu.Lock()
	defer h.oneTimeMu.Unlock()
	state, ok := h.oneTimeActions[key]
	return state, ok
}

// mutateOneTime applies change to one need's row and saves it. It reports
// whether the row was there; a row that is gone is left gone. The save may
// return a store error, and then the caller stops before any write that needed
// it.
func (h *HubServer) mutateOneTime(key oneTimeActionKey, change func(*oneTimeActionState)) (bool, error) {
	h.oneTimeMu.Lock()
	defer h.oneTimeMu.Unlock()
	state, ok := h.oneTimeActions[key]
	if !ok {
		return false, nil
	}
	change(&state)
	return true, h.saveOneTimeLocked(key, state)
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

func (h *HubServer) logOneTimeWait(key oneTimeActionKey, err error) {
	h.logger.Warn("one-time action operator wait is not settled; it is retried", "host", key.machine, "kind", key.kind, "need", key.need, "error", err)
}

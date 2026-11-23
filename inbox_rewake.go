package panewire

import "time"

// Inbox re-wake (hk 6111; operator decision 481, option A). An inbox-only lane
// gets its one-line wake when a wake-worthy item arrives (inbox_wake.go). An
// item that never wakes -- info, a quiet READY, MERGED -- or a wake that found
// the pane busy leaves the lane's unread set with nothing to announce it, and
// an idle pane can sit on it for a day. This pass announces the set again: it
// sends the same wake line (rewake=N in the fleet event) while the lane still
// has unread-and-unacked items and all of these hold:
//
//   - the pane is idle (receiverInboxState): a working pane is never woken;
//   - the lane's coalescing window is closed and no line is owed;
//   - relay.inbox_rewake_interval (default 30m) has passed since the lane's
//     last wake line of any reason, or since the set was first seen, whichever
//     is later;
//   - fewer than relay.inbox_rewake_max (default 3) re-wake lines went out for
//     the set.
//
// A new unread item starts a new set, which resets the count. An ack that
// leaves nothing unread sends nothing. No item body is typed: only the wake
// line, as inbox_wake.go does (docs/quiet-delivery.md#the-re-wake).
//
// The bookkeeping is in memory (inboxWakes.rewake). A hub restart forgets each
// lane's set and last line, so a lane may get up to relay.inbox_rewake_max
// more re-wakes after a restart; the startup seed still announces what is
// unread above info. Persisting the count would need a store table, and the
// fleet event lines are the record hk 5509 counts.

const inboxRewakeScanEvery = 10 * time.Second

// inboxRewake is one inbox-only lane's re-wake state.
type inboxRewake struct {
	// lastLine is the lane's last wake line of any reason, or the time the
	// current unread set was first seen when that is later.
	lastLine time.Time
	// setMaxID is the newest unread item the current set has seen. A newer
	// unread item (a higher id) starts a new set.
	setMaxID int64
	// lines counts the re-wake lines sent for the current set.
	lines int
}

// rewakeFor returns the lane's re-wake state, creating it. The caller holds
// the wake mutex.
func (w *inboxWakes) rewakeFor(lane string) *inboxRewake {
	if w.rewake == nil {
		w.rewake = map[string]*inboxRewake{}
	}
	st := w.rewake[lane]
	if st == nil {
		st = &inboxRewake{}
		w.rewake[lane] = st
	}
	return st
}

// noteInboxLine records a wake line of any reason, the start of the interval
// before the next re-wake. The caller holds the wake mutex.
func (h *HubServer) noteInboxLine(lane string, now time.Time) {
	h.inbox.wakes.rewakeFor(lane).lastLine = now
}

// rewakeInboxLanes is the re-wake pass runInbox calls after flushInboxWakes.
// It reads the store at most once per inboxRewakeScanEvery: the scan lists the
// lanes and reads two counts per inbox-only lane with unread items, while a
// re-wake is due in tens of minutes.
func (h *HubServer) rewakeInboxLanes() {
	if h.inbox == nil {
		return
	}
	w := &h.inbox.wakes
	w.mu.Lock()
	defer w.mu.Unlock()
	now := h.now()
	if now.Before(w.nextRewakeScan) {
		return
	}
	w.nextRewakeScan = now.Add(inboxRewakeScanEvery)
	lanes, err := h.inbox.store.laneNames()
	if err != nil {
		h.logger.Warn("inbox re-wake skipped: lanes unreadable", "error", h.inbox.store.loggable("inbox rewake", err))
		return
	}
	for _, lane := range lanes {
		if class := h.inbox.policy.inboxOnlyClass(lane, now); class != "" {
			h.rewakeLane(lane, class, now)
		}
	}
}

// rewakeLane sends the lane's next re-wake line when its unread set is due.
// A failed read sends nothing: a count that cannot be read is not "0 unread",
// and a set that cannot be read is not "empty". The caller holds the wake mutex.
func (h *HubServer) rewakeLane(lane, class string, now time.Time) {
	counts, err := h.inbox.store.counts(lane)
	if err != nil {
		h.logger.Warn("inbox re-wake skipped: unread count unavailable", "lane", lane, "error", h.inbox.store.loggable("inbox rewake", err))
		return
	}
	if counts[lane] == 0 {
		return
	}
	items, err := h.inbox.store.list(lane, true)
	if err != nil {
		h.logger.Warn("inbox re-wake skipped: unread items unreadable", "lane", lane, "error", h.inbox.store.loggable("inbox rewake", err))
		return
	}
	if len(items) == 0 {
		return
	}
	policy := h.inbox.policy.Relay
	st := h.inbox.wakes.rewakeFor(lane)
	// The list is newest first: an id above the set's newest is an arrival.
	if newest := items[0].ID; newest > st.setMaxID {
		st.setMaxID, st.lines = newest, 0
		if now.After(st.lastLine) {
			st.lastLine = now
		}
	}
	if st.lines >= policy.rewakeMax || now.Sub(st.lastLine) < policy.rewakeInterval {
		return
	}
	state := h.inbox.wakes.lanes[lane]
	if state != nil && (state.pending || now.Before(state.windowEnd)) {
		// An open window or an owed line is announced by flushInboxWakes.
		return
	}
	if !h.receiverInboxState(lane).idle {
		return
	}
	if state == nil {
		if h.inbox.wakes.lanes == nil {
			h.inbox.wakes.lanes = map[string]*inboxWakeLane{}
		}
		state = &inboxWakeLane{}
		h.inbox.wakes.lanes[lane] = state
	}
	state.class, state.startup = class, false
	line := st.lines + 1
	if h.attemptInboxWake(lane, state, now, line) == inboxWakeSent {
		st.lines = line
	}
}

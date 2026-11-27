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
// A new unread item starts a new set, which resets the count, and so does a
// completion merged into a READY, which adds no item (restartInboxRewake). An
// ack that leaves nothing unread sends nothing. No item body is typed: only the
// wake line, as inbox_wake.go does (docs/quiet-delivery.md#the-re-wake).
//
// The pass reads the store only for what changed (hk 6257). An unread set
// changes only when a row of its lane is written, so the pass keeps, per lane,
// whether it has unread items and the newest unread id, with the store's
// change version at the read (inbox_change_signal.go), and reads the lane again
// only when that version moved. The lane list is read again only when a write
// may have added or removed a lane. Every inboxRewakeHealEvery the pass reads
// both regardless, to heal a writer that missed its bump. Every time rule is
// still evaluated on the cached state at each scan, so a re-wake falls due at
// the same scan as when every scan read the store.
//
// The bookkeeping is in memory (inboxWakes.rewake). A hub restart forgets each
// lane's set and last line, so a lane may get up to relay.inbox_rewake_max
// more re-wakes after a restart; the startup seed still announces what is
// unread above info. Persisting the count would need a store table, and the
// fleet event lines are the record hk 5509 counts.

const inboxRewakeScanEvery = 10 * time.Second

// inboxRewakeHealEvery is how often the pass reads the lane list and every
// lane it would otherwise skip, whatever the change signal says. It only heals
// a writer that missed its noteInboxChanged call; it is not the cadence, and
// every correct writer is read at the next scan by the signal. Like
// inboxBatchPageFallback for the batch page, it bounds how long a missed bump
// can hide an unread set.
const inboxRewakeHealEvery = 5 * time.Minute

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
	// read is set once the lane's unread state below was read from the store,
	// at store version readVer. unread and newest are that read's result: the
	// lane had unread items, and the newest one's id.
	read    bool
	readVer uint64
	unread  bool
	newest  int64
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

// restartInboxRewake starts the count of the lane's unread set again for news
// that added no item: a completion merged into a READY (result_identity.go),
// whose own item would have started a new set.
func (h *HubServer) restartInboxRewake(lane string) {
	w := &h.inbox.wakes
	w.mu.Lock()
	defer w.mu.Unlock()
	st := w.rewakeFor(lane)
	st.lines = 0
	if now := h.now(); now.After(st.lastLine) {
		st.lastLine = now
	}
}

// rewakeInboxLanes is the re-wake pass runInbox calls after flushInboxWakes.
// Every inboxRewakeScanEvery it evaluates each inbox-only lane's rules. It
// reads the store only when a write may have changed what it knows: the lane
// list when a lane may have appeared or gone, a lane's unread state when a row
// of that lane was written (see the header).
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
	// The version is taken before the read: a write that lands in between is
	// read now and costs one more read at the next scan, never a missed one.
	// heal re-reads everything this scan; the first scan reads everything
	// because nothing is cached yet.
	heal := !w.rewakeLanesRead || !now.Before(w.rewakeHealAt)
	if version := h.inbox.store.inboxMembersVersion(); heal || version != w.rewakeLanesVersion {
		w.noteRewakeRead("lanes", "")
		lanes, err := h.inbox.store.laneNames()
		if err != nil {
			h.logger.Warn("inbox re-wake skipped: lanes unreadable", "error", h.inbox.store.loggable("inbox rewake", err))
			return
		}
		w.rewakeLanes, w.rewakeLanesVersion, w.rewakeLanesRead = lanes, version, true
	}
	if heal {
		w.rewakeHealAt = now.Add(inboxRewakeHealEvery)
	}
	for _, lane := range w.rewakeLanes {
		if class := h.inbox.policy.inboxOnlyClass(lane, now); class != "" {
			h.rewakeLane(lane, class, now, heal)
		}
	}
}

// noteRewakeRead reports a store read of the pass to a test; it is nil in
// production. what is "lanes" or "unread".
func (w *inboxWakes) noteRewakeRead(what, lane string) {
	if w.rewakeRead != nil {
		w.rewakeRead(what, lane)
	}
}

// refreshRewakeUnread brings st's unread state up to date: it reads the lane
// only if the store's version for it moved since the last read. A failed read
// leaves the state as it was, so the next scan reads again, and reports the
// failure as false; force reads regardless of the version (the heal scan).
// The caller holds the wake mutex.
func (h *HubServer) refreshRewakeUnread(lane string, st *inboxRewake, force bool) bool {
	store := h.inbox.store
	version := store.inboxLaneVersion(lane)
	if !force && st.read && st.readVer == version {
		return true
	}
	h.inbox.wakes.noteRewakeRead("unread", lane)
	counts, err := store.counts(lane)
	if err != nil {
		h.logger.Warn("inbox re-wake skipped: unread count unavailable", "lane", lane, "error", store.loggable("inbox rewake", err))
		return false
	}
	unread, newest := false, int64(0)
	if counts[lane] > 0 {
		items, err := store.list(lane, true)
		if err != nil {
			h.logger.Warn("inbox re-wake skipped: unread items unreadable", "lane", lane, "error", store.loggable("inbox rewake", err))
			return false
		}
		// The list is newest first.
		if len(items) > 0 {
			unread, newest = true, items[0].ID
		}
	}
	st.read, st.readVer, st.unread, st.newest = true, version, unread, newest
	return true
}

// rewakeLane sends the lane's next re-wake line when its unread set is due.
// A failed read sends nothing: a count that cannot be read is not "0 unread",
// and a set that cannot be read is not "empty". The caller holds the wake mutex.
func (h *HubServer) rewakeLane(lane, class string, now time.Time, heal bool) {
	st := h.inbox.wakes.rewakeFor(lane)
	if !h.refreshRewakeUnread(lane, st, heal) || !st.unread {
		return
	}
	policy := h.inbox.policy.Relay
	// An id above the set's newest is an arrival.
	if st.newest > st.setMaxID {
		st.setMaxID, st.lines = st.newest, 0
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

package panewire

import (
	"context"
	"time"
)

// oneTimeActionSchema is also installed by Postgres migrations 0026 and 0034 (MySQL 0019 and 0027).
// The milliseconds are UTC; 0 means unset: told_ms is 0 while a recurrence is
// untold, gone_ms is 0 while the need shows, confirmed_ms is 0 until anti-flap
// clearing confirms the need gone. wait_task_id and wait_id name the
// handoffkeep anchor task and operator wait (hub_one_time_wait.go); anchor_ref
// is where the anchor walk last reached.
const oneTimeActionSchema = `CREATE TABLE IF NOT EXISTS one_time_action_notices (
 machine TEXT NOT NULL, kind TEXT NOT NULL, need TEXT NOT NULL,
 told_ms BIGINT NOT NULL, gone_ms BIGINT NOT NULL DEFAULT 0,
 wait_task_id BIGINT NOT NULL DEFAULT 0, wait_id BIGINT NOT NULL DEFAULT 0,
 confirmed_ms BIGINT NOT NULL DEFAULT 0, anchor_ref TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(machine, kind, need))`

const oneTimeActionStoreTimeout = 5 * time.Second

// saveOneTimeLocked writes one need's state through to the hub store, or drops
// its row once nothing is left to remember (oneTimeRowKept). It returns the
// store error. The in-memory state is kept either way, so the next pass retries
// the same write; a caller that must not act before the write lands (the wait's
// write-ahead anchor) stops on the error. The caller holds oneTimeMu.
func (h *HubServer) saveOneTimeLocked(key oneTimeActionKey, state oneTimeActionState) error {
	if h.oneTimeActions == nil {
		h.oneTimeActions = map[oneTimeActionKey]oneTimeActionState{}
	}
	defer h.refreshLoginNeedsLocked()
	if !oneTimeRowKept(key, state) {
		if err := h.deleteOneTimeActionLocked(key); err != nil {
			h.oneTimeActions[key] = state
			return err
		}
		delete(h.oneTimeActions, key)
		return nil
	}
	h.oneTimeActions[key] = state
	return h.putOneTimeActionLocked(key, state)
}

// putOneTimeActionLocked writes one need's row through to the hub store. The
// caller holds oneTimeMu, so writes land in the order the transitions happened.
func (h *HubServer) putOneTimeActionLocked(key oneTimeActionKey, state oneTimeActionState) error {
	if h.inbox == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), oneTimeActionStoreTimeout)
	defer cancel()
	return h.inbox.store.putOneTimeAction(ctx, key, state)
}

// deleteOneTimeActionLocked drops a need's row from the hub store. The caller
// holds oneTimeMu.
func (h *HubServer) deleteOneTimeActionLocked(key oneTimeActionKey) error {
	if h.inbox == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), oneTimeActionStoreTimeout)
	defer cancel()
	return h.inbox.store.deleteOneTimeAction(ctx, key)
}

// loadOneTimeActions reads the rows back from the hub store at start, so a
// standing need is not told again after a restart, and every row that still
// holds a wait or a confirmed clear is handed to the wait worker, so that
// close is owed even when no readiness report comes. A read that fails leaves
// the map empty and is logged.
func (h *HubServer) loadOneTimeActions() {
	if h.inbox == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), oneTimeActionStoreTimeout)
	defer cancel()
	told, err := h.inbox.store.oneTimeActions(ctx)
	if err != nil {
		h.logger.Warn("one-time action notices could not be read; a standing need may be told again", "error", err)
		return
	}
	var owed []oneTimeActionKey
	h.oneTimeMu.Lock()
	h.oneTimeActions = told
	h.refreshLoginNeedsLocked()
	for key, state := range told {
		if state.holdsWait() || !state.ConfirmedAt.IsZero() {
			owed = append(owed, key)
		}
	}
	h.oneTimeMu.Unlock()
	h.enqueueOneTimeWaits(owed...)
}

func (s *laneInboxStore) putOneTimeAction(parent context.Context, key oneTimeActionKey, state oneTimeActionState) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(s.dialect.upsert(`INSERT INTO one_time_action_notices(machine,kind,need,told_ms,gone_ms,wait_task_id,wait_id,confirmed_ms,anchor_ref) VALUES(?,?,?,?,?,?,?,?,?)`, "machine,kind,need", s.dialect.assignIncoming("told_ms", "gone_ms", "wait_task_id", "wait_id", "confirmed_ms", "anchor_ref"))),
		key.machine, key.kind, key.need, milliOrZero(state.ToldAt), milliOrZero(state.GoneSince), state.WaitTaskID, state.WaitID, milliOrZero(state.ConfirmedAt), state.AnchorRef)
	return err
}

func (s *laneInboxStore) deleteOneTimeAction(parent context.Context, key oneTimeActionKey) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`DELETE FROM one_time_action_notices WHERE machine=? AND kind=? AND need=?`), key.machine, key.kind, key.need)
	return err
}

// oneTimeActions reads every stored need row.
func (s *laneInboxStore) oneTimeActions(parent context.Context) (map[oneTimeActionKey]oneTimeActionState, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT machine,kind,need,told_ms,gone_ms,wait_task_id,wait_id,confirmed_ms,anchor_ref FROM one_time_action_notices`))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	told := map[oneTimeActionKey]oneTimeActionState{}
	for rows.Next() {
		var key oneTimeActionKey
		var state oneTimeActionState
		var toldMS, goneMS, confirmedMS int64
		if err := rows.Scan(&key.machine, &key.kind, &key.need, &toldMS, &goneMS, &state.WaitTaskID, &state.WaitID, &confirmedMS, &state.AnchorRef); err != nil {
			return nil, err
		}
		state.ToldAt = timeOrZero(toldMS)
		state.GoneSince = timeOrZero(goneMS)
		state.ConfirmedAt = timeOrZero(confirmedMS)
		told[key] = state
	}
	return told, rows.Err()
}

// milliOrZero is the stored form of a timestamp: 0 for unset.
func milliOrZero(at time.Time) int64 {
	if at.IsZero() {
		return 0
	}
	return at.UnixMilli()
}

// timeOrZero is the reverse of milliOrZero.
func timeOrZero(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

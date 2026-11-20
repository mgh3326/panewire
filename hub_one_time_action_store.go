package panewire

import (
	"context"
	"time"
)

// oneTimeActionSchema is also installed by Postgres migrations 0026 and 0034 (MySQL 0019 and 0027).
// The milliseconds are UTC; gone_ms is 0 while the need shows. wait_task_id
// and wait_id name the handoffkeep anchor task and operator wait; both are 0
// until the wait write is confirmed (hub_one_time_wait.go). confirmed_ms is
// when anti-flap clearing confirmed the need gone; the row then only carries
// the wait-close obligation.
const oneTimeActionSchema = `CREATE TABLE IF NOT EXISTS one_time_action_notices (
 machine TEXT NOT NULL, kind TEXT NOT NULL, need TEXT NOT NULL,
 told_ms BIGINT NOT NULL, gone_ms BIGINT NOT NULL DEFAULT 0,
 wait_task_id BIGINT NOT NULL DEFAULT 0, wait_id BIGINT NOT NULL DEFAULT 0,
 confirmed_ms BIGINT NOT NULL DEFAULT 0,
 PRIMARY KEY(machine, kind, need))`

const oneTimeActionStoreTimeout = 5 * time.Second

// putOneTimeActionLocked writes one told need's state through to the hub
// store. A write that fails is logged and kept in memory: the process still
// tells each need once, and only a restart can tell it again. The caller holds
// oneTimeMu, so writes land in the order the transitions happened.
func (h *HubServer) putOneTimeActionLocked(key oneTimeActionKey, state oneTimeActionState) {
	if h.inbox == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), oneTimeActionStoreTimeout)
	defer cancel()
	if err := h.inbox.store.putOneTimeAction(ctx, key, state); err != nil {
		h.logger.Warn("one-time action notice could not be stored", "host", key.machine, "kind", key.kind, "error", err)
	}
}

// deleteOneTimeActionLocked drops a cleared need from the hub store. The caller
// holds oneTimeMu.
func (h *HubServer) deleteOneTimeActionLocked(key oneTimeActionKey) {
	if h.inbox == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), oneTimeActionStoreTimeout)
	defer cancel()
	if err := h.inbox.store.deleteOneTimeAction(ctx, key); err != nil {
		h.logger.Warn("one-time action clear could not be stored", "host", key.machine, "kind", key.kind, "error", err)
	}
}

// loadOneTimeActions reads the told needs back from the hub store at start, so
// a standing need is not told again after a restart. A read that fails leaves
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
	h.oneTimeMu.Lock()
	h.oneTimeActions = told
	h.oneTimeMu.Unlock()
}

func (s *laneInboxStore) putOneTimeAction(parent context.Context, key oneTimeActionKey, state oneTimeActionState) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(s.dialect.upsert(`INSERT INTO one_time_action_notices(machine,kind,need,told_ms,gone_ms,wait_task_id,wait_id,confirmed_ms) VALUES(?,?,?,?,?,?,?,?)`, "machine,kind,need", s.dialect.assignIncoming("told_ms", "gone_ms", "wait_task_id", "wait_id", "confirmed_ms"))),
		key.machine, key.kind, key.need, state.ToldAt.UnixMilli(), goneMilli(state.GoneSince), state.WaitTaskID, state.WaitID, confirmedMilli(state.ConfirmedAt))
	return err
}

func (s *laneInboxStore) deleteOneTimeAction(parent context.Context, key oneTimeActionKey) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`DELETE FROM one_time_action_notices WHERE machine=? AND kind=? AND need=?`), key.machine, key.kind, key.need)
	return err
}

// oneTimeActions reads every stored told need.
func (s *laneInboxStore) oneTimeActions(parent context.Context) (map[oneTimeActionKey]oneTimeActionState, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT machine,kind,need,told_ms,gone_ms,wait_task_id,wait_id,confirmed_ms FROM one_time_action_notices`))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	told := map[oneTimeActionKey]oneTimeActionState{}
	for rows.Next() {
		var key oneTimeActionKey
		var state oneTimeActionState
		var toldMS, goneMS, confirmedMS int64
		if err := rows.Scan(&key.machine, &key.kind, &key.need, &toldMS, &goneMS, &state.WaitTaskID, &state.WaitID, &confirmedMS); err != nil {
			return nil, err
		}
		state.ToldAt = time.UnixMilli(toldMS).UTC()
		if goneMS != 0 {
			state.GoneSince = time.UnixMilli(goneMS).UTC()
		}
		if confirmedMS != 0 {
			state.ConfirmedAt = time.UnixMilli(confirmedMS).UTC()
		}
		told[key] = state
	}
	return told, rows.Err()
}

func goneMilli(gone time.Time) int64 {
	if gone.IsZero() {
		return 0
	}
	return gone.UnixMilli()
}

func confirmedMilli(confirmed time.Time) int64 {
	if confirmed.IsZero() {
		return 0
	}
	return confirmed.UnixMilli()
}

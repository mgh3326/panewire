package panewire

// The durable owner-notice decision table (hk 5360, packet
// handoff/2026-11-19/panewire-design-hk5360-root-cause). One row per decided
// notice, keyed by its event key: the outbox event id for a spawn notice, the
// "job:" event id for a withheld completion, the "unconfirmed:" relay row id for
// a withheld delivery-unconfirmed notice. The primary key makes the first
// decision the only one; the count rows are the daily spawn_notices group and
// the repeat window, so nothing about counts lives in memory.

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// ownerNoticeDecisionsTable creates the decision table on the sqlite hub store
// (the postgres copy is migration 0032).
const ownerNoticeDecisionsTable = `CREATE TABLE IF NOT EXISTS owner_notice_decisions (event_key TEXT PRIMARY KEY,lane TEXT NOT NULL,cause TEXT NOT NULL,decision TEXT NOT NULL,day TEXT NOT NULL,at_ms BIGINT NOT NULL)`

// ownerNoticeDecisionsWindowIndex serves the repeat window and the day's counts.
const ownerNoticeDecisionsWindowIndex = `CREATE INDEX IF NOT EXISTS owner_notice_decisions_window ON owner_notice_decisions(lane,cause,decision,at_ms)`

// ownerNoticeDecision returns the recorded decision for key, if one exists.
func (s *laneInboxStore) ownerNoticeDecision(parent context.Context, key string) (string, bool, error) {
	op, done := s.op(parent)
	defer done()
	var decision string
	err := s.db.QueryRowContext(op, s.q(`SELECT decision FROM owner_notice_decisions WHERE event_key=?`), key).Scan(&decision)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return decision, true, nil
}

// recordOwnerNoticeDecision stores decision for key, unless key already has one,
// and returns the decision that is stored. The first writer's decision is the
// one every routing applies.
func (s *laneInboxStore) recordOwnerNoticeDecision(parent context.Context, key, lane, cause, decision string, now time.Time) (string, error) {
	op, done := s.op(parent)
	defer done()
	if _, err := s.db.ExecContext(op, s.q(`INSERT INTO owner_notice_decisions(event_key,lane,cause,decision,day,at_ms) VALUES(?,?,?,?,?,?) ON CONFLICT DO NOTHING`), key, lane, cause, decision, loopDateOf(now), now.UnixMilli()); err != nil {
		return "", err
	}
	var stored string
	if err := s.db.QueryRowContext(op, s.q(`SELECT decision FROM owner_notice_decisions WHERE event_key=?`), key).Scan(&stored); err != nil {
		return "", err
	}
	return stored, nil
}

// ownerNoticeRepeatDue reports whether the next count for (lane, cause) is the
// repeat: spawnNoticeRepeatCount-1 counts already fall in the window, counting
// from the last repeat when it is inside the window.
func (s *laneInboxStore) ownerNoticeRepeatDue(parent context.Context, lane, cause string, now time.Time) (bool, error) {
	op, done := s.op(parent)
	defer done()
	var lastRepeat int64
	if err := s.db.QueryRowContext(op, s.q(`SELECT COALESCE(MAX(at_ms),0) FROM owner_notice_decisions WHERE lane=? AND cause=? AND decision=?`), lane, cause, ownerDecisionRepeat).Scan(&lastRepeat); err != nil {
		return false, err
	}
	since := now.Add(-spawnNoticeRepeatWindow).UnixMilli()
	if lastRepeat > since {
		since = lastRepeat
	}
	var counted int64
	if err := s.db.QueryRowContext(op, s.q(`SELECT COUNT(*) FROM owner_notice_decisions WHERE lane=? AND cause=? AND decision=? AND at_ms>?`), lane, cause, ownerDecisionCount, since).Scan(&counted); err != nil {
		return false, err
	}
	return counted >= spawnNoticeRepeatCount-1, nil
}

// ownerNoticeCounts returns the day's count decisions by owner lane and cause:
// the daily loops row's spawn_notices group.
func (s *laneInboxStore) ownerNoticeCounts(parent context.Context, day string) ([]spawnNoticeRow, error) {
	op, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(op, s.q(`SELECT lane,cause,COUNT(*) FROM owner_notice_decisions WHERE decision=? AND day=? GROUP BY lane,cause ORDER BY lane,cause`), ownerDecisionCount, day)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []spawnNoticeRow{}
	for rows.Next() {
		var row spawnNoticeRow
		if err := rows.Scan(&row.Lane, &row.Cause, &row.Silenced); err != nil {
			return nil, err
		}
		row.Machine = "hub"
		out = append(out, row)
	}
	return out, rows.Err()
}

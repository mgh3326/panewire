package panewire

// hk 6924: the rows of the daily stale-replay notice (relay_stale_notice.go).
// One row per real message the replay gate retired as stale. The notice day is
// the UTC day of the retirement; filed is set per design lane once that lane's
// notice for the day is in its inbox (or there was nothing to list), so a
// restart mid-day keeps the rows and a closed day is filed exactly once.

import (
	"context"
)

// relayStaleNoticeSchema is SQLite's form of the table. Postgres has it in
// migration 0043 and MySQL in migration 0039; both carry the same columns.
var relayStaleNoticeSchema = []string{
	`CREATE TABLE IF NOT EXISTS relay_stale_notice_rows (
 relay_id INTEGER PRIMARY KEY, notice_day VARCHAR(32) NOT NULL, design_lane VARCHAR(255) NOT NULL,
 worker_lane VARCHAR(255) NOT NULL, job VARCHAR(255) NOT NULL DEFAULT '', task_id VARCHAR(32) NOT NULL DEFAULT '',
 kind VARCHAR(64) NOT NULL, sender VARCHAR(32) NOT NULL DEFAULT '', hk_event_id VARCHAR(255) NOT NULL DEFAULT '',
 received_ms BIGINT NOT NULL, retired_ms BIGINT NOT NULL, filed INTEGER NOT NULL DEFAULT 0)`,
	`CREATE INDEX IF NOT EXISTS relay_stale_notice_rows_day ON relay_stale_notice_rows(notice_day, design_lane, filed)`,
}

// Column widths of relay_stale_notice_rows. MySQL refuses a longer value, so
// each one is cut before the insert.
const (
	staleReplayWidthLane  = 255
	staleReplayWidthJob   = 255
	staleReplayWidthTask  = 32
	staleReplayWidthKind  = 64
	staleReplayWidthSend  = 32
	staleReplayWidthEvent = 255
)

// staleReplayRow is one retired real message as the notice reads it.
type staleReplayRow struct {
	RelayID                 int64
	NoticeDay               string
	DesignLane, WorkerLane  string
	Job, Task, Kind, Sender string
	HKEventID               string
	ReceivedMS, RetiredMS   int64
}

// insertStaleReplayRow records one retired row. A row the store already holds
// (the same handoffkeep row retired again) keeps its first record.
func (s *laneInboxStore) insertStaleReplayRow(parent context.Context, r staleReplayRow) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(s.dialect.insertIgnore(`INSERT INTO relay_stale_notice_rows(relay_id,notice_day,design_lane,worker_lane,job,task_id,kind,sender,hk_event_id,received_ms,retired_ms,filed) VALUES(?,?,?,?,?,?,?,?,?,?,?,0)`, "relay_id")),
		r.RelayID, r.NoticeDay, utf8PrefixWithin(r.DesignLane, staleReplayWidthLane), utf8PrefixWithin(r.WorkerLane, staleReplayWidthLane),
		utf8PrefixWithin(r.Job, staleReplayWidthJob), utf8PrefixWithin(r.Task, staleReplayWidthTask), utf8PrefixWithin(r.Kind, staleReplayWidthKind),
		utf8PrefixWithin(r.Sender, staleReplayWidthSend), utf8PrefixWithin(r.HKEventID, staleReplayWidthEvent), r.ReceivedMS, r.RetiredMS)
	return err
}

// staleReplayDaysBefore lists the days before day that still hold unfiled rows,
// oldest first.
func (s *laneInboxStore) staleReplayDaysBefore(parent context.Context, day string) ([]string, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT DISTINCT notice_day FROM relay_stale_notice_rows WHERE filed=0 AND notice_day<? ORDER BY notice_day`), day)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var days []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		days = append(days, d)
	}
	return days, rows.Err()
}

// staleReplayLanes lists the design lanes with unfiled rows on day.
func (s *laneInboxStore) staleReplayLanes(parent context.Context, day string) ([]string, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT DISTINCT design_lane FROM relay_stale_notice_rows WHERE notice_day=? AND filed=0 ORDER BY design_lane`), day)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var lanes []string
	for rows.Next() {
		var lane string
		if err := rows.Scan(&lane); err != nil {
			return nil, err
		}
		lanes = append(lanes, lane)
	}
	return lanes, rows.Err()
}

// staleReplayRowsOf lists the unfiled rows of one design lane on day, in
// retirement order.
func (s *laneInboxStore) staleReplayRowsOf(parent context.Context, day, lane string) ([]staleReplayRow, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT relay_id,notice_day,design_lane,worker_lane,job,task_id,kind,sender,hk_event_id,received_ms,retired_ms FROM relay_stale_notice_rows WHERE notice_day=? AND design_lane=? AND filed=0 ORDER BY retired_ms,relay_id`), day, lane)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []staleReplayRow
	for rows.Next() {
		var r staleReplayRow
		if err := rows.Scan(&r.RelayID, &r.NoticeDay, &r.DesignLane, &r.WorkerLane, &r.Job, &r.Task, &r.Kind, &r.Sender, &r.HKEventID, &r.ReceivedMS, &r.RetiredMS); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// markStaleReplayFiled closes one lane's rows for day once its notice is in the
// inbox, or once there was nothing to list.
func (s *laneInboxStore) markStaleReplayFiled(parent context.Context, day, lane string) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE relay_stale_notice_rows SET filed=1 WHERE notice_day=? AND design_lane=? AND filed=0`), day, lane)
	return err
}

// pruneStaleReplayRows deletes filed rows of days before cutoff.
func (s *laneInboxStore) pruneStaleReplayRows(parent context.Context, cutoff string) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`DELETE FROM relay_stale_notice_rows WHERE filed=1 AND notice_day<?`), cutoff)
	return err
}

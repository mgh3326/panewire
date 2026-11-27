package panewire

// hk 6924: the rows of the daily stale-replay notice (relay_stale_notice.go).
// One row per real message the replay gate retired as stale. The notice day is
// the UTC day of the retirement; filed is set per design lane once that lane's
// notice for the day is in its inbox (or there was nothing to list), so a
// restart mid-day keeps the rows and a closed day is filed exactly once.

import (
	"context"
	"database/sql"
	"errors"
	"strings"
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
	`CREATE TABLE IF NOT EXISTS relay_stale_notice_lanes (
 worker_lane VARCHAR(255) NOT NULL, job VARCHAR(255) NOT NULL, first_ms BIGINT NOT NULL,
 PRIMARY KEY (worker_lane, job))`,
	`CREATE INDEX IF NOT EXISTS relay_stale_notice_lanes_first ON relay_stale_notice_lanes(worker_lane, first_ms)`,
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
	// staleReplayFileChunk bounds the ids one filing UPDATE names.
	staleReplayFileChunk = 500
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

// insertStaleReplayLane binds a worker lane to the job its route named when a
// row arrived. The first binding of a pair is kept: the bound job is the one the
// lane had at the earliest receipt the hub saw for it.
func (s *laneInboxStore) insertStaleReplayLane(parent context.Context, lane, job string, firstMS int64) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(s.dialect.insertIgnore(`INSERT INTO relay_stale_notice_lanes(worker_lane,job,first_ms) VALUES(?,?,?)`, "worker_lane,job")),
		utf8PrefixWithin(lane, staleReplayWidthLane), utf8PrefixWithin(job, staleReplayWidthJob), firstMS)
	return err
}

// staleReplayJobAt names the job a worker lane was bound to when a row received
// at atMS arrived: the binding whose first sighting is the latest at or before
// atMS. found is false when no binding is that old.
func (s *laneInboxStore) staleReplayJobAt(parent context.Context, lane string, atMS int64) (job string, found bool, err error) {
	ctx, done := s.op(parent)
	defer done()
	err = s.db.QueryRowContext(ctx, s.q(`SELECT job FROM relay_stale_notice_lanes WHERE worker_lane=? AND first_ms<=? ORDER BY first_ms DESC LIMIT 1`), lane, atMS).Scan(&job)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return job, true, nil
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

// markStaleReplayFiled closes the rows named by ids once their lane's notice is
// in the inbox, or once there was nothing to list. A row the filing pass did not
// read (one retired after the read) stays owed for the next pass.
func (s *laneInboxStore) markStaleReplayFiled(parent context.Context, ids []int64) error {
	ctx, done := s.op(parent)
	defer done()
	for len(ids) > 0 {
		n := min(len(ids), staleReplayFileChunk)
		args := make([]any, n)
		marks := make([]string, n)
		for i, id := range ids[:n] {
			args[i] = id
			marks[i] = "?"
		}
		if _, err := s.db.ExecContext(ctx, s.q(`UPDATE relay_stale_notice_rows SET filed=1 WHERE filed=0 AND relay_id IN (`+strings.Join(marks, ",")+`)`), args...); err != nil {
			return err
		}
		ids = ids[n:]
	}
	return nil
}

// pruneStaleReplayRows deletes filed rows of days before cutoff.
func (s *laneInboxStore) pruneStaleReplayRows(parent context.Context, cutoff string) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`DELETE FROM relay_stale_notice_rows WHERE filed=1 AND notice_day<?`), cutoff)
	return err
}

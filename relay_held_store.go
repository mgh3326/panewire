package panewire

import (
	"context"
	"database/sql"
	"time"
)

// relayHeld is deliberately node-local.  eventID is handoffkeep's durable
// relay-event id, while recvSeq is the receiving node's FIFO ordering key.
type relayHeld struct {
	Pane, Lane, JobID, Text, DeliverPolicy string
	EventID                                int64
	HeldSince                              time.Time
	MaxWait                                time.Duration
	RecvSeq                                int64
	Edited                                 bool
	fresh                                  bool // precise local receipt time, never persisted.
}

func (s *Store) InsertRelayHeld(ctx context.Context, held relayHeld) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var next int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(recv_seq),0)+1 FROM relay_held`).Scan(&next); err != nil {
		return false, err
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO relay_held(pane,lane,event_id,job_id,text,held_since,deliver_policy,max_wait,recv_seq,edited)
VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(lane,event_id) DO NOTHING`, held.Pane, held.Lane, held.EventID, held.JobID, held.Text,
		held.HeldSince.UnixMilli(), held.DeliverPolicy, held.MaxWait.Milliseconds(), next, boolInt(held.Edited))
	if err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	inserted, err := result.RowsAffected()
	return inserted == 1, err
}

func (s *Store) RelayHeldForPane(ctx context.Context, pane string) ([]relayHeld, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx, `SELECT pane,lane,event_id,job_id,text,held_since,deliver_policy,max_wait,recv_seq,edited
FROM relay_held WHERE pane=? ORDER BY recv_seq`, pane)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRelayHeld(rows)
}

func (s *Store) RelayHeldAll(ctx context.Context) ([]relayHeld, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx, `SELECT pane,lane,event_id,job_id,text,held_since,deliver_policy,max_wait,recv_seq,edited
FROM relay_held ORDER BY pane,recv_seq`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRelayHeld(rows)
}

func (s *Store) RelayHeldByKey(ctx context.Context, lane string, eventID int64) (relayHeld, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var item relayHeld
	var heldSince, maxWait int64
	var edited int
	err := s.db.QueryRowContext(ctx, `SELECT pane,lane,event_id,job_id,text,held_since,deliver_policy,max_wait,recv_seq,edited
FROM relay_held WHERE lane=? AND event_id=?`, lane, eventID).Scan(&item.Pane, &item.Lane, &item.EventID, &item.JobID, &item.Text, &heldSince, &item.DeliverPolicy, &maxWait, &item.RecvSeq, &edited)
	if err == sql.ErrNoRows {
		return relayHeld{}, false, nil
	}
	if err != nil {
		return relayHeld{}, false, err
	}
	item.HeldSince, item.MaxWait, item.Edited = time.UnixMilli(heldSince), time.Duration(maxWait)*time.Millisecond, edited != 0
	return item, true, nil
}

func scanRelayHeld(rows *sql.Rows) ([]relayHeld, error) {
	var held []relayHeld
	for rows.Next() {
		var item relayHeld
		var heldSince, maxWait int64
		var edited int
		if err := rows.Scan(&item.Pane, &item.Lane, &item.EventID, &item.JobID, &item.Text, &heldSince, &item.DeliverPolicy, &maxWait, &item.RecvSeq, &edited); err != nil {
			return nil, err
		}
		item.HeldSince = time.UnixMilli(heldSince)
		item.MaxWait = time.Duration(maxWait) * time.Millisecond
		item.Edited = edited != 0
		held = append(held, item)
	}
	return held, rows.Err()
}

func (s *Store) UpdateRelayHeldText(ctx context.Context, eventID int64, text string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.db.ExecContext(ctx, `UPDATE relay_held SET text=?,edited=1 WHERE event_id=?`, text, eventID)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n == 1, err
}

func (s *Store) DeleteRelayHeld(ctx context.Context, eventID int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.db.ExecContext(ctx, `DELETE FROM relay_held WHERE event_id=?`, eventID)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n == 1, err
}

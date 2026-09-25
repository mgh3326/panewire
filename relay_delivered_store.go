package panewire

import (
	"context"
	"database/sql"
	"time"
)

// relayDeliveredMaxAge bounds how long a delivered record can matter. Hub
// replay stops offering a row after relayReplayMaxAge, so anything older is
// dead weight; the tripled margin keeps the table from ever being the
// authority for what a replay might still retry.
const relayDeliveredMaxAge = 3 * 24 * time.Hour

// relayDelivered is the durable proof that one (lane, event_id) already
// reached a pane. The fingerprint pair exists so a repeat carrying a
// different payload — or naming a different destination pane — is a recorded
// mismatch rather than a silent duplicate.
type relayDelivered struct {
	Pane        string
	PayloadSHA  string
	DeliveredAt time.Time
}

// RecordRelayDelivered marks (lane, eventID) delivered. It is first-writer-
// wins like the relay row it shadows: a second delivery attempt under the
// same key never rewrites the first delivery's fingerprint, so replays keep
// comparing against the pane's original record.
func (s *Store) RecordRelayDelivered(ctx context.Context, lane string, eventID int64, pane, payloadSHA string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO relay_delivered(lane,event_id,pane,payload_sha,delivered_at) VALUES(?,?,?,?,?)`,
		lane, eventID, pane, payloadSHA, at.UnixMilli()); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM relay_delivered WHERE delivered_at < ?`, at.Add(-relayDeliveredMaxAge).UnixMilli()); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) RelayDeliveredByKey(ctx context.Context, lane string, eventID int64) (relayDelivered, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var record relayDelivered
	var deliveredAt int64
	err := s.db.QueryRowContext(ctx, `SELECT pane,payload_sha,delivered_at FROM relay_delivered WHERE lane=? AND event_id=?`, lane, eventID).Scan(&record.Pane, &record.PayloadSHA, &deliveredAt)
	if err == sql.ErrNoRows {
		return relayDelivered{}, false, nil
	}
	if err != nil {
		return relayDelivered{}, false, err
	}
	record.DeliveredAt = time.UnixMilli(deliveredAt)
	return record, true, nil
}

// DeleteRelayDelivered exists for tests and operator repair only: the relay
// path never un-delivers a recorded event.
func (s *Store) DeleteRelayDelivered(ctx context.Context, lane string, eventID int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.db.ExecContext(ctx, `DELETE FROM relay_delivered WHERE lane=? AND event_id=?`, lane, eventID)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n == 1, err
}

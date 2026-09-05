package panewire

import (
	"context"
	"database/sql"
	"os"
	"strconv"
	"time"
)

const (
	defaultRelayOutboxMaxAge = 24 * time.Hour
	// relayOutboxBackoff keeps a failing handoffkeep from being retried on
	// every ten-second scan. A row is eligible again once it has aged out.
	relayOutboxBackoff = 60 * time.Second
)

// relayOutboxMaxAge bounds which local event files the node still offers to
// the hub. It is deliberately a different axis from hubJobActiveMaxAge, which
// bounds the active-job heartbeat rather than undelivered relay traffic.
func relayOutboxMaxAge() time.Duration {
	if value, err := time.ParseDuration(os.Getenv("PANEWIRE_RELAY_OUTBOX_MAX_AGE")); err == nil && value > 0 {
		return value
	}
	return defaultRelayOutboxMaxAge
}

// relayEventOutboxKey is the one dedupe key shared by `panewire emit`, the node
// outbox, and handoffkeep's idempotency index. Its field order is part of the
// contract; changing it would silently resend every retained event once.
func relayEventOutboxKey(kind, jobID string, epoch uint64, reportPath, reason string) string {
	return kind + "\x00" + jobID + "\x00" + strconv.FormatUint(epoch, 10) + "\x00" + reportPath + "\x00" + reason
}

func relayLaneEventOutboxKey(lane, eventID string) string {
	return "lane.event\x00" + lane + "\x00" + eventID
}

type relayOutboxKey struct {
	Kind       string
	JobID      string
	Epoch      uint64
	ReportPath string
	Reason     string
	Lane       string
	EventID    string
}

func (k relayOutboxKey) String() string {
	if k.Kind == "lane.event" {
		return relayLaneEventOutboxKey(k.Lane, k.EventID)
	}
	return relayEventOutboxKey(k.Kind, k.JobID, k.Epoch, k.ReportPath, k.Reason)
}

// relayOutboxState is the durable answer to "has the hub already taken this
// event". persisted is terminal; sentAt only starts the retry backoff.
type relayOutboxState struct {
	SentAt    time.Time
	Persisted bool
	Found     bool
}

func (s *Store) relayOutboxArgs(key relayOutboxKey) []any {
	return []any{key.Kind, key.JobID, int64(key.Epoch), key.ReportPath, key.Reason}
}

// RelayOutboxState reports the durable send state for one relay event.
func (s *Store) RelayOutboxState(ctx context.Context, key relayOutboxKey) (relayOutboxState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var sentAt, persistedAt sql.NullInt64
	query, args := `SELECT sent_at,persisted_at FROM relay_sent WHERE kind=? AND job_id=? AND epoch=? AND report_path=? AND reason=?`, s.relayOutboxArgs(key)
	if key.Kind == "lane.event" {
		query, args = `SELECT sent_at,persisted_at FROM relay_sent WHERE kind='lane.event' AND lane=? AND event_id=?`, []any{key.Lane, key.EventID}
	}
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&sentAt, &persistedAt)
	if err == sql.ErrNoRows {
		return relayOutboxState{}, nil
	}
	if err != nil {
		return relayOutboxState{}, err
	}
	state := relayOutboxState{Found: true, Persisted: persistedAt.Valid}
	if sentAt.Valid && sentAt.Int64 > 0 {
		state.SentAt = time.UnixMilli(sentAt.Int64)
	}
	return state, nil
}

// UnpersistedRelayOutboxKey returns the one outstanding key that can raise a
// scanned event after a restart lost its in-memory assignment epoch. It never
// lowers a live event: doing so would fold a new assignment epoch into a stale
// unpersisted row for the same file. Ambiguous rows are left alone because this
// table deliberately has no event-file ID.
func (s *Store) UnpersistedRelayOutboxKey(ctx context.Context, key relayOutboxKey) (relayOutboxKey, bool, error) {
	if key.Kind == "lane.event" {
		return relayOutboxKey{}, false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx, `SELECT epoch FROM relay_sent
 WHERE kind=? AND job_id=? AND report_path=? AND reason=? AND persisted_at IS NULL
 ORDER BY sent_at DESC LIMIT 2`, key.Kind, key.JobID, key.ReportPath, key.Reason)
	if err != nil {
		return relayOutboxKey{}, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return relayOutboxKey{}, false, rows.Err()
	}
	var epoch int64
	if err := rows.Scan(&epoch); err != nil {
		return relayOutboxKey{}, false, err
	}
	if rows.Next() {
		return relayOutboxKey{}, false, rows.Err()
	}
	if err := rows.Err(); err != nil {
		return relayOutboxKey{}, false, err
	}
	if uint64(epoch) < key.Epoch {
		return relayOutboxKey{}, false, nil
	}
	key.Epoch = uint64(epoch)
	return key, true, nil
}

// RecordRelaySent stamps an attempt. It never clears persisted_at, so a row the
// hub has already durably stored stays out of the scan for good.
func (s *Store) RecordRelaySent(ctx context.Context, key relayOutboxKey, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if key.Kind == "lane.event" {
		_, err := s.db.ExecContext(ctx, `INSERT INTO relay_sent(kind,job_id,epoch,report_path,reason,lane,event_id,sent_at) VALUES(?,?,?,?,?,?,?,?)
 ON CONFLICT(lane,event_id) WHERE kind='lane.event' DO UPDATE SET sent_at=excluded.sent_at`, key.Kind, key.JobID, int64(key.Epoch), key.ReportPath, key.Reason, key.Lane, key.EventID, at.UnixMilli())
		return err
	}
	args := append(s.relayOutboxArgs(key), at.UnixMilli())
	_, err := s.db.ExecContext(ctx, `INSERT INTO relay_sent(kind,job_id,epoch,report_path,reason,sent_at) VALUES(?,?,?,?,?,?)
 ON CONFLICT(kind,job_id,epoch,report_path,reason) DO UPDATE SET sent_at=excluded.sent_at`, args...)
	return err
}

// RecordRelayPersisted records the hub's relay.persisted acknowledgement.
func (s *Store) RecordRelayPersisted(ctx context.Context, key relayOutboxKey, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if key.Kind == "lane.event" {
		_, err := s.db.ExecContext(ctx, `INSERT INTO relay_sent(kind,job_id,epoch,report_path,reason,lane,event_id,sent_at,persisted_at) VALUES(?,?,?,?,?,?,?,?,?)
 ON CONFLICT(lane,event_id) WHERE kind='lane.event' DO UPDATE SET persisted_at=excluded.persisted_at`, key.Kind, key.JobID, int64(key.Epoch), key.ReportPath, key.Reason, key.Lane, key.EventID, at.UnixMilli(), at.UnixMilli())
		return err
	}
	args := append(s.relayOutboxArgs(key), at.UnixMilli(), at.UnixMilli())
	_, err := s.db.ExecContext(ctx, `INSERT INTO relay_sent(kind,job_id,epoch,report_path,reason,sent_at,persisted_at) VALUES(?,?,?,?,?,?,?)
 ON CONFLICT(kind,job_id,epoch,report_path,reason) DO UPDATE SET persisted_at=excluded.persisted_at`, args...)
	return err
}

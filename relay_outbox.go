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
	// relayMigrationGrace is subtracted from this node's event-identity
	// migration time to form the suppression cutoff. A deploy restarts each
	// node, so a completion emitted minutes before that restart is fresh news,
	// not backlog - suppressing it would lose the one notification a finished
	// job ever gets. Ten minutes covers a rolling restart with margin, and the
	// window length caps how much recent work a deployment may relay.
	relayMigrationGrace = 10 * time.Minute
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

// relayEventOutboxKey is the delivery half of the dedupe key shared by
// `panewire emit`, the node outbox, and handoffkeep's idempotency index. Its
// field order is part of the contract; changing it would silently resend every
// retained event once. The producer's event identity travels separately:
// these five fields alone cannot tell a resend of one event from the same
// job's next round.
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
	return relayEventOutboxKey(k.Kind, k.JobID, k.Epoch, k.ReportPath, k.Reason) + "\x00" + k.EventID
}

// relayOutboxState is the durable answer to "has the hub already taken this
// event". persisted and suppressed are terminal; sentAt only starts the retry
// backoff.
type relayOutboxState struct {
	SentAt     time.Time
	Persisted  bool
	Suppressed bool
	Found      bool
}

func (s *Store) relayOutboxArgs(key relayOutboxKey) []any {
	return []any{key.Kind, key.JobID, int64(key.Epoch), key.ReportPath, key.Reason}
}

// RelayEventIDMigrationAt reports when this node's outbox was rekeyed onto
// event identity. The stamp exists only when that open actually carried
// legacy job.* rows across - a fresh or jobless database records no migration,
// and its absence means no cutoff: nothing an old binary sent needs
// suppressing. It is per-node on purpose: rollouts are sequential, so each
// node suppresses only the events its own pre-migration binary could have
// left behind.
func (s *Store) RelayEventIDMigrationAt(ctx context.Context) (time.Time, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var at int64
	err := s.db.QueryRowContext(ctx, `SELECT value FROM relay_meta WHERE key='event_id_since'`).Scan(&at)
	if err == sql.ErrNoRows {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	return time.UnixMilli(at), true, nil
}

// RelayOutboxState reports the durable send state for one relay event.
func (s *Store) RelayOutboxState(ctx context.Context, key relayOutboxKey) (relayOutboxState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var sentAt, persistedAt, suppressedAt sql.NullInt64
	query, args := `SELECT sent_at,persisted_at,suppressed_at FROM relay_sent WHERE kind=? AND job_id=? AND epoch=? AND report_path=? AND reason=?`, s.relayOutboxArgs(key)
	if key.Kind == "lane.event" {
		query, args = `SELECT sent_at,persisted_at,suppressed_at FROM relay_sent WHERE kind='lane.event' AND lane=? AND event_id=?`, []any{key.Lane, key.EventID}
	} else if key.EventID != "" {
		query, args = `SELECT sent_at,persisted_at,suppressed_at FROM relay_sent WHERE kind=? AND job_id=? AND event_id=?`, []any{key.Kind, key.JobID, key.EventID}
	}
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&sentAt, &persistedAt, &suppressedAt)
	if err == sql.ErrNoRows {
		return relayOutboxState{}, nil
	}
	if err != nil {
		return relayOutboxState{}, err
	}
	state := relayOutboxState{Found: true, Persisted: persistedAt.Valid, Suppressed: suppressedAt.Valid}
	if sentAt.Valid && sentAt.Int64 > 0 {
		state.SentAt = time.UnixMilli(sentAt.Int64)
	}
	return state, nil
}

// UnpersistedRelayOutboxKey returns the one outstanding key that can raise a
// scanned event after a restart lost its in-memory assignment epoch. It never
// lowers a live event: doing so would fold a new assignment epoch into a stale
// unpersisted row for the same file.
func (s *Store) UnpersistedRelayOutboxKey(ctx context.Context, key relayOutboxKey) (relayOutboxKey, bool, error) {
	if key.Kind == "lane.event" {
		return relayOutboxKey{}, false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if key.EventID != "" {
		// One event names at most one row, so the outstanding lookup is exact
		// rather than the ambiguous five-field scan the legacy key needed.
		var epoch int64
		err := s.db.QueryRowContext(ctx, `SELECT epoch FROM relay_sent WHERE kind=? AND job_id=? AND event_id=? AND persisted_at IS NULL AND suppressed_at IS NULL`, key.Kind, key.JobID, key.EventID).Scan(&epoch)
		if err == sql.ErrNoRows {
			return relayOutboxKey{}, false, nil
		}
		if err != nil {
			return relayOutboxKey{}, false, err
		}
		if uint64(epoch) < key.Epoch {
			return relayOutboxKey{}, false, nil
		}
		key.Epoch = uint64(epoch)
		return key, true, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT epoch FROM relay_sent
 WHERE kind=? AND job_id=? AND report_path=? AND reason=? AND persisted_at IS NULL AND suppressed_at IS NULL
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
	if key.EventID != "" {
		// (kind,job_id,event_id) is the authoritative key for an identified
		// event: a resend folds into the same row even when the live epoch or
		// the normalized text fields have drifted since the first attempt.
		_, err := s.db.ExecContext(ctx, `INSERT INTO relay_sent(kind,job_id,epoch,report_path,reason,lane,event_id,sent_at) VALUES(?,?,?,?,?,'',?,?)
 ON CONFLICT(kind,job_id,event_id) WHERE kind<>'lane.event' AND event_id<>'' DO UPDATE SET sent_at=excluded.sent_at`, key.Kind, key.JobID, int64(key.Epoch), key.ReportPath, key.Reason, key.EventID, at.UnixMilli())
		return err
	}
	args := append(s.relayOutboxArgs(key), at.UnixMilli())
	_, err := s.db.ExecContext(ctx, `INSERT INTO relay_sent(kind,job_id,epoch,report_path,reason,event_id,sent_at) VALUES(?,?,?,?,?,'',?)
 ON CONFLICT(kind,job_id,epoch,report_path,reason,event_id) DO UPDATE SET sent_at=excluded.sent_at`, args...)
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
	if key.EventID != "" {
		_, err := s.db.ExecContext(ctx, `INSERT INTO relay_sent(kind,job_id,epoch,report_path,reason,lane,event_id,sent_at,persisted_at) VALUES(?,?,?,?,?,'',?,?,?)
 ON CONFLICT(kind,job_id,event_id) WHERE kind<>'lane.event' AND event_id<>'' DO UPDATE SET persisted_at=excluded.persisted_at`, key.Kind, key.JobID, int64(key.Epoch), key.ReportPath, key.Reason, key.EventID, at.UnixMilli(), at.UnixMilli())
		return err
	}
	args := append(s.relayOutboxArgs(key), at.UnixMilli(), at.UnixMilli())
	_, err := s.db.ExecContext(ctx, `INSERT INTO relay_sent(kind,job_id,epoch,report_path,reason,event_id,sent_at,persisted_at) VALUES(?,?,?,?,?,'',?,?)
 ON CONFLICT(kind,job_id,epoch,report_path,reason,event_id) DO UPDATE SET persisted_at=excluded.persisted_at`, args...)
	return err
}

// RecordRelaySuppressed retires a pre-migration event without sending it. The
// row is the audit record that the file was seen and deliberately held back:
// sent_at stays NULL because nothing was written, persisted_at stays NULL
// because the hub never took it. A conflicting row - a send that somehow beat
// the cutoff - is never clobbered. It reports true only when the audit row
// was newly written, so the caller can log each suppression exactly once.
func (s *Store) RecordRelaySuppressed(ctx context.Context, key relayOutboxKey, at time.Time) (bool, error) {
	if key.Kind == "lane.event" || key.EventID == "" {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.db.ExecContext(ctx, `INSERT INTO relay_sent(kind,job_id,epoch,report_path,reason,lane,event_id,suppressed_at) VALUES(?,?,?,?,?,'',?,?)
 ON CONFLICT DO NOTHING`, key.Kind, key.JobID, int64(key.Epoch), key.ReportPath, key.Reason, key.EventID, at.UnixMilli())
	if err != nil {
		return false, err
	}
	inserted, _ := res.RowsAffected()
	return inserted == 1, nil
}

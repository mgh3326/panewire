package core

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type OutboxState string

const (
	OutboxSubmitted OutboxState = "SUBMITTED"
	OutboxPublished OutboxState = "PUBLISHED"
	OutboxCompleted OutboxState = "COMPLETED"
	OutboxExpired   OutboxState = "EXPIRED"
)

type InboxState string

const (
	InboxReceived         InboxState = "RECEIVED"
	InboxStaged           InboxState = "STAGED"
	InboxLogicalPublished InboxState = "LOGICAL_PUBLISHED"
	InboxGateDenied       InboxState = "GATE_DENIED"
	InboxSpawnRequested   InboxState = "SPAWN_REQUESTED"
	InboxSpawnUnknown     InboxState = "SPAWN_UNKNOWN"
	InboxSpawnAccepted    InboxState = "SPAWN_ACCEPTED"
	InboxCompleted        InboxState = "COMPLETED"
	InboxAcked            InboxState = "ACKED"
	InboxTerminalReject   InboxState = "TERMINAL_REJECT"
)

type OutboxRecord struct {
	MessageID, DestinationMachineID, DeliveryID string
	SourcePath, SHA256                          string
	SizeBytes                                   int64
	InboxNamespace, LogicalPath                 string
	Classification                              Classification
	ContentType                                 string
	PolicyVersion                               string
	MessageKind                                 MessageKind
	Source                                      Identity
	Expect                                      Expectation
	CorrelationID, CausationID                  string
	Reply                                       Reply
	Spawn                                       Spawn
	CreatedAt, ExpiresAt, UpdatedAt             time.Time
	LastAttemptAt                               time.Time
	Attempts                                    int
	State                                       OutboxState
	ReceiptID, LastErrorCode                    string
}

func (o OutboxRecord) Envelope() Envelope {
	return Envelope{
		SchemaVersion: SchemaVersion,
		MessageID:     o.MessageID,
		DeliveryID:    o.DeliveryID,
		MessageKind:   o.MessageKind,
		Source:        o.Source,
		Destination: Destination{
			MachineID: o.DestinationMachineID, InboxNamespace: o.InboxNamespace, LogicalPath: o.LogicalPath,
		},
		Expect:        o.Expect,
		Payload:       PayloadMeta{Mode: "inline", ContentType: o.ContentType, SizeBytes: o.SizeBytes, SHA256: o.SHA256, Classification: o.Classification},
		CreatedAt:     o.CreatedAt,
		ExpiresAt:     o.ExpiresAt,
		CorrelationID: o.CorrelationID,
		CausationID:   o.CausationID,
		Reply:         o.Reply,
		Spawn:         o.Spawn,
	}
}

type InboxRecord struct {
	DeliveryID, MessageID, DestinationMachineID string
	InboxNamespace, LogicalPath                 string
	StagingPath, QuarantinePath                 string
	SHA256                                      string
	SizeBytes                                   int64
	Classification                              Classification
	State                                       InboxState
	TerminalReason                              string
	Acked                                       bool
	Attempts                                    int
	CreatedAt, UpdatedAt                        time.Time
	CompletionID                                string
}

type CompletionRecord struct {
	CorrelationID, CompletionID, CausationID, ResultHash string
	TerminalOutcome                                      string
	CreatedAt                                            time.Time
}

// MetadataStore contains only stage2 metadata. Its schema has no payload
// column, JSON audit field, or blob field by construction.
type MetadataStore struct {
	db   *sql.DB
	path string
	mu   sync.Mutex
}

func OpenMetadataStore(file string) (*MetadataStore, error) {
	if file == "" {
		return nil, fmt.Errorf("empty stage2 SQLite path")
	}
	if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
		return nil, fmt.Errorf("create metadata directory: %w", err)
	}
	db, err := sql.Open("sqlite", file)
	if err != nil {
		return nil, err
	}
	s := &MetadataStore{db: db, path: file}
	for _, statement := range metadataSchema {
		if _, err := db.Exec(statement); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("create stage2 metadata schema: %w", err)
		}
	}
	if err := ensureOutboxSpawnLabelColumn(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate stage2 metadata schema: %w", err)
	}
	return s, nil
}

// ensureOutboxSpawnLabelColumn keeps pre-R4 metadata databases readable. The
// sender outbox must retain the policy label across a crash before publication;
// otherwise a recovered spawn request could silently lose its receiver policy
// selector.
func ensureOutboxSpawnLabelColumn(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(s2_outbox)`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, typ string
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &primaryKey); err != nil {
			return err
		}
		if name == "spawn_label" {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = db.Exec(`ALTER TABLE s2_outbox ADD COLUMN spawn_label TEXT NOT NULL DEFAULT ''`)
	return err
}

var metadataSchema = []string{
	"PRAGMA busy_timeout=5000",
	"PRAGMA journal_mode=WAL",
	`CREATE TABLE IF NOT EXISTS s2_outbox (
 delivery_id TEXT PRIMARY KEY,
 message_id TEXT NOT NULL,
 destination_machine_id TEXT NOT NULL,
 source_path TEXT NOT NULL,
 sha256 TEXT NOT NULL,
 size_bytes INTEGER NOT NULL,
 inbox_namespace TEXT NOT NULL,
 logical_path TEXT NOT NULL,
 classification TEXT NOT NULL,
	 content_type TEXT NOT NULL,
 policy_version TEXT NOT NULL,
 message_kind TEXT NOT NULL,
 source_machine_id TEXT NOT NULL,
 source_instance_id TEXT NOT NULL,
 expect_machine_id TEXT NOT NULL,
 expect_pane_name TEXT NOT NULL,
 expect_pane_label TEXT NOT NULL,
 expect_pane_cwd TEXT NOT NULL,
 expect_workspace_id TEXT NOT NULL,
 correlation_id TEXT NOT NULL,
 causation_id TEXT NOT NULL,
 reply_destination_machine_id TEXT NOT NULL,
 reply_correlation_id TEXT NOT NULL,
 reply_requested INTEGER NOT NULL,
 spawn_requested INTEGER NOT NULL,
	spawn_label TEXT NOT NULL DEFAULT '',
 created_at_ms INTEGER NOT NULL,
 expires_at_ms INTEGER NOT NULL,
 updated_at_ms INTEGER NOT NULL,
 last_attempt_at_ms INTEGER,
 attempts INTEGER NOT NULL DEFAULT 0,
 state TEXT NOT NULL,
 receipt_id TEXT NOT NULL DEFAULT '',
 last_error_code TEXT NOT NULL DEFAULT '',
 UNIQUE(message_id, destination_machine_id)
)`,
	`CREATE TABLE IF NOT EXISTS s2_inbox (
 delivery_id TEXT PRIMARY KEY,
 message_id TEXT NOT NULL,
 destination_machine_id TEXT NOT NULL,
 inbox_namespace TEXT NOT NULL,
 logical_path TEXT NOT NULL,
 staging_path TEXT NOT NULL DEFAULT '',
 quarantine_path TEXT NOT NULL DEFAULT '',
 sha256 TEXT NOT NULL,
 size_bytes INTEGER NOT NULL,
 classification TEXT NOT NULL,
 state TEXT NOT NULL,
 terminal_reason TEXT NOT NULL DEFAULT '',
 completion_id TEXT NOT NULL DEFAULT '',
 acked_at_ms INTEGER,
 attempts INTEGER NOT NULL DEFAULT 0,
 created_at_ms INTEGER NOT NULL,
 updated_at_ms INTEGER NOT NULL
)`,
	`CREATE TABLE IF NOT EXISTS s2_completions (
 correlation_id TEXT PRIMARY KEY,
 completion_id TEXT NOT NULL,
 causation_id TEXT NOT NULL,
 result_hash TEXT NOT NULL,
 terminal_outcome TEXT NOT NULL,
 created_at_ms INTEGER NOT NULL
)`,
	`CREATE TABLE IF NOT EXISTS s2_completion_anomalies (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 correlation_id TEXT NOT NULL,
 completion_id TEXT NOT NULL,
 result_hash TEXT NOT NULL,
 observed_at_ms INTEGER NOT NULL
)`,
	"CREATE INDEX IF NOT EXISTS s2_outbox_state_expiry ON s2_outbox(state, expires_at_ms)",
	"CREATE INDEX IF NOT EXISTS s2_inbox_state ON s2_inbox(state)",
}

func (s *MetadataStore) Path() string { return s.path }
func (s *MetadataStore) Close() error { return s.db.Close() }

func (s *MetadataStore) InsertOutbox(ctx context.Context, r OutboxRecord) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `INSERT INTO s2_outbox(
delivery_id,message_id,destination_machine_id,source_path,sha256,size_bytes,inbox_namespace,logical_path,classification,content_type,policy_version,message_kind,
source_machine_id,source_instance_id,expect_machine_id,expect_pane_name,expect_pane_label,expect_pane_cwd,expect_workspace_id,
correlation_id,causation_id,reply_destination_machine_id,reply_correlation_id,reply_requested,spawn_requested,spawn_label,created_at_ms,expires_at_ms,updated_at_ms,state)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		r.DeliveryID, r.MessageID, r.DestinationMachineID, r.SourcePath, r.SHA256, r.SizeBytes, r.InboxNamespace, r.LogicalPath,
		string(r.Classification), r.ContentType, r.PolicyVersion, string(r.MessageKind), r.Source.MachineID, r.Source.InstanceID, r.Expect.MachineID,
		r.Expect.Pane.Name, r.Expect.Pane.Label, r.Expect.Pane.CWD, r.Expect.Pane.WorkspaceID, r.CorrelationID, r.CausationID,
		r.Reply.DestinationMachineID, r.Reply.CorrelationID, boolInt(r.Reply.Requested), boolInt(r.Spawn.Requested), r.Spawn.Label, millis(r.CreatedAt), millis(r.ExpiresAt), millis(r.UpdatedAt), string(r.State))
	if err == nil {
		return true, nil
	}
	if isUnique(err) {
		existing, found, getErr := s.outboxByDeliveryLocked(ctx, r.DeliveryID)
		if getErr != nil {
			return false, getErr
		}
		if found && sameOutboxImmutable(existing, r) {
			return false, nil
		}
		return false, validation(CodeSchema, "outbox idempotency conflict")
	}
	return false, err
}

func sameOutboxImmutable(a, b OutboxRecord) bool {
	return a.DeliveryID == b.DeliveryID && a.MessageID == b.MessageID && a.DestinationMachineID == b.DestinationMachineID &&
		a.SourcePath == b.SourcePath && a.SHA256 == b.SHA256 && a.SizeBytes == b.SizeBytes && a.InboxNamespace == b.InboxNamespace &&
		a.LogicalPath == b.LogicalPath && a.Classification == b.Classification && a.Spawn == b.Spawn && a.ExpiresAt.Equal(b.ExpiresAt)
}

func (s *MetadataStore) OutboxByDelivery(ctx context.Context, id string) (OutboxRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.outboxByDeliveryLocked(ctx, id)
}

// ListOutbox returns metadata-only status rows for the operator CLI.  It never
// opens a source file or queries a transport, so inspection is safe while the
// daemon is offline.
func (s *MetadataStore) ListOutbox(ctx context.Context, state OutboxState) ([]OutboxRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	query := `SELECT delivery_id,message_id,destination_machine_id,source_path,sha256,size_bytes,inbox_namespace,logical_path,classification,content_type,policy_version,message_kind,
source_machine_id,source_instance_id,expect_machine_id,expect_pane_name,expect_pane_label,expect_pane_cwd,expect_workspace_id,correlation_id,causation_id,
reply_destination_machine_id,reply_correlation_id,reply_requested,spawn_requested,spawn_label,created_at_ms,expires_at_ms,updated_at_ms,last_attempt_at_ms,attempts,state,receipt_id,last_error_code
FROM s2_outbox`
	var args []any
	if state != "" {
		query += ` WHERE state=?`
		args = append(args, string(state))
	}
	query += ` ORDER BY created_at_ms,message_id`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []OutboxRecord
	for rows.Next() {
		record, _, err := scanOutbox(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (s *MetadataStore) outboxByDeliveryLocked(ctx context.Context, id string) (OutboxRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT delivery_id,message_id,destination_machine_id,source_path,sha256,size_bytes,inbox_namespace,logical_path,classification,content_type,policy_version,message_kind,
source_machine_id,source_instance_id,expect_machine_id,expect_pane_name,expect_pane_label,expect_pane_cwd,expect_workspace_id,correlation_id,causation_id,
reply_destination_machine_id,reply_correlation_id,reply_requested,spawn_requested,spawn_label,created_at_ms,expires_at_ms,updated_at_ms,last_attempt_at_ms,attempts,state,receipt_id,last_error_code
FROM s2_outbox WHERE delivery_id=?`, id)
	return scanOutbox(row)
}

func (s *MetadataStore) PendingOutbox(ctx context.Context, now time.Time) ([]OutboxRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx, `SELECT delivery_id,message_id,destination_machine_id,source_path,sha256,size_bytes,inbox_namespace,logical_path,classification,content_type,policy_version,message_kind,
source_machine_id,source_instance_id,expect_machine_id,expect_pane_name,expect_pane_label,expect_pane_cwd,expect_workspace_id,correlation_id,causation_id,
reply_destination_machine_id,reply_correlation_id,reply_requested,spawn_requested,spawn_label,created_at_ms,expires_at_ms,updated_at_ms,last_attempt_at_ms,attempts,state,receipt_id,last_error_code
FROM s2_outbox WHERE state=? ORDER BY created_at_ms`, string(OutboxSubmitted))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OutboxRecord
	for rows.Next() {
		r, _, err := scanOutbox(rows)
		if err != nil {
			return nil, err
		}
		if !now.Before(r.ExpiresAt) {
			if _, err := s.db.ExecContext(ctx, `UPDATE s2_outbox SET state=?,updated_at_ms=? WHERE delivery_id=?`, string(OutboxExpired), millis(now), r.DeliveryID); err != nil {
				return nil, err
			}
			continue
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *MetadataStore) MarkOutboxAttempt(ctx context.Context, id string, now time.Time, receipt PublishReceipt, code string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := OutboxSubmitted
	if code == "" {
		state = OutboxPublished
	}
	_, err := s.db.ExecContext(ctx, `UPDATE s2_outbox SET state=?,receipt_id=?,last_error_code=?,attempts=attempts+1,last_attempt_at_ms=?,updated_at_ms=? WHERE delivery_id=?`, string(state), receipt.DeliveryID, code, millis(now), millis(now), id)
	return err
}

func (s *MetadataStore) MarkOutboxExpired(ctx context.Context, id string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `UPDATE s2_outbox SET state=?,last_error_code=?,updated_at_ms=? WHERE delivery_id=? AND state=?`, string(OutboxExpired), CodeExpired, millis(now), id, string(OutboxSubmitted))
	return err
}

func (s *MetadataStore) MarkOutboxCompleted(ctx context.Context, messageID string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `UPDATE s2_outbox SET state=?,updated_at_ms=? WHERE message_id=? AND state IN (?,?)`, string(OutboxCompleted), millis(now), messageID, string(OutboxSubmitted), string(OutboxPublished))
	return err
}

func (s *MetadataStore) ReserveInbox(ctx context.Context, env Envelope, now time.Time) (InboxRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, found, err := s.inboxByDeliveryLocked(ctx, env.DeliveryID)
	if err != nil {
		return InboxRecord{}, false, err
	}
	if found {
		if existing.MessageID != env.MessageID || existing.SHA256 != env.Payload.SHA256 || existing.SizeBytes != env.Payload.SizeBytes || existing.LogicalPath != env.Destination.LogicalPath || existing.DestinationMachineID != env.Destination.MachineID {
			return InboxRecord{}, false, validation(CodeCollision, "delivery metadata conflicts with durable dedupe record")
		}
		_, err := s.db.ExecContext(ctx, `UPDATE s2_inbox SET attempts=attempts+1,updated_at_ms=? WHERE delivery_id=?`, millis(now), env.DeliveryID)
		if err != nil {
			return InboxRecord{}, false, err
		}
		existing.Attempts++
		existing.UpdatedAt = now
		return existing, false, nil
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO s2_inbox(delivery_id,message_id,destination_machine_id,inbox_namespace,logical_path,sha256,size_bytes,classification,state,created_at_ms,updated_at_ms)
VALUES(?,?,?,?,?,?,?,?,?,?,?)`, env.DeliveryID, env.MessageID, env.Destination.MachineID, env.Destination.InboxNamespace, env.Destination.LogicalPath, env.Payload.SHA256, env.Payload.SizeBytes, string(env.Payload.Classification), string(InboxReceived), millis(now), millis(now))
	if err != nil {
		return InboxRecord{}, false, err
	}
	return InboxRecord{DeliveryID: env.DeliveryID, MessageID: env.MessageID, DestinationMachineID: env.Destination.MachineID, InboxNamespace: env.Destination.InboxNamespace, LogicalPath: env.Destination.LogicalPath, SHA256: env.Payload.SHA256, SizeBytes: env.Payload.SizeBytes, Classification: env.Payload.Classification, State: InboxReceived, Attempts: 1, CreatedAt: now, UpdatedAt: now}, true, nil
}

func (s *MetadataStore) InboxByDelivery(ctx context.Context, id string) (InboxRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inboxByDeliveryLocked(ctx, id)
}

func (s *MetadataStore) inboxByDeliveryLocked(ctx context.Context, id string) (InboxRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT delivery_id,message_id,destination_machine_id,inbox_namespace,logical_path,staging_path,quarantine_path,sha256,size_bytes,classification,state,terminal_reason,completion_id,acked_at_ms,attempts,created_at_ms,updated_at_ms FROM s2_inbox WHERE delivery_id=?`, id)
	return scanInbox(row)
}

func (s *MetadataStore) MarkStaged(ctx context.Context, id, stagingPath string, now time.Time) error {
	return s.updateInbox(ctx, id, InboxStaged, "", stagingPath, "", false, now)
}

func (s *MetadataStore) MarkPublished(ctx context.Context, id string, now time.Time) error {
	return s.updateInbox(ctx, id, InboxLogicalPublished, "", "", "", false, now)
}

func (s *MetadataStore) MarkCompleted(ctx context.Context, id, completionID string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `UPDATE s2_inbox SET state=?,completion_id=?,updated_at_ms=? WHERE delivery_id=?`, string(InboxCompleted), completionID, millis(now), id)
	return err
}

func (s *MetadataStore) MarkTerminal(ctx context.Context, id, reason, quarantinePath string, now time.Time) error {
	return s.updateInbox(ctx, id, InboxTerminalReject, reason, "", quarantinePath, false, now)
}

func (s *MetadataStore) MarkGateState(ctx context.Context, id string, state InboxState, reason string, now time.Time) error {
	if state != InboxGateDenied && state != InboxSpawnRequested && state != InboxSpawnUnknown && state != InboxSpawnAccepted {
		return fmt.Errorf("invalid gate state %q", state)
	}
	return s.updateInbox(ctx, id, state, reason, "", "", false, now)
}

func (s *MetadataStore) MarkAcked(ctx context.Context, id string, now time.Time) error {
	return s.updateInbox(ctx, id, InboxAcked, "", "", "", true, now)
}

func (s *MetadataStore) updateInbox(ctx context.Context, id string, state InboxState, reason, staging, quarantine string, ack bool, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sets := "state=?,updated_at_ms=?"
	args := []any{string(state), millis(now)}
	if reason != "" {
		sets += ",terminal_reason=?"
		args = append(args, reason)
	}
	if staging != "" {
		sets += ",staging_path=?"
		args = append(args, staging)
	}
	if quarantine != "" {
		sets += ",quarantine_path=?"
		args = append(args, quarantine)
	}
	if ack {
		sets += ",acked_at_ms=?"
		args = append(args, millis(now))
	}
	args = append(args, id)
	_, err := s.db.ExecContext(ctx, "UPDATE s2_inbox SET "+sets+" WHERE delivery_id=?", args...)
	return err
}

// RecordCompletion preserves the first valid terminal transition and records
// later completion IDs only as metadata-only anomalies.
func (s *MetadataStore) RecordCompletion(ctx context.Context, r CompletionRecord) (first bool, anomaly bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, false, err
	}
	defer tx.Rollback()
	var existingID string
	err = tx.QueryRowContext(ctx, `SELECT completion_id FROM s2_completions WHERE correlation_id=?`, r.CorrelationID).Scan(&existingID)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.ExecContext(ctx, `INSERT INTO s2_completions(correlation_id,completion_id,causation_id,result_hash,terminal_outcome,created_at_ms) VALUES(?,?,?,?,?,?)`, r.CorrelationID, r.CompletionID, r.CausationID, r.ResultHash, r.TerminalOutcome, millis(r.CreatedAt))
		if err != nil {
			return false, false, err
		}
		if err = tx.Commit(); err != nil {
			return false, false, err
		}
		return true, false, nil
	}
	if err != nil {
		return false, false, err
	}
	if existingID == r.CompletionID {
		return false, false, tx.Commit()
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO s2_completion_anomalies(correlation_id,completion_id,result_hash,observed_at_ms) VALUES(?,?,?,?)`, r.CorrelationID, r.CompletionID, r.ResultHash, millis(r.CreatedAt))
	if err != nil {
		return false, false, err
	}
	return false, true, tx.Commit()
}

func (s *MetadataStore) CompletionByCorrelation(ctx context.Context, id string) (CompletionRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var r CompletionRecord
	var created int64
	err := s.db.QueryRowContext(ctx, `SELECT correlation_id,completion_id,causation_id,result_hash,terminal_outcome,created_at_ms FROM s2_completions WHERE correlation_id=?`, id).Scan(&r.CorrelationID, &r.CompletionID, &r.CausationID, &r.ResultHash, &r.TerminalOutcome, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return CompletionRecord{}, false, nil
	}
	if err != nil {
		return CompletionRecord{}, false, err
	}
	r.CreatedAt = fromMillis(created)
	return r, true, nil
}

func (s *MetadataStore) CompletionAnomalyCount(ctx context.Context, correlationID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM s2_completion_anomalies WHERE correlation_id=?`, correlationID).Scan(&count)
	return count, err
}

func scanOutbox(row interface{ Scan(...any) error }) (OutboxRecord, bool, error) {
	var r OutboxRecord
	var class, kind, state string
	var spawnLabel string
	var replyRequested, spawnRequested int
	var created, expires, updated int64
	var last sql.NullInt64
	err := row.Scan(&r.DeliveryID, &r.MessageID, &r.DestinationMachineID, &r.SourcePath, &r.SHA256, &r.SizeBytes, &r.InboxNamespace, &r.LogicalPath, &class, &r.ContentType, &r.PolicyVersion, &kind,
		&r.Source.MachineID, &r.Source.InstanceID, &r.Expect.MachineID, &r.Expect.Pane.Name, &r.Expect.Pane.Label, &r.Expect.Pane.CWD, &r.Expect.Pane.WorkspaceID, &r.CorrelationID, &r.CausationID,
		&r.Reply.DestinationMachineID, &r.Reply.CorrelationID, &replyRequested, &spawnRequested, &spawnLabel, &created, &expires, &updated, &last, &r.Attempts, &state, &r.ReceiptID, &r.LastErrorCode)
	if errors.Is(err, sql.ErrNoRows) {
		return OutboxRecord{}, false, nil
	}
	if err != nil {
		return OutboxRecord{}, false, err
	}
	r.Classification, r.MessageKind, r.State = Classification(class), MessageKind(kind), OutboxState(state)
	r.Reply.Requested, r.Spawn.Requested, r.Spawn.Label = replyRequested != 0, spawnRequested != 0, spawnLabel
	r.CreatedAt, r.ExpiresAt, r.UpdatedAt = fromMillis(created), fromMillis(expires), fromMillis(updated)
	if last.Valid {
		r.LastAttemptAt = fromMillis(last.Int64)
	}
	return r, true, nil
}

func scanInbox(row interface{ Scan(...any) error }) (InboxRecord, bool, error) {
	var r InboxRecord
	var class, state string
	var acked sql.NullInt64
	var created, updated int64
	err := row.Scan(&r.DeliveryID, &r.MessageID, &r.DestinationMachineID, &r.InboxNamespace, &r.LogicalPath, &r.StagingPath, &r.QuarantinePath, &r.SHA256, &r.SizeBytes, &class, &state, &r.TerminalReason, &r.CompletionID, &acked, &r.Attempts, &created, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return InboxRecord{}, false, nil
	}
	if err != nil {
		return InboxRecord{}, false, err
	}
	r.Classification, r.State = Classification(class), InboxState(state)
	r.Acked = acked.Valid
	r.CreatedAt, r.UpdatedAt = fromMillis(created), fromMillis(updated)
	return r, true, nil
}

func millis(v time.Time) int64     { return v.UTC().UnixMilli() }
func fromMillis(v int64) time.Time { return time.UnixMilli(v).UTC() }
func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func isUnique(err error) bool {
	return err != nil && (contains(err.Error(), "UNIQUE constraint failed") || contains(err.Error(), "constraint failed"))
}

func contains(value, needle string) bool {
	for i := 0; i+len(needle) <= len(value); i++ {
		if value[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

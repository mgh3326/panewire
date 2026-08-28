package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	db   *sql.DB
	path string
	mu   sync.Mutex
}

// Delivery is the durable, body-free audit record for one prompt request.
type Delivery struct {
	DeliveryID, Sender, TargetInput, ResolvedPaneID, ResolvedWorkspaceID string
	SourcePath, PromptSHA256, PreflightReadSHA256, PreflightResult       string
	HerdrAcceptance, SubmissionResult, UptakeMode, UptakeResult          string
	ErrorCode                                                            string
	RequestedAtMS, CompletedAtMS, PreflightRevision, SendRevision        int64
	EvidenceRevision                                                     int64
	BodyStored                                                           bool
}

// NewMemoryStore is a temporary on-disk SQLite store for fixtures and callers
// that need persistence without choosing a production path.
func NewMemoryStore(t interface{ TempDir() string }) *Store {
	s, err := OpenStore(filepath.Join(t.TempDir(), "panewire.sqlite3"))
	if err != nil {
		panic(err)
	}
	return s
}
func OpenStore(path string) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("empty sqlite path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	s := &Store{db: db, path: path}
	for _, q := range []string{"PRAGMA busy_timeout=5000", "PRAGMA journal_mode=WAL", "PRAGMA foreign_keys=ON", `CREATE TABLE IF NOT EXISTS events (
 id INTEGER PRIMARY KEY AUTOINCREMENT, observed_at_ms INTEGER NOT NULL, source TEXT NOT NULL,
 event_kind TEXT NOT NULL, protocol INTEGER, schema_version INTEGER, pane_id TEXT, workspace_id TEXT,
 agent TEXT, agent_status TEXT, revision INTEGER, path TEXT, payload_json TEXT NOT NULL, unknown_fields_json TEXT
)`, `CREATE TABLE IF NOT EXISTS deliveries (
 delivery_id TEXT PRIMARY KEY, requested_at_ms INTEGER, completed_at_ms INTEGER, sender TEXT NOT NULL DEFAULT '',
 target_input TEXT NOT NULL DEFAULT '', resolved_pane_id TEXT, resolved_workspace_id TEXT, source_path TEXT NOT NULL DEFAULT '',
 prompt_sha256 TEXT NOT NULL DEFAULT '', body_stored INTEGER NOT NULL DEFAULT 0, preflight_revision INTEGER,
 send_revision INTEGER, preflight_read_sha256 TEXT, preflight_result TEXT NOT NULL DEFAULT '', herdr_acceptance TEXT,
 submission_result TEXT, uptake_mode TEXT, uptake_result TEXT, evidence_revision INTEGER, error_code TEXT
)`, `CREATE INDEX IF NOT EXISTS events_kind_time ON events(event_kind, observed_at_ms)`, `CREATE INDEX IF NOT EXISTS events_pane_time ON events(pane_id, observed_at_ms)`, `CREATE INDEX IF NOT EXISTS deliveries_pane_time ON deliveries(resolved_pane_id, requested_at_ms)`, `CREATE INDEX IF NOT EXISTS deliveries_path_time ON deliveries(source_path, requested_at_ms)`} {
		if _, err := db.Exec(q); err != nil {
			db.Close()
			return nil, err
		}
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS delivery_bodies (
	 delivery_id TEXT PRIMARY KEY REFERENCES deliveries(delivery_id) ON DELETE CASCADE,
	 body TEXT NOT NULL
)`); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}
func (s *Store) Path() string { return s.path }
func (s *Store) Close() error { return s.db.Close() }

type Event struct {
	ObservedAt                                                  time.Time
	Source, Kind, PaneID, WorkspaceID, Agent, AgentStatus, Path string
	Protocol, SchemaVersion, Revision                           int64
	Payload                                                     json.RawMessage
	Unknown                                                     json.RawMessage
}

func (s *Store) RecordEvent(ctx context.Context, e Event) error {
	if e.ObservedAt.IsZero() {
		e.ObservedAt = time.Now().UTC()
	}
	if len(e.Payload) == 0 {
		e.Payload = json.RawMessage(`{}`)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `INSERT INTO events(observed_at_ms,source,event_kind,protocol,schema_version,pane_id,workspace_id,agent,agent_status,revision,path,payload_json,unknown_fields_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, e.ObservedAt.UnixMilli(), e.Source, e.Kind, nullableInt(e.Protocol), nullableInt(e.SchemaVersion), e.PaneID, e.WorkspaceID, e.Agent, e.AgentStatus, nullableInt(e.Revision), e.Path, string(e.Payload), nullableJSON(e.Unknown))
	return err
}

func nullableInt(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}
func nullableJSON(b json.RawMessage) any {
	if len(b) == 0 {
		return nil
	}
	return string(b)
}
func (s *Store) CountEvents() int {
	var n int
	_ = s.db.QueryRow(`SELECT count(*) FROM events`).Scan(&n)
	return n
}
func (s *Store) CountEventKind(k string) int {
	var n int
	_ = s.db.QueryRow(`SELECT count(*) FROM events WHERE event_kind=?`, k).Scan(&n)
	return n
}
func (s *Store) ContainsPayload(value string) bool {
	var n int
	_ = s.db.QueryRow(`SELECT count(*) FROM events WHERE payload_json LIKE ?`, "%"+value+"%").Scan(&n)
	return n > 0
}

func (s *Store) CountDeliveries() int {
	var n int
	_ = s.db.QueryRow(`SELECT count(*) FROM deliveries`).Scan(&n)
	return n
}

func (s *Store) LatestDelivery(ctx context.Context) (Delivery, bool, error) {
	var id string
	err := s.db.QueryRowContext(ctx, `SELECT delivery_id FROM deliveries ORDER BY requested_at_ms DESC LIMIT 1`).Scan(&id)
	if err == sql.ErrNoRows {
		return Delivery{}, false, nil
	}
	if err != nil {
		return Delivery{}, false, err
	}
	return s.GetDelivery(ctx, id)
}

func (s *Store) PromptBody(ctx context.Context, id string) (string, bool, error) {
	var body string
	err := s.db.QueryRowContext(ctx, `SELECT body FROM delivery_bodies WHERE delivery_id=?`, id).Scan(&body)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return body, true, nil
}

func (s *Store) GetDelivery(ctx context.Context, id string) (Delivery, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var d Delivery
	var stored int
	var requested, completed, preflight, send, evidence sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT delivery_id,requested_at_ms,completed_at_ms,sender,target_input,
resolved_pane_id,resolved_workspace_id,source_path,prompt_sha256,body_stored,preflight_revision,send_revision,
preflight_read_sha256,preflight_result,herdr_acceptance,submission_result,uptake_mode,uptake_result,evidence_revision,error_code
	FROM deliveries WHERE delivery_id=?`, id).Scan(&d.DeliveryID, &requested, &completed, &d.Sender, &d.TargetInput,
		&d.ResolvedPaneID, &d.ResolvedWorkspaceID, &d.SourcePath, &d.PromptSHA256, &stored, &preflight, &send,
		&d.PreflightReadSHA256, &d.PreflightResult, &d.HerdrAcceptance, &d.SubmissionResult, &d.UptakeMode, &d.UptakeResult, &evidence, &d.ErrorCode)
	if err == sql.ErrNoRows {
		return Delivery{}, false, nil
	}
	if err != nil {
		return Delivery{}, false, err
	}
	d.BodyStored = stored != 0
	if requested.Valid {
		d.RequestedAtMS = requested.Int64
	}
	if completed.Valid {
		d.CompletedAtMS = completed.Int64
	}
	if preflight.Valid {
		d.PreflightRevision = preflight.Int64
	}
	if send.Valid {
		d.SendRevision = send.Int64
	}
	if evidence.Valid {
		d.EvidenceRevision = evidence.Int64
	}
	return d, true, nil
}

// InsertDelivery commits the preflight record before any herdr prompt call.
// It returns false when the correlation id already exists, making retries
// idempotent across CLI processes.
func (s *Store) InsertDelivery(ctx context.Context, d Delivery, body string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO deliveries(delivery_id,requested_at_ms,completed_at_ms,sender,target_input,
resolved_pane_id,resolved_workspace_id,source_path,prompt_sha256,body_stored,preflight_revision,send_revision,
preflight_read_sha256,preflight_result,herdr_acceptance,submission_result,uptake_mode,uptake_result,evidence_revision,error_code)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, d.DeliveryID, d.RequestedAtMS, d.CompletedAtMS, d.Sender, d.TargetInput,
		d.ResolvedPaneID, d.ResolvedWorkspaceID, d.SourcePath, d.PromptSHA256, boolInt(d.BodyStored), nullableInt(d.PreflightRevision),
		nullableInt(d.SendRevision), d.PreflightReadSHA256, d.PreflightResult, d.HerdrAcceptance, d.SubmissionResult, d.UptakeMode,
		d.UptakeResult, nullableInt(d.EvidenceRevision), d.ErrorCode)
	if err != nil {
		return false, err
	}
	if d.BodyStored {
		if _, err = tx.ExecContext(ctx, `INSERT INTO delivery_bodies(delivery_id,body) VALUES(?,?)`, d.DeliveryID, body); err != nil {
			return false, err
		}
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) UpdateDelivery(ctx context.Context, d Delivery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `UPDATE deliveries SET completed_at_ms=?,resolved_pane_id=?,resolved_workspace_id=?,
preflight_revision=?,send_revision=?,preflight_read_sha256=?,preflight_result=?,herdr_acceptance=?,submission_result=?,
uptake_mode=?,uptake_result=?,evidence_revision=?,error_code=? WHERE delivery_id=?`, d.CompletedAtMS, d.ResolvedPaneID,
		d.ResolvedWorkspaceID, nullableInt(d.PreflightRevision), nullableInt(d.SendRevision), d.PreflightReadSHA256, d.PreflightResult,
		d.HerdrAcceptance, d.SubmissionResult, d.UptakeMode, d.UptakeResult, nullableInt(d.EvidenceRevision), d.ErrorCode, d.DeliveryID)
	return err
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

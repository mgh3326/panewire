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
	ErrorCode, ErrorDetail, SubmissionEvidence                           string // SubmissionEvidence: source:rule (#547)
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
func storeHasColumn(db *sql.DB, table, column string) bool {
	var name string
	err := db.QueryRow(`SELECT name FROM pragma_table_info(?) WHERE name=?`, table, column).Scan(&name)
	return err == nil && name == column
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
	// R3 adds detail for structured expect mismatch reasons. Ignore the
	// duplicate-column error when opening a database already migrated.
	_, _ = db.Exec(`ALTER TABLE deliveries ADD COLUMN error_detail TEXT`)
	// #547 records which read and rule judged the submission. Rows written
	// before this column existed read back as empty.
	_, _ = db.Exec(`ALTER TABLE deliveries ADD COLUMN submission_evidence TEXT`)
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS delivery_bodies (
	 delivery_id TEXT PRIMARY KEY REFERENCES deliveries(delivery_id) ON DELETE CASCADE,
	 body TEXT NOT NULL
)`); err != nil {
		db.Close()
		return nil, err
	}
	// R20 moves the node's "already relayed" mark off the heap. A restart used
	// to replay every retained event because the marker lived in memory only.
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS relay_sent (
	 kind TEXT NOT NULL, job_id TEXT NOT NULL, epoch INTEGER NOT NULL, report_path TEXT NOT NULL, reason TEXT NOT NULL, lane TEXT NOT NULL DEFAULT '', event_id TEXT NOT NULL DEFAULT '',
	 sent_at INTEGER, persisted_at INTEGER, suppressed_at INTEGER,
	 PRIMARY KEY(kind, job_id, epoch, report_path, reason, event_id)
)`); err != nil {
		db.Close()
		return nil, err
	}
	// R21 adds a direct-address key beside the historic five-field job key.
	// The producer's event identity is now part of the job.* key as well: five
	// shared fields cannot tell a resend of one completion from the same job's
	// next round, which is how later rounds were silently discarded. SQLite
	// cannot extend a primary key in place, so an existing table is rebuilt
	// row-for-row; rows written before event_id existed keep the empty string
	// and stay inert - the per-node migration cutoff suppresses their event
	// files before they reach the send gate.
	_, _ = db.Exec(`ALTER TABLE relay_sent ADD COLUMN lane TEXT NOT NULL DEFAULT ''`)
	_, _ = db.Exec(`ALTER TABLE relay_sent ADD COLUMN event_id TEXT NOT NULL DEFAULT ''`)
	// relay_meta holds this node's migration stamp; it is created before the
	// rekey so the migration can record itself in the same transaction.
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS relay_meta (
	 key TEXT PRIMARY KEY, value INTEGER NOT NULL
)`); err != nil {
		db.Close()
		return nil, err
	}
	var eventIDInPrimaryKey int
	if err := db.QueryRow(`SELECT pk FROM pragma_table_info('relay_sent') WHERE name='event_id'`).Scan(&eventIDInPrimaryKey); err != nil {
		db.Close()
		return nil, err
	}
	if eventIDInPrimaryKey == 0 {
		tx, err := db.Begin()
		if err != nil {
			db.Close()
			return nil, err
		}
		// Only a rekey that actually carried legacy job.* rows earns the
		// migration stamp. A fresh or rowless database has no migration to
		// date: the old binary demonstrably never relayed a job event from
		// it, so stamping one would mint a cutoff out of nothing and
		// permanently suppress completions that were still owed.
		var legacyJobRows int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM relay_sent WHERE kind<>'lane.event'`).Scan(&legacyJobRows); err != nil {
			tx.Rollback()
			db.Close()
			return nil, err
		}
		// The outer err must see every failure: a shadowed one here let a
		// failed copy fall through to DROP+RENAME+commit, silently replacing
		// the populated legacy table with an empty rekeyed one.
		if _, err = tx.Exec(`CREATE TABLE relay_sent_rekeyed (
	 kind TEXT NOT NULL, job_id TEXT NOT NULL, epoch INTEGER NOT NULL, report_path TEXT NOT NULL, reason TEXT NOT NULL, lane TEXT NOT NULL DEFAULT '', event_id TEXT NOT NULL DEFAULT '',
	 sent_at INTEGER, persisted_at INTEGER, suppressed_at INTEGER,
	 PRIMARY KEY(kind, job_id, epoch, report_path, reason, event_id)
)`); err == nil {
			_, err = tx.Exec(`INSERT INTO relay_sent_rekeyed SELECT kind,job_id,epoch,report_path,reason,lane,event_id,sent_at,persisted_at,NULL FROM relay_sent`)
		}
		if err == nil {
			_, err = tx.Exec(`DROP TABLE relay_sent`)
		}
		if err == nil {
			_, err = tx.Exec(`ALTER TABLE relay_sent_rekeyed RENAME TO relay_sent`)
		}
		// The instant this node's outbox was rekeyed onto event identity is
		// the base of its deployment cutoff. It is per-node because rollouts
		// are sequential: a global constant would suppress ordinary
		// completions on the nodes that migrate last. INSERT OR IGNORE keeps
		// the first - and truest - stamp.
		if err == nil && legacyJobRows > 0 {
			_, err = tx.Exec(`INSERT OR IGNORE INTO relay_meta(key,value) VALUES('event_id_since',?)`, time.Now().UnixMilli())
		}
		if err != nil {
			tx.Rollback()
			db.Close()
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			db.Close()
			return nil, err
		}
	}
	// Covers databases already rekeyed by a build that predates the column.
	_, _ = db.Exec(`ALTER TABLE relay_sent ADD COLUMN suppressed_at INTEGER`)
	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS relay_sent_lane_event_idempotency ON relay_sent(lane,event_id) WHERE kind='lane.event'`); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS relay_sent_job_event_idempotency ON relay_sent(kind,job_id,event_id) WHERE kind<>'lane.event' AND event_id<>''`); err != nil {
		db.Close()
		return nil, err
	}
	// R27 keeps delayed relay delivery on the receiving node.  handoffkeep is
	// still the durable record of what was sent; this table only answers when a
	// local pane may safely receive it.
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS relay_held (
	 pane TEXT NOT NULL, lane TEXT NOT NULL, event_id INTEGER NOT NULL, job_id TEXT NOT NULL,
	 text TEXT NOT NULL, held_since INTEGER NOT NULL, deliver_policy TEXT NOT NULL,
	 max_wait INTEGER NOT NULL, recv_seq INTEGER NOT NULL, edited INTEGER NOT NULL DEFAULT 0,
	 attempts INTEGER NOT NULL DEFAULT 0,
	 PRIMARY KEY(lane,event_id)
)`); err != nil {
		db.Close()
		return nil, err
	}
	// Kept additive for databases created by an early R27 build.
	_, _ = db.Exec(`ALTER TABLE relay_held ADD COLUMN edited INTEGER NOT NULL DEFAULT 0`)
	// #264 D1: bounds the inject-retry rearm loop across a store round-trip.
	_, _ = db.Exec(`ALTER TABLE relay_held ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0`)
	// #449: lease is the pane-occupant identity captured when the row was held.
	// Rows from before this column exist carry '', which the release gate
	// treats as unverifiable — never as a match. Unlike the columns above the
	// presence check is explicit so a real ALTER failure (not "already there")
	// cannot be swallowed silently.
	if !storeHasColumn(db, "relay_held", "lease") {
		if _, err := db.Exec(`ALTER TABLE relay_held ADD COLUMN lease TEXT NOT NULL DEFAULT ''`); err != nil {
			db.Close()
			return nil, err
		}
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS relay_held_pane_recv_seq ON relay_held(pane,recv_seq)`); err != nil {
		db.Close()
		return nil, err
	}
	// #725: relay_delivered is the node's durable answer to "did this pane
	// already receive this relay event". event_id is handoffkeep's row id,
	// which the lane.event idempotency index pins 1:1 to (owner_lane,
	// producer event_id), so the pair (lane, event_id) is the canonical
	// event identity the dedupe contract names. The row is written only
	// after the inject verdict proves the text reached the pane; a payload
	// or destination fingerprint mismatch is recorded, never suppressed.
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS relay_delivered (
	 lane TEXT NOT NULL, event_id INTEGER NOT NULL, pane TEXT NOT NULL,
	 payload_sha TEXT NOT NULL, delivered_at INTEGER NOT NULL,
	 PRIMARY KEY(lane,event_id)
	)`); err != nil {
		db.Close()
		return nil, err
	}
	// ROB-1353 keeps its observation sequence and settle candidates in the
	// node journal.  These tables are deliberately separate from relay_sent:
	// observation is not durable delivery, and only the existing R21 outbox may
	// answer whether a lane.event has been accepted by handoffkeep.
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS idle_wake_meta (
	 key TEXT PRIMARY KEY, value TEXT NOT NULL
	)`); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS idle_wake_panes (
	 pane_id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL DEFAULT '', label TEXT NOT NULL DEFAULT '',
	 agent_status TEXT NOT NULL, state_change_seq INTEGER NOT NULL, upstream_revision INTEGER NOT NULL DEFAULT 0,
	 upstream_state_change_seq INTEGER NOT NULL DEFAULT 0,
	 upstream_generation INTEGER NOT NULL DEFAULT 1, changed_at INTEGER NOT NULL
	)`); err != nil {
		db.Close()
		return nil, err
	}
	// Additive migration for journals created by the first ROB-1353 candidate.
	// Inspect first so only the expected already-migrated case is skipped;
	// incompatible schemas and other SQLite failures must fail OpenStore.
	var sourceSequenceColumn string
	columnErr := db.QueryRow(`SELECT name FROM pragma_table_info('idle_wake_panes') WHERE name='upstream_state_change_seq'`).Scan(&sourceSequenceColumn)
	if columnErr == sql.ErrNoRows {
		if _, err := db.Exec(`ALTER TABLE idle_wake_panes ADD COLUMN upstream_state_change_seq INTEGER NOT NULL DEFAULT 0`); err != nil {
			db.Close()
			return nil, err
		}
	} else if columnErr != nil {
		db.Close()
		return nil, columnErr
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS idle_wake_candidates (
	 pane_id TEXT NOT NULL, state_change_seq INTEGER NOT NULL, workspace_id TEXT NOT NULL DEFAULT '',
	 label TEXT NOT NULL DEFAULT '', agent_status TEXT NOT NULL, changed_at INTEGER NOT NULL,
	 due_at INTEGER NOT NULL, settled_at INTEGER, route_requested_at INTEGER,
	 decision TEXT NOT NULL DEFAULT '', decision_reason TEXT NOT NULL DEFAULT '', owner_lane TEXT NOT NULL DEFAULT '',
	 event_id TEXT NOT NULL UNIQUE, text TEXT NOT NULL DEFAULT '', job_id TEXT NOT NULL DEFAULT '', materialized_at INTEGER,
	 PRIMARY KEY(pane_id,state_change_seq)
	)`); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idle_wake_candidates_due ON idle_wake_candidates(decision,settled_at,due_at)`); err != nil {
		db.Close()
		return nil, err
	}
	if err := stallDetectMigrate(db); err != nil {
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
	var submissionEvidence sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT delivery_id,requested_at_ms,completed_at_ms,sender,target_input,
resolved_pane_id,resolved_workspace_id,source_path,prompt_sha256,body_stored,preflight_revision,send_revision,
preflight_read_sha256,preflight_result,herdr_acceptance,submission_result,uptake_mode,uptake_result,evidence_revision,error_code,error_detail,submission_evidence
	FROM deliveries WHERE delivery_id=?`, id).Scan(&d.DeliveryID, &requested, &completed, &d.Sender, &d.TargetInput,
		&d.ResolvedPaneID, &d.ResolvedWorkspaceID, &d.SourcePath, &d.PromptSHA256, &stored, &preflight, &send,
		&d.PreflightReadSHA256, &d.PreflightResult, &d.HerdrAcceptance, &d.SubmissionResult, &d.UptakeMode, &d.UptakeResult, &evidence, &d.ErrorCode, &d.ErrorDetail, &submissionEvidence)
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
	d.SubmissionEvidence = submissionEvidence.String
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
preflight_read_sha256,preflight_result,herdr_acceptance,submission_result,uptake_mode,uptake_result,evidence_revision,error_code,error_detail,submission_evidence)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, d.DeliveryID, d.RequestedAtMS, d.CompletedAtMS, d.Sender, d.TargetInput,
		d.ResolvedPaneID, d.ResolvedWorkspaceID, d.SourcePath, d.PromptSHA256, boolInt(d.BodyStored), nullableInt(d.PreflightRevision),
		nullableInt(d.SendRevision), d.PreflightReadSHA256, d.PreflightResult, d.HerdrAcceptance, d.SubmissionResult, d.UptakeMode,
		d.UptakeResult, nullableInt(d.EvidenceRevision), d.ErrorCode, d.ErrorDetail, d.SubmissionEvidence)
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
uptake_mode=?,uptake_result=?,evidence_revision=?,error_code=?,error_detail=?,submission_evidence=? WHERE delivery_id=?`, d.CompletedAtMS, d.ResolvedPaneID,
		d.ResolvedWorkspaceID, nullableInt(d.PreflightRevision), nullableInt(d.SendRevision), d.PreflightReadSHA256, d.PreflightResult,
		d.HerdrAcceptance, d.SubmissionResult, d.UptakeMode, d.UptakeResult, nullableInt(d.EvidenceRevision), d.ErrorCode, d.ErrorDetail, d.SubmissionEvidence, d.DeliveryID)
	return err
}

func (s *Store) DeleteDelivery(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `DELETE FROM deliveries WHERE delivery_id=?`, id)
	return err
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

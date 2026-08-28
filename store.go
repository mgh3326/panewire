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
	for _, q := range []string{"PRAGMA journal_mode=WAL", "PRAGMA foreign_keys=ON", `CREATE TABLE IF NOT EXISTS events (
 id INTEGER PRIMARY KEY AUTOINCREMENT, observed_at_ms INTEGER NOT NULL, source TEXT NOT NULL,
 event_kind TEXT NOT NULL, protocol INTEGER, schema_version INTEGER, pane_id TEXT, workspace_id TEXT,
 agent TEXT, agent_status TEXT, revision INTEGER, path TEXT, payload_json TEXT NOT NULL, unknown_fields_json TEXT
)`, `CREATE TABLE IF NOT EXISTS deliveries (
 delivery_id TEXT PRIMARY KEY, requested_at_ms INTEGER, completed_at_ms INTEGER, sender TEXT NOT NULL DEFAULT '',
 target_input TEXT NOT NULL DEFAULT '', resolved_pane_id TEXT, resolved_workspace_id TEXT, source_path TEXT NOT NULL DEFAULT '',
 prompt_sha256 TEXT NOT NULL DEFAULT '', body_stored INTEGER NOT NULL DEFAULT 0, preflight_revision INTEGER,
 send_revision INTEGER, preflight_read_sha256 TEXT, preflight_result TEXT NOT NULL DEFAULT '', herdr_acceptance TEXT,
 submission_result TEXT, uptake_mode TEXT, uptake_result TEXT, evidence_revision INTEGER, error_code TEXT
)`, `CREATE INDEX IF NOT EXISTS events_kind_time ON events(event_kind, observed_at_ms)`, `CREATE INDEX IF NOT EXISTS events_pane_time ON events(pane_id, observed_at_ms)`} {
		if _, err := db.Exec(q); err != nil {
			db.Close()
			return nil, err
		}
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

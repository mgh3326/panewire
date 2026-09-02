package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	contextMaxBytes       = 64 << 10
	contextCheckpointKeep = 500
	contextMemoryKeep     = 2000
)

var (
	ErrContextTrigramUnavailable = errors.New("PostgreSQL pg_trgm extension unavailable")
	contextKinds                 = map[string]bool{"checkpoint": true, "handoff": true, "decision": true, "open_question": true, "next_action": true}
	memoryTypes                  = map[string]bool{"user": true, "feedback": true, "project": true, "reference": true}
	contextNamePattern           = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)
	secretLikePatterns           = []struct {
		name string
		re   *regexp.Regexp
	}{
		{"openai_key", regexp.MustCompile(`(?i)\bsk-[A-Za-z0-9_-]{8,}`)},
		{"github_token", regexp.MustCompile(`\bghp_[A-Za-z0-9]{8,}`)},
		{"slack_token", regexp.MustCompile(`\bxoxb-[A-Za-z0-9-]{8,}`)},
		{"aws_access_key", regexp.MustCompile(`\bAKIA[A-Z0-9]{8,}`)},
		{"private_key", regexp.MustCompile(`-----BEGIN`)},
		{"bearer_token", regexp.MustCompile(`(?i)\bBearer [A-Za-z0-9._-]{20,}`)},
		{"token_assignment", regexp.MustCompile(`(?m)\b[A-Z_]*(?:TOKEN|SECRET|KEY)=[^\s]{12,}`)},
		{"github_fine_grained_token", regexp.MustCompile(`\bgithub_pat_[A-Za-z0-9_]{8,}`)},
	}
)

// ContextStore is the hub's PostgreSQL-backed cross-machine context authority.
// It deliberately does not share the daemon's local SQLite Store.
type ContextStore struct {
	db *sql.DB
	mu sync.Mutex
}
type ContextCheckpoint struct {
	ID        int64             `json:"id"`
	Session   string            `json:"session"`
	Kind      string            `json:"kind"`
	Title     string            `json:"title"`
	Body      string            `json:"body"`
	Refs      map[string]string `json:"refs"`
	CreatedBy string            `json:"created_by"`
	CreatedAt time.Time         `json:"created_at"`
}
type ContextMemory struct {
	Agent       string    `json:"agent"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	MemoryType  string    `json:"type"`
	Content     string    `json:"content,omitempty"`
	UpdatedBy   string    `json:"updated_by,omitempty"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func OpenContextStore(databaseURL string) (*ContextStore, error) {
	if !strings.HasPrefix(databaseURL, "postgres://") && !strings.HasPrefix(databaseURL, "postgresql://") {
		return nil, errors.New("context database URL must be PostgreSQL")
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	for _, statement := range []string{
		`CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY)`,
		`INSERT INTO schema_version(version) VALUES (1) ON CONFLICT DO NOTHING`,
		`CREATE EXTENSION IF NOT EXISTS pg_trgm`,
		`CREATE TABLE IF NOT EXISTS checkpoints (id BIGSERIAL PRIMARY KEY, session TEXT NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('checkpoint','handoff','decision','open_question','next_action')), title TEXT NOT NULL, body TEXT NOT NULL, refs JSONB NOT NULL DEFAULT '{}'::jsonb, created_by TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS checkpoints_session_created ON checkpoints(session, created_at DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS checkpoints_fts ON checkpoints USING GIN (to_tsvector('simple', title || ' ' || body))`,
		`CREATE INDEX IF NOT EXISTS checkpoints_trgm ON checkpoints USING GIN ((title || ' ' || body) gin_trgm_ops)`,
		`CREATE TABLE IF NOT EXISTS memory (agent TEXT NOT NULL, name TEXT NOT NULL, description TEXT NOT NULL, memory_type TEXT NOT NULL CHECK(memory_type IN ('user','feedback','project','reference')), content TEXT NOT NULL, updated_by TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL, UNIQUE(agent,name))`,
		`CREATE INDEX IF NOT EXISTS memory_agent_updated ON memory(agent, updated_at DESC, name DESC)`,
		`CREATE INDEX IF NOT EXISTS memory_fts ON memory USING GIN (to_tsvector('simple', name || ' ' || description || ' ' || content))`,
		`CREATE INDEX IF NOT EXISTS memory_trgm ON memory USING GIN ((name || ' ' || description || ' ' || content) gin_trgm_ops)`,
		`CREATE TABLE IF NOT EXISTS documents (id BIGSERIAL PRIMARY KEY, key TEXT UNIQUE NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('brief','report','answer','handoff','note','other')), session TEXT NOT NULL DEFAULT '', job TEXT NOT NULL DEFAULT '', body TEXT NOT NULL, sha256 TEXT NOT NULL, created_by TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS documents_prefix ON documents(key)`,
		`CREATE INDEX IF NOT EXISTS documents_fts ON documents USING GIN (to_tsvector('simple', key || ' ' || body))`,
		`CREATE INDEX IF NOT EXISTS documents_trgm ON documents USING GIN ((key || ' ' || body) gin_trgm_ops)`,
		`INSERT INTO schema_version(version) VALUES (2) ON CONFLICT DO NOTHING`,
	} {
		if _, err := db.Exec(statement); err != nil {
			_ = db.Close()
			if statement == `CREATE EXTENSION IF NOT EXISTS pg_trgm` {
				return nil, ErrContextTrigramUnavailable
			}
			return nil, err
		}
	}
	return &ContextStore{db: db}, nil
}
func (s *ContextStore) Close() error { return s.db.Close() }
func contextSecret(content string) string {
	for _, p := range secretLikePatterns {
		if p.re.MatchString(content) {
			return p.name
		}
	}
	return ""
}
func validContextText(value string) bool {
	return len(value) <= contextMaxBytes && strings.IndexByte(value, 0) < 0
}
func validContextName(value string) bool { return contextNamePattern.MatchString(value) }

func (s *ContextStore) CreateCheckpoint(ctx context.Context, item ContextCheckpoint) (ContextCheckpoint, error) {
	if !validContextName(item.Session) || !contextKinds[item.Kind] || item.Title == "" || !validContextText(item.Title) || !validContextText(item.Body) || !validContextName(item.CreatedBy) {
		return item, errors.New("invalid context checkpoint")
	}
	if secret := contextSecret(item.Title + "\n" + item.Body); secret != "" {
		return item, fmt.Errorf("secret_like_content:%s", secret)
	}
	refs, err := json.Marshal(item.Refs)
	if err != nil || len(refs) > contextMaxBytes {
		return item, errors.New("invalid checkpoint refs")
	}
	if secret := contextSecret(string(refs)); secret != "" {
		return item, fmt.Errorf("secret_like_content:%s", secret)
	}
	item.CreatedAt = time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return item, err
	}
	defer tx.Rollback()
	err = tx.QueryRowContext(ctx, `INSERT INTO checkpoints(session,kind,title,body,refs,created_by,created_at) VALUES($1,$2,$3,$4,$5::jsonb,$6,$7) RETURNING id`, item.Session, item.Kind, item.Title, item.Body, string(refs), item.CreatedBy, item.CreatedAt).Scan(&item.ID)
	if err != nil {
		return item, err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM checkpoints WHERE session=$1 AND id IN (SELECT id FROM checkpoints WHERE session=$1 ORDER BY created_at DESC,id DESC OFFSET $2)`, item.Session, contextCheckpointKeep); err != nil {
		return item, err
	}
	return item, tx.Commit()
}
func (s *ContextStore) RecentCheckpoints(ctx context.Context, session, kind string, limit int) ([]ContextCheckpoint, error) {
	if !validContextName(session) || (kind != "" && !contextKinds[kind]) {
		return nil, errors.New("invalid context query")
	}
	if limit <= 0 {
		limit = 3
	}
	if limit > contextCheckpointKeep {
		limit = contextCheckpointKeep
	}
	q := `SELECT id,session,kind,title,body,refs,created_by,created_at FROM checkpoints WHERE session=$1`
	args := []any{session}
	if kind != "" {
		q += " AND kind=$2 ORDER BY created_at DESC,id DESC LIMIT $3"
		args = append(args, kind, limit)
	} else {
		q += " ORDER BY created_at DESC,id DESC LIMIT $2"
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ContextCheckpoint
	for rows.Next() {
		var item ContextCheckpoint
		var refs []byte
		if err := rows.Scan(&item.ID, &item.Session, &item.Kind, &item.Title, &item.Body, &refs, &item.CreatedBy, &item.CreatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(refs, &item.Refs); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
func (s *ContextStore) PutMemory(ctx context.Context, item ContextMemory) (ContextMemory, error) {
	if !validContextName(item.Agent) || !validContextName(item.Name) || !memoryTypes[item.MemoryType] || !validContextText(item.Description) || !validContextText(item.Content) || !validContextName(item.UpdatedBy) {
		return item, errors.New("invalid context memory")
	}
	if secret := contextSecret(item.Description + "\n" + item.Content); secret != "" {
		return item, fmt.Errorf("secret_like_content:%s", secret)
	}
	item.UpdatedAt = time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return item, err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO memory(agent,name,description,memory_type,content,updated_by,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(agent,name) DO UPDATE SET description=EXCLUDED.description,memory_type=EXCLUDED.memory_type,content=EXCLUDED.content,updated_by=EXCLUDED.updated_by,updated_at=EXCLUDED.updated_at`, item.Agent, item.Name, item.Description, item.MemoryType, item.Content, item.UpdatedBy, item.UpdatedAt)
	if err != nil {
		return item, err
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM memory WHERE agent=$1 AND name IN (SELECT name FROM memory WHERE agent=$1 ORDER BY updated_at DESC,name DESC OFFSET $2)`, item.Agent, contextMemoryKeep)
	if err != nil {
		return item, err
	}
	return item, tx.Commit()
}
func (s *ContextStore) ListMemory(ctx context.Context, agent string, includeContent bool) ([]ContextMemory, error) {
	if !validContextName(agent) {
		return nil, errors.New("invalid memory agent")
	}
	columns := `agent,name,description,memory_type,updated_by,updated_at`
	if includeContent {
		columns = `agent,name,description,memory_type,content,updated_by,updated_at`
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+columns+` FROM memory WHERE agent=$1 ORDER BY name`, agent)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ContextMemory
	for rows.Next() {
		var item ContextMemory
		if includeContent {
			err = rows.Scan(&item.Agent, &item.Name, &item.Description, &item.MemoryType, &item.Content, &item.UpdatedBy, &item.UpdatedAt)
		} else {
			err = rows.Scan(&item.Agent, &item.Name, &item.Description, &item.MemoryType, &item.UpdatedBy, &item.UpdatedAt)
		}
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
func (s *ContextStore) GetMemory(ctx context.Context, agent, name string) (ContextMemory, bool, error) {
	if !validContextName(agent) || !validContextName(name) {
		return ContextMemory{}, false, errors.New("invalid memory name")
	}
	var item ContextMemory
	err := s.db.QueryRowContext(ctx, `SELECT agent,name,description,memory_type,content,updated_by,updated_at FROM memory WHERE agent=$1 AND name=$2`, agent, name).Scan(&item.Agent, &item.Name, &item.Description, &item.MemoryType, &item.Content, &item.UpdatedBy, &item.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return item, false, nil
	}
	return item, err == nil, err
}
func (s *ContextStore) DeleteMemory(ctx context.Context, agent, name string) error {
	if !validContextName(agent) || !validContextName(name) {
		return errors.New("invalid memory name")
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM memory WHERE agent=$1 AND name=$2`, agent, name)
	return err
}

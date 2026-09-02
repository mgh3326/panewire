package panewire

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"
)

const contextDocumentMaxBytes = 512 << 10

var documentKinds = map[string]bool{"brief": true, "report": true, "answer": true, "handoff": true, "note": true, "other": true}

type ContextDocument struct {
	ID        int64     `json:"id"`
	Key       string    `json:"key"`
	Kind      string    `json:"kind"`
	Session   string    `json:"session"`
	Job       string    `json:"job"`
	Body      string    `json:"body,omitempty"`
	SHA256    string    `json:"sha256"`
	CreatedBy string    `json:"created_by,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
type ContextSearchResult struct {
	Scope     string    `json:"scope"`
	Key       string    `json:"key,omitempty"`
	Session   string    `json:"session,omitempty"`
	Kind      string    `json:"kind"`
	Title     string    `json:"title"`
	Snippet   string    `json:"snippet"`
	CreatedAt time.Time `json:"created_at"`
}

func validDocumentKey(key string) bool {
	return key != "" && len(key) <= 1024 && strings.IndexByte(key, 0) < 0 && !strings.HasPrefix(key, "/") && !strings.Contains(key, `\`) && path.Clean(key) == key && key != "." && !strings.HasPrefix(key, "../")
}
func validDocumentField(value string) bool {
	return len(value) <= 1024 && strings.IndexByte(value, 0) < 0
}

func (s *ContextStore) PutDocument(ctx context.Context, item ContextDocument) (ContextDocument, bool, error) {
	if !validDocumentKey(item.Key) || !documentKinds[item.Kind] || !validDocumentField(item.Session) || !validDocumentField(item.Job) || len(item.Body) > contextDocumentMaxBytes || strings.IndexByte(item.Body, 0) >= 0 || !validContextName(item.CreatedBy) {
		return item, false, errors.New("invalid context document")
	}
	if secret := contextSecret(item.Body); secret != "" {
		return item, false, fmt.Errorf("secret_like_content:%s", secret)
	}
	digest := sha256.Sum256([]byte(item.Body))
	item.SHA256 = fmt.Sprintf("%x", digest[:])
	item.UpdatedAt = time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	var existing ContextDocument
	err := s.db.QueryRowContext(ctx, `SELECT id,key,kind,session,job,body,sha256,created_by,created_at,updated_at FROM documents WHERE key=$1`, item.Key).Scan(&existing.ID, &existing.Key, &existing.Kind, &existing.Session, &existing.Job, &existing.Body, &existing.SHA256, &existing.CreatedBy, &existing.CreatedAt, &existing.UpdatedAt)
	if err == nil && existing.SHA256 == item.SHA256 {
		return existing, false, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return item, false, err
	}
	if errors.Is(err, sql.ErrNoRows) {
		item.CreatedAt = item.UpdatedAt
		err = s.db.QueryRowContext(ctx, `INSERT INTO documents(key,kind,session,job,body,sha256,created_by,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id`, item.Key, item.Kind, item.Session, item.Job, item.Body, item.SHA256, item.CreatedBy, item.CreatedAt, item.UpdatedAt).Scan(&item.ID)
		return item, err == nil, err
	}
	item.ID, item.CreatedBy, item.CreatedAt = existing.ID, existing.CreatedBy, existing.CreatedAt
	err = s.db.QueryRowContext(ctx, `UPDATE documents SET kind=$2,session=$3,job=$4,body=$5,sha256=$6,updated_at=$7 WHERE key=$1 RETURNING id`, item.Key, item.Kind, item.Session, item.Job, item.Body, item.SHA256, item.UpdatedAt).Scan(&item.ID)
	return item, err == nil, err
}
func (s *ContextStore) GetDocument(ctx context.Context, key string) (ContextDocument, bool, error) {
	if !validDocumentKey(key) {
		return ContextDocument{}, false, errors.New("invalid document key")
	}
	var item ContextDocument
	err := s.db.QueryRowContext(ctx, `SELECT id,key,kind,session,job,body,sha256,created_by,created_at,updated_at FROM documents WHERE key=$1`, key).Scan(&item.ID, &item.Key, &item.Kind, &item.Session, &item.Job, &item.Body, &item.SHA256, &item.CreatedBy, &item.CreatedAt, &item.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return item, false, nil
	}
	return item, err == nil, err
}
func (s *ContextStore) ListDocuments(ctx context.Context, prefix, kind, session string, limit int) ([]ContextDocument, error) {
	if (prefix != "" && (!strings.HasSuffix(prefix, "/") || !validDocumentKey(strings.TrimSuffix(prefix, "/")))) || (kind != "" && !documentKinds[kind]) || !validDocumentField(session) {
		return nil, errors.New("invalid document query")
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,key,kind,session,job,sha256,created_by,created_at,updated_at FROM documents WHERE ($1='' OR key LIKE $1 || '%') AND ($2='' OR kind=$2) AND ($3='' OR session=$3) ORDER BY key LIMIT $4`, prefix, kind, session, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ContextDocument
	for rows.Next() {
		var item ContextDocument
		if err := rows.Scan(&item.ID, &item.Key, &item.Kind, &item.Session, &item.Job, &item.SHA256, &item.CreatedBy, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
func (s *ContextStore) Search(ctx context.Context, query, scope, session, kind string, limit int) ([]ContextSearchResult, error) {
	if strings.TrimSpace(query) == "" || len(query) > 512 || !validDocumentField(session) || (scope != "ctx" && scope != "docs" && scope != "all") {
		return nil, errors.New("invalid context search")
	}
	if kind != "" && !contextKinds[kind] && !memoryTypes[kind] && !documentKinds[kind] {
		return nil, errors.New("invalid context search")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	parts := make([]string, 0, 3)
	if scope == "ctx" || scope == "all" {
		parts = append(parts, `SELECT 'ctx' AS scope,'' AS key,session,kind,title,ts_headline('simple',body,plainto_tsquery('simple',$3),'MaxWords=24,MinWords=8') AS snippet,created_at FROM checkpoints WHERE ($1='' OR session=$1) AND ($2='' OR kind=$2) AND (to_tsvector('simple',title || ' ' || body) @@ plainto_tsquery('simple',$3) OR title || ' ' || body ILIKE '%' || $3 || '%')`, `SELECT 'memory' AS scope,'' AS key,agent AS session,memory_type AS kind,name AS title,ts_headline('simple',content,plainto_tsquery('simple',$3),'MaxWords=24,MinWords=8') AS snippet,updated_at AS created_at FROM memory WHERE ($1='' OR agent=$1) AND ($2='' OR memory_type=$2) AND (to_tsvector('simple',name || ' ' || description || ' ' || content) @@ plainto_tsquery('simple',$3) OR name || ' ' || description || ' ' || content ILIKE '%' || $3 || '%')`)
	}
	if scope == "docs" || scope == "all" {
		parts = append(parts, `SELECT 'docs' AS scope,key,session,kind,key AS title,ts_headline('simple',body,plainto_tsquery('simple',$3),'MaxWords=24,MinWords=8') AS snippet,created_at FROM documents WHERE ($1='' OR session=$1) AND ($2='' OR kind=$2) AND (to_tsvector('simple',key || ' ' || body) @@ plainto_tsquery('simple',$3) OR key || ' ' || body ILIKE '%' || $3 || '%')`)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT scope,key,session,kind,title,snippet,created_at FROM (`+strings.Join(parts, ` UNION ALL `)+`) AS search_results ORDER BY created_at DESC LIMIT $4`, session, kind, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ContextSearchResult
	for rows.Next() {
		var item ContextSearchResult
		if err := rows.Scan(&item.Scope, &item.Key, &item.Session, &item.Kind, &item.Title, &item.Snippet, &item.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

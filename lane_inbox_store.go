package panewire

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// This is the hub's durable queue, not the node's filesystem emit spool.
// Synchronous commits precede producer ACKs. The SQLite database and its WAL
// belong on persistent storage and must be backed up together using SQLite;
// the Postgres backend (lane_inbox_store_postgres.go) holds the same tables.
// Both backends run the same statements through s.q, which only rebinds
// placeholders, so one method set is the store contract for either.
type laneInboxStore struct {
	db      *sql.DB
	dialect laneInboxDialect
	mu      sync.Mutex
	// notify is closed and replaced when a new inbox-class item lands, waking
	// every long-poll waiter so they re-check for unread work.
	notify chan struct{}
}

type laneInboxItem struct {
	Fresh           bool               `json:"-"`
	ID              int64              `json:"id"`
	Lane            string             `json:"lane"`
	Kind            string             `json:"kind"`
	Class           string             `json:"delivery_class"`
	Event           hubJobEventPayload `json:"event"`
	CreatedAt       int64              `json:"created_at_ms"`
	ReadAt          int64              `json:"read_at_ms,omitempty"`
	AckedAt         int64              `json:"acked_at_ms,omitempty"`
	DueAt           int64              `json:"due_at_ms,omitempty"`
	DigestAt        int64              `json:"digest_at_ms,omitempty"`
	DigestBatch     string             `json:"digest_batch,omitempty"`
	EscalationBatch string             `json:"escalation_batch,omitempty"`
}

type laneInboxBatch struct {
	ID   string
	Lane string
	Text string
}

func (b laneInboxBatch) invalidReason() string {
	if len(b.Text) > laneEventTextLimit {
		return "lane.event text exceeds 2048 bytes"
	}
	if !validLaneEventText(b.Text) {
		return "lane.event text is empty, invalid UTF-8, or contains control characters"
	}
	if !validLaneEventID(b.ID) || !hubAgentLabelPattern.MatchString(b.Lane) {
		return "invalid lane.event batch identity or lane"
	}
	return ""
}

func openLaneInboxStore(path string) (*laneInboxStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	err = f.Chmod(0600)
	closeErr := f.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, q := range []string{
		`PRAGMA busy_timeout=5000`, `PRAGMA journal_mode=WAL`, `PRAGMA synchronous=FULL`,
		`CREATE TABLE IF NOT EXISTS lane_inbox (
   id INTEGER PRIMARY KEY, event_key TEXT NOT NULL UNIQUE, lane TEXT NOT NULL,
   kind TEXT NOT NULL, class TEXT NOT NULL CHECK(class IN ('inbox','interrupt')),
   payload TEXT NOT NULL, created_at INTEGER NOT NULL, read_at INTEGER NOT NULL DEFAULT 0,
   acked_at INTEGER NOT NULL DEFAULT 0, due_at INTEGER NOT NULL DEFAULT 0,
   digest_at INTEGER NOT NULL DEFAULT 0, digest_batch TEXT NOT NULL DEFAULT '',
   escalation_batch TEXT NOT NULL DEFAULT '')`,
		`CREATE INDEX IF NOT EXISTS lane_inbox_unread ON lane_inbox(lane) WHERE class='inbox' AND read_at=0`,
		`CREATE INDEX IF NOT EXISTS lane_inbox_due ON lane_inbox(due_at) WHERE class='inbox' AND read_at=0 AND escalation_batch=''`,
		`CREATE INDEX IF NOT EXISTS lane_inbox_digest ON lane_inbox(lane,digest_at) WHERE digest_at>0 AND digest_batch=''`,
		`CREATE TABLE IF NOT EXISTS lane_inbox_batches (id TEXT PRIMARY KEY, lane TEXT NOT NULL, text TEXT NOT NULL, sent INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE IF NOT EXISTS lane_inbox_batch_quarantine (batch_id TEXT PRIMARY KEY, reason TEXT NOT NULL)`,
	} {
		if _, err := db.Exec(q); err != nil {
			db.Close()
			return nil, err
		}
	}
	return &laneInboxStore{db: db, dialect: laneInboxSQLite, notify: make(chan struct{})}, nil
}

type laneInboxDialect int

const (
	laneInboxSQLite laneInboxDialect = iota
	laneInboxPostgres
)

// q rebinds the store's ?-placeholders for the backend. No statement in this
// store carries a literal '?', so a positional rewrite is exact.
func (s *laneInboxStore) q(query string) string {
	if s.dialect != laneInboxPostgres {
		return query
	}
	var b strings.Builder
	n := 0
	for _, r := range query {
		if r == '?' {
			n++
			b.WriteString("$" + strconv.Itoa(n))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func (s *laneInboxStore) close() error { return s.db.Close() }

// markBatchSent records that handoffkeep now owns a batch's identity.
func (s *laneInboxStore) markBatchSent(id string) error {
	_, err := s.db.Exec(s.q(`UPDATE lane_inbox_batches SET sent=1 WHERE id=?`), id)
	return err
}

func (s *laneInboxStore) quarantineBatch(id, reason string) error {
	_, err := s.db.Exec(s.q(`INSERT INTO lane_inbox_batch_quarantine(batch_id,reason) VALUES(?,?) ON CONFLICT(batch_id) DO NOTHING`), id, reason)
	return err
}

func (s *laneInboxStore) quarantineInvalidBatches() error {
	rows, err := s.db.Query(`SELECT id,lane,text FROM lane_inbox_batches WHERE sent=0 AND id NOT IN (SELECT batch_id FROM lane_inbox_batch_quarantine)`)
	if err != nil {
		return err
	}
	var invalid []laneInboxBatch
	for rows.Next() {
		var b laneInboxBatch
		if err := rows.Scan(&b.ID, &b.Lane, &b.Text); err != nil {
			rows.Close()
			return err
		}
		if b.invalidReason() != "" {
			invalid = append(invalid, b)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, b := range invalid {
		if err := s.quarantineBatch(b.ID, b.invalidReason()); err != nil {
			return err
		}
	}
	return nil
}

// recoverMustAct scans all unread originals at startup, including rows without
// a deadline and rows claimed by a quarantined legacy batch. A valid existing
// escalation claim stays intact, so restart cannot escalate an item twice.
func (s *laneInboxStore) recoverMustAct(policy *deliveryPolicy) error {
	rows, err := s.db.Query(`SELECT ` + laneInboxColumns + ` FROM lane_inbox WHERE class='inbox' AND read_at=0 AND (escalation_batch='' OR escalation_batch IN (SELECT batch_id FROM lane_inbox_batch_quarantine))`)
	if err != nil {
		return err
	}
	var ids []int64
	for rows.Next() {
		i, err := scanLaneInbox(rows)
		if err != nil {
			rows.Close()
			return err
		}
		if policy.mustAct(i.Lane, i.Event) {
			ids = append(ids, i.ID)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range ids {
		// Any positive timestamp in the past makes it immediately eligible.
		if _, err := s.db.Exec(s.q(`UPDATE lane_inbox SET due_at=1,escalation_batch='' WHERE id=?`), id); err != nil {
			return err
		}
	}
	return nil
}

const laneInboxColumns = `id,lane,kind,class,payload,created_at,read_at,acked_at,due_at,digest_at,digest_batch,escalation_batch`

type inboxScanner interface{ Scan(...any) error }

func scanLaneInbox(row inboxScanner) (laneInboxItem, error) {
	var i laneInboxItem
	var payload string
	err := row.Scan(&i.ID, &i.Lane, &i.Kind, &i.Class, &payload, &i.CreatedAt, &i.ReadAt, &i.AckedAt, &i.DueAt, &i.DigestAt, &i.DigestBatch, &i.EscalationBatch)
	if err == nil {
		err = json.Unmarshal([]byte(payload), &i.Event)
	}
	return i, err
}

// put is first-writer-wins, including the class. A policy edit or source retry
// cannot turn a previously accepted inbox item into a prompt after restart.
func (s *laneInboxStore) put(key, lane, kind, class string, e hubJobEventPayload, now time.Time, deadline, window time.Duration) (laneInboxItem, error) {
	raw, err := json.Marshal(e)
	if err != nil {
		return laneInboxItem{}, err
	}
	due, digest, read := int64(0), int64(0), int64(0)
	if deadline > 0 {
		due = now.Add(deadline).UnixMilli()
	}
	if class == "interrupt" {
		read = now.UnixMilli()
		if window > 0 {
			digest = now.Add(window).UnixMilli()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	insert, err := s.db.Exec(s.q(`INSERT INTO lane_inbox(event_key,lane,kind,class,payload,created_at,read_at,due_at,digest_at) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(event_key) DO NOTHING`), key, lane, kind, class, string(raw), now.UnixMilli(), read, due, digest)
	if err != nil {
		return laneInboxItem{}, err
	}
	item, err := scanLaneInbox(s.db.QueryRow(s.q(`SELECT `+laneInboxColumns+` FROM lane_inbox WHERE event_key=?`), key))
	n, _ := insert.RowsAffected()
	item.Fresh = n == 1
	if n == 1 && class == "inbox" {
		close(s.notify)
		s.notify = make(chan struct{})
	}
	return item, err
}

// waitChan returns the channel closed by the next inbox-class insert. A waiter
// must take it before re-checking unread state so an insert that lands between
// the check and the select still wakes it.
func (s *laneInboxStore) waitChan() chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.notify
}

// firstUnread returns the oldest still-unread inbox item for one lane, or nil
// when the lane has none.
func (s *laneInboxStore) firstUnread(ctx context.Context, lane string) (*laneInboxItem, error) {
	item, err := scanLaneInbox(s.db.QueryRowContext(ctx, s.q(`SELECT `+laneInboxColumns+` FROM lane_inbox WHERE lane=? AND class='inbox' AND read_at=0 ORDER BY id LIMIT 1`), lane))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &item, nil
}

func (s *laneInboxStore) list(lane string, unread bool) ([]laneInboxItem, error) {
	q := `SELECT ` + laneInboxColumns + ` FROM lane_inbox WHERE (?='' OR lane=?)`
	if unread {
		q += ` AND class='inbox' AND read_at=0`
	}
	q += ` ORDER BY id DESC LIMIT 500`
	rows, err := s.db.Query(s.q(q), lane, lane)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []laneInboxItem{}
	for rows.Next() {
		i, err := scanLaneInbox(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	return items, rows.Err()
}

func (s *laneInboxStore) read(id int64, lane string, now time.Time) (laneInboxItem, error) {
	if _, err := s.db.Exec(s.q(`UPDATE lane_inbox SET read_at=CASE WHEN read_at=0 THEN ? ELSE read_at END WHERE id=? AND (?='' OR lane=?)`), now.UnixMilli(), id, lane, lane); err != nil {
		return laneInboxItem{}, err
	}
	return scanLaneInbox(s.db.QueryRow(s.q(`SELECT `+laneInboxColumns+` FROM lane_inbox WHERE id=? AND (?='' OR lane=?)`), id, lane, lane))
}

func (s *laneInboxStore) ack(id int64, lane string, all bool, now time.Time) (int64, error) {
	result, err := s.db.Exec(s.q(`UPDATE lane_inbox SET acked_at=?,read_at=CASE WHEN read_at=0 THEN ? ELSE read_at END WHERE acked_at=0 AND ((? AND (?='' OR lane=?)) OR (NOT ? AND id=? AND (?='' OR lane=?)))`), now.UnixMilli(), now.UnixMilli(), all, lane, lane, all, id, lane, lane)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *laneInboxStore) counts(lane string) (map[string]int, error) {
	rows, err := s.db.Query(s.q(`SELECT lane,count(*) FROM lane_inbox WHERE class='inbox' AND read_at=0 AND (?='' OR lane=?) GROUP BY lane`), lane, lane)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := map[string]int{}
	if lane != "" {
		counts[lane] = 0
	}
	for rows.Next() {
		var l string
		var n int
		if err := rows.Scan(&l, &n); err != nil {
			return nil, err
		}
		counts[l] = n
	}
	return counts, rows.Err()
}

// makeBatches atomically saves a stable outgoing digest and claims its source
// items. A crash before sending leaves a pending batch; a crash after sending
// retries the same relay event ID. Escalations never claim an item twice.
func (s *laneInboxStore) makeBatches(now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, escalation := range []bool{false, true} {
		filter := `class='interrupt' AND digest_at>0 AND digest_batch=''`
		if escalation {
			filter = `class='inbox' AND read_at=0 AND due_at>0 AND due_at<=? AND escalation_batch=''`
		}
		args := []any{}
		if escalation {
			args = append(args, now.UnixMilli())
		}
		rows, err := tx.Query(s.q(`SELECT `+laneInboxColumns+` FROM lane_inbox WHERE `+filter+` ORDER BY id`), args...)
		if err != nil {
			return err
		}
		groups := map[string][]laneInboxItem{}
		for rows.Next() {
			i, e := scanLaneInbox(rows)
			if e != nil {
				rows.Close()
				return e
			}
			groups[i.Lane] = append(groups[i.Lane], i)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		lanes := make([]string, 0, len(groups))
		for lane := range groups {
			lanes = append(lanes, lane)
		}
		sort.Strings(lanes)
		for _, lane := range lanes {
			items := groups[lane]
			if !escalation {
				if items[0].DigestAt > now.UnixMilli() {
					continue
				}
				// A late flush must not widen the configured arrival window.
				cutoff := items[0].DigestAt
				end := 1
				for end < len(items) && items[end].CreatedAt <= cutoff {
					end++
				}
				items = items[:end]
			}
			id := "quiet-digest-" + uuid.NewString()
			// The digest is one line — a count and the inbox pointer — so it
			// always satisfies the lane.event text contract (no control
			// bytes, far under the length cap) and never breaks receivers.
			// Full originals stay in the store, readable by `inbox list`.
			text := fmt.Sprintf("%d interrupt events | lane %s | panewire inbox list --lane %s", len(items), lane, lane)
			column := "digest_batch"
			if escalation {
				text = fmt.Sprintf("%d unread inbox items | lane %s | panewire inbox list --lane %s", len(items), lane, lane)
				column = "escalation_batch"
			}
			if _, err := tx.Exec(s.q(`INSERT INTO lane_inbox_batches(id,lane,text) VALUES(?,?,?)`), id, lane, text); err != nil {
				return err
			}
			for _, i := range items {
				if _, err := tx.Exec(s.q(`UPDATE lane_inbox SET `+column+`=? WHERE id=?`), id, i.ID); err != nil {
					return err
				}
			}
		}
	}
	return tx.Commit()
}

func (s *laneInboxStore) pendingBatches() ([]laneInboxBatch, error) {
	// Batches go out in creation order: SQLite's implicit rowid, or the
	// identity column Postgres needs in its place.
	order := "rowid"
	if s.dialect == laneInboxPostgres {
		order = "seq"
	}
	rows, err := s.db.Query(`SELECT id,lane,text FROM lane_inbox_batches WHERE sent=0 AND id NOT IN (SELECT batch_id FROM lane_inbox_batch_quarantine) ORDER BY ` + order + ` LIMIT 100`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	batches := []laneInboxBatch{}
	for rows.Next() {
		var b laneInboxBatch
		if err := rows.Scan(&b.ID, &b.Lane, &b.Text); err != nil {
			return nil, err
		}
		batches = append(batches, b)
	}
	return batches, rows.Err()
}

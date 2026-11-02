package panewire

import (
	"context"
	"database/sql"
	"encoding/hex"
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
	// life ends when abort is called: every in-flight and later call fails
	// at once, so shutdown never waits on a stalled database.
	life  context.Context
	abort context.CancelFunc
	mu    sync.Mutex
	// notify is closed and replaced when a new unread item lands (an
	// inbox-class item or a must-ack one), waking every long-poll waiter so
	// they re-check for unread work.
	notify chan struct{}
}

type laneInboxItem struct {
	HoldUntil       int64              `json:"hold_until_ms,omitempty"`
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
	// MustAck marks an item that stays unread until `inbox ack`, even after
	// it was injected into a pane or read (mustAckNotifyTypes).
	MustAck bool `json:"must_ack,omitempty"`
}

// laneInboxUnread is the unread predicate every reader shares: a must-ack
// item until it is acked, any other inbox-class item until it is read or
// acked. Interrupt items without must-ack are read when stored.
const laneInboxUnread = `((must_ack=1 AND acked_at=0) OR (must_ack=0 AND class='inbox' AND read_at=0))`

type laneInboxBatch struct {
	ID   string
	Lane string
	Text string
}

func (b laneInboxBatch) invalidReason() string {
	if len(b.Text) > laneEventTextLimit && !strings.HasPrefix(b.ID, "held-") {
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
	ctx, cancel := context.WithTimeout(context.Background(), laneInboxOpTimeout)
	defer cancel()
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
		if _, err := db.ExecContext(ctx, q); err != nil {
			db.Close()
			return nil, err
		}
	}
	if err := addLaneInboxColumn(ctx, db, "hold_until"); err != nil {
		db.Close()
		return nil, err
	}
	if err := addLaneInboxMustAck(ctx, db); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS lane_inbox_hold ON lane_inbox(lane,hold_until) WHERE hold_until>0 AND digest_batch='' AND acked_at=0`); err != nil {
		db.Close()
		return nil, err
	}
	return newLaneInboxStore(db, laneInboxSQLite), nil
}

type laneInboxDialect int

const (
	laneInboxSQLite laneInboxDialect = iota
	laneInboxPostgres
)

// laneInboxOpTimeout bounds every store call, including one makeBatches
// transaction. SQLite's own busy_timeout is 5s. laneInboxStartupTimeout
// bounds the startup scans, which read every unread item once: a backlog
// slower than one ordinary call must not disable digests at start.
const (
	laneInboxOpTimeout      = 10 * time.Second
	laneInboxStartupTimeout = 2 * time.Minute
)

// laneInboxClaimChunk is how many item ids one claim UPDATE names, so a
// backlog costs one statement per chunk, not one per item.
const laneInboxClaimChunk = 500

func newLaneInboxStore(db *sql.DB, dialect laneInboxDialect) *laneInboxStore {
	life, abort := context.WithCancel(context.Background())
	return &laneInboxStore{db: db, dialect: dialect, life: life, abort: abort, notify: make(chan struct{})}
}

// op returns the context for one store call: parent's deadline capped at
// laneInboxOpTimeout, and cancelled by abort. Every database call in this
// store takes one (TestLaneInboxStoreCallsTakeAContext).
func (s *laneInboxStore) op(parent context.Context) (context.Context, context.CancelFunc) {
	return s.opWithin(parent, laneInboxOpTimeout)
}

func (s *laneInboxStore) opWithin(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	stop := context.AfterFunc(s.life, cancel)
	return ctx, func() { stop(); cancel() }
}

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

// storedKey is the event_key column's form of key. Hub keys join their parts
// with NUL (relayEventDedupeKey), and producer-supplied parts need not be
// UTF-8; Postgres text holds neither, so the Postgres store keeps the key
// hex-encoded. SQLite keeps the raw key, exactly as before.
func (s *laneInboxStore) storedKey(key string) string {
	return laneInboxStoredKey(s.dialect, key)
}

func laneInboxStoredKey(dialect laneInboxDialect, key string) string {
	if dialect == laneInboxPostgres {
		return hex.EncodeToString([]byte(key))
	}
	return key
}

// laneInboxKeyFromStored inverts laneInboxStoredKey.
func laneInboxKeyFromStored(dialect laneInboxDialect, stored string) (string, error) {
	if dialect != laneInboxPostgres {
		return stored, nil
	}
	raw, err := hex.DecodeString(stored)
	return string(raw), err
}

// close ends every in-flight call, then closes the database.
func (s *laneInboxStore) close() error {
	s.abort()
	return s.db.Close()
}

// loggable returns err fit for a log line. A Postgres error after connect
// can still name the server's address (a dropped connection's net error),
// so it is reduced like a connect error; SQLite errors carry no credential.
func (s *laneInboxStore) loggable(step string, err error) error {
	if s.dialect == laneInboxPostgres {
		return hubDatabaseError(step, err)
	}
	return err
}

// markBatchSent records that handoffkeep now owns a batch's identity.
func (s *laneInboxStore) markBatchSent(id string) error {
	ctx, done := s.op(s.life)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE lane_inbox_batches SET sent=1 WHERE id=?`), id)
	return err
}

// addLaneInboxColumn adds an integer state column to pre-feature SQLite stores.
func addLaneInboxColumn(ctx context.Context, db *sql.DB, column string) error {
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(lane_inbox)`)
	if err != nil {
		return err
	}
	found := false
	for rows.Next() {
		var cid, notNull, pk int
		var name, typ string
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
			rows.Close()
			return err
		}
		found = found || name == column
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if !found {
		// A second hub opening the same store may add the column first.
		if _, err := db.ExecContext(ctx, `ALTER TABLE lane_inbox ADD COLUMN `+column+` INTEGER NOT NULL DEFAULT 0`); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
			return err
		}
	}
	return nil
}

func addLaneInboxMustAck(ctx context.Context, db *sql.DB) error {
	if err := addLaneInboxColumn(ctx, db, "must_ack"); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS lane_inbox_must_ack ON lane_inbox(lane) WHERE must_ack=1 AND acked_at=0`)
	return err
}

func (s *laneInboxStore) quarantineBatch(id, reason string) error {
	ctx, done := s.op(s.life)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`INSERT INTO lane_inbox_batch_quarantine(batch_id,reason) VALUES(?,?) ON CONFLICT(batch_id) DO NOTHING`), id, reason)
	return err
}

func (s *laneInboxStore) quarantineInvalidBatches() error {
	invalid, err := s.invalidBatches()
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

func (s *laneInboxStore) invalidBatches() ([]laneInboxBatch, error) {
	ctx, done := s.opWithin(s.life, laneInboxStartupTimeout)
	defer done()
	rows, err := s.db.QueryContext(ctx, `SELECT id,lane,text FROM lane_inbox_batches WHERE sent=0 AND id NOT IN (SELECT batch_id FROM lane_inbox_batch_quarantine)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var invalid []laneInboxBatch
	for rows.Next() {
		var b laneInboxBatch
		if err := rows.Scan(&b.ID, &b.Lane, &b.Text); err != nil {
			return nil, err
		}
		if b.invalidReason() != "" {
			invalid = append(invalid, b)
		}
	}
	return invalid, rows.Err()
}

// recoverMustAct scans all unread originals at startup, including rows without
// a deadline and rows claimed by a quarantined legacy batch. A valid existing
// escalation claim stays intact, so restart cannot escalate an item twice.
func (s *laneInboxStore) recoverMustAct(policy *deliveryPolicy) error {
	unclaimed, err := s.unclaimedUnread()
	if err != nil {
		return err
	}
	var ids []int64
	for _, i := range unclaimed {
		if policy.mustAct(i.Lane, i.Event) {
			ids = append(ids, i.ID)
		}
	}
	ctx, done := s.opWithin(s.life, laneInboxStartupTimeout)
	defer done()
	// Any positive timestamp in the past makes it immediately eligible.
	return s.updateIDs(ctx, s.db, `UPDATE lane_inbox SET due_at=1,escalation_batch=''`, nil, ids)
}

type laneInboxExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// updateIDs runs update (a SET clause, its args first) on the rows named by
// ids, laneInboxClaimChunk ids per statement.
func (s *laneInboxStore) updateIDs(ctx context.Context, db laneInboxExecer, update string, args []any, ids []int64) error {
	for start := 0; start < len(ids); start += laneInboxClaimChunk {
		chunk := ids[start:min(start+laneInboxClaimChunk, len(ids))]
		values := append([]any{}, args...)
		for _, id := range chunk {
			values = append(values, id)
		}
		marks := strings.TrimSuffix(strings.Repeat("?,", len(chunk)), ",")
		if _, err := db.ExecContext(ctx, s.q(update+` WHERE id IN (`+marks+`)`), values...); err != nil {
			return err
		}
	}
	return nil
}

func (s *laneInboxStore) unclaimedUnread() ([]laneInboxItem, error) {
	ctx, done := s.opWithin(s.life, laneInboxStartupTimeout)
	defer done()
	rows, err := s.db.QueryContext(ctx, `SELECT `+laneInboxColumns+` FROM lane_inbox WHERE class='inbox' AND read_at=0 AND (escalation_batch='' OR escalation_batch IN (SELECT batch_id FROM lane_inbox_batch_quarantine))`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []laneInboxItem
	for rows.Next() {
		i, err := scanLaneInbox(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	return items, rows.Err()
}

const laneInboxColumns = `id,lane,kind,class,payload,created_at,read_at,acked_at,due_at,digest_at,digest_batch,escalation_batch,must_ack,hold_until`

type inboxScanner interface{ Scan(...any) error }

func scanLaneInbox(row inboxScanner) (laneInboxItem, error) {
	var i laneInboxItem
	var payload string
	err := row.Scan(&i.ID, &i.Lane, &i.Kind, &i.Class, &payload, &i.CreatedAt, &i.ReadAt, &i.AckedAt, &i.DueAt, &i.DigestAt, &i.DigestBatch, &i.EscalationBatch, &i.MustAck, &i.HoldUntil)
	if err == nil {
		err = json.Unmarshal([]byte(payload), &i.Event)
	}
	return i, err
}

// put is first-writer-wins, including the class. A policy edit or source retry
// cannot turn a previously accepted inbox item into a prompt after restart.
// An interrupt is stored read — injection is its delivery — unless its
// declared type must be acked: then it stays unread until `inbox ack`.
func (s *laneInboxStore) put(key, lane, kind, class string, e hubJobEventPayload, now time.Time, deadline, window time.Duration, hold ...time.Duration) (laneInboxItem, error) {
	raw, err := json.Marshal(e)
	if err != nil {
		return laneInboxItem{}, err
	}
	due, digest, read := int64(0), int64(0), int64(0)
	mustAck := mustAckNotifyTypes[e.NotifyType]
	// must_ack is an INTEGER column on both backends; Postgres takes no bool.
	mustAckColumn := 0
	if mustAck {
		mustAckColumn = 1
	}
	if deadline > 0 {
		due = now.Add(deadline).UnixMilli()
	}
	if class == "interrupt" {
		if !mustAck {
			read = now.UnixMilli()
		}
		// A must-ack item is injected on its own and acked by the receiver; a
		// digest would fold it into a summary that is not must-ack. put holds
		// this for every caller, not only receiveInbox.
		if window > 0 && !mustAck {
			digest = now.Add(window).UnixMilli()
		}
	}
	holdUntil := int64(0)
	if len(hold) > 0 && hold[0] > 0 {
		holdUntil = now.Add(hold[0]).UnixMilli()
		due = 0
		digest = 0
	}
	ctx, done := s.op(s.life)
	defer done()
	s.mu.Lock()
	defer s.mu.Unlock()
	insert, err := s.db.ExecContext(ctx, s.q(`INSERT INTO lane_inbox(event_key,lane,kind,class,payload,created_at,read_at,due_at,digest_at,must_ack,hold_until) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(event_key) DO NOTHING`), s.storedKey(key), lane, kind, class, string(raw), now.UnixMilli(), read, due, digest, mustAckColumn, holdUntil)
	if err != nil {
		return laneInboxItem{}, err
	}
	item, err := scanLaneInbox(s.db.QueryRowContext(ctx, s.q(`SELECT `+laneInboxColumns+` FROM lane_inbox WHERE event_key=?`), s.storedKey(key)))
	n, _ := insert.RowsAffected()
	item.Fresh = n == 1
	if n == 1 && (class == "inbox" || mustAck) {
		close(s.notify)
		s.notify = make(chan struct{})
	}
	return item, err
}

// waitChan returns the channel closed by the next unread insert. A waiter
// must take it before re-checking unread state so an insert that lands between
// the check and the select still wakes it.
func (s *laneInboxStore) waitChan() chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.notify
}

// firstUnread returns the oldest still-unread item for one lane (an inbox item
// not yet read, or a must-ack item not yet acked), or nil when it has none.
func (s *laneInboxStore) firstUnread(parent context.Context, lane string) (*laneInboxItem, error) {
	ctx, done := s.op(parent)
	defer done()
	item, err := scanLaneInbox(s.db.QueryRowContext(ctx, s.q(`SELECT `+laneInboxColumns+` FROM lane_inbox WHERE lane=? AND `+laneInboxUnread+` ORDER BY id LIMIT 1`), lane))
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
		q += ` AND ` + laneInboxUnread
	}
	q += ` ORDER BY id DESC LIMIT 500`
	ctx, done := s.op(s.life)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(q), lane, lane)
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
	ctx, done := s.op(s.life)
	defer done()
	if _, err := s.db.ExecContext(ctx, s.q(`UPDATE lane_inbox SET read_at=CASE WHEN read_at=0 THEN ? ELSE read_at END WHERE id=? AND (?='' OR lane=?)`), now.UnixMilli(), id, lane, lane); err != nil {
		return laneInboxItem{}, err
	}
	return scanLaneInbox(s.db.QueryRowContext(ctx, s.q(`SELECT `+laneInboxColumns+` FROM lane_inbox WHERE id=? AND (?='' OR lane=?)`), id, lane, lane))
}

func (s *laneInboxStore) ack(id int64, lane string, all bool, now time.Time) (int64, error) {
	ctx, done := s.op(s.life)
	defer done()
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE lane_inbox SET acked_at=?,read_at=CASE WHEN read_at=0 THEN ? ELSE read_at END WHERE acked_at=0 AND ((? AND (?='' OR lane=?)) OR (NOT ? AND id=? AND (?='' OR lane=?)))`), now.UnixMilli(), now.UnixMilli(), all, lane, lane, all, id, lane, lane)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *laneInboxStore) counts(lane string) (map[string]int, error) {
	ctx, done := s.op(s.life)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT lane,count(*) FROM lane_inbox WHERE `+laneInboxUnread+` AND (?='' OR lane=?) GROUP BY lane`), lane, lane)
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
	ctx, done := s.op(s.life)
	defer done()
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, escalation := range []bool{false, true} {
		filter := `class='interrupt' AND digest_at>0 AND digest_batch='' AND hold_until=0`
		if escalation {
			filter = `class='inbox' AND read_at=0 AND due_at>0 AND due_at<=? AND escalation_batch=''`
		}
		args := []any{}
		if escalation {
			args = append(args, now.UnixMilli())
		}
		rows, err := tx.QueryContext(ctx, s.q(`SELECT `+laneInboxColumns+` FROM lane_inbox WHERE `+filter+` ORDER BY id`), args...)
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
			if _, err := tx.ExecContext(ctx, s.q(`INSERT INTO lane_inbox_batches(id,lane,text) VALUES(?,?,?)`), id, lane, text); err != nil {
				return err
			}
			ids := make([]int64, len(items))
			for n, i := range items {
				ids[n] = i.ID
			}
			if err := s.updateIDs(ctx, tx, `UPDATE lane_inbox SET `+column+`=?`, []any{id}, ids); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func (s *laneInboxStore) pendingBatches() ([]laneInboxBatch, error) {
	ctx, done := s.op(s.life)
	defer done()
	rows, err := s.db.QueryContext(ctx, `SELECT id,lane,text FROM lane_inbox_batches WHERE sent=0 AND id NOT IN (SELECT batch_id FROM lane_inbox_batch_quarantine) ORDER BY `+s.batchOrder()+` LIMIT 100`)
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

// batchOrder is the column batches are sent in, creation order: SQLite's
// implicit rowid, or the identity column Postgres needs in its place.
func (s *laneInboxStore) batchOrder() string {
	if s.dialect == laneInboxPostgres {
		return "seq"
	}
	return "rowid"
}

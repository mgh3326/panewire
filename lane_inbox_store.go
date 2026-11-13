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
	SurfacedAt      int64              `json:"surfaced_at_ms,omitempty"`
	HoldUntil       int64              `json:"hold_until_ms,omitempty"`
	SupersededAt    int64              `json:"superseded_at_ms,omitempty"`
	SupersededBy    int64              `json:"superseded_by,omitempty"`
	Fresh           bool               `json:"-"`
	ID              int64              `json:"id"`
	Lane            string             `json:"lane"`
	Kind            string             `json:"kind"`
	Class           string             `json:"delivery_class"`
	Event           hubJobEventPayload `json:"event"`
	CreatedAt       int64              `json:"created_at_ms"`
	DeliveredAt     int64              `json:"delivered_at_ms,omitempty"`
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
const laneInboxUnread = `(superseded_at=0 AND ((must_ack=1 AND acked_at=0) OR (must_ack=0 AND class='inbox' AND read_at=0)))`

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
	if !validLaneEventID(b.ID) || !LaneNamePattern.MatchString(b.Lane) {
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
		inboxSurfaceSchema,
		`CREATE INDEX IF NOT EXISTS lane_inbox_surfaces_lane ON lane_inbox_surfaces(lane,delivered_at)`,
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
		// Pruning must not let SQLite reuse a deleted, publicly ackable ID.
		`CREATE TABLE IF NOT EXISTS lane_inbox_id_floor (singleton INTEGER PRIMARY KEY CHECK(singleton=1), floor INTEGER NOT NULL)`,
		`INSERT OR IGNORE INTO lane_inbox_id_floor(singleton,floor) VALUES(1,0)`,
		`CREATE TABLE IF NOT EXISTS lane_inbox_batches (id TEXT PRIMARY KEY, lane TEXT NOT NULL, text TEXT NOT NULL, sent INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE IF NOT EXISTS lane_inbox_batch_quarantine (batch_id TEXT PRIMARY KEY, reason TEXT NOT NULL)`,
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			db.Close()
			return nil, err
		}
	}
	if _, err := addLaneInboxColumn(ctx, db, "hold_until", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := addLaneInboxColumn(ctx, db, "delivered_at", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		db.Close()
		return nil, err
	}
	stageAdded, err := addLaneInboxColumn(ctx, db, "delivery_stage", "TEXT NOT NULL DEFAULT ''")
	if err != nil {
		db.Close()
		return nil, err
	}
	for _, column := range []struct{ name, definition string }{
		{"delivery_reason", "TEXT NOT NULL DEFAULT ''"},
		{"delivery_machine", "TEXT NOT NULL DEFAULT ''"},
		{"delivery_pane", "TEXT NOT NULL DEFAULT ''"},
		{"delivery_at", "INTEGER NOT NULL DEFAULT 0"},
		{"relay_id", "INTEGER NOT NULL DEFAULT 0"},
	} {
		if _, err := addLaneInboxColumn(ctx, db, column.name, column.definition); err != nil {
			db.Close()
			return nil, err
		}
	}
	if stageAdded {
		if err := backfillLaneInboxDeliveryStage(ctx, db); err != nil {
			db.Close()
			return nil, err
		}
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS lane_inbox_relay_id ON lane_inbox(relay_id) WHERE relay_id<>0`); err != nil {
		db.Close()
		return nil, err
	}
	for _, column := range []struct{ name, definition string }{{"cause", "TEXT"}, {"superseded_at", "INTEGER NOT NULL DEFAULT 0"}, {"superseded_by", "INTEGER NOT NULL DEFAULT 0"}} {
		if _, err := addLaneInboxColumn(ctx, db, column.name, column.definition); err != nil {
			db.Close()
			return nil, err
		}
	}
	if err := addLaneInboxAckState(ctx, db); err != nil {
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
	if s.life.Err() != nil {
		cancel()
	}
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

// addLaneInboxColumn adds a state column to pre-feature SQLite stores. added
// is true only when this call created it, so a caller can backfill once.
func addLaneInboxColumn(ctx context.Context, db *sql.DB, column, definition string) (added bool, err error) {
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(lane_inbox)`)
	if err != nil {
		return false, err
	}
	found := false
	for rows.Next() {
		var cid, notNull, pk int
		var name, typ string
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
			rows.Close()
			return false, err
		}
		found = found || name == column
	}
	err = rows.Err()
	rows.Close()
	if err != nil || found {
		return false, err
	}
	// A second hub opening the same store may add the column first.
	if _, err := db.ExecContext(ctx, `ALTER TABLE lane_inbox ADD COLUMN `+column+` `+definition); err != nil {
		return false, ignoreDuplicateColumn(err)
	}
	return true, nil
}

func ignoreDuplicateColumn(err error) error {
	if strings.Contains(err.Error(), "duplicate column name") {
		return nil
	}
	return err
}

// addLaneInboxAckState adds the columns the ack-state lookup reads: the machine
// that relayed the row and the producer's event id. A store from before the
// columns gets event_id backfilled from the stored payload; sender stays empty
// for those rows, so only the operator and scoped services can see them.
func addLaneInboxAckState(ctx context.Context, db *sql.DB) error {
	if _, err := addLaneInboxColumn(ctx, db, "sender", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if _, err := addLaneInboxColumn(ctx, db, "event_id", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := backfillLaneInboxEventIDs(db); err != nil {
		return err
	}
	var err error
	_, err = db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS lane_inbox_event_id ON lane_inbox(event_id) WHERE event_id<>''`)
	return err
}

// backfillLaneInboxEventIDs fills event_id from the payload of rows stored
// before the column, once per store. The marker row and the UPDATE commit
// together, so a hub that stops half way (or two that open one file) redoes
// the backfill rather than skipping it, and a finished one is never rescanned:
// rows whose payload names no id would otherwise be read again on every start.
// The marker insert is the transaction's first statement, so it takes the
// write lock up front. A large store gets the startup timeout, not one call's.
func backfillLaneInboxEventIDs(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), laneInboxStartupTimeout)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS lane_inbox_event_id_backfill (singleton INTEGER PRIMARY KEY CHECK(singleton=1))`); err != nil {
		return err
	}
	marked, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO lane_inbox_event_id_backfill(singleton) VALUES(1)`)
	if err != nil {
		return err
	}
	if n, _ := marked.RowsAffected(); n == 1 {
		if _, err := tx.ExecContext(ctx, `UPDATE lane_inbox SET event_id=json_extract(payload,'$.event_id') WHERE event_id='' AND json_valid(payload) AND json_type(payload,'$.event_id')='text' AND json_extract(payload,'$.event_id')<>''`); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// backfillLaneInboxDeliveryStage gives rows stored before the stage columns
// the stage their other columns already prove: a confirmed delivery, or an
// inbox item the lane reads rather than receives in its pane. Every other old
// row stays empty and reads as queued.
func backfillLaneInboxDeliveryStage(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `UPDATE lane_inbox SET delivery_stage='delivered',delivery_at=delivered_at WHERE delivered_at>0 AND delivery_stage=''`); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx, `UPDATE lane_inbox SET delivery_stage='inbox_only' WHERE delivery_stage='' AND class='inbox'`)
	return err
}

func addLaneInboxMustAck(ctx context.Context, db *sql.DB) error {
	if _, err := addLaneInboxColumn(ctx, db, "must_ack", "INTEGER NOT NULL DEFAULT 0"); err != nil {
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
// a deadline and unsent, invalid legacy claims. A sent or externally owned
// claim stays intact, so restart cannot escalate an item twice.
func (s *laneInboxStore) recoverMustAct(policy *deliveryPolicy, now time.Time) error {
	unclaimed, err := s.unclaimedUnread()
	if err != nil {
		return err
	}
	var ids []int64
	for _, i := range unclaimed {
		// A design lane never escalates unread items, even a misfiled must-act
		// original; it can still read them with `inbox list` (hk 3410). Nor
		// does any other inbox-only lane: its wake is the line (hk 3602).
		if policy.inboxOnlyClass(i.Lane, now) != "" {
			continue
		}
		if !(i.Kind == "job.completed" && i.Event.Late) && policy.mustAct(i.Lane, i.Event) {
			ids = append(ids, i.ID)
		}
	}
	ctx, done := s.opWithin(s.life, laneInboxStartupTimeout)
	defer done()
	// Any positive timestamp in the past makes it immediately eligible.
	return s.updateIDs(ctx, s.db, `UPDATE lane_inbox SET due_at=1,escalation_batch=CASE WHEN escalation_batch IN (SELECT q.batch_id FROM lane_inbox_batch_quarantine q JOIN lane_inbox_batches b ON b.id=q.batch_id WHERE b.sent=0 AND q.reason<>'external scheduler owns lane digest') THEN '' ELSE escalation_batch END`, nil, ids)
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
	rows, err := s.db.QueryContext(ctx, `SELECT `+laneInboxColumns+` FROM lane_inbox WHERE class='inbox' AND read_at=0 AND acked_at=0 AND superseded_at=0 AND (escalation_batch='' OR escalation_batch IN (SELECT q.batch_id FROM lane_inbox_batch_quarantine q JOIN lane_inbox_batches b ON b.id=q.batch_id WHERE b.sent=0 AND q.reason<>'external scheduler owns lane digest'))`)
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

const laneInboxColumns = `id,lane,kind,class,payload,created_at,read_at,acked_at,due_at,digest_at,digest_batch,escalation_batch,must_ack,hold_until,delivered_at,superseded_at,superseded_by`

type inboxScanner interface{ Scan(...any) error }

func scanLaneInbox(row inboxScanner, surfaced ...bool) (laneInboxItem, error) {
	var i laneInboxItem
	var payload string
	fields := []any{&i.ID, &i.Lane, &i.Kind, &i.Class, &payload, &i.CreatedAt, &i.ReadAt, &i.AckedAt, &i.DueAt, &i.DigestAt, &i.DigestBatch, &i.EscalationBatch, &i.MustAck, &i.HoldUntil, &i.DeliveredAt, &i.SupersededAt, &i.SupersededBy}
	if len(surfaced) > 0 && surfaced[0] {
		fields = append(fields, &i.SurfacedAt)
	}
	err := row.Scan(fields...)
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
	return s.putFrom("", key, lane, kind, class, e, now, deadline, window, hold...)
}

// putFrom is put recording which node or service relayed the event. sender is
// first-writer-wins with the row; it is empty for a row the hub itself wrote
// (replay, HTTP ingress, its own digests) and is never part of the item JSON.
func (s *laneInboxStore) putFrom(sender, key, lane, kind, class string, e hubJobEventPayload, now time.Time, deadline, window time.Duration, hold ...time.Duration) (laneInboxItem, error) {
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
	// Only an id the lookup route would accept is stored, so a producer id
	// Postgres text cannot hold never fails the insert.
	eventID := e.EventID
	if !validLaneEventID(eventID) {
		eventID = ""
	}
	// The stage a sender may soon ask about: an inbox-class row is delivered
	// through the lane's inbox, every other row is queued for the pane.
	stage := laneInboxStageQueued
	if class == "inbox" {
		stage = laneInboxStageInboxOnly
	}
	query := `INSERT INTO lane_inbox(event_key,lane,kind,class,payload,created_at,read_at,due_at,digest_at,must_ack,hold_until,sender,event_id,cause,delivery_stage,delivery_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(event_key) DO NOTHING`
	if s.dialect == laneInboxSQLite {
		query = `INSERT INTO lane_inbox(id,event_key,lane,kind,class,payload,created_at,read_at,due_at,digest_at,must_ack,hold_until,sender,event_id,cause,delivery_stage,delivery_at) VALUES((SELECT max(COALESCE((SELECT max(id) FROM lane_inbox),0),floor)+1 FROM lane_inbox_id_floor WHERE singleton=1),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(event_key) DO NOTHING`
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return laneInboxItem{}, err
	}
	defer tx.Rollback()
	if s.dialect == laneInboxPostgres && e.Cause != "" {
		if _, err := tx.ExecContext(ctx, s.q(`SELECT pg_advisory_xact_lock(hashtext(?))`), lane+"\x1f"+e.Cause); err != nil {
			return laneInboxItem{}, err
		}
	}
	insert, err := tx.ExecContext(ctx, s.q(query), s.storedKey(key), lane, kind, class, string(raw), now.UnixMilli(), read, due, digest, mustAckColumn, holdUntil, sender, eventID, nullableCause(e.Cause), stage, now.UnixMilli())
	if err != nil {
		return laneInboxItem{}, err
	}
	item, err := scanLaneInbox(tx.QueryRowContext(ctx, s.q(`SELECT `+laneInboxColumns+` FROM lane_inbox WHERE event_key=?`), s.storedKey(key)))
	n, _ := insert.RowsAffected()
	item.Fresh = n == 1
	if err != nil {
		return item, err
	}
	if item.Fresh && e.Cause != "" {
		if err := s.supersede(ctx, tx, item, now); err != nil {
			return laneInboxItem{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return laneInboxItem{}, err
	}
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
	q := `SELECT ` + laneInboxColumns + `,(SELECT COALESCE(max(delivered_at),0) FROM lane_inbox_surfaces surfaced WHERE surfaced.lane=lane_inbox.lane AND surfaced.through_id>=lane_inbox.id) FROM lane_inbox WHERE (?='' OR lane=?)`
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
		i, err := scanLaneInbox(rows, true)
		if err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	return items, rows.Err()
}

// laneInboxAckState is what the ack-state lookup may say about an event: the
// lane it targets, the delivery stage its latest row change reached, and
// whether, and when, its inbox row was acked or superseded. It carries no
// kind or body.
type laneInboxAckState struct {
	EventID string `json:"event_id"`
	Lane    string `json:"lane,omitempty"`
	Stage   string `json:"stage"`
	Reason  string `json:"reason,omitempty"`
	Machine string `json:"machine,omitempty"`
	Pane    string `json:"pane,omitempty"`
	At      string `json:"at,omitempty"`
	Acked   bool   `json:"acked"`
	AckedAt string `json:"acked_at,omitempty"`
}

// laneInboxAckRow is one visible row behind an event id, before the answer is
// aggregated across the lanes the same event reached.
type laneInboxAckRow struct {
	lane, stage, reason, machine, pane string
	deliveryAt, deliveredAt            int64
	ackedAt, supersededAt              int64
}

// ackStates answers for each event id the rows the caller may see: every row
// when sender is "", otherwise only rows that sender relayed. An id with
// several visible rows (the same event relayed to several lanes) is acked
// once all are acked or superseded, at the latest resolution time, and its
// delivery is the row whose stage last changed. An id with no visible row is
// absent from the result, so a hidden row and a missing one look the same.
func (s *laneInboxStore) ackStates(eventIDs []string, sender string) (map[string]laneInboxAckState, error) {
	args := make([]any, 0, len(eventIDs)+2)
	marks := make([]string, len(eventIDs))
	for i, id := range eventIDs {
		marks[i] = "?"
		args = append(args, id)
	}
	args = append(args, sender, sender)
	ctx, done := s.op(s.life)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT event_id,lane,delivery_stage,delivery_reason,delivery_machine,delivery_pane,delivery_at,delivered_at,acked_at,superseded_at FROM lane_inbox WHERE event_id IN (`+strings.Join(marks, ",")+`) AND (?='' OR sender=?) ORDER BY event_id,id`), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type aggregate struct {
		state   laneInboxAckState
		total   int64
		settled int64
		latest  int64
		at      int64
		rank    int
		have    bool
	}
	aggregates := map[string]*aggregate{}
	for rows.Next() {
		var eventID string
		var row laneInboxAckRow
		if err := rows.Scan(&eventID, &row.lane, &row.stage, &row.reason, &row.machine, &row.pane, &row.deliveryAt, &row.deliveredAt, &row.ackedAt, &row.supersededAt); err != nil {
			return nil, err
		}
		agg := aggregates[eventID]
		if agg == nil {
			agg = &aggregate{}
			aggregates[eventID] = agg
		}
		agg.total++
		settledAt := row.ackedAt
		if row.supersededAt > settledAt {
			settledAt = row.supersededAt
		}
		if settledAt > 0 {
			agg.settled++
			if settledAt > agg.latest {
				agg.latest = settledAt
			}
		}
		stage := laneInboxStageFor(row.stage, row.deliveredAt)
		at := row.deliveryAt
		if at == 0 {
			at = row.deliveredAt
		}
		rank := laneInboxStageRank(stage)
		if !agg.have || at > agg.at || (at == agg.at && rank > agg.rank) {
			agg.have, agg.at, agg.rank = true, at, rank
			agg.state.Lane, agg.state.Stage, agg.state.Reason = row.lane, stage, row.reason
			agg.state.Machine, agg.state.Pane = row.machine, row.pane
			agg.state.At = ""
			if at > 0 {
				agg.state.At = time.UnixMilli(at).UTC().Format(time.RFC3339)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	states := map[string]laneInboxAckState{}
	for id, agg := range aggregates {
		state := agg.state
		state.EventID = id
		state.Acked = agg.settled == agg.total
		if state.Acked {
			state.AckedAt = time.UnixMilli(agg.latest).UTC().Format(time.RFC3339)
		}
		states[id] = state
	}
	return states, nil
}

func (s *laneInboxStore) read(id int64, lane string, now time.Time) (laneInboxItem, error) {
	ctx, done := s.op(s.life)
	defer done()
	if _, err := s.db.ExecContext(ctx, s.q(`UPDATE lane_inbox SET read_at=CASE WHEN read_at=0 THEN ? ELSE read_at END WHERE id=? AND (?='' OR lane=?)`), now.UnixMilli(), id, lane, lane); err != nil {
		return laneInboxItem{}, err
	}
	return scanLaneInbox(s.db.QueryRowContext(ctx, s.q(`SELECT `+laneInboxColumns+` FROM lane_inbox WHERE id=? AND (?='' OR lane=?)`), id, lane, lane))
}

// itemLane is the lane that owns item id; sql.ErrNoRows when there is none.
func (s *laneInboxStore) itemLane(id int64) (string, error) {
	ctx, done := s.op(s.life)
	defer done()
	var lane string
	err := s.db.QueryRowContext(ctx, s.q(`SELECT lane FROM lane_inbox WHERE id=?`), id).Scan(&lane)
	return lane, err
}

func (s *laneInboxStore) ack(id int64, lane string, all bool, now time.Time) (int64, error) {
	ctx, done := s.op(s.life)
	defer done()
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE lane_inbox SET acked_at=?,read_at=CASE WHEN read_at=0 THEN ? ELSE read_at END WHERE acked_at=0 AND superseded_at=0 AND ((? AND (?='' OR lane=?)) OR (NOT ? AND id=? AND (?='' OR lane=?)))`), now.UnixMilli(), now.UnixMilli(), all, lane, lane, all, id, lane, lane)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// laneNames lists the lanes with stored items so a policy sweep can cover a
// lane the policy's lanes: block never names (a globbed class, for example).
func (s *laneInboxStore) laneNames() ([]string, error) {
	ctx, done := s.op(s.life)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT DISTINCT lane FROM lane_inbox ORDER BY lane`))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var lanes []string
	for rows.Next() {
		var lane string
		if err := rows.Scan(&lane); err != nil {
			return nil, err
		}
		lanes = append(lanes, lane)
	}
	return lanes, rows.Err()
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
		filter := `class='interrupt' AND acked_at=0 AND superseded_at=0 AND digest_at>0 AND digest_batch='' AND hold_until=0`
		if escalation {
			filter = `class='inbox' AND read_at=0 AND acked_at=0 AND superseded_at=0 AND due_at>0 AND due_at<=? AND escalation_batch=''`
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
			id := quietDigestBatchPrefix + uuid.NewString()
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
	return s.pendingBatchesForContext(s.life)
}

func (s *laneInboxStore) pendingBatchesForContext(parent context.Context) ([]laneInboxBatch, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, `SELECT id,lane,text,
 (SELECT count(*) FROM lane_inbox i WHERE i.digest_batch=b.id OR i.escalation_batch=b.id),
 (SELECT count(*) FROM lane_inbox i WHERE (i.digest_batch=b.id OR i.escalation_batch=b.id) AND i.acked_at=0 AND i.superseded_at=0 AND (i.escalation_batch<>b.id OR i.read_at=0))
 FROM lane_inbox_batches b WHERE sent=0 AND id NOT IN (SELECT batch_id FROM lane_inbox_batch_quarantine) ORDER BY `+s.batchOrder()+` LIMIT 100`)
	if err != nil {
		return nil, err
	}
	batches := []laneInboxBatch{}
	var retired []string
	for rows.Next() {
		var b laneInboxBatch
		var total, active int
		if err := rows.Scan(&b.ID, &b.Lane, &b.Text, &total, &active); err != nil {
			rows.Close()
			return nil, err
		}
		if total > 0 && active == 0 {
			retired = append(retired, b.ID)
			continue
		}
		if total > active && !strings.HasPrefix(b.ID, "held-") {
			if strings.Contains(b.Text, " unread inbox items | ") {
				b.Text = fmt.Sprintf("%d unread inbox items | lane %s | panewire inbox list --lane %s", active, b.Lane, b.Lane)
			} else {
				b.Text = fmt.Sprintf("%d interrupt events | lane %s | panewire inbox list --lane %s", active, b.Lane, b.Lane)
			}
		}
		batches = append(batches, b)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	for _, id := range retired {
		if err := s.markBatchSent(id); err != nil {
			return nil, err
		}
	}
	return batches, nil
}

// batchOrder is the column batches are sent in, creation order: SQLite's
// implicit rowid, or the identity column Postgres needs in its place.
func (s *laneInboxStore) batchOrder() string {
	if s.dialect == laneInboxPostgres {
		return "seq"
	}
	return "rowid"
}

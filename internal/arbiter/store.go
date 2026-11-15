// Package arbiter is the local job ledger panewire used to get by executing
// the agent-skills `arbiter` script: leases (quota_pool, path, linear_permit),
// local job events and gc. It reads and writes the same on-disk state, so
// state an earlier arbiter wrote is read without migration:
//
//   - $XDG_DATA_HOME/arbiter/state.db, a SQLite database, schema version 2
//     (tables meta, jobs, quota_pool_records, leases, lease_history, events);
//   - <jobs root>/<job>/events/NNNNN-<kind>.json, one artifact per event,
//     written under <jobs root>/<job>/.wrk-events.lock.
//
// Cross-host job state lives in hk job records; this package holds only the
// host-local half. Failures are fail-closed: every error carries the exit
// status the script used (see the Exit* constants).
package arbiter

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver; the state is plain SQLite
)

// SchemaVersion is the only schema version this package reads or writes. A
// stored value that differs is refused, never migrated.
const SchemaVersion = 2

const busyTimeoutMS = 10000

// Exit statuses, unchanged from the script.
const (
	ExitUsage     = 2 // usage error
	ExitDenied    = 3 // exclusive lease held / job not registered / gate refused
	ExitDuplicate = 4 // duplicate claim, duplicate (job, seq) event
	ExitFencing   = 5 // stale owner or token mismatch on release
	ExitStorage   = 6 // state or evidence defect (fail-closed)
	ExitNotFound  = 7 // job-get / orphan release: no such job
)

// Lease kinds.
const (
	KindQuotaPool    = "quota_pool"
	KindPath         = "path"
	KindLinearPermit = "linear_permit"
)

// DefaultTTL is the lease time-to-live per exclusive kind. quota_pool is an
// execution record, not an exclusive resource, and has no TTL.
var DefaultTTL = map[string]time.Duration{KindPath: 90 * time.Minute, KindLinearPermit: 30 * time.Minute}

// Error is every failure that ends a command non-zero.
type Error struct {
	Code   int
	Msg    string
	Detail map[string]any
}

func (e *Error) Error() string { return e.Msg }

func errf(code int, format string, args ...any) *Error {
	return &Error{Code: code, Msg: fmt.Sprintf(format, args...)}
}

// Code returns the exit status of err: an *Error's own, else ExitStorage.
func Code(err error) int {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ExitStorage
}

// Store is the ledger over one state database and one jobs root. The zero
// value is unusable; set DB and Inbox. Each operation opens its own
// connection, as the script did, so a Store is safe for concurrent use and
// for concurrent use with a running arbiter script.
type Store struct {
	DB    string           // path of state.db
	Inbox string           // jobs root holding <job>/events
	Now   func() time.Time // clock; nil means time.Now
}

// testForbiddenHomes lists home directories no Store may open state under.
// Only a test binary's TestMain registers one — the home captured before it
// moves HOME to a temp dir — so a test that still resolves the operator's
// real home fails loudly instead of writing live ledger state (hk 4685).
// Production never registers one; the check is a nil test.
var testForbiddenHomes []string

// ForbidHomeForTests registers home as forbidden for every Store this test
// binary opens: state.db and the events inbox must both stay out from under
// it. It exists for TestMain only.
func ForbidHomeForTests(home string) {
	if home == "" || home == string(filepath.Separator) {
		return
	}
	testForbiddenHomes = append(testForbiddenHomes, filepath.Clean(home))
}

// checkTestHome panics when the store's database or inbox resolves under a
// home ForbidHomeForTests registered.
func (s *Store) checkTestHome() {
	if len(testForbiddenHomes) == 0 {
		return
	}
	for _, home := range testForbiddenHomes {
		for _, path := range []string{s.DB, s.Inbox} {
			abs, err := filepath.Abs(path)
			if err != nil {
				continue
			}
			if rel, err := filepath.Rel(home, abs); err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				panic(fmt.Sprintf("arbiter: test opened ledger state under the real home %s: %s", home, abs))
			}
		}
	}
}

// StatePath is the state database for a data home ($XDG_DATA_HOME or
// ~/.local/share).
func StatePath(dataHome string) string { return filepath.Join(dataHome, "arbiter", "state.db") }

func (s *Store) now() time.Time {
	t := time.Now()
	if s.Now != nil {
		t = s.Now()
	}
	return t.UTC().Truncate(time.Second)
}

// iso renders a time the way the script stored it: UTC, whole seconds,
// "+00:00" offset.
func iso(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05-07:00") }

func parseISO(v string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05", "2006-01-02 15:04:05", "2006-01-02T15:04:05.999999999"} {
		if t, e := time.Parse(layout, v); e == nil {
			return t, nil
		}
	}
	return time.Time{}, errf(ExitStorage, "stored time cannot be parsed: %q", v)
}

// maskToken keeps the fencing sequence and hides the secret half.
func maskToken(token string) string {
	head, _, _ := strings.Cut(token, "-")
	return head + "-…"
}

var ddl = []string{
	`CREATE TABLE meta (
		id             INTEGER PRIMARY KEY CHECK (id = 1),
		schema_version INTEGER NOT NULL,
		token_seq      INTEGER NOT NULL,
		created_at     TEXT    NOT NULL
	)`,
	`CREATE TABLE jobs (
		job_id     TEXT PRIMARY KEY,
		owner_lane TEXT NOT NULL,
		state      TEXT NOT NULL,
		t_level    TEXT NOT NULL,
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL
	)`,
	`CREATE TABLE quota_pool_records (
		job_id    TEXT NOT NULL,
		pool      TEXT NOT NULL,
		profile   TEXT NOT NULL,
		started_at TEXT NOT NULL,
		PRIMARY KEY (job_id, pool)
	)`,
	`CREATE TABLE leases (
		resource    TEXT NOT NULL,
		kind        TEXT NOT NULL,
		job_id      TEXT NOT NULL,
		token       TEXT NOT NULL,
		acquired_at TEXT NOT NULL,
		expires_at  TEXT NOT NULL,
		PRIMARY KEY (resource, kind)
	)`,
	`CREATE TABLE lease_history (
		id          INTEGER PRIMARY KEY AUTOINCREMENT,
		resource    TEXT NOT NULL,
		kind        TEXT NOT NULL,
		job_id      TEXT NOT NULL,
		token       TEXT NOT NULL,
		acquired_at TEXT NOT NULL,
		expires_at  TEXT NOT NULL,
		state       TEXT NOT NULL,
		recorded_at TEXT NOT NULL,
		UNIQUE (resource, kind, token, state)
	)`,
	`CREATE TABLE events (
		id         INTEGER PRIMARY KEY AUTOINCREMENT,
		job_id     TEXT    NOT NULL,
		seq        INTEGER NOT NULL,
		kind       TEXT    NOT NULL,
		payload    TEXT    NOT NULL,
		created_at TEXT    NOT NULL,
		UNIQUE (job_id, seq)
	)`,
}

// conn is one open database handle.
type conn struct {
	db  *sql.DB
	c   *sql.Conn
	ctx context.Context
}

func (c *conn) Close() {
	if c.c != nil {
		c.c.Close()
	}
	c.db.Close()
}

// fileURI quotes only what a SQLite URI needs, as the script did.
func fileURI(path, mode string) string {
	r := strings.NewReplacer("%", "%25", "?", "%3f", "#", "%23")
	return "file:" + r.Replace(path) + "?mode=" + mode + "&_pragma=busy_timeout(" + fmt.Sprint(busyTimeoutMS) + ")"
}

func openURI(ctx context.Context, uri string) (*conn, error) {
	db, e := sql.Open("sqlite", uri)
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	c, e := db.Conn(ctx)
	if e != nil {
		db.Close()
		return nil, e
	}
	return &conn{db: db, c: c, ctx: ctx}, nil
}

// rw opens the database for writing. Only bootstrap may create it.
func (s *Store) rw(ctx context.Context, bootstrap bool) (*conn, error) {
	s.checkTestHome()
	_, statErr := os.Stat(s.DB)
	fresh := statErr != nil
	if fresh {
		if !bootstrap {
			return nil, errf(ExitStorage, "state db is missing: %s", s.DB)
		}
		// The database holds the full fencing tokens: a new ledger is owner-only
		// (directory 0700, database 0600; SQLite gives the -wal and -shm files
		// the database's mode). An existing ledger keeps the modes it has.
		if e := os.MkdirAll(filepath.Dir(s.DB), 0o700); e != nil {
			return nil, errf(ExitStorage, "cannot create the state db directory: %v", e)
		}
		f, e := os.OpenFile(s.DB, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		switch {
		case e == nil:
			f.Close()
		case !errors.Is(e, fs.ErrExist):
			return nil, errf(ExitStorage, "cannot create the state db: %v", e)
		}
	}
	mode := "rw"
	if bootstrap {
		mode = "rwc"
	}
	c, e := openURI(ctx, fileURI(s.DB, mode))
	if e != nil {
		return nil, errf(ExitStorage, "cannot open the state db %s: %v", s.DB, e)
	}
	if _, e = c.c.ExecContext(ctx, "PRAGMA journal_mode=WAL"); e != nil {
		c.Close()
		return nil, errf(ExitStorage, "cannot open the state db %s: %v", s.DB, e)
	}
	if fresh {
		if e = s.createSchema(ctx, c); e != nil {
			c.Close()
			return nil, e
		}
	}
	if e = s.checkSchema(ctx, c); e != nil {
		c.Close()
		return nil, e
	}
	return c, nil
}

// ro opens the database read-only. A WAL index (-shm) that does not exist
// cannot be created by a read-only handle; with no unapplied WAL frames the
// read falls back to an immutable one, otherwise it refuses rather than guess.
func (s *Store) ro(ctx context.Context) (*conn, error) {
	s.checkTestHome()
	if _, e := os.Stat(s.DB); e != nil {
		return nil, errf(ExitStorage, "state db is missing: %s", s.DB)
	}
	open := func(mode string) (*conn, error) {
		c, e := openURI(ctx, fileURI(s.DB, mode))
		if e != nil {
			return nil, e
		}
		if _, e = c.c.ExecContext(ctx, "PRAGMA query_only=1"); e == nil {
			var n int
			e = c.c.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master").Scan(&n)
		}
		if e != nil {
			c.Close()
			return nil, e
		}
		return c, nil
	}
	c, first := open("ro")
	if first != nil {
		if !walClean(s.DB) {
			return nil, errf(ExitStorage, "state db is unreadable: %s: %v", s.DB, first)
		}
		var e error
		if c, e = open("ro&immutable=1"); e != nil {
			return nil, errf(ExitStorage, "state db is unreadable: %s: %v", s.DB, e)
		}
	}
	if e := s.checkSchema(ctx, c); e != nil {
		c.Close()
		return nil, e
	}
	return c, nil
}

func walClean(path string) bool {
	if _, e := os.Stat(path + "-shm"); e == nil {
		return false
	}
	st, e := os.Stat(path + "-wal")
	if e != nil {
		return os.IsNotExist(e)
	}
	return st.Size() == 0
}

func (s *Store) createSchema(ctx context.Context, c *conn) error {
	if e := c.begin(); e != nil {
		return errf(ExitStorage, "cannot initialise the state db: %v", e)
	}
	var tables int
	if e := c.c.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE name = 'meta'").Scan(&tables); e != nil {
		c.rollback()
		return errf(ExitStorage, "cannot initialise the state db: %v", e)
	}
	if tables == 0 { // another creator may have won the race
		for _, stmt := range ddl {
			if _, e := c.c.ExecContext(ctx, stmt); e != nil {
				c.rollback()
				return errf(ExitStorage, "cannot initialise the state db: %v", e)
			}
		}
		if _, e := c.c.ExecContext(ctx, "INSERT INTO meta (id, schema_version, token_seq, created_at) VALUES (1, ?, 0, ?)", SchemaVersion, iso(s.now())); e != nil {
			c.rollback()
			return errf(ExitStorage, "cannot initialise the state db: %v", e)
		}
	}
	if e := c.commit(); e != nil {
		return errf(ExitStorage, "cannot initialise the state db: %v", e)
	}
	return nil
}

func (s *Store) checkSchema(ctx context.Context, c *conn) error {
	mismatch := func(observed any, detail string) error {
		if detail != "" {
			detail = "; " + detail
		}
		return errf(ExitStorage, "schema mismatch: db_path=%s observed_version=%v expected_version=%d; action: back up the existing DB, then recreate it; no automatic migration, deletion, or rollback is performed%s", s.DB, observed, SchemaVersion, detail)
	}
	var raw any
	e := c.c.QueryRowContext(ctx, "SELECT schema_version FROM meta WHERE id = 1").Scan(&raw)
	switch {
	case errors.Is(e, sql.ErrNoRows):
		return mismatch("missing-meta-row", "")
	case e != nil:
		return mismatch("unreadable", "schema check failed: "+e.Error())
	}
	v, ok := raw.(int64)
	if !ok {
		return mismatch(raw, "schema_version is not an integer")
	}
	if v != SchemaVersion {
		return mismatch(v, "")
	}
	return nil
}

func (c *conn) begin() error {
	_, e := c.c.ExecContext(c.ctx, "BEGIN IMMEDIATE")
	return e
}
func (c *conn) commit() error {
	_, e := c.c.ExecContext(c.ctx, "COMMIT")
	return e
}
func (c *conn) rollback() { _, _ = c.c.ExecContext(context.WithoutCancel(c.ctx), "ROLLBACK") }

func (c *conn) exec(q string, args ...any) (sql.Result, error) {
	return c.c.ExecContext(c.ctx, q, args...)
}
func (c *conn) row(q string, args ...any) *sql.Row { return c.c.QueryRowContext(c.ctx, q, args...) }
func (c *conn) rows(q string, args ...any) (*sql.Rows, error) {
	return c.c.QueryContext(c.ctx, q, args...)
}

// write runs fn inside BEGIN IMMEDIATE ... COMMIT, rolling back on any error.
func (c *conn) write(fn func() error) error {
	if e := c.begin(); e != nil {
		return errf(ExitStorage, "sqlite error (fail-closed): %v", e)
	}
	if e := fn(); e != nil {
		c.rollback()
		return e
	}
	if e := c.commit(); e != nil {
		c.rollback()
		return errf(ExitStorage, "sqlite error (fail-closed): %v", e)
	}
	return nil
}

// nextToken mints a fencing token: a monotonically increasing sequence, then
// a random half that never appears in status or events.
func nextToken(c *conn) (string, error) {
	if _, e := c.exec("UPDATE meta SET token_seq = token_seq + 1 WHERE id = 1"); e != nil {
		return "", e
	}
	var seq int64
	if e := c.row("SELECT token_seq FROM meta WHERE id = 1").Scan(&seq); e != nil {
		return "", e
	}
	b := make([]byte, 8)
	if _, e := rand.Read(b); e != nil {
		return "", e
	}
	return fmt.Sprintf("%d-%s", seq, hex.EncodeToString(b)), nil
}

// sqlErr maps a database failure to the fail-closed storage error.
func sqlErr(e error) error {
	if e == nil {
		return nil
	}
	var ae *Error
	if errors.As(e, &ae) {
		return e
	}
	return errf(ExitStorage, "sqlite error (fail-closed): %v", e)
}

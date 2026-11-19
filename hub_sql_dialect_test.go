package panewire

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// stmtLog is the statements and transaction options a stmtDriver saw.
type stmtLog struct {
	mu         sync.Mutex
	statements []string
	isolations []sql.IsolationLevel
}

func (l *stmtLog) statement(q string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.statements = append(l.statements, q)
}

func (l *stmtLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.statements...)
}

func (l *stmtLog) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.statements, l.isolations = nil, nil
}

// stmtDriver is a database/sql driver that records every statement and
// answers each one with one affected row and no result rows. It lets a test
// read the SQL a store method sends for either dialect without a database.
type stmtDriver struct{ log *stmtLog }

func (d stmtDriver) Connect(context.Context) (driver.Conn, error) { return stmtConn(d), nil }
func (d stmtDriver) Driver() driver.Driver                        { return d }
func (d stmtDriver) Open(string) (driver.Conn, error)             { return stmtConn(d), nil }

type stmtConn struct{ log *stmtLog }

func (c stmtConn) Prepare(q string) (driver.Stmt, error) {
	c.log.statement(q)
	return stmtStmt{}, nil
}
func (stmtConn) Close() error { return nil }
func (stmtConn) Begin() (driver.Tx, error) {
	return stmtTx{}, nil
}
func (c stmtConn) BeginTx(_ context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.log.mu.Lock()
	c.log.isolations = append(c.log.isolations, sql.IsolationLevel(opts.Isolation))
	c.log.mu.Unlock()
	return stmtTx{}, nil
}

type stmtTx struct{}

func (stmtTx) Commit() error   { return nil }
func (stmtTx) Rollback() error { return nil }

type stmtStmt struct{}

func (stmtStmt) Close() error                               { return nil }
func (stmtStmt) NumInput() int                              { return -1 }
func (stmtStmt) Exec([]driver.Value) (driver.Result, error) { return driver.RowsAffected(1), nil }
func (stmtStmt) Query([]driver.Value) (driver.Rows, error)  { return stmtRows{}, nil }

type stmtRows struct{}

func (stmtRows) Columns() []string         { return nil }
func (stmtRows) Close() error              { return nil }
func (stmtRows) Next([]driver.Value) error { return io.EOF }

// recordedStore is a store on dialect whose database records what it is sent.
func recordedStore(t *testing.T, dialect sqlDialect) (*laneInboxStore, *stmtLog) {
	t.Helper()
	log := &stmtLog{}
	db := sql.OpenDB(stmtDriver{log})
	s := newLaneInboxStore(db, dialect)
	t.Cleanup(func() { s.close() })
	return s, log
}

// testRebind is the Postgres placeholder numbering, written independently of
// the dialect's own so the expected text is not the code under test.
func testRebind(query string) string {
	var out strings.Builder
	n := 0
	for _, r := range query {
		if r != '?' {
			out.WriteRune(r)
			continue
		}
		n++
		out.WriteString("$" + strconv.Itoa(n))
	}
	return out.String()
}

// The generated text of every seam operation, per dialect. SQLite and
// Postgres spell the same forms alike, so one expectation serves both; the
// MySQL dialect of hk 4898 adds its own column here. A change to any text is a
// change to the statements the live hub runs: update this table on purpose.
func TestSQLDialectGeneratedStatements(t *testing.T) {
	const insert = `INSERT INTO t(a,b) VALUES(?,?)`
	cases := []struct {
		name string
		gen  func(d sqlDialect) string
		want string
	}{
		{"insertIgnore target", func(d sqlDialect) string { return d.insertIgnore(insert, "a") },
			`INSERT INTO t(a,b) VALUES(?,?) ON CONFLICT(a) DO NOTHING`},
		{"insertIgnore composite target", func(d sqlDialect) string { return d.insertIgnore(insert, "a,b") },
			`INSERT INTO t(a,b) VALUES(?,?) ON CONFLICT(a,b) DO NOTHING`},
		{"insertIgnore any key", func(d sqlDialect) string { return d.insertIgnore(insert, "") },
			`INSERT INTO t(a,b) VALUES(?,?) ON CONFLICT DO NOTHING`},
		{"upsert", func(d sqlDialect) string { return d.upsert(insert, "a", d.assignIncoming("b")) },
			`INSERT INTO t(a,b) VALUES(?,?) ON CONFLICT(a) DO UPDATE SET b=excluded.b`},
		{"upsert two columns", func(d sqlDialect) string { return d.upsert(insert, "a", d.assignIncoming("a", "b")) },
			`INSERT INTO t(a,b) VALUES(?,?) ON CONFLICT(a) DO UPDATE SET a=excluded.a,b=excluded.b`},
		{"upsertWhere", func(d sqlDialect) string { return d.upsertWhere(insert, "a", "t", "recorded=0", "b") },
			`INSERT INTO t(a,b) VALUES(?,?) ON CONFLICT(a) DO UPDATE SET b=excluded.b WHERE t.recorded=0`},
		{"upsertGreatest", func(d sqlDialect) string { return d.upsertGreatest(insert, "a", "t", "b") },
			`INSERT INTO t(a,b) VALUES(?,?) ON CONFLICT(a) DO UPDATE SET b=CASE WHEN excluded.b>t.b THEN excluded.b ELSE t.b END`},
		{"incoming", func(d sqlDialect) string { return d.incoming("b") }, `excluded.b`},
		{"castInt", func(d sqlDialect) string { return d.castInt("?") }, `CAST(? AS BIGINT)`},
		{"concat", func(d sqlDialect) string { return d.concat("','", "park_reason", "','") }, `','||park_reason||','`},
		{"escapeClause", func(d sqlDialect) string { return d.escapeClause() }, ` ESCAPE '\'`},
		{"deleteWhereIn", func(d sqlDialect) string {
			return d.deleteWhereIn("t", "id", "SELECT id FROM t WHERE n<? ORDER BY n,id LIMIT ?")
		}, `DELETE FROM t WHERE id IN (SELECT id FROM t WHERE n<? ORDER BY n,id LIMIT ?)`},
		{"updateOrdered", func(d sqlDialect) string {
			return d.updateOrdered("t", "id", "n=?", "n>? AND k<>''", "n,id")
		}, `UPDATE t SET n=? WHERE id IN (SELECT id FROM t WHERE n>? AND k<>'' ORDER BY n,id LIMIT ?)`},
	}
	for _, d := range []struct {
		name    string
		dialect sqlDialect
	}{{"sqlite", dialectSQLite}, {"postgres", dialectPostgres}} {
		for _, c := range cases {
			if got := c.gen(d.dialect); got != c.want {
				t.Errorf("%s %s:\n got %s\nwant %s", d.name, c.name, got, c.want)
			}
		}
	}
	if got := dialectSQLite.rebind(`SELECT ? FROM t WHERE a=? AND b=?`); got != `SELECT ? FROM t WHERE a=? AND b=?` {
		t.Errorf("sqlite rebind = %s", got)
	}
	if got := dialectPostgres.rebind(`SELECT ? FROM t WHERE a=? AND b=?`); got != `SELECT $1 FROM t WHERE a=$2 AND b=$3` {
		t.Errorf("postgres rebind = %s", got)
	}
	if got := dialectSQLite.batchOrder() + "/" + dialectPostgres.batchOrder(); got != "rowid/seq" {
		t.Errorf("batchOrder = %s", got)
	}
}

// The lane_inbox INSERT is the one statement whose column list differs: SQLite
// takes the id from its floor row, Postgres from an identity column.
func TestSQLDialectLaneInboxInsert(t *testing.T) {
	const tail = `event_key,lane,kind,class,payload,created_at,read_at,due_at,digest_at,must_ack,hold_until,sender,event_id,cause,delivery_stage,delivery_at) VALUES(`
	if got, want := dialectPostgres.laneInboxInsert(), `INSERT INTO lane_inbox(`+tail+`?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`; got != want {
		t.Errorf("postgres insert:\n got %s\nwant %s", got, want)
	}
	if got, want := dialectSQLite.laneInboxInsert(), `INSERT INTO lane_inbox(id,`+tail+`(SELECT max(COALESCE((SELECT max(id) FROM lane_inbox),0),floor)+1 FROM lane_inbox_id_floor WHERE singleton=1),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`; got != want {
		t.Errorf("sqlite insert:\n got %s\nwant %s", got, want)
	}
}

// Every transaction states its isolation: READ COMMITTED on Postgres, which a
// server default must not change, and the driver's own on SQLite.
func TestSQLDialectBeginsAtItsIsolationLevel(t *testing.T) {
	for _, c := range []struct {
		name    string
		dialect sqlDialect
		want    sql.IsolationLevel
	}{{"sqlite", dialectSQLite, sql.LevelDefault}, {"postgres", dialectPostgres, sql.LevelReadCommitted}} {
		s, log := recordedStore(t, c.dialect)
		tx, err := s.beginTx(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		tx.Rollback()
		if len(log.isolations) != 1 || log.isolations[0] != c.want {
			t.Errorf("%s began at %v, want %v", c.name, log.isolations, c.want)
		}
	}
}

// The two locks, per dialect. Postgres holds them as transaction advisory
// locks; SQLite sends nothing, its single writer already serializes both.
func TestSQLDialectLocks(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name    string
		dialect sqlDialect
		want    []string
	}{
		{"sqlite", dialectSQLite, nil},
		{"postgres", dialectPostgres, []string{`SELECT pg_advisory_xact_lock($1)`, `SELECT pg_advisory_xact_lock(hashtext($1))`}},
	} {
		s, log := recordedStore(t, c.dialect)
		tx, err := s.beginTx(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.dialect.lockMigration(ctx, tx); err != nil {
			t.Fatal(err)
		}
		if err := c.dialect.lockLaneCause(ctx, tx, "lane", "cause"); err != nil {
			t.Fatal(err)
		}
		tx.Rollback()
		if got := log.snapshot(); strings.Join(got, "\n") != strings.Join(c.want, "\n") {
			t.Errorf("%s locks sent %q, want %q", c.name, got, c.want)
		}
	}
}

// The event key: raw on SQLite, hex on Postgres.
func TestSQLDialectStoredKey(t *testing.T) {
	key := "lane.event\x00ev-1\xff"
	if got := dialectSQLite.storedKey(key); got != key {
		t.Errorf("sqlite storedKey = %q", got)
	}
	if got := dialectPostgres.storedKey(key); got != "6c616e652e6576656e740065762d31ff" {
		t.Errorf("postgres storedKey = %q", got)
	}
}

// The statements the store's call sites send. Each is the text the hub ran
// before the seam existed (SQLite form; Postgres numbers the placeholders), so
// moving a call site off the seam or changing what it spells fails here, per
// dialect, without a database.
func TestHubStoreCallSiteStatements(t *testing.T) {
	ctx := context.Background()
	now := time.UnixMilli(1_700_000_000_000).UTC()
	calls := []struct {
		name string
		run  func(s *laneInboxStore)
		want string
	}{
		{"account credit mark", func(s *laneInboxStore) {
			s.putAccountCreditMark(ctx, "acct", accountCreditMark{Pool: "p", Marked: now})
		},
			`INSERT INTO account_credit_marks(account_key,pool,marked_ms) VALUES(?,?,?) ON CONFLICT(account_key) DO UPDATE SET pool=excluded.pool,marked_ms=excluded.marked_ms`},
		{"drain wake", func(s *laneInboxStore) { s.saveDrainWake(ctx, "lane", &drainWakeLane{}) },
			`INSERT INTO drain_wake_lanes(lane,pending,last_sent) VALUES(?,?,?) ON CONFLICT(lane) DO UPDATE SET pending=excluded.pending,last_sent=excluded.last_sent`},
		{"one-time action", func(s *laneInboxStore) { s.putOneTimeAction(ctx, oneTimeActionKey{}, oneTimeActionState{}) },
			`INSERT INTO one_time_action_notices(machine,kind,need,told_ms,gone_ms) VALUES(?,?,?,?,?) ON CONFLICT(machine,kind,need) DO UPDATE SET told_ms=excluded.told_ms,gone_ms=excluded.gone_ms`},
		{"quota refresh attempt", func(s *laneInboxStore) { s.recordQuotaRefreshAttempt(ctx, "m", "p", "ok", now) },
			`INSERT INTO quota_refresh_attempts(machine,pool,outcome,seen_ms) VALUES(?,?,?,?) ON CONFLICT(machine,pool) DO UPDATE SET outcome=excluded.outcome,seen_ms=excluded.seen_ms`},
		{"spawn start-by", func(s *laneInboxStore) { s.recordSpawnStartBy(ctx, "node", 5) },
			`INSERT INTO spawn_attempt_start_by(node_id,start_by_ms) VALUES(?,?) ON CONFLICT(node_id) DO UPDATE SET start_by_ms=CASE WHEN excluded.start_by_ms>spawn_attempt_start_by.start_by_ms THEN excluded.start_by_ms ELSE spawn_attempt_start_by.start_by_ms END`},
		{"placement snapshot", func(s *laneInboxStore) { s.saveSpawnPlacementSnapshot(ctx, "req", 1, []byte("{}"), "") },
			`INSERT INTO spawn_placement_snapshots(request_id,attempt,payload,delivery) VALUES(?,?,?,?) ON CONFLICT(request_id,attempt) DO UPDATE SET payload=excluded.payload,delivery=excluded.delivery WHERE spawn_placement_snapshots.recorded=0`},
		{"inbox summary", func(s *laneInboxStore) { s.saveInboxSummary("k", "lane", "line", 1) },
			`INSERT INTO lane_inbox_surfaces(delivery_key,lane,through_id,summary) VALUES(?,?,?,?) ON CONFLICT(delivery_key) DO NOTHING`},
		{"batch quarantine", func(s *laneInboxStore) { s.quarantineBatch("b", "r") },
			`INSERT INTO lane_inbox_batch_quarantine(batch_id,reason) VALUES(?,?) ON CONFLICT(batch_id) DO NOTHING`},
		{"spawn queue", func(s *laneInboxStore) {
			tx, _ := s.beginTx(ctx)
			s.enqueueSpawn(ctx, tx, "req")
			tx.Rollback()
		}, `INSERT INTO spawn_queue(request_id,next_check_ms) VALUES(?,0) ON CONFLICT DO NOTHING`},
		{"retention of consults", func(s *laneInboxStore) { s.pruneHubStore(now, 10) },
			`DELETE FROM consult_requests WHERE id IN (SELECT id FROM consult_requests WHERE state NOT IN ('queued','running') AND NOT (state='answered' AND notified=0) AND updated_ms<? ORDER BY updated_ms,id LIMIT ?)`},
		{"retention of sent batches", func(s *laneInboxStore) { s.pruneHubStore(now, 10) },
			`DELETE FROM lane_inbox_batches WHERE id IN (` + sentBatchesPrunable + `)`},
		{"spawn grant rows", func(s *laneInboxStore) { s.spawnGrantRowCount(ctx) },
			`SELECT count(*) FROM spawn_operations WHERE status LIKE '%\_grant' ESCAPE '\'`},
		{"park spawn", func(s *laneInboxStore) { s.parkSpawn(ctx, "req", 1, "why", now, now) },
			`UPDATE spawn_operations SET park_reason=CASE WHEN next_check_ms>? AND park_reason<>'' THEN park_reason ELSE ? END,next_check_ms=CASE WHEN next_check_ms>? AND park_reason<>'' THEN next_check_ms ELSE ? END,park_since_ms=CASE WHEN park_since_ms=0 THEN CAST(? AS BIGINT) ELSE park_since_ms END,park_priority=CASE WHEN park_since_ms=0 THEN -1 ELSE park_priority END,park_priority_failures=CASE WHEN park_since_ms=0 THEN 0 ELSE park_priority_failures END WHERE request_id=? AND attempt=? AND status='accepted' AND node_id IS NULL`},
		{"wake parked spawns", func(s *laneInboxStore) { s.wakeParkedSpawns(ctx, []string{"a", "b"}, 5, now) },
			`UPDATE spawn_operations SET next_check_ms=? WHERE request_id IN (SELECT request_id FROM spawn_operations WHERE status='accepted' AND node_id IS NULL AND next_check_ms>? AND ((','||park_reason||',') LIKE ? OR (','||park_reason||',') LIKE ?) ORDER BY next_check_ms,request_id LIMIT ?)`},
	}
	for _, d := range []struct {
		name    string
		dialect sqlDialect
		bind    func(string) string
	}{
		{"sqlite", dialectSQLite, func(q string) string { return q }},
		{"postgres", dialectPostgres, testRebind},
	} {
		for _, c := range calls {
			s, log := recordedStore(t, d.dialect)
			c.run(s)
			want := d.bind(c.want)
			if got := log.snapshot(); !slices.Contains(got, want) {
				t.Errorf("%s %s sent none of its expected statement\nwant %s\n got %q", d.name, c.name, want, got)
			}
		}
	}
}

// Where a dialect difference exists, a store query asks the dialect for it. The
// forms below are the ones the seam owns; spelled inline in a store file they
// would run on one backend and break on the next. A file that is the seam, or
// the Postgres backend's own connection and ledger code, may spell them.
func TestHubStoreQueriesGoThroughTheDialectSeam(t *testing.T) {
	forbidden := []struct{ form, use string }{
		{`ON CONFLICT`, "dialect.insertIgnore / upsert / upsertWhere / upsertGreatest"},
		{`DO UPDATE SET`, "dialect.upsert"},
		{`DO NOTHING`, "dialect.insertIgnore"},
		{`ON DUPLICATE KEY`, "dialect.upsert"},
		{`INSERT IGNORE`, "dialect.insertIgnore"},
		{`pg_advisory`, "dialect.lockMigration / lockLaneCause"},
		{`hashtext(`, "dialect.lockLaneCause"},
		{`CAST(`, "dialect.castInt"},
		{` ESCAPE `, "dialect.escapeClause"},
		{`'||`, "dialect.concat"},
		{`||'`, "dialect.concat"},
		{`.BeginTx(`, "store.beginTx"},
		{`RowsAffected(`, "dialect.rowsMatched"},
		{`dialect ==`, "a dialect method (ownsSchema, ...)"},
		{`dialect !=`, "a dialect method (ownsSchema, ...)"},
		{`LIMIT ?)`, "dialect.updateOrdered / deleteWhereIn"},
	}
	exempt := map[string]bool{"hub_sql_dialect.go": true, "lane_inbox_store_postgres.go": true}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	scanned := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") || exempt[file] {
			continue
		}
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		if !strings.Contains(text, "*laneInboxStore)") && !strings.Contains(text, "inbox.store") {
			continue
		}
		scanned++
		for i, line := range strings.Split(text, "\n") {
			code := strings.TrimSpace(line)
			if strings.HasPrefix(code, "//") {
				continue
			}
			for _, f := range forbidden {
				if strings.Contains(line, f.form) {
					t.Errorf("%s:%d spells %q inline; use %s:\n\t%s", file, i+1, f.form, f.use, code)
				}
			}
		}
	}
	if scanned < 20 {
		t.Fatalf("scanned %d store files; the file selection no longer finds the store", scanned)
	}
}

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
	// args are the arguments each statement was executed with, in order.
	args       [][]driver.Value
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
	l.statements, l.args, l.isolations = nil, nil, nil
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
	return stmtStmt{q, c.log}, nil
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

type stmtStmt struct {
	q   string
	log *stmtLog
}

func (stmtStmt) Close() error  { return nil }
func (stmtStmt) NumInput() int { return -1 }
func (st stmtStmt) Exec(args []driver.Value) (driver.Result, error) {
	st.log.mu.Lock()
	st.log.args = append(st.log.args, append([]driver.Value(nil), args...))
	st.log.mu.Unlock()
	return driver.RowsAffected(1), nil
}

// Query answers a lock statement with the one row "granted" (1), so the
// migration lock can be read without a database, and every other with none.
func (st stmtStmt) Query([]driver.Value) (driver.Rows, error) {
	if strings.HasPrefix(st.q, "SELECT GET_LOCK(") || strings.HasPrefix(st.q, "SELECT RELEASE_LOCK(") {
		return &stmtRows{rows: 1}, nil
	}
	return &stmtRows{}, nil
}

type stmtRows struct{ rows int }

func (r *stmtRows) Columns() []string {
	if r.rows == 0 {
		return nil
	}
	return []string{"granted"}
}
func (*stmtRows) Close() error { return nil }
func (r *stmtRows) Next(dest []driver.Value) error {
	if r.rows == 0 {
		return io.EOF
	}
	r.rows--
	dest[0] = int64(1)
	return nil
}

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
	// want is the text SQLite and Postgres share; mysql is the MySQL text.
	cases := []struct {
		name        string
		gen         func(d sqlDialect) string
		want, mysql string
	}{
		{"insertIgnore target", func(d sqlDialect) string { return d.insertIgnore(insert, "a") },
			`INSERT INTO t(a,b) VALUES(?,?) ON CONFLICT(a) DO NOTHING`,
			`INSERT INTO t(a,b) VALUES(?,?) ON DUPLICATE KEY UPDATE t.a=t.a`},
		{"insertIgnore composite target", func(d sqlDialect) string { return d.insertIgnore(insert, "a,b") },
			`INSERT INTO t(a,b) VALUES(?,?) ON CONFLICT(a,b) DO NOTHING`,
			`INSERT INTO t(a,b) VALUES(?,?) ON DUPLICATE KEY UPDATE t.a=t.a`},
		{"insertIgnore any key", func(d sqlDialect) string { return d.insertIgnore(insert, "") },
			`INSERT INTO t(a,b) VALUES(?,?) ON CONFLICT DO NOTHING`,
			`INSERT INTO t(a,b) VALUES(?,?) ON DUPLICATE KEY UPDATE t.a=t.a`},
		{"insertIgnore of a select", func(d sqlDialect) string {
			return d.insertIgnore("INSERT INTO t(a,b)\n\tSELECT a,b FROM s", "a")
		},
			"INSERT INTO t(a,b)\n\tSELECT a,b FROM s ON CONFLICT(a) DO NOTHING",
			"INSERT INTO t(a,b)\n\tSELECT a,b FROM s ON DUPLICATE KEY UPDATE t.a=t.a"},
		{"insertOnce", func(d sqlDialect) string { return d.insertOnce(insert, "a") },
			`INSERT INTO t(a,b) VALUES(?,?) ON CONFLICT(a) DO NOTHING`,
			`INSERT INTO t(a,b) VALUES(?,?)`},
		{"upsert", func(d sqlDialect) string { return d.upsert(insert, "a", d.assignIncoming("b")) },
			`INSERT INTO t(a,b) VALUES(?,?) ON CONFLICT(a) DO UPDATE SET b=excluded.b`,
			`INSERT INTO t(a,b) VALUES(?,?) AS incoming ON DUPLICATE KEY UPDATE b=incoming.b`},
		{"upsert two columns", func(d sqlDialect) string { return d.upsert(insert, "a", d.assignIncoming("a", "b")) },
			`INSERT INTO t(a,b) VALUES(?,?) ON CONFLICT(a) DO UPDATE SET a=excluded.a,b=excluded.b`,
			`INSERT INTO t(a,b) VALUES(?,?) AS incoming ON DUPLICATE KEY UPDATE a=incoming.a,b=incoming.b`},
		{"upsertWhere", func(d sqlDialect) string { return d.upsertWhere(insert, "a", "t", "recorded=0", "b") },
			`INSERT INTO t(a,b) VALUES(?,?) ON CONFLICT(a) DO UPDATE SET b=excluded.b WHERE t.recorded=0`,
			`INSERT INTO t(a,b) VALUES(?,?) AS incoming ON DUPLICATE KEY UPDATE b=IF(t.recorded=0,incoming.b,t.b)`},
		{"upsertWhere two columns", func(d sqlDialect) string { return d.upsertWhere(insert, "a", "t", "recorded=0", "a", "b") },
			`INSERT INTO t(a,b) VALUES(?,?) ON CONFLICT(a) DO UPDATE SET a=excluded.a,b=excluded.b WHERE t.recorded=0`,
			`INSERT INTO t(a,b) VALUES(?,?) AS incoming ON DUPLICATE KEY UPDATE a=IF(t.recorded=0,incoming.a,t.a),b=IF(t.recorded=0,incoming.b,t.b)`},
		{"upsertGreatest", func(d sqlDialect) string { return d.upsertGreatest(insert, "a", "t", "b") },
			`INSERT INTO t(a,b) VALUES(?,?) ON CONFLICT(a) DO UPDATE SET b=CASE WHEN excluded.b>t.b THEN excluded.b ELSE t.b END`,
			`INSERT INTO t(a,b) VALUES(?,?) AS incoming ON DUPLICATE KEY UPDATE b=GREATEST(t.b,incoming.b)`},
		{"incoming", func(d sqlDialect) string { return d.incoming("b") }, `excluded.b`, `incoming.b`},
		{"castInt", func(d sqlDialect) string { return d.castInt("?") }, `CAST(? AS BIGINT)`, `CAST(? AS SIGNED)`},
		{"concat", func(d sqlDialect) string { return d.concat("','", "park_reason", "','") }, `','||park_reason||','`, `CONCAT(',',park_reason,',')`},
		{"escapeClause", func(d sqlDialect) string { return d.escapeClause() }, ` ESCAPE '\'`, ` ESCAPE '\\'`},
		{"deleteWhereIn", func(d sqlDialect) string {
			return d.deleteWhereIn("t", "id", "SELECT id FROM t WHERE n<? ORDER BY n,id LIMIT ?")
		}, `DELETE FROM t WHERE id IN (SELECT id FROM t WHERE n<? ORDER BY n,id LIMIT ?)`,
			`DELETE FROM t WHERE id IN (SELECT * FROM (SELECT id FROM t WHERE n<? ORDER BY n,id LIMIT ?) AS picked)`},
		{"updateOrdered", func(d sqlDialect) string {
			return d.updateOrdered("t", "id", "n=?", "n>? AND k<>''", "n,id")
		}, `UPDATE t SET n=? WHERE id IN (SELECT id FROM t WHERE n>? AND k<>'' ORDER BY n,id LIMIT ?)`,
			`UPDATE t SET n=? WHERE n>? AND k<>'' ORDER BY n,id LIMIT ?`},
	}
	for _, d := range []struct {
		name    string
		dialect sqlDialect
		mysql   bool
	}{{"sqlite", dialectSQLite, false}, {"postgres", dialectPostgres, false}, {"mysql", dialectMySQL, true}} {
		for _, c := range cases {
			want := c.want
			if d.mysql {
				want = c.mysql
			}
			if got := c.gen(d.dialect); got != want {
				t.Errorf("%s %s:\n got %s\nwant %s", d.name, c.name, got, want)
			}
		}
	}
	if got := dialectSQLite.rebind(`SELECT ? FROM t WHERE a=? AND b=?`); got != `SELECT ? FROM t WHERE a=? AND b=?` {
		t.Errorf("sqlite rebind = %s", got)
	}
	if got := dialectPostgres.rebind(`SELECT ? FROM t WHERE a=? AND b=?`); got != `SELECT $1 FROM t WHERE a=$2 AND b=$3` {
		t.Errorf("postgres rebind = %s", got)
	}
	if got := dialectMySQL.rebind(`SELECT ? FROM t WHERE a=? AND b=?`); got != `SELECT ? FROM t WHERE a=? AND b=?` {
		t.Errorf("mysql rebind = %s", got)
	}
	if got := dialectSQLite.batchOrder() + "/" + dialectPostgres.batchOrder() + "/" + dialectMySQL.batchOrder(); got != "rowid/seq/seq" {
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
	if dialectMySQL.laneInboxInsert() != dialectPostgres.laneInboxInsert() {
		t.Errorf("mysql insert:\n got %s\nwant the Postgres text", dialectMySQL.laneInboxInsert())
	}
	if got, want := dialectSQLite.laneInboxInsert(), `INSERT INTO lane_inbox(id,`+tail+`(SELECT max(COALESCE((SELECT max(id) FROM lane_inbox),0),floor)+1 FROM lane_inbox_id_floor WHERE singleton=1),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`; got != want {
		t.Errorf("sqlite insert:\n got %s\nwant %s", got, want)
	}
}

// Every transaction states its isolation: READ COMMITTED on Postgres and on
// MySQL (whose InnoDB default is REPEATABLE READ with next-key locks), which a
// server default must not change, and the driver's own on SQLite.
func TestSQLDialectBeginsAtItsIsolationLevel(t *testing.T) {
	for _, c := range []struct {
		name    string
		dialect sqlDialect
		want    sql.IsolationLevel
	}{{"sqlite", dialectSQLite, sql.LevelDefault}, {"postgres", dialectPostgres, sql.LevelReadCommitted}, {"mysql", dialectMySQL, sql.LevelReadCommitted}} {
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
// MySQL takes the migration lock as a session lock it gives back, and the
// supersede lock as a row lock the transaction's end releases.
func TestSQLDialectLocks(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name    string
		dialect sqlDialect
		want    []string
	}{
		{"sqlite", dialectSQLite, nil},
		{"postgres", dialectPostgres, []string{`SELECT pg_advisory_xact_lock($1)`, `SELECT pg_advisory_xact_lock(hashtext($1))`}},
		{"mysql", dialectMySQL, []string{
			`SELECT GET_LOCK(CONCAT('panewire-migrate-',MD5(DATABASE())),?)`,
			`INSERT INTO supersede_lane_cause(lock_key) VALUES(SHA2(?,256)) AS incoming ON DUPLICATE KEY UPDATE lock_key=supersede_lane_cause.lock_key`,
			`SELECT RELEASE_LOCK(CONCAT('panewire-migrate-',MD5(DATABASE())))`,
		}},
	} {
		s, log := recordedStore(t, c.dialect)
		tx, err := s.beginTx(ctx)
		if err != nil {
			t.Fatal(err)
		}
		release, err := c.dialect.lockMigration(ctx, tx)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.dialect.lockLaneCause(ctx, tx, "lane", "cause"); err != nil {
			t.Fatal(err)
		}
		if err := release(); err != nil {
			t.Fatal(err)
		}
		tx.Rollback()
		if got := log.snapshot(); strings.Join(got, "\n") != strings.Join(c.want, "\n") {
			t.Errorf("%s locks sent %q, want %q", c.name, got, c.want)
		}
	}
}

// The supersede lock names the (lane, cause) pair: two pairs never share a row,
// and the pair is hashed, not stored, so the table's key has a fixed size.
func TestMySQLLaneCauseLockKeysThePair(t *testing.T) {
	s, log := recordedStore(t, dialectMySQL)
	tx, err := s.beginTx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := dialectMySQL.lockLaneCause(context.Background(), tx, "lane-a", "cause-b"); err != nil {
		t.Fatal(err)
	}
	if len(log.args) != 1 || len(log.args[0]) != 1 || log.args[0][0] != "lane-a\x1fcause-b" {
		t.Errorf("lock bound %q, want the lane and cause joined by US", log.args)
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
	if got, ok := dialectMySQL.storedKey(key).([]byte); !ok || string(got) != key {
		t.Errorf("mysql storedKey = %#v, want the raw bytes", dialectMySQL.storedKey(key))
	}
}

// The statements the store's call sites send. Each is the text the hub ran
// before the seam existed (SQLite form; Postgres numbers the placeholders; the
// MySQL text is spelled out where it differs), so
// moving a call site off the seam or changing what it spells fails here, per
// dialect, without a database.
func TestHubStoreCallSiteStatements(t *testing.T) {
	ctx := context.Background()
	now := time.UnixMilli(1_700_000_000_000).UTC()
	// want is the SQLite text (Postgres numbers its placeholders); mysql, when
	// set, is the MySQL text of a statement that differs.
	calls := []struct {
		name        string
		run         func(s *laneInboxStore)
		want, mysql string
	}{
		{"account credit mark", func(s *laneInboxStore) {
			s.putAccountCreditMark(ctx, "acct", accountCreditMark{Pool: "p", Marked: now})
		},
			`INSERT INTO account_credit_marks(account_key,pool,marked_ms) VALUES(?,?,?) ON CONFLICT(account_key) DO UPDATE SET pool=excluded.pool,marked_ms=excluded.marked_ms`,
			`INSERT INTO account_credit_marks(account_key,pool,marked_ms) VALUES(?,?,?) AS incoming ON DUPLICATE KEY UPDATE pool=incoming.pool,marked_ms=incoming.marked_ms`},
		{"drain wake", func(s *laneInboxStore) { s.saveDrainWake(ctx, "lane", &drainWakeLane{}) },
			`INSERT INTO drain_wake_lanes(lane,pending,last_sent) VALUES(?,?,?) ON CONFLICT(lane) DO UPDATE SET pending=excluded.pending,last_sent=excluded.last_sent`,
			`INSERT INTO drain_wake_lanes(lane,pending,last_sent) VALUES(?,?,?) AS incoming ON DUPLICATE KEY UPDATE pending=incoming.pending,last_sent=incoming.last_sent`},
		{"one-time action", func(s *laneInboxStore) { s.putOneTimeAction(ctx, oneTimeActionKey{}, oneTimeActionState{}) },
			`INSERT INTO one_time_action_notices(machine,kind,need,told_ms,gone_ms) VALUES(?,?,?,?,?) ON CONFLICT(machine,kind,need) DO UPDATE SET told_ms=excluded.told_ms,gone_ms=excluded.gone_ms`,
			`INSERT INTO one_time_action_notices(machine,kind,need,told_ms,gone_ms) VALUES(?,?,?,?,?) AS incoming ON DUPLICATE KEY UPDATE told_ms=incoming.told_ms,gone_ms=incoming.gone_ms`},
		{"quota refresh attempt", func(s *laneInboxStore) { s.recordQuotaRefreshAttempt(ctx, "m", "p", "ok", now) },
			`INSERT INTO quota_refresh_attempts(machine,pool,outcome,seen_ms) VALUES(?,?,?,?) ON CONFLICT(machine,pool) DO UPDATE SET outcome=excluded.outcome,seen_ms=excluded.seen_ms`,
			`INSERT INTO quota_refresh_attempts(machine,pool,outcome,seen_ms) VALUES(?,?,?,?) AS incoming ON DUPLICATE KEY UPDATE outcome=incoming.outcome,seen_ms=incoming.seen_ms`},
		{"spawn start-by", func(s *laneInboxStore) { s.recordSpawnStartBy(ctx, "node", 5) },
			`INSERT INTO spawn_attempt_start_by(node_id,start_by_ms) VALUES(?,?) ON CONFLICT(node_id) DO UPDATE SET start_by_ms=CASE WHEN excluded.start_by_ms>spawn_attempt_start_by.start_by_ms THEN excluded.start_by_ms ELSE spawn_attempt_start_by.start_by_ms END`,
			`INSERT INTO spawn_attempt_start_by(node_id,start_by_ms) VALUES(?,?) AS incoming ON DUPLICATE KEY UPDATE start_by_ms=GREATEST(spawn_attempt_start_by.start_by_ms,incoming.start_by_ms)`},
		{"placement snapshot", func(s *laneInboxStore) { s.saveSpawnPlacementSnapshot(ctx, "req", 1, []byte("{}"), "") },
			`INSERT INTO spawn_placement_snapshots(request_id,attempt,payload,delivery) VALUES(?,?,?,?) ON CONFLICT(request_id,attempt) DO UPDATE SET payload=excluded.payload,delivery=excluded.delivery WHERE spawn_placement_snapshots.recorded=0`,
			`INSERT INTO spawn_placement_snapshots(request_id,attempt,payload,delivery) VALUES(?,?,?,?) AS incoming ON DUPLICATE KEY UPDATE payload=IF(spawn_placement_snapshots.recorded=0,incoming.payload,spawn_placement_snapshots.payload),delivery=IF(spawn_placement_snapshots.recorded=0,incoming.delivery,spawn_placement_snapshots.delivery)`},
		{"inbox summary", func(s *laneInboxStore) { s.saveInboxSummary("k", "lane", "line", 1) },
			`INSERT INTO lane_inbox_surfaces(delivery_key,lane,through_id,summary) VALUES(?,?,?,?) ON CONFLICT(delivery_key) DO NOTHING`,
			`INSERT INTO lane_inbox_surfaces(delivery_key,lane,through_id,summary) VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE lane_inbox_surfaces.delivery_key=lane_inbox_surfaces.delivery_key`},
		{"batch quarantine", func(s *laneInboxStore) { s.quarantineBatch("b", "r") },
			`INSERT INTO lane_inbox_batch_quarantine(batch_id,reason) VALUES(?,?) ON CONFLICT(batch_id) DO NOTHING`,
			`INSERT INTO lane_inbox_batch_quarantine(batch_id,reason) VALUES(?,?) ON DUPLICATE KEY UPDATE lane_inbox_batch_quarantine.batch_id=lane_inbox_batch_quarantine.batch_id`},
		{"spawn queue", func(s *laneInboxStore) {
			tx, _ := s.beginTx(ctx)
			s.enqueueSpawn(ctx, tx, "req")
			tx.Rollback()
		}, `INSERT INTO spawn_queue(request_id,next_check_ms) VALUES(?,0) ON CONFLICT DO NOTHING`,
			`INSERT INTO spawn_queue(request_id,next_check_ms) VALUES(?,0) ON DUPLICATE KEY UPDATE spawn_queue.request_id=spawn_queue.request_id`},
		{"retention of consults", func(s *laneInboxStore) { s.pruneHubStore(now, 10) },
			`DELETE FROM consult_requests WHERE id IN (SELECT id FROM consult_requests WHERE state NOT IN ('queued','running') AND NOT (state='answered' AND notified=0) AND updated_ms<? ORDER BY updated_ms,id LIMIT ?)`,
			`DELETE FROM consult_requests WHERE id IN (SELECT * FROM (SELECT id FROM consult_requests WHERE state NOT IN ('queued','running') AND NOT (state='answered' AND notified=0) AND updated_ms<? ORDER BY updated_ms,id LIMIT ?) AS picked)`},
		{"retention of sent batches", func(s *laneInboxStore) { s.pruneHubStore(now, 10) },
			`DELETE FROM lane_inbox_batches WHERE id IN (` + sentBatchesPrunable + `)`,
			`DELETE FROM lane_inbox_batches WHERE id IN (SELECT * FROM (` + sentBatchesPrunable + `) AS picked)`},
		{"spawn grant rows", func(s *laneInboxStore) { s.spawnGrantRowCount(ctx) },
			`SELECT count(*) FROM spawn_operations WHERE status LIKE '%\_grant' ESCAPE '\'`,
			`SELECT count(*) FROM spawn_operations WHERE status LIKE '%\_grant' ESCAPE '\\'`},
		{"park spawn", func(s *laneInboxStore) { s.parkSpawn(ctx, "req", 1, "why", nil, now, now) },
			`UPDATE spawn_operations SET next_check_ms=CASE WHEN next_check_ms>? AND park_reason<>'' THEN next_check_ms ELSE ? END,park_reason=CASE WHEN next_check_ms>? AND park_reason<>'' THEN park_reason ELSE ? END,park_priority=CASE WHEN park_since_ms=0 THEN -1 ELSE park_priority END,park_priority_failures=CASE WHEN park_since_ms=0 THEN 0 ELSE park_priority_failures END,park_since_ms=CASE WHEN park_since_ms=0 THEN CAST(? AS BIGINT) ELSE park_since_ms END WHERE request_id=? AND attempt=? AND status='accepted' AND node_id IS NULL`,
			`UPDATE spawn_operations SET next_check_ms=CASE WHEN next_check_ms>? AND park_reason<>'' THEN next_check_ms ELSE ? END,park_reason=CASE WHEN next_check_ms>? AND park_reason<>'' THEN park_reason ELSE ? END,park_priority=CASE WHEN park_since_ms=0 THEN -1 ELSE park_priority END,park_priority_failures=CASE WHEN park_since_ms=0 THEN 0 ELSE park_priority_failures END,park_since_ms=CASE WHEN park_since_ms=0 THEN CAST(? AS SIGNED) ELSE park_since_ms END WHERE request_id=? AND attempt=? AND status='accepted' AND node_id IS NULL`},
		{"wake parked spawns", func(s *laneInboxStore) { s.wakeParkedSpawns(ctx, []string{"a", "b"}, 5, now) },
			`UPDATE spawn_operations SET next_check_ms=? WHERE request_id IN (SELECT request_id FROM spawn_operations WHERE status='accepted' AND node_id IS NULL AND next_check_ms>? AND ((','||park_reason||',') LIKE ? OR (','||park_reason||',') LIKE ?) ORDER BY next_check_ms,request_id LIMIT ?)`,
			`UPDATE spawn_operations SET next_check_ms=? WHERE status='accepted' AND node_id IS NULL AND next_check_ms>? AND ((CONCAT(',',park_reason,',')) LIKE ? OR (CONCAT(',',park_reason,',')) LIKE ?) ORDER BY next_check_ms,request_id LIMIT ?`},
	}
	for _, d := range []struct {
		name    string
		dialect sqlDialect
		bind    func(string) string
		mysql   bool
	}{
		{"sqlite", dialectSQLite, func(q string) string { return q }, false},
		{"postgres", dialectPostgres, testRebind, false},
		{"mysql", dialectMySQL, func(q string) string { return q }, true},
	} {
		for _, c := range calls {
			s, log := recordedStore(t, d.dialect)
			c.run(s)
			want := d.bind(c.want)
			if d.mysql && c.mysql != "" {
				want = c.mysql
			}
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

package panewire

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// These run against the MySQL service container CI starts (PANEWIRE_TEST_HUB_MYSQL_URL)
// and skip where it is unset: no database server is started on a host for
// them. The store contract suite itself runs on MySQL through
// laneInboxBackends; what follows is what only MySQL has: its migrations, its
// isolation setting, its duplicate-key no-op and its locks.

// hubMySQLSchemaState is everything a migration could change in the test
// database: columns, indexes, constraints and the recorded migrations.
func hubMySQLSchemaState(t *testing.T, s *laneInboxStore) []string {
	t.Helper()
	// One query per kind of line: information_schema and the ledger carry
	// different collations, which a UNION of them would refuse to mix.
	var state []string
	for _, q := range []string{
		`SELECT CONCAT('column ',table_name,'.',column_name,' ',column_type,' ',is_nullable,' ',COALESCE(column_default,'NULL'),' ',COALESCE(collation_name,'')) FROM information_schema.columns WHERE table_schema=DATABASE()`,
		`SELECT CONCAT('index ',table_name,'.',index_name,' ',seq_in_index,' ',column_name,' ',non_unique) FROM information_schema.statistics WHERE table_schema=DATABASE()`,
		`SELECT CONCAT('constraint ',table_name,'.',constraint_name,' ',constraint_type) FROM information_schema.table_constraints WHERE table_schema=DATABASE()`,
		`SELECT CONCAT('check ',constraint_name,' ',check_clause) FROM information_schema.check_constraints WHERE constraint_schema=DATABASE()`,
		`SELECT CONCAT('migration ',version,' ',applied_at) FROM panewire_schema_migrations`,
	} {
		rows, err := s.db.Query(q)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			state = append(state, line)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	slices.Sort(state)
	return state
}

func hubMySQLMigrationCount(t *testing.T) int {
	t.Helper()
	names, err := fs.Glob(hubMySQLMigrations, "hub_mysql_migrations/*.sql")
	if err != nil || len(names) == 0 {
		t.Fatalf("embedded migrations = %v, %v", names, err)
	}
	return len(names)
}

func countRows(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// Applying the migrations a second time changes nothing: same columns,
// indexes, constraints, and the same recorded versions and times.
func TestLaneInboxStoreMySQLMigrationsTwiceChangeNothing(t *testing.T) {
	config, _ := hubMySQLTestDatabase(t)
	first, err := openLaneInboxStoreMySQLConfig(context.Background(), config.Clone())
	if err != nil {
		t.Fatal(err)
	}
	before := hubMySQLSchemaState(t, first)
	_ = first.close()
	if !slices.ContainsFunc(before, func(line string) bool { return strings.HasPrefix(line, "column lane_inbox.must_ack ") }) {
		t.Fatalf("schema lacks must_ack: %v", before)
	}
	second, err := openLaneInboxStoreMySQLConfig(context.Background(), config.Clone())
	if err != nil {
		t.Fatal(err)
	}
	defer second.close()
	if after := hubMySQLSchemaState(t, second); !slices.Equal(before, after) {
		t.Fatalf("second migration run changed the schema:\nbefore %v\nafter  %v", before, after)
	}
	if n := countRows(t, second.db, `SELECT count(*) FROM panewire_schema_migrations`); n != hubMySQLMigrationCount(t) {
		t.Fatalf("recorded migrations = %d, want %d", n, hubMySQLMigrationCount(t))
	}
	// The fence's one row is there once.
	if n := countRows(t, second.db, `SELECT count(*) FROM supersede_fence`); n != 1 {
		t.Fatalf("supersede_fence rows = %d, want 1", n)
	}
}

// A run that stopped after a statement and before its ledger row (MySQL
// commits DDL as it runs) is repeated without error: each file's applied-if
// guard sees its effect and skips the statement.
func TestLaneInboxStoreMySQLRerunAfterAStoppedMigrationIsANoOp(t *testing.T) {
	config, _ := hubMySQLTestDatabase(t)
	first, err := openLaneInboxStoreMySQLConfig(context.Background(), config.Clone())
	if err != nil {
		t.Fatal(err)
	}
	before := hubMySQLSchemaState(t, first)
	// Forget every file after the first and the fence seed's row: the tables,
	// indexes and the seed are all in place.
	if _, err := first.db.Exec(`DELETE FROM panewire_schema_migrations WHERE version>'0001'`); err != nil {
		t.Fatal(err)
	}
	_ = first.close()
	second, err := openLaneInboxStoreMySQLConfig(context.Background(), config.Clone())
	if err != nil {
		t.Fatal(err)
	}
	defer second.close()
	after := hubMySQLSchemaState(t, second)
	strip := func(lines []string) []string {
		return slices.DeleteFunc(slices.Clone(lines), func(l string) bool { return strings.HasPrefix(l, "migration ") })
	}
	if !slices.Equal(strip(before), strip(after)) {
		t.Fatalf("the rerun changed the schema:\nbefore %v\nafter  %v", before, after)
	}
	if n := countRows(t, second.db, `SELECT count(*) FROM panewire_schema_migrations`); n != hubMySQLMigrationCount(t) {
		t.Fatalf("recorded migrations = %d, want %d", n, hubMySQLMigrationCount(t))
	}
	if n := countRows(t, second.db, `SELECT count(*) FROM supersede_fence`); n != 1 {
		t.Fatalf("supersede_fence rows = %d, want 1", n)
	}
}

// Hubs starting together on one database migrate it once: the session lock
// holds the later ones until the first has recorded every file.
func TestLaneInboxStoreMySQLConcurrentStartsMigrateOnce(t *testing.T) {
	config, _ := hubMySQLTestDatabase(t)
	const starts = 4
	stores := make([]*laneInboxStore, starts)
	errs := make([]error, starts)
	var wg sync.WaitGroup
	for i := range stores {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stores[i], errs[i] = openLaneInboxStoreMySQLConfig(context.Background(), config.Clone())
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("start %d: %v", i, err)
		}
		defer stores[i].close()
	}
	if n := countRows(t, stores[0].db, `SELECT count(*) FROM panewire_schema_migrations`); n != hubMySQLMigrationCount(t) {
		t.Fatalf("recorded migrations = %d, want %d", n, hubMySQLMigrationCount(t))
	}
}

// The pool stays under the account's 24 connections, and the migration lock is
// back: a second migration on the same database does not wait for it.
func TestLaneInboxStoreMySQLPoolAndLockLimits(t *testing.T) {
	config, _ := hubMySQLTestDatabase(t)
	s, err := openLaneInboxStoreMySQLConfig(context.Background(), config.Clone())
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if got := s.db.Stats().MaxOpenConnections; got != hubMySQLMaxConns || got > 24 {
		t.Errorf("MaxOpenConnections = %d, want %d (at most 24)", got, hubMySQLMaxConns)
	}
	start := time.Now()
	if err := migrateHubMySQL(context.Background(), s.db); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Errorf("a second migration took %v: the first run kept the lock", took)
	}
}

// Every transaction states READ COMMITTED itself: the server's default is
// REPEATABLE READ, and this pool does not set the session's. Mutant: begin
// without the isolation option leaves REPEATABLE-READ here.
func TestMySQLTransactionsStateReadCommitted(t *testing.T) {
	config, _ := hubMySQLTestDatabase(t)
	bare := config.Clone()
	delete(bare.Params, "transaction_isolation")
	db, err := openHubMySQL(context.Background(), bare)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sessionLevel := func(q interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	}) string {
		var got string
		if err := q.QueryRowContext(context.Background(), `SELECT @@transaction_isolation`).Scan(&got); err != nil {
			t.Fatal(err)
		}
		return got
	}
	if got := sessionLevel(db); got != "REPEATABLE-READ" {
		t.Fatalf("the server default is %s: this test no longer shows the explicit setting", got)
	}
	// SET TRANSACTION (the driver's form) changes the next transaction only,
	// not @@transaction_isolation, so the level a transaction runs at is read
	// from InnoDB once the transaction has touched a table.
	if _, err := db.Exec(`CREATE TABLE isolation_probe (x INT)`); err != nil {
		t.Fatal(err)
	}
	tx, err := dialectMySQL.begin(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`SELECT COUNT(*) FROM isolation_probe`); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := tx.QueryRow(`SELECT trx_isolation_level FROM information_schema.innodb_trx WHERE trx_mysql_thread_id=CONNECTION_ID()`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "READ COMMITTED" {
		t.Errorf("a store transaction runs at %s, want READ COMMITTED", got)
	}
	// The store's own pool, opened from the URL, runs at it for statements
	// outside a transaction too.
	s, err := openLaneInboxStoreMySQLConfig(context.Background(), config.Clone())
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if got := sessionLevel(s.db); got != "READ-COMMITTED" {
		t.Errorf("the store's pool runs at %s, want READ-COMMITTED", got)
	}
}

// The duplicate-key no-op ignores a duplicate and nothing else: INSERT IGNORE
// would store a value too long for its column truncated, with a warning.
func TestMySQLDuplicateKeyNoOpDoesNotSwallowOtherErrors(t *testing.T) {
	config, _ := hubMySQLTestDatabase(t)
	s, err := openLaneInboxStoreMySQLConfig(context.Background(), config.Clone())
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	const insert = `INSERT INTO lane_inbox_batch_quarantine(batch_id,reason) VALUES(?,?)`
	for i := 0; i < 2; i++ {
		if _, err := s.db.Exec(s.dialect.insertIgnore(insert, "batch_id"), "b1", "r"); err != nil {
			t.Fatalf("duplicate no-op %d: %v", i, err)
		}
	}
	if n := countRows(t, s.db, `SELECT count(*) FROM lane_inbox_batch_quarantine`); n != 1 {
		t.Fatalf("rows = %d, want 1", n)
	}
	_, err = s.db.Exec(s.dialect.insertIgnore(insert, "batch_id"), strings.Repeat("x", 300), "r")
	var tooLong *mysql.MySQLError
	if !errors.As(err, &tooLong) || tooLong.Number != 1406 {
		t.Fatalf("an over-long key: err = %v, want MySQL error 1406 (data too long)", err)
	}
}

// The supersede lock is a row lock: a second supersede of the same (lane,
// cause) waits for the first transaction to end, and one of another pair does
// not.
func TestMySQLLaneCauseLockSerializesThePair(t *testing.T) {
	config, _ := hubMySQLTestDatabase(t)
	s, err := openLaneInboxStoreMySQLConfig(context.Background(), config.Clone())
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	first, err := s.beginTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Rollback()
	if err := s.dialect.lockLaneCause(ctx, first, "lane-a", "cause-1"); err != nil {
		t.Fatal(err)
	}
	other, err := s.beginTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Rollback()
	if err := s.dialect.lockLaneCause(ctx, other, "lane-a", "cause-2"); err != nil {
		t.Fatalf("another pair waited: %v", err)
	}
	second, err := s.beginTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Rollback()
	got := make(chan error, 1)
	go func() { got <- s.dialect.lockLaneCause(ctx, second, "lane-a", "cause-1") }()
	select {
	case err := <-got:
		t.Fatalf("the same pair did not wait: %v", err)
	case <-time.After(300 * time.Millisecond): // a window that proves nothing more happens
	}
	if err := first.Commit(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "the second supersede takes the row once the first ends", func() bool {
		select {
		case err := <-got:
			if err != nil {
				t.Fatal(err)
			}
			return true
		default:
			return false
		}
	})
}

// A hub started with a mysql:// URL opens the MySQL store, and not the SQLite
// fallback.
func TestHubServerOpensMySQLInbox(t *testing.T) {
	_, dsn := hubMySQLTestDatabase(t)
	now := time.UnixMilli(1_700_000_000_000).UTC()
	fallback := filepath.Join(t.TempDir(), "unused.sqlite3")
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "op"}, InboxStorePath: fallback, InboxDatabaseURL: dsn, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if h.inbox == nil || h.inbox.store.dialect != dialectMySQL {
		t.Fatal("hub did not open the MySQL inbox")
	}
	if _, err := os.Stat(fallback); err == nil {
		t.Fatal("hub created the SQLite fallback while a database URL was set")
	}
}

// A producer ACK waits for a durable commit, so the hub refuses a server that
// does not flush its log at every commit. MySQL has no session setting for it
// (Postgres' synchronous_commit is one): the hub reads the server's.
func TestLaneInboxStoreMySQLRefusesARelaxedServer(t *testing.T) {
	config, _ := hubMySQLTestDatabase(t)
	admin, err := openHubMySQL(context.Background(), config.Clone())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.Exec(`SET GLOBAL innodb_flush_log_at_trx_commit=2`); err != nil {
		t.Skipf("the test account cannot change the server's setting: %v", err)
	}
	restored := false
	restore := func() {
		if !restored {
			restored = true
			if _, err := admin.Exec(`SET GLOBAL innodb_flush_log_at_trx_commit=1`); err != nil {
				t.Error(err)
			}
		}
	}
	defer restore()
	if _, err := openHubMySQL(context.Background(), config.Clone()); err == nil || !strings.Contains(err.Error(), "innodb_flush_log_at_trx_commit") {
		t.Fatalf("open on a relaxed server = %v, want a refusal naming the setting", err)
	}
	restore()
	db, err := openHubMySQL(context.Background(), config.Clone())
	if err != nil {
		t.Fatalf("open after restoring the setting: %v", err)
	}
	db.Close()
}

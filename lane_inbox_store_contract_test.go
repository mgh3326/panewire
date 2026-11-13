package panewire

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// hubTestDatabaseURLEnv names a disposable Postgres database. CI sets it from
// a service container; locally it is unset and the Postgres half skips — no
// database server is started on a host for these tests.
const hubTestDatabaseURLEnv = "PANEWIRE_TEST_HUB_DATABASE_URL"

// laneInboxBackend opens fresh, empty storage. The returned opener opens that
// same storage again on every call, so a test can close and reopen it.
type laneInboxBackend struct {
	name  string
	fresh func(t *testing.T) (open func() *laneInboxStore)
}

func laneInboxBackends() []laneInboxBackend {
	return []laneInboxBackend{
		{name: "sqlite", fresh: func(t *testing.T) func() *laneInboxStore {
			path := filepath.Join(t.TempDir(), "inbox.sqlite3")
			return func() *laneInboxStore {
				s, err := openLaneInboxStore(path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = s.close() })
				return s
			}
		}},
		{name: "postgres", fresh: func(t *testing.T) func() *laneInboxStore {
			config, _ := hubPostgresTestSchema(t)
			return func() *laneInboxStore {
				s, err := openLaneInboxStorePostgresConfig(context.Background(), config.Copy())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = s.close() })
				return s
			}
		}},
	}
}

// hubPostgresTestSchema creates a private schema in the CI database and
// returns a config and a DSN whose search_path points at it. The schema is
// dropped when the test ends, so runs with -count=N never share rows.
func hubPostgresTestSchema(t *testing.T) (*pgx.ConnConfig, string) {
	t.Helper()
	base := os.Getenv(hubTestDatabaseURLEnv)
	if base == "" {
		t.Skip(hubTestDatabaseURLEnv + " is unset; the Postgres backend runs in CI against a service container")
	}
	admin, err := pgx.ParseConfig(base)
	if err != nil {
		t.Fatal("test database URL is invalid")
	}
	schema := "pwtest_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	db := stdlib.OpenDB(*admin)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := db.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); err != nil {
			t.Error(err)
		}
	})
	config := admin.Copy()
	config.RuntimeParams["search_path"] = schema
	u, err := url.Parse(base)
	if err != nil || u.Scheme == "" {
		t.Fatal("test database URL must be a postgres:// URL")
	}
	query := u.Query()
	query.Set("search_path", schema)
	u.RawQuery = query.Encode()
	return config, u.String()
}

// hubInboxStorage names where a test hub keeps its inbox: a SQLite file, or
// a Postgres schema when dsn is set.
type hubInboxStorage struct{ path, dsn string }

// forEachHubInboxStorage runs a hub-level inbox test once per backend.
func forEachHubInboxStorage(t *testing.T, run func(t *testing.T, storage hubInboxStorage)) {
	t.Run("sqlite", func(t *testing.T) {
		run(t, hubInboxStorage{path: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	})
	t.Run("postgres", func(t *testing.T) {
		_, dsn := hubPostgresTestSchema(t)
		run(t, hubInboxStorage{dsn: dsn})
	})
}

func forEachLaneInboxBackend(t *testing.T, run func(t *testing.T, open func() *laneInboxStore)) {
	for _, backend := range laneInboxBackends() {
		t.Run(backend.name, func(t *testing.T) {
			run(t, backend.fresh(t))
		})
	}
}

func TestLaneInboxStoreContractPutIsFirstWriterWins(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		now := time.UnixMilli(1_700_000_000_000).UTC()
		first, err := s.put("k1", "lane-a", "lane.event", "inbox", quietEvent("k1", "FYI one"), now, time.Minute, 0)
		if err != nil || !first.Fresh || first.ID < 1 || first.Class != "inbox" || first.ReadAt != 0 || first.DueAt != now.Add(time.Minute).UnixMilli() {
			t.Fatalf("first put = %+v, %v", first, err)
		}
		if first.Event.Text != "FYI one" || first.CreatedAt != now.UnixMilli() {
			t.Fatalf("payload or created_at not round-tripped: %+v", first)
		}
		again, err := s.put("k1", "lane-a", "lane.event", "interrupt", quietEvent("k1", "changed"), now.Add(time.Second), 0, time.Second)
		if err != nil || again.Fresh || again.ID != first.ID || again.Class != "inbox" || again.Event.Text != "FYI one" {
			t.Fatalf("retry changed the stored item: %+v, %v", again, err)
		}
		interrupt, err := s.put("k2", "lane-a", "lane.event", "interrupt", quietEvent("k2", "now"), now, 0, 30*time.Second)
		if err != nil || !interrupt.Fresh || interrupt.ID <= first.ID || interrupt.ReadAt != now.UnixMilli() || interrupt.DigestAt != now.Add(30*time.Second).UnixMilli() {
			t.Fatalf("interrupt put = %+v, %v", interrupt, err)
		}
	})
}

func TestLaneInboxStoreContractListCountsReadAck(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		now := time.UnixMilli(1_700_000_000_000).UTC()
		var ids []int64
		for i, lane := range []string{"lane-a", "lane-a", "lane-b"} {
			key := "k" + string(rune('1'+i))
			item, err := s.put(key, lane, "lane.event", "inbox", quietEvent(key, "FYI"), now, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, item.ID)
		}
		if _, err := s.put("k4", "lane-a", "lane.event", "interrupt", quietEvent("k4", "now"), now, 0, 0); err != nil {
			t.Fatal(err)
		}
		all, err := s.list("", false)
		if err != nil || len(all) != 4 || all[0].ID < all[3].ID {
			t.Fatalf("list all = %+v, %v (want 4, newest first)", all, err)
		}
		unread, err := s.list("lane-a", true)
		if err != nil || len(unread) != 2 || unread[0].ID != ids[1] || unread[1].ID != ids[0] {
			t.Fatalf("unread lane-a = %+v, %v", unread, err)
		}
		counts, err := s.counts("")
		if err != nil || len(counts) != 2 || counts["lane-a"] != 2 || counts["lane-b"] != 1 {
			t.Fatalf("counts = %v, %v", counts, err)
		}
		if counts, err := s.counts("lane-z"); err != nil || len(counts) != 1 || counts["lane-z"] != 0 {
			t.Fatalf("empty lane counts = %v, %v", counts, err)
		}
		if _, err := s.read(ids[0], "lane-b", now); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("read across lanes = %v, want sql.ErrNoRows", err)
		}
		read, err := s.read(ids[0], "lane-a", now.Add(time.Second))
		if err != nil || read.ReadAt != now.Add(time.Second).UnixMilli() {
			t.Fatalf("read = %+v, %v", read, err)
		}
		if again, err := s.read(ids[0], "", now.Add(time.Hour)); err != nil || again.ReadAt != read.ReadAt {
			t.Fatalf("second read moved read_at: %+v, %v", again, err)
		}
		if n, err := s.ack(ids[1], "lane-b", false, now); err != nil || n != 0 {
			t.Fatalf("ack across lanes = %d, %v", n, err)
		}
		if n, err := s.ack(ids[1], "lane-a", false, now.Add(2*time.Second)); err != nil || n != 1 {
			t.Fatalf("ack one = %d, %v", n, err)
		}
		if n, err := s.ack(0, "lane-a", true, now); err != nil || n != 2 {
			t.Fatalf("ack all lane-a = %d, %v (want the read item and the interrupt)", n, err)
		}
		if n, err := s.ack(0, "lane-a", true, now); err != nil || n != 0 {
			t.Fatalf("ack all again = %d, %v", n, err)
		}
		if counts, err := s.counts(""); err != nil || len(counts) != 1 || counts["lane-b"] != 1 {
			t.Fatalf("counts after ack = %v, %v", counts, err)
		}
	})
}

func TestLaneInboxStoreContractFirstUnreadAndWake(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		ctx := context.Background()
		now := time.UnixMilli(1_700_000_000_000).UTC()
		if item, err := s.firstUnread(ctx, "lane-a"); err != nil || item != nil {
			t.Fatalf("empty firstUnread = %+v, %v", item, err)
		}
		wake := s.waitChan()
		if _, err := s.put("i1", "lane-a", "lane.event", "interrupt", quietEvent("i1", "now"), now, 0, 0); err != nil {
			t.Fatal(err)
		}
		select {
		case <-wake:
			t.Fatal("an interrupt insert woke inbox waiters")
		default:
		}
		first, err := s.put("k1", "lane-a", "lane.event", "inbox", quietEvent("k1", "FYI"), now, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-wake:
		default:
			t.Fatal("an inbox insert did not wake waiters")
		}
		if _, err := s.put("k2", "lane-a", "lane.event", "inbox", quietEvent("k2", "FYI"), now, 0, 0); err != nil {
			t.Fatal(err)
		}
		if item, err := s.firstUnread(ctx, "lane-a"); err != nil || item == nil || item.ID != first.ID {
			t.Fatalf("firstUnread = %+v, %v (want the oldest)", item, err)
		}
	})
}

func TestLaneInboxStoreContractDigestAndEscalationBatches(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		now := time.UnixMilli(1_700_000_000_000).UTC()
		for _, lane := range []string{"lane-b", "lane-a"} {
			for _, n := range []string{"1", "2"} {
				key := lane + "-i" + n
				if _, err := s.put(key, lane, "lane.event", "interrupt", quietEvent(key, "now"), now, 0, 10*time.Second); err != nil {
					t.Fatal(err)
				}
			}
		}
		due, err := s.put("due", "lane-a", "lane.event", "inbox", quietEvent("due", "FYI"), now, time.Minute, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.makeBatches(now.Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if batches, err := s.pendingBatches(); err != nil || len(batches) != 0 {
			t.Fatalf("batches before the window closed = %+v, %v", batches, err)
		}
		if err := s.makeBatches(now.Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
		batches, err := s.pendingBatches()
		if err != nil || len(batches) != 3 {
			t.Fatalf("pending = %+v, %v (want two digests and one escalation)", batches, err)
		}
		// Digests are created first, lanes sorted, then escalations.
		want := []string{
			"2 interrupt events | lane lane-a | panewire inbox list --lane lane-a",
			"2 interrupt events | lane lane-b | panewire inbox list --lane lane-b",
			"1 unread inbox items | lane lane-a | panewire inbox list --lane lane-a",
		}
		for i, b := range batches {
			if b.Text != want[i] || b.invalidReason() != "" {
				t.Fatalf("batch %d = %+v, want text %q", i, b, want[i])
			}
		}
		if err := s.makeBatches(now.Add(2 * time.Minute)); err != nil {
			t.Fatal(err)
		}
		if again, err := s.pendingBatches(); err != nil || len(again) != 3 {
			t.Fatalf("items were claimed twice: %+v, %v", again, err)
		}
		items, err := s.list("lane-a", false)
		if err != nil {
			t.Fatal(err)
		}
		for _, i := range items {
			if i.ID == due.ID && i.EscalationBatch != batches[2].ID {
				t.Fatalf("due item claim = %q, want %q", i.EscalationBatch, batches[2].ID)
			}
		}
		if err := s.markBatchSent(batches[0].ID); err != nil {
			t.Fatal(err)
		}
		if err := s.quarantineBatch(batches[1].ID, "test"); err != nil {
			t.Fatal(err)
		}
		if err := s.quarantineBatch(batches[1].ID, "again"); err != nil {
			t.Fatalf("quarantine is not idempotent: %v", err)
		}
		if left, err := s.pendingBatches(); err != nil || len(left) != 1 || left[0].ID != batches[2].ID {
			t.Fatalf("pending after sent+quarantine = %+v, %v", left, err)
		}
	})
}

func TestLaneInboxStartupKeepsHistoricalDottedBatchPending(t *testing.T) {
	store, err := openLaneInboxStore(filepath.Join(t.TempDir(), "inbox.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	if _, err := store.db.ExecContext(context.Background(), `INSERT INTO lane_inbox_batches(id,lane,text) VALUES(?,?,?)`, "batch-legacy", "legacy.parent", "1 unread inbox items | lane legacy.parent"); err != nil {
		t.Fatal(err)
	}
	if err := store.quarantineInvalidBatches(); err != nil {
		t.Fatal(err)
	}
	batches, err := store.pendingBatches()
	if err != nil || len(batches) != 1 || batches[0].Lane != "legacy.parent" {
		t.Fatalf("historical batch was lost: batches=%+v err=%v", batches, err)
	}
}

func TestLaneInboxStoreContractQuarantineInvalidAndRecoverMustAct(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		now := time.UnixMilli(1_700_000_000_000).UTC()
		if _, err := s.db.Exec(s.q(`INSERT INTO lane_inbox_batches(id,lane,text) VALUES(?,?,?),(?,?,?)`), "bad", "lane-a", "two\nlines", "good", "lane-a", "one line"); err != nil {
			t.Fatal(err)
		}
		if err := s.quarantineInvalidBatches(); err != nil {
			t.Fatal(err)
		}
		if batches, err := s.pendingBatches(); err != nil || len(batches) != 1 || batches[0].ID != "good" {
			t.Fatalf("pending = %+v, %v", batches, err)
		}
		mustAct, err := s.put("ready", "lane-a", "lane.event", "inbox", quietEvent("ready", "READY | PR"), now, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		plain, err := s.put("plain", "lane-a", "lane.event", "inbox", quietEvent("plain", "FYI"), now, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		policy, err := loadDeliveryPolicy("default")
		if err != nil {
			t.Fatal(err)
		}
		if err := s.recoverMustAct(policy, time.Now()); err != nil {
			t.Fatal(err)
		}
		items, err := s.list("lane-a", true)
		if err != nil {
			t.Fatal(err)
		}
		for _, i := range items {
			if i.ID == mustAct.ID && i.DueAt != 1 {
				t.Fatalf("must-act item was not made due: %+v", i)
			}
			if i.ID == plain.ID && i.DueAt != 0 {
				t.Fatalf("plain item was made due: %+v", i)
			}
		}
	})
}

func TestLaneInboxStoreContractSurvivesReopen(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		now := time.UnixMilli(1_700_000_000_000).UTC()
		first, err := s.put("k1", "lane-a", "lane.event", "inbox", quietEvent("k1", "FYI"), now, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.close(); err != nil {
			t.Fatal(err)
		}
		// Reopening reruns the schema setup; it must be a no-op on live data.
		s = open()
		again, err := s.put("k1", "lane-a", "lane.event", "inbox", quietEvent("k1", "changed"), now, 0, 0)
		if err != nil || again.Fresh || again.ID != first.ID || again.Event.Text != "FYI" {
			t.Fatalf("item after reopen = %+v, %v", again, err)
		}
		next, err := s.put("k2", "lane-a", "lane.event", "inbox", quietEvent("k2", "FYI"), now, 0, 0)
		if err != nil || next.ID <= first.ID {
			t.Fatalf("id after reopen = %+v, %v", next, err)
		}
	})
}

// hubPostgresSchemaState is everything a migration could change in the test
// schema: columns, indexes, constraints, and the recorded migrations.
func hubPostgresSchemaState(t *testing.T, s *laneInboxStore) []string {
	t.Helper()
	rows, err := s.db.Query(`
SELECT 'column '||table_name||'.'||column_name||' '||data_type||' '||is_nullable||' '||coalesce(column_default,'')
  FROM information_schema.columns WHERE table_schema=current_schema()
UNION ALL SELECT 'index '||indexname||' '||indexdef FROM pg_indexes WHERE schemaname=current_schema()
UNION ALL SELECT 'constraint '||conname||' '||pg_get_constraintdef(oid) FROM pg_constraint WHERE connamespace=current_schema()::regnamespace
UNION ALL SELECT 'migration '||version||' '||applied_at::text FROM panewire_schema_migrations
ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var state []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		state = append(state, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return state
}

func hubPostgresMigrationCount(t *testing.T) int {
	t.Helper()
	names, err := fs.Glob(hubPostgresMigrations, "hub_postgres_migrations/*.sql")
	if err != nil || len(names) == 0 {
		t.Fatalf("embedded migrations = %v, %v", names, err)
	}
	return len(names)
}

// Applying the migrations a second time changes nothing: same columns,
// indexes, constraints, and the same recorded versions and times.
func TestLaneInboxStorePostgresMigrationsTwiceChangeNothing(t *testing.T) {
	config, _ := hubPostgresTestSchema(t)
	first, err := openLaneInboxStorePostgresConfig(context.Background(), config.Copy())
	if err != nil {
		t.Fatal(err)
	}
	before := hubPostgresSchemaState(t, first)
	_ = first.close()
	if !slices.ContainsFunc(before, func(line string) bool { return strings.HasPrefix(line, "column lane_inbox.must_ack ") }) {
		t.Fatalf("schema lacks must_ack: %v", before)
	}
	second, err := openLaneInboxStorePostgresConfig(context.Background(), config.Copy())
	if err != nil {
		t.Fatal(err)
	}
	defer second.close()
	if after := hubPostgresSchemaState(t, second); !slices.Equal(before, after) {
		t.Fatalf("second migration run changed the schema:\nbefore %v\nafter  %v", before, after)
	}
	var versions int
	if err := second.db.QueryRow(`SELECT count(*) FROM panewire_schema_migrations`).Scan(&versions); err != nil || versions != hubPostgresMigrationCount(t) {
		t.Fatalf("recorded migrations = %d, %v", versions, err)
	}
}

// Hubs starting at once on an empty database all open it, and each
// migration is applied once: one recorded row each, and no start fails on a
// second CREATE racing the first.
func TestLaneInboxStorePostgresConcurrentStartsMigrateOnce(t *testing.T) {
	config, _ := hubPostgresTestSchema(t)
	const hubs = 4
	stores := make([]*laneInboxStore, hubs)
	errs := make([]error, hubs)
	var start, wg sync.WaitGroup
	start.Add(1)
	for i := range hubs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start.Wait()
			stores[i], errs[i] = openLaneInboxStorePostgresConfig(context.Background(), config.Copy())
		}()
	}
	start.Done()
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("hub %d failed to start: %v", i, err)
		}
		defer stores[i].close()
	}
	rows, err := stores[0].db.Query(`SELECT version, count(*) FROM panewire_schema_migrations GROUP BY version`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	versions := 0
	for rows.Next() {
		var version string
		var n int
		if err := rows.Scan(&version, &n); err != nil || n != 1 {
			t.Fatalf("migration %s recorded %d times, %v", version, n, err)
		}
		versions++
	}
	if err := rows.Err(); err != nil || versions != hubPostgresMigrationCount(t) {
		t.Fatalf("recorded versions = %d, %v", versions, err)
	}
}

// Every database call in the store and the Postgres backend passes a context
// (the CodeRabbit Major on #126). The store's contexts come from
// s.op/s.opWithin and carry a deadline.
func TestLaneInboxStoreCallsTakeAContext(t *testing.T) {
	bare := map[string]bool{"Exec": true, "Query": true, "QueryRow": true, "Begin": true, "Ping": true, "Prepare": true}
	for _, file := range []string{"lane_inbox_store.go", "lane_inbox_store_postgres.go"} {
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && bare[sel.Sel.Name] {
					t.Errorf("%s: %s() without a context; use %sContext", file, sel.Sel.Name, sel.Sel.Name)
				}
			}
			return true
		})
	}
}

// abort (hub Close) ends a store call that waits on a locked table instead
// of leaving Close to wait out the op timeout. Postgres ends the wait at
// once. SQLite's lock wait is its busy handler, which an interrupt does not
// cut short; busy_timeout (5s) already bounded it before this store had a
// context, so that is the bound asserted there.
func TestLaneInboxStoreContractAbortEndsAStalledCall(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		// A second connection holds the write lock the next write needs.
		var unlock func()
		if s.dialect == laneInboxPostgres {
			conn, err := s.db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := conn.ExecContext(context.Background(), `BEGIN; LOCK TABLE lane_inbox_batches IN ACCESS EXCLUSIVE MODE`); err != nil {
				t.Fatal(err)
			}
			unlock = func() { _, _ = conn.ExecContext(context.Background(), `ROLLBACK`); _ = conn.Close() }
		} else {
			var path string
			if err := s.db.QueryRow(`SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&path); err != nil {
				t.Fatal(err)
			}
			other, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			conn, err := other.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := conn.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
				t.Fatal(err)
			}
			unlock = func() { _, _ = conn.ExecContext(context.Background(), `ROLLBACK`); _ = conn.Close(); _ = other.Close() }
		}
		defer unlock()
		result := make(chan error, 1)
		go func() { result <- s.markBatchSent("stalled") }()
		select {
		case err := <-result:
			t.Fatalf("write did not wait on the lock: %v", err)
		case <-time.After(300 * time.Millisecond):
		}
		started := time.Now()
		s.abort()
		select {
		case err := <-result:
			if err == nil {
				t.Fatal("aborted call reported success")
			}
			bound := 2 * time.Second
			if s.dialect == laneInboxSQLite {
				bound = 6 * time.Second
			}
			if waited := time.Since(started); waited > bound {
				t.Fatalf("abort took %v", waited)
			}
		case <-time.After(laneInboxOpTimeout):
			t.Fatal("abort did not end the stalled call")
		}
	})
}

// Must-ack contract: a must-ack interrupt is stored unread and never
// digested, stays unread through read, leaves only on ack, and wakes waiters.
func TestLaneInboxStoreContractMustAck(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		ctx := context.Background()
		now := time.UnixMilli(1_700_000_000_000).UTC()
		wake := s.waitChan()
		e := quietEvent("ready", "[worker] READY | pr 7")
		e.NotifyType = "ready"
		item, err := s.put("ready", "lane-a", "lane.event", "interrupt", e, now, 0, 10*time.Second)
		if err != nil || !item.Fresh || !item.MustAck || item.ReadAt != 0 || item.DigestAt != 0 {
			t.Fatalf("must-ack put = %+v, %v", item, err)
		}
		select {
		case <-wake:
		default:
			t.Fatal("a must-ack insert did not wake waiters")
		}
		plain := quietEvent("plain", "now")
		if p, err := s.put("plain", "lane-a", "lane.event", "interrupt", plain, now, 0, 10*time.Second); err != nil || p.MustAck || p.ReadAt == 0 || p.DigestAt == 0 {
			t.Fatalf("plain interrupt put = %+v, %v", p, err)
		}
		if first, err := s.firstUnread(ctx, "lane-a"); err != nil || first == nil || first.ID != item.ID {
			t.Fatalf("firstUnread = %+v, %v", first, err)
		}
		if read, err := s.read(item.ID, "lane-a", now.Add(time.Second)); err != nil || read.ReadAt == 0 || read.AckedAt != 0 {
			t.Fatalf("read = %+v, %v", read, err)
		}
		if counts, err := s.counts("lane-a"); err != nil || counts["lane-a"] != 1 {
			t.Fatalf("read retired a must-ack item: %v, %v", counts, err)
		}
		if unread, err := s.list("lane-a", true); err != nil || len(unread) != 1 || unread[0].ID != item.ID {
			t.Fatalf("unread list = %+v, %v", unread, err)
		}
		if err := s.makeBatches(now.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if items, err := s.list("lane-a", false); err != nil {
			t.Fatal(err)
		} else {
			for _, i := range items {
				if i.ID == item.ID && i.DigestBatch != "" {
					t.Fatalf("must-ack item was digested: %+v", i)
				}
			}
		}
		if n, err := s.ack(item.ID, "lane-a", false, now.Add(2*time.Second)); err != nil || n != 1 {
			t.Fatalf("ack = %d, %v", n, err)
		}
		if counts, err := s.counts("lane-a"); err != nil || counts["lane-a"] != 0 {
			t.Fatalf("counts after ack = %v, %v", counts, err)
		}
		if first, err := s.firstUnread(ctx, "lane-a"); err != nil || first != nil {
			t.Fatalf("firstUnread after ack = %+v, %v", first, err)
		}
	})
}

func TestHubServerOpensPostgresInbox(t *testing.T) {
	_, dsn := hubPostgresTestSchema(t)
	now := time.UnixMilli(1_700_000_000_000).UTC()
	fallback := filepath.Join(t.TempDir(), "unused.sqlite3")
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "op"}, InboxStorePath: fallback, InboxDatabaseURL: dsn, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if h.inbox == nil || h.inbox.store.dialect != laneInboxPostgres {
		t.Fatal("hub did not open the Postgres inbox")
	}
	if _, err := os.Stat(fallback); err == nil {
		t.Fatal("hub created the SQLite fallback while a database URL was set")
	}
}

// A database URL is a credential: a failed connect must not echo the
// password, user, or host, in the error or the hub log. Port 1 on loopback
// refuses at once; no database server is involved.
func TestHubDatabaseURLNeverEchoed(t *testing.T) {
	const dsn = "postgres://pwsecretuser:pwsecretpass@127.0.0.1:1/pwsecretdb?connect_timeout=2"
	var logs bytes.Buffer
	_, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "op"}, InboxDatabaseURL: dsn, Logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))})
	if err == nil {
		t.Fatal("hub started with an unreachable database")
	}
	for _, secret := range []string{"pwsecret", "127.0.0.1"} {
		if strings.Contains(err.Error(), secret) || strings.Contains(logs.String(), secret) {
			t.Fatalf("error %q or log %q echoes %q", err, logs.String(), secret)
		}
	}
	// The CLI path: the DSN comes from the environment, and the rejection is
	// printed with its wrapping.
	auth := filepath.Join(t.TempDir(), "auth.env")
	if err := os.WriteFile(auth, []byte("HUB_TOKEN_operator=op\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(hubDatabaseURLEnv, dsn)
	_, _, _, err = newHubServerForCLI([]string{"--hub-auth", auth}, slog.New(slog.NewTextHandler(&logs, nil)))
	if err == nil || !strings.Contains(err.Error(), "hub database connect failed") {
		t.Fatalf("hub CLI error = %v, want the database connect failure", err)
	}
	for _, secret := range []string{"pwsecret", "127.0.0.1:1"} {
		if strings.Contains(err.Error(), secret) || strings.Contains(logs.String(), secret) {
			t.Fatalf("CLI error %q or log %q echoes %q", err, logs.String(), secret)
		}
	}
	if _, err := openLaneInboxStorePostgres(context.Background(), "postgres://pwsecretuser:pwsecretpass@[::1"); err == nil || strings.Contains(err.Error(), "pwsecret") {
		t.Fatalf("parse error = %v", err)
	}
}

// A producer ACK waits for a durable commit: every pooled connection runs
// with synchronous_commit=on, even when the DSN asks for less.
func TestLaneInboxStorePostgresForcesSynchronousCommit(t *testing.T) {
	_, dsn := hubPostgresTestSchema(t)
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := u.Query()
	query.Set("synchronous_commit", "off")
	u.RawQuery = query.Encode()
	s, err := openLaneInboxStorePostgres(context.Background(), u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	// Hold every pool slot at once so each connection is checked.
	conns := make([]*sql.Conn, 4)
	for i := range conns {
		if conns[i], err = s.db.Conn(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer conns[i].Close()
		var setting string
		if err := conns[i].QueryRowContext(context.Background(), `SHOW synchronous_commit`).Scan(&setting); err != nil || setting != "on" {
			t.Fatalf("connection %d synchronous_commit = %q, %v", i, setting, err)
		}
	}
}

// A store error after connect is logged without the server's address on
// Postgres; SQLite errors keep their text.
func TestLaneInboxStoreLoggableErrors(t *testing.T) {
	leaky := &net.OpError{Op: "read", Net: "tcp", Addr: &net.TCPAddr{IP: net.IPv4(192, 0, 2, 7), Port: 5432}, Err: errors.New("i/o timeout")}
	pg := &laneInboxStore{dialect: laneInboxPostgres}
	if got := pg.loggable("startup recovery", leaky).Error(); strings.Contains(got, "192.0.2.7") || got != "hub database startup recovery failed (*net.OpError)" {
		t.Fatalf("postgres loggable = %q", got)
	}
	lite := &laneInboxStore{dialect: laneInboxSQLite}
	if got := lite.loggable("startup recovery", errors.New("fixture recovery failure")); got.Error() != "fixture recovery failure" {
		t.Fatalf("sqlite loggable = %q", got)
	}
}

// The hub's real keys join their parts with NUL, and a job event's parts come
// from the producer and need not be UTF-8. Both backends store them, keep
// them distinct, and dedupe a retry of the same key.
func TestLaneInboxStoreContractHubShapedKeys(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		now := time.UnixMilli(1_700_000_000_000).UTC()
		job := quietEvent("ev-1", "done")
		job.ReportPath = "reports/\xff\xfe.md"
		keys := []string{
			relayEventDedupeKey("lane.event", quietEvent("ev-1", "FYI")),
			relayEventDedupeKey("job.done", job),
			// The same parts split differently: must not collide.
			"lane.event\x00lane-a\x00ev-1\x00",
			"lane.event\x00lane-a\x00ev-\x001",
		}
		ids := map[int64]bool{}
		for _, key := range keys {
			if !strings.Contains(key, "\x00") {
				t.Fatalf("fixture key %q has no NUL", key)
			}
			first, err := s.put(key, "lane-a", "lane.event", "inbox", quietEvent("x", "FYI"), now, 0, 0)
			if err != nil || !first.Fresh {
				t.Fatalf("put %q = %+v, %v", key, first, err)
			}
			again, err := s.put(key, "lane-a", "lane.event", "inbox", quietEvent("x", "changed"), now, 0, 0)
			if err != nil || again.Fresh || again.ID != first.ID {
				t.Fatalf("retry of %q = %+v, %v", key, again, err)
			}
			ids[first.ID] = true
		}
		if len(ids) != len(keys) {
			t.Fatalf("distinct keys collided: %v", ids)
		}
	})
}

func TestLaneInboxStoreSpawnDurabilityContract(t *testing.T) {
	for _, backend := range laneInboxBackends() {
		t.Run(backend.name, func(t *testing.T) {
			open := backend.fresh(t)
			store := open()
			if err := store.ensureSpawnTasks(); err != nil {
				t.Fatal(err)
			}
			request := durableSpawnRequest()
			task, fresh, err := store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
			if err != nil || !fresh {
				t.Fatalf("claim %v %v", fresh, err)
			}
			second := open()
			if err = second.ensureSpawnTasks(); err != nil {
				t.Fatal(err)
			}
			replay, fresh, err := second.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
			if err != nil || fresh || replay.Request.RequestID != task.Request.RequestID {
				t.Fatalf("restart replay %v %v", fresh, err)
			}
			if _, err = store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 124, Stage: "placement", Reason: "timeout"}, time.Now()); err != nil {
				t.Fatal(err)
			}
			task, err = second.spawnTask(t.Context(), request.RequestID)
			if err != nil || task.Status != "retryable" {
				t.Fatalf("unbound timeout retained ownership: %+v %v", task, err)
			}
			task, err = second.resumeCleanedSpawn(t.Context(), task, time.Now())
			if err != nil || task.Attempt != 2 {
				t.Fatalf("same-key retry: %+v %v", task, err)
			}
			task, err = store.bindSpawn(t.Context(), request.RequestID, task.Attempt, hubSpawnRequest{RequestID: t14RequestID(984), Machine: "machine-a", Args: []string{"--job", "test-job"}, WaitSeconds: 300}, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			if _, err = store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Retryable: true}, time.Now()); err != nil {
				t.Fatal(err)
			}
			if _, err = second.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Pane: "pane-1"}, time.Now()); err != nil {
				t.Fatal(err)
			}
			var notifications int
			if err = second.db.QueryRow(`SELECT count(*) FROM spawn_notifications`).Scan(&notifications); err != nil || notifications != 3 {
				t.Fatalf("outbox %d %v", notifications, err)
			}
			if _, err = store.db.Exec(`UPDATE spawn_notifications SET delivered=1`); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// settled is what the unconfirmed replay consults: an item the receiver has
// (delivered, read, acked, superseded) answers true on either backend, and an
// item that is merely stored, an insert-time read, or a missing key answers
// false.
func TestLaneInboxStoreContractSettled(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		now := time.UnixMilli(1_700_000_000_000).UTC()
		must := quietEvent("must", "READY")
		must.NotifyType = "ready"
		item, err := s.put("must", "lane-a", "lane.event", "interrupt", must, now, 0, 0)
		if err != nil || !item.MustAck {
			t.Fatalf("must-ack put = %+v, %v", item, err)
		}
		plain := quietEvent("plain", "now")
		if _, err := s.put("plain", "lane-a", "lane.event", "interrupt", plain, now, 0, 0); err != nil {
			t.Fatal(err)
		}
		expect := func(key string, want bool) {
			t.Helper()
			if got, err := s.settled(key); err != nil || got != want {
				t.Fatalf("settled(%q) = %v, %v; want %v", key, got, err, want)
			}
		}
		expect("missing", false)
		expect("must", false)
		expect("plain", false) // read at insert is not the receiver's read
		if _, err := s.read(item.ID, "lane-a", now.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		expect("must", true)
		if err := s.recordDelivery("plain", "", "", now.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		expect("plain", true)
	})
}

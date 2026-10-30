package panewire

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
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
		if err := s.recoverMustAct(policy); err != nil {
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

func TestLaneInboxStorePostgresMigrationsAreRecorded(t *testing.T) {
	config, _ := hubPostgresTestSchema(t)
	for range 2 {
		s, err := openLaneInboxStorePostgresConfig(context.Background(), config.Copy())
		if err != nil {
			t.Fatal(err)
		}
		var versions int
		if err := s.db.QueryRow(`SELECT count(*) FROM panewire_schema_migrations`).Scan(&versions); err != nil || versions != 1 {
			t.Fatalf("recorded migrations = %d, %v", versions, err)
		}
		_ = s.close()
	}
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
// password, user, or host. Port 1 on loopback refuses at once; no database
// server is involved.
func TestHubDatabaseURLNeverEchoed(t *testing.T) {
	const dsn = "postgres://pwsecretuser:pwsecretpass@127.0.0.1:1/pwsecretdb?connect_timeout=2"
	_, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "op"}, InboxDatabaseURL: dsn})
	if err == nil {
		t.Fatal("hub started with an unreachable database")
	}
	for _, secret := range []string{"pwsecret", "127.0.0.1"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error %q echoes %q", err, secret)
		}
	}
	if _, err := openLaneInboxStorePostgres(context.Background(), "postgres://pwsecretuser:pwsecretpass@[::1"); err == nil || strings.Contains(err.Error(), "pwsecret") {
		t.Fatalf("parse error = %v", err)
	}
}

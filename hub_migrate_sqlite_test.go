package panewire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// hubMigrateSQLiteSource writes an inbox file holding interrupts, inbox
// items (one read), must-ack items (one acked, one not), a sent batch, a
// pending batch, and two quarantined batches. With pendingWAL the file is
// what a killed hub leaves: every row sits in an uncheckpointed -wal beside
// a database file that lacks them.
func hubMigrateSQLiteSource(t *testing.T, pendingWAL bool) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "hub-inbox.sqlite3")
	s, err := openLaneInboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if pendingWAL {
		if _, err := s.db.Exec(`PRAGMA wal_autocheckpoint=0`); err != nil {
			t.Fatal(err)
		}
	}
	now := time.UnixMilli(1_700_000_000_000).UTC()
	for _, key := range []string{"i1", "i2"} {
		if _, err := s.put(key, "lane-a", "lane.event", "interrupt", quietEvent(key, "now"), now, 0, time.Second); err != nil {
			t.Fatal(err)
		}
	}
	var inbox []laneInboxItem
	for _, key := range []string{"k1", "k2", "k3"} {
		item, err := s.put(key, "lane-b", "lane.event", "inbox", quietEvent(key, "FYI é"), now, time.Minute, 0)
		if err != nil {
			t.Fatal(err)
		}
		inbox = append(inbox, item)
	}
	if err := s.makeBatches(now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	batches, err := s.pendingBatches()
	if err != nil || len(batches) != 2 {
		t.Fatalf("source batches = %+v, %v", batches, err)
	}
	if err := s.markBatchSent(batches[0].ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.read(inbox[0].ID, "lane-b", now.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	for _, typ := range []string{"ready", "decision"} {
		e := quietEvent("m-"+typ, "[worker] "+strings.ToUpper(typ)+" | pr 7")
		e.NotifyType = typ
		item, err := s.put("m-"+typ, "lane-a", "lane.event", "interrupt", e, now, 0, 0)
		if err != nil || !item.MustAck {
			t.Fatalf("must-ack put = %+v, %v", item, err)
		}
		if typ == "decision" {
			if n, err := s.ack(item.ID, "lane-a", false, now.Add(3*time.Hour)); err != nil || n != 1 {
				t.Fatalf("ack = %d, %v", n, err)
			}
		}
	}
	// A key shaped like the hub's own: NUL separators and a producer part
	// that is not UTF-8.
	job := quietEvent("job-1", "done")
	job.ReportPath = "reports/\xff.md"
	if _, err := s.put(relayEventDedupeKey("job.done", job), "lane-a", "job.done", "interrupt", job, now, 0, 0); err != nil {
		t.Fatal(err)
	}
	// Byte order puts "Zlegacy" first, a locale collation "legacy": the copy
	// check must not depend on either backend's text ordering.
	if _, err := s.db.Exec(`INSERT INTO lane_inbox_batches(id,lane,text) VALUES('legacy','lane-b','two
lines'),('Zlegacy','lane-b','also
two')`); err != nil {
		t.Fatal(err)
	}
	if err := s.quarantineInvalidBatches(); err != nil {
		t.Fatal(err)
	}
	if pendingWAL {
		// Copy the files while the store is still open: the copy is the
		// state a killed hub leaves on disk.
		crashed := filepath.Join(t.TempDir(), "hub-inbox.sqlite3")
		for _, suffix := range []string{"", "-wal"} {
			raw, err := os.ReadFile(path + suffix)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(crashed+suffix, raw, 0600); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.close(); err != nil {
			t.Fatal(err)
		}
		return crashed
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func fileDigest(t *testing.T, path string) [32]byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return sha256.Sum256(raw)
}

// dirDigests is every file in path's directory with its digest.
func dirDigests(t *testing.T, path string) map[string][32]byte {
	t.Helper()
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	digests := map[string][32]byte{}
	for _, entry := range entries {
		digests[entry.Name()] = fileDigest(t, filepath.Join(filepath.Dir(path), entry.Name()))
	}
	return digests
}

const hubMigrateSQLiteSourceSummary = "source: lane_inbox=8 (unread=3 must_ack=2 unacked=1) lane_inbox_batches=4 (pending=1) lane_inbox_batch_quarantine=2"

func TestHubMigrateSQLiteDryRunReadsOnly(t *testing.T) {
	for _, pendingWAL := range []bool{false, true} {
		t.Run(map[bool]string{false: "checkpointed", true: "pending-wal"}[pendingWAL], func(t *testing.T) {
			path := hubMigrateSQLiteSource(t, pendingWAL)
			before := dirDigests(t, path)
			var stdout, stderr bytes.Buffer
			code := runHubMigrateSQLite(context.Background(), []string{"--from", path}, "", &stdout, &stderr)
			if code != ExitOK {
				t.Fatalf("code = %d, stderr = %s", code, stderr.String())
			}
			out := stdout.String()
			for _, want := range []string{hubMigrateSQLiteSourceSummary, "PANEWIRE_HUB_DATABASE_URL unset; not checked", "dry-run: nothing written"} {
				if !strings.Contains(out, want) {
					t.Fatalf("stdout %q lacks %q", out, want)
				}
			}
			// Not one byte of the source files changes, and no sidecar appears.
			if after := dirDigests(t, path); !reflect.DeepEqual(before, after) {
				t.Fatalf("reading the source changed its directory: %v -> %v", before, after)
			}
		})
	}
}

// The pending-WAL fixture really keeps its rows only in the -wal: the
// database file alone holds none of them. Without this the WAL test above
// could pass on a source whose WAL was already checkpointed.
func TestHubMigrateSQLitePendingWALFixtureHoldsRowsOnlyInTheWAL(t *testing.T) {
	path := hubMigrateSQLiteSource(t, true)
	if info, err := os.Stat(path + "-wal"); err != nil || info.Size() == 0 {
		t.Fatalf("fixture -wal = %v, %v", info, err)
	}
	alone := filepath.Join(t.TempDir(), "alone.sqlite3")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(alone, raw, 0600); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", alone)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var items int
	if err := db.QueryRow(`SELECT count(*) FROM lane_inbox`).Scan(&items); err == nil && items != 0 {
		t.Fatalf("database file without its -wal holds %d items; the fixture's WAL is not pending", items)
	}
}

func TestHubMigrateSQLiteSourcePathIsNotAURIFragment(t *testing.T) {
	src := hubMigrateSQLiteSource(t, false)
	dir := filepath.Join(t.TempDir(), "odd#dir")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "hub?mode=rwc&x=%41.sqlite3")
	if err := os.Rename(src, path); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := runHubMigrateSQLite(context.Background(), []string{"--from", path}, "", &stdout, &stderr); code != ExitOK || !strings.Contains(stdout.String(), hubMigrateSQLiteSourceSummary) {
		t.Fatalf("code = %d, stdout = %s, stderr = %s", code, stdout.String(), stderr.String())
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != filepath.Base(path) {
		t.Fatalf("reading the source created files: %v", entries)
	}
}

func TestHubMigrateSQLiteRefusals(t *testing.T) {
	path := hubMigrateSQLiteSource(t, false)
	// A node store is SQLite but has none of the inbox tables.
	other := NewMemoryStore(t)
	_ = other.Close()
	notInbox := other.Path()
	notSQLite := filepath.Join(t.TempDir(), "notes.txt")
	if err := os.WriteFile(notSQLite, bytes.Repeat([]byte("not a database\n"), 200), 0600); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		args []string
		dsn  string
		code int
	}{
		{"missing from", nil, "", ExitUsage},
		{"stray argument", []string{"--from", path, "extra"}, "", ExitUsage},
		{"missing file", []string{"--from", filepath.Join(t.TempDir(), "absent.sqlite3")}, "", ExitConditionInvalid},
		{"directory", []string{"--from", t.TempDir()}, "", ExitConditionInvalid},
		{"not sqlite", []string{"--from", notSQLite}, "", ExitConditionInvalid},
		{"not an inbox store", []string{"--from", notInbox}, "", ExitConditionInvalid},
		{"apply without database URL", []string{"--from", path, "--apply"}, "", ExitConditionInvalid},
		{"unparsable database URL", []string{"--from", path}, "postgres://pwsecretuser:pwsecretpass@[::1", ExitConditionInvalid},
		{"unreachable database", []string{"--from", path, "--apply"}, "postgres://pwsecretuser:pwsecretpass@127.0.0.1:1/pwsecretdb?connect_timeout=2", ExitInternal},
		{"unreachable database dry-run", []string{"--from", path}, "postgres://pwsecretuser:pwsecretpass@127.0.0.1:1/pwsecretdb?connect_timeout=2", ExitInternal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := fileDigest(t, notInbox)
			var stdout, stderr bytes.Buffer
			if code := runHubMigrateSQLite(context.Background(), tc.args, tc.dsn, &stdout, &stderr); code != tc.code {
				t.Fatalf("code = %d, want %d; stderr = %s", code, tc.code, stderr.String())
			}
			for _, secret := range []string{"pwsecret", "127.0.0.1"} {
				if strings.Contains(stdout.String()+stderr.String(), secret) {
					t.Fatalf("output echoes %q: %s%s", secret, stdout.String(), stderr.String())
				}
			}
			if fileDigest(t, notInbox) != before {
				t.Fatal("refusing a non-inbox store modified it")
			}
		})
	}
}

// hubInboxTargetSnapshot reads the Postgres target the way the copy check
// does, plus its schema state.
func hubInboxTargetSnapshot(t *testing.T, dsn string) (hubInboxSnapshot, []string) {
	t.Helper()
	s, err := openLaneInboxStorePostgres(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	snapshot, err := readHubInboxSnapshot(context.Background(), s.db, laneInboxPostgres)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot, hubPostgresSchemaState(t, s)
}

func TestHubMigrateSQLiteCopiesToPostgres(t *testing.T) {
	for _, pendingWAL := range []bool{false, true} {
		t.Run(map[bool]string{false: "checkpointed", true: "pending-wal"}[pendingWAL], func(t *testing.T) {
			testHubMigrateSQLiteCopiesToPostgres(t, pendingWAL)
		})
	}
}

func testHubMigrateSQLiteCopiesToPostgres(t *testing.T, pendingWAL bool) {
	_, dsn := hubPostgresTestSchema(t)
	path := hubMigrateSQLiteSource(t, pendingWAL)
	sourceFiles := dirDigests(t, path)
	ctx := context.Background()
	var stdout, stderr bytes.Buffer
	if code := runHubMigrateSQLite(ctx, []string{"--from", path}, dsn, &stdout, &stderr); code != ExitOK || !strings.Contains(stdout.String(), "target: schema not created yet") {
		t.Fatalf("dry-run code = %d, stdout = %s, stderr = %s", code, stdout.String(), stderr.String())
	}
	stdout.Reset()
	if code := runHubMigrateSQLite(ctx, []string{"--from", path, "--apply"}, dsn, &stdout, &stderr); code != ExitOK || !strings.Contains(stdout.String(), "applied: lane_inbox=8 (unread=3 must_ack=2 unacked=1)") {
		t.Fatalf("apply code = %d, stdout = %s, stderr = %s", code, stdout.String(), stderr.String())
	}
	if !reflect.DeepEqual(dirDigests(t, path), sourceFiles) {
		t.Fatal("apply changed the source files")
	}
	source, err := readHubInboxSQLiteSource(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	copied, schema := hubInboxTargetSnapshot(t, dsn)
	if !reflect.DeepEqual(copied, source) {
		t.Fatalf("target rows differ from the source:\ngot  %+v\nwant %+v", copied, source)
	}

	// Read and ack state behave the same on the target: the unacked READY is
	// the lane's unread must-ack item, the acked DECISION is not, the read
	// inbox item is not unread.
	dst, err := openLaneInboxStorePostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	counts, err := dst.counts("")
	if err != nil || !reflect.DeepEqual(counts, map[string]int{"lane-a": 1, "lane-b": 2}) {
		t.Fatalf("target counts = %v, %v", counts, err)
	}
	unread, err := dst.list("lane-a", true)
	if err != nil || len(unread) != 1 || unread[0].Event.EventID != "m-ready" || !unread[0].MustAck || unread[0].AckedAt != 0 {
		t.Fatalf("target unread lane-a = %+v, %v", unread, err)
	}
	all, err := dst.list("lane-a", false)
	if err != nil {
		t.Fatal(err)
	}
	if i := slices.IndexFunc(all, func(i laneInboxItem) bool { return i.Event.EventID == "m-decision" }); i < 0 || !all[i].MustAck || all[i].AckedAt == 0 {
		t.Fatalf("acked decision not copied with its ack: %+v", all)
	}
	pending, err := dst.pendingBatches()
	if err != nil || len(pending) != 1 || !strings.HasPrefix(pending[0].Text, "3 unread inbox items | lane lane-b") {
		t.Fatalf("pending batches = %+v, %v", pending, err)
	}
	_ = dst.close()

	// A rerun changes nothing: same rows, same schema and migration record.
	stdout.Reset()
	if code := runHubMigrateSQLite(ctx, []string{"--from", path, "--apply"}, dsn, &stdout, &stderr); code != ExitOK || !strings.Contains(stdout.String(), "unchanged: the target already holds this source") {
		t.Fatalf("rerun code = %d, stdout = %s, stderr = %s", code, stdout.String(), stderr.String())
	}
	again, schemaAgain := hubInboxTargetSnapshot(t, dsn)
	if !reflect.DeepEqual(again, copied) || !slices.Equal(schemaAgain, schema) {
		t.Fatal("rerun changed the target")
	}
	stdout.Reset()
	if code := runHubMigrateSQLite(ctx, []string{"--from", path}, dsn, &stdout, &stderr); code != ExitOK || !strings.Contains(stdout.String(), "already holds this source") {
		t.Fatalf("dry-run after apply: code = %d, stdout = %s", code, stdout.String())
	}

	// Once the hub has written on Postgres the target differs: a rerun is
	// refused and leaves it as it is.
	dst, err = openLaneInboxStorePostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	next, err := dst.put("new", "lane-b", "lane.event", "inbox", quietEvent("new", "FYI"), time.Now(), 0, 0)
	if err != nil || next.ID <= copied.items[len(copied.items)-1].item.ID {
		t.Fatalf("id after copy = %+v, %v; must follow the copied ids", next, err)
	}
	_ = dst.close()
	written, _ := hubInboxTargetSnapshot(t, dsn)
	stdout.Reset()
	stderr.Reset()
	if code := runHubMigrateSQLite(ctx, []string{"--from", path, "--apply"}, dsn, &stdout, &stderr); code != ExitConditionInvalid || !strings.Contains(stderr.String(), "differ from the source") {
		t.Fatalf("apply onto a used target: code = %d, stderr = %s", code, stderr.String())
	}
	if after, _ := hubInboxTargetSnapshot(t, dsn); !reflect.DeepEqual(after, written) {
		t.Fatal("refused apply changed the target")
	}
}

// A hub left running holds the inbox tables; apply fails with the lock
// timeout instead of waiting without end, and writes nothing.
func TestHubMigrateSQLiteApplyFailsFastOnAHeldLock(t *testing.T) {
	_, dsn := hubPostgresTestSchema(t)
	path := hubMigrateSQLiteSource(t, false)
	hub, err := openLaneInboxStorePostgres(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer hub.close()
	conn, err := hub.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(context.Background(), `BEGIN; LOCK TABLE lane_inbox IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	defer conn.ExecContext(context.Background(), `ROLLBACK`)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	started := time.Now()
	var stdout, stderr bytes.Buffer
	code := runHubMigrateSQLite(ctx, []string{"--from", path, "--apply"}, dsn, &stdout, &stderr)
	if code != ExitInternal || !strings.Contains(stderr.String(), "SQLSTATE 55P03") {
		t.Fatalf("code = %d, stdout = %s, stderr = %s", code, stdout.String(), stderr.String())
	}
	if waited := time.Since(started); waited > 20*time.Second {
		t.Fatalf("apply waited %v on the held lock", waited)
	}
}

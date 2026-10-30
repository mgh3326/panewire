package panewire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// hubMigrateSQLiteSource writes a closed inbox file holding items, a sent
// batch, a pending batch, and a quarantined batch.
func hubMigrateSQLiteSource(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "hub-inbox.sqlite3")
	s, err := openLaneInboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.UnixMilli(1_700_000_000_000).UTC()
	for _, key := range []string{"i1", "i2"} {
		if _, err := s.put(key, "lane-a", "lane.event", "interrupt", quietEvent(key, "now"), now, 0, time.Second); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range []string{"k1", "k2", "k3"} {
		if _, err := s.put(key, "lane-b", "lane.event", "inbox", quietEvent(key, "FYI é"), now, time.Minute, 0); err != nil {
			t.Fatal(err)
		}
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

const hubMigrateSQLiteSourceSummary = "source: lane_inbox=5 (unread=3) lane_inbox_batches=4 (pending=1) lane_inbox_batch_quarantine=2"

func TestHubMigrateSQLiteDryRunReadsOnly(t *testing.T) {
	path := hubMigrateSQLiteSource(t)
	before := fileDigest(t, path)
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
	if fileDigest(t, path) != before {
		t.Fatal("dry-run modified the source file")
	}
	// A WAL reader may create the -shm index; only -wal would hold writes.
	if info, err := os.Stat(path + "-wal"); err == nil && info.Size() != 0 {
		t.Fatal("dry-run wrote to the source WAL")
	}
}

func TestHubMigrateSQLiteRefusals(t *testing.T) {
	path := hubMigrateSQLiteSource(t)
	// A node store is SQLite but has none of the inbox tables.
	other := NewMemoryStore(t)
	_ = other.Close()
	notInbox := other.Path()
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
		{"not an inbox store", []string{"--from", notInbox}, "", ExitConditionInvalid},
		{"apply without database URL", []string{"--from", path, "--apply"}, "", ExitConditionInvalid},
		{"unparsable database URL", []string{"--from", path}, "postgres://pwsecretuser:pwsecretpass@[::1", ExitConditionInvalid},
		{"unreachable database", []string{"--from", path, "--apply"}, "postgres://pwsecretuser:pwsecretpass@127.0.0.1:1/pwsecretdb?connect_timeout=2", ExitInternal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := runHubMigrateSQLite(context.Background(), tc.args, tc.dsn, &stdout, &stderr); code != tc.code {
				t.Fatalf("code = %d, want %d; stderr = %s", code, tc.code, stderr.String())
			}
			for _, secret := range []string{"pwsecret", "127.0.0.1"} {
				if strings.Contains(stdout.String()+stderr.String(), secret) {
					t.Fatalf("output echoes %q: %s%s", secret, stdout.String(), stderr.String())
				}
			}
		})
	}
}

func TestHubMigrateSQLiteCopiesToPostgres(t *testing.T) {
	_, dsn := hubPostgresTestSchema(t)
	path := hubMigrateSQLiteSource(t)
	ctx := context.Background()
	var stdout, stderr bytes.Buffer
	if code := runHubMigrateSQLite(ctx, []string{"--from", path}, dsn, &stdout, &stderr); code != ExitOK || !strings.Contains(stdout.String(), "target: schema not created yet") {
		t.Fatalf("dry-run code = %d, stdout = %s, stderr = %s", code, stdout.String(), stderr.String())
	}
	stdout.Reset()
	if code := runHubMigrateSQLite(ctx, []string{"--from", path, "--apply"}, dsn, &stdout, &stderr); code != ExitOK || !strings.Contains(stdout.String(), "applied: lane_inbox=5") {
		t.Fatalf("apply code = %d, stdout = %s, stderr = %s", code, stdout.String(), stderr.String())
	}
	src, err := openLaneInboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer src.close()
	dst, err := openLaneInboxStorePostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer dst.close()
	want, err := src.list("", false)
	if err != nil {
		t.Fatal(err)
	}
	got, err := dst.list("", false)
	if err != nil || len(got) != len(want) {
		t.Fatalf("copied items = %d, %v; want %d", len(got), err, len(want))
	}
	for i := range want {
		if got[i].ID != want[i].ID || got[i].EscalationBatch != want[i].EscalationBatch || got[i].Event.Text != want[i].Event.Text {
			t.Fatalf("item %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	wantBatches, _ := src.pendingBatches()
	gotBatches, err := dst.pendingBatches()
	if err != nil || len(gotBatches) != 1 || gotBatches[0] != wantBatches[0] {
		t.Fatalf("pending batches = %+v, %v; want %+v", gotBatches, err, wantBatches)
	}
	next, err := dst.put("new", "lane-b", "lane.event", "inbox", quietEvent("new", "FYI"), time.Now(), 0, 0)
	if err != nil || next.ID <= want[0].ID {
		t.Fatalf("id after copy = %+v, %v; must follow the copied ids", next, err)
	}
	stdout.Reset()
	stderr.Reset()
	if code := runHubMigrateSQLite(ctx, []string{"--from", path, "--apply"}, dsn, &stdout, &stderr); code != ExitConditionInvalid || !strings.Contains(stderr.String(), "already holds inbox rows") {
		t.Fatalf("second apply code = %d, stderr = %s", code, stderr.String())
	}
}

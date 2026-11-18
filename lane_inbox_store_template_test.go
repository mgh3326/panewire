package panewire

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// laneInboxSlowOpen is the open time above which a test logs the open. The
// production open budget is laneInboxOpTimeout (10 s); a slow open below it is
// still worth seeing in a CI log (hk 5218).
const laneInboxSlowOpen = time.Second

// openLaneInboxStoreTimed opens a SQLite lane inbox for a test and logs the
// open when it takes longer than laneInboxSlowOpen.
func openLaneInboxStoreTimed(t *testing.T, path string) (*laneInboxStore, error) {
	t.Helper()
	start := time.Now()
	s, err := openLaneInboxStore(path)
	if d := time.Since(start); d > laneInboxSlowOpen {
		t.Logf("openLaneInboxStore took %v", d)
	}
	return s, err
}

// laneInboxTemplate is one migrated, empty SQLite inbox that every fresh test
// file is copied from. The migration chain runs once per test binary instead
// of once per store: a copy of a closed, migrated file reopens in milliseconds,
// because the chain then has nothing to write (hk 5218). The template is built
// by the production open, so a migration that blocks still fails the build
// under laneInboxOpTimeout.
var laneInboxTemplate struct {
	once sync.Once
	path string
	err  error
}

// freshLaneInboxFile returns a new path holding a copy of the migrated, empty
// template. Each test owns its copy under t.TempDir.
func freshLaneInboxFile(t *testing.T) string {
	t.Helper()
	laneInboxTemplate.once.Do(func() {
		laneInboxTemplate.path, laneInboxTemplate.err = buildLaneInboxTemplate()
	})
	if laneInboxTemplate.err != nil {
		t.Fatalf("lane inbox template: %v", laneInboxTemplate.err)
	}
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	if err := copyLaneInboxFile(laneInboxTemplate.path, path); err != nil {
		t.Fatal(err)
	}
	return path
}

// buildLaneInboxTemplate runs the full migration chain from an empty file and
// closes it. A closed WAL database has no -wal left to copy, so the file alone
// is the whole store; a leftover -wal would make a copy lose its writes.
func buildLaneInboxTemplate() (string, error) {
	dir, err := os.MkdirTemp(testStateDir, "lane-inbox-template-")
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, "template.sqlite3")
	s, err := openLaneInboxStore(path)
	if err != nil {
		return "", err
	}
	if err := s.close(); err != nil {
		return "", err
	}
	if _, err := os.Stat(path + "-wal"); err == nil {
		return "", errors.New("closed template still has a WAL file")
	}
	return path, nil
}

// The full chain from an empty file is the one place the migrations run in
// full for a fresh store. A template copy must carry the same schema, since
// the store-contract tests run on copies (hk 5218).
func TestLaneInboxTemplateCopyMatchesFullChainFromEmpty(t *testing.T) {
	full, err := openLaneInboxStore(filepath.Join(t.TempDir(), "full.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer full.close()
	copied, err := openLaneInboxStore(freshLaneInboxFile(t))
	if err != nil {
		t.Fatal(err)
	}
	defer copied.close()
	want, got := laneInboxSchema(t, full), laneInboxSchema(t, copied)
	if got != want {
		t.Fatalf("template copy schema differs from a full chain from empty:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

// laneInboxSchema lists every schema object of a store, in a stable order.
func laneInboxSchema(t *testing.T, s *laneInboxStore) string {
	t.Helper()
	rows, err := s.db.Query(`SELECT type || ' ' || name || ' ' || IFNULL(sql, '') FROM sqlite_master ORDER BY type, name`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var b strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		b.WriteString(line + "\n")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

// copyLaneInboxFile copies a closed database file into a new file, fsynced
// before the copy is returned.
func copyLaneInboxFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

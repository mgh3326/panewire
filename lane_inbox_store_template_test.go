package panewire

import (
	"database/sql"
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

// laneInboxTemplate holds the path of one migrated, empty SQLite inbox that
// each fresh test file is copied from. Only a successful build is kept: a
// failed one (for example a stalled open that hit laneInboxOpTimeout) is
// retried by the next caller, so one stall fails one test, as it did before
// the template existed (hk 5218).
var laneInboxTemplate struct {
	mu   sync.Mutex
	path string
}

// buildLaneInboxTemplateFn builds the template. A test swaps it to inject a
// failed build.
var buildLaneInboxTemplateFn = buildLaneInboxTemplate

// laneInboxTemplatePath returns the built template, building it if none exists.
func laneInboxTemplatePath() (string, error) {
	laneInboxTemplate.mu.Lock()
	defer laneInboxTemplate.mu.Unlock()
	if laneInboxTemplate.path == "" {
		path, err := buildLaneInboxTemplateFn()
		if err != nil {
			return "", err
		}
		laneInboxTemplate.path = path
	}
	return laneInboxTemplate.path, nil
}

// freshLaneInboxFile returns a new path holding a copy of the template. Each
// test owns its copy under t.TempDir.
//
// A copy is opened through openLaneInboxStore, which re-runs the whole chain.
// Correctness does not depend on the template: the chain is idempotent. The
// template being fully migrated is what makes that re-run write nothing, so
// the open stays cheap.
func freshLaneInboxFile(t *testing.T) string {
	t.Helper()
	template, err := laneInboxTemplatePath()
	if err != nil {
		t.Fatalf("lane inbox template: %v", err)
	}
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	if err := copyLaneInboxFile(template, path); err != nil {
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
	built := false
	defer func() {
		if !built {
			os.RemoveAll(dir)
		}
	}()
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
	built = true
	return path, nil
}

// The template must already hold the full schema, which is what keeps a copy's
// reopen write-free (hk 5218). It is read through a raw handle: openLaneInboxStore
// re-runs the chain and would repair a partial template before any comparison.
func TestLaneInboxTemplateIsFullyMigrated(t *testing.T) {
	template, err := laneInboxTemplatePath()
	if err != nil {
		t.Fatal(err)
	}
	full, err := openLaneInboxStore(filepath.Join(t.TempDir(), "full.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer full.close()
	raw, err := sql.Open("sqlite", template)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	want, got := laneInboxSchema(t, full.db), laneInboxSchema(t, raw)
	if got != want {
		t.Fatalf("template is not fully migrated:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

// A failed template build is not kept: the next caller builds again (hk 5218).
func TestLaneInboxTemplateRetriesAFailedBuild(t *testing.T) {
	build := buildLaneInboxTemplateFn
	t.Cleanup(func() { buildLaneInboxTemplateFn = build })
	laneInboxTemplate.mu.Lock()
	laneInboxTemplate.path = ""
	laneInboxTemplate.mu.Unlock()
	calls := 0
	buildLaneInboxTemplateFn = func() (string, error) {
		calls++
		if calls == 1 {
			return "", errors.New("stalled open")
		}
		return build()
	}
	if _, err := laneInboxTemplatePath(); err == nil {
		t.Fatal("first build did not fail")
	}
	path, err := laneInboxTemplatePath()
	if err != nil {
		t.Fatalf("second caller got the failed build's error: %v", err)
	}
	if calls != 2 || path == "" {
		t.Fatalf("calls=%d path=%q, want a second build that returns a path", calls, path)
	}
}

// laneInboxSchema lists every schema object of a database, in a stable order.
func laneInboxSchema(t *testing.T, db *sql.DB) string {
	t.Helper()
	rows, err := db.Query(`SELECT type || ' ' || name || ' ' || IFNULL(sql, '') FROM sqlite_master ORDER BY type, name`)
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

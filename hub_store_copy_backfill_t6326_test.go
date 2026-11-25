package panewire

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// hk 6326: the copier refuses a source that has not run the succeeded-notice
// backfill (hk 6283), so the target never holds unmarked succeeded notices.
// The SQLite check runs here; the copy tests need MySQL and Postgres servers and
// run in CI, like the other store-copy round trips.

func TestStoreCopySourceBackfilledSQLite_t6326(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source.sqlite")
	s := openSpawnStore(t, path)
	src, err := openStoreCopyDB(t.Context(), "sqlite://"+path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer src.db.Close()
	backfilled, err := storeCopySourceBackfilled(t.Context(), src)
	if err != nil || !backfilled {
		t.Fatalf("a store started on this build: backfilled=%v err=%v", backfilled, err)
	}
	// A store that never ran the backfill has user_version 0.
	forgetSpawnNoticeBackfill(t, s)
	backfilled, err = storeCopySourceBackfilled(t.Context(), src)
	if err != nil || backfilled {
		t.Fatalf("a store without the backfill: backfilled=%v err=%v", backfilled, err)
	}
}

// noticeRow is one spawn notice seeded straight into a source's table.
type noticeRow struct {
	eventID  string
	recorded int
}

// seedT6326NoticeRows writes notices as the hub writes them: succeeded outcomes
// and every other status, each with the telemetry mark its status gets.
func seedT6326NoticeRows(t *testing.T, db *sql.DB, rows []noticeRow) {
	t.Helper()
	for _, row := range rows {
		request := strings.Split(row.eventID, ":")[0]
		if _, err := db.Exec(`INSERT INTO spawn_notifications(event_id,request_id,job,owner,result,created_ms,delivered,telemetry_recorded) VALUES(?,?,?,?,?,?,?,?)`,
			row.eventID, request, "job-"+request, "parent", `{"rc":0}`, 1, 1, row.recorded); err != nil {
			t.Fatal(err)
		}
	}
}

// t6326NoticeRows are the rows the copy tests compare: a succeeded notice, which
// the hub writes recorded, and one notice of each non-succeeded status, which
// stays unrecorded.
func t6326NoticeRows(succeededRecorded int) []noticeRow {
	return []noticeRow{
		{"req-ok:1:succeeded", succeededRecorded},
		{"req-ok:1:succeeded:parent", succeededRecorded},
		{"req-failed:1:failed", 0},
		{"req-given:1:given_up", 0},
		{"req-refused:1:refused", 0},
		{"req-retry:1:retryable", 0},
	}
}

// t6326TargetMarks reads the event id and telemetry mark of each row on a target.
func t6326TargetMarks(t *testing.T, db *sql.DB) map[string]int {
	t.Helper()
	rows, err := db.Query(`SELECT event_id, telemetry_recorded FROM spawn_notifications`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	marks := make(map[string]int)
	for rows.Next() {
		var id string
		var recorded int
		if err := rows.Scan(&id, &recorded); err != nil {
			t.Fatal(err)
		}
		marks[id] = recorded
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return marks
}

// (a) A source that never ran the backfill is refused before the target is
// migrated: the copy names the step, and the target holds no spawn_notifications.
func TestHubStoreCopyRefusesUnbackfilledSource_t6326(t *testing.T) {
	sourceConfig, sourceURL := hubMySQLTestDatabase(t)
	source, err := openLaneInboxStoreMySQLConfig(t.Context(), sourceConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.close() })
	seedT6326NoticeRows(t, source.db, t6326NoticeRows(0))
	forgetSpawnNoticeBackfill(t, source)
	_, targetURL := hubPostgresTestSchema(t)
	var report bytes.Buffer
	err = copyHubStore(t.Context(), sourceURL, targetURL, "", false, false, &report)
	if !errors.Is(err, errStoreCopySourceNotBackfilled) {
		t.Fatalf("unbackfilled source: err=%v, report=%q", err, report.String())
	}
	target, err := openStoreCopyDB(t.Context(), targetURL, false)
	if err != nil {
		t.Fatal(err)
	}
	defer target.db.Close()
	var migrated bool
	if err := target.db.QueryRow(`SELECT to_regclass('spawn_notifications') IS NOT NULL`).Scan(&migrated); err != nil || migrated {
		t.Fatalf("refused copy touched the target: migrated=%v err=%v", migrated, err)
	}
}

// (b), (c) and (d): a copy from a backfilled source keeps every mark as stored,
// succeeded and otherwise; --verify-only after it reports matching checksums.
func TestHubStoreCopyKeepsBackfilledNoticeMarks_t6326(t *testing.T) {
	sourceConfig, sourceURL := hubMySQLTestDatabase(t)
	source, err := openLaneInboxStoreMySQLConfig(t.Context(), sourceConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.close() })
	recordSpawnBoundBackfill(t, source)
	rows := t6326NoticeRows(1)
	seedT6326NoticeRows(t, source.db, rows)
	_, targetURL := hubPostgresTestSchema(t)
	var report bytes.Buffer
	if err := copyHubStore(t.Context(), sourceURL, targetURL, "spawn_notifications", false, false, &report); err != nil {
		t.Fatalf("copy from a backfilled source: %v\n%s", err, report.String())
	}
	target, err := openStoreCopyDB(t.Context(), targetURL, false)
	if err != nil {
		t.Fatal(err)
	}
	defer target.db.Close()
	marks := t6326TargetMarks(t, target.db)
	if len(marks) != len(rows) {
		t.Fatalf("target holds %d notices, want %d", len(marks), len(rows))
	}
	for _, row := range rows {
		if marks[row.eventID] != row.recorded {
			t.Errorf("%s: target telemetry_recorded=%d, source %d", row.eventID, marks[row.eventID], row.recorded)
		}
	}
	// --verify-only compares each table's count and SHA-256, and a mismatch is an error.
	report.Reset()
	if err := copyHubStore(t.Context(), sourceURL, targetURL, "spawn_notifications", true, false, &report); err != nil {
		t.Fatalf("verify after copy: %v\n%s", err, report.String())
	}
	if !strings.Contains(report.String(), fmt.Sprintf("spawn_notifications source=%d ", len(rows))) {
		t.Fatalf("verify report lacks the notice count:\n%s", report.String())
	}
}

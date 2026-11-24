package panewire

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
)

// hk 6283: a succeeded outcome owes no failure event, so its spawn notice is
// written telemetry_recorded=1, and a one-time backfill marks the succeeded
// rows written before. Every other notice keeps the mark it had. The tests
// named LaneInboxStore run on SQLite and, in CI, on Postgres and MySQL.

const spawnNoticeBackfillLog = "hub backfilled succeeded spawn notifications as telemetry-recorded"

// spawnNoticeMark is the telemetry_recorded value of one notice, which must
// exist.
func spawnNoticeMark(t *testing.T, s *laneInboxStore, eventID string) int {
	t.Helper()
	var mark int
	if err := s.db.QueryRow(s.q(`SELECT telemetry_recorded FROM spawn_notifications WHERE event_id=?`), eventID).Scan(&mark); err != nil {
		t.Fatalf("notice %s: %v", eventID, err)
	}
	return mark
}

func wantSpawnNoticeMarks(t *testing.T, s *laneInboxStore, want int, eventIDs ...string) {
	t.Helper()
	for _, id := range eventIDs {
		if got := spawnNoticeMark(t, s, id); got != want {
			t.Errorf("notice %s: telemetry_recorded=%d, want %d", id, got, want)
		}
	}
}

// noticeClaim claims queue request n with submitter as its --requested-by lane
// ("" for none, so only the owner is told).
func noticeClaim(t *testing.T, s *laneInboxStore, n int, submitter string, now time.Time) hubSpawnTask {
	t.Helper()
	job := fmt.Sprintf("queue-job-%d", n)
	request := queueTestRequest(n, job)
	if submitter != "" {
		request.Args = append(request.Args, "--requested-by", submitter)
	}
	task, _, err := s.claimSpawn(t.Context(), request, job, "parent", now)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func noticeComplete(t *testing.T, s *laneInboxStore, task hubSpawnTask, outcome hubSpawnOutcome, now time.Time) {
	t.Helper()
	if changed, err := s.completeSpawn(t.Context(), task, outcome, now); err != nil || !changed {
		t.Fatalf("complete %s: changed=%v err=%v", task.Job, changed, err)
	}
}

// noticeUnresolved binds task, ends its wait with no answer and queues the
// unresolved notice. It returns the bound task.
func noticeUnresolved(t *testing.T, s *laneInboxStore, task hubSpawnTask, now time.Time) hubSpawnTask {
	t.Helper()
	bound, err := s.bindSpawn(t.Context(), task.Request.RequestID, task.Attempt, boundTestNode(task), now)
	if err != nil {
		t.Fatal(err)
	}
	bound = sentSpawnFixture(t, s, bound)
	noticeComplete(t, s, bound, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout"}, now)
	if queued, err := s.noteUnresolvedSpawn(t.Context(), bound, now); err != nil || !queued {
		t.Fatalf("unresolved notice: queued=%v err=%v", queued, err)
	}
	return bound
}

var noticeLanded = hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "pane-1", NodeResult: true}

// Every path that inserts a notice of a succeeded outcome writes it recorded.
// The two paths that write no outcome keep their own mark: the stuck marker
// owes its warning (0 until emitted), the ladder release owes nothing (1).
func TestLaneInboxStoreSpawnNoticeSucceededInsertedRecorded_t6283(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := queueTestStore(t, open)
		now := expiryNow()

		// The task store's outcome write: the owner's SPAWNED.
		landed := noticeClaim(t, s, 1, "", now)
		noticeComplete(t, s, landed, noticeLanded, now)
		wantSpawnNoticeMarks(t, s, 1, landed.Request.RequestID+":1:succeeded")

		// A landing after an unresolved notice is told to the owner and to the
		// submitter: both corrections are succeeded outcomes. The unresolved
		// rows are failures and stay 0.
		late := noticeUnresolved(t, s, noticeClaim(t, s, 2, "requester", now), now)
		noticeComplete(t, s, late, noticeLanded, now)
		id := late.Request.RequestID
		wantSpawnNoticeMarks(t, s, 1, id+":1:succeeded", id+":1:succeeded:requester")
		wantSpawnNoticeMarks(t, s, 0, id+":1:deadline_unresolved", id+":1:deadline_unresolved:requester")

		// The outbox's own insert, for a producer with no transaction.
		outbox := noticeClaim(t, s, 3, "", now)
		id = outbox.Request.RequestID
		for _, event := range []string{id + ":1:succeeded", id + ":1:placement_record_conflict"} {
			if err := s.saveSpawnNotice(t.Context(), event, outbox, `{"rc":0}`, now); err != nil {
				t.Fatal(err)
			}
		}
		wantSpawnNoticeMarks(t, s, 1, id+":1:succeeded")
		wantSpawnNoticeMarks(t, s, 0, id+":1:placement_record_conflict")

		// The orphan sweep's stuck marker.
		stuck := noticeClaim(t, s, 4, "", now)
		event, pending, err := s.pendingSpawnStuck(t.Context(), stuck, now)
		if err != nil || !pending {
			t.Fatalf("stuck marker: pending=%v err=%v", pending, err)
		}
		wantSpawnNoticeMarks(t, s, 0, event)
		s.recordSpawnStuck(t.Context(), event)
		wantSpawnNoticeMarks(t, s, 1, event)

		// The ladder release's record.
		tx, err := s.beginTx(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		release := t14RequestID(628300)
		if err := s.noteLadderRelease(t.Context(), tx, release, "job", "parent", []byte(`{"notice":"released"}`), now); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		wantSpawnNoticeMarks(t, s, 1, ladderReleaseEventPrefix+release)
	})
}

// Every notice of an outcome that is not succeeded is written 0, including the
// failed copy to a submitting lane that is itself named "succeeded" and so
// ends its event id in ":succeeded".
func TestLaneInboxStoreSpawnNoticeUnsucceededInsertedUnrecorded_t6283(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := queueTestStore(t, open)
		now := expiryNow()
		failure := hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "spawn_failed: boom", Host: "machine-a"}

		for n, submitter := range []string{"requester", "succeeded", "team:succeeded"} {
			task := noticeClaim(t, s, 1+n, submitter, now)
			noticeComplete(t, s, task, failure, now)
			id := task.Request.RequestID
			wantSpawnNoticeMarks(t, s, 0, id+":1:failed", id+":1:failed:"+submitter)
		}

		expired := queueTestWaiting(t, s, 4, now)
		if ok, err := s.giveUpWaitingSpawn(t.Context(), expired, now); err != nil || !ok {
			t.Fatalf("give up: %v %v", ok, err)
		}
		wantSpawnNoticeMarks(t, s, 0, expired.Request.RequestID+":1:given_up")

		for n, status := range []string{"refused", "cancelled"} {
			task := queueTestWaiting(t, s, 5+n, now)
			if ok, err := s.endWaitingSpawn(t.Context(), task, status, "task:42=missing", "", 78, now); err != nil || !ok {
				t.Fatalf("end %s: %v %v", status, ok, err)
			}
			wantSpawnNoticeMarks(t, s, 0, task.Request.RequestID+":1:"+status)
		}
	})
}

// The failure claim still works on a row inserted 0: one SPAWN_FAILED error
// event per failed outcome and per unresolved notice, however often the
// outcome is observed.
func TestSpawnNoticeFailureStillClaimedOnce_t6283(t *testing.T) {
	for _, test := range []struct {
		name string
		// run ends a request and observes its outcome twice. It returns the
		// claimed row.
		run func(t *testing.T, h *HubServer) string
	}{
		{"failed", func(t *testing.T, h *HubServer) string {
			s := h.inbox.store
			task := noticeClaim(t, s, 1, "", time.Now())
			failure := hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "spawn_failed", Host: "machine-a"}
			noticeComplete(t, s, task, failure, time.Now())
			event := task.Request.RequestID + ":1:failed"
			wantSpawnNoticeMarks(t, s, 0, event)
			h.recordSpawnTrace(task, failure)
			h.recordSpawnTrace(task, failure)
			return event
		}},
		{"unresolved", func(t *testing.T, h *HubServer) string {
			s := h.inbox.store
			task := noticeUnresolved(t, s, noticeClaim(t, s, 1, "", time.Now()), time.Now())
			event := task.Request.RequestID + ":1:deadline_unresolved"
			wantSpawnNoticeMarks(t, s, 0, event)
			h.recordUnresolvedSpawnError(task)
			h.recordUnresolvedSpawnError(task)
			return event
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := asyncSpawnHub(t)
			transport := &spawnSentryTransport{}
			client, err := sentry.NewClient(sentry.ClientOptions{Dsn: "https://public@example.invalid/1", Transport: transport})
			if err != nil {
				t.Fatal(err)
			}
			h.spawnSentry = client
			event := test.run(t, h)
			wantSpawnNoticeMarks(t, h.inbox.store, 1, event)
			transport.mu.Lock()
			defer transport.mu.Unlock()
			failures := 0
			for _, e := range transport.events {
				if e.Type != "transaction" && strings.HasPrefix(e.Message, "SPAWN_FAILED ") {
					failures++
				}
			}
			if failures != 1 {
				t.Fatalf("SPAWN_FAILED error events = %d, want exactly 1", failures)
			}
		})
	}
}

// legacySpawnNotice is a row as an earlier hub left it.
type legacySpawnNotice struct {
	event, request, owner string
	delivered, recorded   int
	operation             string
	marked                bool
}

// legacySpawnNotices is every shape of notice row, with marked set on exactly
// those the backfill must change: an unrecorded row whose own outcome is
// succeeded, whoever it was told to and whether or not it was delivered.
func legacySpawnNotices() []legacySpawnNotice {
	var rows []legacySpawnNotice
	add := func(n int, suffix, owner string, delivered, recorded int, operation string, marked bool) {
		request := t14RequestID(628300 + n)
		rows = append(rows, legacySpawnNotice{request + suffix, request, owner, delivered, recorded, operation, marked})
	}
	// Succeeded outcomes.
	add(1, ":1:succeeded", "parent", 1, 0, "succeeded", true)
	add(2, ":12:succeeded", "parent", 1, 0, "succeeded", true)
	add(3, ":1:succeeded", "parent", 1, 0, "succeeded", true)
	add(3, ":1:succeeded:req_lane", "req_lane", 1, 0, "succeeded", true)
	add(4, ":1:succeeded:team:lane", "team:lane", 1, 0, "succeeded", true)
	add(5, ":1:succeeded:succeeded", "succeeded", 1, 0, "succeeded", true)
	add(6, ":1:succeeded", "parent", 0, 0, "succeeded", true)
	add(7, ":1:succeeded", "parent", 2, 0, "succeeded", true)
	// Already recorded.
	add(8, ":1:succeeded", "parent", 1, 1, "succeeded", false)
	// Failed copies whose submitting lane makes the id look succeeded.
	add(9, ":1:failed", "parent", 1, 0, "failed", false)
	add(9, ":1:failed:succeeded", "succeeded", 1, 0, "failed", false)
	add(10, ":1:failed:x:succeeded", "x:succeeded", 1, 0, "failed", false)
	add(11, ":1:failed:succeeded:x", "succeeded:x", 1, 0, "failed", false)
	add(12, ":1:failed:a:1:succeeded", "a:1:succeeded", 1, 0, "failed", false)
	// Every other status.
	add(13, ":1:given_up", "parent", 1, 0, "given_up", false)
	add(14, ":1:retryable", "parent", 1, 0, "retryable", false)
	add(15, ":1:refused", "parent", 1, 0, "refused", false)
	add(16, ":1:cancelled", "parent", 1, 0, "cancelled", false)
	add(17, ":1:deadline_exceeded", "parent", 1, 0, "deadline_exceeded", false)
	// Rows of an operation that later succeeded, whose own outcome did not.
	add(18, ":1:deadline_unresolved", "parent", 1, 0, "succeeded", false)
	add(18, ":1:deadline_unresolved:succeeded", "succeeded", 1, 0, "succeeded", false)
	add(19, ":1:stuck", "parent", 1, 0, "succeeded", false)
	add(20, ":1:placement_record_conflict", "parent", 1, 0, "succeeded", false)
	add(21, ":1:failed", "parent", 1, 0, "succeeded", false)
	rows = append(rows, legacySpawnNotice{event: ladderReleaseEventPrefix + t14RequestID(628322), request: t14RequestID(628322), owner: "succeeded", delivered: 0, recorded: 0, operation: ""})
	return rows
}

func seedLegacySpawnNotices(t *testing.T, s *laneInboxStore, rows []legacySpawnNotice) {
	t.Helper()
	operations := map[string]bool{}
	for _, row := range rows {
		if row.operation != "" && !operations[row.request] {
			operations[row.request] = true
			retentionOp(t, s, row.request, row.operation, time.UnixMilli(1_700_000_000_000), 1)
		}
		if _, err := s.db.Exec(s.q(`INSERT INTO spawn_notifications(event_id,request_id,job,owner,result,created_ms,delivered,telemetry_recorded) VALUES(?,?,?,?,?,?,?,?)`), row.event, row.request, "job-"+row.request, row.owner, `{"rc":0}`, 1_700_000_000_000, row.delivered, row.recorded); err != nil {
			t.Fatal(err)
		}
	}
}

// spawnNoticeSnapshot is every column of every notice and of every operation.
func spawnNoticeSnapshot(t *testing.T, s *laneInboxStore) map[string]string {
	t.Helper()
	out := map[string]string{}
	rows, err := s.db.Query(`SELECT event_id,request_id,job,owner,result,created_ms,delivered,telemetry_recorded FROM spawn_notifications`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var event, request, job, owner, result string
		var created int64
		var delivered, recorded int
		if err := rows.Scan(&event, &request, &job, &owner, &result, &created, &delivered, &recorded); err != nil {
			t.Fatal(err)
		}
		out[event] = fmt.Sprint(request, job, owner, result, created, " delivered=", delivered, " recorded=", recorded)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	operations, err := s.db.Query(`SELECT request_id,job,owner,payload,status,result,terminal_ms FROM spawn_operations`)
	if err != nil {
		t.Fatal(err)
	}
	defer operations.Close()
	for operations.Next() {
		var request, job, owner, payload, status, result string
		var terminal int64
		if err := operations.Scan(&request, &job, &owner, &payload, &status, &result, &terminal); err != nil {
			t.Fatal(err)
		}
		out["operation "+request] = fmt.Sprint(job, owner, payload, status, result, terminal)
	}
	if err := operations.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// forgetSpawnNoticeBackfill makes the store look as it did before the backfill
// was applied, so the next start runs it.
func forgetSpawnNoticeBackfill(t *testing.T, s *laneInboxStore) {
	t.Helper()
	statement := `DELETE FROM panewire_schema_migrations WHERE version LIKE '%spawn_notifications_succeeded_recorded'`
	if s.dialect.ownsSchema() {
		statement = `PRAGMA user_version=0`
	}
	if _, err := s.db.Exec(statement); err != nil {
		t.Fatal(err)
	}
}

func spawnNoticeBackfillApplied(t *testing.T, s *laneInboxStore) bool {
	t.Helper()
	query := `SELECT count(*) FROM panewire_schema_migrations WHERE version LIKE '%spawn_notifications_succeeded_recorded'`
	if s.dialect.ownsSchema() {
		query = `PRAGMA user_version`
	}
	var n int
	if err := s.db.QueryRow(query).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n == 1
}

// The backfill changes telemetry_recorded on the unrecorded succeeded rows and
// nothing else, is recorded as applied, runs once, and run again finds nothing.
func TestLaneInboxStoreSpawnNoticeBackfillMarksOnlySucceeded_t6283(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		first := queueTestStore(t, open)
		rows := legacySpawnNotices()
		seedLegacySpawnNotices(t, first, rows)
		before := spawnNoticeSnapshot(t, first)
		forgetSpawnNoticeBackfill(t, first)
		if err := first.close(); err != nil {
			t.Fatal(err)
		}

		second := queueTestStore(t, open)
		want := maps.Clone(before)
		for _, row := range rows {
			if row.marked {
				want[row.event] = strings.Replace(want[row.event], "recorded=0", "recorded=1", 1)
			}
		}
		after := spawnNoticeSnapshot(t, second)
		for _, key := range slices.Sorted(maps.Keys(want)) {
			if after[key] != want[key] {
				t.Errorf("%s:\n got %s\nwant %s", key, after[key], want[key])
			}
		}
		if len(after) != len(want) {
			t.Errorf("rows = %d, want %d", len(after), len(want))
		}
		if !spawnNoticeBackfillApplied(t, second) {
			t.Fatal("the backfill is not recorded as applied")
		}

		// Run again: nothing is left to change.
		forgetSpawnNoticeBackfill(t, second)
		if err := second.close(); err != nil {
			t.Fatal(err)
		}
		third := queueTestStore(t, open)
		if again := spawnNoticeSnapshot(t, third); !maps.Equal(again, after) {
			t.Errorf("a second run changed rows:\n got %v\nwant %v", again, after)
		}

		// Applied: a later start does not run it, even with a row it would take.
		unrecorded := []legacySpawnNotice{{event: t14RequestID(628399) + ":1:succeeded", request: t14RequestID(628399), owner: "parent", delivered: 1}}
		seedLegacySpawnNotices(t, third, unrecorded)
		if err := third.close(); err != nil {
			t.Fatal(err)
		}
		fourth := queueTestStore(t, open)
		wantSpawnNoticeMarks(t, fourth, 0, unrecorded[0].event)
	})
}

// The hub start that applies the backfill logs one line with the rows it
// updated; a start that does not apply it logs nothing about it.
func TestLaneInboxStoreSpawnNoticeBackfillLogsCountOnce_t6283(t *testing.T) {
	forEachHubInboxStorage(t, func(t *testing.T, storage hubInboxStorage) {
		path := storage.path
		if path == "" {
			path = filepath.Join(t.TempDir(), "inbox.sqlite3")
		}
		// edit changes the stored rows between two hub starts.
		edit := func(change func(s *laneInboxStore)) {
			t.Helper()
			var s *laneInboxStore
			var err error
			if storage.dsn != "" {
				s, err = openLaneInboxStoreURL(context.Background(), storage.dsn)
			} else {
				s, err = openLaneInboxStoreTimed(t, storage.path)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			if err := s.ensureSpawnTasks(); err != nil {
				t.Fatal(err)
			}
			change(s)
		}
		start := func() []string {
			t.Helper()
			var logs bytes.Buffer
			h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken}, InboxStorePath: path, InboxDatabaseURL: storage.dsn, Logger: slog.New(slog.NewTextHandler(io.Writer(&logs), nil))})
			if err != nil {
				t.Fatal(err)
			}
			if err := h.Close(); err != nil {
				t.Fatal(err)
			}
			var lines []string
			for _, line := range strings.Split(logs.String(), "\n") {
				if strings.Contains(line, spawnNoticeBackfillLog) {
					lines = append(lines, line)
				}
			}
			return lines
		}
		edit(func(s *laneInboxStore) {
			seedLegacySpawnNotices(t, s, legacySpawnNotices())
			forgetSpawnNoticeBackfill(t, s)
		})
		marked := 0
		for _, row := range legacySpawnNotices() {
			if row.marked {
				marked++
			}
		}
		if lines := start(); len(lines) != 1 || !strings.HasSuffix(lines[0], fmt.Sprintf(" rows=%d", marked)) {
			t.Fatalf("applying start logged %q, want one line ending rows=%d", lines, marked)
		}
		if lines := start(); len(lines) != 0 {
			t.Fatalf("a later start logged %q", lines)
		}
		edit(func(s *laneInboxStore) { forgetSpawnNoticeBackfill(t, s) })
		if lines := start(); len(lines) != 1 || !strings.HasSuffix(lines[0], " rows=0") {
			t.Fatalf("a second run logged %q, want one line ending rows=0", lines)
		}
	})
}

// Nothing becomes deletable under today's retention rule because of the mark: a
// succeeded operation past the 30-day window keeps its reservation, and the
// reservation holds it, whether its notice was inserted recorded or
// backfilled. Without the reservation the same rows are past every other clause.
func TestLaneInboxStoreSpawnNoticeRetiresNothingWhileReserved_t6283(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		first := queueTestStore(t, open)
		now := expiryNow()
		old := now.Add(-31 * retentionDay)
		inserted := noticeClaim(t, first, 1, "", old)
		noticeComplete(t, first, inserted, noticeLanded, old)
		backfilled := noticeClaim(t, first, 2, "", old)
		noticeComplete(t, first, backfilled, noticeLanded, old)
		for _, statement := range []string{
			`UPDATE spawn_notifications SET delivered=1`,
			`UPDATE spawn_notifications SET telemetry_recorded=0 WHERE request_id='` + backfilled.Request.RequestID + `'`,
		} {
			if _, err := first.db.Exec(statement); err != nil {
				t.Fatal(err)
			}
		}
		forgetSpawnNoticeBackfill(t, first)
		if err := first.close(); err != nil {
			t.Fatal(err)
		}
		s := queueTestStore(t, open)
		if err := s.ensureConsults(); err != nil {
			t.Fatal(err)
		}
		ids := []string{inserted.Request.RequestID, backfilled.Request.RequestID}
		wantRows := func(operations, reservations int) {
			t.Helper()
			for _, id := range ids {
				for table, want := range map[string]int{"spawn_operations": operations, "spawn_reservations": reservations, "spawn_notifications": 1} {
					if got := retentionCount(t, s, table, "request_id", id); got != want {
						t.Errorf("%s of %s: rows=%d, want %d", table, id, got, want)
					}
				}
			}
		}
		counts, err := s.pruneHubStore(now, 100)
		if err != nil {
			t.Fatal(err)
		}
		if counts != (hubStoreRetentionCounts{}) {
			t.Errorf("a pass deleted rows: %+v", counts)
		}
		wantRows(1, 1)
		// The control: only the reservation held them.
		for _, id := range ids {
			if _, err := s.db.Exec(s.q(`DELETE FROM spawn_reservations WHERE request_id=?`), id); err != nil {
				t.Fatal(err)
			}
		}
		if counts, err = s.pruneHubStore(now, 100); err != nil || counts.SpawnOperations != 2 {
			t.Fatalf("without reservations the pass deleted %d operations (%v), want 2", counts.SpawnOperations, err)
		}
		wantRows(0, 0)
	})
}

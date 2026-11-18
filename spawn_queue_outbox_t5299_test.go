package panewire

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// Tests for spawn_queue and task_fail_notice_outbox (hk 5299). The store
// tests run on both dialects (Postgres in CI); the hub tests use SQLite.

// queueTestTerminal lists the terminal statuses on its own, not through the
// production constant, so a change to that list shows up here.
const queueTestTerminal = `'succeeded','failed','refused','given_up','cancelled'`

// queueRows reads spawn_queue: request id to next_check_ms.
func queueRows(t *testing.T, s *laneInboxStore) map[string]int64 {
	t.Helper()
	return idMillis(t, s, `SELECT request_id,next_check_ms FROM spawn_queue`)
}

// outboxRows reads task_fail_notice_outbox: request id to retry_ms.
func outboxRows(t *testing.T, s *laneInboxStore) map[string]int64 {
	t.Helper()
	return idMillis(t, s, `SELECT request_id,retry_ms FROM task_fail_notice_outbox`)
}

func idMillis(t *testing.T, s *laneInboxStore, query string) map[string]int64 {
	t.Helper()
	rows, err := s.db.Query(query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]int64{}
	for rows.Next() {
		var id string
		var ms int64
		if err := rows.Scan(&id, &ms); err != nil {
			t.Fatal(err)
		}
		got[id] = ms
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return got
}

// assertQueueMatchesOperations is the queue's invariant: one row per
// non-terminal operation, carrying that operation's next_check_ms, and
// nothing else.
func assertQueueMatchesOperations(t *testing.T, s *laneInboxStore) {
	t.Helper()
	want := idMillis(t, s, `SELECT request_id,next_check_ms FROM spawn_operations WHERE status NOT IN (`+queueTestTerminal+`)`)
	if got := queueRows(t, s); !mapsEqual(got, want) {
		t.Fatalf("spawn_queue = %v, want the non-terminal operations %v", got, want)
	}
}

func mapsEqual(a, b map[string]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}

func queueTestRequest(n int, job string) fleet.Submission {
	request := durableSpawnRequest()
	request.RequestID = t14RequestID(52990 + n)
	request.Args = replaceSpawnArg(replaceSpawnArg(replaceSpawnArg(request.Args, "--job", job), "-l", job), "--task", fmt.Sprint(5290+n))
	return request
}

func queueTestClaim(t *testing.T, s *laneInboxStore, n int, now time.Time) hubSpawnTask {
	t.Helper()
	task, _, err := s.claimSpawn(t.Context(), queueTestRequest(n, fmt.Sprintf("queue-job-%d", n)), fmt.Sprintf("queue-job-%d", n), "parent", now)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func queueTestStore(t *testing.T, open func() *laneInboxStore) *laneInboxStore {
	t.Helper()
	s := open()
	if err := s.ensureSpawnTasks(); err != nil {
		t.Fatal(err)
	}
	return s
}

// Every path that writes a terminal status removes the operation's queue row
// in the same transaction, and a failed or refused one writes its outbox row.
// Each case fails if its path leaves the row (hk 5299: the mutant of one
// terminal path that forgets the queue).
func TestSpawnQueueLeavesOnEveryTerminalTransition_t5299(t *testing.T) {
	failed := hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "spawn_failed: boom", Host: "machine-a"}
	notStarted := hubSpawnOutcome{RC: 124, Stage: "claim", Reason: "deadline_not_started", Retryable: true, NodeResult: true, Host: "machine-a"}
	for _, test := range []struct {
		name   string
		status string
		// run drives the transition and returns the request id that ended.
		run func(t *testing.T, s *laneInboxStore, now time.Time) string
	}{
		{"completeSpawn succeeded", "succeeded", func(t *testing.T, s *laneInboxStore, now time.Time) string {
			task := queueTestClaim(t, s, 1, now)
			if ok, err := s.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "pane-1", NodeResult: true}, now); err != nil || !ok {
				t.Fatalf("complete: %v %v", ok, err)
			}
			return task.Request.RequestID
		}},
		{"completeSpawn failed", "failed", func(t *testing.T, s *laneInboxStore, now time.Time) string {
			task := queueTestClaim(t, s, 2, now)
			if ok, err := s.completeSpawn(t.Context(), task, failed, now); err != nil || !ok {
				t.Fatalf("complete: %v %v", ok, err)
			}
			return task.Request.RequestID
		}},
		{"endUnstartedAttempt given_up", "given_up", func(t *testing.T, s *laneInboxStore, now time.Time) string {
			request := queueTestRequest(3, "queue-job-3")
			request.ExpiresSeconds = 3600
			task, _, err := s.claimSpawn(t.Context(), request, "queue-job-3", "parent", now)
			if err != nil {
				t.Fatal(err)
			}
			if task, err = s.bindSpawn(t.Context(), request.RequestID, task.Attempt, boundTestNode(task), now); err != nil {
				t.Fatal(err)
			}
			task = sentSpawnFixture(t, s, task)
			if ok, err := s.completeSpawn(t.Context(), task, notStarted, now.Add(2*time.Hour)); err != nil || !ok {
				t.Fatalf("complete: %v %v", ok, err)
			}
			return request.RequestID
		}},
		{"giveUpWaitingSpawn given_up", "given_up", func(t *testing.T, s *laneInboxStore, now time.Time) string {
			task := queueTestWaiting(t, s, 4, now)
			if ok, err := s.giveUpWaitingSpawn(t.Context(), task, now); err != nil || !ok {
				t.Fatalf("give up: %v %v", ok, err)
			}
			return task.Request.RequestID
		}},
		{"endWaitingSpawn refused", "refused", func(t *testing.T, s *laneInboxStore, now time.Time) string {
			task := queueTestWaiting(t, s, 5, now)
			if ok, err := s.endWaitingSpawn(t.Context(), task, "refused", "task:42=missing", "", 78, now); err != nil || !ok {
				t.Fatalf("end: %v %v", ok, err)
			}
			return task.Request.RequestID
		}},
		{"endWaitingSpawn cancelled", "cancelled", func(t *testing.T, s *laneInboxStore, now time.Time) string {
			task := queueTestWaiting(t, s, 6, now)
			if ok, err := s.endWaitingSpawn(t.Context(), task, "cancelled", "task:42=cancelled", "", 84, now); err != nil || !ok {
				t.Fatalf("end: %v %v", ok, err)
			}
			return task.Request.RequestID
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
				s := queueTestStore(t, open)
				now := expiryNow()
				// A bystander that stays queued proves the delete is the row's own.
				bystander := queueTestClaim(t, s, 9, now)
				id := test.run(t, s, now)
				var status string
				if err := s.db.QueryRow(s.q(`SELECT status FROM spawn_operations WHERE request_id=?`), id).Scan(&status); err != nil || status != test.status {
					t.Fatalf("status = %q, %v; want %q", status, err, test.status)
				}
				queue := queueRows(t, s)
				if _, left := queue[id]; left {
					t.Fatalf("%s left its spawn_queue row", test.name)
				}
				if _, kept := queue[bystander.Request.RequestID]; !kept {
					t.Fatalf("the bystander lost its queue row: %v", queue)
				}
				assertQueueMatchesOperations(t, s)
				_, owes := outboxRows(t, s)[id]
				if want := test.status == "failed" || test.status == "refused"; owes != want {
					t.Fatalf("outbox row for a %s operation = %v, want %v", test.status, owes, want)
				}
			})
		})
	}
}

func queueTestWaiting(t *testing.T, s *laneInboxStore, n int, now time.Time) hubSpawnTask {
	t.Helper()
	request := queueTestRequest(n, fmt.Sprintf("queue-job-%d", n))
	request.StartConditions = []fleet.StartCondition{{Kind: "task", TaskID: "42", State: "merged"}}
	task, _, err := s.claimSpawn(t.Context(), request, fmt.Sprintf("queue-job-%d", n), "parent", now)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != "waiting" {
		t.Fatalf("conditioned claim status = %q, want waiting", task.Status)
	}
	return task
}

// A staged operation is queued and its cancel or discard deletes the row with
// the operation.
func TestSpawnQueueFollowsStagedOperations_t5299(t *testing.T) {
	for _, discard := range []bool{false, true} {
		name := "cancel"
		if discard {
			name = "discard"
		}
		t.Run(name, func(t *testing.T) {
			forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
				s := queueTestStore(t, open)
				now := expiryNow()
				previous := queueTestClaim(t, s, 1, now)
				roleKey := "5291:implementer"
				staged := queueTestRequest(2, "queue-job-staged")
				staged.Args = replaceSpawnArg(staged.Args, "--task", "5291")
				if _, _, err := s.claimSpawnReplacing(t.Context(), staged, "queue-job-staged", "parent", now, previous.Request.RequestID, roleKey, "staged"); err != nil {
					t.Fatal(err)
				}
				if _, queued := queueRows(t, s)[staged.RequestID]; !queued {
					t.Fatal("a staged operation is not queued")
				}
				assertQueueMatchesOperations(t, s)
				var err error
				if discard {
					err = s.discardStagedSpawn(t.Context(), staged.RequestID, roleKey)
				} else {
					err = s.cancelStagedSpawn(t.Context(), staged.RequestID, previous.Request.RequestID, roleKey)
				}
				if err != nil {
					t.Fatal(err)
				}
				if _, left := queueRows(t, s)[staged.RequestID]; left {
					t.Fatalf("%s left the staged operation's queue row", name)
				}
				assertQueueMatchesOperations(t, s)
			})
		})
	}
}

// A queued operation is picked up by both coordinator reads, and its terminal
// transition ends that.
func TestPendingSpawnsReadTheQueue_t5299(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := queueTestStore(t, open)
		now := expiryNow()
		task := queueTestClaim(t, s, 1, now)
		ids := func() (page, ordinary []string) {
			tasks, _, err := s.pendingSpawnPage(t.Context(), spawnScanPosition{}, now)
			if err != nil {
				t.Fatal(err)
			}
			for _, task := range tasks {
				page = append(page, task.Request.RequestID)
			}
			tasks, err = s.pendingSpawns(t.Context(), now)
			if err != nil {
				t.Fatal(err)
			}
			for _, task := range tasks {
				ordinary = append(ordinary, task.Request.RequestID)
			}
			return page, ordinary
		}
		if page, ordinary := ids(); !slices.Equal(page, []string{task.Request.RequestID}) || !slices.Equal(ordinary, page) {
			t.Fatalf("queued operation read as page %v, ordinary %v", page, ordinary)
		}
		// The queue, not the status history, says what is to do: an operation
		// whose queue row is gone is not read, whatever its status says.
		if _, err := s.db.Exec(s.q(`DELETE FROM spawn_queue WHERE request_id=?`), task.Request.RequestID); err != nil {
			t.Fatal(err)
		}
		if page, ordinary := ids(); len(page) != 0 || len(ordinary) != 0 {
			t.Fatalf("an unqueued operation was read: page %v, ordinary %v", page, ordinary)
		}
		if _, err := s.db.Exec(s.q(`INSERT INTO spawn_queue(request_id,next_check_ms) VALUES(?,0)`), task.Request.RequestID); err != nil {
			t.Fatal(err)
		}
		if ok, err := s.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "pane-1", NodeResult: true}, now); err != nil || !ok {
			t.Fatalf("complete: %v %v", ok, err)
		}
		if page, ordinary := ids(); len(page) != 0 || len(ordinary) != 0 {
			t.Fatalf("a terminal operation was read: page %v, ordinary %v", page, ordinary)
		}
		assertQueueMatchesOperations(t, s)
	})
}

// A parked operation is read only once its next check passes, and every
// writer of next_check_ms carries it to the queue row.
func TestSpawnQueueNextCheckMirrorsTheOperation_t5299(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := queueTestStore(t, open)
		now := expiryNow()
		task := queueTestClaim(t, s, 1, now)
		id := task.Request.RequestID
		due := func(at time.Time) bool {
			tasks, _, err := s.pendingSpawnPage(t.Context(), spawnScanPosition{}, at)
			if err != nil {
				t.Fatal(err)
			}
			return len(tasks) == 1
		}
		next := now.Add(10 * time.Minute)
		if ok, err := s.parkSpawn(t.Context(), id, task.Attempt, spawnParkLaunchBackoff, next, now); err != nil || !ok {
			t.Fatalf("park: %v %v", ok, err)
		}
		if got := queueRows(t, s)[id]; got != next.UnixMilli() {
			t.Fatalf("queue next_check_ms after park = %d, want %d", got, next.UnixMilli())
		}
		assertQueueMatchesOperations(t, s)
		if due(now.Add(time.Minute)) || !due(next) {
			t.Fatal("a parked operation is read before its next check, or not read at it")
		}
		woken, err := s.wakeParkedSpawns(t.Context(), []string{spawnParkLaunchBackoff}, 10, now.Add(time.Minute))
		if err != nil || woken != 1 {
			t.Fatalf("wake: %d %v", woken, err)
		}
		if got := queueRows(t, s)[id]; got != now.Add(time.Minute).UnixMilli() {
			t.Fatalf("queue next_check_ms after wake = %d, want %d", got, now.Add(time.Minute).UnixMilli())
		}
		assertQueueMatchesOperations(t, s)
		if !due(now.Add(time.Minute)) {
			t.Fatal("a woken operation is not read")
		}
	})
}

// An attempt that ended unstarted before its limit goes back to pending with
// a back-off park: it stays queued and its queue row carries the park.
func TestSpawnQueueKeepsAnUnstartedRetry_t5299(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := queueTestStore(t, open)
		s.spawnRetryDelay = func(int) time.Duration { return 5 * time.Minute }
		now := expiryNow()
		request := queueTestRequest(1, "queue-job-1")
		request.ExpiresSeconds = 3600
		task, _, err := s.claimSpawn(t.Context(), request, "queue-job-1", "parent", now)
		if err != nil {
			t.Fatal(err)
		}
		if task, err = s.bindSpawn(t.Context(), request.RequestID, task.Attempt, boundTestNode(task), now); err != nil {
			t.Fatal(err)
		}
		task = sentSpawnFixture(t, s, task)
		notStarted := hubSpawnOutcome{RC: 124, Stage: "claim", Reason: "deadline_not_started", Retryable: true, NodeResult: true, Host: "machine-a"}
		if ok, err := s.completeSpawn(t.Context(), task, notStarted, now.Add(11*time.Minute)); err != nil || !ok {
			t.Fatalf("complete: %v %v", ok, err)
		}
		saved, err := s.spawnTask(t.Context(), request.RequestID)
		if err != nil || saved.Status != "accepted" || saved.Attempt != 2 || saved.NextCheck.IsZero() {
			t.Fatalf("after the unstarted end: %+v %v; want accepted attempt 2 with a park", saved, err)
		}
		if got := queueRows(t, s)[request.RequestID]; got != saved.NextCheck.UnixMilli() {
			t.Fatalf("queue next_check_ms = %d, want the operation's %d", got, saved.NextCheck.UnixMilli())
		}
		assertQueueMatchesOperations(t, s)
	})
}

// seedBackfillWorld leaves operations in every shape the backfills must sort:
// a queued and a waiting one (both non-terminal), a parked one, an unjudged
// failure, a judged failure, an unjudged refusal with a retry delay, and a
// success.
func seedBackfillWorld(t *testing.T, s *laneInboxStore, now time.Time) (queued []string, owed map[string]int64, judged string) {
	t.Helper()
	accepted := queueTestClaim(t, s, 1, now)
	waiting := queueTestWaiting(t, s, 2, now)
	parked := queueTestClaim(t, s, 3, now)
	if ok, err := s.parkSpawn(t.Context(), parked.Request.RequestID, parked.Attempt, spawnParkLaunchBackoff, now.Add(time.Hour), now); err != nil || !ok {
		t.Fatalf("park: %v %v", ok, err)
	}
	failed := queueTestClaim(t, s, 4, now)
	if ok, err := s.completeSpawn(t.Context(), failed, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "spawn_failed: boom", Host: "machine-a"}, now); err != nil || !ok {
		t.Fatalf("complete: %v %v", ok, err)
	}
	old := queueTestClaim(t, s, 5, now)
	if ok, err := s.completeSpawn(t.Context(), old, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "spawn_failed: old", Host: "machine-a"}, now); err != nil || !ok {
		t.Fatalf("complete: %v %v", ok, err)
	}
	// The failure migration 0023 stamped judged: its mark is set.
	if _, err := s.db.Exec(s.q(`UPDATE spawn_operations SET task_fail_notice_ms=? WHERE request_id=?`), now.UnixMilli(), old.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(s.q(`DELETE FROM task_fail_notice_outbox WHERE request_id=?`), old.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	refused := queueTestWaiting(t, s, 6, now)
	if ok, err := s.endWaitingSpawn(t.Context(), refused, "refused", "task:42=missing", "", 78, now); err != nil || !ok {
		t.Fatalf("end: %v %v", ok, err)
	}
	retry := now.Add(30 * time.Minute).UnixMilli()
	if _, err := s.db.Exec(s.q(`UPDATE spawn_operations SET task_fail_retry_ms=? WHERE request_id=?`), retry, refused.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(s.q(`UPDATE task_fail_notice_outbox SET retry_ms=? WHERE request_id=?`), retry, refused.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	won := queueTestClaim(t, s, 7, now)
	if ok, err := s.completeSpawn(t.Context(), won, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "pane-1", NodeResult: true}, now); err != nil || !ok {
		t.Fatalf("complete: %v %v", ok, err)
	}
	return []string{accepted.Request.RequestID, waiting.Request.RequestID, parked.Request.RequestID},
		map[string]int64{failed.Request.RequestID: 0, refused.Request.RequestID: retry}, old.Request.RequestID
}

func checkBackfilled(t *testing.T, s *laneInboxStore, queued []string, owed map[string]int64, judged string) {
	t.Helper()
	assertQueueMatchesOperations(t, s)
	queue := queueRows(t, s)
	if len(queue) != len(queued) {
		t.Fatalf("spawn_queue = %v, want exactly %v", queue, queued)
	}
	for _, id := range queued {
		if _, ok := queue[id]; !ok {
			t.Fatalf("spawn_queue lacks the non-terminal operation %s: %v", id, queue)
		}
	}
	outbox := outboxRows(t, s)
	if !mapsEqual(outbox, owed) {
		t.Fatalf("task_fail_notice_outbox = %v, want %v (never the judged %s)", outbox, owed, judged)
	}
}

// The SQLite store copies the operations that predate the tables, once, and
// does not put a judged failure back.
func TestSpawnQueueBackfillOnSQLite_t5299(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	s := openSpawnStore(t, path)
	now := expiryNow()
	queued, owed, judged := seedBackfillWorld(t, s, now)
	checkBackfilled(t, s, queued, owed, judged)
	// A store from before the tables: empty them and forget the guard.
	for _, statement := range []string{`DELETE FROM spawn_queue`, `DELETE FROM task_fail_notice_outbox`, `DELETE FROM spawn_queue_backfill`} {
		if _, err := s.db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.ensureSpawnTasks(); err != nil {
		t.Fatal(err)
	}
	checkBackfilled(t, s, queued, owed, judged)
	// Once per store: rows the hub since removed stay removed, and a second
	// start copies nothing.
	if _, err := s.db.Exec(`DELETE FROM spawn_queue`); err != nil {
		t.Fatal(err)
	}
	if err := s.ensureSpawnTasks(); err != nil {
		t.Fatal(err)
	}
	if got := queueRows(t, s); len(got) != 0 {
		t.Fatalf("a finished backfill ran again: %v", got)
	}
}

// The migration's own SQL does the same on either dialect: Postgres re-runs
// the recorded migration (CI); SQLite runs the embedded file's statements,
// which are portable, so the file is exercised in the local run too.
func TestSpawnQueueMigrationBackfills_t5299(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := queueTestStore(t, open)
		now := expiryNow()
		queued, owed, judged := seedBackfillWorld(t, s, now)
		checkBackfilled(t, s, queued, owed, judged)
		for _, statement := range []string{`DELETE FROM spawn_queue`, `DELETE FROM task_fail_notice_outbox`} {
			if _, err := s.db.Exec(statement); err != nil {
				t.Fatal(err)
			}
		}
		body, err := hubPostgresMigrations.ReadFile("hub_postgres_migrations/0030_spawn_queue_outbox.sql")
		if err != nil {
			t.Fatal(err)
		}
		if s.dialect == laneInboxPostgres {
			if _, err := s.db.Exec(`DELETE FROM panewire_schema_migrations WHERE version='0030_spawn_queue_outbox'`); err != nil {
				t.Fatal(err)
			}
			_ = s.close()
			s = open()
		} else if _, err := s.db.Exec(string(body)); err != nil {
			t.Fatal(err)
		}
		checkBackfilled(t, s, queued, owed, judged)
		// Running the migration again copies nothing twice.
		if s.dialect != laneInboxPostgres {
			if _, err := s.db.Exec(string(body)); err != nil {
				t.Fatal(err)
			}
			checkBackfilled(t, s, queued, owed, judged)
		}
	})
}

// ---- the task-fail outbox, through the hub ----

func taskFailOutboxHub(t *testing.T, path string) (*HubServer, *fakeHandoffkeep) {
	t.Helper()
	fake, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	h := taskFailHub(t, path, client, nil)
	h.spawnTaskFailBackoffBase = time.Minute
	return h, fake
}

func failTaskForOutbox(t *testing.T, h *HubServer, seq int) hubSpawnTask {
	t.Helper()
	task := taskFailClaim(t, h, seq)
	if _, err := h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "spawn_failed: boom", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	return task
}

// A failed operation sends one notice and leaves no outbox row; the operation
// keeps its mark as the record.
func TestTaskFailOutboxSendsOneNoticeAndEmpties_t5299(t *testing.T) {
	h, fake := taskFailOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	task := failTaskForOutbox(t, h, 1)
	store := h.inbox.store
	if got := outboxRows(t, store); len(got) != 1 {
		t.Fatalf("outbox after the failure = %v, want its one row", got)
	}
	h.flushSpawnTaskFailNotices(t.Context())
	h.flushSpawnTaskFailNotices(t.Context())
	if got := taskFailComments(t, fake); len(got) != 1 {
		t.Fatalf("comments = %d, want 1: %+v", len(got), got)
	}
	if got := outboxRows(t, store); len(got) != 0 {
		t.Fatalf("outbox after the notice = %v, want empty", got)
	}
	var mark int64
	if err := store.db.QueryRow(`SELECT task_fail_notice_ms FROM spawn_operations WHERE request_id=?`, task.Request.RequestID).Scan(&mark); err != nil || mark == 0 {
		t.Fatalf("the operation's notice mark = %d, %v; want it recorded", mark, err)
	}
}

// A retry keeps the outbox row with a later retry_ms, and the sweep leaves it
// alone until then.
func TestTaskFailOutboxRetryKeepsTheRowLater_t5299(t *testing.T) {
	h, fake := taskFailOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	clock := expiryClock(h, expiryNow())
	task := failTaskForOutbox(t, h, 1)
	fake.mu.Lock()
	fake.commentStatus = http.StatusServiceUnavailable
	fake.mu.Unlock()
	h.flushSpawnTaskFailNotices(t.Context())
	retry, ok := outboxRows(t, h.inbox.store)[task.Request.RequestID]
	if !ok || retry != clock.Add(time.Minute).UnixMilli() {
		t.Fatalf("outbox after a refused post = %v, want the row with retry_ms %d", outboxRows(t, h.inbox.store), clock.Add(time.Minute).UnixMilli())
	}
	fake.mu.Lock()
	fake.commentStatus = 0
	fake.mu.Unlock()
	// Not due: no hk call, the row is unchanged.
	callsBefore := len(taskFailComments(t, fake))
	h.flushSpawnTaskFailNotices(t.Context())
	if got := len(taskFailComments(t, fake)); got != callsBefore || outboxRows(t, h.inbox.store)[task.Request.RequestID] != retry {
		t.Fatalf("a not-due row was attempted or changed: comments %d, outbox %v", got, outboxRows(t, h.inbox.store))
	}
	*clock = clock.Add(2 * time.Minute)
	h.flushSpawnTaskFailNotices(t.Context())
	if got := taskFailComments(t, fake); len(got) != 1 {
		t.Fatalf("comments once due = %d, want 1", len(got))
	}
	if got := outboxRows(t, h.inbox.store); len(got) != 0 {
		t.Fatalf("outbox after the retry succeeded = %v, want empty", got)
	}
}

// The sweep reads the outbox, not the operations: a failed operation with no
// outbox row owes nothing to the sweep, and an outbox row is what it lists.
func TestTaskFailSweepReadsTheOutboxNotTheOperations_t5299(t *testing.T) {
	h, fake := taskFailOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	task := failTaskForOutbox(t, h, 1)
	if _, err := h.inbox.store.db.Exec(`DELETE FROM task_fail_notice_outbox`); err != nil {
		t.Fatal(err)
	}
	if taskFailUnjudged(t, h) != 1 {
		t.Fatal("the failed operation should still be unjudged on the operation")
	}
	h.flushSpawnTaskFailNotices(t.Context())
	if got := taskFailComments(t, fake); len(got) != 0 {
		t.Fatalf("the sweep scanned spawn_operations: %+v", got)
	}
	if _, err := h.inbox.store.db.Exec(`INSERT INTO task_fail_notice_outbox(request_id,retry_ms) VALUES(?,0)`, task.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	h.flushSpawnTaskFailNotices(t.Context())
	if got := taskFailComments(t, fake); len(got) != 1 {
		t.Fatalf("the sweep did not read its outbox row: %+v", got)
	}
}

// A restart resumes from both tables with no duplicate: the queue and the
// outbox rows survive, a second store start adds none, and the notice posts
// once across both hubs.
func TestSpawnQueueAndOutboxResumeAfterRestart_t5299(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	fake, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	first := taskFailHub(t, path, client, nil)
	failTaskForOutbox(t, first, 1)
	live := taskFailClaimTask(t, first, 2, "4299")
	queueBefore, outboxBefore := queueRows(t, first.inbox.store), outboxRows(t, first.inbox.store)
	if len(queueBefore) != 1 || len(outboxBefore) != 1 {
		t.Fatalf("before the restart: queue %v, outbox %v", queueBefore, outboxBefore)
	}
	first.Close()

	second := taskFailHub(t, path, client, nil)
	if got := queueRows(t, second.inbox.store); !mapsEqual(got, queueBefore) {
		t.Fatalf("queue after the restart = %v, want %v", got, queueBefore)
	}
	if got := outboxRows(t, second.inbox.store); !mapsEqual(got, outboxBefore) {
		t.Fatalf("outbox after the restart = %v, want %v", got, outboxBefore)
	}
	tasks, err := second.inbox.store.pendingSpawns(t.Context(), time.Now())
	if err != nil || len(tasks) != 1 || tasks[0].Request.RequestID != live.Request.RequestID {
		t.Fatalf("the restarted hub reads %v, %v; want the live operation", tasks, err)
	}
	second.flushSpawnTaskFailNotices(t.Context())
	second.Close()
	third := taskFailHub(t, path, client, nil)
	third.flushSpawnTaskFailNotices(t.Context())
	if got := taskFailComments(t, fake); len(got) != 1 {
		t.Fatalf("comments across restarts = %d, want 1: %+v", len(got), got)
	}
	if got := outboxRows(t, third.inbox.store); len(got) != 0 {
		t.Fatalf("outbox after the notice = %v, want empty", got)
	}
	if !strings.Contains(fmt.Sprint(queueRows(t, third.inbox.store)), live.Request.RequestID) {
		t.Fatalf("the live operation lost its queue row: %v", queueRows(t, third.inbox.store))
	}
}

// The startup judge of a hub with no hk client empties the outbox with the
// marks, so enabling hk later never floods old tasks.
func TestJudgedBacklogEmptiesTheOutbox_t5299(t *testing.T) {
	h, fake := taskFailOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	failTaskForOutbox(t, h, 1)
	n, err := h.inbox.store.judgeSpawnTaskFailBacklog(context.Background(), time.Now())
	if err != nil || n != 1 {
		t.Fatalf("judged = %d, %v; want 1", n, err)
	}
	if got := outboxRows(t, h.inbox.store); len(got) != 0 {
		t.Fatalf("outbox after judging the backlog = %v, want empty", got)
	}
	h.flushSpawnTaskFailNotices(t.Context())
	if got := taskFailComments(t, fake); len(got) != 0 {
		t.Fatalf("a judged failure posted: %+v", got)
	}
}

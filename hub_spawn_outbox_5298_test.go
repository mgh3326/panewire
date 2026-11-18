package panewire

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// hk 5298: the placement-record and spawn-notice outboxes. The recorder and
// the notice router read these small tables, delete a row in the transaction
// that marks its work done, and carry pending rows over at the upgrade.

func outboxKeys(t *testing.T, s *laneInboxStore, query string) []string {
	t.Helper()
	rows, err := s.db.Query(query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	slices.Sort(keys)
	return keys
}

func placementOutboxKeys(t *testing.T, s *laneInboxStore) []string {
	t.Helper()
	return outboxKeys(t, s, `SELECT request_id||'/'||attempt FROM placement_record_outbox`)
}

func noticeOutboxKeys(t *testing.T, s *laneInboxStore) []string {
	t.Helper()
	return outboxKeys(t, s, `SELECT event_id FROM spawn_notice_outbox`)
}

func wantKeys(t *testing.T, what string, got []string, want ...string) {
	t.Helper()
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
}

// A snapshot owing a record has an outbox row from the transaction that wrote
// it; the seal removes it; and a stale emission after the seal cannot owe the
// record again. Both dialects.
func TestSpawnOutboxPlacementRowFollowsSnapshot_t5298(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		if err := s.ensureSpawnTasks(); err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		if err := s.saveSpawnPlacementSnapshot(ctx, "req-a", 1, []byte(`{"v":1}`), ""); err != nil {
			t.Fatal(err)
		}
		wantKeys(t, "outbox after the snapshot write", placementOutboxKeys(t, s), "req-a/1")
		// A later emission for the same attempt replaces the payload and keeps one row.
		if err := s.saveSpawnPlacementSnapshot(ctx, "req-a", 1, []byte(`{"v":2}`), ""); err != nil {
			t.Fatal(err)
		}
		wantKeys(t, "outbox after the update", placementOutboxKeys(t, s), "req-a/1")
		if err := s.markSpawnPlacementRecorded(ctx, "req-a", 1); err != nil {
			t.Fatal(err)
		}
		wantKeys(t, "outbox after the seal", placementOutboxKeys(t, s))
		if err := s.saveSpawnPlacementSnapshot(ctx, "req-a", 1, []byte(`{"v":3}`), ""); err != nil {
			t.Fatal(err)
		}
		wantKeys(t, "outbox after a stale emission", placementOutboxKeys(t, s))
	})
}

// The upgrade copies every snapshot not yet recorded (0 and frozen 2) and every
// notification not yet delivered (0, held 2, publishing 3) into the outboxes
// and nothing else. SQLite runs it from ensureSpawnTasks, Postgres from
// migration 0028.
func TestSpawnOutboxUpgradeBackfillsPendingRows_t5298(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		if err := s.ensureSpawnTasks(); err != nil {
			t.Fatal(err)
		}
		// The store as the previous release left it: no outbox, history only.
		legacy := []string{`DROP TABLE placement_record_outbox`, `DROP TABLE spawn_notice_outbox`}
		if s.dialect == laneInboxPostgres {
			legacy = append(legacy, `DELETE FROM panewire_schema_migrations WHERE version='0028_spawn_outboxes'`)
		}
		for _, statement := range legacy {
			if _, err := s.db.Exec(statement); err != nil {
				t.Fatal(err)
			}
		}
		for _, row := range []struct {
			request  string
			attempt  int
			recorded int
		}{{"req-open", 1, 0}, {"req-sealed", 1, 1}, {"req-frozen", 2, 2}} {
			if _, err := s.db.Exec(s.q(`INSERT INTO spawn_placement_snapshots(request_id,attempt,payload,recorded,delivery) VALUES(?,?,?,?,?)`), row.request, row.attempt, `{}`, row.recorded, ``); err != nil {
				t.Fatal(err)
			}
		}
		for id, delivered := range map[string]int{"ev-pending": spawnNoticePending, "ev-done": spawnNoticeDone, "ev-held": spawnNoticeHeld, "ev-publishing": spawnNoticePublishing} {
			if _, err := s.db.Exec(s.q(`INSERT INTO spawn_notifications(event_id,request_id,job,owner,result,created_ms,delivered) VALUES(?,?,?,?,?,?,?)`), id, "req-"+id, "job", "owner", `{}`, 1, delivered); err != nil {
				t.Fatal(err)
			}
		}
		_ = s.close()
		upgraded := open()
		if err := upgraded.ensureSpawnTasks(); err != nil {
			t.Fatal(err)
		}
		wantKeys(t, "placement outbox", placementOutboxKeys(t, upgraded), "req-open/1", "req-frozen/2")
		wantKeys(t, "notice outbox", noticeOutboxKeys(t, upgraded), "ev-pending", "ev-held", "ev-publishing")
	})
}

// A hub binary from before the outboxes (a rollback) writes and seals history
// rows without touching the outboxes. The first start back on this release
// carries over what it left pending and drops what it finished, on both
// dialects (review of hk 5298).
func TestSpawnOutboxReconcilesRowsAnOlderBinaryWrote_t5298(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		if err := s.ensureSpawnTasks(); err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		// This release owes two records and one notice.
		for _, request := range []string{"req-sealed-later", "req-stays"} {
			if err := s.saveSpawnPlacementSnapshot(ctx, request, 1, []byte(`{}`), ""); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.db.Exec(s.q(`INSERT INTO spawn_notifications(event_id,request_id,job,owner,result,created_ms,delivered) VALUES(?,?,?,?,?,?,?)`), "ev-finished-later", "req-n", "job", "owner", `{}`, 1, spawnNoticePending); err != nil {
			t.Fatal(err)
		}
		if err := s.enqueueSpawnNotice(ctx, s.db, "ev-finished-later"); err != nil {
			t.Fatal(err)
		}
		// The older binary, while delivery fails: a new pending snapshot and
		// notification (no outbox row), and the sealing of two owed ones.
		for _, statement := range []string{
			`UPDATE spawn_placement_snapshots SET recorded=1 WHERE request_id='req-sealed-later'`,
			`UPDATE spawn_notifications SET delivered=1 WHERE event_id='ev-finished-later'`,
		} {
			if _, err := s.db.Exec(statement); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.db.Exec(s.q(`INSERT INTO spawn_placement_snapshots(request_id,attempt,payload,recorded,delivery) VALUES(?,?,?,?,?)`), "req-old-binary", 1, `{}`, 0, ``); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec(s.q(`INSERT INTO spawn_notifications(event_id,request_id,job,owner,result,created_ms,delivered) VALUES(?,?,?,?,?,?,?)`), "ev-old-binary", "req-o", "job", "owner", `{}`, 2, spawnNoticePending); err != nil {
			t.Fatal(err)
		}
		_ = s.close()
		back := open()
		if err := back.ensureSpawnTasks(); err != nil {
			t.Fatal(err)
		}
		wantKeys(t, "placement outbox after the rollback", placementOutboxKeys(t, back), "req-stays/1", "req-old-binary/1")
		wantKeys(t, "notice outbox after the rollback", noticeOutboxKeys(t, back), "ev-old-binary")
	})
}

// The probe reports what the outbox holds. A snapshot row the outbox does not
// name is not read, so an idle hub never scans the snapshot history; a row the
// outbox names wakes the recorder.
func TestSpawnOutboxProbeReadsTheOutboxNotTheSnapshots_t5298(t *testing.T) {
	h := placementOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	store := h.inbox.store
	if _, err := store.db.Exec(`INSERT INTO spawn_placement_snapshots(request_id,attempt,payload,recorded,delivery) VALUES('req-history',1,'{}',0,'')`); err != nil {
		t.Fatal(err)
	}
	if h.pendingPlacementRecords(t.Context()) {
		t.Fatal("probe read an unsealed snapshot the outbox does not name")
	}
	if _, err := store.db.Exec(`INSERT INTO placement_record_outbox(request_id,attempt) VALUES('req-history',1)`); err != nil {
		t.Fatal(err)
	}
	if !h.pendingPlacementRecords(t.Context()) {
		t.Fatal("probe missed an outbox row")
	}
}

// A record not yet delivered keeps its outbox row; the delivery that lands
// removes it with the seal and posts one record. A restart in between loses
// nothing and duplicates nothing.
func TestSpawnOutboxPlacementRecordFinishesAfterRestart_t5298(t *testing.T) {
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusInternalServerError) }))
	defer failing.Close()
	hk := placementOutboxServer(t)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	req := durableSpawnRequest()
	req.RequestID = "d676220b-e763-4af1-aefd-000000000051"
	h1 := placementOutboxHub(t, path)
	task := boundSpawnTask(t, h1, req)
	h1.notePlacementTrace(task, placementTraceSnapshot(), "")
	wantKeys(t, "outbox at bind", placementOutboxKeys(t, h1.inbox.store), req.RequestID+"/1")
	// The terminal result commits, and hk is down when the recorder runs.
	t.Setenv("HANDOFFKEEP_URL", failing.URL)
	if _, err := h1.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "pane-7", NodeResult: true}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	h1.flushPlacementRecords(t.Context())
	wantKeys(t, "outbox while hk refuses the record", placementOutboxKeys(t, h1.inbox.store), req.RequestID+"/1")
	if n := len(hk.records()); n != 0 {
		t.Fatalf("record posted to the wrong hk: %d", n)
	}
	h1.Close()
	// The restarted hub finishes it once hk answers.
	t.Setenv("HANDOFFKEEP_URL", hk.server.URL)
	h2 := placementOutboxHub(t, path)
	wantKeys(t, "outbox after the restart", placementOutboxKeys(t, h2.inbox.store), req.RequestID+"/1")
	h2.flushPlacementRecords(t.Context())
	h2.flushPlacementRecords(t.Context())
	if n := len(hk.records()); n != 1 {
		t.Fatalf("records after the restart = %d, want exactly one", n)
	}
	wantKeys(t, "outbox after delivery", placementOutboxKeys(t, h2.inbox.store))
	if h2.pendingPlacementRecords(t.Context()) {
		t.Fatal("probe still sees work after the record was delivered")
	}
}

// A notice leaves the outbox in the transaction that marks it delivered: a
// failed relay keeps it, a lost mark (crash between the send and the commit)
// sends it again under the same event id without a second event, and the
// router pages the outbox rather than the notification history.
func TestSpawnOutboxNoticeLifecycle_t5298(t *testing.T) {
	h := asyncSpawnHub(t)
	fake, client, stop := newFakeHandoffkeep(t)
	defer stop()
	h.handoffkeep = client
	req := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), req, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "pane-1"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	ids := noticeOutboxKeys(t, h.inbox.store)
	if len(ids) != 1 || !strings.HasSuffix(ids[0], ":succeeded") {
		t.Fatalf("outbox after the notice was queued = %v", ids)
	}
	rows := func() int {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		return len(fake.rows)
	}
	setStatus := func(status int) {
		fake.mu.Lock()
		fake.status = status
		fake.mu.Unlock()
	}
	setStatus(http.StatusInternalServerError)
	h.flushSpawnNotifications(t.Context())
	wantKeys(t, "outbox while the relay fails", noticeOutboxKeys(t, h.inbox.store), ids...)
	setStatus(http.StatusCreated)
	h.flushSpawnNotifications(t.Context())
	wantKeys(t, "outbox after delivery", noticeOutboxKeys(t, h.inbox.store))
	var delivered int
	if err := h.inbox.store.db.QueryRow(`SELECT delivered FROM spawn_notifications WHERE event_id=?`, ids[0]).Scan(&delivered); err != nil || delivered != spawnNoticeDone {
		t.Fatalf("delivered = %d, %v", delivered, err)
	}
	if rows() != 1 {
		t.Fatalf("events after delivery = %d", rows())
	}
	// The crash: the event was sent and the mark never committed.
	if _, err := h.inbox.store.db.Exec(`UPDATE spawn_notifications SET delivered=0`); err != nil {
		t.Fatal(err)
	}
	if _, err := h.inbox.store.db.Exec(`INSERT INTO spawn_notice_outbox(event_id) VALUES(?)`, ids[0]); err != nil {
		t.Fatal(err)
	}
	h.flushSpawnNotifications(t.Context())
	if rows() != 1 {
		t.Fatalf("redo created a second event: %d", rows())
	}
	wantKeys(t, "outbox after the redo", noticeOutboxKeys(t, h.inbox.store))
	// A history row the outbox does not name is not paged.
	if _, err := h.inbox.store.db.Exec(`UPDATE spawn_notifications SET delivered=0`); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	clear(fake.rows)
	fake.mu.Unlock()
	h.flushSpawnNotifications(t.Context())
	if rows() != 0 {
		t.Fatalf("router paged the notification history: %d events", rows())
	}
}

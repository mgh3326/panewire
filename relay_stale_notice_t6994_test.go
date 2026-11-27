package panewire

// hk 6994: the stale-replay notice follow-ups that are not lane bindings. The
// cases use the rig of relay_stale_notice_t6924_test.go.

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

// hk 6994 #1: a row retired after a filing pass read its rows is owed to the next
// pass. That pass files under its own event id, so the inbox's event-id dedupe
// does not hide the late row. Killed by: a notice event id that names only the
// day (stale-replay-<day>): the second pass's item is then deduplicated away.
func TestT6994LateRowIsFiledUnderItsOwnEventID(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	rig.taskOf[t6924Job(1)] = "1001"
	rig.tasks.set(t6924HKTask{ID: 1001, State: "started", UpdatedAt: rig.before(26)})
	rig.retire(1, rig.before(26), "[consult-done] read by the first pass")
	// The first pass reads its rows, then a row for the same day is retired
	// before it files: the row is inserted at the pass's first task read.
	rig.tasks.once(func() {
		err := rig.hub.inbox.store.insertStaleReplayRow(context.Background(), staleReplayRow{
			RelayID: 900, NoticeDay: staleReplayDay(t6924Day), DesignLane: t6924Design, WorkerLane: t6924Lane(2), Job: t6924Job(2),
			Kind: "lane.event", Sender: "consult-done", HKEventID: "t6994-late", ReceivedMS: rig.before(26).UnixMilli(), RetiredMS: t6924Day.UnixMilli(),
		})
		if err != nil {
			t.Error(err)
		}
	})
	rig.fileNextDay()
	rig.fileNextDay()
	items := rig.designItems()
	if len(items) != 2 {
		t.Fatalf("design lane items=%d, want the first pass's notice and the late row's: %v", len(items), items)
	}
	if strings.Contains(items[0], "t6994-late") || !strings.Contains(items[1], "hk t6994-late") {
		t.Fatalf("the late row is not in its own notice: %v", items)
	}
}

// hk 6994 #3: a row whose job lookup keeps failing is held for a later replay
// while its tries last, and then kept on the hub notice lane and closed, so a
// persistent store fault stops holding it at every replay. Killed by: holding the
// row for every replay (no bound on the lookup retries).
func TestT6994PersistentLookupFailureIsBounded(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	// The binding table is gone, so every job lookup of a lane.event row fails.
	if _, err := rig.hub.inbox.store.db.Exec(`DROP TABLE relay_stale_notice_lanes`); err != nil {
		t.Fatal(err)
	}
	rig.retire(1, rig.before(26), "[consult-done] the lookup fails")
	if got := rig.retiredCount(); got != 0 {
		t.Fatalf("a row whose lookup failed once was closed: %d closed", got)
	}
	for i := 0; i < 5 && rig.retiredCount() == 0; i++ {
		rig.replay()
	}
	if got := rig.retiredCount(); got != 1 {
		t.Fatalf("row closed=%d after repeated replays, want 1: a persistent lookup failure holds it forever", got)
	}
	rig.fileNextDay()
	items := rig.itemsOf(defaultLoopsNoticeLane)
	if len(items) != 1 || !strings.Contains(items[0], "hk t6924-1") {
		t.Fatalf("row kept after its tries ran out must be listed on the notice lane %s: items=%v", defaultLoopsNoticeLane, items)
	}
}

// hk 6994 #3: a store whose bindings carry the first key, (worker_lane, job), is
// rebuilt on open to the key (worker_lane, first_ms), and its rows are kept. Killed
// by: leaving the first key in place on open.
func TestT6994BindingKeyRebuildKeepsRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	rig := newT6924Rig(t, path)
	client := rig.hub.handoffkeep
	// Put back the first key, with one binding made under it.
	for _, stmt := range []string{
		`DROP TABLE relay_stale_notice_lanes`,
		`CREATE TABLE relay_stale_notice_lanes (worker_lane VARCHAR(255) NOT NULL, job VARCHAR(255) NOT NULL, first_ms BIGINT NOT NULL, PRIMARY KEY (worker_lane, job))`,
		`INSERT INTO relay_stale_notice_lanes(worker_lane,job,first_ms) VALUES('stale-6924-w1','job-6924-w1',1000)`,
	} {
		if _, err := rig.hub.inbox.store.db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	if err := rig.hub.Close(); err != nil {
		t.Fatal(err)
	}
	rig.open(client)
	var jobKeyed int
	if err := rig.hub.inbox.store.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('relay_stale_notice_lanes') WHERE name='job' AND pk>0`).Scan(&jobKeyed); err != nil {
		t.Fatal(err)
	}
	if jobKeyed != 0 {
		t.Fatal("the bindings table is still keyed (worker_lane, job) after open")
	}
	var kept int
	if err := rig.hub.inbox.store.db.QueryRow(`SELECT COUNT(*) FROM relay_stale_notice_lanes WHERE worker_lane='stale-6924-w1' AND job='job-6924-w1' AND first_ms=1000`).Scan(&kept); err != nil {
		t.Fatal(err)
	}
	if kept != 1 {
		t.Fatalf("the binding made under the first key was not carried over: kept=%d", kept)
	}
}

package panewire

// hk 7098: a retired row with no lane binding, whose worker lane name is the job
// id of a spawn created before the row arrived, resolves through that spawn to its
// design owner and hk task (relay_stale_notice.go:staleReplayOwnerOf). The cases
// reuse the hk 6924 rig: receipts and the replay are the real paths, and hk and the
// spawn-store read are doubles.

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// t7098Spawns answers the spawn-store read from spawns, keyed by job id.
func t7098Spawns(rig *t6924Rig, spawns map[string]staleReplaySpawn) {
	rig.hub.staleReplaySpawn = func(_ context.Context, job string) (staleReplaySpawn, bool) {
		spawn, ok := spawns[job]
		return spawn, ok
	}
}

// A row with no binding and no route, on a lane named like a spawned job, is
// resolved to that spawn's owner and task: the row the task did not overtake is
// listed on the design lane, named as resolved by the lane name, and the row the
// task overtook is counted. Killed by: dropping the lane-name fallback (the rows
// fall to the notice lane), or by ignoring the task (no overtake count).
func TestStaleNoticeLaneFallbackResolvesToLaneNamedSpawn(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	lane := t6924Lane(1)
	t7098Spawns(rig, map[string]staleReplaySpawn{lane: {owner: t6924Design, task: "1001"}})
	rig.tasks.set(t6924HKTask{ID: 1001, State: "merged", CompletedAt: t6924At(rig.before(26)), UpdatedAt: rig.before(26)})
	rig.setRoute(1, "")
	listed := rig.retire(1, rig.before(40), "[consult-done] task ended 14 h after this receipt")
	overtaken := rig.retire(1, rig.before(30), "[consult-done] task ended 4 h after this receipt")
	rig.fileNextDay()

	items := rig.designItems()
	if len(items) != 1 {
		t.Fatalf("design lane items=%d, want one notice: %v", len(items), items)
	}
	if !strings.Contains(items[0], "hk "+listed) || !strings.Contains(items[0], "job "+lane+" (via lane name)") {
		t.Fatalf("the unbound row is not listed on its design lane, named as resolved by the lane name: %s", items[0])
	}
	if strings.Contains(items[0], "hk "+overtaken) || !strings.Contains(items[0], "Overtaken by their task within 6 h of receipt, not listed: 1.") {
		t.Fatalf("the row its task overtook is not counted in the footer: %s", items[0])
	}
	if items := rig.itemsOf(defaultLoopsNoticeLane); len(items) != 0 {
		t.Fatalf("a resolved row was also listed on the notice lane: %v", items)
	}
}

// A lane whose name matches no spawn keeps its row on the hub notice lane, with
// the job shown as "-" and no lane-name marker. Killed by: resolving a row to a
// spawn that does not exist, or by marking an unresolved row as resolved.
func TestStaleNoticeLaneFallbackUnmatchedLaneStaysOnNoticeLane(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	t7098Spawns(rig, map[string]staleReplaySpawn{"job-some-other-lane": {owner: t6924Design, task: "1001"}})
	rig.setRoute(1, "")
	eventID := rig.retire(1, rig.before(30), "[consult-done] no spawn is named like this lane")
	rig.fileNextDay()

	if items := rig.designItems(); len(items) != 0 {
		t.Fatalf("an unmatched row was filed on the design lane: %v", items)
	}
	items := rig.itemsOf(defaultLoopsNoticeLane)
	if len(items) != 1 || !strings.Contains(items[0], "hk "+eventID) || !strings.Contains(items[0], "job - received") {
		t.Fatalf("unmatched row not listed on the notice lane with job -: items=%v", items)
	}
	if strings.Contains(items[0], "via lane name") {
		t.Fatalf("an unresolved row is marked as resolved by the lane name: %s", items[0])
	}
}

// A binding for the row's receipt wins over the lane-name fallback. The bound job's
// task is still open, so the row is listed under that job; the lane-name spawn's
// task has ended in the window, and would overtake the row if it were used.
// Killed by: trying the lane-name fallback before the binding.
func TestStaleNoticeLaneFallbackBindingWins(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	bound := t6924Job(1)
	t7098Spawns(rig, map[string]staleReplaySpawn{
		bound:        {owner: t6924Design, task: "1001"},
		t6924Lane(1): {owner: t6924Design, task: "2002"},
	})
	rig.tasks.set(t6924HKTask{ID: 1001, State: "started", UpdatedAt: rig.before(26)})
	rig.tasks.set(t6924HKTask{ID: 2002, State: "merged", CompletedAt: t6924At(rig.before(26)), UpdatedAt: rig.before(26)})
	eventID := rig.receive(1, rig.before(30), "[consult-done] bound to the job it was received for")
	rig.setRoute(1, "")
	rig.replay()
	rig.fileNextDay()

	items := rig.designItems()
	if len(items) != 1 || !strings.Contains(items[0], "hk "+eventID) {
		t.Fatalf("the bound row is not listed on its design lane: items=%v", items)
	}
	if !strings.Contains(items[0], "job "+bound+" received") || strings.Contains(items[0], "via lane name") {
		t.Fatalf("the bound row is not named by its binding's job: %s", items[0])
	}
	if strings.Contains(items[0], "Overtaken") {
		t.Fatalf("the bound row was judged by the lane-name spawn's task: %s", items[0])
	}
}

// A lane-name spawn created after the row arrived is not the spawn the row was
// received for, so the row is not resolved through it. Killed by: dropping the
// created-before-receipt check on the fallback.
func TestStaleNoticeLaneFallbackSpawnAfterReceiptIsNotUsed(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	t7098Spawns(rig, map[string]staleReplaySpawn{t6924Lane(1): {owner: t6924Design, task: "1001", created: rig.before(10)}})
	rig.setRoute(1, "")
	eventID := rig.retire(1, rig.before(30), "[consult-done] received before its lane-named spawn existed")
	rig.fileNextDay()

	if items := rig.designItems(); len(items) != 0 {
		t.Fatalf("a row received before the spawn was filed on the design lane: %v", items)
	}
	items := rig.itemsOf(defaultLoopsNoticeLane)
	if len(items) != 1 || !strings.Contains(items[0], "hk "+eventID) {
		t.Fatalf("row received before its lane-named spawn not listed on the notice lane: items=%v", items)
	}
}

// A store written before hk 7098 has no job_via column. Opening it adds the
// column, and a row the lane name resolves is then kept with its job source.
// Killed by: leaving addStaleReplayJobVia out of the opener, or not storing the
// job source.
func TestStaleNoticeJobViaColumnAddedOnOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	rig := newT6924Rig(t, path)
	client := rig.hub.handoffkeep
	if _, err := rig.hub.inbox.store.db.Exec(`ALTER TABLE relay_stale_notice_rows DROP COLUMN job_via`); err != nil {
		t.Fatal(err)
	}
	if err := rig.hub.Close(); err != nil {
		t.Fatal(err)
	}
	rig.open(client)
	t7098Spawns(rig, map[string]staleReplaySpawn{t6924Lane(1): {owner: t6924Design, task: "1001"}})
	var columns int
	if err := rig.hub.inbox.store.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('relay_stale_notice_rows') WHERE name='job_via'`).Scan(&columns); err != nil {
		t.Fatal(err)
	}
	if columns != 1 {
		t.Fatalf("job_via column count after open = %d, want 1", columns)
	}
	rig.setRoute(1, "")
	rig.retire(1, rig.before(30), "[consult-done] kept after the upgrade")
	var via string
	if err := rig.hub.inbox.store.db.QueryRow(`SELECT job_via FROM relay_stale_notice_rows WHERE worker_lane=?`, t6924Lane(1)).Scan(&via); err != nil {
		t.Fatal(err)
	}
	if via != staleReplayJobViaLaneName {
		t.Fatalf("job_via of the lane-name row = %q, want %q", via, staleReplayJobViaLaneName)
	}
}

// With the hub's spawn store (no hook), the lane-name read is bounded by the
// receipt: the newest spawn of the lane's name created before the row arrived
// names the owner, and a later spawn of that name is not read. Killed by: reading
// the newest spawn of the job with no time bound, whose worker-lane owner would
// leave the row out of the notice.
func TestStaleNoticeLaneFallbackStoreReadIsBoundedByReceipt(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	rig.hub.staleReplaySpawn = nil
	lane := t6924Lane(1)
	store := rig.hub.inbox.store
	for _, spawn := range []struct {
		request, owner string
		createdAt      time.Time
	}{
		{"t7098-early", t6924Design, rig.before(40)},
		{"t7098-late", t6924Lane(2), rig.before(10)},
	} {
		if _, err := store.db.Exec(store.q(`INSERT INTO spawn_operations(request_id,job,owner,fingerprint,payload,status,created_ms,deadline_ms) VALUES(?,?,?,?,?,?,?,?)`),
			spawn.request, lane, spawn.owner, "fp-"+spawn.request, "{}", "succeeded", spawn.createdAt.UnixMilli(), spawn.createdAt.UnixMilli()+1); err != nil {
			t.Fatal(err)
		}
	}
	rig.setRoute(1, "")
	eventID := rig.retire(1, rig.before(30), "[consult-done] received before the later spawn of its lane name")
	rig.fileNextDay()

	items := rig.designItems()
	if len(items) != 1 || !strings.Contains(items[0], "hk "+eventID) || !strings.Contains(items[0], "job "+lane+" (via lane name)") {
		t.Fatalf("the row is not resolved to the spawn created before its receipt: items=%v", items)
	}
}

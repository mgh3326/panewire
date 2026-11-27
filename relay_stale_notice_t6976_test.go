package panewire

// hk 6976: the stale-replay notice's lane bindings and entries, as the rig in
// relay_stale_notice_t6924_test.go drives them.

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

// hk 6976 #1: a lane that moves A -> B -> A has one binding per change of its job,
// so a row from its second stint of A names A. Killed by: keeping only the first
// binding of each (lane, job) pair, which resolves the second stint's row to B.
func TestStaleNoticeBindingFollowsTheJobInEachStint(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	const jobB = "job-6924-w1b"
	rig.taskOf[t6924Job(1)] = "1001"
	rig.taskOf[jobB] = "1002"
	rig.tasks.set(t6924HKTask{ID: 1001, State: "started", UpdatedAt: rig.before(30)})
	// B's task ends an hour after its row and before the second A row arrives.
	rig.tasks.set(t6924HKTask{ID: 1002, State: "merged", CompletedAt: t6924At(rig.before(27)), UpdatedAt: rig.before(27)})

	firstA := rig.receive(1, rig.before(30), "[consult-done] first stint of A")
	rig.setRoute(1, jobB)
	rowB := rig.receive(1, rig.before(29), "[consult-done] the stint on B")
	rig.setRoute(1, t6924Job(1))
	secondA := rig.receive(1, rig.before(28), "[consult-done] second stint of A")
	rig.replay()
	rig.fileNextDay()

	items := rig.designItems()
	if len(items) != 1 {
		t.Fatalf("design lane items=%d, want one notice: %v", len(items), items)
	}
	if !strings.Contains(items[0], "Overtaken by their task within 6 h of receipt, not listed: 1.") {
		t.Fatalf("the row of the B stint is not counted as overtaken: %s", items[0])
	}
	if strings.Contains(items[0], "hk "+rowB+" ") {
		t.Fatalf("the row of the B stint is listed: %s", items[0])
	}
	if !strings.Contains(items[0], "hk "+secondA) || !strings.Contains(items[0], "hk "+firstA) {
		t.Fatalf("the rows of A are not listed against the open A task: %s", items[0])
	}
}

// hk 6976 #2: a row its lane already read or acked is not listed in the notice.
// Killed by: inserting the row before its settled check.
func TestStaleNoticeSettledRowIsNotNoticed(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	eventID := rig.receive(1, rig.before(26), "[consult-done] acked by its lane")
	rig.fake.mu.Lock()
	var key string
	for _, row := range rig.fake.rows {
		if row.EventID == eventID {
			key = relayEventDedupeKey(row.Kind, relayEventFromRecord(*row))
		}
	}
	rig.fake.mu.Unlock()
	if key == "" {
		t.Fatal("the receipt has no handoffkeep row")
	}
	// The receipt is in the lane's inbox (the receipt injected it); the lane has
	// acknowledged it.
	res, err := rig.hub.inbox.store.db.Exec(`UPDATE lane_inbox SET acked_at=1 WHERE event_key=?`, key)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("the receipt has %d inbox items under its key, want the one the lane holds", n)
	}
	rig.replay()
	if got := rig.noticeRows(); got != 0 {
		t.Fatalf("a row its lane acked was kept for the notice: rows=%d", got)
	}
	if got := rig.retiredCount(); got != 1 {
		t.Fatalf("the acked row was not retired: closed=%d", got)
	}
	rig.fileNextDay()
	if items := rig.designItems(); len(items) != 0 {
		t.Fatalf("an acked row was filed in the notice: %v", items)
	}
}

// hk 6976 #3: a binding is stamped with the received_at handoffkeep gave the row,
// not the hub's clock. Here the hub's clock runs an hour behind the receipts it
// records, so a binding stamped by the hub clock would start after its rows and
// leave them with no binding in force. Killed by: stamping the binding with the
// hub clock.
func TestStaleNoticeBindingUsesHandoffkeepClock(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	const jobB = "job-6924-w1b"
	rig.taskOf[t6924Job(1)] = "1001"
	rig.taskOf[jobB] = "1002"
	rig.tasks.set(t6924HKTask{ID: 1001, State: "started", UpdatedAt: rig.before(30)})
	rig.tasks.set(t6924HKTask{ID: 1002, State: "merged", CompletedAt: t6924At(rig.before(28)), UpdatedAt: rig.before(28)})
	hubClock := rig.before(20)

	open := rig.receiveStamped(1, hubClock, rig.before(30), "[consult-done] on A")
	rig.setRoute(1, jobB)
	onB := rig.receiveStamped(1, hubClock, rig.before(29), "[consult-done] on B")
	rig.replay()
	rig.fileNextDay()

	items := rig.designItems()
	if len(items) != 1 {
		t.Fatalf("design lane items=%d, want one notice: %v", len(items), items)
	}
	if strings.Contains(items[0], "hk "+onB+" ") {
		t.Fatalf("the row received on B is listed: the hub clock put its binding after the row: %s", items[0])
	}
	if !strings.Contains(items[0], "Overtaken by their task within 6 h of receipt, not listed: 1.") {
		t.Fatalf("the row received on B is not counted as overtaken: %s", items[0])
	}
	if !strings.Contains(items[0], "hk "+open) {
		t.Fatalf("the row received on A is not listed: %s", items[0])
	}
}

// hk 6976 #4 and #5: each notice entry names its row's worker lane and job, each
// cut to 40 bytes. Killed by: an entry that names only the row.
func TestStaleNoticeEntryNamesLaneAndJob(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	rig.retire(1, rig.before(26), "[consult-done] named by its entry")
	rig.fileNextDay()
	items := rig.designItems()
	if len(items) != 1 {
		t.Fatalf("design lane items=%d, want one notice: %v", len(items), items)
	}
	if !strings.Contains(items[0], " lane "+t6924Lane(1)+" job "+t6924Job(1)+" received ") {
		t.Fatalf("the entry does not name its lane and job: %s", items[0])
	}
}

// hk 6994 (PR #933 review): a row a restarted hub resends is answered 200 with the
// original receipt, so it must not bind the lane's current job at that old time.
// Here the lane moves from A to B after row X arrives on A, with no receipt yet on
// B; a restart then resends X. Killed by: binding on every successful persist, not only a created row: X
// would resolve to B, and B's merged task would overtake it.
func TestStaleNoticeResentRowDoesNotRebindItsLane(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	const jobB = "job-6924-w1b"
	rig.taskOf[t6924Job(1)] = "1001"
	rig.taskOf[jobB] = "1002"
	rig.tasks.set(t6924HKTask{ID: 1001, State: "started", UpdatedAt: rig.before(30)})
	rig.tasks.set(t6924HKTask{ID: 1002, State: "merged", CompletedAt: t6924At(rig.before(27)), UpdatedAt: rig.before(27)})

	// The lane's A binding starts at its first receipt; row X is a later receipt
	// on A that adds no binding.
	rig.receive(1, rig.before(31), "[consult-done] first receipt on A")
	rowX := rig.receive(1, rig.before(28), "[consult-done] received on A")
	// The lane is reassigned to B, and no receipt has bound B yet.
	rig.setRoute(1, jobB)

	// Restart, then resend row X's body: handoffkeep answers 200 with X's receipt.
	client := rig.hub.handoffkeep
	if err := rig.hub.Close(); err != nil {
		t.Fatal(err)
	}
	rig.open(client)
	rig.hub.relayLaneEventContext(context.Background(), hubJobEventPayload{
		JobID: laneEventTransportID(t6924Lane(1), rowX), Epoch: 1, OwnerLane: t6924Lane(1), EventID: rowX,
		Text: "[consult-done] received on A",
	}, nil)

	rig.replay()
	rig.fileNextDay()
	items := rig.designItems()
	if len(items) != 1 {
		t.Fatalf("design lane items=%d, want one notice: %v", len(items), items)
	}
	// The item is JSON: the last named row is followed by the closing quote.
	if !strings.Contains(items[0], "hk "+rowX+" ") && !strings.Contains(items[0], "hk "+rowX+`","`) {
		t.Fatalf("row X, received on A while A's task was open, is not listed: a resend rebound its lane to B: %s", items[0])
	}
}

package panewire

import (
	"strings"
	"testing"
	"time"
)

// hk 6384: the obligation store behaves the same on SQLite, Postgres and
// MySQL. Killed by: a statement that spells one dialect's form, a schema that
// differs on one backend, a transition that rewrites a final row, or an adopt
// cursor that only updates a row already there.
func TestObligationStoreContract(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		ctx := t.Context()
		now := time.UnixMilli(1_800_000_000_000).UTC()
		count := func(query string, args ...any) (n int) {
			t.Helper()
			if err := s.db.QueryRowContext(ctx, s.q(query), args...).Scan(&n); err != nil {
				t.Fatal(err)
			}
			return n
		}
		exec := func(query string, args ...any) error {
			_, err := s.db.ExecContext(ctx, s.q(query), args...)
			return err
		}
		if got := count(`SELECT count(*) FROM delivery_reconciler_state WHERE singleton=1 AND adopt_after_id=0 AND adopt_wrapped_at=0`); got != 1 {
			t.Fatalf("seeded reconciler rows = %d", got)
		}

		// Hub keys join their parts with NUL, and one longer than MySQL's
		// column is stored hashed there.
		short := relayLaneEventOutboxKey("lane-a", "short")
		long := relayLaneEventOutboxKey("lane-a", strings.Repeat("x", mysqlEventKeyMax))
		third := relayLaneEventOutboxKey("lane-a", "third")
		row := func(key string, relayID int64) newDeliveryObligation {
			return newDeliveryObligation{key: key, relayID: relayID, kind: "lane.event", lane: "lane-a", machine: "host-a", pane: "w1:p1", attempts: 2}
		}
		for _, o := range []newDeliveryObligation{row(short, 1), row(long, 2)} {
			if err := s.createObligation(ctx, o, now, now.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
		}
		// The first writer wins, by create and by adopt.
		if err := s.createObligation(ctx, row(short, 99), now.Add(time.Hour), now); err != nil {
			t.Fatal(err)
		}
		wide := row(third, 3)
		wide.lane, wide.pane = strings.Repeat("l", 300), strings.Repeat("페", 100)
		if err := s.adoptObligations(ctx, []newDeliveryObligation{row(short, 98), row(long, 97), wide}, now, now.Add(30*time.Second)); err != nil {
			t.Fatal(err)
		}
		if got := count(`SELECT count(*) FROM delivery_obligations`); got != 3 {
			t.Fatalf("obligations = %d, want one per event key", got)
		}

		if due, err := s.dueObligations(ctx, now, 10); err != nil || len(due) != 0 {
			t.Fatalf("due before any is = %+v, %v", due, err)
		}
		due, err := s.dueObligations(ctx, now.Add(time.Minute), 10)
		if err != nil || len(due) != 3 || due[0].relayID != 3 || due[1].relayID != 1 || due[2].relayID != 2 {
			t.Fatalf("due = %+v, %v; want the longest due first", due, err)
		}
		if limited, err := s.dueObligations(ctx, now.Add(time.Minute), 2); err != nil || len(limited) != 2 {
			t.Fatalf("limited due = %+v, %v", limited, err)
		}
		byRelay := map[int64]openObligation{}
		for _, o := range due {
			byRelay[o.relayID] = o
			if o.state != obligationOwed || o.createdAt != now.UnixMilli() {
				t.Fatalf("due row = %+v", o)
			}
		}
		for id, key := range map[int64]string{1: short, 2: long, 3: third} {
			if !s.tracks(byRelay[id], key) || s.tracks(byRelay[id], key+"y") {
				t.Fatalf("obligation %d does not track exactly its key", id)
			}
		}

		// Inbox evidence joins on the stored event key.
		item, err := s.put(long, "lane-a", "lane.event", "interrupt", quietEvent("long", "body"), now, 0, 0)
		if err != nil || !item.Fresh {
			t.Fatal(item, err)
		}
		if settled, err := s.obligationInboxSettled(ctx, byRelay[2].id, now.Add(time.Hour)); err != nil || settled {
			t.Fatalf("an interrupt read at insert settled its obligation: %v, %v", settled, err)
		}
		if err := s.recordDelivery(long, "host-a", "w1:p1", now); err != nil {
			t.Fatal(err)
		}
		for before, want := range map[time.Time]bool{now: false, now.Add(time.Second): true} {
			if settled, err := s.obligationInboxSettled(ctx, byRelay[2].id, before); err != nil || settled != want {
				t.Fatalf("delivered before %v settled = %v, %v; want %v", before, settled, err, want)
			}
		}
		if settled, err := s.obligationInboxSettled(ctx, byRelay[1].id, now.Add(time.Hour)); err != nil || settled {
			t.Fatalf("another event's item settled the obligation: %v, %v", settled, err)
		}

		// A digest batch is released once nothing claims it in a releasable
		// state.
		if err := exec(`INSERT INTO lane_inbox_batches(id,lane,text) VALUES(?,?,?)`, "quiet-digest-1", "lane-a", "1 unread"); err != nil {
			t.Fatal(err)
		}
		released := func(lane, id string) bool {
			t.Helper()
			got, err := s.digestBatchReleased(ctx, lane, id)
			if err != nil {
				t.Fatal(err)
			}
			return got
		}
		for _, column := range []string{"digest_batch", "escalation_batch"} {
			if err := exec(`UPDATE lane_inbox SET `+column+`=?,read_at=0 WHERE id=?`, "quiet-digest-1", item.ID); err != nil {
				t.Fatal(err)
			}
			if released("lane-a", "quiet-digest-1") {
				t.Fatalf("a batch claimed through %s counts as released", column)
			}
			if err := s.releaseBatchClaims("quiet-digest-1"); err != nil {
				t.Fatal(err)
			}
			if !released("lane-a", "quiet-digest-1") {
				t.Fatalf("a batch released from %s still counts as claimed", column)
			}
		}
		if released("lane-b", "quiet-digest-1") || released("lane-a", "not-a-batch") {
			t.Fatal("an id that is no batch of the lane counts as released")
		}

		// Transitions: exhausted stays open, final rows never change.
		later := now.Add(2 * time.Hour)
		if err := s.exhaustObligation(ctx, byRelay[1].id, obligationFaultUnconfirmed, later, later); err != nil {
			t.Fatal(err)
		}
		if err := s.closeObligation(ctx, byRelay[2].id, obligationSettled, strings.Repeat("r", 100), later); err != nil {
			t.Fatal(err)
		}
		if err := s.closeObligation(ctx, byRelay[3].id, obligationConfirmed, "", later); err != nil {
			t.Fatal(err)
		}
		for _, id := range []int64{byRelay[2].id, byRelay[3].id} {
			after := later.Add(time.Hour)
			for _, change := range []error{
				s.exhaustObligation(ctx, id, "x", after, after),
				s.deferObligation(ctx, id, after, after),
				s.closeObligation(ctx, id, obligationSettled, "again", after),
			} {
				if change != nil {
					t.Fatal(change)
				}
			}
		}
		if got := count(`SELECT count(*) FROM delivery_obligations WHERE updated_at=? AND ((relay_id=2 AND state='settled' AND settle_reason=?) OR (relay_id=3 AND state='confirmed'))`, later.UnixMilli(), strings.Repeat("r", 64)); got != 2 {
			t.Fatalf("final rows kept = %d, want 2", got)
		}
		due, err = s.dueObligations(ctx, later, 10)
		if err != nil || len(due) != 1 || due[0].relayID != 1 || due[0].state != obligationExhausted {
			t.Fatalf("open after the transitions = %+v, %v", due, err)
		}
		if err := s.deferObligation(ctx, byRelay[1].id, later, later.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if due, err := s.dueObligations(ctx, later, 10); err != nil || len(due) != 0 {
			t.Fatalf("due after a deferral = %+v, %v", due, err)
		}
		// A noticed row keeps its state when exhausted again, and can close.
		if err := exec(`UPDATE delivery_obligations SET state='noticed' WHERE relay_id=1`); err != nil {
			t.Fatal(err)
		}
		if err := s.exhaustObligation(ctx, byRelay[1].id, obligationFaultUnconfirmed, later, later); err != nil {
			t.Fatal(err)
		}
		if got := count(`SELECT count(*) FROM delivery_obligations WHERE relay_id=1 AND state='noticed' AND fault='unconfirmed'`); got != 1 {
			t.Fatal("a noticed row lost its state")
		}

		// The adopt cursor, which a store copied without the row still saves.
		cursorIs := func(s *laneInboxStore, after, wrapped int64) {
			t.Helper()
			gotAfter, gotWrapped, err := s.adoptCursor(ctx)
			if err != nil || gotAfter != after || gotWrapped != wrapped {
				t.Fatalf("cursor = %d/%d, %v; want %d/%d", gotAfter, gotWrapped, err, after, wrapped)
			}
		}
		if err := s.saveAdoptCursor(ctx, 41, 7); err != nil {
			t.Fatal(err)
		}
		cursorIs(s, 41, 7)
		if err := exec(`DELETE FROM delivery_reconciler_state`); err != nil {
			t.Fatal(err)
		}
		cursorIs(s, 0, 0)
		if err := s.saveAdoptCursor(ctx, 42, 8); err != nil {
			t.Fatal(err)
		}
		cursorIs(s, 42, 8)

		// Uniqueness holds on result_id, on an incident's open slot and on a
		// notice key; NULL never collides.
		if err := exec(`UPDATE delivery_obligations SET result_id=? WHERE relay_id=1`, "ready-1"); err != nil {
			t.Fatal(err)
		}
		if err := exec(`UPDATE delivery_obligations SET result_id=? WHERE relay_id=2`, "ready-1"); err == nil {
			t.Fatal("two obligations share a result_id")
		}
		if got := count(`SELECT count(*) FROM delivery_obligations WHERE result_id IS NULL`); got != 2 {
			t.Fatalf("obligations without a result = %d", got)
		}
		incident := func(slot any) error {
			return exec(`INSERT INTO delivery_incidents(design_lane,machine,fault,state,open_slot,opened_at) VALUES('d','m','f','open',?,?)`, slot, now.UnixMilli())
		}
		for _, slot := range []any{nil, nil, "d|m|f"} {
			if err := incident(slot); err != nil {
				t.Fatal(err)
			}
		}
		if err := incident("d|m|f"); err == nil {
			t.Fatal("two open incidents share a slot")
		}
		if got := count(`SELECT count(DISTINCT id) FROM delivery_incidents`); got != 3 {
			t.Fatalf("incident ids = %d, want 3 the store handed out", got)
		}
		slot := `INSERT INTO delivery_notice_slots(design_lane,machine,fault) VALUES('d','m','f')`
		if err := exec(slot); err != nil {
			t.Fatal(err)
		}
		if err := exec(slot); err == nil {
			t.Fatal("a notice key has two slots")
		}

		// A reopen keeps every row and seeds nothing twice.
		if err := s.close(); err != nil {
			t.Fatal(err)
		}
		s = open()
		cursorIs(s, 42, 8)
		for table, want := range map[string]int{"delivery_obligations": 3, "delivery_incidents": 3, "delivery_notice_slots": 1, "delivery_reconciler_state": 1} {
			if got := count(`SELECT count(*) FROM ` + table); got != want {
				t.Fatalf("%s rows after reopen = %d, want %d", table, got, want)
			}
		}
	})
}

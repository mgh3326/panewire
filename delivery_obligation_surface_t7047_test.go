package panewire

// hk 7047 (hk 6823, PR #941 review): a marker is retired only once its origin
// row has no replay left, a digest batch's origin is retired before its marker,
// and the retry count is the budget the retry backoff spends.

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

// A marker whose origin is still undelivered stays linked and undelivered when
// its obligation closes as settled by the receiver's inbox item, so a rollback
// replay of that origin still finds the labelled marker. Once the origin is
// retired, the next pass releases the marker and clears the link.
// Killed by: retiring the marker at close, before the origin has no replay left.
func TestMarkerKeptUntilOriginRetired(t *testing.T) {
	rig := newT6384Rig(t)
	acked := rig.send("lane-a", "ev-kept")
	marker := t6384Row(5100, relayUnconfirmedMarkLane, unconfirmedMarkEventID(acked.EventID), 1)
	rig.fake.seedUndelivered(marker)
	rig.pass(0)
	rig.exec(`UPDATE delivery_obligations SET marker_event_id=? WHERE relay_id=?`, marker.EventID, acked.EventID)

	// The origin stays undelivered: its /delivered write fails while the lane
	// reads its inbox item.
	rig.fake.mu.Lock()
	rig.fake.deliveredFailIDs = map[int64]bool{acked.EventID: true}
	rig.fake.mu.Unlock()
	item := t3426Item(t, rig.hub, "lane-a", "lane.event", acked.JobID)
	if n, err := rig.hub.inbox.store.ack(item.ID, "lane-a", false, rig.clock.Now()); err != nil || n != 1 {
		t.Fatalf("ack = %d, %v", n, err)
	}
	rig.pass(2 * time.Minute)
	rig.want(acked.EventID, obligationSettled, obligationSettledInbox)
	if got := rig.row(acked.EventID); got.DeliveredAt != "" {
		t.Fatalf("origin = %+v, want it still undelivered", got)
	}
	if got := rig.row(marker.ID); got.DeliveredAt != "" {
		t.Fatalf("marker retired while its origin is undelivered: %+v", got)
	}
	if link := t7047MarkerLink(t, rig, acked.EventID); link != marker.EventID {
		t.Fatalf("marker_event_id = %q, want %q kept", link, marker.EventID)
	}

	// The origin is retired later; the next pass releases the marker.
	rig.fake.mu.Lock()
	rig.fake.deliveredFailIDs = nil
	rig.fake.mu.Unlock()
	if err := rig.client.markDelivered(rig.t.Context(), acked.EventID, "hub", relayReplayRetiredPane+relayUnconfirmedRetireReason); err != nil {
		t.Fatal(err)
	}
	rig.pass(time.Minute)
	if got := rig.row(marker.ID); got.DeliveredAt == "" || !strings.HasPrefix(got.DeliveredTo, relayReplayRetiredMarker) {
		t.Fatalf("marker row = %+v, want it retired once its origin was", got)
	}
	if link := t7047MarkerLink(t, rig, acked.EventID); link != "" {
		t.Fatalf("marker_event_id = %q, want it cleared", link)
	}
}

// A digest batch's origin is retired with digest-not-replayed before its
// marker, so the marker is released in the same close. When that origin write
// fails, the marker stays linked and undelivered; the next pass retires the
// origin and then the marker.
// Killed by: retiring the marker without retiring its digest origin, or
// retiring the marker when the origin write failed.
func TestDigestCloseRetiresOriginBeforeMarker(t *testing.T) {
	rig := newT6384Rig(t)
	batchID := t7047DigestBatch(t, rig)
	marker := t6384Row(5200, relayUnconfirmedMarkLane, unconfirmedMarkEventID(batchID)+relayUnconfirmedDigestMarkSuffix, 1)
	rig.fake.seedUndelivered(marker)
	rig.pass(0)
	rig.exec(`UPDATE delivery_obligations SET marker_event_id=? WHERE relay_id=?`, marker.EventID, batchID)
	rig.want(batchID, obligationOwed, "")

	// The origin's retire write fails: the batch still settles, the marker stays.
	rig.fake.mu.Lock()
	rig.fake.deliveredFailIDs = map[int64]bool{batchID: true}
	rig.fake.mu.Unlock()
	if err := rig.hub.inbox.store.releaseBatchClaims(rig.row(batchID).EventID); err != nil {
		t.Fatal(err)
	}
	rig.pass(2 * time.Minute)
	rig.want(batchID, obligationSettled, obligationSettledDigest)
	if got := rig.row(marker.ID); got.DeliveredAt != "" {
		t.Fatalf("marker retired while the digest origin write failed: %+v", got)
	}
	if link := t7047MarkerLink(t, rig, batchID); link != marker.EventID {
		t.Fatalf("marker_event_id = %q, want %q kept", link, marker.EventID)
	}

	// The write succeeds on a later pass: the origin is retired first, then the
	// marker is released and unlinked.
	rig.fake.mu.Lock()
	rig.fake.deliveredFailIDs = nil
	rig.fake.mu.Unlock()
	rig.pass(time.Minute)
	if got := rig.row(batchID); got.DeliveredTo != relayReplayRetiredMarker+relayDigestNotReplayedRetireReason {
		t.Fatalf("digest origin delivered_to = %q, want %q", got.DeliveredTo, relayReplayRetiredMarker+relayDigestNotReplayedRetireReason)
	}
	if got := rig.row(marker.ID); got.DeliveredAt == "" {
		t.Fatalf("marker row = %+v, want it retired once the origin was", got)
	}
	if link := t7047MarkerLink(t, rig, batchID); link != "" {
		t.Fatalf("marker_event_id = %q, want it cleared", link)
	}
}

// Retries count the copies the backoff spent past the handoffkeep attempts a
// row arrived with: none before any retry, and one after one retry, whether
// the row was seeded with 0 or 2 attempts.
// Killed by: counting the spent budget without the attempts seed it starts from.
func TestRetriesCountTheBudgetTheBackoffSpends(t *testing.T) {
	for _, attempts := range []int{0, 2} {
		t.Run("attempts "+strconv.Itoa(attempts), func(t *testing.T) {
			rig := newT6384Rig(t)
			row := t6384Row(int64(8100+attempts), "lane-a", "ev-budget-"+strconv.Itoa(attempts), attempts)
			rig.fake.seedUndelivered(row)
			rig.pass(0) // adopts the row's obligation; it is not yet due
			counts := func() hubObligationCounts {
				c, err := rig.hub.inbox.store.deliveryCounts(rig.t.Context())
				if err != nil {
					t.Fatal(err)
				}
				return c
			}
			if c := counts(); c.Retries != 0 {
				t.Fatalf("attempts %d: retries before any retry = %d, want 0", attempts, c.Retries)
			}
			// One retry claims the copy through the same compare-and-swap the
			// reconciler uses.
			store := rig.hub.inbox.store
			o, err := scanOpenObligation(store.db.QueryRowContext(rig.t.Context(), `SELECT `+openObligationColumns+` FROM delivery_obligations WHERE relay_id=?`, row.ID))
			if err != nil {
				t.Fatal(err)
			}
			spent, err := store.spendObligationRetry(rig.t.Context(), o, 0, rig.clock.Now(), rig.clock.Now().Add(time.Minute))
			if err != nil || !spent {
				t.Fatalf("attempts %d: spend = %v, %v", attempts, spent, err)
			}
			if c := counts(); c.Retries != 1 {
				t.Fatalf("attempts %d: retries after one retry = %d, want 1", attempts, c.Retries)
			}
		})
	}
}

// t7047DigestBatch makes the lane's info event a digest batch that is owed and
// returns its durable row.
func t7047DigestBatch(t *testing.T, rig *t6384Rig) int64 {
	t.Helper()
	info := t6384Event("lane-a", "ev-digested-7047", "nightly numbers")
	info.NotifyType = "info"
	rig.hub.relayLaneEvent(info, rig.agent)
	rig.clock.advance(31 * time.Minute) // past the inbox deadline
	rig.hub.flushInbox()
	batch := rig.injected()
	rig.want(batch.EventID, obligationOwed, "")
	return batch.EventID
}

// t7047MarkerLink is the marker_event_id the obligation of relay id holds.
func t7047MarkerLink(t *testing.T, rig *t6384Rig, relayID int64) string {
	t.Helper()
	var link string
	if err := rig.hub.inbox.store.db.QueryRow(`SELECT marker_event_id FROM delivery_obligations WHERE relay_id=?`, relayID).Scan(&link); err != nil {
		t.Fatal(err)
	}
	return link
}

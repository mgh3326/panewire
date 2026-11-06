package panewire

// An unconfirmed relay row that is an inbox digest or escalation batch is not
// replayed after a hub restart: its count is stale, and it has no lane inbox
// item for the settled lookup to consult. Its still-unread items are released
// so the normal flush escalates them once more with the current count.

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type unconfirmedDigestRig struct {
	unconfirmedInboxRig
	batch int64
	ids   []int64
}

func digestRow(t *testing.T, fake *fakeHandoffkeep, id int64) handoffkeepRelayEvent {
	t.Helper()
	fake.mu.Lock()
	defer fake.mu.Unlock()
	for _, row := range fake.rows {
		if row.ID == id {
			return *row
		}
	}
	t.Fatalf("no durable row %d", id)
	return handoffkeepRelayEvent{}
}

// unconfirmedDigestStart escalates two unread inbox items as one batch,
// reports that batch's pane submission unproven (which writes the marker),
// and returns the rig with the first hub closed.
func unconfirmedDigestStart(t *testing.T) *unconfirmedDigestRig {
	t.Helper()
	return unconfirmedDigestStartClass(t, "inbox", "2 unread inbox items | lane lane-a")
}

// unconfirmedDigestStartClass is unconfirmedDigestStart for either batch kind:
// class "inbox" escalates past-deadline unread items, class "interrupt" sends
// the quiet digest of interrupts whose window has closed. wantText is the
// count line the first batch must carry.
func unconfirmedDigestStartClass(t *testing.T, class, wantText string) *unconfirmedDigestRig {
	t.Helper()
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	now := time.Now().UTC()
	store, err := openLaneInboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	rig := &unconfirmedDigestRig{}
	for i, text := range []string{"Blocker: merged, install broken", "ready | report"} {
		e := quietEvent(fmt.Sprint(i), text)
		e.DeliveryClass, e.Replay = "inbox", true
		var item laneInboxItem
		var err error
		if class == "interrupt" {
			item, err = store.put(fmt.Sprint(i), "lane-a", "lane.event", class, e, now.Add(-time.Hour), 0, time.Minute)
		} else {
			item, err = store.put(fmt.Sprint(i), "lane-a", "lane.event", class, e, now, 0, 0)
		}
		if err != nil {
			t.Fatal(err)
		}
		rig.ids = append(rig.ids, item.ID)
	}
	store.db.Close()
	fake, client, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	rig.fake, rig.hk, rig.path, rig.now = fake, client, path, now
	h, a := quietHub(t, path, "default", client, &rig.now)
	h.flushInbox()
	directive := holdReceive(t, a)
	if !strings.Contains(directive.Text, wantText) {
		t.Fatalf("first escalation = %q", directive.Text)
	}
	rig.batch = directive.EventID
	row := digestRow(t, fake, rig.batch)
	if !strings.HasPrefix(row.EventID, quietDigestBatchPrefix) {
		t.Fatalf("batch row event id = %q", row.EventID)
	}
	rig.event = hubJobEventPayload{JobID: row.JobID, Epoch: 1, OwnerLane: row.OwnerLane, EventID: row.EventID, Text: row.Text}
	if !h.markRelayEventUnconfirmed(relayPending{eventID: rig.batch, kind: "lane.event", event: rig.event}) {
		t.Fatal("the unconfirmed marker was not written")
	}
	h.Close()
	t.Cleanup(func() {
		if rig.hub != nil {
			rig.hub.Close()
		}
	})
	return rig
}

func TestUnconfirmedDigestIsNotReplayedAndStillUnreadItemsEscalateOnce(t *testing.T) {
	rig := unconfirmedDigestStart(t)
	// One of the batch's items is read while the hub is down; one stays unread.
	store, err := openLaneInboxStore(rig.path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.read(rig.ids[0], "lane-a", rig.now); err != nil {
		t.Fatal(err)
	}
	store.db.Close()

	h, a := rig.restart(t)
	if len(a.relays) != 0 {
		t.Fatalf("the stale batch was injected: %q", (<-a.relays).Text)
	}
	if got := rig.fake.deliveredToFor(rig.batch); got != "hub/"+relayReplayRetiredPane+relayDigestNotReplayedRetireReason {
		t.Fatalf("batch row delivered_to = %q", got)
	}
	if got := rig.fake.deliveredToFor(rig.markerRow(t)); got != "hub/"+relayReplayRetiredPane+relayUnconfirmedMarkRetireReason {
		t.Fatalf("marker delivered_to = %q", got)
	}
	h.flushInbox()
	if len(a.relays) != 1 {
		t.Fatalf("fresh escalations = %d, want exactly one", len(a.relays))
	}
	fresh := <-a.relays
	if !strings.Contains(fresh.Text, "1 unread inbox items | lane lane-a") || strings.Contains(fresh.Text, "[replayed after hub restart") {
		t.Fatalf("fresh escalation = %q", fresh.Text)
	}
	// A further restart and flush neither replays nor re-escalates.
	h.replayUndeliveredRelayEvents(context.Background())
	h.replayUndeliveredLaneEvents(context.Background())
	h.flushInbox()
	if len(a.relays) != 0 || rig.retiredWrites(rig.batch) != 1 {
		t.Fatalf("second walk acted: injects=%d retire writes=%d", len(a.relays), rig.retiredWrites(rig.batch))
	}
}

func TestUnconfirmedDigestWithNothingUnreadEscalatesNothing(t *testing.T) {
	rig := unconfirmedDigestStart(t)
	store, err := openLaneInboxStore(rig.path)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range rig.ids {
		if _, err := store.read(id, "lane-a", rig.now); err != nil {
			t.Fatal(err)
		}
	}
	store.db.Close()
	h, a := rig.restart(t)
	h.flushInbox()
	if len(a.relays) != 0 {
		t.Fatalf("nothing is unread but %q was injected", (<-a.relays).Text)
	}
	if got := rig.fake.deliveredToFor(rig.batch); got != "hub/"+relayReplayRetiredPane+relayDigestNotReplayedRetireReason {
		t.Fatalf("batch row delivered_to = %q", got)
	}
}

func TestIsDigestBatchNamesStoredBatchesNotHeldOrOrdinaryEvents(t *testing.T) {
	store, err := openLaneInboxStore(filepath.Join(t.TempDir(), "inbox.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.db.Close()
	for _, id := range []string{"stored-batch", "held-1"} {
		if _, err := store.db.Exec(`INSERT INTO lane_inbox_batches(id,lane,text) VALUES(?,'lane-a','x')`, id); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		lane, id string
		want     bool
	}{
		{"lane-a", "stored-batch", true},
		{"lane-b", "stored-batch", false},
		{"lane-a", "held-1", false},
		{"lane-a", quietDigestBatchPrefix + "producer-chosen-id", false},
		{"lane-a", "ev-unconfirmed", false},
	} {
		got, err := store.isDigestBatch(tc.lane, tc.id)
		if err != nil || got != tc.want {
			t.Errorf("isDigestBatch(%q, %q) = %v, %v; want %v", tc.lane, tc.id, got, err, tc.want)
		}
	}
}

// A late delivery the inbox recorded while the durable /delivered write failed
// leaves the batch row marked and undelivered. The settled check retires it as
// inbox-delivered; the receiver has the digest, so nothing escalates again.
func TestUnconfirmedDigestTheInboxDeliveredIsRetiredAsInboxDelivered(t *testing.T) {
	rig := unconfirmedDigestStart(t)
	store, err := openLaneInboxStore(rig.path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.recordDelivery(relayEventDedupeKey("lane.event", rig.event), rig.now); err != nil {
		t.Fatal(err)
	}
	store.db.Close()
	h, a := rig.restart(t)
	h.flushInbox()
	if len(a.relays) != 0 {
		t.Fatalf("a digest the inbox shows delivered was injected: %q", (<-a.relays).Text)
	}
	if got := rig.fake.deliveredToFor(rig.batch); got != "hub/"+relayReplayRetiredPane+relayUnconfirmedInboxRetireReason {
		t.Fatalf("batch row delivered_to = %q", got)
	}
}

// With no inbox store open the batch table cannot be read, but the marker
// written while a store was open says the row was a batch: the real walk
// injects nothing and spends nothing, and a later run with the store retires
// the row, releases the items and escalates them once.
func TestUnconfirmedDigestWithNoInboxStoreIsDeferredUntilTheStoreReturns(t *testing.T) {
	rig := unconfirmedDigestStart(t)
	h, a := quietHub(t, "", "off", rig.hk, &rig.now)
	if h.inbox != nil {
		t.Fatal("the kill switch left an inbox store open")
	}
	h.replayUndeliveredRelayEvents(context.Background())
	h.replayUndeliveredLaneEvents(context.Background())
	if len(a.relays) != 0 {
		t.Fatalf("the no-store walk injected %q", (<-a.relays).Text)
	}
	if got := rig.fake.deliveredToFor(rig.batch); got != "" {
		t.Fatalf("the no-store walk spent the batch row: %q", got)
	}
	if got := rig.fake.deliveredToFor(rig.markerRow(t)); got != "" {
		t.Fatalf("the marker retired with its row still pending: %q", got)
	}
	h2, a2 := rig.restart(t)
	if len(a2.relays) != 0 || rig.fake.deliveredToFor(rig.batch) != "hub/"+relayReplayRetiredPane+relayDigestNotReplayedRetireReason {
		t.Fatal("a later run with the store did not retire the batch without a copy")
	}
	h2.flushInbox()
	if len(a2.relays) != 1 || !strings.Contains((<-a2.relays).Text, "2 unread inbox items | lane lane-a") {
		t.Fatal("items released by the later run were not escalated once")
	}
}

// A digest of interrupts claims its items through digest_batch, not
// escalation_batch; the release must cover that column too.
func TestUnconfirmedInterruptDigestIsNotReplayedAndStillUnackedItemsGetOneFreshDigest(t *testing.T) {
	rig := unconfirmedDigestStartClass(t, "interrupt", "2 interrupt events | lane lane-a")
	store, err := openLaneInboxStore(rig.path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ack(rig.ids[0], "lane-a", false, rig.now); err != nil {
		t.Fatal(err)
	}
	store.db.Close()
	h, a := rig.restart(t)
	if len(a.relays) != 0 {
		t.Fatalf("the stale digest was injected: %q", (<-a.relays).Text)
	}
	if got := rig.fake.deliveredToFor(rig.batch); got != "hub/"+relayReplayRetiredPane+relayDigestNotReplayedRetireReason {
		t.Fatalf("digest row delivered_to = %q", got)
	}
	h.flushInbox()
	if len(a.relays) != 1 {
		t.Fatalf("fresh digests = %d, want exactly one", len(a.relays))
	}
	if fresh := <-a.relays; !strings.Contains(fresh.Text, "1 interrupt events | lane lane-a") || strings.Contains(fresh.Text, "[replayed after hub restart") {
		t.Fatalf("fresh digest = %q", fresh.Text)
	}
	h.flushInbox()
	if len(a.relays) != 0 {
		t.Fatal("the released item was digested twice")
	}
}

// A producer may name an ordinary event quiet-digest-...; only the batch table
// makes a batch, so it keeps its one labelled replay (#1002).
func TestUnconfirmedReplayOfProducerEventNamedLikeADigestIsStillReplayed(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	now := time.Now().UTC()
	h, a := quietHub(t, path, "default", client, &now)
	event := quietEvent(quietDigestBatchPrefix+"producer-chosen-id", "READY review")
	event.NotifyType = "ready"
	h.relayLaneEvent(event, a)
	row := holdReceive(t, a).EventID
	if !h.markRelayEventUnconfirmed(relayPending{eventID: row, kind: "lane.event", event: event}) {
		t.Fatal("the unconfirmed marker was not written")
	}
	h.Close()
	h2, a2 := quietHub(t, path, "default", client, &now)
	h2.replayUndeliveredRelayEvents(context.Background())
	h2.replayUndeliveredLaneEvents(context.Background())
	if len(a2.relays) != 1 {
		t.Fatalf("injections = %d, want the one labelled replay", len(a2.relays))
	}
	if got := fake.deliveredToFor(row); got != "hub/"+relayReplayRetiredPane+relayUnconfirmedRetireReason {
		t.Fatalf("row delivered_to = %q", got)
	}
}

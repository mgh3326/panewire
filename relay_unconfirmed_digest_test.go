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
		item, err := store.put(fmt.Sprint(i), "lane-a", "lane.event", "inbox", e, now, 0, 0)
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
	if !strings.Contains(directive.Text, "2 unread inbox items | lane lane-a") {
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
		{"lane-a", quietDigestBatchPrefix + "pruned-row", true},
		{"lane-a", "ev-unconfirmed", false},
	} {
		got, err := store.isDigestBatch(tc.lane, tc.id)
		if err != nil || got != tc.want {
			t.Errorf("isDigestBatch(%q, %q) = %v, %v; want %v", tc.lane, tc.id, got, err, tc.want)
		}
	}
}

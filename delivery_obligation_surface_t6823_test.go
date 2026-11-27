package panewire

// hk 6823 (hk 4289 T6): the operator surface of the delivery obligations —
// the marker a terminal obligation retires, the rows retention may delete,
// and what status and the feed counters say.

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Closing an obligation retires the pending marker its row kept as the
// downgrade breadcrumb, and the link is cleared once the retire is durable.
// Killed by: not retiring on close, retiring without clearing the link, or
// clearing it before the marker is delivered.
func TestMarkerRetiredWhenObligationTerminal(t *testing.T) {
	rig := newT6384Rig(t)
	directive := rig.send("lane-a", "ev-marked")
	marker := t6384Row(5000, relayUnconfirmedMarkLane, unconfirmedMarkEventID(directive.EventID), 1)
	rig.fake.seedUndelivered(marker)

	// The first pass adopts the row's obligation; the marker link is the field
	// a converted legacy marker leaves on it.
	rig.pass(time.Minute)
	rig.exec(`UPDATE delivery_obligations SET marker_event_id=? WHERE relay_id=?`, marker.EventID, directive.EventID)
	rig.delivered(directive)
	rig.pass(2 * time.Minute)

	rig.want(directive.EventID, obligationConfirmed, "")
	if got := rig.row(marker.ID); got.DeliveredAt == "" || !strings.HasPrefix(got.DeliveredTo, relayReplayRetiredMarker) {
		t.Fatalf("marker row = %+v, want it retired", got)
	}
	var link string
	if err := rig.hub.inbox.store.db.QueryRow(`SELECT marker_event_id FROM delivery_obligations WHERE relay_id=?`, directive.EventID).Scan(&link); err != nil || link != "" {
		t.Fatalf("marker_event_id = %q, %v: want it cleared", link, err)
	}
}

// A marker whose retire failed at close stays linked, and each pass retries it
// — bounded — until it is delivered. A marker whose origin is still open is
// never touched.
// Killed by: an unbounded retry sweep, or retiring an open origin's marker.
func TestReconcilerPassIsBounded(t *testing.T) {
	rig := newT6384Rig(t)
	const total = obligationMarkerRetireLimit + 4
	var openMarker handoffkeepRelayEvent
	for i := 0; i < total; i++ {
		origin := int64(6000 + i)
		rig.exec(`INSERT INTO delivery_obligations (event_key,relay_id,kind,lane,state,marker_event_id,created_at,updated_at)
			VALUES (?,?,'lane.event','lane-a','settled',?,0,0)`, "key-"+strconv.Itoa(i), origin, unconfirmedMarkEventID(origin))
		rig.fake.seedUndelivered(t6384Row(int64(7000+i), relayUnconfirmedMarkLane, unconfirmedMarkEventID(origin), 1))
	}
	rig.exec(`INSERT INTO delivery_obligations (event_key,relay_id,kind,lane,state,marker_event_id,created_at,updated_at)
		VALUES ('key-open',6999,'lane.event','lane-a','owed',?,0,0)`, unconfirmedMarkEventID(6999))
	openMarker = t6384Row(7999, relayUnconfirmedMarkLane, unconfirmedMarkEventID(6999), 1)
	rig.fake.seedUndelivered(openMarker)

	pending := func() (markers, links int) {
		for id := int64(7000); id < 7000+total; id++ {
			if row := rig.row(id); row.DeliveredAt == "" {
				markers++
			}
		}
		var n int
		if err := rig.hub.inbox.store.db.QueryRow(`SELECT count(*) FROM delivery_obligations WHERE marker_event_id<>''`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return markers, n
	}

	rig.pass(0)
	if markers, links := pending(); markers != total-obligationMarkerRetireLimit || links != total-obligationMarkerRetireLimit+1 {
		t.Fatalf("after one pass pending markers=%d links=%d, want %d each plus the open one", markers, links, total-obligationMarkerRetireLimit)
	}
	rig.pass(0)
	if markers, links := pending(); markers != 0 || links != 1 {
		t.Fatalf("after two passes pending markers=%d links=%d, want 0 and the open row's link", markers, links)
	}
	if got := rig.row(openMarker.ID); got.DeliveredAt != "" {
		t.Fatalf("the open origin's marker was retired: %+v", got)
	}
}

// Retention deletes terminal obligations, closed incidents and cold notice
// slots past the window — and nothing open or recent.
// Killed by: deleting an open obligation, or never pruning a closed one.
func TestClosedObligationPrunesOpenNeverDoes(t *testing.T) {
	rig := newT6384Rig(t)
	store := rig.hub.inbox.store
	old := rig.clock.Now().Add(-2 * deliveryObligationRetention).UnixMilli()
	recent := rig.clock.Now().Add(-time.Hour).UnixMilli()
	insert := func(id int64, state string, at int64, incident int64) {
		rig.exec(`INSERT INTO delivery_obligations (event_key,relay_id,kind,lane,state,incident_id,created_at,updated_at)
			VALUES (?,?,'lane.event','lane-a',?,?,0,?)`, "key-"+strconv.FormatInt(id, 10), id, state, incident, at)
	}
	// Terminal past the window prunes; recent terminal and every open state
	// stay.
	insert(1, obligationSettled, old, 0)
	insert(2, obligationConfirmed, old, 0)
	insert(3, obligationSettled, recent, 0)
	insert(4, obligationOwed, old, 0)
	insert(5, obligationNoticed, old, 0)
	insert(6, obligationExhausted, old, 0)
	// A closed obligation still named by an open incident stays.
	insert(7, obligationSettled, old, 10)
	// Incidents: closed and unreferenced prunes; open never does, and the open
	// one keeps its member's row.
	rig.exec(`INSERT INTO delivery_incidents (design_lane,machine,fault,state,open_slot,opened_at,closed_at,close_reason)
		VALUES ('lane-a-design','host-a','retries_exhausted','closed',NULL,0,?, 'quiet')`, old)
	rig.exec(`INSERT INTO delivery_incidents (id,design_lane,machine,fault,state,open_slot,opened_at)
		VALUES (10,'lane-a-design','host-a','pane_read_failed','open','lane-a-design|host-a|pane_read_failed',0)`)
	// Notice slots: cold and unheld prunes; one an open incident still holds stays.
	rig.exec(`INSERT INTO delivery_notice_slots (design_lane,machine,fault,notice_at) VALUES ('lane-a-design','host-a','gone',?)`, old)
	rig.exec(`INSERT INTO delivery_notice_slots (design_lane,machine,fault,notice_at) VALUES ('lane-a-design','host-a','pane_read_failed',?)`, old)

	counts, err := store.pruneHubStore(rig.clock.Now(), 100)
	if err != nil {
		t.Fatal(err)
	}
	if counts.DeliveryObligations != 2 || counts.DeliveryIncidents != 1 || counts.DeliveryNoticeSlots != 1 {
		t.Fatalf("counts = %+v, want 2 obligations, 1 incident, 1 slot", counts)
	}
	// relay 7 survives only because the open incident still names it; once the
	// incident closes and ages out it goes too.
	var left []int64
	rows, err := store.db.Query(`SELECT relay_id FROM delivery_obligations ORDER BY relay_id`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		left = append(left, id)
	}
	rows.Close()
	if got := fmt.Sprint(left); got != "[3 4 5 6 7]" {
		t.Fatalf("obligations left = %s, want the open and recent and incident-named ones", got)
	}
	var incidents int
	if err := store.db.QueryRow(`SELECT count(*) FROM delivery_incidents`).Scan(&incidents); err != nil || incidents != 1 {
		t.Fatalf("incidents left = %d, %v: want only the open one", incidents, err)
	}
	var slot string
	if err := store.db.QueryRow(`SELECT fault FROM delivery_notice_slots`).Scan(&slot); err != nil || slot != "pane_read_failed" {
		t.Fatalf("slot left = %q, %v: want the held one", slot, err)
	}
}

// No pass ever deletes an open obligation, however old: owed, noticed and
// exhausted rows all outlive the window, and a closed incident keeps its open
// member's row.
// Killed by: pruning on age alone, or treating 'exhausted' as terminal.
func TestObligationRetentionNeverDeletesOpen(t *testing.T) {
	rig := newT6384Rig(t)
	store := rig.hub.inbox.store
	old := rig.clock.Now().Add(-100 * deliveryObligationRetention).UnixMilli()
	for i, state := range []string{obligationOwed, obligationNoticed, obligationExhausted} {
		rig.exec(`INSERT INTO delivery_obligations (event_key,relay_id,kind,lane,state,created_at,updated_at)
			VALUES (?,?,'lane.event','lane-a',?,0,?)`, "open-"+strconv.Itoa(i), int64(100+i), state, old)
	}
	for i := 0; i < 3; i++ {
		if _, err := store.pruneHubStore(rig.clock.Now(), 1000); err != nil {
			t.Fatal(err)
		}
	}
	var n int
	if err := store.db.QueryRow(`SELECT count(*) FROM delivery_obligations`).Scan(&n); err != nil || n != 3 {
		t.Fatalf("obligations left = %d, %v: want all 3 open rows", n, err)
	}
}

// The ack-state answer and the status line carry the obligation of the row the
// delivery fields name.
// Killed by: not joining the obligation, or dropping the fields from the text.
func TestInboxStatusSurfacesObligation(t *testing.T) {
	rig := newT6384Rig(t)
	directive := rig.send("lane-a", "ev-status")
	rig.pass(time.Minute)
	rig.exec(`UPDATE delivery_obligations SET state=?,fault=?,settle_reason=? WHERE relay_id=?`,
		obligationSettled, relayDroppedRetriesExhausted, "stale", directive.EventID)

	states, err := rig.hub.inbox.store.ackStates([]string{"ev-status"}, "")
	if err != nil {
		t.Fatal(err)
	}
	state := states["ev-status"]
	if state.Obligation != obligationSettled || state.Fault != relayDroppedRetriesExhausted || state.Settle != "stale" {
		t.Fatalf("ack state = %+v, want settled/retries_exhausted/stale", state)
	}
	raw := []byte(`{"events":[{"event_id":"ev-status","lane":"lane-a","stage":"unconfirmed","acked":false,"obligation":"settled","fault":"retries_exhausted","settle":"stale"}]}`)
	var out bytes.Buffer
	if code := printInboxStatus(raw, &out, io.Discard, false); code != ExitOK {
		t.Fatalf("status exit = %d", code)
	}
	want := "event=ev-status lane=lane-a stage=unconfirmed reason=- machine=- pane=- obligation=settled fault=retries_exhausted settle=stale acked=no\n"
	if out.String() != want {
		t.Fatalf("status line = %q, want %q", out.String(), want)
	}
}

// The feed counters read the store: a transition moves its count once, and the
// hub data names the counts.
// Killed by: in-process counters lost at restart, or double-counting.
func TestObligationCountsFollowTheStore(t *testing.T) {
	rig := newT6384Rig(t)
	directive := rig.send("lane-a", "ev-counted")
	rig.pass(time.Minute)
	counts := func() hubObligationCounts {
		c, err := rig.hub.inbox.store.deliveryCounts(rig.t.Context())
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	if c := counts(); c.Open != 1 || c.Confirmed != 0 {
		t.Fatalf("after send counts = %+v, want one open", c)
	}
	rig.delivered(directive)
	rig.pass(2 * time.Minute)
	c := counts()
	if c.Open != 0 || c.Confirmed != 1 {
		t.Fatalf("after confirm counts = %+v, want one confirmed", c)
	}
	// A restart counts the same rows: nothing is held in process.
	path, clock, logs := rig.path, rig.clock, rig.logs
	rig.hub.Close()
	rig = &t6384Rig{t: t, fake: rig.fake, client: rig.client, path: path, clock: clock, logs: logs}
	rig.start()
	if c := counts(); c.Confirmed != 1 {
		t.Fatalf("after restart counts = %+v, want one confirmed", c)
	}
	if got := rig.hub.uiData().Hub.Obligations; got == nil || got.Confirmed != 1 {
		t.Fatalf("ui obligations = %+v, want one confirmed", got)
	}
}

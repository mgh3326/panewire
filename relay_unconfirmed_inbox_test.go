package panewire

// A relay row whose pane submission was not proven gets one labelled replay
// after a hub restart (#1002). When the lane inbox already shows the event to
// the receiver — delivered, read, acked or superseded — that replay is spent
// on nothing: the row and its marker retire without a pane injection.

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

type unconfirmedInboxRig struct {
	fake  *fakeHandoffkeep
	hk    *handoffkeepRelayClient
	path  string
	now   time.Time
	row   int64
	event hubJobEventPayload
	hub   *HubServer
}

// unconfirmedInboxStart sends one must-ack READY through the inbox, has the
// node report its submission unproven (which writes the marker), applies the
// inbox state, and returns the rig with the first hub closed.
func unconfirmedInboxStart(t *testing.T, state string) *unconfirmedInboxRig {
	t.Helper()
	fake, client, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	rig := &unconfirmedInboxRig{fake: fake, hk: client, path: filepath.Join(t.TempDir(), "inbox.sqlite3"), now: time.Now().UTC()}
	h, a := quietHub(t, rig.path, "default", client, &rig.now)
	rig.event = quietEvent("ev-unconfirmed", "READY review")
	rig.event.NotifyType = "ready"
	h.relayLaneEvent(rig.event, a)
	directive := holdReceive(t, a)
	rig.row = directive.EventID
	if !h.markRelayEventUnconfirmed(relayPending{eventID: rig.row, kind: "lane.event", event: rig.event}) {
		t.Fatal("the unconfirmed marker was not written")
	}
	store := h.inbox.store
	key := relayEventDedupeKey("lane.event", rig.event)
	var id int64
	if err := store.db.QueryRow(`SELECT id FROM lane_inbox`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	switch state {
	case "unread":
	case "read":
		if _, err := store.read(id, "lane-a", rig.now); err != nil {
			t.Fatal(err)
		}
	case "delivered":
		if err := store.recordDelivery(key, "", "", rig.now); err != nil {
			t.Fatal(err)
		}
	case "acked":
		if _, err := store.ack(id, "lane-a", false, rig.now); err != nil {
			t.Fatal(err)
		}
	case "superseded":
		if _, err := store.db.Exec(`UPDATE lane_inbox SET superseded_at=5`); err != nil {
			t.Fatal(err)
		}
	case "auto-read-interrupt":
		// A plain interrupt is stamped read at insert; that is not the
		// receiver reading it, so the replay is still owed.
		if _, err := store.db.Exec(`UPDATE lane_inbox SET must_ack=0,class='interrupt',read_at=5`); err != nil {
			t.Fatal(err)
		}
	case "absent":
		if _, err := store.db.Exec(`DELETE FROM lane_inbox`); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("unknown inbox state", state)
	}
	h.Close()
	rig.hub = nil
	t.Cleanup(func() {
		if rig.hub != nil {
			rig.hub.Close()
		}
	})
	return rig
}

// restart is what a deploy does: a fresh hub over the same inbox and the same
// handoffkeep, then the startup replay and the hello replay.
func (rig *unconfirmedInboxRig) restart(t *testing.T) (*HubServer, *hubAgent) {
	t.Helper()
	h, a := quietHub(t, rig.path, "default", rig.hk, &rig.now)
	rig.hub = h
	h.replayUndeliveredRelayEvents(context.Background())
	h.replayUndeliveredLaneEvents(context.Background())
	return h, a
}

func (rig *unconfirmedInboxRig) markerRow(t *testing.T) int64 {
	t.Helper()
	rig.fake.mu.Lock()
	defer rig.fake.mu.Unlock()
	for _, row := range rig.fake.rows {
		if row.OwnerLane == relayUnconfirmedMarkLane {
			return row.ID
		}
	}
	t.Fatal("no unconfirmed marker row")
	return 0
}

func (rig *unconfirmedInboxRig) retiredWrites(id int64) int {
	rig.fake.mu.Lock()
	defer rig.fake.mu.Unlock()
	n := 0
	for _, call := range rig.fake.calls {
		if call.Method == "POST" && strings.Contains(call.Path, "/"+strconv.FormatInt(id, 10)+"/delivered") && strings.HasPrefix(asString(call.Body["pane"]), relayReplayRetiredPane) {
			n++
		}
	}
	return n
}

func TestUnconfirmedReplaySkipsEventTheInboxAlreadyDelivered(t *testing.T) {
	for _, state := range []string{"read", "delivered", "acked", "superseded"} {
		t.Run(state, func(t *testing.T) {
			rig := unconfirmedInboxStart(t, state)
			h, a := rig.restart(t)
			if len(a.relays) != 0 {
				t.Fatalf("a row the inbox shows as %s was injected: %q", state, (<-a.relays).Text)
			}
			if got := rig.fake.deliveredToFor(rig.row); got != "hub/"+relayReplayRetiredPane+relayUnconfirmedInboxRetireReason {
				t.Fatalf("row delivered_to = %q", got)
			}
			if got := rig.fake.deliveredToFor(rig.markerRow(t)); got != "hub/"+relayReplayRetiredPane+relayUnconfirmedMarkRetireReason {
				t.Fatalf("marker delivered_to = %q", got)
			}
			// A second restart finds nothing left to replay or retire.
			h.replayUndeliveredRelayEvents(context.Background())
			h.replayUndeliveredLaneEvents(context.Background())
			if len(a.relays) != 0 || rig.retiredWrites(rig.row) != 1 {
				t.Fatalf("second walk acted: injects=%d retire writes=%d", len(a.relays), rig.retiredWrites(rig.row))
			}
		})
	}
}

func TestUnconfirmedReplayStillReplaysOnceWhenTheInboxHoldsNothingDelivered(t *testing.T) {
	for _, state := range []string{"unread", "auto-read-interrupt", "absent"} {
		t.Run(state, func(t *testing.T) {
			rig := unconfirmedInboxStart(t, state)
			h, a := rig.restart(t)
			if len(a.relays) != 1 {
				t.Fatalf("injections = %d, want the one labelled replay", len(a.relays))
			}
			directive := <-a.relays
			if !strings.HasPrefix(directive.Text, "[replayed after hub restart - first sent ") {
				t.Fatalf("replay carries no label: %q", directive.Text)
			}
			if got := rig.fake.deliveredToFor(rig.row); got != "hub/"+relayReplayRetiredPane+relayUnconfirmedRetireReason {
				t.Fatalf("row delivered_to = %q", got)
			}
			h.replayUndeliveredRelayEvents(context.Background())
			h.replayUndeliveredLaneEvents(context.Background())
			if len(a.relays) != 0 {
				t.Fatal("the row was replayed a second time")
			}
		})
	}
}

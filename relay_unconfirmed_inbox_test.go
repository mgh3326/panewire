package panewire

// Inbox evidence settles an unconfirmed obligation without a pane injection.

import (
	"context"
	"path/filepath"
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
// node report its submission unproven, applies the
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
		t.Fatal("the retry was not scheduled")
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
	h.reconcileObligations(t.Context())
	return h, a
}

func wantUnconfirmedSettlement(t *testing.T, h *HubServer, fake *fakeHandoffkeep, id int64, reason string) {
	t.Helper()
	for _, o := range t6384Obligations(t, h.inbox.store) {
		if o.RelayID == id {
			if o.State != obligationSettled || o.SettleReason != reason || fake.deliveredToFor(id) != "" {
				t.Fatalf("obligation=%+v, durable destination=%q", o, fake.deliveredToFor(id))
			}
			return
		}
	}
	t.Fatalf("missing obligation for row %d", id)
}

func TestUnconfirmedReplaySkipsEventTheInboxAlreadyDelivered(t *testing.T) {
	for _, state := range []string{"read", "delivered", "acked", "superseded"} {
		t.Run(state, func(t *testing.T) {
			rig := unconfirmedInboxStart(t, state)
			h, a := rig.restart(t)
			if len(a.relays) != 0 {
				t.Fatalf("a row the inbox shows as %s was injected: %q", state, (<-a.relays).Text)
			}
			wantUnconfirmedSettlement(t, h, rig.fake, rig.row, obligationSettledInbox)
			h.replayUndeliveredRelayEvents(t.Context())
			h.reconcileObligations(t.Context())
			if len(a.relays) != 0 {
				t.Fatal("second pass injected a settled row")
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
			if got := rig.fake.deliveredToFor(rig.row); got != "" {
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

// exerciseUnconfirmedPass sets up the same durable fault as a live report,
// using a pre-existing relay row fixture.
func exerciseUnconfirmedPass(t *testing.T, h *HubServer, record handoffkeepRelayEvent) {
	t.Helper()
	event := relayEventFromRecord(record)
	o := newDeliveryObligation{key: relayEventDedupeKey(record.Kind, event), relayID: record.ID, kind: record.Kind, lane: record.OwnerLane, attempts: record.Attempts}
	if err := h.inbox.store.flagObligationRetry(t.Context(), o, obligationFaultUnconfirmed, "", h.now().UTC()); err != nil {
		t.Fatal(err)
	}
	opened, err := h.inbox.store.retryObligationForKey(t.Context(), o.key)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.classifyObligation(t.Context(), opened, record, h.now().UTC()); err != nil {
		t.Fatal(err)
	}
}

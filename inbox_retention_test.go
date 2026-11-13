package panewire

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestInboxRetentionKeepsUnreadAndReplayEvidence(t *testing.T) {
	for _, backend := range laneInboxBackends() {
		t.Run(backend.name, func(t *testing.T) {
			s := backend.fresh(t)()
			now := time.Now().UTC()
			old := now.Add(-30 * 24 * time.Hour)
			add := func(id, lane, class, notify string, at time.Time) laneInboxItem {
				t.Helper()
				e := quietEvent(id, id)
				e.NotifyType = notify
				item, err := s.put(id, lane, "lane.event", class, e, at, 0, 0)
				if err != nil {
					t.Fatal(err)
				}
				return item
			}
			ack := func(i laneInboxItem, at time.Time) {
				t.Helper()
				if _, err := s.ack(i.ID, i.Lane, false, at); err != nil {
					t.Fatal(err)
				}
			}
			add("unread", "lane-a", "inbox", "", old)
			add("must-ack", "lane-a", "interrupt", "ready", old)
			add("unconfirmed", "lane-a", "interrupt", "", old)
			ack(add("old-acked", "lane-a", "interrupt", "ready", old), old)
			ack(add("recent-ack", "lane-a", "interrupt", "ready", old), now)
			ack(add("recent-created", "lane-a", "interrupt", "ready", now), old)
			add("old-delivered", "lane-a", "interrupt", "", old)
			if err := s.recordDelivery("old-delivered", "", "", old); err != nil {
				t.Fatal(err)
			}
			add("recent-delivery", "lane-a", "interrupt", "", old)
			if err := s.recordDelivery("recent-delivery", "", "", now); err != nil {
				t.Fatal(err)
			}

			_, err := s.put("pending-held", "lane-a", "lane.event", "interrupt", quietEvent("pending-held", "held"), old, 0, 0, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.recordDelivery("pending-held", "", "", old); err != nil {
				t.Fatal(err)
			}
			_, err = s.put("pending-digest", "lane-a", "lane.event", "interrupt", quietEvent("pending-digest", "digest"), old, 0, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.recordDelivery("pending-digest", "", "", old); err != nil {
				t.Fatal(err)
			}
			// Count pruning must not destroy even recent creation/ack/delivery evidence.
			n, err := s.prune(now, inboxRetentionPolicy{Days: 14, PerLane: 1})
			if err != nil || n != 2 {
				t.Fatalf("removed=%d err=%v", n, err)
			}
			items, err := s.list("lane-a", false)
			if err != nil {
				t.Fatal(err)
			}
			if len(items) != 8 {
				t.Fatalf("retained=%+v", items)
			}
			for _, i := range items {
				if i.Event.EventID == "old-acked" || i.Event.EventID == "old-delivered" {
					t.Fatalf("old completed item survived: %+v", i)
				}
			}
		})
	}
}

func TestInboxRetentionAgeAndPerLane(t *testing.T) {
	for _, backend := range laneInboxBackends() {
		t.Run(backend.name, func(t *testing.T) {
			s := backend.fresh(t)()
			now := time.Now().UTC()
			for _, lane := range []string{"lane-a", "lane-b"} {
				for n, age := range []time.Duration{20 * 24 * time.Hour, 4 * 24 * time.Hour, 3 * 24 * time.Hour} {
					key := fmt.Sprintf("%s-%d", lane, n)
					at := now.Add(-age)
					i, err := s.put(key, lane, "lane.event", "inbox", quietEvent(key, key), at, 0, 0)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := s.ack(i.ID, lane, false, at); err != nil {
						t.Fatal(err)
					}
				}
			}
			n, err := s.prune(now, inboxRetentionPolicy{Days: 14, PerLane: 2})
			if err != nil || n != 2 {
				t.Fatalf("age removed=%d err=%v", n, err)
			}
			n, err = s.prune(now, inboxRetentionPolicy{Days: 14, PerLane: 1})
			if err != nil || n != 2 {
				t.Fatalf("count removed=%d err=%v", n, err)
			}
			for _, lane := range []string{"lane-a", "lane-b"} {
				items, err := s.list(lane, false)
				if err != nil || len(items) != 1 || items[0].Event.EventID != lane+"-2" {
					t.Fatalf("%s: %+v %v", lane, items, err)
				}
			}
			n, err = s.prune(now, inboxRetentionPolicy{Days: 14, PerLane: 1})
			if err != nil || n != 0 {
				t.Fatalf("repeat removed=%d err=%v", n, err)
			}
		})
	}
}

func TestInboxRetentionPolicy(t *testing.T) {
	for _, raw := range []string{"default: interrupt", "default: interrupt\nretention: {days: 0, per_lane: 0}"} {
		p, err := parseDeliveryPolicy([]byte(raw))
		if err != nil || p.Retention.Days != 30 || p.Retention.PerLane != 2000 {
			t.Fatalf("%+v %v", p, err)
		}
	}
	p, err := parseDeliveryPolicy([]byte("default: interrupt\nretention: {days: 2, per_lane: 7}"))
	if err != nil || p.Retention.Days != 2 || p.Retention.PerLane != 7 {
		t.Fatalf("%+v %v", p, err)
	}
	for _, r := range []string{"{days: -1}", "{per_lane: -1}", "{days: 36501}"} {
		if _, err := parseDeliveryPolicy([]byte("default: interrupt\nretention: " + r)); err == nil {
			t.Fatal("accepted", r)
		}
	}
	// An unknown key inside a known table is ignored with a warning, not refused.
	p, err = parseDeliveryPolicy([]byte("default: interrupt\nretention: {typo: 2}"))
	if err != nil || p.Retention.Days != 30 || p.Retention.PerLane != 2000 {
		t.Fatalf("typo key: %+v %v", p, err)
	}
	if len(p.ignoredKeys) != 1 || p.ignoredKeys[0] != "retention.typo" {
		t.Fatalf("ignoredKeys=%v, want [retention.typo]", p.ignoredKeys)
	}
}

func TestInboxRestartSuppressesCompletedAndReplaysUnconfirmedOnce(t *testing.T) {
	for _, completion := range []string{"acked", "delivered", "confirmed-inbox", "unconfirmed"} {
		t.Run(completion, func(t *testing.T) {
			fake, client, closeServer := newFakeHandoffkeep(t)
			t.Cleanup(closeServer)
			path := filepath.Join(t.TempDir(), "inbox.sqlite3")
			now := time.Now().UTC()
			h, a := quietHub(t, path, "default", client, &now)
			e := quietEvent("restart-proof", "READY review")
			e.NotifyType = "ready"
			h.relayLaneEvent(e, a)
			d := holdReceive(t, a)
			t650Send(t, h, "host-a", a, "relay.unconfirmed", relayAckPayload{JobID: d.JobID, Pane: d.Pane, OriginalEventID: d.EventID, Reason: "maybe_in_pane"})
			switch completion {
			case "acked":
				items, err := h.inbox.store.list("lane-a", false)
				if err != nil || len(items) != 1 {
					t.Fatalf("%+v %v", items, err)
				}
				if _, err := h.inbox.store.ack(items[0].ID, "lane-a", false, now); err != nil {
					t.Fatal(err)
				}
			case "delivered":
				t650Send(t, h, "host-a", a, "relay.delivered", relayAckPayload{JobID: d.JobID, Pane: d.Pane, OriginalEventID: d.EventID})
			case "confirmed-inbox":
				if err := h.inbox.store.recordDelivery(relayEventDedupeKey("lane.event", e), "", "", now); err != nil {
					t.Fatal(err)
				}
			}
			h.Close()
			h2, a2 := quietHub(t, path, "default", client, &now)
			if completion != "unconfirmed" {
				fake.mu.Lock()
				var original handoffkeepRelayEvent
				for _, row := range fake.rows {
					if row.ID == d.EventID {
						original = *row
					}
				}
				fake.mu.Unlock()
				h2.replayRelayEvent(original, relayReplaySourceStartup)
			}
			h2.replayUndeliveredRelayEvents(context.Background())
			h2.replayUndeliveredLaneEvents(context.Background())
			if completion == "unconfirmed" {
				got := holdReceive(t, a2)
				if !strings.HasPrefix(got.Text, "[replayed after hub restart") {
					t.Fatal(got)
				}
			}
			if len(a2.relays) != 0 {
				t.Fatalf("unexpected re-injection: %+v", <-a2.relays)
			}
			h2.Close()
			h3, a3 := quietHub(t, path, "default", client, &now)
			h3.replayUndeliveredRelayEvents(context.Background())
			h3.replayUndeliveredLaneEvents(context.Background())
			if len(a3.relays) != 0 {
				t.Fatal("second restart injected again")
			}
		})
	}
}

func TestInboxRetentionDoesNotReuseAckIDs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	s, err := openLaneInboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	old := now.Add(-30 * 24 * time.Hour)
	first, err := s.put("first", "lane-a", "lane.event", "inbox", quietEvent("first", "first"), old, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ack(first.ID, "lane-a", false, old); err != nil {
		t.Fatal(err)
	}
	if n, err := s.prune(now, inboxRetentionPolicy{Days: 14, PerLane: 2000}); err != nil || n != 1 {
		t.Fatalf("removed=%d err=%v", n, err)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	s, err = openLaneInboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.close() })
	second, err := s.put("second", "lane-a", "lane.event", "inbox", quietEvent("second", "second"), now, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if second.ID <= first.ID {
		t.Fatal("pruning reused an ackable ID")
	}
	if n, err := s.ack(first.ID, "lane-a", false, now); err != nil || n != 0 {
		t.Fatalf("stale ack changed new work: n=%d err=%v", n, err)
	}
}

func TestInboxReadMustAckWaitsForAck(t *testing.T) {
	for _, backend := range laneInboxBackends() {
		t.Run(backend.name, func(t *testing.T) {
			store := backend.fresh(t)()
			old := time.Now().UTC().Add(-30 * 24 * time.Hour)
			now := old.Add(30 * 24 * time.Hour)
			event := quietEvent("read-must-ack", "review this")
			event.NotifyType = "ready"
			key := relayEventDedupeKey("lane.event", event)
			item, err := store.put(key, "lane-a", "lane.event", "inbox", event, old, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.read(item.ID, "lane-a", old); err != nil {
				t.Fatal(err)
			}
			// The original was already owned by the inbox; replay must neither
			// inject it nor remove the receiver's outstanding acknowledgement.
			h, a := quietHub(t, filepath.Join(t.TempDir(), "hub.sqlite3"), "default", nil, &now)
			originalStore := h.inbox.store
			h.inbox.store = store
			t.Cleanup(func() { h.inbox.store = originalStore })
			record := handoffkeepRelayEvent{ID: 42, Kind: "lane.event", OwnerLane: event.OwnerLane, JobID: event.JobID, Epoch: 1, EventID: event.EventID, Text: event.Text, ReceivedAt: now.Format(time.RFC3339Nano)}
			for _, delivered := range []bool{false, true} {
				if delivered {
					if err := store.recordDelivery(key, "", "", old); err != nil {
						t.Fatal(err)
					}
				}
				if n, err := store.prune(now, inboxRetentionPolicy{Days: 14, PerLane: 1}); err != nil || n != 0 {
					t.Fatalf("read unacked pruned: n=%d err=%v", n, err)
				}
				h.replayRelayEvent(record, relayReplaySourceStartup)
				h.replayMarkedUnconfirmed(context.Background(), record, relayReplaySourceStartup, false)
				if len(a.relays) != 0 {
					t.Fatal("read must-ack item was re-injected")
				}
				unread, err := store.list("lane-a", true)
				if err != nil || len(unread) != 1 || unread[0].ReadAt == 0 || unread[0].AckedAt != 0 {
					t.Fatalf("outstanding ack lost: %+v %v", unread, err)
				}
			}
			if _, err := store.ack(item.ID, "lane-a", false, now); err != nil {
				t.Fatal(err)
			}
			h.replayRelayEvent(record, relayReplaySourceStartup)
			h.replayMarkedUnconfirmed(context.Background(), record, relayReplaySourceStartup, false)
			if len(a.relays) != 0 {
				t.Fatal("acked item was re-injected")
			}
			if n, err := store.prune(now, inboxRetentionPolicy{Days: 14, PerLane: 1}); err != nil || n != 0 {
				t.Fatalf("recent ack evidence pruned: n=%d err=%v", n, err)
			}
			if n, err := store.prune(now.Add(2*24*time.Hour), inboxRetentionPolicy{Days: 14, PerLane: 1}); err != nil || n != 1 {
				t.Fatalf("old acked item not pruned: n=%d err=%v", n, err)
			}
		})
	}
}

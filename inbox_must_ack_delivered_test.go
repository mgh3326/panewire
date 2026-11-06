package panewire

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// A must-ack interrupt that reached the receiver but was not acked stays
// unread. It is neither escalated as "N unread inbox items" nor injected a
// second time when the hub restarts and replays the original relay row.
func TestInboxDeliveredMustAckInterruptNoEscalationNoReplay(t *testing.T) {
	fake, client, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	now := time.Now().UTC()
	h, a := quietHub(t, path, "default", client, &now)
	e := quietEvent("delivered-must-ack", "READY review")
	e.NotifyType = "ready"
	h.relayLaneEvent(e, a)
	d := holdReceive(t, a)
	if err := h.inbox.store.recordDelivery(relayEventDedupeKey("lane.event", e), now); err != nil {
		t.Fatal(err)
	}
	var class string
	if err := h.inbox.store.db.QueryRow(`SELECT class FROM lane_inbox`).Scan(&class); err != nil || class != "interrupt" {
		t.Fatal("fixture must store a must-ack interrupt", class, err)
	}
	// An overdue deadline is what would make an inbox-class item escalate.
	if _, err := h.inbox.store.db.Exec(`UPDATE lane_inbox SET due_at=1`); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	var original handoffkeepRelayEvent
	for _, row := range fake.rows {
		if row.ID == d.EventID {
			original = *row
		}
	}
	fake.mu.Unlock()
	h.Close()

	now = now.Add(48 * time.Hour)
	h2, a2 := quietHub(t, path, "default", client, &now)
	h2.flushInbox()
	h2.flushInbox()
	h2.replayRelayEvent(original, relayReplaySourceStartup)
	h2.replayMarkedUnconfirmed(context.Background(), original, relayReplaySourceStartup, false)
	h2.replayUndeliveredRelayEvents(context.Background())
	h2.replayUndeliveredLaneEvents(context.Background())
	h2.flushInbox()
	select {
	case got := <-a2.relays:
		t.Fatalf("delivered must-ack item was escalated or re-injected: %q", got.Text)
	default:
	}
	if batches, err := h2.inbox.store.pendingBatches(); err != nil || len(batches) != 0 {
		t.Fatal("unexpected batch", batches, err)
	}
	unread, err := h2.inbox.store.list("lane-a", true)
	if err != nil || len(unread) != 1 || unread[0].AckedAt != 0 {
		t.Fatalf("outstanding ack lost: %+v %v", unread, err)
	}
}

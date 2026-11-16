package panewire

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// TestQuietBatchPageReadsOnChange guards hk 4910. The pending batch page used
// to be read on every 100 ms flush tick, and on the live hub that was eleven
// full scans a second of lane_inbox_batches and lane_inbox_batch_quarantine.
// The page names no time-based transition, so it is read only after a write
// that can change it, plus a bounded fallback for a writer the gate does not
// know about.
func TestQuietBatchPageReadsOnChange(t *testing.T) {
	const batchText = "1 interrupt events | lane lane-a | panewire inbox list --lane lane-a"

	t.Run("unchanged page is not re-read before the fallback", func(t *testing.T) {
		now := time.Now().UTC()
		_, hk, closeServer := newFakeHandoffkeep(t)
		defer closeServer()
		h, agent := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", hk, &now)
		h.flushInbox()
		if len(agent.relays) != 0 {
			t.Fatalf("empty store relayed %d events", len(agent.relays))
		}
		// A writer outside the store's change counter stands for a path this
		// gate has not been taught about: the batch must not be sent on a tick,
		// but the fallback must still find it.
		if _, err := h.inbox.store.db.Exec(`INSERT INTO lane_inbox_batches(id,lane,text) VALUES('offline','lane-a',?)`, batchText); err != nil {
			t.Fatal(err)
		}
		h.flushInbox()
		if len(agent.relays) != 0 {
			t.Fatal("page was re-read with nothing to report changed")
		}
		now = now.Add(inboxBatchPageFallback)
		h.flushInbox()
		if len(agent.relays) != 1 {
			t.Fatalf("fallback read sent %d batches, want 1", len(agent.relays))
		}
		batches, err := h.inbox.store.pendingBatches()
		if err != nil || len(batches) != 0 {
			t.Fatalf("pending batches after fallback read: %v err=%v", batches, err)
		}
	})

	t.Run("a full page drains without waiting for the fallback", func(t *testing.T) {
		const send = laneInboxBatchPageLimit + 50
		now := time.Now().UTC()
		fake, hk, closeServer := newFakeHandoffkeep(t)
		defer closeServer()
		h, agent := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", hk, &now)
		// A backlog past one page, for a lane no route names: the flush persists
		// each batch and marks it sent, so the page shrinks by what it took.
		for i := range send {
			if _, err := h.inbox.store.db.Exec(`INSERT INTO lane_inbox_batches(id,lane,text) VALUES(?,'lane-noroute','1 interrupt events | lane lane-noroute | panewire inbox list --lane lane-noroute')`, fmt.Sprintf("backlog-%03d", i)); err != nil {
				t.Fatal(err)
			}
		}
		// Two passes on a frozen clock: a full first page must reopen the gate
		// on its own, or the backlog waits for the 1 s fallback (hk 4910).
		h.flushInbox()
		h.flushInbox()
		fake.mu.Lock()
		persisted := len(fake.stored)
		fake.mu.Unlock()
		batches, err := h.inbox.store.pendingBatches()
		if err != nil || persisted != send || len(batches) != 0 || len(agent.relays) != 0 {
			t.Fatalf("drained persisted=%d/%d pending=%d relays=%d err=%v", persisted, send, len(batches), len(agent.relays), err)
		}
	})

	t.Run("an ack wakes the page to retire a spent batch", func(t *testing.T) {
		now := time.Now().UTC()
		fake, hk, closeServer := newFakeHandoffkeep(t)
		defer closeServer()
		h, agent := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", hk, &now)
		h.flushInbox()
		// One pending batch whose only item is still unread: the page counts it
		// active and would relay it.
		for _, q := range []string{
			`INSERT INTO lane_inbox_batches(id,lane,text) VALUES('spent','lane-a','` + batchText + `')`,
			`INSERT INTO lane_inbox(id,event_key,lane,kind,class,payload,created_at,digest_batch) VALUES(1,'spent-1','lane-a','lane.event','interrupt','{}',0,'spent')`,
		} {
			if _, err := h.inbox.store.db.Exec(q); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := h.inbox.store.ack(1, "lane-a", false, now); err != nil {
			t.Fatal(err)
		}
		// A frozen clock: only the ack's noted change can bring the page back.
		h.flushInbox()
		fake.mu.Lock()
		posts := len(fake.stored)
		fake.mu.Unlock()
		var sent int
		if err := h.inbox.store.db.QueryRow(`SELECT sent FROM lane_inbox_batches WHERE id='spent'`).Scan(&sent); err != nil {
			t.Fatal(err)
		}
		if posts != 0 || len(agent.relays) != 0 || sent != 1 {
			t.Fatalf("spent batch: posts=%d relays=%d sent=%d", posts, len(agent.relays), sent)
		}
	})
}

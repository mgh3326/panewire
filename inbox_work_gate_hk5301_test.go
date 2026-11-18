package panewire

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// countingConnector counts the statements and transactions a store sends, so a
// test can assert that an idle flush tick reaches the database zero times.
type countingConnector struct {
	inner driver.Driver
	dsn   string
	calls *atomic.Int64
}

func (c countingConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.inner.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return countingConn{Conn: conn, calls: c.calls}, nil
}
func (c countingConnector) Driver() driver.Driver { return c.inner }

type countingConn struct {
	driver.Conn
	calls *atomic.Int64
}

func (c countingConn) Prepare(q string) (driver.Stmt, error) {
	c.calls.Add(1)
	return c.Conn.Prepare(q)
}
func (c countingConn) Begin() (driver.Tx, error) {
	c.calls.Add(1)
	return c.Conn.Begin() //nolint:staticcheck // the wrapper deliberately hides ConnBeginTx so every call lands here
}

// countStoreCalls swaps the hub's inbox store onto a connection that counts
// every statement and transaction, and returns the counter.
func countStoreCalls(t *testing.T, h *HubServer, path string) *atomic.Int64 {
	t.Helper()
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	calls := &atomic.Int64{}
	db := sql.OpenDB(countingConnector{inner: raw.Driver(), dsn: path, calls: calls})
	raw.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA busy_timeout=5000`); err != nil {
		t.Fatal(err)
	}
	s := h.inbox.store
	old := s.db
	s.db = db
	old.Close()
	calls.Store(0) // the setup above is not the hub's traffic
	return calls
}

func batchRows(t *testing.T, h *HubServer) int {
	t.Helper()
	var n int
	if err := h.inbox.store.db.QueryRow(`SELECT count(*) FROM lane_inbox_batches`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func putDigestItem(t *testing.T, h *HubServer, id string, created time.Time, window time.Duration) {
	t.Helper()
	e := quietEvent(id, "message "+id)
	if _, err := h.inbox.store.put(relayEventDedupeKey("lane.event", e), "lane-a", "lane.event", "interrupt", e, created, 0, window); err != nil {
		t.Fatal(err)
	}
}

// TestInboxWorkGate guards hk 5301: the 100 ms flush tick builds held and
// digest batches only when a write was noted, a known deadline passed, a
// watched hold may have ended, or the safety fallback lapsed.
func TestInboxWorkGate(t *testing.T) {
	t.Run("an idle hub sends no query for many ticks", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "inbox.sqlite3")
		now := time.Now().UTC()
		h, _ := quietHub(t, path, "default", nil, &now)
		h.flushInbox() // primes the gate and reads the page once
		calls := countStoreCalls(t, h, path)
		for range 200 {
			h.flushInbox()
		}
		if got := calls.Load(); got != 0 {
			t.Fatalf("idle ticks sent %d store calls, want 0", got)
		}
		// Control: the counter sees a real pass once the safety fallback lapses.
		now = now.Add(inboxWorkFallback)
		h.flushInbox()
		if calls.Load() == 0 {
			t.Fatal("the counting wrapper saw no call from a pass that must query")
		}
	})

	t.Run("a new item builds its batch on the next tick", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "inbox.sqlite3")
		now := time.Now().UTC()
		h, _ := quietHub(t, path, "default", nil, &now)
		h.flushInbox()
		// Created a minute ago with a 1 s window: due at once, and no clock
		// movement can be what opens the gate. Only the insert's mark can.
		putDigestItem(t, h, "fresh", now.Add(-time.Minute), time.Second)
		h.flushInbox()
		if n := batchRows(t, h); n != 1 {
			t.Fatalf("batches after one tick = %d, want 1", n)
		}
	})

	t.Run("a digest due in 3 s is built at 3 s, not at the fallback", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "inbox.sqlite3")
		now := time.Now().UTC()
		h, _ := quietHub(t, path, "default", nil, &now)
		h.flushInbox()
		putDigestItem(t, h, "later", now, 3*time.Second)
		h.flushInbox() // the insert opens a pass; it records the 3 s deadline
		start := now
		now = start.Add(2900 * time.Millisecond)
		h.flushInbox()
		if n := batchRows(t, h); n != 0 {
			t.Fatalf("batch built %d ms early", 100)
		}
		now = start.Add(3 * time.Second)
		h.flushInbox()
		if n := batchRows(t, h); n != 1 {
			t.Fatalf("batches at the due time = %d, want 1", n)
		}
	})

	t.Run("a later item's earlier deadline does not keep the gate open", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "inbox.sqlite3")
		now := time.Now().UTC()
		h, _ := quietHub(t, path, "default", nil, &now)
		h.flushInbox()
		// A long window queued first, then a short one (a policy change between
		// them): the lane makes progress only at the first item's deadline, so
		// the second's expired deadline must not be what the gate waits for.
		putDigestItem(t, h, "long", now, 30*time.Second)
		putDigestItem(t, h, "short", now, time.Second)
		h.flushInbox()
		now = now.Add(2 * time.Second)
		h.flushInbox() // the short deadline passed: one pass, which finds nothing to build
		if n := batchRows(t, h); n != 0 {
			t.Fatalf("batch built before the first item's window ended: %d", n)
		}
		calls := countStoreCalls(t, h, path)
		for range 50 {
			h.flushInbox()
		}
		if got := calls.Load(); got != 0 {
			t.Fatalf("ticks before the lane's first deadline sent %d store calls, want 0", got)
		}
		now = now.Add(29 * time.Second)
		h.flushInbox()
		if n := batchRows(t, h); n != 1 {
			t.Fatalf("batches at the first item's deadline = %d, want 1", n)
		}
	})

	t.Run("an escalation due in 3 s is built at 3 s", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "inbox.sqlite3")
		now := time.Now().UTC()
		h, _ := quietHub(t, path, "default", nil, &now)
		h.flushInbox()
		e := quietEvent("esc", "message esc")
		if _, err := h.inbox.store.put(relayEventDedupeKey("lane.event", e), "lane-a", "lane.event", "inbox", e, now, 3*time.Second, 0); err != nil {
			t.Fatal(err)
		}
		h.flushInbox()
		start := now
		now = start.Add(2900 * time.Millisecond)
		h.flushInbox()
		if n := batchRows(t, h); n != 0 {
			t.Fatal("escalation built early")
		}
		now = start.Add(3 * time.Second)
		h.flushInbox()
		if n := batchRows(t, h); n != 1 {
			t.Fatalf("batches at the due time = %d, want 1", n)
		}
	})

	t.Run("a hold due in 3 s releases at 3 s across a restart", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "inbox.sqlite3")
		h, a, now, _, _ := holdHubWith(t, "3s", "", true, path, nil)
		start := *now
		holdNotice(t, h, a, "held", "ready", "READY held for review")
		h.flushInbox()
		if batchRows(t, h) != 0 {
			t.Fatal("held notice batched while its receiver works")
		}
		h.Close()

		clock := start.Add(time.Second)
		h2, a2, now2, _, _ := holdHubWith(t, "3s", "", true, path, &clock)
		refresh := func() { h2.nodes["host-a"].sessionSnapshot.receivedAt = *now2 }
		h2.flushInbox() // first pass after start rebuilds the due time
		*now2 = start.Add(2900 * time.Millisecond)
		refresh()
		h2.flushInbox()
		if len(a2.relays) != 0 || batchRows(t, h2) != 0 {
			t.Fatal("hold released before its deadline")
		}
		*now2 = start.Add(3 * time.Second)
		refresh()
		h2.flushInbox()
		if len(a2.relays) != 1 {
			t.Fatalf("hold released %d relays at its deadline, want 1", len(a2.relays))
		}
	})

	t.Run("a held lane that stops working releases on the next tick", func(t *testing.T) {
		h, a, now, _, _ := holdHub(t, "", true)
		holdNotice(t, h, a, "held", "ready", "READY held for review")
		h.flushInbox()
		for range 5 {
			h.flushInbox()
		}
		if len(a.relays) != 0 {
			t.Fatal("injected while working")
		}
		holdIdle(h, *now)
		h.flushInbox()
		if len(a.relays) != 1 {
			t.Fatalf("relays after the receiver went idle = %d, want 1", len(a.relays))
		}
	})
}

// TestInboxWorkWritersMarkTheGate lists every write that can create or change
// batch work and checks each one moves the change counter the gate reads. A
// path that forgets its mark would wait for the safety fallback.
func TestInboxWorkWritersMarkTheGate(t *testing.T) {
	setup := func(t *testing.T) (*HubServer, time.Time) {
		path := filepath.Join(t.TempDir(), "inbox.sqlite3")
		now := time.Now().UTC()
		h, _ := quietHub(t, path, "default", nil, &now)
		return h, now
	}
	moves := func(t *testing.T, name string, h *HubServer, write func()) {
		t.Helper()
		before := h.inbox.store.workVersion()
		write()
		if h.inbox.store.workVersion() == before {
			t.Fatalf("%s did not mark the inbox dirty", name)
		}
	}
	t.Run("insert", func(t *testing.T) {
		h, now := setup(t)
		moves(t, "insert", h, func() { putDigestItem(t, h, "one", now, time.Second) })
	})
	t.Run("insert with a deadline", func(t *testing.T) {
		h, now := setup(t)
		e := quietEvent("inbox", "message")
		moves(t, "inbox insert", h, func() {
			if _, err := h.inbox.store.put(relayEventDedupeKey("lane.event", e), "lane-a", "lane.event", "inbox", e, now, time.Minute, 0); err != nil {
				t.Fatal(err)
			}
		})
	})
	t.Run("insert with a hold", func(t *testing.T) {
		h, now := setup(t)
		e := quietEvent("held", "message")
		moves(t, "held insert", h, func() {
			if _, err := h.inbox.store.put(relayEventDedupeKey("lane.event", e), "lane-a", "lane.event", "interrupt", e, now, 0, 0, time.Minute); err != nil {
				t.Fatal(err)
			}
		})
	})
	t.Run("read", func(t *testing.T) {
		h, now := setup(t)
		e := quietEvent("inbox", "message")
		item, err := h.inbox.store.put(relayEventDedupeKey("lane.event", e), "lane-a", "lane.event", "inbox", e, now, time.Minute, 0)
		if err != nil {
			t.Fatal(err)
		}
		moves(t, "read", h, func() {
			if _, err := h.inbox.store.read(item.ID, "lane-a", now); err != nil {
				t.Fatal(err)
			}
		})
	})
	t.Run("ack", func(t *testing.T) {
		h, now := setup(t)
		putDigestItem(t, h, "one", now, time.Second)
		moves(t, "ack", h, func() {
			if _, err := h.inbox.store.ack(0, "lane-a", true, now); err != nil {
				t.Fatal(err)
			}
		})
	})
	t.Run("supersede", func(t *testing.T) {
		h, now := setup(t)
		// A superseding insert is an insert: the mark comes with the row.
		first := quietEvent("a", "message")
		first.Cause = "pr:1"
		if _, err := h.inbox.store.put(relayEventDedupeKey("lane.event", first), "lane-a", "lane.event", "interrupt", first, now, 0, time.Second); err != nil {
			t.Fatal(err)
		}
		next := quietEvent("b", "message")
		next.Cause = "pr:1"
		moves(t, "supersede", h, func() {
			if _, err := h.inbox.store.put(relayEventDedupeKey("lane.event", next), "lane-a", "lane.event", "interrupt", next, now, 0, time.Second); err != nil {
				t.Fatal(err)
			}
		})
	})
	t.Run("file as unread", func(t *testing.T) {
		h, now := setup(t)
		putDigestItem(t, h, "one", now, time.Minute)
		moves(t, "fileUnread", h, func() {
			if _, err := h.inbox.store.fileUnread(now, `lane=?`, "lane-a"); err != nil {
				t.Fatal(err)
			}
		})
	})
	t.Run("release batch claims", func(t *testing.T) {
		h, now := setup(t)
		putDigestItem(t, h, "one", now.Add(-time.Minute), time.Second)
		if err := h.inbox.store.makeBatches(now); err != nil {
			t.Fatal(err)
		}
		var batch string
		if err := h.inbox.store.db.QueryRow(`SELECT id FROM lane_inbox_batches`).Scan(&batch); err != nil {
			t.Fatal(err)
		}
		moves(t, "releaseBatchClaims", h, func() {
			if err := h.inbox.store.releaseBatchClaims(batch); err != nil {
				t.Fatal(err)
			}
		})
	})
}

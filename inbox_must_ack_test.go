package panewire

import (
	"bytes"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// inboxAckCLI runs `panewire inbox <args>` as the operator against hub.
func inboxAckCLI(t *testing.T, h *HubServer, args ...string) string {
	t.Helper()
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	token := filepath.Join(t.TempDir(), "op.env")
	if err := os.WriteFile(token, []byte("HUB_MACHINE_ID=operator\nHUB_TOKEN=op\n"), 0600); err != nil {
		t.Fatal(err)
	}
	deps := hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true}
	var out, errOut bytes.Buffer
	args = append(args, "--hub-url", server.URL, "--hub-token-env", token)
	if code := runInboxCLI(args, &out, &errOut, deps); code != ExitOK {
		t.Fatalf("inbox %v: code=%d err=%q", args, code, errOut.String())
	}
	return out.String()
}

func laneUnread(t *testing.T, h *HubServer) int {
	t.Helper()
	counts, err := h.inbox.store.counts("lane-a")
	if err != nil {
		t.Fatal(err)
	}
	return counts["lane-a"]
}

// A READY, an ESC decision, a BLOCKED notice or a findings delivery is typed
// into the receiving pane and still stays unread — through injection and an
// explicit read — until the session runs `panewire inbox ack`. Runs on both
// inbox backends.
func TestMustAckTypesStayUnreadUntilInboxAck(t *testing.T) {
	forEachHubInboxStorage(t, testMustAckTypesStayUnreadUntilInboxAck)
}

func testMustAckTypesStayUnreadUntilInboxAck(t *testing.T, storage hubInboxStorage) {
	for _, typ := range []string{"ready", "decision", "blocked", "findings"} {
		t.Run(typ, func(t *testing.T) {
			_, hk, closeServer := newFakeHandoffkeep(t)
			defer closeServer()
			now := time.Now()
			if storage.path != "" {
				storage.path = filepath.Join(t.TempDir(), "inbox.sqlite3")
			}
			h, agent := quietHubOn(t, storage, "default", hk, &now)
			e := quietEvent("ev-"+typ, "[worker] "+strings.ToUpper(typ)+" | pr 7")
			e.NotifyType = typ
			r := h.relayLaneEvent(e, agent)
			if r.ID == 0 || !r.Routed {
				t.Fatalf("not routed: %+v", r)
			}
			if len(agent.relays) != 1 {
				t.Fatalf("%s injected %d times, want 1", typ, len(agent.relays))
			}
			if got := laneUnread(t, h); got != 1 {
				t.Fatalf("after injection unread=%d, want 1", got)
			}
			first, err := h.inbox.store.firstUnread(t.Context(), "lane-a")
			if err != nil || first == nil || first.Event.EventID != "ev-"+typ || !first.MustAck || first.ReadAt != 0 {
				t.Fatalf("inbox wait would not return it: %+v %v", first, err)
			}
			id := fmt.Sprint(first.ID)
			listed := inboxAckCLI(t, h, "list", "--lane", "lane-a", "--unread")
			if !strings.Contains(listed, "\tinterrupt\tunacked\t") {
				t.Fatalf("list --unread = %q", listed)
			}
			read := inboxAckCLI(t, h, "read", id, "--lane", "lane-a")
			if !strings.Contains(read, "pr 7") {
				t.Fatalf("read = %q", read)
			}
			if got := laneUnread(t, h); got != 1 {
				t.Fatalf("read retired a must-ack item: unread=%d", got)
			}
			if listed := inboxAckCLI(t, h, "list", "--lane", "lane-a"); !strings.Contains(listed, "\tunacked\t") {
				t.Fatalf("list after read = %q", listed)
			}
			inboxAckCLI(t, h, "ack", id, "--lane", "lane-a")
			if got := laneUnread(t, h); got != 0 {
				t.Fatalf("after inbox ack unread=%d, want 0", got)
			}
			if listed := inboxAckCLI(t, h, "list", "--lane", "lane-a"); !strings.Contains(listed, "\tacked\t") {
				t.Fatalf("list after ack = %q", listed)
			}
		})
	}
}

// Idle-wake keeps its behaviour: a busy receiver gets it as an inbox item that
// a read retires, an idle receiver gets it injected and stored read. Runs on
// both inbox backends.
func TestIdleWakeKeepsReadOnDelivery(t *testing.T) {
	forEachHubInboxStorage(t, testIdleWakeKeepsReadOnDelivery)
}

func testIdleWakeKeepsReadOnDelivery(t *testing.T, storage hubInboxStorage) {
	wake := func(id string) hubJobEventPayload {
		e := quietEvent(id, idleWakeRouteText(idleWakeRouteRequest{EventID: id, Pane: "w2:p1", Label: "worker", State: "done", ChangedAt: time.Now().UTC().Format(time.RFC3339Nano), StateChangeSeq: 1}, idleWakeOwnerResolution{Lane: "lane-a"}))
		return e
	}
	_, hk, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	now := time.Now()
	h, agent := quietHubOn(t, storage, "default", hk, &now)
	r := h.relayLaneEvent(wake("wake-busy"), agent)
	if r.ID == 0 || len(agent.relays) != 0 {
		t.Fatalf("busy receiver: %+v relays=%d", r, len(agent.relays))
	}
	if got := laneUnread(t, h); got != 1 {
		t.Fatalf("busy idle-wake unread=%d, want 1", got)
	}
	if _, err := h.inbox.store.read(r.ID, "lane-a", now); err != nil {
		t.Fatal(err)
	}
	if got := laneUnread(t, h); got != 0 {
		t.Fatalf("read did not retire idle-wake: unread=%d", got)
	}

	setReceiverSession(h, "host-a", receiverSession("w1:p1", "idle", "claude"), now)
	r = h.relayLaneEvent(wake("wake-idle"), agent)
	if r.ID == 0 || len(agent.relays) != 1 {
		t.Fatalf("idle receiver: %+v relays=%d", r, len(agent.relays))
	}
	items, err := h.inbox.store.list("lane-a", false)
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range items {
		if i.Event.EventID == "wake-idle" && (i.Class != "interrupt" || i.ReadAt == 0 || i.MustAck) {
			t.Fatalf("idle-wake stored %+v, want an interrupt read on insert", i)
		}
	}
	if got := laneUnread(t, h); got != 0 {
		t.Fatalf("injected idle-wake left unread=%d, want 0", got)
	}
}

// A store created before must_ack existed gains the column; its rows keep
// their recorded read state. SQLite only: no Postgres inbox predates must_ack
// (migration 0001 creates the column).
func TestLaneInboxMustAckMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	s, err := openLaneInboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`DROP INDEX lane_inbox_must_ack`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`ALTER TABLE lane_inbox DROP COLUMN must_ack`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO lane_inbox(event_key,lane,kind,class,payload,created_at,read_at) VALUES('old','lane-a','lane.event','interrupt','{"notify_type":"ready"}',1,1)`); err != nil {
		t.Fatal(err)
	}
	s.db.Close()
	s, err = openLaneInboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.db.Close()
	items, err := s.list("lane-a", true)
	if err != nil || len(items) != 0 {
		t.Fatalf("legacy read interrupt resurfaced as unread: %+v %v", items, err)
	}
}

// A must-ack insert wakes `inbox wait` waiters, and a digest window never
// folds a must-ack event into a digest: it is injected itself. Runs on both
// inbox backends.
func TestMustAckWakesWaitersAndSkipsDigest(t *testing.T) {
	forEachHubInboxStorage(t, testMustAckWakesWaitersAndSkipsDigest)
}

func testMustAckWakesWaitersAndSkipsDigest(t *testing.T, storage hubInboxStorage) {
	_, hk, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	now := time.Now()
	policy := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(policy, []byte("default: interrupt\ndigest_window: 10s\n"), 0600); err != nil {
		t.Fatal(err)
	}
	h, agent := quietHubOn(t, storage, policy, hk, &now)
	wake := h.inbox.store.waitChan()
	e := quietEvent("ev-findings", "review complete, 3 findings")
	e.NotifyType = "findings"
	if r := h.relayLaneEvent(e, agent); !r.Routed {
		t.Fatalf("not routed: %+v", r)
	}
	select {
	case <-wake:
	case <-time.After(time.Second):
		t.Fatal("must-ack insert did not wake inbox waiters")
	}
	if len(agent.relays) != 1 {
		t.Fatalf("must-ack event folded into a digest: relays=%d", len(agent.relays))
	}
	items, err := h.inbox.store.list("lane-a", true)
	if err != nil || len(items) != 1 || items[0].DigestAt != 0 {
		t.Fatalf("%+v %v", items, err)
	}
}

// put itself never digests a must-ack interrupt, whatever window a caller
// passes (replay and other paths reach put without receiveInbox's guard);
// other interrupts keep their digest. Runs on both inbox backends.
func TestPutNeverDigestsMustAck(t *testing.T) {
	forEachLaneInboxBackend(t, testPutNeverDigestsMustAck)
}

func testPutNeverDigestsMustAck(t *testing.T, open func() *laneInboxStore) {
	s := open()
	now := time.Now()
	for _, typ := range []string{"ready", "decision", "blocked", "findings", "", "order"} {
		e := quietEvent("put-"+typ, "text")
		e.NotifyType = typ
		item, err := s.put("key-"+typ, "lane-a", "lane.event", "interrupt", e, now, 0, 10*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if must := mustAckNotifyTypes[typ]; must != (item.DigestAt == 0) || item.MustAck != must {
			t.Fatalf("type %q: must_ack=%v digest_at=%d", typ, item.MustAck, item.DigestAt)
		}
	}
}

package panewire

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// hk 4269 (operator decision 151): an order's priority decides whether it
// waits for the lane's next idle moment. The fixture lane opts into
// hold_while_working, so a normal or low order sent while the pane works is
// held into the digest batch; stop, hold, urgent and an unmarked order reach
// the pane at once.

func holdOrder(t *testing.T, h *HubServer, a *hubAgent, id, priority, text string) {
	t.Helper()
	e := quietEvent(id, text)
	e.NotifyType = "order"
	e.Priority = priority
	if r := h.relayLaneEvent(e, a); r.ID == 0 || r.PersistFailed {
		t.Fatalf("accept: %+v", r)
	}
}

func TestOrderDigestNormalAndLowHeldUntilIdle_t4269(t *testing.T) {
	h, a, now, _, _ := holdHub(t, "", true)
	holdOrder(t, h, a, "normal", "normal", "rebase when you can")
	holdOrder(t, h, a, "low", "low", "tidy the notes")
	h.flushInbox()
	if len(a.relays) != 0 {
		t.Fatalf("a normal or low order was injected while the pane worked: %+v", <-a.relays)
	}
	holdIdle(h, *now)
	h.flushInbox()
	d := holdReceive(t, a)
	if !strings.HasPrefix(d.Text, "[batch 2: ORDER 2]") || !strings.Contains(d.Text, "rebase when you can") || !strings.Contains(d.Text, "tidy the notes") {
		t.Fatalf("digest = %q", d.Text)
	}
	if len(a.relays) != 0 {
		t.Fatal("the orders were delivered more than once")
	}
}

func TestOrderDigestReleasedAtMaxHold_t4269(t *testing.T) {
	h, a, now, _, _ := holdHub(t, "", true)
	holdOrder(t, h, a, "cap", "low", "still busy")
	*now = now.Add(10 * time.Minute)
	h.nodes["host-a"].sessionSnapshot.receivedAt = *now
	h.flushInbox()
	if d := holdReceive(t, a); !strings.Contains(d.Text, "still busy") {
		t.Fatal(d.Text)
	}
}

func TestOrderDigestInterruptsStopHoldUrgentAndUnmarked_t4269(t *testing.T) {
	for _, priority := range []string{"", "stop", "hold", "urgent"} {
		t.Run("priority="+priority, func(t *testing.T) {
			h, a, _, _, _ := holdHub(t, "", true)
			holdOrder(t, h, a, "now", priority, "do this now")
			d := holdReceive(t, a)
			if !strings.Contains(d.Text, "do this now") || strings.HasPrefix(d.Text, "[batch") {
				t.Fatalf("injected %q, want the order itself", d.Text)
			}
		})
	}
}

// A worker lane (one whose route names a parent) gets an order at once
// (hk 3276) unless its sender marked it normal or low.
func TestOrderDigestWorkerLane_t4269(t *testing.T) {
	for _, tc := range []struct {
		priority string
		held     bool
	}{{"", false}, {"urgent", false}, {"normal", true}, {"low", true}} {
		t.Run("priority="+tc.priority, func(t *testing.T) {
			h, a, now, _, _ := holdHub(t, "", true)
			lanes := `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1","parent":"lane-b"},"lane-b":{"machine":"host-b","pane":"w2:p1"}}}`
			if err := os.WriteFile(h.reportRelayPath, []byte(lanes), 0o600); err != nil {
				t.Fatal(err)
			}
			holdOrder(t, h, a, "worker", tc.priority, "follow-up")
			if tc.held {
				if len(a.relays) != 0 {
					t.Fatalf("a %s order reached a working worker: %+v", tc.priority, <-a.relays)
				}
				holdIdle(h, *now)
				h.flushInbox()
			}
			d := holdReceive(t, a)
			if !strings.Contains(d.Text, "follow-up") {
				t.Fatal(d.Text)
			}
		})
	}
}

// A lane that never opted into hold_while_working keeps its orders as they
// were: every one is injected at once, whatever its priority.
func TestOrderDigestNeedsHoldPolicy_t4269(t *testing.T) {
	policy := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(policy, []byte("default: interrupt\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	hk := func() *handoffkeepRelayClient { h, _, _, _, _ := holdHub(t, "", true); return h.handoffkeep }()
	h, a := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), policy, hk, &now)
	holdOrder(t, h, a, "plain", "normal", "no hold policy")
	holdReceive(t, a)
}

// A held order is durable: a hub restart between the hold and the idle moment
// still delivers it.
func TestOrderDigestSurvivesRestart_t4269(t *testing.T) {
	h, a, now, _, policy := holdHub(t, "", true)
	holdOrder(t, h, a, "restart", "normal", "survives restart")
	path := ""
	if err := h.inbox.store.db.QueryRow(`SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&path); err != nil {
		t.Fatal(err)
	}
	hk := h.handoffkeep
	h.Close()
	h2, a2 := quietHub(t, path, policy, hk, now)
	holdIdle(h2, *now)
	h2.flushInbox()
	if d := holdReceive(t, a2); !strings.Contains(d.Text, "survives restart") {
		t.Fatal(d.Text)
	}
}

// A lane that is retired (its route leaves the lanes file) while an order
// is held keeps the order in its inbox: it is recorded, never dropped.
func TestOrderDigestSurvivesLaneRetire_t4269(t *testing.T) {
	h, a, now, _, _ := holdHub(t, "", true)
	holdOrder(t, h, a, "retire", "low", "outlives the lane")
	if err := os.WriteFile(h.reportRelayPath, []byte(`{"lanes":{"lane-b":{"machine":"host-b","pane":"w2:p1"}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(11 * time.Minute)
	h.flushInbox()
	for len(a.relays) > 0 {
		<-a.relays // delivered if the lane were still routable; the row below is the record either way
	}
	items, err := h.inbox.store.list("lane-a", false)
	if err != nil {
		t.Fatal(err)
	}
	var kept *laneInboxItem
	for n := range items {
		if items[n].Event.EventID == "retire" {
			kept = &items[n]
		}
	}
	if kept == nil || kept.Event.Text != "outlives the lane" || kept.Event.Priority != "low" || kept.SupersededAt != 0 || kept.AckedAt != 0 {
		t.Fatalf("the held order was dropped: %+v", items)
	}
	if kept.HoldUntil != 0 && kept.DigestBatch == "" {
		t.Fatalf("the held order is neither batched nor delivered: %+v", kept)
	}
}

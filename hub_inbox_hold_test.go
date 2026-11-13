package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func holdHub(t *testing.T, thresholds string, docOK bool) (*HubServer, *hubAgent, *time.Time, *[]string, string) {
	t.Helper()
	fake := &fakeHandoffkeep{nextID: 100, status: 201, rows: map[string]*handoffkeepRelayEvent{}}
	docs := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/documents/inbox/") {
			var body struct {
				Body string `json:"body"`
				Kind string `json:"kind"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body.Kind != "handoff" {
				t.Error("document kind must use hk handoff vocabulary")
				w.WriteHeader(400)
				return
			}
			docs = append(docs, body.Body)
			if !docOK {
				w.WriteHeader(503)
				return
			}
			w.WriteHeader(200)
			return
		}
		fake.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	hk, err := newHandoffkeepRelayClient(hubHandoffkeepEnv{URL: server.URL, Token: "fixture-token"}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	policy := filepath.Join(t.TempDir(), "policy.yaml")
	raw := "default: interrupt\nlanes:\n  lane-a:\n    hold_while_working:\n      max_hold: 10m\n" + thresholds
	if err := os.WriteFile(policy, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	h, a := quietHub(t, path, policy, hk, &now)
	return h, a, &now, &docs, policy
}
func holdNotice(t *testing.T, h *HubServer, a *hubAgent, id, kind, text string) hubJobEventPayload {
	t.Helper()
	e := quietEvent(id, text)
	e.NotifyType = kind
	r := h.relayLaneEvent(e, a)
	if r.ID == 0 || r.PersistFailed {
		t.Fatalf("accept: %+v", r)
	}
	return e
}
func holdIdle(h *HubServer, now time.Time) {
	h.nodes["host-a"].sessionSnapshot.sessions[0].Status = "idle"
	h.nodes["host-a"].sessionSnapshot.receivedAt = now
}
func holdReceive(t *testing.T, a *hubAgent) hubRelayInjectEvent {
	t.Helper()
	select {
	case d := <-a.relays:
		return d
	default:
		t.Fatal("missing injection")
		return hubRelayInjectEvent{}
	}
}
func TestHoldWhileWorkingReleasedOnIdle(t *testing.T) {
	h, a, now, _, _ := holdHub(t, "", true)
	holdNotice(t, h, a, "first", "ready", "READY ready for review")
	holdNotice(t, h, a, "second", "findings", "FINDINGS review complete")
	h.flushInbox()
	if len(a.relays) != 0 {
		t.Fatal("injected while working")
	}
	holdIdle(h, *now)
	h.flushInbox()
	d := holdReceive(t, a)
	if !strings.HasPrefix(d.Text, "[batch 2: FINDINGS 1, READY 1]") || !strings.Contains(d.Text, "READY ready for review") || len(a.relays) != 0 {
		t.Fatal(d)
	}
	wire, _ := json.Marshal(d)
	if _, ok := parseHubOutbound(wire); !ok {
		t.Fatalf("node rejected %s", wire)
	}
}
func TestHoldReleasedAtMaxHold(t *testing.T) {
	h, a, now, _, _ := holdHub(t, "", true)
	holdNotice(t, h, a, "cap", "ready", "READY cap")
	*now = now.Add(10 * time.Minute)
	h.nodes["host-a"].sessionSnapshot.receivedAt = *now
	h.flushInbox()
	holdReceive(t, a)
}
func TestHoldNeverHeldKinds(t *testing.T) {
	for _, tc := range []struct{ kind, text string }{{"decision", "choose"}, {"order", "do this"}, {"", "BLOCKER"}, {"", "URGENT"}, {"", "ESC"}} {
		t.Run(tc.kind+tc.text, func(t *testing.T) {
			h, a, _, _, _ := holdHub(t, "", true)
			holdNotice(t, h, a, "immediate", tc.kind, tc.text)
			holdReceive(t, a)
		})
	}
	for _, kind := range []string{"job.escalate", "job.lost", "job.revoked"} {
		t.Run(kind, func(t *testing.T) {
			h, a, _, _, _ := holdHub(t, "", true)
			e := quietEvent("critical", "act now")
			e.DeliveryClass = "interrupt"
			if _, handled := h.receiveInbox(kind, &e, a, "", false); handled {
				t.Fatal("critical job event held")
			}
			items, err := h.inbox.store.list("", false)
			if err != nil || len(items) != 1 || items[0].HoldUntil != 0 {
				t.Fatal(items, err)
			}
		})
	}
}
func TestHoldUnknownStateNotHeld(t *testing.T) {
	for _, state := range []string{"missing", "stale", "unknown", "truncated"} {
		t.Run(state, func(t *testing.T) {
			h, a, now, _, _ := holdHub(t, "", true)
			snap := h.nodes["host-a"].sessionSnapshot
			switch state {
			case "missing":
				h.nodes["host-a"].sessionSnapshot = nil
			case "stale":
				snap.receivedAt = now.Add(-3 * time.Minute)
			case "unknown":
				snap.sessions[0].Status = ""
			case "truncated":
				snap.truncated = true
			}
			holdNotice(t, h, a, "unknown", "ready", "READY unknown")
			holdReceive(t, a)
		})
	}
}
func TestHoldReplayDeliveredAndAckedDropped(t *testing.T) {
	h, a, now, _, _ := holdHub(t, "", true)
	e := holdNotice(t, h, a, "acked", "ready", "READY acked")
	items, _ := h.inbox.store.list("lane-a", false)
	if _, err := h.inbox.store.ack(items[0].ID, "lane-a", false, *now); err != nil {
		t.Fatal(err)
	}
	h.relayLaneEvent(e, a)
	// Existing delivered relay rows must not enter the held queue on replay.
	record := handoffkeepRelayEvent{ID: 999, Kind: "lane.event", OwnerLane: "lane-a", EventID: "delivered", Text: "READY delivered", DeliveredAt: now.Format(time.RFC3339)}
	h.replayRelayEvent(record, "startup")
	holdNotice(t, h, a, "new", "ready", "READY new")
	holdIdle(h, *now)
	h.flushInbox()
	d := holdReceive(t, a)
	if !strings.HasPrefix(d.Text, "[batch 1: READY 1]") || strings.Contains(d.Text, "acked") || strings.Contains(d.Text, "delivered") {
		t.Fatal(d.Text)
	}
}
func TestHoldHandoffAboveByteThreshold(t *testing.T) {
	testHoldHandoff(t, "      handoff_bytes: 100\n      handoff_items: 20\n", 1, true)
}
func TestHoldHandoffAboveItemThreshold(t *testing.T) {
	testHoldHandoff(t, "      handoff_bytes: 10000\n", 4, true)
}
func TestHoldHandoffFailureFallsBackToFullText(t *testing.T) {
	testHoldHandoff(t, "      handoff_bytes: 100\n", 4, false)
}
func testHoldHandoff(t *testing.T, thresholds string, n int, success bool) {
	h, a, now, docs, _ := holdHub(t, thresholds, success)
	for i := 0; i < n; i++ {
		holdNotice(t, h, a, fmt.Sprint(i), "ready", fmt.Sprintf("READY item %d %s", i, strings.Repeat("body", 150)))
	}
	holdIdle(h, *now)
	h.flushInbox()
	d := holdReceive(t, a)
	if len(*docs) != 1 {
		t.Fatal("missing document write")
	}
	for i := 0; i < n; i++ {
		if !strings.Contains((*docs)[0], fmt.Sprintf("READY item %d", i)) {
			t.Fatal("lost original")
		}
	}
	if success {
		if !strings.Contains(d.Text, "hk:doc inbox/lane-a/") || len(d.Text) > 2048 {
			t.Fatal(d.Text)
		}
	} else {
		if strings.Contains(d.Text, "hk:doc") || !strings.Contains(d.Text, strings.Repeat("body", 150)) {
			t.Fatal(d.Text)
		}
	}
	wire, _ := json.Marshal(d)
	if _, ok := parseHubOutbound(wire); !ok {
		t.Fatal("node rejected batch")
	}
}
func TestHoldRestartAndReplayUsesFullBatch(t *testing.T) {
	h, a, now, _, policy := holdHub(t, "      handoff_bytes: 10000\n", false)
	holdNotice(t, h, a, "restart", "ready", "READY survives restart")
	path := ""
	if err := h.inbox.store.db.QueryRow(`SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&path); err != nil {
		t.Fatal(err)
	}
	hk := h.handoffkeep
	h.Close()
	h2, a2 := quietHub(t, path, policy, hk, now)
	holdIdle(h2, *now)
	h2.flushInbox()
	d := holdReceive(t, a2)
	record, found, err := hk.relayEvent(context.Background(), d.EventID)
	if err != nil || !found {
		t.Fatal(record, found, err)
	}
	h2.Close()
	h3, a3 := quietHub(t, path, policy, hk, now)
	h3.replayRelayEvent(record, "startup")
	replay := holdReceive(t, a3)
	if replay.Text != d.Text {
		t.Fatalf("replay lost full batch: %q", replay.Text)
	}
}
func TestHoldPolicyValidation(t *testing.T) {
	for _, settings := range []string{"max_hold: 0s", "max_hold: nope", "max_hold: 10m, handoff_bytes: -1", "max_hold: 10m, handoff_items: -1"} {
		if _, err := parseDeliveryPolicy([]byte("default: interrupt\nhold_while_working: {" + settings + "}\n")); err == nil {
			t.Fatal(settings)
		}
	}
}

func TestHoldKindsCoalesce(t *testing.T) {
	h, a, now, _, _ := holdHub(t, "      handoff_items: 10\n", true)
	for n, kind := range []string{"READY", "FINDINGS", "BLOCKED", "MERGED"} {
		holdNotice(t, h, a, fmt.Sprint(n), "", kind+" notice")
	}
	if len(a.relays) != 0 {
		t.Fatal("notice interrupted working receiver")
	}
	holdIdle(h, *now)
	h.flushInbox()
	d := holdReceive(t, a)
	if !strings.HasPrefix(d.Text, "[batch 4: BLOCKED 1, FINDINGS 1, MERGED 1, READY 1]") {
		t.Fatal(d.Text)
	}
}
func TestHoldTwentyFiveItemsHandoffFailureKeepsEveryItem(t *testing.T) {
	testHoldHandoff(t, "", 25, false)
}

// A held batch that cannot be prepared (too large for a direct relay and no
// document stored) must block only its own lane; other lanes still flush.
func TestHoldPrepareFailureBlocksOnlyItsLane(t *testing.T) {
	_, _, now, _, _ := holdHub(t, "", false)
	policy := filepath.Join(t.TempDir(), "policy.yaml")
	raw := "default: interrupt\nlanes:\n  lane-a:\n    hold_while_working:\n      max_hold: 10m\n      handoff_bytes: 100\n  lane-b:\n    hold_while_working:\n      max_hold: 10m\n"
	if err := os.WriteFile(policy, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	fake := &fakeHandoffkeep{nextID: 100, status: 201, rows: map[string]*handoffkeepRelayEvent{}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/documents/inbox/") {
			w.WriteHeader(503)
			return
		}
		fake.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	hk, err := newHandoffkeepRelayClient(hubHandoffkeepEnv{URL: server.URL, Token: "fixture-token"}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	h, a := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), policy, hk, now)
	for i := 0; i < 30; i++ {
		holdNotice(t, h, a, fmt.Sprint("a", i), "ready", fmt.Sprintf("READY big %d %s", i, strings.Repeat("body", 250)))
	}
	b := quietEvent("b0", "READY small lane-b notice")
	b.OwnerLane, b.JobID, b.NotifyType = "lane-b", laneEventTransportID("lane-b", "b0"), "ready"
	if r := h.relayLaneEvent(b, a); r.ID == 0 || r.PersistFailed {
		t.Fatalf("accept lane-b: %+v", r)
	}
	if len(a.relays) != 0 {
		t.Fatal("injected while working")
	}
	holdIdle(h, *now)
	h.nodes["host-b"].sessionSnapshot.sessions[0].Status = "idle"
	h.nodes["host-b"].sessionSnapshot.receivedAt = *now
	h.flushInbox()
	d := holdReceive(t, a)
	if !strings.Contains(d.Text, "lane-b notice") {
		t.Fatalf("lane-b batch not delivered past lane-a failure: %q", d.Text)
	}
	if len(a.relays) != 0 {
		t.Fatal("lane-a batch must stay retained while its document write fails")
	}
}

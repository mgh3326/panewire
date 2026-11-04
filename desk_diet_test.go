package panewire

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func deskDietHub(t *testing.T, path string, now *time.Time) (*HubServer, *hubAgent, string) {
	t.Helper()
	fake := &fakeHandoffkeep{nextID: 1000, status: 201, rows: map[string]*handoffkeepRelayEvent{}}
	doc := "inbox/operator-desk/digest/" + now.Format("20060102T150405Z")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/documents" {
			docs := []map[string]string{}
			if strings.HasPrefix(doc, r.URL.Query().Get("prefix")) {
				docs = append(docs, map[string]string{"key": doc, "updated_at": now.Format(time.RFC3339)})
			}
			json.NewEncoder(w).Encode(map[string]any{"documents": docs})
			return
		}
		if r.Method == http.MethodPut {
			t.Error("hub must not create scheduler digests")
		}
		fake.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	hk, e := newHandoffkeepRelayClient(hubHandoffkeepEnv{URL: server.URL, Token: "fixture-token"}, server.Client())
	if e != nil {
		t.Fatal(e)
	}
	h, a := quietHub(t, path, "default", hk, now)
	h.reportRelayPath = r20LanesFile(t, `{"lanes":{"operator-desk":{"machine":"host-a","pane":"w1:p1"}}}`)
	return h, a, doc
}

// assertDeskInjectParses sends a hub-built directive through the node's
// wire parser. The hub test fixtures read the in-memory struct, so only this
// round trip proves the node keeps what the hub sends.
func assertDeskInjectParses(t *testing.T, directive hubRelayInjectEvent) {
	t.Helper()
	wire, err := json.Marshal(directive)
	if err != nil {
		t.Fatal(err)
	}
	parsed, ok := parseHubOutbound(wire)
	if !ok {
		t.Fatalf("node parser rejects hub inject text %q", directive.Text)
	}
	if parsed.Text != directive.Text || parsed.EventID != directive.EventID {
		t.Fatalf("node parsed %q, hub built %q", parsed.Text, directive.Text)
	}
}

func deskDietEvent(id, typ, text string) hubJobEventPayload {
	return hubJobEventPayload{JobID: laneEventTransportID("operator-desk", id), Epoch: 1, OwnerLane: "operator-desk", EventID: id, NotifyType: typ, Text: text}
}
func drainDeskPersisted(a *hubAgent) {
	for len(a.persisted) > 0 {
		<-a.persisted
	}
}

func TestDeskDietReviewedPolicyAndReceiverStates(t *testing.T) {
	p, e := loadDeliveryPolicy("default")
	if e != nil {
		t.Fatal(e)
	}
	for _, typ := range []string{"ready", "merged", "blocked", "findings", "idle-wake", "progress", "report"} {
		if c := p.classify("operator-desk", "lane.event", deskDietEvent("routine", typ, "READY summary"), false); c != "inbox" {
			t.Fatalf("%s: %s", typ, c)
		}
	}
	for _, typ := range []string{"decision", "order", "deploy-approval", "culprit-hold", "watch-hold", "automation-failed"} {
		ev := deskDietEvent("act", typ, "choose")
		ev.DeliveryClass = "inbox"
		if c := p.classify("operator-desk", "lane.event", ev, false); c != "interrupt" {
			t.Fatalf("%s: %s", typ, c)
		}
	}
	if p.classify("worker-lane", "lane.event", deskDietEvent("ready", "ready", "READY"), false) != "interrupt" {
		t.Fatal("changed other lane safety")
	}
	for _, state := range []string{"idle", "working", "unknown"} {
		t.Run(state, func(t *testing.T) {
			now := time.Now().UTC()
			h, a, _ := deskDietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), &now)
			h.nodes["host-a"].sessionSnapshot.sessions[0].Status = state
			for _, typ := range []string{"ready", "merged", "report", "idle-wake"} {
				r := h.relayLaneEvent(deskDietEvent(typ, typ, "READY routine"), a)
				if r.ID == 0 || r.PersistFailed || len(a.relays) != 0 {
					t.Fatal(r, len(a.relays))
				}
				drainDeskPersisted(a)
			}
			now = now.Add(time.Hour)
			h.flushInbox()
			if len(a.relays) != 0 {
				t.Fatal("hub generated an extra scheduled digest")
			}
		})
	}
}

func TestDeskDietPiggybackBoundPointerSurfacingAndRestart(t *testing.T) {
	now := time.Now().UTC()
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	h, a, doc := deskDietHub(t, path, &now)
	for _, ev := range []hubJobEventPayload{
		deskDietEvent("dots", "report", "[dots] RESULT ISSUE-42 concise title\nDO NOT COPY REPORT BODY"),
		deskDietEvent("report", "report", "[report] weekly-update "+strings.Repeat("한", 500)+"\nDO NOT COPY REPORT BODY"),
		deskDietEvent("ready", "ready", "READY routine"),
	} {
		h.relayLaneEvent(ev, a)
		drainDeskPersisted(a)
	}
	first := deskDietEvent("decision-one", "decision", "ESC choose rollout window")
	result := h.relayLaneEvent(first, a)
	drainDeskPersisted(a)
	if result.PersistFailed || len(a.relays) != 1 {
		t.Fatal(result, len(a.relays))
	}
	directive := <-a.relays
	assertDeskInjectParses(t, directive)
	lines := strings.SplitN(directive.Text, inboxSummaryJoin, 2)
	if len(lines) != 2 || utf8.RuneCountInString(lines[1]) > 300 || !utf8.ValidString(lines[1]) || !strings.HasSuffix(lines[1], "hk:doc "+doc) {
		t.Fatal(directive.Text)
	}
	for _, want := range []string{"dots 1", "report 1", "ready 1", "ISSUE-42", "weekly-update"} {
		if !strings.Contains(lines[1], want) {
			t.Fatalf("missing %s: %s", want, lines[1])
		}
	}
	if strings.Contains(directive.Text, "DO NOT COPY") {
		t.Fatal("copied full report")
	}
	// Merely queueing the interrupt has not surfaced or acknowledged anything.
	items, e := h.inbox.store.unsurfacedInbox("operator-desk")
	if e != nil || len(items) != 3 {
		t.Fatal(items, e)
	}
	h.Close()
	h2, a2, _ := deskDietHub(t, path, &now)
	replay := h2.inboxRelayDirective("lane.event", first, reportRelayRoute{Machine: "host-a", Pane: "w1:p1"}, result.ID)
	if replay.Text != directive.Text {
		t.Fatal("retry changed its frozen snapshot", replay.Text, directive.Text)
	}
	h2.markRelayEventDelivered(relayPending{kind: "lane.event", event: first, eventID: result.ID, machine: "host-a", pane: "w1:p1"})
	unread, e := h2.inbox.store.list("operator-desk", true)
	if e != nil {
		t.Fatal(e)
	}
	inboxCount := 0
	for _, item := range unread {
		if item.Class == "inbox" {
			inboxCount++
			if item.AckedAt != 0 || item.ReadAt != 0 || item.SurfacedAt == 0 {
				t.Fatal("surfaced incorrectly", item)
			}
		}
	}
	if inboxCount != 3 {
		t.Fatal("surfacing retired unread items", unread)
	}
	h2.relayLaneEvent(deskDietEvent("progress", "progress", "new progress"), a2)
	drainDeskPersisted(a2)
	h2.relayLaneEvent(deskDietEvent("decision-two", "decision", "ESC next decision"), a2)
	next := <-a2.relays
	assertDeskInjectParses(t, next)
	if !strings.Contains(next.Text, "[inbox] 1 new: progress 1") || strings.Contains(next.Text, "dots 1") {
		t.Fatal(next.Text)
	}
}

// Synthetic replay preserves the supplied counts without private event text.
// The named groups total 414; the remaining 86 of the 500 are routine info.
func TestDeskDietReplay500Events(t *testing.T) {
	now := time.Now().UTC()
	h, a, _ := deskDietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), &now)
	fixture := []struct {
		kind, typ, text string
		n               int
	}{
		{"job.completed", "ready", "READY worker review available", 163},
		{"lane.event", "blocked", "[mergewake] BLOCKED synthetic check", 78},
		{"lane.event", "findings", "[mergewake] FINDINGS synthetic review", 30},
		{"lane.event", "idle-wake", `{"kind":"idle-wake"}`, 67},
		{"lane.event", "ready", "[mergewake] READY synthetic PR", 27},
		{"lane.event", "merged", "[mergewake] MERGED synthetic PR", 27},
		{"lane.event", "info", "routine synthetic information", 86},
		{"lane.event", "decision", "ESC decision required", 22},
	}
	total, interrupts := 0, 0
	for _, group := range fixture {
		for j := 0; j < group.n; j++ {
			total++
			e := deskDietEvent(fmt.Sprintf("replay-%d", total), group.typ, group.text)
			if group.kind == "lane.event" {
				r := h.relayLaneEvent(e, a)
				if r.ID == 0 || r.PersistFailed {
					t.Fatal(r)
				}
			} else {
				h.relayJobEventFrom("host-a", group.kind, e)
			}
			drainDeskPersisted(a)
			for len(a.relays) > 0 {
				d := <-a.relays
				interrupts++
				if group.typ != "decision" {
					t.Fatal("routine interrupt", d)
				}
			}
		}
	}
	if total != 500 || interrupts != 22 {
		t.Fatalf("events=%d interrupts=%d, want 500/22", total, interrupts)
	}
	now = now.Add(time.Hour)
	h.flushInbox()
	if len(a.relays) != 0 {
		t.Fatal("hub introduced an extra timed interrupt")
	}
	unread, e := h.inbox.store.list("operator-desk", true)
	if e != nil {
		t.Fatal(e)
	}
	inbox := 0
	for _, i := range unread {
		if i.Class == "inbox" {
			inbox++
		}
	}
	if inbox != 478 {
		t.Fatalf("inbox=%d want 478", inbox)
	}
	t.Logf("synthetic events=%d; interrupt=%d; inbox=%d", total, interrupts, inbox)
}

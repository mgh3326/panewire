package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestQuietClassification(t *testing.T) {
	p, err := loadDeliveryPolicy("default")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, kind, text, explicit, want string
		replay                           bool
	}{
		{"completion", "lane.event", "[worker] done | PR posted", "", "inbox", false},
		{"job completion", "job.completed", "VERDICT: DONE", "", "inbox", false},
		{"FYI", "lane.event", "FYI new build available", "", "inbox", false},
		{"ack", "lane.event", "[desk] ack | received", "", "inbox", false},
		{"install done", "lane.event", "[installer] install-done | healthy", "", "inbox", false},
		{"merged install", "lane.event", "merged, please install", "", "inbox", false},
		{"info", "lane.event", "info alert: quota is low", "", "inbox", false},
		{"replay", "lane.event", "old message", "", "inbox", true},
		{"node replay", "lane.event", "old node message", "", "inbox", true},
		{"blocker beats FYI", "lane.event", "FYI BLOCKER: disk full", "", "interrupt", false},
		{"decision", "lane.event", "decision request: choose a plan", "", "interrupt", false},
		{"action alert", "lane.event", "host alert: action required", "", "interrupt", false},
		{"question", "lane.event", "Can you review this?", "", "interrupt", false},
		{"Korean question", "lane.event", "이 방법으로 진행할까요？", "", "interrupt", false},
		{"escalation", "job.escalate", "pick a path", "", "interrupt", false},
		{"unknown action", "lane.event", "host disk failed", "", "interrupt", false},
		{"must-act beats explicit inbox", "lane.event", "BLOCKER?", "inbox", "interrupt", false},
		{"explicit interrupt", "lane.event", "[worker] done | ready", "interrupt", "interrupt", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := hubJobEventPayload{Text: tc.text, DeliveryClass: tc.explicit, Replay: tc.replay}
			if got := p.classify("lane-a", tc.kind, e, tc.replay); got != tc.want {
				t.Fatalf("got %s want %s", got, tc.want)
			}
		})
	}
	local, err := parseDeliveryPolicy([]byte("default: interrupt\ndeadlines: {inbox: 30m}\ndigest_window: 0s\nrules: []\nlanes:\n  lane-a:\n    deadlines: {inbox: 5m}\n    digest_window: 2s\n    rules:\n      - match: notice\n        class: inbox\n"))
	if err != nil {
		t.Fatal(err)
	}
	if local.classify("lane-a", "lane.event", hubJobEventPayload{Text: "notice"}, false) != "inbox" {
		t.Fatal("lane rule not applied")
	}
	d, w := local.timing("lane-a", "inbox")
	if d != 5*time.Minute || w != 2*time.Second {
		t.Fatal(d, w)
	}
	for _, raw := range []string{"default: typo", "default: interrupt\nunknown: true", "default: interrupt\ndigest_window: -1s", "default: interrupt\nrules: [{match: '[', class: inbox}]", "default: inbox\n---\ndefault: interrupt"} {
		t.Run(raw, func(t *testing.T) {
			if _, err := parseDeliveryPolicy([]byte(raw)); err == nil {
				t.Fatal("invalid policy accepted")
			}
		})
	}
}

func quietEvent(id, text string) hubJobEventPayload {
	return hubJobEventPayload{JobID: laneEventTransportID("lane-a", id), Epoch: 1, OwnerLane: "lane-a", EventID: id, Text: text}
}
func quietHub(t *testing.T, path, policy string, hk *handoffkeepRelayClient, now *time.Time) (*HubServer, *hubAgent) {
	t.Helper()
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "op", "host-a": "node", "host-b": "other"}, ReportRelayPath: r20LanesFile(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"},"lane-b":{"machine":"host-b","pane":"w2:p1"},"child":{"machine":"host-a","pane":"w1:p2","parent":"lane-b"}}}`), DeliveryPolicyPath: policy, InboxStorePath: path, handoffkeep: hk, Now: func() time.Time { return *now }, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 20), persisted: make(chan hubRelayPersistedEvent, 20)}
	h.nodes["host-a"] = &hubNodeRecord{agent: agent}
	h.nodes["host-b"] = &hubNodeRecord{agent: agent}
	t.Cleanup(func() { h.Close() })
	return h, agent
}

func TestQuietInboxStorageAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	now := time.Now().UTC()
	h, agent := quietHub(t, path, "default", nil, &now)
	event := quietEvent("one", "[worker] done | report posted")
	event.DeliveryClass = "inbox"
	r := h.relayLaneEvent(event, agent)
	if r.ID == 0 || r.PersistFailed || len(agent.relays) != 0 || len(agent.persisted) != 1 {
		t.Fatalf("result=%+v relays=%d persisted=%d", r, len(agent.relays), len(agent.persisted))
	}
	h.relayLaneEvent(event, agent)
	counts, err := h.inbox.store.counts("lane-a")
	if err != nil || counts["lane-a"] != 1 {
		t.Fatal(counts, err)
	}
	h.Close()
	h2, agent2 := quietHub(t, path, "default", nil, &now)
	// A new explicit class or changed retry body cannot reclassify this identity.
	event.DeliveryClass = "interrupt"
	event.Text = "changed retry?"
	h2.relayLaneEvent(event, agent2)
	items, err := h2.inbox.store.list("lane-a", true)
	if err != nil || len(items) != 1 || items[0].Event.Text != "[worker] done | report posted" {
		t.Fatal(items, err)
	}
	if len(agent2.relays) != 0 {
		t.Fatal("inbox retry injected")
	}
	if _, err := h2.inbox.store.read(r.ID, "lane-a", now); err != nil {
		t.Fatal(err)
	}
	counts, _ = h2.inbox.store.counts("lane-a")
	if counts["lane-a"] != 0 {
		t.Fatal(counts)
	}
	if n, err := h2.inbox.store.ack(r.ID, "lane-b", false, now); err != nil || n != 0 {
		t.Fatal(n, err)
	}
	if n, err := h2.inbox.store.ack(0, "lane-a", true, now); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0600 {
		t.Fatal(info.Mode())
	}
}

func TestQuietDurableBeforeAckFailure(t *testing.T) {
	now := time.Now()
	h, agent := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", nil, &now)
	h.inbox.store.db.Close()
	r := h.relayLaneEvent(quietEvent("fail", "FYI notice"), agent)
	if !r.PersistFailed || len(agent.persisted) != 0 || len(agent.relays) != 0 {
		t.Fatal(r)
	}
}

func TestQuietInboxConcurrentDedupe(t *testing.T) {
	now := time.Now()
	h, _ := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", nil, &now)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); h.relayLaneEvent(quietEvent("same", "FYI notice"), nil) }()
	}
	wg.Wait()
	counts, err := h.inbox.store.counts("lane-a")
	if err != nil || counts["lane-a"] != 1 {
		t.Fatal(counts, err)
	}
}

func TestQuietEscalationAndDigest(t *testing.T) {
	for _, tc := range []struct {
		name, class, window   string
		deadline, timeAdvance time.Duration
		want                  int
	}{
		{"escalation", "inbox", "0s", time.Minute, 2 * time.Minute, 1},
		{"deadline not reached", "inbox", "0s", time.Hour, time.Minute, 0},
		{"deadline disabled", "inbox", "0s", 0, time.Hour, 0},
		{"interrupt batch", "interrupt", "2s", 0, 3 * time.Second, 1},
		{"window open", "interrupt", "2s", 0, time.Second, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now().UTC()
			path := filepath.Join(t.TempDir(), "inbox.sqlite3")
			s, err := openLaneInboxStore(path)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 3; i++ {
				e := quietEvent(fmt.Sprint(i), "message "+fmt.Sprint(i))
				w, _ := time.ParseDuration(tc.window)
				if _, err := s.put(relayEventDedupeKey("lane.event", e), "lane-a", "lane.event", tc.class, e, now, tc.deadline, w); err != nil {
					t.Fatal(err)
				}
			}
			s.db.Close()
			s, err = openLaneInboxStore(path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.db.Close()
			for i := 0; i < 2; i++ {
				if err := s.makeBatches(now.Add(tc.timeAdvance)); err != nil {
					t.Fatal(err)
				}
			}
			batches, err := s.pendingBatches()
			if err != nil || len(batches) != tc.want {
				t.Fatal(batches, err)
			}
			// The digest is one line — a count and the inbox pointer — so it
			// always satisfies the lane.event text contract.
			if tc.want > 0 && (!strings.Contains(batches[0].Text, "3 ") || strings.Contains(batches[0].Text, "\n") || !validLaneEventText(batches[0].Text)) {
				t.Fatal(batches[0])
			}
		})
	}
}

func TestQuietEscalationSkipsReadAndKeepsLanesSeparate(t *testing.T) {
	now := time.Now()
	s, err := openLaneInboxStore(filepath.Join(t.TempDir(), "inbox.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.db.Close()
	for i, lane := range []string{"lane-a", "lane-a", "lane-b"} {
		e := quietEvent(fmt.Sprint(i), "FYI")
		item, err := s.put(fmt.Sprint(i), lane, "lane.event", "inbox", e, now, time.Minute, 0)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			if _, err := s.read(item.ID, lane, now); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := s.makeBatches(now.Add(2 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	b, _ := s.pendingBatches()
	if len(b) != 2 || b[0].Lane == b[1].Lane {
		t.Fatal(b)
	}
}

func TestQuietReplayHandling(t *testing.T) {
	for _, mode := range []string{"ordinary", "unconfirmed"} {
		t.Run(mode, func(t *testing.T) {
			fake, hk, closeServer := newFakeHandoffkeep(t)
			defer closeServer()
			now := time.Now()
			path := filepath.Join(t.TempDir(), "inbox.sqlite3")
			h, agent := quietHub(t, path, "default", hk, &now)
			record := handoffkeepRelayEvent{ID: 101, Kind: "lane.event", OwnerLane: "lane-a", EventID: "old", Text: "old FYI report", Epoch: 1}
			fake.seedUndelivered(record)
			replay := func(h *HubServer) {
				if mode == "ordinary" {
					h.replayRelayEvent(record, "startup")
				} else {
					h.replayMarkedUnconfirmed(context.Background(), record, "startup")
				}
			}
			replay(h)
			if len(agent.relays) != 0 {
				t.Fatal("replay injected")
			}
			h.Close()
			h2, a2 := quietHub(t, path, "default", hk, &now)
			replay(h2)
			if len(a2.relays) != 0 {
				t.Fatal("replay injected after restart")
			}
			c, err := h2.inbox.store.counts("lane-a")
			if err != nil || c["lane-a"] != 1 {
				t.Fatal(c, err)
			}
		})
	}
}

func TestQuietDigestHubHandoffAndRetry(t *testing.T) {
	fake, hk, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	now := time.Now()
	policyPath := filepath.Join(t.TempDir(), "delivery.yaml")
	raw := strings.Replace(string(defaultDeliveryPolicy), "digest_window: 0s", "digest_window: 2s", 1)
	if err := os.WriteFile(policyPath, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	h, agent := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), policyPath, hk, &now)
	for i := 0; i < 3; i++ {
		e := quietEvent(fmt.Sprint(i), "question?")
		if r := h.relayLaneEvent(e, agent); r.ID == 0 {
			t.Fatal(r)
		}
	}
	if len(agent.relays) != 0 {
		t.Fatal("digest injected before deadline")
	}
	now = now.Add(3 * time.Second)
	fake.mu.Lock()
	fake.status = 503
	fake.mu.Unlock()
	h.flushInbox()
	b, _ := h.inbox.store.pendingBatches()
	if len(b) != 1 {
		t.Fatal(b)
	}
	id := b[0].ID
	fake.mu.Lock()
	fake.status = 201
	fake.mu.Unlock()
	// The failed pass opened a backoff window; the retry waits it out.
	now = now.Add(inboxFlushBackoffBase)
	h.flushInbox()
	h.flushInbox()
	if len(agent.relays) != 1 {
		t.Fatalf("wanted one turn, got %d", len(agent.relays))
	}
	d := <-agent.relays
	if d.Kind != "lane.event" || !strings.Contains(d.Text, "3 interrupt events") || d.Deliver != "now" {
		t.Fatal(d)
	}
	b, _ = h.inbox.store.pendingBatches()
	if len(b) != 0 {
		t.Fatal(b)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.stored) != 1 || fake.stored[0].EventID != id {
		t.Fatal(fake.stored)
	}
}

func TestQuietExplicitClassEmitAndScan(t *testing.T) {
	for _, kind := range []string{"lane.event", "job.completed", "job.escalate"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			args := []string{"--kind", kind, "--deliver", "inbox", "--inbox-root", root}
			if kind == "lane.event" {
				args = append(args, "--lane", "lane-a", "--event-id", "e1", "--text", "BLOCKER?")
			} else {
				args = append(args, "--job", "quiet-job", "--owner-lane", "lane-a", "--report", "report.md", "--question", "question?", "--reason", "needed")
			}
			if code := runEmitCLI(args, io.Discard, io.Discard, CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent.sock")}); code != ExitOK {
				t.Fatal(code)
			}
			records := scanHubRelayEvents(root)
			if len(records) != 1 || records[0].DeliveryClass != "inbox" {
				t.Fatal(records)
			}
			node := &HubClient{completedJobs: map[string]uint64{}, completedReports: map[string]struct{}{}}
			wire, ok := node.relayEventForSend(records[0])
			if !ok {
				t.Fatal("no wire event")
			}
			var e hubJobEventPayload
			if kind == "lane.event" {
				e, ok = decodeHubLaneEventPayload(wire.Payload)
			} else if kind == "job.completed" {
				e, ok = decodeHubJobCompletionPayload(wire.Payload)
			} else {
				e, ok = decodeHubJobEscalationPayload(wire.Payload)
			}
			if !ok || e.DeliveryClass != "inbox" {
				t.Fatalf("%s decoded=%+v ok=%v", wire.Payload, e, ok)
			}
		})
	}
}

func TestQuietInboxCLIAndNodeScope(t *testing.T) {
	now := time.Now()
	h, _ := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", nil, &now)
	a := h.relayLaneEvent(quietEvent("one", "FYI one"), nil)
	e := quietEvent("two", "FYI two")
	e.OwnerLane = "lane-b"
	e.JobID = laneEventTransportID(e.OwnerLane, e.EventID)
	b := h.relayLaneEvent(e, nil)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	token := filepath.Join(t.TempDir(), "node.env")
	if err := os.WriteFile(token, []byte("HUB_MACHINE_ID=host-a\nHUB_TOKEN=node\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		args     []string
		want     int
		contains string
	}{
		{[]string{"count", "--lane", "lane-a"}, ExitOK, "inbox: 1 unread"},
		{[]string{"list", "--lane", "lane-a", "--unread", "--json"}, ExitOK, "FYI one"},
		{[]string{"count", "--lane", "lane-b"}, ExitDeliveryFailure, ""},
		{[]string{"read", fmt.Sprint(b.ID), "--lane", "lane-a"}, ExitDeliveryFailure, ""},
		{[]string{"ack", fmt.Sprint(b.ID), "--lane", "lane-a"}, ExitOK, "acked: 0"},
		{[]string{"read", fmt.Sprint(a.ID), "--lane", "lane-a", "--json"}, ExitOK, "read_at_ms"},
		{[]string{"count", "--lane", "lane-a", "--json"}, ExitOK, "\"lane-a\":0"},
		{[]string{"ack", "--all", "--lane", "lane-a"}, ExitOK, "acked: 1"},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			args := append(append([]string{}, tc.args...), "--hub-url", server.URL, "--hub-token-env", token)
			var out, errOut bytes.Buffer
			code := runInboxCLI(args, &out, &errOut, hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true})
			if code != tc.want || !strings.Contains(out.String(), tc.contains) {
				t.Fatalf("code=%d out=%s err=%s", code, out.String(), errOut.String())
			}
		})
	}
	request := httptest.NewRequest(http.MethodGet, "/v1/inbox/count", nil)
	w := httptest.NewRecorder()
	h.Handler().ServeHTTP(w, request)
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
	counts, _ := h.inbox.store.counts("lane-b")
	if counts["lane-b"] != 1 {
		t.Fatal(counts)
	}
}

func TestQuietLegacyWireAndChat(t *testing.T) {
	p, _ := loadDeliveryPolicy("default")
	if p.classify("lane-a", "lane.event", hubJobEventPayload{EventID: "chat-1", Text: "FYI"}, false) != "interrupt" {
		t.Fatal("empty class")
	}
	raw := []byte(`{"job_id":"j1","epoch":1}`)
	if !bytes.Equal(raw, relayPayloadDeliveryClass(raw, "")) {
		t.Fatal("legacy shape changed")
	}
	for _, class := range []string{"inbox", "interrupt", "invalid"} {
		payload := relayPayloadDeliveryClass(raw, class)
		event, ok := decodeHubJobCompletionPayload(payload)
		if ok != (class != "invalid") || (ok && event.DeliveryClass != class) {
			t.Fatal(class, event, ok)
		}
	}
	// Outbound relay.inject is unchanged: older daemons never see inbox items
	// or a new field they would reject in their strict decoder.
	wire, _ := json.Marshal(relayInjectDirective("lane.event", quietEvent("x", "question?"), reportRelayRoute{Pane: "w1:p1"}, 42))
	if _, ok := parseHubOutbound(wire); !ok {
		t.Fatalf("legacy daemon shape rejected: %s", wire)
	}
}

func TestQuietCLIDefaultPolicyEnabled(t *testing.T) {
	auth := filepath.Join(t.TempDir(), "auth.env")
	if err := os.WriteFile(auth, []byte("HUB_TOKEN_operator=op\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ policy, want string }{{"", "inbox"}, {"off", "interrupt"}} {
		t.Run(tc.policy, func(t *testing.T) {
			args := []string{"--hub-auth", auth, "--inbox-store", filepath.Join(t.TempDir(), "inbox.sqlite3")}
			if tc.policy != "" {
				args = append(args, "--delivery-policy", tc.policy)
			}
			h, _, code, err := newHubServerForCLI(args, nil)
			if err != nil || code != ExitOK {
				t.Fatal(code, err)
			}
			defer h.Close()
			if tc.policy == "off" {
				if h.inbox != nil {
					t.Fatal("off opened the inbox")
				}
				return
			}
			if h.inbox == nil {
				t.Fatal("default inbox disabled")
			}
			if got := h.inbox.policy.classify("lane-a", "lane.event", quietEvent("x", "FYI"), false); got != tc.want {
				t.Fatal(got, tc.want)
			}
		})
	}
}

func TestQuietDigestArrivalWindowAndParentPolicy(t *testing.T) {
	now := time.Now()
	s, err := openLaneInboxStore(filepath.Join(t.TempDir(), "inbox.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.db.Close()
	for i, delay := range []time.Duration{0, time.Second, 3 * time.Second} {
		e := quietEvent(fmt.Sprint(i), "action")
		if _, err := s.put(fmt.Sprint(i), "lane-a", "lane.event", "interrupt", e, now.Add(delay), 0, 2*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.makeBatches(now.Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	b, _ := s.pendingBatches()
	if len(b) != 1 || !strings.Contains(b[0].Text, "2 interrupt events") {
		t.Fatal(b)
	}
	if err := s.makeBatches(now.Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	b, _ = s.pendingBatches()
	if len(b) != 2 {
		t.Fatal(b)
	}
	h, agent := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", nil, &now)
	e := hubJobEventPayload{JobID: "j1", Epoch: 1, OwnerLane: "child", ReportPath: "report.md"}
	h.relayJobEventFrom("host-a", "job.joined", e)
	c, _ := h.inbox.store.counts("lane-b")
	if c["lane-b"] != 1 || len(agent.relays) != 0 || len(agent.persisted) != 1 {
		t.Fatal(c)
	}
}

// Check the actual closed wire shape and node journal, not only the hub's
// queue length: a lane acknowledgement without Lane is silently rejected by
// both old and new node decoders, leaving a quiet event's outbox unretired.
func TestQuietProducerAcknowledgementRetiresOutbox(t *testing.T) {
	for _, tc := range []struct{ kind, class, window string }{{"lane.event", "inbox", "0s"}, {"lane.event", "interrupt", "2s"}, {"job.completed", "inbox", "0s"}} {
		t.Run(tc.kind+tc.class+tc.window, func(t *testing.T) {
			now := time.Now()
			policyPath := filepath.Join(t.TempDir(), "policy.yaml")
			raw := strings.Replace(string(defaultDeliveryPolicy), "digest_window: 0s", "digest_window: "+tc.window, 1)
			if err := os.WriteFile(policyPath, []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			h, agent := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), policyPath, nil, &now)
			e := quietEvent("ack-test", "action?")
			e.DeliveryClass = tc.class
			if tc.kind != "lane.event" {
				e.JobID = "j1"
				e.ReportPath = "report.md"
			}
			if tc.kind == "lane.event" {
				h.relayLaneEvent(e, agent)
			} else {
				h.relayJobEventFrom("host-a", tc.kind, e)
			}
			select {
			case ack := <-agent.persisted:
				raw, err := json.Marshal(ack)
				if err != nil {
					t.Fatal(err)
				}
				parsed, ok := parseHubOutbound(raw)
				if !ok {
					t.Fatalf("node rejected ACK: %s", raw)
				}
				store := NewMemoryStore(t)
				defer store.Close()
				node := &HubClient{outbox: store}
				node.recordRelayPersisted(parsed)
				key := relayOutboxKey{Kind: tc.kind, JobID: e.JobID, Epoch: e.Epoch, ReportPath: e.ReportPath, Reason: e.Reason, Lane: e.OwnerLane, EventID: e.EventID}
				state, err := store.RelayOutboxState(context.Background(), key)
				if err != nil || !state.Persisted {
					t.Fatalf("outbox not persisted: %+v %v", state, err)
				}
			default:
				t.Fatal("no producer ACK")
			}
		})
	}
}

// Every must-act signal the shipped policy knows still interrupts; a stray
// informational word inside the same message can no longer steal it.
func TestQuietMustActWholeWordPolicy(t *testing.T) {
	p, err := loadDeliveryPolicy("default")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, kind, text string }{
		{"BLOCKER", "lane.event", "[desk] BLOCKER | disk full"},
		{"READY", "lane.event", "[job] READY | abc123 | report.md"},
		{"ESC", "lane.event", "[desk] ESC | needs the operator"},
		{"BOUNCE", "lane.event", "[checker] BOUNCE | missing test"},
		{"URGENT", "lane.event", "URGENT: deploy is broken"},
		{"needs_decision", "lane.event", "needs_decision: pick option A or B"},
		{"must-act beats info substring", "lane.event", "ESC: info needed for deploy"},
		{"must-act beats quota substring", "lane.event", "URGENT: quota nearly exhausted"},
		{"decision request", "lane.event", "decision request: choose a plan"},
		{"action required", "lane.event", "host alert: action required"},
		{"needs a decision", "lane.event", "needs a decision on rollout"},
		{"question", "lane.event", "Can you review this?"},
		{"korean question", "lane.event", "이 방법으로 진행할까요？"},
		{"escalate kind", "job.escalate", "pick a path"},
		{"lost kind", "job.lost", "node gone"},
		{"revoked kind", "job.revoked", "access revoked"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.classify("lane-a", tc.kind, hubJobEventPayload{Text: tc.text}, false); got != "interrupt" {
				t.Fatalf("must-act %q classified %s", tc.text, got)
			}
		})
	}
	// A strict inbox-default view isolates rule hits from the fallback:
	// look-alikes must not trip the whole-word tags.
	p.Default = "inbox"
	for _, text := range []string{"READYING the fleet", "ESCAPE plan drafted", "BLOCKERED pipe cleared", "unready review", "escorted out", "bounced twice", "urgency noted"} {
		if got := p.classify("lane-a", "lane.event", hubJobEventPayload{Text: text}, false); got != "inbox" {
			t.Fatalf("look-alike %q matched a must-act rule (%s)", text, got)
		}
	}
	for _, text := range []string{"READY | abc123 | done", "ESC | operator now", "BLOCKER | stopped"} {
		if got := p.classify("lane-a", "lane.event", hubJobEventPayload{Text: text}, false); got != "interrupt" {
			t.Fatalf("tag %q not delivered under strict default", text)
		}
	}
	if _, err := parseDeliveryPolicy([]byte("default: interrupt\nrules: [{tags: ['has space '], class: inbox}]")); err == nil {
		t.Fatal("tag with a non-word edge accepted")
	}
}

// A failed flush pass must not spin: attempts happen once per growing backoff
// window; individual permanent rejections are quarantined separately.
func TestQuietDigestPersistenceBackoff(t *testing.T) {
	fake, hk, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	now := time.Now()
	policyPath := filepath.Join(t.TempDir(), "delivery.yaml")
	raw := strings.Replace(string(defaultDeliveryPolicy), "digest_window: 0s", "digest_window: 2s", 1)
	if err := os.WriteFile(policyPath, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	h, agent := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), policyPath, hk, &now)
	for i := 0; i < 3; i++ {
		h.relayLaneEvent(quietEvent(fmt.Sprint(i), "question?"), agent)
	}
	now = now.Add(3 * time.Second)
	posts := func() int {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		n := 0
		for _, c := range fake.calls {
			if c.Method == http.MethodPost && c.Path == "/v1/relay/events" {
				n++
			}
		}
		return n
	}
	fake.mu.Lock()
	fake.status = http.StatusServiceUnavailable
	fake.mu.Unlock()
	h.flushInbox()
	if posts() != 1 || h.InboxFlushFailureCount() != 1 {
		t.Fatalf("first failure posts=%d failures=%d", posts(), h.InboxFlushFailureCount())
	}
	// Inside the backoff window a tick makes no handoffkeep call at all.
	for i := 0; i < 10; i++ {
		h.flushInbox()
	}
	if posts() != 1 {
		t.Fatalf("backoff window made %d posts", posts())
	}
	// Six simulated minutes of 100ms ticks stay bounded: attempts at +1s,
	// +3s, +7s, +15s, +31s then one per capped 60s window — a dozen at most,
	// not the per-second storm this loop used to be.
	for i := 0; i < 3600; i++ {
		now = now.Add(100 * time.Millisecond)
		h.flushInbox()
	}
	if got := posts(); got > 12 {
		t.Fatalf("six minutes of failures made %d persist attempts", got)
	} else if got < 4 {
		t.Fatalf("backoff swallowed legitimate retries: %d posts", got)
	}
	// Recovery resets the schedule: the next pass sends immediately.
	fake.mu.Lock()
	fake.status = http.StatusCreated
	fake.mu.Unlock()
	now = now.Add(inboxFlushBackoffMax)
	h.flushInbox()
	if len(agent.relays) != 1 || h.inbox.failures != 0 {
		t.Fatalf("recovery relays=%d failures=%d", len(agent.relays), h.inbox.failures)
	}
}

// The startup probe reads the contract instead of writing it: a pre-v7
// handoffkeep answers the kind filter with a 4xx and the flush stands down
// before it can loop. Anything inconclusive leaves the bounded retry path.
func TestQuietInboxContractProbe(t *testing.T) {
	for _, tc := range []struct {
		name         string
		status       int
		wantDisabled bool
	}{
		{"capable", 0, false},
		{"schema gap", http.StatusBadRequest, true},
		{"route missing", http.StatusNotFound, true},
		{"outage", http.StatusServiceUnavailable, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake, hk, closeServer := newFakeHandoffkeep(t)
			defer closeServer()
			if tc.status != 0 {
				fake.mu.Lock()
				fake.getLaneEventStatus = tc.status
				fake.mu.Unlock()
			}
			now := time.Now()
			h, _ := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", hk, &now)
			h.probeInboxRelayContract(context.Background())
			if h.inboxDigestDisabled() != tc.wantDisabled {
				t.Fatalf("persistDisabled=%v want %v", h.inboxDigestDisabled(), tc.wantDisabled)
			}
			h.flushInbox()
			if tc.wantDisabled {
				fake.mu.Lock()
				defer fake.mu.Unlock()
				for _, c := range fake.calls {
					if c.Method == http.MethodPost {
						t.Fatal("disabled flush persisted")
					}
				}
			}
		})
	}
	// runInbox probes once before its first tick.
	fake, hk, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	fake.mu.Lock()
	fake.getLaneEventStatus = http.StatusBadRequest
	fake.mu.Unlock()
	now := time.Now()
	h, _ := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", hk, &now)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.runInbox(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for !h.inboxDigestDisabled() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !h.inboxDigestDisabled() {
		t.Fatal("runInbox did not probe the contract")
	}
}

// PANEWIRE_DELIVERY_POLICY=off is the env-var kill switch equivalent to the
// flag; an explicit --delivery-policy always wins over it.
func TestQuietDeliveryPolicyEnvKillSwitch(t *testing.T) {
	auth := filepath.Join(t.TempDir(), "auth.env")
	if err := os.WriteFile(auth, []byte("HUB_TOKEN_operator=op\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ env, want string }{{"off", "interrupt"}, {"default", "inbox"}} {
		t.Run(tc.env, func(t *testing.T) {
			t.Setenv("PANEWIRE_DELIVERY_POLICY", tc.env)
			path := filepath.Join(t.TempDir(), "inbox.sqlite3")
			h, _, code, err := newHubServerForCLI([]string{"--hub-auth", auth, "--inbox-store", path}, nil)
			if err != nil || code != ExitOK {
				t.Fatal(code, err)
			}
			defer h.Close()
			if tc.env == "off" {
				if h.inbox != nil {
					t.Fatal("off opened the inbox")
				}
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatal("env kill switch touched inbox path", err)
				}
				return
			}
			if got := h.inbox.policy.classify("lane-a", "lane.event", quietEvent("x", "FYI"), false); got != tc.want {
				t.Fatal(got, tc.want)
			}
		})
	}
	t.Setenv("PANEWIRE_DELIVERY_POLICY", "off")
	h, _, code, err := newHubServerForCLI([]string{"--hub-auth", auth, "--delivery-policy", "default", "--inbox-store", filepath.Join(t.TempDir(), "inbox.sqlite3")}, nil)
	if err != nil || code != ExitOK {
		t.Fatal(code, err)
	}
	defer h.Close()
	if got := h.inbox.policy.classify("lane-a", "lane.event", quietEvent("x", "FYI"), false); got != "inbox" {
		t.Fatal("explicit flag lost to env", got)
	}
}

package panewire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNotifyTypeClassification(t *testing.T) {
	p, err := loadDeliveryPolicy("default")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, notifyType, explicit, text, want string
	}{
		{"decision", "decision", "", "status note", "interrupt"},
		{"order", "order", "", "status note", "interrupt"},
		{"ready", "ready", "", "status note", "interrupt"},
		{"report", "report", "", "Can you review this?", "inbox"},
		{"ack", "ack", "", "Can you review this?", "inbox"},
		{"info", "info", "", "status note", "inbox"},
		{"must-act beats type", "info", "", "BLOCKER disk full", "interrupt"},
		{"explicit class beats type", "ready", "inbox", "status note", "inbox"},
		{"type beats chat pattern", "report", "", "status note", "inbox"},
		{"untyped falls to rules", "", "", "Can you review this?", "interrupt"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := hubJobEventPayload{Text: tc.text, NotifyType: tc.notifyType, DeliveryClass: tc.explicit}
			if tc.name == "type beats chat pattern" {
				e.EventID = "chat-42"
			}
			if got := p.classify("lane-a", "lane.event", e, false); got != tc.want {
				t.Fatalf("got %s want %s", got, tc.want)
			}
		})
	}
}

func TestNotifyTypePolicyOverrides(t *testing.T) {
	p, err := parseDeliveryPolicy([]byte(`default: interrupt
rules: []
types:
  report: interrupt
lanes:
  lane-a:
    types:
      report: inbox
      ready: inbox
`))
	if err != nil {
		t.Fatal(err)
	}
	e := hubJobEventPayload{NotifyType: "report", Text: "note"}
	if got := p.classify("lane-b", "lane.event", e, false); got != "interrupt" {
		t.Fatalf("global override: got %s", got)
	}
	if got := p.classify("lane-a", "lane.event", e, false); got != "inbox" {
		t.Fatalf("lane override: got %s", got)
	}
	e.NotifyType = "ready"
	if got := p.classify("lane-a", "lane.event", e, false); got != "inbox" {
		t.Fatalf("lane override ready: got %s", got)
	}
	if got := p.classify("lane-b", "lane.event", e, false); got != "interrupt" {
		t.Fatalf("built-in ready: got %s", got)
	}
	for _, raw := range []string{
		"default: interrupt\ntypes: {bogus: inbox}",
		"default: interrupt\ntypes: {report: bogus}",
		"default: interrupt\ntypes: {report: ''}",
		"default: interrupt\nlanes:\n  lane-a:\n    types: {bogus: inbox}",
	} {
		t.Run(raw, func(t *testing.T) {
			if _, err := parseDeliveryPolicy([]byte(raw)); err == nil {
				t.Fatal("invalid types map accepted")
			}
		})
	}
}

func TestNotifyTypeEmitScanAndWire(t *testing.T) {
	for _, kind := range []string{"lane.event", "job.completed", "job.escalate"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			args := []string{"--kind", kind, "--type", "report", "--inbox-root", root}
			if kind == "lane.event" {
				args = append(args, "--lane", "lane-a", "--event-id", "e1", "--text", "question?")
			} else {
				args = append(args, "--job", "typed-job", "--owner-lane", "lane-a", "--report", "report.md", "--question", "question?", "--reason", "needed")
			}
			if code := runEmitCLI(args, io.Discard, io.Discard, CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent.sock")}); code != ExitOK {
				t.Fatal(code)
			}
			records := scanHubRelayEvents(root)
			if len(records) != 1 || records[0].NotifyType != "report" {
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
			if !ok || e.NotifyType != "report" {
				t.Fatalf("%s decoded=%+v ok=%v", wire.Payload, e, ok)
			}
		})
	}
	// Unknown types are refused at every boundary: the CLI, the daemon request
	// contract, the wire decoders, and the HTTP ingress.
	if code := runEmitCLI([]string{"--kind", "lane.event", "--lane", "lane-a", "--event-id", "e1", "--text", "note", "--type", "bogus", "--inbox-root", t.TempDir()}, io.Discard, io.Discard, CLIConfig{}); code != ExitUsage {
		t.Fatal(code)
	}
	payload := []byte(`{"owner_lane":"lane-a","event_id":"e1","text":"note","notify_type":"bogus"}`)
	if _, ok := decodeHubLaneEventPayload(payload); ok {
		t.Fatal("bogus notify_type decoded")
	}
	jobPayload := []byte(`{"job_id":"j1","epoch":1,"notify_type":"bogus"}`)
	if _, ok := decodeHubJobCompletionPayload(jobPayload); ok {
		t.Fatal("bogus notify_type decoded")
	}
	escPayload := []byte(`{"job_id":"j1","epoch":1,"reason":"r","notify_type":"bogus"}`)
	if _, ok := decodeHubJobEscalationPayload(escPayload); ok {
		t.Fatal("bogus notify_type decoded")
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/relay/events", strings.NewReader(`{"kind":"lane.event","lane":"lane-a","event_id":"e1","text":"note","label":"desk","notify_type":"bogus"}`))
	if _, ok := decodeHubRelayIngressRequest(httptest.NewRecorder(), request); ok {
		t.Fatal("bogus notify_type accepted at ingress")
	}
}

func TestNotifyTypeRoutesThroughInbox(t *testing.T) {
	now := time.Now()
	h, agent := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", nil, &now)
	e := quietEvent("typed", "question?")
	e.NotifyType = "report"
	if r := h.relayLaneEvent(e, agent); r.ID == 0 || !r.Routed {
		t.Fatal(r)
	}
	if len(agent.relays) != 0 {
		t.Fatal("typed report injected instead of inboxed")
	}
	counts, err := h.inbox.store.counts("lane-a")
	if err != nil || counts["lane-a"] != 1 {
		t.Fatal(counts, err)
	}
	e2 := quietEvent("typed2", "FYI notice")
	e2.NotifyType = "ready"
	h.relayLaneEvent(e2, agent)
	items, err := h.inbox.store.list("lane-a", false)
	if err != nil {
		t.Fatal(err)
	}
	var stored laneInboxItem
	for _, i := range items {
		if i.Event.EventID == "typed2" {
			stored = i
		}
	}
	if stored.Class != "interrupt" {
		t.Fatalf("typed ready stored as %q, want interrupt", stored.Class)
	}
}

// Regression: `panewire inbox read <id>` printed nothing for an existing item
// when an ambient PANEWIRE_LANE named a different lane — the lane-scoped store
// lookup turned a valid id into a silent 404. Operator tokens now scope read
// and ack <id> only by an explicit --lane.
func TestInboxReadIgnoresAmbientLaneForOperator(t *testing.T) {
	now := time.Now()
	h, _ := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", nil, &now)
	item := h.relayLaneEvent(quietEvent("one", "FYI one"), nil)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	token := filepath.Join(t.TempDir(), "op.env")
	if err := os.WriteFile(token, []byte("HUB_MACHINE_ID=operator\nHUB_TOKEN=op\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PANEWIRE_LANE", "lane-b")
	deps := hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true}
	args := []string{"read", fmt.Sprint(item.ID), "--hub-url", server.URL, "--hub-token-env", token}
	var out, errOut bytes.Buffer
	if code := runInboxCLI(args, &out, &errOut, deps); code != ExitOK || !strings.Contains(out.String(), "FYI one") {
		t.Fatalf("ambient lane hid the item: code=%d out=%q err=%q", code, out.String(), errOut.String())
	}
	// An explicit --lane still scopes the lookup honestly.
	out.Reset()
	errOut.Reset()
	args = []string{"read", fmt.Sprint(item.ID), "--lane", "lane-b", "--hub-url", server.URL, "--hub-token-env", token}
	if code := runInboxCLI(args, &out, &errOut, deps); code != ExitDeliveryFailure || out.Len() != 0 {
		t.Fatalf("explicit lane must still scope: code=%d out=%q err=%q", code, out.String(), errOut.String())
	}
	// And the ambient default still serves node credentials for their own lane.
	nodeToken := filepath.Join(t.TempDir(), "node.env")
	if err := os.WriteFile(nodeToken, []byte("HUB_MACHINE_ID=host-a\nHUB_TOKEN=node\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PANEWIRE_LANE", "lane-a")
	out.Reset()
	errOut.Reset()
	args = []string{"read", fmt.Sprint(item.ID), "--hub-url", server.URL, "--hub-token-env", nodeToken}
	if code := runInboxCLI(args, &out, &errOut, deps); code != ExitOK || !strings.Contains(out.String(), "FYI one") {
		t.Fatalf("node ambient lane broken: code=%d out=%q err=%q", code, out.String(), errOut.String())
	}
}

func inboxWaitRequest(t *testing.T, h *HubServer, url, token, machine string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, url, nil)
	if token != "" {
		request.Header.Set(hubAuthorizationHeader, "Bearer "+token)
	}
	if machine != "" {
		request.Header.Set(hubMachineIDHeader, machine)
	}
	writer := httptest.NewRecorder()
	h.Handler().ServeHTTP(writer, request)
	return writer
}

func TestInboxWaitEndpoint(t *testing.T) {
	now := time.Now()
	h, _ := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", nil, &now)
	// Immediate answer when an unread item already exists.
	item := h.relayLaneEvent(quietEvent("one", "FYI one"), nil)
	w := inboxWaitRequest(t, h, "/v1/inbox/wait?lane=lane-a", "op", "")
	var body struct {
		Item *laneInboxItem `json:"item"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &body) != nil || body.Item == nil || body.Item.ID != item.ID {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}
	// Waiting on an empty lane returns a null item once the wait runs out.
	start := time.Now()
	w = inboxWaitRequest(t, h, "/v1/inbox/wait?lane=lane-b&timeout=50ms", "op", "")
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &body) != nil || body.Item != nil {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Fatal("wait returned before the timeout elapsed")
	}
	// A put wakes a blocked waiter without waiting for the timeout.
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- inboxWaitRequest(t, h, "/v1/inbox/wait?lane=lane-b&timeout=5s", "op", "")
	}()
	time.Sleep(50 * time.Millisecond)
	e := quietEvent("two", "FYI two")
	e.OwnerLane, e.JobID = "lane-b", laneEventTransportID("lane-b", "two")
	h.relayLaneEvent(e, nil)
	select {
	case w = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("waiter did not wake on insert")
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &body) != nil || body.Item == nil || body.Item.Event.Text != "FYI two" {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}
	// Lane and auth checks.
	for _, tc := range []struct {
		name, url, token, machine string
		want                      int
	}{
		{"lane required", "/v1/inbox/wait", "op", "", 400},
		{"unauthenticated", "/v1/inbox/wait?lane=lane-a", "", "", 401},
		{"node unowned lane", "/v1/inbox/wait?lane=lane-b&timeout=10ms", "node", "host-a", 401},
		{"node owned lane", "/v1/inbox/wait?lane=lane-a&timeout=10ms", "node", "host-a", 200},
		{"invalid timeout", "/v1/inbox/wait?lane=lane-a&timeout=bogus", "op", "", 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if w := inboxWaitRequest(t, h, tc.url, tc.token, tc.machine); w.Code != tc.want {
				t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
	// The waiter cap refuses additional long-polls rather than queueing them.
	for i := 0; i < hubInboxWaitMaxWaiters; i++ {
		h.inbox.waiters <- struct{}{}
	}
	w = inboxWaitRequest(t, h, "/v1/inbox/wait?lane=lane-a", "op", "")
	for i := 0; i < hubInboxWaitMaxWaiters; i++ {
		<-h.inbox.waiters
	}
	if w.Code != 429 {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}
}

func TestInboxWaitCLI(t *testing.T) {
	now := time.Now()
	h, _ := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", nil, &now)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	token := filepath.Join(t.TempDir(), "op.env")
	if err := os.WriteFile(token, []byte("HUB_MACHINE_ID=operator\nHUB_TOKEN=op\n"), 0600); err != nil {
		t.Fatal(err)
	}
	deps := hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true}
	run := func(args ...string) (int, string, string) {
		args = append(args, "--hub-url", server.URL, "--hub-token-env", token)
		var out, errOut bytes.Buffer
		code := runInboxCLI(args, &out, &errOut, deps)
		return code, out.String(), errOut.String()
	}
	// Timeout: exits 3 and prints nothing.
	code, out, errOut := run("wait", "--lane", "lane-b", "--timeout", "120ms")
	if code != ExitTimeout || out != "" {
		t.Fatalf("code=%d out=%q err=%q", code, out, errOut)
	}
	// An unread item prints like read and exits 0.
	item := h.relayLaneEvent(quietEvent("one", "FYI one"), nil)
	code, out, _ = run("wait", "--lane", "lane-a", "--timeout", "1s")
	if code != ExitOK || !strings.Contains(out, "#"+fmt.Sprint(item.ID)+" lane-a inbox (lane.event)") || !strings.Contains(out, "FYI one") {
		t.Fatalf("code=%d out=%q", code, out)
	}
	// --json prints the item document.
	code, out, _ = run("wait", "--lane", "lane-a", "--timeout", "1s", "--json")
	var decoded laneInboxItem
	if code != ExitOK || json.Unmarshal([]byte(out), &decoded) != nil || decoded.ID != item.ID {
		t.Fatalf("code=%d out=%q", code, out)
	}
}

package panewire

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

const checkoutPullOrderPath = "/v1/fleet/checkout-pull"

// checkoutPullHK is a fake handoffkeep that records each document PUT.
type checkoutPullHK struct {
	mu     sync.Mutex
	keys   []string
	bodies []string
	status int
}

func newCheckoutPullHK(t *testing.T, status int) (*checkoutPullHK, *handoffkeepRelayClient) {
	t.Helper()
	fake := &checkoutPullHK{status: status}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		fake.mu.Lock()
		defer fake.mu.Unlock()
		if r.Method != http.MethodPut || !strings.HasPrefix(r.URL.EscapedPath(), "/v1/documents/") {
			http.Error(w, "unexpected", http.StatusNotFound)
			return
		}
		key, _ := url.PathUnescape(strings.TrimPrefix(r.URL.EscapedPath(), "/v1/documents/"))
		fake.keys = append(fake.keys, key)
		fake.bodies = append(fake.bodies, string(body))
		if fake.status != http.StatusOK && fake.status != http.StatusCreated {
			http.Error(w, "unavailable", fake.status)
			return
		}
		w.WriteHeader(fake.status)
		_, _ = w.Write([]byte(`{"document":{}}`))
	}))
	t.Cleanup(server.Close)
	client, err := newHandoffkeepRelayClient(hubHandoffkeepEnv{URL: server.URL, Token: "test-token"}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	return fake, client
}

func (fake *checkoutPullHK) written() ([]string, []string) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return append([]string(nil), fake.keys...), append([]string(nil), fake.bodies...)
}

func checkoutPullOrder(t *testing.T, server *httptest.Server, body any) <-chan t14HTTPResult {
	t.Helper()
	return t14HTTPAsync(t, http.MethodPost, server.URL+checkoutPullOrderPath, t14OperatorToken, body)
}

// checkoutPullReplyPulled answers a node's checkout.pull with a pulled result.
func checkoutPullReplyPulled(t *testing.T, connection *websocket.Conn, command map[string]json.RawMessage, from, to string) {
	t.Helper()
	nodeCommandReplyTo(t, connection, command, map[string]any{"result": map[string]any{
		"outcome": "pulled", "checkout": "rstack", "from": from, "to": to,
		"install_ran": false, "install_exit": 0, "target": "", "target_reached": nil,
	}})
}

func TestHubCheckoutPullFansOutAndListsDisconnectedNode(t *testing.T) {
	hub, server := t14Hub(t)
	connection := t14Node(t, hub, server, false)
	hub.mu.Lock()
	hub.nodes["machine-b"] = &hubNodeRecord{machineID: "machine-b", state: "disconnected"}
	hub.mu.Unlock()

	from := strings.Repeat("a", 40)
	to := strings.Repeat("b", 40)
	results := checkoutPullOrder(t, server, map[string]any{"checkout": "rstack", "rev": "bbbbbbb"})
	command := nodeCommandRead(t, connection)
	if string(command["op"]) != `"checkout.pull"` || string(command["args"]) != `{"checkout":"rstack","rev":"bbbbbbb"}` {
		t.Fatalf("node received op=%s args=%s", command["op"], command["args"])
	}
	nodeCommandReplyTo(t, connection, command, map[string]any{"result": map[string]any{
		"outcome": "pulled", "checkout": "rstack", "from": from, "to": to,
		"install_ran": false, "install_exit": 0, "target_reached": true,
	}})
	result := t14Result(t, results)
	if result.status != http.StatusOK {
		t.Fatalf("status=%d body=%s", result.status, result.body)
	}
	var answer hubCheckoutPullResponse
	if err := json.Unmarshal(result.body, &answer); err != nil {
		t.Fatalf("body=%s: %v", result.body, err)
	}
	if len(answer.Hosts) != 2 || answer.Hosts[0].Machine != "machine-a" || answer.Hosts[1].Machine != "machine-b" {
		t.Fatalf("hosts=%+v, want machine-a then machine-b", answer.Hosts)
	}
	if answer.Hosts[1].Error != hubCheckoutNodeGone || answer.Hosts[1].State != "disconnected" || answer.Hosts[1].Result != nil {
		t.Fatalf("disconnected node row=%+v, want node_unavailable without a result", answer.Hosts[1])
	}
	if answer.Hosts[0].Error != "" || !strings.Contains(string(answer.Hosts[0].Result), `"outcome":"pulled"`) {
		t.Fatalf("connected node row=%+v", answer.Hosts[0])
	}
	if answer.Record.Outcome != hubCheckoutRecordNoHK {
		t.Fatalf("record=%+v, want unconfigured without hk", answer.Record)
	}
}

func TestHubCheckoutPullWritesOneDeployRecordPerOrder(t *testing.T) {
	hub, server := t14Hub(t)
	connection := t14Node(t, hub, server, false)
	fake, client := newCheckoutPullHK(t, http.StatusCreated)
	hub.handoffkeep = client

	from := strings.Repeat("a", 40)
	to := strings.Repeat("c", 40)
	// Two orders for the same revision on the same day must still get two
	// documents: each key ends in its own order id.
	for _, rev := range []string{"ccccccc", "ccccccc", ""} {
		results := checkoutPullOrder(t, server, map[string]any{"checkout": "rstack", "rev": rev})
		command := nodeCommandRead(t, connection)
		checkoutPullReplyPulled(t, connection, command, from, to)
		result := t14Result(t, results)
		var answer hubCheckoutPullResponse
		if result.status != http.StatusOK || json.Unmarshal(result.body, &answer) != nil {
			t.Fatalf("status=%d body=%s", result.status, result.body)
		}
		if answer.Record.Outcome != hubCheckoutRecordWrote {
			t.Fatalf("record=%+v, want written", answer.Record)
		}
	}

	keys, bodies := fake.written()
	if len(keys) != 3 || keys[0] == keys[1] {
		t.Fatalf("hk documents=%v, want exactly one distinct document per order", keys)
	}
	if !strings.HasPrefix(keys[0], "deploy/rstack/") || !strings.Contains(keys[0], "-ccccccc-cmd-") {
		t.Fatalf("key=%q, want deploy/rstack/<date>-<rev>-<order id>", keys[0])
	}
	if !strings.Contains(keys[2], "-"+to+"-cmd-") {
		// With no target rev the record is named by the revision a host reached.
		t.Fatalf("key=%q, want the reached revision when no target is given", keys[2])
	}
	var document struct {
		Kind string `json:"kind"`
		Body string `json:"body"`
	}
	if err := json.Unmarshal([]byte(bodies[0]), &document); err != nil || document.Kind != hubCheckoutDeployKind {
		t.Fatalf("document=%s: %v", bodies[0], err)
	}
	var record hubCheckoutDeployRecord
	if err := json.Unmarshal([]byte(document.Body), &record); err != nil {
		t.Fatalf("record body=%s: %v", document.Body, err)
	}
	if record.Kind != hubCheckoutRecordKind || record.Checkout != "rstack" || record.Rev != "ccccccc" || len(record.Hosts) != 1 || record.Hosts[0].Machine != "machine-a" {
		t.Fatalf("record=%+v", record)
	}
}

func TestHubCheckoutPullRecordFailureKeepsHostRows(t *testing.T) {
	hub, server := t14Hub(t)
	connection := t14Node(t, hub, server, false)
	_, client := newCheckoutPullHK(t, http.StatusInternalServerError)
	hub.handoffkeep = client

	results := checkoutPullOrder(t, server, map[string]any{"checkout": "rstack", "rev": ""})
	checkoutPullReplyPulled(t, connection, nodeCommandRead(t, connection), strings.Repeat("a", 40), strings.Repeat("b", 40))
	result := t14Result(t, results)
	var answer hubCheckoutPullResponse
	if result.status != http.StatusOK || json.Unmarshal(result.body, &answer) != nil {
		t.Fatalf("status=%d body=%s", result.status, result.body)
	}
	if answer.Record.Outcome != hubCheckoutRecordFailed || len(answer.Hosts) != 1 || answer.Hosts[0].Error != "" {
		t.Fatalf("answer=%+v, want failed record and the host row kept", answer)
	}
}

// TestHandoffkeepDocumentEndpointEncodesKeyOnce: the key's slashes stay %2F on
// the wire, and are not encoded a second time.
func TestHandoffkeepDocumentEndpointEncodesKeyOnce(t *testing.T) {
	client, err := newHandoffkeepRelayClient(hubHandoffkeepEnv{URL: "https://hk.example.test/base", Token: "t"}, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := handoffkeepDocumentEndpoint(client, "deploy/rstack/2026-11-13-abc1234")
	if err != nil {
		t.Fatal(err)
	}
	if want := "https://hk.example.test/base/v1/documents/deploy%2Frstack%2F2026-11-13-abc1234"; endpoint != want {
		t.Fatalf("endpoint=%s, want %s", endpoint, want)
	}
}

// TestHubCheckoutPullTimesOutSilentNode: a node that never answers is a
// node_timeout row once the order's deadline passes, not a hang.
func TestHubCheckoutPullTimesOutSilentNode(t *testing.T) {
	hub, server := t14Hub(t)
	hub.checkoutPullWait = 200 * time.Millisecond
	t14Node(t, hub, server, false)
	results := checkoutPullOrder(t, server, map[string]any{"checkout": "rstack", "rev": ""})
	result := t14Result(t, results)
	var answer hubCheckoutPullResponse
	if result.status != http.StatusOK || json.Unmarshal(result.body, &answer) != nil {
		t.Fatalf("status=%d body=%s", result.status, result.body)
	}
	if len(answer.Hosts) != 1 || answer.Hosts[0].Error != "node_timeout" {
		t.Fatalf("hosts=%+v, want node_timeout", answer.Hosts)
	}
}

func TestHubCheckoutPullRefusesBadRequestAndGenericEndpoint(t *testing.T) {
	hub, server := t14Hub(t)
	connection := t14Node(t, hub, server, false)
	for _, body := range []map[string]any{
		{"checkout": "../etc", "rev": ""},
		{"checkout": "rstack", "rev": "zz"},
		{"checkout": "rstack", "rev": "", "extra": 1},
	} {
		results := checkoutPullOrder(t, server, body)
		if result := t14Result(t, results); result.status != http.StatusBadRequest {
			t.Fatalf("body=%v status=%d %s, want 400", body, result.status, result.body)
		}
	}
	// The generic endpoint must not run the op: it would skip the deploy record.
	results := t14HTTPAsync(t, http.MethodPost, nodeCommandURL(server, "machine-a"), t14OperatorToken,
		map[string]any{"op": "checkout.pull", "args": map[string]any{"checkout": "rstack", "rev": ""}})
	if result := t14Result(t, results); result.status != http.StatusForbidden {
		t.Fatalf("generic endpoint status=%d %s, want 403", result.status, result.body)
	}
	// A bounded read that proves a negative: no node.command frame arrives in
	// this window. It is a deadline, not a sleep, and the refused call has
	// already returned by now.
	if nodeFrameOfType(t, connection, "node.command", 300*time.Millisecond) != nil {
		t.Fatal("generic endpoint reached the node")
	}
}

// nodeFrameOfType returns the first frame of kind that arrives within wait, or
// nil when none does. Other frames are skipped.
func nodeFrameOfType(t *testing.T, connection *websocket.Conn, kind string, wait time.Duration) map[string]json.RawMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), wait)
	defer cancel()
	for {
		_, payload, err := connection.Read(ctx)
		if err != nil {
			return nil
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(payload, &fields) != nil {
			continue
		}
		if string(fields["type"]) == `"`+kind+`"` {
			return fields
		}
	}
}

// TestHubAdvertisesCheckoutPull: the hub lists checkout_pull in hub.features,
// which is what lets a node send checkouts in its heartbeat.
func TestHubAdvertisesCheckoutPull(t *testing.T) {
	hub, server := t14Hub(t)
	connection := t14Node(t, hub, server, false)
	// The hub queues hub.features with a node's first heartbeat.
	if err := wsjson.Write(t.Context(), connection, map[string]any{"type": "event", "kind": "heartbeat", "payload": map[string]any{"status": "alive", "checks": map[string]any{}}}); err != nil {
		t.Fatal(err)
	}
	features := nodeFrameOfType(t, connection, "hub.features", 3*time.Second)
	if features == nil {
		t.Fatal("hub sent no hub.features frame")
	}
	if !strings.Contains(string(features["features"]), `"`+hubFeatureCheckoutPull+`"`) {
		t.Fatalf("hub.features=%s, want %s", features["features"], hubFeatureCheckoutPull)
	}
}

func TestHubHeartbeatCheckoutRevisionsReachNodes(t *testing.T) {
	hub, server := t14Hub(t)
	connection := t14Node(t, hub, server, false)
	heartbeat := map[string]any{"type": "event", "kind": "heartbeat", "payload": map[string]any{
		"status": "alive", "checks": map[string]any{},
		"checkouts": map[string]any{"rstack": map[string]any{"sha": "abc1234", "branch": "main"}, "docs": map[string]any{"sha": "def5678", "branch": ""}},
	}}
	if err := wsjson.Write(t.Context(), connection, heartbeat); err != nil {
		t.Fatal(err)
	}
	eventually(t, 3*time.Second, "checkouts in /v1/nodes", func() bool {
		for _, node := range hub.Nodes() {
			if node.MachineID == "machine-a" && node.Checkouts["rstack"].Sha == "abc1234" && node.Checkouts["docs"].Sha == "def5678" {
				return true
			}
		}
		return false
	})
	status, _, body := t14HTTP(t, http.MethodGet, server.URL+"/v1/nodes", t14OperatorToken, nil)
	if status != http.StatusOK || !strings.Contains(string(body), `"checkouts":{"docs":{"sha":"def5678","branch":""},"rstack":{"sha":"abc1234","branch":"main"}}`) {
		t.Fatalf("/v1/nodes status=%d body=%s", status, body)
	}
	if got := renderHubCheckouts(map[string]HubCheckoutRev{"rstack": {Sha: "abc1234", Branch: "main"}, "docs": {Sha: "def5678"}}); got != "docs=def5678 rstack=abc1234@main" {
		t.Fatalf("rendered checkouts=%q", got)
	}
}

func TestHubHeartbeatRejectsInvalidCheckouts(t *testing.T) {
	base := `{"status":"alive","checks":{},"checkouts":%s}`
	for _, bad := range []string{
		`{"Bad Name":{"sha":"abc1234","branch":"main"}}`,
		`{"rstack":{"sha":"NOTHEX!","branch":"main"}}`,
		`{"rstack":{"sha":"abc1234","branch":"bad..branch"}}`,
		`null`,
	} {
		if _, valid := decodeHubHeartbeatPayload([]byte(strings.Replace(base, "%s", bad, 1))); valid {
			t.Fatalf("accepted heartbeat with checkouts %s", bad)
		}
	}
	if payload, valid := decodeHubHeartbeatPayload([]byte(strings.Replace(base, "%s", `{"rstack":{"sha":"abc1234","branch":""}}`, 1))); !valid || payload.Checkouts["rstack"].Sha != "abc1234" {
		t.Fatalf("valid heartbeat rejected or dropped checkouts: %+v", payload.Checkouts)
	}
}

func TestNodeAdvertisesCheckoutPullOnlyToAcceptingHub(t *testing.T) {
	client := &HubClient{}
	if client.checkoutPeerAccepted() {
		t.Fatal("no hub connection accepts checkouts")
	}
	peer := &hubClientConnection{}
	client.signalsPeer.Store(peer)
	if client.checkoutPeerAccepted() {
		t.Fatal("a hub that did not list checkout_pull received checkouts")
	}
	peer.acceptsCheckoutPull.Store(true)
	if !client.checkoutPeerAccepted() {
		t.Fatal("a hub that listed checkout_pull did not receive checkouts")
	}
}

// TestHubDeployKeyIgnoresRefusedRevisions: a refused host's to is never the
// deployed revision, so it cannot name the record.
func TestHubDeployKeyIgnoresRefusedRevisions(t *testing.T) {
	stale := strings.Repeat("1", 40)
	reached := strings.Repeat("2", 40)
	rows := []hubCheckoutPullHost{
		{Machine: "machine-a", Result: json.RawMessage(`{"outcome":"refused","reason":"not-fast-forward","to":"` + stale + `"}`)},
		{Machine: "machine-b", Result: json.RawMessage(`{"outcome":"pulled","to":"` + reached + `"}`)},
	}
	if got := firstCheckoutPullTarget(rows); got != reached {
		t.Fatalf("target=%q, want the pulled host's %s", got, reached)
	}
	if got := firstCheckoutPullTarget(rows[:1]); got != "" {
		t.Fatalf("target=%q from refusals only, want none", got)
	}
}

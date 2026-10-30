package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func nodeCommandURL(server *httptest.Server, machine string) string {
	return server.URL + "/v1/nodes/" + machine + "/command"
}

func nodeCommandRead(t *testing.T, connection *websocket.Conn) map[string]json.RawMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	for {
		_, payload, err := connection.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(payload, &fields); err != nil {
			t.Fatal(err)
		}
		if string(fields["type"]) == `"node.command"` {
			return fields
		}
	}
}

func nodeCommandReplyTo(t *testing.T, connection *websocket.Conn, command map[string]json.RawMessage, body map[string]any) {
	t.Helper()
	var requestID, op string
	_ = json.Unmarshal(command["request_id"], &requestID)
	_ = json.Unmarshal(command["op"], &op)
	message := map[string]any{"type": "node.command.result", "request_id": requestID, "op": op}
	for key, value := range body {
		message[key] = value
	}
	if err := wsjson.Write(t.Context(), connection, message); err != nil {
		t.Fatal(err)
	}
}

func nodeCommandDecode(t *testing.T, contents []byte) map[string]any {
	t.Helper()
	var decoded map[string]any
	if err := json.Unmarshal(contents, &decoded); err != nil {
		t.Fatalf("body=%s err=%v", contents, err)
	}
	return decoded
}

var nodeCommandCloseBody = map[string]any{"op": "pane.close", "args": map[string]any{"pane_id": "w1:p1", "name": "job-a", "agent_status": "idle", "cwd": "/work/a"}}

func TestHubNodeCommandRoundTrip(t *testing.T) {
	hub, server := t14Hub(t)
	connection := t14Node(t, hub, server, false) // commands do not need accepting
	results := t14HTTPAsync(t, http.MethodPost, nodeCommandURL(server, "machine-a"), t14OperatorToken, nodeCommandCloseBody)
	command := nodeCommandRead(t, connection)
	if len(command) != 4 || string(command["op"]) != `"pane.close"` || string(command["args"]) != `{"agent_status":"idle","cwd":"/work/a","name":"job-a","pane_id":"w1:p1"}` {
		t.Fatalf("node received %v", command)
	}
	nodeCommandReplyTo(t, connection, command, map[string]any{"result": map[string]any{"outcome": "closed"}})
	result := t14Result(t, results)
	body := nodeCommandDecode(t, result.body)
	var requestID string
	_ = json.Unmarshal(command["request_id"], &requestID)
	if result.status != http.StatusOK || body["machine"] != "machine-a" || body["op"] != "pane.close" || body["request_id"] != requestID || body["result"].(map[string]any)["outcome"] != "closed" || body["error"] != nil {
		t.Fatalf("status=%d body=%s", result.status, result.body)
	}

	// A node error answers 502 with the node's closed code.
	results = t14HTTPAsync(t, http.MethodPost, nodeCommandURL(server, "machine-a"), t14OperatorToken, nodeCommandCloseBody)
	nodeCommandReplyTo(t, connection, nodeCommandRead(t, connection), map[string]any{"error": "close_failed"})
	if result := t14Result(t, results); result.status != http.StatusBadGateway || nodeCommandDecode(t, result.body)["error"] != "close_failed" {
		t.Fatalf("status=%d body=%s", result.status, result.body)
	}
}

func TestHubNodeCommandRejectsBeforeSending(t *testing.T) {
	hub, server := t14Hub(t)
	connection := t14Node(t, hub, server, true)
	if status, _, _ := t14HTTP(t, http.MethodPost, nodeCommandURL(server, "machine-a"), "node-token", nodeCommandCloseBody); status != http.StatusUnauthorized {
		t.Fatalf("node token status=%d", status)
	}
	for _, body := range []any{
		map[string]any{"op": "shell.run", "args": map[string]any{}},
		map[string]any{"op": "pane.close", "args": map[string]any{"pane_id": "w1:p1"}},
		map[string]any{"op": "pane.close", "args": map[string]any{"pane_id": "w1:p1", "name": "job-a", "agent_status": "idle", "cwd": "/work/a", "force": true}},
		map[string]any{"op": "worktree.inspect", "args": map[string]any{"cwd": "relative"}},
		map[string]any{"op": "agents.list", "extra": 1},
		map[string]any{"op": "pane.capture", "args": map[string]any{"pane_id": "w1:p1", "name": "job-a", "cwd": "/work/a"}},
		map[string]any{"op": "pane.capture", "args": map[string]any{"pane_id": "w1:p1", "name": "job-a", "cwd": "/work/a", "merged_after": 0, "write": nil}},
	} {
		if status, _, contents := t14HTTP(t, http.MethodPost, nodeCommandURL(server, "machine-a"), t14OperatorToken, body); status != http.StatusBadRequest {
			t.Fatalf("body=%v status=%d response=%s", body, status, contents)
		}
	}
	if status, _, contents := t14HTTP(t, http.MethodPost, nodeCommandURL(server, "machine-b"), t14OperatorToken, nodeCommandCloseBody); status != http.StatusServiceUnavailable || nodeCommandDecode(t, contents)["error"] != "node_unavailable" {
		t.Fatalf("unknown node status=%d body=%s", status, contents)
	}
	// Nothing reached the node: the next frame it reads is the command below.
	results := t14HTTPAsync(t, http.MethodPost, nodeCommandURL(server, "machine-a"), t14OperatorToken, map[string]any{"op": "agents.list"})
	if command := nodeCommandRead(t, connection); string(command["op"]) != `"agents.list"` || string(command["args"]) != `{}` {
		t.Fatalf("first command the node saw: %v", command)
	}
	_ = connection.CloseNow()
	if result := t14Result(t, results); result.status != http.StatusServiceUnavailable || nodeCommandDecode(t, result.body)["error"] != "node_lost" {
		t.Fatalf("lost node status=%d body=%s", result.status, result.body)
	}
}

func TestHubNodeCommandTimeoutDropsMalformedAndLateResults(t *testing.T) {
	hub, server := t14Hub(t)
	hub.nodeCommandWait = 200 * time.Millisecond
	connection := t14Node(t, hub, server, true)
	results := t14HTTPAsync(t, http.MethodPost, nodeCommandURL(server, "machine-a"), t14OperatorToken, nodeCommandCloseBody)
	command := nodeCommandRead(t, connection)
	// An outcome outside the closed set never resolves the request.
	nodeCommandReplyTo(t, connection, command, map[string]any{"result": map[string]any{"outcome": "deleted"}})
	result := t14Result(t, results)
	if result.status != http.StatusGatewayTimeout || nodeCommandDecode(t, result.body)["error"] != "node_timeout" {
		t.Fatalf("status=%d body=%s", result.status, result.body)
	}
	nodeCommandReplyTo(t, connection, command, map[string]any{"result": map[string]any{"outcome": "closed"}})
	eventually(t, time.Second, "pending command cleared", func() bool {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		return len(hub.nodeCommands) == 0
	})
}

func TestHubNodesAgentsFanOut(t *testing.T) {
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token", "machine-b": "node-token-b"}})
	if err != nil {
		t.Fatal(err)
	}
	hub.nodeAgentsWait = 300 * time.Millisecond
	server := httptest.NewServer(hub.Handler())
	t.Cleanup(server.Close)
	connection := t14Node(t, hub, server, true)
	hub.mu.Lock()
	hub.nodes["machine-c"] = &hubNodeRecord{machineID: "machine-c", state: "disconnected"}
	hub.mu.Unlock()
	silent := nodeCommandDial(t, hub, server, "machine-b", "node-token-b")

	results := t14HTTPAsync(t, http.MethodGet, server.URL+"/v1/nodes/agents", t14OperatorToken, nil)
	nodeCommandReplyTo(t, connection, nodeCommandRead(t, connection), map[string]any{"result": map[string]any{
		"agents":   []any{map[string]any{"pane_id": "w1:p1", "name": "job-a", "agent_status": "idle", "agent": "claude", "cwd": "/work/a"}},
		"panes":    []any{map[string]any{"pane_id": "w1:p1", "agent_status": "idle", "cwd": "/work/a"}},
		"panes_ok": true,
	}})
	_ = nodeCommandRead(t, silent) // received, never answered
	result := t14Result(t, results)
	var body struct {
		Nodes []hubNodeAgentsRow `json:"nodes"`
	}
	if err := json.Unmarshal(result.body, &body); err != nil || result.status != http.StatusOK || len(body.Nodes) != 3 {
		t.Fatalf("status=%d body=%s err=%v", result.status, result.body, err)
	}
	a, b, c := body.Nodes[0], body.Nodes[1], body.Nodes[2]
	if a.MachineID != "machine-a" || a.Error != "" || len(a.Agents) != 1 || a.Agents[0].Name != "job-a" || a.Agents[0].Cwd != "/work/a" || !a.PanesOK || len(a.Panes) != 1 {
		t.Fatalf("answering node row=%+v", a)
	}
	if b.MachineID != "machine-b" || b.Error != "node_timeout" || b.Agents != nil {
		t.Fatalf("silent node row=%+v", b)
	}
	if c.MachineID != "machine-c" || c.State != "disconnected" || c.Error != "node_unavailable" {
		t.Fatalf("disconnected node row=%+v", c)
	}
	if status, _, _ := t14HTTP(t, http.MethodGet, server.URL+"/v1/nodes/agents", "", nil); status != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status=%d", status)
	}
}

func nodeCommandDial(t *testing.T, hub *HubServer, server *httptest.Server, machine, token string) *websocket.Conn {
	t.Helper()
	headers := http.Header{}
	headers.Set(hubMachineIDHeader, machine)
	headers.Set(hubAuthorizationHeader, "Bearer "+token)
	connection, _, err := websocket.Dial(t.Context(), t14WSURL(server.URL, "/v1/agent"), &websocket.DialOptions{HTTPHeader: headers})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.CloseNow() })
	if err := wsjson.Write(t.Context(), connection, map[string]any{"type": "hello", "machine_id": machine, "version": "fixture", "accepting": true}); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, machine+" connected", func() bool {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		record := hub.nodes[machine]
		return record != nil && record.agent != nil && record.state == "connected"
	})
	return connection
}

// TestNodeCommandEndToEnd drives a real HubClient against a real hub: the
// close request crosses HTTP, the hub, the node's herdr re-read and back.
func TestNodeCommandEndToEnd(t *testing.T) {
	hub, server := t14Hub(t)
	fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "idle", "/work/a"))}}
	client, err := NewHubClient(HubClientConfig{
		URL: t14WSURL(server.URL, ""), MachineID: "machine-a", Token: "node-token", AllowInsecureForTests: true, PingInterval: time.Hour,
		hostLoadCollector:   func(context.Context) (HubHostLoad, error) { return HubHostLoad{}, errors.New("fixture") },
		hostMemoryCollector: func(context.Context) (*HubHostMemory, error) { return nil, errors.New("fixture") },
		sessionCollector:    func(context.Context) ([]HerdrAgentState, error) { return nil, errors.New("fixture") },
	})
	if err != nil {
		t.Fatal(err)
	}
	client.nodeHerdr = fake.run
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.Run(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()
	eventually(t, 2*time.Second, "node connected", func() bool {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		record := hub.nodes["machine-a"]
		return record != nil && record.agent != nil && record.state == "connected"
	})

	status, _, contents := t14HTTP(t, http.MethodPost, nodeCommandURL(server, "machine-a"), t14OperatorToken, nodeCommandCloseBody)
	if status != http.StatusOK || !strings.Contains(string(contents), `"result":{"outcome":"closed"}`) || fake.called("pane", "close", "w1:p1") != 1 {
		t.Fatalf("status=%d body=%s calls=%v", status, contents, fake.calls)
	}
	status, _, contents = t14HTTP(t, http.MethodGet, server.URL+"/v1/nodes/agents", t14OperatorToken, nil)
	if status != http.StatusOK || !strings.Contains(string(contents), `"pane_id":"w1:p1","name":"job-a","agent_status":"idle","agent":"claude","cwd":"/work/a"`) {
		t.Fatalf("status=%d body=%s", status, contents)
	}
	// The node re-reads herdr: a pane that went back to work is not closed.
	fake.mu.Lock()
	fake.agents = []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "working", "/work/a"))}
	fake.agentRead = 0
	fake.mu.Unlock()
	status, _, contents = t14HTTP(t, http.MethodPost, nodeCommandURL(server, "machine-a"), t14OperatorToken, nodeCommandCloseBody)
	if status != http.StatusOK || !strings.Contains(string(contents), `"outcome":"pane-changed"`) || fake.called("pane", "close") != 1 {
		t.Fatalf("status=%d body=%s calls=%v", status, contents, fake.calls)
	}
}

// TestHubNodeCommandStatelessRecordIsUnavailable: a node record with no state
// and no connection must answer node_unavailable, never reach a nil agent.
func TestHubNodeCommandStatelessRecordIsUnavailable(t *testing.T) {
	hub, server := t14Hub(t)
	hub.mu.Lock()
	hub.nodes["machine-z"] = &hubNodeRecord{machineID: "machine-z"}
	hub.nodes["machine-y"] = &hubNodeRecord{machineID: "machine-y", state: "connected"}
	hub.mu.Unlock()
	for _, machine := range []string{"machine-z", "machine-y"} {
		status, _, contents := t14HTTP(t, http.MethodPost, nodeCommandURL(server, machine), t14OperatorToken, map[string]any{"op": "agents.list"})
		body := nodeCommandDecode(t, contents)
		if status != http.StatusServiceUnavailable || body["error"] != "node_unavailable" || body["state"] != "disconnected" {
			t.Fatalf("%s status=%d body=%s", machine, status, contents)
		}
	}
	status, _, contents := t14HTTP(t, http.MethodGet, server.URL+"/v1/nodes/agents", t14OperatorToken, nil)
	var body struct {
		Nodes []hubNodeAgentsRow `json:"nodes"`
	}
	if err := json.Unmarshal(contents, &body); err != nil || status != http.StatusOK || len(body.Nodes) != 2 {
		t.Fatalf("status=%d body=%s err=%v", status, contents, err)
	}
	for _, row := range body.Nodes {
		if row.State != "disconnected" || row.Error != "node_unavailable" {
			t.Fatalf("row=%+v", row)
		}
	}
}

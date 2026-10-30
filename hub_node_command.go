package panewire

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sort"
	"time"
)

// The hub half of node commands (docs/node-commands.md). The hub keeps a
// pending command only in memory while its HTTP caller waits; it never
// retries, persists, or acts on a result itself.

const (
	hubNodeCommandWait     = 30 * time.Second
	hubNodeAgentsWait      = 10 * time.Second
	hubNodeCommandMaxBytes = 32 << 10
)

type hubNodeCommandResult struct {
	RequestID string
	Op        string
	Result    json.RawMessage
	Error     string
}

type hubNodeCommandPending struct {
	machine string
	agent   *hubAgent
	op      string
	done    chan hubNodeCommandResult
}

type hubNodeCommandRequest struct {
	Op   string          `json:"op"`
	Args json.RawMessage `json:"args,omitempty"`
}

type hubNodeAgentsRow struct {
	MachineID string         `json:"machine_id"`
	State     string         `json:"state"`
	Agents    []nodeAgentRow `json:"agents"`
	Panes     []nodeAgentRow `json:"panes"`
	PanesOK   bool           `json:"panes_ok"`
	Error     string         `json:"error,omitempty"`
}

func hubNodeWait(configured, fallback time.Duration) time.Duration {
	if configured > 0 {
		return configured
	}
	return fallback
}

func newHubNodeCommandID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "cmd-" + time.Now().UTC().Format("20060102150405.000000000")
	}
	return "cmd-" + hex.EncodeToString(buf)
}

// parseHubNodeCommandResult is separate from parseHubInbound for the same
// reason as spawn results: it is a control response, not an event envelope.
// A result is re-encoded through its op's documented shape.
func parseHubNodeCommandResult(raw []byte) (hubNodeCommandResult, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 4 {
		return hubNodeCommandResult{}, false
	}
	var kind string
	var result hubNodeCommandResult
	if json.Unmarshal(fields["type"], &kind) != nil || kind != "node.command.result" ||
		json.Unmarshal(fields["request_id"], &result.RequestID) != nil || !validHubRequestID(result.RequestID) ||
		json.Unmarshal(fields["op"], &result.Op) != nil || !knownNodeCommandOp(result.Op) {
		return hubNodeCommandResult{}, false
	}
	if rawError, found := fields["error"]; found {
		if json.Unmarshal(rawError, &result.Error) != nil || !validNodeCommandError(result.Error) {
			return hubNodeCommandResult{}, false
		}
		return result, true
	}
	canonical, valid := canonicalNodeCommandResult(result.Op, fields["result"])
	if !valid {
		return hubNodeCommandResult{}, false
	}
	result.Result = canonical
	return result, true
}

// nodeCommandStateLocked names why a machine cannot take a command, or "" when
// it can. Commands do not require accepting: reaping a draining node is the
// common case.
func (h *HubServer) nodeCommandStateLocked(machine string) (*hubAgent, string) {
	record := h.nodes[machine]
	switch {
	case record == nil:
		return nil, "disconnected"
	case record.agent != nil && record.state == "connected":
		return record.agent, ""
	case record.state == "" || record.state == "connected":
		// A record without a usable connection always names a state, so a
		// nil agent can never read as available.
		return nil, "disconnected"
	}
	return nil, record.state
}

// sendNodeCommand registers and sends one command. A non-empty state means the
// node was unavailable and nothing was sent.
func (h *HubServer) sendNodeCommand(machine, op string, args json.RawMessage) (*hubNodeCommandPending, string, string) {
	h.mu.Lock()
	agent, state := h.nodeCommandStateLocked(machine)
	if state != "" {
		h.mu.Unlock()
		return nil, "", state
	}
	if h.nodeCommands == nil {
		h.nodeCommands = make(map[string]*hubNodeCommandPending)
	}
	requestID := newHubNodeCommandID()
	pending := &hubNodeCommandPending{machine: machine, agent: agent, op: op, done: make(chan hubNodeCommandResult, 1)}
	h.nodeCommands[requestID] = pending
	h.mu.Unlock()
	message := struct {
		Type      string          `json:"type"`
		RequestID string          `json:"request_id"`
		Op        string          `json:"op"`
		Args      json.RawMessage `json:"args"`
	}{Type: "node.command", RequestID: requestID, Op: op, Args: args}
	if err := agent.writeJSON(message); err != nil {
		h.forgetNodeCommand(requestID)
		return nil, "", "disconnected"
	}
	return pending, requestID, ""
}

func (h *HubServer) forgetNodeCommand(requestID string) {
	h.mu.Lock()
	delete(h.nodeCommands, requestID)
	h.mu.Unlock()
}

// awaitNodeCommand waits for the node's answer; a timeout or caller cancel
// reads as node_timeout and the late answer is dropped.
func (h *HubServer) awaitNodeCommand(request *http.Request, requestID string, pending *hubNodeCommandPending, timer <-chan time.Time) hubNodeCommandResult {
	select {
	case result := <-pending.done:
		return result
	case <-timer:
	case <-request.Context().Done():
	}
	h.forgetNodeCommand(requestID)
	return hubNodeCommandResult{RequestID: requestID, Op: pending.op, Error: "node_timeout"}
}

func (h *HubServer) resolveNodeCommand(machine string, agent *hubAgent, result hubNodeCommandResult) {
	h.mu.Lock()
	pending := h.nodeCommands[result.RequestID]
	if pending == nil || pending.machine != machine || pending.agent != agent || pending.op != result.Op {
		h.mu.Unlock()
		return
	}
	delete(h.nodeCommands, result.RequestID)
	h.mu.Unlock()
	pending.done <- result
}

// loseNodeCommandsLocked removes agent's pending commands; the caller holds
// h.mu and wakes them with wakeLostNodeCommands after releasing it.
func (h *HubServer) loseNodeCommandsLocked(agent *hubAgent) []*hubNodeCommandPending {
	var lost []*hubNodeCommandPending
	for requestID, pending := range h.nodeCommands {
		if pending.agent == agent {
			delete(h.nodeCommands, requestID)
			lost = append(lost, pending)
		}
	}
	return lost
}

func wakeLostNodeCommands(lost []*hubNodeCommandPending) {
	for _, pending := range lost {
		pending.done <- hubNodeCommandResult{Op: pending.op, Error: "node_lost"}
	}
}

func (h *HubServer) handleNodeCommand(writer http.ResponseWriter, request *http.Request) {
	if !h.authorizeOperator(request) {
		hubUnauthorized(writer)
		return
	}
	machine := request.PathValue("machine")
	var body hubNodeCommandRequest
	if !machineIDPattern.MatchString(machine) || !decodeHubJSON(writer, request, hubNodeCommandMaxBytes, &body) || !knownNodeCommandOp(body.Op) {
		writeJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}
	if len(body.Args) == 0 {
		body.Args = json.RawMessage("{}")
	}
	if _, valid := decodeNodeCommandArgs(body.Op, body.Args); !valid {
		writeJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}
	pending, requestID, state := h.sendNodeCommand(machine, body.Op, body.Args)
	if state != "" {
		writeJSON(writer, http.StatusServiceUnavailable, map[string]string{"error": "node_unavailable", "state": state})
		return
	}
	timer := time.NewTimer(hubNodeWait(h.nodeCommandWait, hubNodeCommandWait))
	defer timer.Stop()
	result := h.awaitNodeCommand(request, requestID, pending, timer.C)
	response := struct {
		RequestID string          `json:"request_id"`
		Machine   string          `json:"machine"`
		Op        string          `json:"op"`
		Result    json.RawMessage `json:"result,omitempty"`
		Error     string          `json:"error,omitempty"`
	}{RequestID: requestID, Machine: machine, Op: body.Op, Result: result.Result, Error: result.Error}
	status := http.StatusOK
	switch result.Error {
	case "":
	case "node_timeout":
		status = http.StatusGatewayTimeout
	case "node_lost":
		status = http.StatusServiceUnavailable
	case "invalid_args":
		status = http.StatusBadRequest
	default:
		status = http.StatusBadGateway
	}
	writeJSON(writer, status, response)
}

// handleNodesAgents fans agents.list out to every known node and answers one
// row per machine. A node that is down or does not answer in time keeps its
// row with an error, so a caller never mistakes it for a node with no panes.
func (h *HubServer) handleNodesAgents(writer http.ResponseWriter, request *http.Request) {
	if !h.authorizeOperator(request) {
		hubUnauthorized(writer)
		return
	}
	h.mu.Lock()
	machines := make([]string, 0, len(h.nodes))
	for machine := range h.nodes {
		machines = append(machines, machine)
	}
	h.mu.Unlock()
	sort.Strings(machines)

	type inflight struct {
		pending   *hubNodeCommandPending
		requestID string
	}
	rows := make([]hubNodeAgentsRow, len(machines))
	waits := make([]inflight, len(machines))
	for index, machine := range machines {
		rows[index] = hubNodeAgentsRow{MachineID: machine, State: "connected"}
		pending, requestID, state := h.sendNodeCommand(machine, nodeCommandAgentsList, json.RawMessage("{}"))
		if state != "" {
			rows[index].State, rows[index].Error = state, "node_unavailable"
			continue
		}
		waits[index] = inflight{pending: pending, requestID: requestID}
	}
	timer := time.NewTimer(hubNodeWait(h.nodeAgentsWait, hubNodeAgentsWait))
	defer timer.Stop()
	expired := false
	for index, wait := range waits {
		if wait.pending == nil {
			continue
		}
		var result hubNodeCommandResult
		if expired {
			select {
			case result = <-wait.pending.done:
			default:
				h.forgetNodeCommand(wait.requestID)
				result.Error = "node_timeout"
			}
		} else {
			result = h.awaitNodeCommand(request, wait.requestID, wait.pending, timer.C)
			expired = result.Error == "node_timeout"
		}
		if result.Error != "" {
			rows[index].Error = result.Error
			continue
		}
		var list nodeAgentsListResult
		if json.Unmarshal(result.Result, &list) != nil {
			rows[index].Error = "invalid_result"
			continue
		}
		rows[index].Agents, rows[index].Panes, rows[index].PanesOK = list.Agents, list.Panes, list.PanesOK
	}
	writeJSON(writer, http.StatusOK, struct {
		Nodes []hubNodeAgentsRow `json:"nodes"`
	}{Nodes: rows})
}

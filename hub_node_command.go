package panewire

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
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
	Detail    string
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
	if json.Unmarshal(raw, &fields) != nil || (len(fields) != 4 && len(fields) != 5) {
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
		if detail, found := fields["detail"]; found {
			if len(fields) != 5 || result.Error != "done_failed" || json.Unmarshal(detail, &result.Detail) != nil || len(result.Detail) > 240 || !utf8.ValidString(result.Detail) || strings.ContainsAny(result.Detail, "\r\n") {
				return hubNodeCommandResult{}, false
			}
		} else if len(fields) != 4 {
			return hubNodeCommandResult{}, false
		}
		return result, true
	}
	if len(fields) != 4 {
		return hubNodeCommandResult{}, false
	}
	canonical, valid := canonicalNodeCommandResult(result.Op, fields["result"])
	if !valid {
		return hubNodeCommandResult{}, false
	}
	result.Result = canonical
	return result, true
}

// nodeCommandTargetLocked returns the machine's live agent, or nil when there
// is no connection to send on, together with the presence state to report,
// which is never empty. Availability is the live connection alone: a node
// that is stale (late heartbeat) or not accepting work still takes commands —
// reaping a draining node is the common case — and its state is reported as
// is. A dead connection that the hub has not yet noticed answers node_lost or
// node_timeout, never a silent success.
func (h *HubServer) nodeCommandTargetLocked(machine string) (*hubAgent, string) {
	record := h.nodes[machine]
	switch {
	case record == nil:
		return nil, "disconnected"
	case record.agent != nil:
		state := record.state
		if state == "" {
			state = "connected"
		}
		return record.agent, state
	case record.state == "" || record.state == "connected":
		// No connection means not connected, whatever the record says.
		return nil, "disconnected"
	}
	return nil, record.state
}

// sendNodeCommand registers and sends one command and returns the node's
// state. sent is false when the node had no live connection and nothing was
// sent.
func (h *HubServer) sendNodeCommand(machine, op string, args json.RawMessage) (pending *hubNodeCommandPending, requestID, state string, sent bool) {
	h.mu.Lock()
	agent, state := h.nodeCommandTargetLocked(machine)
	if agent == nil {
		h.mu.Unlock()
		return nil, "", state, false
	}
	if h.nodeCommands == nil {
		h.nodeCommands = make(map[string]*hubNodeCommandPending)
	}
	requestID = newHubNodeCommandID()
	pending = &hubNodeCommandPending{machine: machine, agent: agent, op: op, done: make(chan hubNodeCommandResult, 1)}
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
		// A write that failed may still have delivered a command that acts, so
		// the two acting job commands report an unknown outcome rather than
		// "nothing was sent"; every other op answers a plain disconnect.
		if op == nodeCommandJobClose || op == nodeCommandJobReap {
			return nil, "", "send_failed", false
		}
		return nil, "", "disconnected", false
	}
	return pending, requestID, state, true
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
	// A node credential reaches this route only for pr.status, which
	// authorizeNodeCommand enforces once the body names the op.
	if !h.authenticateScoped(request) && h.authenticate(request).kind != hubCallerNode {
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
	decoded, valid := decodeNodeCommandArgs(body.Op, body.Args)
	logReap := func(outcome string) {
		if nodeCommandAudited(body.Op) {
			pane, job := nodeReapCommandSubject(decoded)
			h.logger.Info("node command", "client", h.authenticate(request).id, "machine", machine, "op", body.Op, "pane", pane, "job", job, "outcome", outcome)
		}
	}
	if !valid {
		logReap("invalid_request")
		writeJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}
	if access := h.authorizeNodeCommand(request, body.Op, machine); access != hubAccessGranted {
		logReap("scope_forbidden")
		writeHubAccessDenied(writer, access)
		return
	}
	if _, isStatus := decoded.(nodePRStatusArgs); isStatus && machine != h.prStatusHost {
		// Only the machine that hosts mergewake can answer; naming any other
		// is refused before a node sees it, whatever the caller's scope.
		writeJSON(writer, http.StatusBadRequest, map[string]string{"error": "wrong_machine"})
		return
	}
	commandStart := h.now().UTC()
	pending, requestID, state, sent := h.sendNodeCommand(machine, body.Op, body.Args)
	if !sent {
		logReap("node_unavailable")
		writeJSON(writer, http.StatusServiceUnavailable, map[string]string{"error": "node_unavailable", "state": state})
		return
	}
	timer := time.NewTimer(hubNodeWait(h.nodeCommandWait, hubNodeCommandWait))
	defer timer.Stop()
	result := h.awaitNodeCommand(request, requestID, pending, timer.C)
	logReap(nodeCommandOutcome(result))
	h.releaseClosedPaneLanes(machine, decoded, result, commandStart)
	response := struct {
		RequestID string          `json:"request_id"`
		Machine   string          `json:"machine"`
		Op        string          `json:"op"`
		Result    json.RawMessage `json:"result,omitempty"`
		Error     string          `json:"error,omitempty"`
		Detail    string          `json:"detail,omitempty"`
	}{RequestID: requestID, Machine: machine, Op: body.Op, Result: result.Result, Error: result.Error, Detail: result.Detail}
	status := http.StatusOK
	switch result.Error {
	case "":
	case "node_timeout":
		status = http.StatusGatewayTimeout
	case "node_lost":
		status = http.StatusServiceUnavailable
	}
	if body.Op == nodeCommandJobResult {
		encoded, err := marshalNodeCommandJSON(response)
		if err == nil {
			writeJSONBytes(writer, status, append(encoded, '\n'))
			return
		}
	}
	writeJSON(writer, status, response)
}

// authorizeNodeCommand adds one rule to authorize: a reap-only op is for a
// service whose scopes name it, so the operator credential is refused too.
func (h *HubServer) authorizeNodeCommand(request *http.Request, op, machine string) hubAccess {
	if op == nodeCommandJobMarkLost || op == nodeCommandJobClose || op == nodeCommandJobReap || op == nodeCommandJobPoolAction {
		// These commands are issued by the hub after its own checks; none is
		// available through the public node-command endpoint.
		return hubAccessForbidden
	}
	if op == nodeCommandCheckoutPull {
		// checkout.pull runs only through POST /v1/fleet/checkout-pull, which
		// writes the order's deploy record; this endpoint would skip it.
		return hubAccessForbidden
	}
	if h.authenticate(request).kind == hubCallerNode {
		// The local node daemon asks for PR state on a worker's behalf, so a
		// node may run the read-only pr.status op and nothing else here.
		if op == nodeCommandPRStatus {
			return hubAccessGranted
		}
		return hubAccessDenied
	}
	if nodeCommandReapOnly(op) && h.authenticate(request).kind == hubCallerOperator {
		return hubAccessForbidden
	}
	if op == nodeCommandWorktreeList || op == nodeCommandJobResult {
		op = nodeCommandWorktreeInspect
	}
	return h.authorize(request, hubScopeNodeCommand+op, machine)
}

// nodeCommandOutcome names a result for the log from its closed vocabulary:
// the node's error code or the result's outcome, never any other result field.
func nodeCommandOutcome(result hubNodeCommandResult) string {
	if result.Error != "" {
		return result.Error
	}
	var named struct {
		Outcome string `json:"outcome"`
	}
	_ = json.Unmarshal(result.Result, &named)
	return named.Outcome
}

// handleNodesAgents fans agents.list out to every known node and answers one
// row per machine. A node that is down or does not answer in time keeps its
// row with an error, so a caller never mistakes it for a node with no panes.
func (h *HubServer) handleNodesAgents(writer http.ResponseWriter, request *http.Request) {
	if access := h.authorize(request, hubScopeNodesAgentsRead, ""); access != hubAccessGranted {
		writeHubAccessDenied(writer, access)
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
		pending, requestID, state, sent := h.sendNodeCommand(machine, nodeCommandAgentsList, json.RawMessage("{}"))
		rows[index] = hubNodeAgentsRow{MachineID: machine, State: state}
		if !sent {
			rows[index].Error = "node_unavailable"
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

// releaseClosedPaneLanes is the hub half of every reap that closes a job pane
// through pane.close: once the node reports the pane closed or already gone,
// the machine's lane rows for it have nothing left to route to.
func (h *HubServer) releaseClosedPaneLanes(machine string, args any, result hubNodeCommandResult, commandStart time.Time) {
	closeArgs, isClose := args.(nodePaneCloseArgs)
	if !isClose || result.Error != "" {
		return
	}
	var closed nodePaneCloseResult
	if json.Unmarshal(result.Result, &closed) != nil || (closed.Outcome != nodePaneClosed && closed.Outcome != nodePaneGone) {
		return
	}
	h.releasePaneLanes(machine, closeArgs.PaneID, commandStart)
}

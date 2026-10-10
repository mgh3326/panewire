package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

// The MCP surface is JSON-RPC 2.0 over streamable HTTP, mirroring
// handoffkeep's internal/mcp approach but without any third-party module:
// a single POST endpoint, one JSON response object per request (a
// spec-legal alternative to an SSE stream), no sessions. Notifications get
// 202. GET/DELETE — the optional server-stream verbs — get 405.

// assistantProtocolVersion is the MCP revision this server speaks; the
// initialize reply echoes the client's version when known and this one
// otherwise.
const assistantProtocolVersion = "2025-06-18"

// assistantPollContract is the no-push contract, carried verbatim in the
// poll tool's description, the initialize instructions and
// docs/assistant-mcp.md. Keep the sentences byte-identical everywhere.
const assistantPollContract = "This server never pushes. Being registered only means the tools can be called; nothing is delivered unless you call. Missing a poll means missing a deadline. A deadline passing is never consent."

type assistantRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type assistantRPCResponse struct {
	JSONRPC string             `json:"jsonrpc"`
	ID      json.RawMessage    `json:"id"`
	Result  any                `json:"result,omitempty"`
	Error   *assistantRPCError `json:"error,omitempty"`
}

type assistantRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

const (
	rpcParseError     = -32700
	rpcInvalidRequest = -32600
	rpcMethodNotFound = -32601
	rpcInvalidParams  = -32602
	rpcInternalError  = -32603
)

// serveMCP handles one authenticated POST. Requests are bounded and the
// response is a single JSON-RPC message — never an SSE stream.
func (s *assistantServer) serveMCP(w http.ResponseWriter, r *http.Request, identity string) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, assistantMaxRequestBytes))
	if err != nil {
		s.logAudit(identity, "parse", "-", "-", "request_too_large")
		s.writeRPCError(w, nil, rpcInvalidRequest, "request_too_large")
		return
	}
	var request assistantRPCRequest
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&request); err != nil {
		s.logAudit(identity, "parse", "-", "-", "parse_error")
		s.writeRPCError(w, nil, rpcParseError, "parse_error")
		return
	}
	if request.JSONRPC != "2.0" {
		s.logAudit(identity, "dispatch", "-", "-", "invalid_request")
		s.writeRPCError(w, request.ID, rpcInvalidRequest, "invalid_request")
		return
	}
	// Notifications carry no id and get no response body — 202 ends them.
	if len(request.ID) == 0 || string(request.ID) == "null" {
		s.logAudit(identity, request.Method, "-", "-", "notified")
		w.WriteHeader(http.StatusAccepted)
		return
	}
	result, rpcErr := s.dispatchRPC(r.Context(), identity, &request)
	if rpcErr != nil {
		s.writeRPCError(w, request.ID, rpcErr.Code, rpcErr.Message)
		return
	}
	writeAssistantJSON(w, http.StatusOK, assistantRPCResponse{JSONRPC: "2.0", ID: request.ID, Result: result})
}

func (s *assistantServer) writeRPCError(w http.ResponseWriter, id json.RawMessage, code int, message string) {
	if id == nil {
		id = json.RawMessage("null")
	}
	writeAssistantJSON(w, http.StatusOK, assistantRPCResponse{JSONRPC: "2.0", ID: id, Error: &assistantRPCError{Code: code, Message: message}})
}

func writeAssistantJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// dispatchRPC routes one request. Protocol-level failures come back as
// JSON-RPC errors; tool-level failures come back inside the CallToolResult
// with isError so a model reads them as tool output, never as transport.
func (s *assistantServer) dispatchRPC(ctx context.Context, identity string, request *assistantRPCRequest) (result any, rpcErr *assistantRPCError) {
	// Belt under the tool-call recover: a panic anywhere in dispatch still
	// answers a named JSON-RPC error and lands in the audit log, never a
	// dropped connection with a stack in stderr.
	defer func() {
		if recover() != nil {
			s.logAudit(identity, request.Method, "-", "-", "internal_error")
			result = nil
			rpcErr = &assistantRPCError{Code: rpcInternalError, Message: "internal_error"}
		}
	}()
	switch request.Method {
	case "initialize":
		s.logAudit(identity, "initialize", "-", "-", "ok")
		// Negotiation: echo the client's version when it offers one — the
		// server speaks the same surface regardless.
		version := assistantProtocolVersion
		var init struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		if json.Unmarshal(request.Params, &init) == nil && init.ProtocolVersion != "" {
			version = init.ProtocolVersion
		}
		return map[string]any{
			"protocolVersion": version,
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]any{"name": "panewire-assistant", "version": "v0"},
			"instructions":    s.instructions(),
		}, nil
	case "ping":
		s.logAudit(identity, "ping", "-", "-", "ok")
		return map[string]any{}, nil
	case "tools/list":
		s.logAudit(identity, "tools/list", "-", "-", "ok")
		return map[string]any{"tools": assistantToolList(s.writes)}, nil
	case "tools/call":
		return s.dispatchToolCall(ctx, identity, request.Params)
	default:
		s.logAudit(identity, request.Method, "-", "-", "method_not_found")
		return nil, &assistantRPCError{Code: rpcMethodNotFound, Message: "method_not_found"}
	}
}

func (s *assistantServer) dispatchToolCall(ctx context.Context, identity string, params json.RawMessage) (result any, rpcErr *assistantRPCError) {
	var call struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	toolName := "-"
	subject := "-"
	// A panic inside a tool must never leak: the caller sees a named
	// JSON-RPC error and the audit log records the failure — the panic text
	// stays out of both (and out of the response entirely).
	defer func() {
		if recover() != nil {
			s.logAudit(identity, "tools/call", toolName, subject, "internal_error")
			result = nil
			rpcErr = &assistantRPCError{Code: rpcInternalError, Message: "internal_error"}
		}
	}()
	if err := json.Unmarshal(params, &call); err != nil || call.Name == "" {
		s.logAudit(identity, "tools/call", "-", "-", "invalid_params")
		return nil, &assistantRPCError{Code: rpcInvalidParams, Message: "invalid_params"}
	}
	toolName = call.Name
	if !assistantToolKnown(call.Name) {
		s.logAudit(identity, "tools/call", call.Name, "-", "unknown_tool")
		return nil, &assistantRPCError{Code: rpcInvalidParams, Message: "unknown_tool"}
	}
	subject = toolSubject(call.Name, call.Arguments)
	result, toolErr := s.callAssistantTool(ctx, identity, call.Name, call.Arguments)
	if toolErr != nil {
		s.logAudit(identity, "tools/call", call.Name, subject, toolErr.Error())
		errObject := map[string]any{"error": toolErr.Error()}
		if withDetail, ok := toolErr.(*assistantDetailError); ok {
			for k, v := range withDetail.detail {
				errObject[k] = v
			}
		}
		text, _ := json.Marshal(errObject)
		return map[string]any{
			"content": []map[string]any{{"type": "text", "text": string(text)}},
			"isError": true,
		}, nil
	}
	s.logAudit(identity, "tools/call", call.Name, subject, "ok")
	text, err := json.Marshal(result)
	if err != nil {
		return nil, &assistantRPCError{Code: rpcInvalidParams, Message: "internal"}
	}
	return map[string]any{
		"content": []map[string]any{{"type": "text", "text": string(text)}},
		"isError": false,
	}, nil
}

// instructions is the initialize reply's surface description: the no-push
// contract verbatim, the read tools always, and the enabled write tools by
// name so a caller never has to guess which half of the flag is on.
func (s *assistantServer) instructions() string {
	out := assistantPollContract + " Read tools: targets, pending_list, pending_detail, progress, poll."
	var writeNames []string
	for _, tool := range assistantWriteToolList() {
		if name, _ := tool["name"].(string); s.writeEnabled(name) {
			writeNames = append(writeNames, name)
		}
	}
	if len(writeNames) == 0 {
		return out
	}
	return out + " Write tools enabled: " + strings.Join(writeNames, ", ") + ". A write call on a tool not listed answers writes_disabled. Write results carry the outbox event id; delivery is confirmed through progress, never assumed."
}

// toolSubject pulls the one audit-safe identifier out of a call's arguments —
// the opaque target id, a request id, or a pending item id — without ever
// logging raw arguments.
func toolSubject(name string, args json.RawMessage) string {
	var parsed struct {
		Target    string `json:"target"`
		RequestID string `json:"request_id"`
		ID        string `json:"id"`
	}
	if json.Unmarshal(args, &parsed) != nil {
		return "-"
	}
	for _, candidate := range []string{parsed.Target, parsed.RequestID, parsed.ID} {
		if candidate != "" {
			return candidate
		}
	}
	return "-"
}

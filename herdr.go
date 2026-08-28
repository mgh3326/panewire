package panewire

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// Herdr's ordinary API is one request per connection. Subscription is the
// sole long-lived stream and therefore gets its own connection.
type HerdrClient struct {
	path    string
	mu      sync.Mutex
	seq     int
	subConn net.Conn
	closed  chan struct{}
}

func NewHerdrClient(path string) (*HerdrClient, error) {
	conn, err := net.DialTimeout("unix", path, 2*time.Second)
	if err != nil {
		return nil, err
	}
	_ = conn.Close()
	return &HerdrClient{path: path, closed: make(chan struct{})}, nil
}
func (c *HerdrClient) nextID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	return fmt.Sprintf("panewire-%d", c.seq)
}
func (c *HerdrClient) Close() error {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.subConn != nil {
		return c.subConn.Close()
	}
	return nil
}
func writeRequest(w io.Writer, id, method string, params any) error {
	b, _ := json.Marshal(map[string]any{"id": id, "version": 1, "protocol": 20, "method": method, "params": params})
	_, err := fmt.Fprintf(w, "%s\n", b)
	return err
}
func readResponse(ctx context.Context, r *bufio.Reader, id string) (json.RawMessage, error) {
	for {
		line, e := r.ReadBytes('\n')
		if e != nil && len(line) == 0 {
			return nil, e
		}
		var msg map[string]json.RawMessage
		if json.Unmarshal(line, &msg) != nil {
			continue
		}
		var got string
		_ = json.Unmarshal(msg["id"], &got)
		if got != id {
			continue
		}
		if er := msg["error"]; len(er) > 0 {
			return nil, fmt.Errorf("herdr %s", string(er))
		}
		return msg["result"], nil
	}
}
func (c *HerdrClient) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	conn, err := net.DialTimeout("unix", c.path, 2*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if d, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(d)
	}
	id := c.nextID()
	if err := writeRequest(conn, id, method, params); err != nil {
		return nil, err
	}
	return readResponse(ctx, bufio.NewReader(conn), id)
}

type HerdrEvent struct {
	Kind                             string
	PaneID, WorkspaceID, AgentStatus string
	Revision                         int64
	Raw                              json.RawMessage
	UnknownFields                    json.RawMessage
}

func (c *HerdrClient) Subscribe(ctx context.Context) (<-chan HerdrEvent, error) {
	snapshot, err := c.Call(ctx, "agent.list", map[string]any{})
	if err != nil {
		return nil, err
	}
	var listed struct {
		Agents []struct {
			PaneID string `json:"pane_id"`
		} `json:"agents"`
	}
	if json.Unmarshal(snapshot, &listed) != nil {
		return nil, fmt.Errorf("invalid herdr agent list")
	}
	subs := make([]any, 0, len(listed.Agents)*3)
	for _, a := range listed.Agents {
		if a.PaneID == "" {
			continue
		}
		subs = append(subs, map[string]any{"type": "pane.agent_status_changed", "pane_id": a.PaneID}, map[string]any{"type": "pane.output_matched", "pane_id": a.PaneID, "source": "recent_unwrapped", "match": map[string]any{"type": "regex", "value": ".*"}, "strip_ansi": true}, map[string]any{"type": "pane.scroll_changed", "pane_id": a.PaneID})
	}
	conn, err := net.DialTimeout("unix", c.path, 2*time.Second)
	if err != nil {
		return nil, err
	}
	if d, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(d)
	}
	id := c.nextID()
	if err := writeRequest(conn, id, "events.subscribe", map[string]any{"subscriptions": subs}); err != nil {
		conn.Close()
		return nil, err
	}
	reader := bufio.NewReader(conn)
	if _, err := readResponse(ctx, reader, id); err != nil {
		conn.Close()
		return nil, err
	}
	c.mu.Lock()
	c.subConn = conn
	c.mu.Unlock()
	out := make(chan HerdrEvent, 32)
	go func() {
		defer close(out)
		defer conn.Close()
		for {
			line, e := reader.ReadBytes('\n')
			if e != nil && len(line) == 0 {
				return
			}
			ev, ok := decodeHerdrEvent(line)
			if !ok {
				continue
			}
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}
func decodeHerdrEvent(line []byte) (HerdrEvent, bool) {
	var env struct {
		Event json.RawMessage `json:"event"`
		Data  json.RawMessage `json:"data"`
	}
	if json.Unmarshal(line, &env) != nil || len(env.Event) == 0 {
		return HerdrEvent{}, false
	}
	var e struct {
		Type        string `json:"type"`
		PaneID      string `json:"pane_id"`
		WorkspaceID string `json:"workspace_id"`
		AgentStatus string `json:"agent_status"`
		Revision    int64  `json:"revision"`
	}
	var kind string
	if json.Unmarshal(env.Event, &kind) != nil {
		if json.Unmarshal(env.Event, &e) != nil {
			return HerdrEvent{}, false
		}
		kind = e.Type
	} else {
		_ = json.Unmarshal(env.Data, &e)
	}
	if kind == "" {
		return HerdrEvent{}, false
	}
	if e.AgentStatus == "" && len(env.Data) > 0 {
		var d struct {
			AgentStatus string `json:"agent_status"`
		}
		_ = json.Unmarshal(env.Data, &d)
		e.AgentStatus = d.AgentStatus
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(env.Event, &object) != nil {
		_ = json.Unmarshal(env.Data, &object)
	}
	known := map[string]bool{
		"type": true, "pane_id": true, "workspace_id": true, "agent_status": true, "revision": true,
		"agent": true, "display_agent": true, "title": true, "state_labels": true, "scroll": true,
		"matched_line": true, "read": true, "truncated": true, "format": true, "tab_id": true, "text": true,
		"source": true, "offset_from_bottom": true, "max_offset_from_bottom": true, "viewport_rows": true,
	}
	unknown := map[string]json.RawMessage{}
	for k, v := range object {
		if !known[k] {
			unknown[k] = v
		}
	}
	uj, _ := json.Marshal(unknown)
	return HerdrEvent{Kind: kind, PaneID: e.PaneID, WorkspaceID: e.WorkspaceID, AgentStatus: e.AgentStatus, Revision: e.Revision, Raw: append(json.RawMessage(nil), line...), UnknownFields: uj}, true
}
func DecodeHerdrEvent(line []byte) (HerdrEvent, bool) { return decodeHerdrEvent(line) }

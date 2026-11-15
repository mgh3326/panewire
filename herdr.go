package panewire

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"
)

// Herdr's ordinary API is one request per connection. Subscription is the
// sole long-lived stream and therefore gets its own connection.
type HerdrClient struct {
	path    string
	logger  *slog.Logger
	mu      sync.Mutex
	seq     int
	subConn net.Conn
	closed  chan struct{}
}

func NewHerdrClient(path string) (*HerdrClient, error) {
	return newHerdrClient(path, nil)
}

func newHerdrClient(path string, logger *slog.Logger) (*HerdrClient, error) {
	conn, err := net.DialTimeout("unix", path, 2*time.Second)
	if err != nil {
		return nil, err
	}
	_ = conn.Close()
	return &HerdrClient{path: path, closed: make(chan struct{}), logger: logger}, nil
}

func (c *HerdrClient) log() *slog.Logger {
	if c.logger != nil {
		return c.logger
	}
	return slog.Default()
}

// The stall detector only ever holds the read-only projection — the compiler
// enforces that this narrow interface stays satisfiable.
var _ stallReader = (*HerdrClient)(nil)

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
			return nil, &herdrReplyError{raw: er}
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

// herdrReplyError is an error object herdr returned for a request. It keeps the
// raw object so a caller can read the error code; the text is unchanged.
type herdrReplyError struct{ raw json.RawMessage }

func (e *herdrReplyError) Error() string { return "herdr " + string(e.raw) }

// PanesAlive reports the panes herdr currently has an agent on. agent.list
// returns live agents only, which is the definition this needs: a pane with no
// agent left on it is not running a job.
func (c *HerdrClient) PanesAlive(ctx context.Context) (map[string]bool, error) {
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
	alive := make(map[string]bool, len(listed.Agents))
	for _, agent := range listed.Agents {
		if agent.PaneID != "" {
			alive[agent.PaneID] = true
		}
	}
	return alive, nil
}

// AgentStates reads the real herdr agent snapshot and joins the tab label from
// tab.list, its existing source of truth. A missing tab snapshot costs only
// optional display context; pane, status, revision, and herdr's independent
// state-change sequence remain usable.
func (c *HerdrClient) AgentStates(ctx context.Context) ([]HerdrAgentState, error) {
	return c.agentStates(ctx)
}

func (c *HerdrClient) agentStates(ctx context.Context) ([]HerdrAgentState, error) {
	snapshot, err := c.Call(ctx, "agent.list", map[string]any{})
	if err != nil {
		return nil, err
	}
	var listed struct {
		Agents []map[string]any `json:"agents"`
	}
	if json.Unmarshal(snapshot, &listed) != nil {
		return nil, fmt.Errorf("invalid herdr agent list")
	}
	labels := tabLabels(ctx, c)
	states := make([]HerdrAgentState, 0, len(listed.Agents))
	for _, raw := range listed.Agents {
		pane := identityFromMap(raw)
		displayLabel := labels[pane.TabID]
		if pane.Label == "" {
			pane.Label = displayLabel
		}
		if !validIdleWakePane(pane.PaneID) {
			continue
		}
		status := pane.Status
		if !validObservedAgentStatus(status) {
			continue
		}
		states = append(states, HerdrAgentState{PaneID: pane.PaneID, WorkspaceID: pane.WorkspaceID, Label: pane.Label, AgentName: pane.Name, DisplayLabel: displayLabel, Harness: pane.Harness, Status: status, Revision: pane.Revision, SourceStateChangeSeq: pane.StateChangeSeq, InteractiveReady: pane.InteractiveReady, Authoritative: true})
	}
	return states, nil
}

type HerdrEvent struct {
	Kind                             string
	PaneID, WorkspaceID, AgentStatus string
	Revision                         int64
	Raw                              json.RawMessage
	UnknownFields                    json.RawMessage
	// Agent is pane_agent_detected's agent member verbatim — empty when the
	// key is absent, "null" when herdr reports no agent — and Released is the
	// same event's released flag. Both stay zero for every other kind.
	Agent    json.RawMessage
	Released bool
}

// Global pane lifecycle subscriptions ride the same events.subscribe request
// as the per-pane set, so a pane spawned after the stream opened is seen in
// seconds instead of at the next coverage scan. Subscription types are
// dotted; the events they produce arrive underscored.
var herdrLifecycleSubscriptions = []any{
	map[string]any{"type": "pane.created"},
	map[string]any{"type": "pane.closed"},
	map[string]any{"type": "pane.exited"},
	map[string]any{"type": "pane.agent_detected"},
}

// Subscribe also returns the pane set the request covered. Callers that need
// to prove observation coverage — the stall detector on attach and reconnect —
// diff this set against what agent.list later reports.
func (c *HerdrClient) Subscribe(ctx context.Context) (<-chan HerdrEvent, []string, error) {
	snapshot, err := c.Call(ctx, "agent.list", map[string]any{})
	if err != nil {
		return nil, nil, err
	}
	var listed struct {
		Agents []struct {
			PaneID string `json:"pane_id"`
		} `json:"agents"`
	}
	if json.Unmarshal(snapshot, &listed) != nil {
		return nil, nil, fmt.Errorf("invalid herdr agent list")
	}
	perPane := make([]any, 0, len(listed.Agents)*3)
	panes := make([]string, 0, len(listed.Agents))
	for _, a := range listed.Agents {
		if a.PaneID == "" {
			continue
		}
		panes = append(panes, a.PaneID)
		perPane = append(perPane, map[string]any{"type": "pane.agent_status_changed", "pane_id": a.PaneID}, map[string]any{"type": "pane.output_matched", "pane_id": a.PaneID, "source": "recent_unwrapped", "match": map[string]any{"type": "regex", "value": ".*"}, "strip_ansi": true}, map[string]any{"type": "pane.scroll_changed", "pane_id": a.PaneID})
	}
	subs := make([]any, 0, len(perPane)+len(herdrLifecycleSubscriptions))
	subs = append(subs, herdrLifecycleSubscriptions...)
	subs = append(subs, perPane...)
	events, err := c.openEventStream(ctx, subs)
	if err != nil {
		// A herdr that predates the global lifecycle types rejects the mixed
		// request; retry once on a new connection with the per-pane set only,
		// which is exactly the coverage the base contract already proved.
		c.log().Warn("herdr lifecycle subscriptions rejected; subscribing per-pane only", "error", err)
		events, err = c.openEventStream(ctx, perPane)
		if err != nil {
			return nil, nil, err
		}
	}
	return events, panes, nil
}

func (c *HerdrClient) openEventStream(ctx context.Context, subs []any) (<-chan HerdrEvent, error) {
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

// AgentDetails returns the full agent.list identity rows — cwd and harness
// included, which the HubSession projection deliberately drops. The stall
// detector needs both for worktree conflict observation and family checks.
func (c *HerdrClient) AgentDetails(ctx context.Context) ([]paneIdentity, error) {
	snapshot, err := c.Call(ctx, "agent.list", map[string]any{})
	if err != nil {
		return nil, err
	}
	var listed struct {
		Agents []map[string]any `json:"agents"`
	}
	if json.Unmarshal(snapshot, &listed) != nil {
		return nil, fmt.Errorf("invalid herdr agent list")
	}
	labels := tabLabels(ctx, c)
	agents := make([]paneIdentity, 0, len(listed.Agents))
	for _, raw := range listed.Agents {
		pane := identityFromMap(raw)
		if pane.Label == "" {
			pane.Label = labels[pane.TabID]
		}
		if pane.PaneID == "" {
			continue
		}
		agents = append(agents, pane)
	}
	return agents, nil
}

// ReadPane performs one bounded recent_unwrapped read, falling back to the
// visible viewport when the recent buffer is empty. The detector uses this
// after an output_matched wake and on its measured poll cadence; it is a read
// only and never touches pane input.
func (c *HerdrClient) ReadPane(ctx context.Context, paneID string) (readEvidence, error) {
	evidence, err := readPane(ctx, c, paneIdentity{PaneID: paneID}, "recent_unwrapped")
	if err != nil {
		return readEvidence{}, err
	}
	if evidence.Text == "" {
		return readPane(ctx, c, paneIdentity{PaneID: paneID}, "visible")
	}
	return evidence, nil
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
	payload := env.Data
	if json.Unmarshal(env.Event, &object) == nil {
		payload = env.Event
	} else {
		_ = json.Unmarshal(env.Data, &object)
	}
	known := map[string]bool{
		"type": true, "pane_id": true, "workspace_id": true, "agent_status": true, "revision": true,
		"agent": true, "display_agent": true, "title": true, "state_labels": true, "scroll": true,
		"matched_line": true, "read": true, "truncated": true, "format": true, "tab_id": true, "text": true,
		"source": true, "offset_from_bottom": true, "max_offset_from_bottom": true, "viewport_rows": true,
		"pane": true, "final_status": true, "released": true,
	}
	unknown := map[string]json.RawMessage{}
	for k, v := range object {
		if !known[k] {
			unknown[k] = v
		}
	}
	uj, _ := json.Marshal(unknown)
	ev := HerdrEvent{Kind: kind, PaneID: e.PaneID, WorkspaceID: e.WorkspaceID, AgentStatus: e.AgentStatus, Revision: e.Revision, Raw: append(json.RawMessage(nil), line...), UnknownFields: uj}
	if isPaneCreatedEvent(kind) || isAgentDetectedEvent(kind) {
		var data struct {
			Pane *struct {
				PaneID      string `json:"pane_id"`
				WorkspaceID string `json:"workspace_id"`
			} `json:"pane"`
			Agent    json.RawMessage `json:"agent"`
			Released bool            `json:"released"`
		}
		// payload is whichever member carried the event's fields — the
		// object-valued form keeps them under "event", the string form
		// under "data" — so lifecycle extraction sees both shapes.
		if json.Unmarshal(payload, &data) == nil {
			// pane_created nests identity under data.pane; the lifecycle
			// payloads carry no agent_status, so the event's status fields
			// stay empty and cannot leak into status-change consumers.
			if isPaneCreatedEvent(kind) && data.Pane != nil {
				if ev.PaneID == "" {
					ev.PaneID = data.Pane.PaneID
				}
				if ev.WorkspaceID == "" {
					ev.WorkspaceID = data.Pane.WorkspaceID
				}
			}
			if isAgentDetectedEvent(kind) {
				ev.Agent = data.Agent
				ev.Released = data.Released
			}
		}
	}
	return ev, true
}
func DecodeHerdrEvent(line []byte) (HerdrEvent, bool) { return decodeHerdrEvent(line) }

// The four global pane lifecycle kinds arrive on the stream in herdr's
// underscored wire form; the dotted subscription form is accepted too so a
// kind rename on either side still classifies.
func isPaneCreatedEvent(kind string) bool { return kind == "pane.created" || kind == "pane_created" }
func isPaneDepartureEvent(kind string) bool {
	return kind == "pane.closed" || kind == "pane_closed" || kind == "pane.exited" || kind == "pane_exited"
}
func isAgentDetectedEvent(kind string) bool {
	return kind == "pane.agent_detected" || kind == "pane_agent_detected"
}
func isPaneLifecycleEvent(kind string) bool {
	return isPaneCreatedEvent(kind) || isPaneDepartureEvent(kind) || isAgentDetectedEvent(kind)
}

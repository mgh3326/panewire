package panewire

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

const (
	daemonRetryDelay             = 100 * time.Millisecond
	daemonCapabilityRetryInitial = time.Second
	daemonCapabilityRetryMax     = 30 * time.Second
)

type Config struct {
	SocketPath, HerdrSocket, DBPath, InboxRoot string
	StorePromptBody                            bool
	IdleWakeSettle                             time.Duration
	Logging                                    LoggingConfig
	Stage2                                     Stage2Config
	Hub                                        HubDaemonConfig
	Store                                      *Store
	SchemaCommand                              []string
	Logger                                     *slog.Logger
}

type LoggingConfig struct {
	StorePromptBody bool
}
type Daemon struct {
	cfg        Config
	store      *Store
	listener   net.Listener
	cancel     context.CancelFunc
	herdr      *HerdrClient
	idleWake   *idleWakeManager
	caps       GuardResult
	eventDone  chan struct{}
	idleDone   chan struct{}
	stage2Done chan struct{}
	hubDone    chan struct{}
	mu         sync.Mutex
}

func NewDaemon(cfg Config) *Daemon {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Daemon{cfg: cfg, store: cfg.Store}
}
func (d *Daemon) RecordSchemaGuard(ctx context.Context, g GuardResult, phase string) error {
	payload, _ := json.Marshal(map[string]any{"phase": phase, "protocol": g.Protocol, "schema_version": g.Schema, "events": g.Events, "agent_wait": g.AgentWait, "agent_read": g.AgentRead, "prompt": g.Prompt, "warnings": g.Warnings, "unavailable": g.Unavailable})
	return d.store.RecordEvent(ctx, Event{Source: "herdr", Kind: "herdr.schema_guard", Payload: payload})
}
func (d *Daemon) Start(ctx context.Context) error {
	if d.store == nil {
		path := d.cfg.DBPath
		if path == "" {
			home, _ := os.UserHomeDir()
			path = filepath.Join(home, "Library", "Application Support", "panewire", "panewire.sqlite3")
		}
		s, err := OpenStore(path)
		if err != nil {
			return err
		}
		d.store = s
	}
	guard := d.runGuard(ctx, "startup")
	d.setCapabilities(guard)
	_ = d.RecordSchemaGuard(ctx, guard, "startup")
	if guard.Events && d.cfg.HerdrSocket != "" {
		if c, err := NewHerdrClient(d.cfg.HerdrSocket); err == nil {
			d.setHerdrClient(c)
		} else {
			d.cfg.Logger.Warn("herdr unavailable", "error", err)
		}
	}
	if d.cfg.Hub.Client != nil {
		// The outbox lives in the daemon's own SQLite file, so it is attached
		// once the store is open rather than at hub client construction.
		d.cfg.Hub.Client.SetRelayOutbox(d.store)
		d.cfg.Hub.Client.SetPanesAlive(hubPanesAliveHook(d.cfg.HerdrSocket))
		idleRoot := d.cfg.Hub.Client.jobsInboxRoot
		if idleRoot == "" {
			idleRoot = d.cfg.InboxRoot
		}
		if d.cfg.HerdrSocket != "" && idleRoot != "" {
			manager, err := newIdleWakeManager(d.store, idleRoot, d.cfg.IdleWakeSettle, d.cfg.Hub.Client.EnqueueIdleWakeRouteRequest, d.cfg.Hub.Client.EnqueueRelayEvent, d.cfg.Logger)
			if err != nil {
				return err
			}
			d.idleWake = manager
			d.cfg.Hub.Client.SetIdleWakeManager(manager)
		}
	}
	if d.cfg.InboxRoot != "" {
		if w, err := NewInboxWatcher(d.cfg.InboxRoot, d.store); err == nil {
			go func() { _ = w.Run(ctx) }()
		} else {
			return err
		}
	}
	path := d.cfg.SocketPath
	if path == "" {
		path = defaultSocketPath()
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	_ = os.Remove(path)
	l, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	d.listener = l
	runCtx, cancel := context.WithCancel(ctx)
	d.cancel = cancel
	go d.serve(runCtx)
	if d.cfg.HerdrSocket != "" && (guard.Events || d.idleWake != nil) {
		d.eventDone = make(chan struct{})
		go func() {
			defer close(d.eventDone)
			d.eventLoop(runCtx)
		}()
	}
	if d.idleWake != nil {
		d.idleDone = make(chan struct{})
		go func() {
			defer close(d.idleDone)
			d.idleWakeRetryLoop(runCtx)
		}()
	}
	if d.cfg.Stage2.Enabled {
		d.stage2Done = make(chan struct{})
		go func() {
			defer close(d.stage2Done)
			d.stage2Loop(runCtx)
		}()
	}
	if d.cfg.Hub.Enabled && d.cfg.Hub.Client != nil {
		d.hubDone = make(chan struct{})
		go func() {
			defer close(d.hubDone)
			d.cfg.Hub.Client.Run(runCtx)
		}()
	}
	return nil
}

// hubPanesAliveHook dials herdr per scan the way wait.agent and prompt do: the
// subscription client belongs to the event loop and is reconnected there, so
// the hub's heartbeat goroutine must not borrow it. No socket means no hook,
// which leaves the heartbeat's active set exactly as it was.
func hubPanesAliveHook(socket string) panesAliveFunc {
	if socket == "" {
		return nil
	}
	return func(ctx context.Context) (map[string]bool, error) {
		client, err := NewHerdrClient(socket)
		if err != nil {
			return nil, err
		}
		defer client.Close()
		return client.PanesAlive(ctx)
	}
}

func (d *Daemon) runGuard(ctx context.Context, phase string) GuardResult {
	cmd := d.cfg.SchemaCommand
	if len(cmd) == 0 {
		cmd = []string{"herdr", "api", "schema", "--json"}
	}
	if len(cmd) == 0 {
		return GuardResult{Unavailable: []string{"schema"}}
	}
	out, err := exec.CommandContext(ctx, cmd[0], cmd[1:]...).Output()
	if err != nil {
		d.cfg.Logger.Warn("herdr schema guard failed", "phase", phase, "error", err)
		return GuardResult{Warnings: []string{"schema command failed"}, Unavailable: []string{"events", "agent.wait", "agent.read", "prompt"}}
	}
	g, err := GuardSchema(bytes.NewReader(out))
	if err != nil {
		d.cfg.Logger.Warn("herdr schema guard failed", "phase", phase, "error", err)
		return GuardResult{Warnings: []string{err.Error()}, Unavailable: []string{"events", "agent.wait", "agent.read", "prompt"}}
	}
	d.cfg.Logger.Info("herdr schema guard", "phase", phase, "protocol", g.Protocol, "schema_version", g.Schema, "events", g.Events, "agent_wait", g.AgentWait)
	return g
}
func (d *Daemon) eventLoop(ctx context.Context) {
	var settle <-chan time.Time
	var poll <-chan time.Time
	var settleTicker, pollTicker *time.Ticker
	capabilityBackoff := daemonCapabilityRetryInitial
	if d.idleWake != nil {
		settleTicker = time.NewTicker(time.Second)
		pollTicker = time.NewTicker(idleWakeObservationPoll)
		settle, poll = settleTicker.C, pollTicker.C
		defer settleTicker.Stop()
		defer pollTicker.Stop()
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		// Idle-wake starts this loop even when the startup guard could not
		// prove events support. Probe that external capability command on a
		// bounded backoff instead of spawning it on the ordinary 100ms socket
		// reconnect cadence. A successful probe proceeds immediately.
		if !d.capabilities().Events {
			if !waitDaemonRetryFor(ctx, capabilityBackoff) {
				return
			}
			guard := d.runGuard(ctx, "reconnect")
			d.setCapabilities(guard)
			_ = d.RecordSchemaGuard(ctx, guard, "reconnect")
			if !guard.Events {
				capabilityBackoff = nextDaemonCapabilityBackoff(capabilityBackoff)
				continue
			}
			capabilityBackoff = daemonCapabilityRetryInitial
		}
		client := d.herdrClient()
		if client == nil {
			connected, err := NewHerdrClient(d.cfg.HerdrSocket)
			if err != nil {
				if !waitDaemonRetry(ctx) {
					return
				}
				continue
			}
			if !d.adoptHerdrClient(ctx, connected) {
				return
			}
			client = connected
		}
		events, err := client.Subscribe(ctx)
		if err != nil {
			d.cfg.Logger.Warn("herdr subscribe failed", "error", err)
			d.clearHerdrClient(client)
			if !waitDaemonRetry(ctx) {
				return
			}
			if g := d.runGuard(ctx, "reconnect"); true {
				d.setCapabilities(g)
				_ = d.RecordSchemaGuard(ctx, g, "reconnect")
			}
			continue
		}
		if d.idleWake != nil {
			d.observeIdleWakeSnapshot(ctx, client)
			d.idleWake.Tick(ctx, time.Now().UTC())
		}
		streamOpen := true
		for streamOpen {
			select {
			case <-ctx.Done():
				return
			case ev, open := <-events:
				if !open {
					streamOpen = false
					continue
				}
				if len(ev.UnknownFields) > 2 || !knownHerdrEvent(ev.Kind) {
					d.cfg.Logger.Warn("unknown herdr event; recording without inference", "kind", ev.Kind, "fields", string(ev.UnknownFields))
				}
				caps := d.capabilities()
				recordHerdrEvent(ctx, d.store, ev, caps.Protocol, caps.Schema)
				if d.idleWake != nil && ev.AgentStatus != "" && (ev.Kind == "pane.agent_status_changed" || ev.Kind == "pane_agent_status_changed") {
					if err := d.idleWake.Observe(ctx, HerdrAgentState{PaneID: ev.PaneID, WorkspaceID: ev.WorkspaceID, Status: ev.AgentStatus, Revision: ev.Revision}, time.Now().UTC()); err != nil {
						d.cfg.Logger.Warn("idle-wake observation rejected")
					}
				}
			case at := <-settle:
				d.idleWake.Tick(ctx, at.UTC())
			case <-poll:
				d.observeIdleWakeSnapshot(ctx, client)
			}
		}
		d.clearHerdrClient(client)
		if !waitDaemonRetry(ctx) {
			return
		}
		g := d.runGuard(ctx, "reconnect")
		d.setCapabilities(g)
		_ = d.RecordSchemaGuard(ctx, g, "reconnect")
	}
}

func waitDaemonRetry(ctx context.Context) bool {
	return waitDaemonRetryFor(ctx, daemonRetryDelay)
}

func waitDaemonRetryFor(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextDaemonCapabilityBackoff(current time.Duration) time.Duration {
	if current >= daemonCapabilityRetryMax || current > daemonCapabilityRetryMax/2 {
		return daemonCapabilityRetryMax
	}
	return current * 2
}

func (d *Daemon) setHerdrClient(client *HerdrClient) {
	d.mu.Lock()
	d.herdr = client
	d.mu.Unlock()
}

func (d *Daemon) setCapabilities(capabilities GuardResult) {
	d.mu.Lock()
	d.caps = capabilities
	d.mu.Unlock()
}

func (d *Daemon) capabilities() GuardResult {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.caps
}

func (d *Daemon) adoptHerdrClient(ctx context.Context, client *HerdrClient) bool {
	d.mu.Lock()
	if ctx.Err() != nil {
		d.mu.Unlock()
		_ = client.Close()
		return false
	}
	d.herdr = client
	d.mu.Unlock()
	return true
}

func (d *Daemon) herdrClient() *HerdrClient {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.herdr
}

func (d *Daemon) clearHerdrClient(client *HerdrClient) {
	d.mu.Lock()
	if d.herdr == client {
		d.herdr = nil
	}
	d.mu.Unlock()
	_ = client.Close()
}

func (d *Daemon) idleWakeRetryLoop(ctx context.Context) {
	d.idleWake.RetrySettled(ctx, time.Now().UTC())
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case at := <-ticker.C:
			d.idleWake.RetrySettled(ctx, at.UTC())
		}
	}
}

func (d *Daemon) observeIdleWakeSnapshot(ctx context.Context, client *HerdrClient) {
	lookup, cancel := context.WithTimeout(ctx, hubPanesAliveTimeout)
	defer cancel()
	states, err := client.AgentStates(lookup)
	if err != nil {
		return
	}
	if err := d.idleWake.ObserveSnapshot(ctx, states, time.Now().UTC()); err != nil {
		d.cfg.Logger.Warn("idle-wake snapshot observation rejected")
	}
}
func knownHerdrEvent(kind string) bool {
	switch kind {
	case "pane.agent_status_changed", "pane.output_matched", "pane.scroll_changed", "pane_output_changed", "pane_agent_status_changed", "pane_scroll_changed":
		return true
	}
	return false
}
func (d *Daemon) serve(ctx context.Context) {
	for {
		c, err := d.listener.Accept()
		if err != nil {
			return
		}
		go d.handle(ctx, c)
	}
}

type localRequest struct {
	Op        string `json:"op"`
	Path      string `json:"path,omitempty"`
	Target    string `json:"target,omitempty"`
	Sender    string `json:"sender,omitempty"`
	Uptake    string `json:"uptake,omitempty"`
	StoreBody bool   `json:"store_body,omitempty"`
	Status    string `json:"status,omitempty"`
	SettleMS  int64  `json:"settle_ms,omitempty"`
	TimeoutMS int64  `json:"timeout_ms,omitempty"`
	// The fields below belong to the R20 emit op. The names above are the
	// established wait/prompt request contract and must not be renamed.
	Kind           string `json:"kind,omitempty"`
	JobID          string `json:"job_id,omitempty"`
	Epoch          uint64 `json:"epoch,omitempty"`
	OwnerLane      string `json:"owner_lane,omitempty"`
	AgentLabel     string `json:"agent_label,omitempty"`
	Label          string `json:"label,omitempty"`
	Host           string `json:"host,omitempty"`
	PaneID         string `json:"pane_id,omitempty"`
	ReportPath     string `json:"report_path,omitempty"`
	ReportLastLine string `json:"report_last_line,omitempty"`
	Reason         string `json:"reason,omitempty"`
	Question       string `json:"question,omitempty"`
	PR             string `json:"pr,omitempty"`
	Head           string `json:"head,omitempty"`
	EventID        string `json:"event_id,omitempty"`
	Text           string `json:"text,omitempty"`
	Truncated      bool   `json:"truncated,omitempty"`
	// InboxRoot names the namespace the caller recorded the event in. A daemon
	// that watches a different root must refuse it: the event file lives in the
	// caller's namespace, but the relay outbox row would be written in this
	// daemon's. That is how a test run against a temporary inbox root ends up
	// stamping the operator's production journal.
	InboxRoot string `json:"inbox_root,omitempty"`
}
type localResponse struct {
	OK     bool   `json:"ok"`
	Code   int    `json:"code"`
	Error  string `json:"error,omitempty"`
	Result any    `json:"result,omitempty"`
}

func (d *Daemon) handle(ctx context.Context, c net.Conn) {
	defer c.Close()
	scan := bufio.NewScanner(c)
	for scan.Scan() {
		var req localRequest
		if json.Unmarshal(scan.Bytes(), &req) != nil {
			writeLocal(c, localResponse{Code: ExitUsage, Error: "invalid request"})
			continue
		}
		timeout := time.Duration(req.TimeoutMS) * time.Millisecond
		if timeout <= 0 {
			writeLocal(c, localResponse{Code: ExitUsage, Error: "timeout is required"})
			continue
		}
		callCtx, cancel := context.WithTimeout(ctx, timeout)
		var result any
		var err error
		caps := d.capabilities()
		switch req.Op {
		case "wait.file":
			result, err = WaitFile(callCtx, d.store, req.Path, time.Duration(req.SettleMS)*time.Millisecond)
		case "wait.agent":
			if !caps.AgentCapability() {
				err = &codedError{ExitDaemonUnavailable, fmt.Errorf("agent capability unavailable")}
			} else {
				c2, e := NewHerdrClient(d.cfg.HerdrSocket)
				if e != nil {
					err = &codedError{ExitDaemonUnavailable, e}
				} else {
					result, err = WaitAgent(callCtx, c2, req.Target, req.Status, time.Duration(req.SettleMS)*time.Millisecond, timeout)
					_ = c2.Close()
				}
			}
		case "emit":
			err = d.emitRelayEvent(req)
		case "prompt":
			if !caps.Prompt || !caps.AgentRead {
				result, err = recordUnavailablePrompt(callCtx, d.store, PromptRequest{Sender: req.Sender, Target: req.Target, Path: req.Path, Uptake: req.Uptake, StorePromptBody: req.StoreBody || d.cfg.StorePromptBody || d.cfg.Logging.StorePromptBody}, ExitDaemonUnavailable, "prompt capability unavailable")
			} else {
				c2, e := NewHerdrClient(d.cfg.HerdrSocket)
				if e != nil {
					result, err = recordUnavailablePrompt(callCtx, d.store, PromptRequest{Sender: req.Sender, Target: req.Target, Path: req.Path, Uptake: req.Uptake, StorePromptBody: req.StoreBody || d.cfg.StorePromptBody || d.cfg.Logging.StorePromptBody}, ExitDaemonUnavailable, e.Error())
				} else {
					result, err = Prompt(callCtx, d.store, c2, PromptRequest{Sender: req.Sender, Target: req.Target, Path: req.Path, Uptake: req.Uptake, StorePromptBody: req.StoreBody || d.cfg.StorePromptBody || d.cfg.Logging.StorePromptBody}, caps)
					_ = c2.Close()
				}
			}
		default:
			err = &codedError{ExitUsage, fmt.Errorf("unknown operation")}
		}
		cancel()
		code := ExitCode(err)
		writeLocal(c, localResponse{OK: err == nil, Code: code, Error: errorString(err), Result: result})
	}
}
func writeLocal(c net.Conn, v localResponse) {
	b, _ := json.Marshal(v)
	_, _ = fmt.Fprintf(c, "%s\n", b)
}
func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
func (d *Daemon) Stop() error {
	if d.cancel != nil {
		d.cancel()
	}
	d.mu.Lock()
	herdr := d.herdr
	d.herdr = nil
	d.mu.Unlock()
	if herdr != nil {
		_ = herdr.Close()
	}
	if d.eventDone != nil {
		<-d.eventDone
		d.eventDone = nil
	}
	if d.idleDone != nil {
		<-d.idleDone
		d.idleDone = nil
	}
	if d.stage2Done != nil {
		<-d.stage2Done
		d.stage2Done = nil
	}
	if d.hubDone != nil {
		<-d.hubDone
		d.hubDone = nil
	}
	if d.listener != nil {
		_ = d.listener.Close()
	}
	if d.cfg.SocketPath != "" {
		_ = os.Remove(d.cfg.SocketPath)
	}
	var stage2Err error
	if d.cfg.Stage2.Close != nil {
		stage2Err = d.cfg.Stage2.Close()
		d.cfg.Stage2.Close = nil
	}
	if d.store != nil {
		if err := d.store.Close(); err != nil {
			return err
		}
	}
	return stage2Err
}
func (d *Daemon) SocketPath() string {
	if d.cfg.SocketPath != "" {
		return d.cfg.SocketPath
	}
	return defaultSocketPath()
}
func defaultSocketPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, "Library", "Application Support", "panewire", "panewire.sock")
}

// emitNamespaceMatches reports whether a caller's inbox root is the one this
// daemon relays from. An empty request root is a pre-R20t7 client and is
// accepted unchanged; an empty daemon root means this daemon has no namespace
// of its own to defend.
func (d *Daemon) emitNamespaceMatches(requested string) bool {
	if requested == "" {
		return true
	}
	local := ""
	if d.cfg.Hub.Client != nil {
		local = d.cfg.Hub.Client.jobsInboxRoot
	}
	if local == "" {
		local = d.cfg.InboxRoot
	}
	if local == "" {
		return true
	}
	return sameLocalPath(requested, local)
}

// sameLocalPath compares two filesystem paths by their resolved absolute form.
func sameLocalPath(left, right string) bool {
	leftAbs, leftErr := filepath.Abs(left)
	rightAbs, rightErr := filepath.Abs(right)
	if leftErr != nil || rightErr != nil {
		return filepath.Clean(left) == filepath.Clean(right)
	}
	if resolved, err := filepath.EvalSymlinks(leftAbs); err == nil {
		leftAbs = resolved
	}
	if resolved, err := filepath.EvalSymlinks(rightAbs); err == nil {
		rightAbs = resolved
	}
	return leftAbs == rightAbs
}

// emitRelayEvent hands one already-recorded relay event to the hub client's
// immediate queue. A hub that is absent or disconnected is not an error: the
// event file is durable and the node outbox retries from it.
func (d *Daemon) emitRelayEvent(req localRequest) error {
	if !emitRelayKinds[req.Kind] {
		return &codedError{ExitUsage, fmt.Errorf("invalid emit request")}
	}
	if req.Kind == "lane.event" {
		if !hubAgentLabelPattern.MatchString(req.OwnerLane) || !validLaneEventID(req.EventID) || !validLaneEventText(req.Text) || len(req.Text) > laneEventTextLimitSink {
			return &codedError{ExitUsage, fmt.Errorf("invalid emit request")}
		}
	} else if !hubJobIDPattern.MatchString(req.JobID) || req.ReportPath == "" {
		return &codedError{ExitUsage, fmt.Errorf("invalid emit request")}
	}
	if !d.emitNamespaceMatches(req.InboxRoot) {
		local := d.emitNamespaceRoot()
		d.cfg.Logger.Warn("emit inbox root mismatch", "daemon", local, "given", req.InboxRoot)
		return &codedError{ExitUsage, fmt.Errorf("inbox root mismatch (daemon=%s, given=%s)", local, req.InboxRoot)}
	}
	epoch := req.Epoch
	if epoch == 0 {
		epoch = 1
	}
	if d.cfg.Hub.Client == nil {
		return nil
	}
	event := hubScannedRelayEvent{
		Kind: req.Kind,
		HubActiveJob: HubActiveJob{
			JobID: req.JobID, Epoch: epoch, AgentLabel: req.AgentLabel, OwnerLane: req.OwnerLane,
			Label: req.Label, Host: req.Host, ReportPath: req.ReportPath, ReportLastLine: req.ReportLastLine,
		},
		Reason: req.Reason, Question: req.Question, PR: req.PR, Head: req.Head, PaneID: req.PaneID, EventID: req.EventID, Text: req.Text, Truncated: req.Truncated,
	}
	if req.Kind == "lane.event" {
		event.JobID = laneEventTransportID(req.OwnerLane, req.EventID)
		event.ReportPath, event.Reason = "", ""
	}
	d.cfg.Hub.Client.EnqueueRelayEvent(event)
	return nil
}

// emitNamespaceRoot is only for rejection diagnostics. emitNamespaceMatches
// remains the authority for its acceptance rules, including empty roots.
func (d *Daemon) emitNamespaceRoot() string {
	local := ""
	if d.cfg.Hub.Client != nil {
		local = d.cfg.Hub.Client.jobsInboxRoot
	}
	if local == "" {
		local = d.cfg.InboxRoot
	}
	return local
}

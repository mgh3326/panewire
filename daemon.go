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

type Config struct {
	SocketPath, HerdrSocket, DBPath, InboxRoot string
	StorePromptBody                            bool
	Logging                                    LoggingConfig
	Stage2                                     Stage2Config
	Sentinel                                   SentinelConfig
	Hub                                        HubDaemonConfig
	Store                                      *Store
	SchemaCommand                              []string
	Logger                                     *slog.Logger
}

type LoggingConfig struct {
	StorePromptBody bool
}
type Daemon struct {
	cfg          Config
	store        *Store
	listener     net.Listener
	cancel       context.CancelFunc
	herdr        *HerdrClient
	caps         GuardResult
	stage2Done   chan struct{}
	sentinelDone chan struct{}
	hubDone      chan struct{}
	mu           sync.Mutex
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
	d.caps = guard
	_ = d.RecordSchemaGuard(ctx, guard, "startup")
	if guard.Events && d.cfg.HerdrSocket != "" {
		if c, err := NewHerdrClient(d.cfg.HerdrSocket); err == nil {
			d.herdr = c
			go d.eventLoop(ctx)
		} else {
			d.cfg.Logger.Warn("herdr unavailable", "error", err)
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
	if d.cfg.Stage2.Enabled {
		d.stage2Done = make(chan struct{})
		go func() {
			defer close(d.stage2Done)
			d.stage2Loop(runCtx)
		}()
	}
	if d.cfg.Sentinel.Enabled {
		d.sentinelDone = make(chan struct{})
		go func() {
			defer close(d.sentinelDone)
			d.sentinelLoop(runCtx)
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
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if d.herdr == nil {
			return
		}
		events, err := d.herdr.Subscribe(ctx)
		if err != nil {
			d.cfg.Logger.Warn("herdr subscribe failed", "error", err)
			_ = d.herdr.Close()
			time.Sleep(100 * time.Millisecond)
			if g := d.runGuard(ctx, "reconnect"); true {
				d.caps = g
				_ = d.RecordSchemaGuard(ctx, g, "reconnect")
			}
			if c, e := NewHerdrClient(d.cfg.HerdrSocket); e == nil {
				d.herdr = c
			}
			continue
		}
		for ev := range events {
			if len(ev.UnknownFields) > 2 || !knownHerdrEvent(ev.Kind) {
				d.cfg.Logger.Warn("unknown herdr event; recording without inference", "kind", ev.Kind, "fields", string(ev.UnknownFields))
			}
			recordHerdrEvent(ctx, d.store, ev, d.caps.Protocol, d.caps.Schema)
		}
		_ = d.herdr.Close()
		time.Sleep(100 * time.Millisecond)
		g := d.runGuard(ctx, "reconnect")
		d.caps = g
		_ = d.RecordSchemaGuard(ctx, g, "reconnect")
		if c, e := NewHerdrClient(d.cfg.HerdrSocket); e == nil {
			d.herdr = c
		}
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
		switch req.Op {
		case "wait.file":
			result, err = WaitFile(callCtx, d.store, req.Path, time.Duration(req.SettleMS)*time.Millisecond)
		case "wait.agent":
			if !d.caps.AgentCapability() {
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
		case "prompt":
			if !d.caps.Prompt || !d.caps.AgentRead {
				result, err = recordUnavailablePrompt(callCtx, d.store, PromptRequest{Sender: req.Sender, Target: req.Target, Path: req.Path, Uptake: req.Uptake, StorePromptBody: req.StoreBody || d.cfg.StorePromptBody || d.cfg.Logging.StorePromptBody}, ExitDaemonUnavailable, "prompt capability unavailable")
			} else {
				c2, e := NewHerdrClient(d.cfg.HerdrSocket)
				if e != nil {
					result, err = recordUnavailablePrompt(callCtx, d.store, PromptRequest{Sender: req.Sender, Target: req.Target, Path: req.Path, Uptake: req.Uptake, StorePromptBody: req.StoreBody || d.cfg.StorePromptBody || d.cfg.Logging.StorePromptBody}, ExitDaemonUnavailable, e.Error())
				} else {
					result, err = Prompt(callCtx, d.store, c2, PromptRequest{Sender: req.Sender, Target: req.Target, Path: req.Path, Uptake: req.Uptake, StorePromptBody: req.StoreBody || d.cfg.StorePromptBody || d.cfg.Logging.StorePromptBody}, d.caps)
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
	if d.stage2Done != nil {
		<-d.stage2Done
		d.stage2Done = nil
	}
	if d.sentinelDone != nil {
		<-d.sentinelDone
		d.sentinelDone = nil
	}
	if d.hubDone != nil {
		<-d.hubDone
		d.hubDone = nil
	}
	if d.herdr != nil {
		_ = d.herdr.Close()
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

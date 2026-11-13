package panewire

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/mgh3326/panewire/internal/daemonop"
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
	QuotaStopAct                               bool
	Logging                                    LoggingConfig
	Hub                                        HubDaemonConfig
	StallDetect                                StallDetectConfig
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
	idleWake     *idleWakeManager
	stall        *stallDetectManager
	paneMeta     *paneMetadataPublisher
	caps         GuardResult
	eventDone    chan struct{}
	idleDone     chan struct{}
	hubDone      chan struct{}
	stallDone    chan struct{}
	paneMetaDone chan struct{}
	resub        chan struct{}
	subscribed   map[string]bool
	// lifecycleResubMin and lifecycleSnapshotWait are the debounce floors for
	// lifecycle-triggered stream cycles and idle-wake snapshots. Zero selects
	// the defaults; tests shrink them.
	lifecycleResubMin     time.Duration
	lifecycleSnapshotWait time.Duration
	mu                    sync.Mutex
}

func NewDaemon(cfg Config) *Daemon {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Hub.Client != nil && cfg.Hub.Client.spawnJournalDir == "" && filepath.IsAbs(cfg.DBPath) {
		cfg.Hub.Client.spawnJournalDir = filepath.Join(filepath.Dir(cfg.DBPath), "spawn")
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
			path = defaultDBPath()
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
		if c, err := newHerdrClient(d.cfg.HerdrSocket, d.cfg.Logger); err == nil {
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
		d.cfg.Hub.Client.SetSessionSnapshot(hubSessionSnapshotHook(d.cfg.HerdrSocket))
		idleRoot := d.cfg.Hub.Client.jobsInboxRoot
		if idleRoot == "" {
			idleRoot = d.cfg.InboxRoot
		}
		if d.cfg.HerdrSocket != "" && idleRoot != "" {
			manager, err := newIdleWakeManager(d.store, idleRoot, d.cfg.IdleWakeSettle, d.cfg.Hub.Client.EnqueueIdleWakeRouteRequest, d.cfg.Hub.Client.EnqueueRelayEvent, d.cfg.Logger)
			if err != nil {
				return err
			}
			manager.quota, err = newQuotaStopManager(d.store, idleRoot, d.cfg.QuotaStopAct, d.cfg.Logger)
			if err != nil {
				return err
			}
			manager.quota.session = func(job, pane string) string {
				if session := nodeJobHerdrSessionByPaneFromRoot(idleRoot, pane); session != "" {
					return session
				}
				return d.cfg.Hub.Client.herdrSessionFor(job, pane)
			}
			manager.quota.enqueue = manager.enqueue
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
	if d.cfg.StallDetect.Enabled && d.cfg.HerdrSocket != "" && os.Getenv("PANEWIRE_STALL_DETECT") != "off" {
		stallRoot := d.stallInboxRoot()
		d.resub = make(chan struct{}, 1)
		socket := d.cfg.HerdrSocket
		// Every herdr handle the detector touches is a stallReader: the
		// interface has no input methods, so the wiring cannot express one.
		dial := func() (stallReader, error) { return NewHerdrClient(socket) }
		deps := stallDeps{
			listAgents: func(ctx context.Context) ([]paneIdentity, error) {
				c, err := dial()
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.AgentDetails(ctx)
			},
			readPane: func(ctx context.Context, pane string) (readEvidence, error) {
				c, err := dial()
				if err != nil {
					return readEvidence{}, err
				}
				defer c.Close()
				return c.ReadPane(ctx, pane)
			},
			subscribed: func() (map[string]bool, time.Time) { return d.subscribedSet() },
			resubscribe: func() {
				select {
				case d.resub <- struct{}{}:
				default:
				}
			},
		}
		if d.cfg.Hub.Client != nil {
			deps.enqueue = d.cfg.Hub.Client.EnqueueRelayEvent
		}
		manager, err := newStallDetectManager(d.store, stallRoot, d.cfg.StallDetect, deps, d.cfg.Logger)
		if err != nil {
			return err
		}
		d.stall = manager
		if d.cfg.Hub.Client != nil {
			d.cfg.Hub.Client.SetStallDetect(manager.StallBeat)
		}
	}
	switch {
	case d.stall == nil:
		d.cfg.Logger.Info("stall detect disabled; job panes will not carry pw_ tokens")
	case d.capabilities().PaneMetadata:
		d.paneMeta = newPaneMetadataPublisher(d.store, d.cfg.HerdrSocket, d.cfg.Logger)
		d.store.SetRelayHeldObserver(d.paneMeta.Kick)
	default:
		d.cfg.Logger.Warn("herdr lacks pane.report_metadata; job panes will not carry pw_ tokens")
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
	if d.cfg.HerdrSocket != "" && (guard.Events || d.idleWake != nil || d.stall != nil) {
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
	if d.cfg.Hub.Enabled && d.cfg.Hub.Client != nil {
		d.hubDone = make(chan struct{})
		go func() {
			defer close(d.hubDone)
			d.cfg.Hub.Client.Run(runCtx)
		}()
	}
	if d.stall != nil {
		d.stallDone = make(chan struct{})
		go func() {
			defer close(d.stallDone)
			d.stall.Run(runCtx)
		}()
	}
	if d.paneMeta != nil {
		d.paneMetaDone = make(chan struct{})
		go func() {
			defer close(d.paneMetaDone)
			d.paneMeta.Run(runCtx)
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

// hubSessionSnapshotHook dials herdr per heartbeat just like the pane
// liveness hook. The heartbeat must use a separate short-lived client because
// the event loop owns the daemon's long-lived subscription connection.
func hubSessionSnapshotHook(socket string) func(context.Context) ([]HerdrAgentState, error) {
	if socket == "" {
		return nil
	}
	return func(ctx context.Context) ([]HerdrAgentState, error) {
		client, err := NewHerdrClient(socket)
		if err != nil {
			return nil, err
		}
		defer client.Close()
		return client.AgentStates(ctx)
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
			connected, err := newHerdrClient(d.cfg.HerdrSocket, d.cfg.Logger)
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
		events, panes, err := client.Subscribe(ctx)
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
		// The subscribe response is the coverage proof the stall detector
		// diffs against: panes listed here are the ones this stream watches.
		d.setSubscribedPanes(panes)
		if d.idleWake != nil {
			d.observeIdleWakeSnapshot(ctx, client)
			d.idleWake.Tick(ctx, time.Now().UTC())
		}
		streamOpen := true
		streamCycleAt := time.Now()
		var resubTimer *time.Timer
		var resubDue <-chan time.Time
		var snapTimer *time.Timer
		var snapDue <-chan time.Time
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
				// output_matched is a wake signal only: the payload carries no
				// text, so the detector classifies on its own bounded read.
				if d.stall != nil && (ev.Kind == "pane.output_matched" || ev.Kind == "pane_output_changed") {
					d.stall.Wake(ev.PaneID)
				}
				// Lifecycle events keep the covered set honest and can pull a
				// stream cycle forward: pane_created is recorded only because
				// herdr detects the agent a moment later, and only
				// pane_agent_detected proves one exists to subscribe to.
				switch {
				case isPaneDepartureEvent(ev.Kind):
					d.dropSubscribedPane(ev.PaneID)
				case isAgentDetectedEvent(ev.Kind) && agentDetectionArrival(ev) && ev.PaneID != "" && !d.subscribedPane(ev.PaneID):
					// A pane outside the covered set just gained an agent:
					// cycle the stream so Subscribe's fresh agent.list adds its
					// per-pane subscriptions. Triggers inside the minimum
					// interval coalesce into one deferred cycle at the
					// interval's end — never dropped, never thundering.
					if wait := streamCycleAt.Add(d.lifecycleResubInterval()).Sub(time.Now()); wait <= 0 {
						streamOpen = false
					} else if resubDue == nil {
						resubTimer = time.NewTimer(wait)
						resubDue = resubTimer.C
					}
				}
				if isPaneLifecycleEvent(ev.Kind) && d.idleWake != nil && snapDue == nil {
					snapTimer = time.NewTimer(d.lifecycleSnapshotDelay())
					snapDue = snapTimer.C
				}
			case at := <-settle:
				d.idleWake.Tick(ctx, at.UTC())
			case <-poll:
				d.observeIdleWakeSnapshot(ctx, client)
			case <-d.resub:
				// The detector saw a tracked pane outside the covered set.
				// Cycling the stream re-runs Subscribe against the fresh
				// agent.list, which is also the reconnect coverage proof.
				streamOpen = false
			case <-resubDue:
				resubDue = nil
				streamOpen = false
			case <-snapDue:
				snapDue = nil
				snapTimer.Stop()
				d.observeIdleWakeSnapshot(ctx, client)
			}
		}
		if resubTimer != nil {
			resubTimer.Stop()
		}
		if snapTimer != nil {
			snapTimer.Stop()
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
	// A dropped stream proves nothing about coverage; the next Subscribe
	// re-establishes the set. Until then the detector must not read the old
	// one as current.
	d.subscribed = nil
	d.mu.Unlock()
	_ = client.Close()
}

func (d *Daemon) setSubscribedPanes(panes []string) {
	set := make(map[string]bool, len(panes))
	for _, pane := range panes {
		set[pane] = true
	}
	d.mu.Lock()
	d.subscribed = set
	d.mu.Unlock()
}

func (d *Daemon) subscribedSet() (map[string]bool, time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.subscribed == nil {
		return nil, time.Time{}
	}
	set := make(map[string]bool, len(d.subscribed))
	for pane := range d.subscribed {
		set[pane] = true
	}
	return set, time.Now().UTC()
}

func (d *Daemon) subscribedPane(paneID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.subscribed[paneID]
}

// dropSubscribedPane removes a pane the stream no longer has reason to watch —
// herdr reported it closed or exited — without touching the stream itself.
func (d *Daemon) dropSubscribedPane(paneID string) {
	if paneID == "" {
		return
	}
	d.mu.Lock()
	if d.subscribed != nil {
		delete(d.subscribed, paneID)
	}
	d.mu.Unlock()
}

const (
	defaultLifecycleResubMinInterval = 2 * time.Second
	defaultLifecycleSnapshotWait     = 200 * time.Millisecond
)

func (d *Daemon) lifecycleResubInterval() time.Duration {
	if d.lifecycleResubMin > 0 {
		return d.lifecycleResubMin
	}
	return defaultLifecycleResubMinInterval
}

func (d *Daemon) lifecycleSnapshotDelay() time.Duration {
	if d.lifecycleSnapshotWait > 0 {
		return d.lifecycleSnapshotWait
	}
	return defaultLifecycleSnapshotWait
}

// agentDetectionArrival reports whether pane_agent_detected names a live,
// unreleased agent — the only lifecycle event that proves a new pane needs
// per-pane subscriptions. A null agent or released flag is a departure.
func agentDetectionArrival(ev HerdrEvent) bool {
	if ev.Released {
		return false
	}
	agent := bytes.TrimSpace(ev.Agent)
	return len(agent) > 0 && !bytes.Equal(agent, []byte("null"))
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
	case "pane.agent_status_changed", "pane.output_matched", "pane.scroll_changed", "pane_output_changed", "pane_agent_status_changed", "pane_scroll_changed",
		"pane.created", "pane_created", "pane.closed", "pane_closed", "pane.exited", "pane_exited", "pane.agent_detected", "pane_agent_detected":
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
	JobClose      *hubJobCloseRequest   `json:"job_close,omitempty"`
	Inbox         *inboxRequest         `json:"inbox,omitempty"`
	OwnJobs       *ownJobsRequest       `json:"own_jobs,omitempty"`
	PRStatus      *localPRStatusRequest `json:"pr_status,omitempty"`
	DeliveryClass string                `json:"delivery_class,omitempty"`
	Late          bool                  `json:"late,omitempty"`
	NotifyType    string                `json:"notify_type,omitempty"`
	Cause         string                `json:"cause,omitempty"`
	Op            string                `json:"op"`
	Path          string                `json:"path,omitempty"`
	Target        string                `json:"target,omitempty"`
	Sender        string                `json:"sender,omitempty"`
	Uptake        string                `json:"uptake,omitempty"`
	Directive     bool                  `json:"directive,omitempty"`
	StoreBody     bool                  `json:"store_body,omitempty"`
	Status        string                `json:"status,omitempty"`
	SinceMS       int64                 `json:"since_ms,omitempty"`
	SettleMS      int64                 `json:"settle_ms,omitempty"`
	TimeoutMS     int64                 `json:"timeout_ms,omitempty"`
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
	// LanesPut carries a lane registration for the lanes.put op; the field
	// names are the shared socket contract in internal/daemonop.
	daemonop.LanesPut
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
		case "inbox":
			result, err = d.handleInbox(callCtx, req.Inbox)
		case "jobs.own":
			result, err = d.handleOwnJobs(callCtx, req.OwnJobs)
		case daemonop.LanesPutOp:
			result, err = d.handleLanesPut(callCtx, req.LanesPut)
		case "job.close":
			result, err = d.handleJobClose(callCtx, req.JobClose)
		case "pr.status":
			result, err = d.handlePRStatus(callCtx, req.PRStatus)
		case "decision.reply":
			if req.Target == "" || req.SinceMS <= 0 {
				err = &codedError{ExitUsage, fmt.Errorf("pane and since are required")}
			} else {
				result, err = d.store.DecisionReplyAfter(callCtx, req.Target, req.OwnerLane, req.SinceMS)
			}
		case "prompt":
			if !caps.Prompt || !caps.AgentRead {
				result, err = recordUnavailablePrompt(callCtx, d.store, PromptRequest{Sender: req.Sender, Target: req.Target, Path: req.Path, Uptake: req.Uptake, StorePromptBody: req.StoreBody || d.cfg.StorePromptBody || d.cfg.Logging.StorePromptBody}, ExitDaemonUnavailable, "prompt capability unavailable")
			} else {
				c2, e := NewHerdrClient(d.cfg.HerdrSocket)
				if e != nil {
					result, err = recordUnavailablePrompt(callCtx, d.store, PromptRequest{Sender: req.Sender, Target: req.Target, Path: req.Path, Uptake: req.Uptake, StorePromptBody: req.StoreBody || d.cfg.StorePromptBody || d.cfg.Logging.StorePromptBody}, ExitDaemonUnavailable, e.Error())
				} else {
					result, err = Prompt(callCtx, d.store, c2, PromptRequest{Sender: req.Sender, Target: req.Target, Path: req.Path, Uptake: req.Uptake, Directive: req.Directive, StorePromptBody: req.StoreBody || d.cfg.StorePromptBody || d.cfg.Logging.StorePromptBody}, caps)
					_ = c2.Close()
				}
			}
		default:
			err = &codedError{ExitUsage, errors.New(localUnknownOperation)}
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
	// The node finishes what is in flight, for a bounded time, before its
	// loops are cancelled: a launch, an injection and a result upload cut off
	// here would otherwise wait for the hub's deadline.
	if d.cfg.Hub.Client != nil {
		d.cfg.Hub.Client.Drain(context.Background())
	}
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
	if d.hubDone != nil {
		<-d.hubDone
		d.hubDone = nil
	}
	if d.stallDone != nil {
		<-d.stallDone
		d.stallDone = nil
	}
	if d.paneMetaDone != nil {
		<-d.paneMetaDone
		d.paneMetaDone = nil
	}
	if d.listener != nil {
		_ = d.listener.Close()
	}
	if d.cfg.SocketPath != "" {
		_ = os.Remove(d.cfg.SocketPath)
	}
	if d.store != nil {
		if err := d.store.Close(); err != nil {
			return err
		}
	}
	return nil
}

// stallInboxRoot is the inbox root the stall detector scans: the hub jobs
// root, else --inbox-root, else the same per-OS default `panewire emit` uses.
func (d *Daemon) stallInboxRoot() string {
	if d.cfg.Hub.Client != nil && d.cfg.Hub.Client.jobsInboxRoot != "" {
		return d.cfg.Hub.Client.jobsInboxRoot
	}
	if d.cfg.InboxRoot != "" {
		return d.cfg.InboxRoot
	}
	return defaultInboxRoot()
}

func (d *Daemon) SocketPath() string {
	if d.cfg.SocketPath != "" {
		return d.cfg.SocketPath
	}
	return defaultSocketPath()
}
func defaultSocketPath() string {
	home, _ := os.UserHomeDir()
	return daemonop.DefaultSocketPath(home)
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
	if !emitRelayKinds[req.Kind] || !validDeliveryClass(req.DeliveryClass) || !validNotifyType(req.NotifyType) || !validInboxCause(req.Cause) {
		return &codedError{ExitUsage, fmt.Errorf("invalid emit request")}
	}
	if req.Kind == "lane.event" {
		if !LaneNamePattern.MatchString(req.OwnerLane) || !validLaneEventID(req.EventID) || !validLaneEventText(req.Text) || len(req.Text) > laneEventTextLimitSink {
			return &codedError{ExitUsage, fmt.Errorf("invalid emit request")}
		}
	} else if !hubJobIDPattern.MatchString(req.JobID) || req.ReportPath == "" {
		return &codedError{ExitUsage, fmt.Errorf("invalid emit request")}
	} else if relayTerminalSignalKinds[req.Kind] && (req.Reason == "" || !validLaneReference(req.OwnerLane)) {
		// Same rule as emitJobRecord: a terminal signal that cannot state why
		// or name its owner lane is refused before it enters the outbox.
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
		return &codedError{ExitDaemonUnavailable, fmt.Errorf("hub client unavailable")}
	}
	event := hubScannedRelayEvent{
		DeliveryClass: req.DeliveryClass, Late: req.Late, NotifyType: req.NotifyType, Cause: req.Cause, Kind: req.Kind,
		HubActiveJob: HubActiveJob{
			JobID: req.JobID, Epoch: epoch, AgentLabel: req.AgentLabel, OwnerLane: req.OwnerLane,
			Label: req.Label, Host: req.Host, ReportPath: req.ReportPath, ReportLastLine: req.ReportLastLine,
		},
		Reason: req.Reason, Question: req.Question, PR: req.PR, Head: req.Head, PaneID: req.PaneID, EventID: req.EventID, Text: req.Text, Truncated: req.Truncated,
	}
	if req.Kind == "lane.event" {
		event.JobID = laneEventTransportID(req.OwnerLane, req.EventID)
		event.ReportPath, event.Reason = "", ""
	} else {
		// A job.* event's identity is its durable file name, derived here from
		// the daemon's own namespace so the immediate send and the later scan
		// name the same outbox row. The file's timestamp feeds the deployment
		// cutoff the scan applies.
		event.EventID, event.EventTime = emitJobEventFileID(d.emitNamespaceRoot(), req, epoch)
		// A terminal signal's absent report — empty or the sentinel's
		// /dev/null — resolves to the same durable event file the scanner
		// substitutes; otherwise a direct push and the scan would key one
		// event two ways and deliver it twice.
		event.ReportPath = relaySignalReportPath(req.Kind, event.ReportPath)
		if relayTerminalSignalKinds[req.Kind] && event.ReportPath == "" && event.EventID != "" {
			event.ReportPath = filepath.Join(d.emitNamespaceRoot(), "jobs", req.JobID, "events", event.EventID)
		}
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

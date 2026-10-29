package panewire

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// IdleShutdownConfig is the node-local machine poweroff, off by default. When
// --idle-shutdown enables it, the node powers itself off only after the idle
// conditions below have held continuously for After.
type IdleShutdownConfig struct {
	Enabled       bool
	After         time.Duration
	PollInterval  time.Duration
	Command       []string
	KeepAwakePath string
	Lane          string
	// NotifySettle gives the durable lane.event a moment to leave the node
	// before the machine carrying it is powered off. Production 2s; tests
	// use zero.
	NotifySettle time.Duration
}

// idleShutdownDeps are the probe and action seams. Every collector is a
// function so tests never run ps, who, or a real shutdown.
type idleShutdownDeps struct {
	machineID   string
	sessions    func(context.Context) ([]HerdrAgentState, error)
	jobLeases   func(context.Context) ([]HubActiveJob, error)
	procs       func(context.Context) ([]string, error)
	interactive func(context.Context) (int, error)
	keepAwake   func() bool
	writeRecord func(string, emitRecord) (string, error)
	enqueue     func(hubScannedRelayEvent) bool
	shutdown    func(context.Context) error
	now         func() time.Time
}

type idleShutdownManager struct {
	cfg       IdleShutdownConfig
	deps      idleShutdownDeps
	inboxRoot string
	log       *slog.Logger

	mu        sync.Mutex
	idleSince time.Time
	lastBlock string
	// committed latches once a shutdown has been emitted and issued, so one
	// daemon run can never repeat the notification or the command. A failed
	// command clears it only after restarting the idle window.
	committed bool
}

func newIdleShutdownManager(cfg IdleShutdownConfig, inboxRoot string, deps idleShutdownDeps, logger *slog.Logger) *idleShutdownManager {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Minute
	}
	if cfg.Lane == "" {
		cfg.Lane = "operator-desk"
	}
	if cfg.NotifySettle <= 0 {
		cfg.NotifySettle = 2 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	if deps.sessions == nil {
		deps.sessions = func(context.Context) ([]HerdrAgentState, error) { return nil, errors.New("session probe unset") }
	}
	if deps.jobLeases == nil {
		deps.jobLeases = func(context.Context) ([]HubActiveJob, error) { return nil, errors.New("job lease probe unset") }
	}
	if deps.procs == nil {
		deps.procs = collectIdleShutdownProcs
	}
	if deps.interactive == nil {
		deps.interactive = collectIdleShutdownLogins
	}
	if deps.keepAwake == nil {
		deps.keepAwake = func() bool { return idleShutdownKeepAwake(cfg.KeepAwakePath) }
	}
	if deps.writeRecord == nil {
		deps.writeRecord = ensureLaneEmitRecord
	}
	if deps.shutdown == nil {
		deps.shutdown = idleShutdownCommandRunner(cfg.Command)
	}
	if deps.now == nil {
		deps.now = time.Now
	}
	return &idleShutdownManager{cfg: cfg, deps: deps, inboxRoot: inboxRoot, log: logger}
}

func (m *idleShutdownManager) Run(ctx context.Context) {
	ticker := time.NewTicker(m.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.tick(ctx)
		}
	}
}

// tick evaluates one observation. A blocker clears the window; a clear window
// only commits a shutdown once it has been open for the whole configured
// duration, so the machine must be continuously idle for After.
func (m *idleShutdownManager) tick(ctx context.Context) {
	at := m.deps.now()
	blocker := m.blockedReason(ctx)
	m.mu.Lock()
	if m.committed {
		m.mu.Unlock()
		return
	}
	if blocker != "" {
		transition := !m.idleSince.IsZero() || blocker != m.lastBlock
		m.idleSince = time.Time{}
		m.lastBlock = blocker
		m.mu.Unlock()
		if transition {
			m.log.Info("idle-shutdown blocked", "reason", blocker)
		}
		return
	}
	if m.idleSince.IsZero() {
		m.idleSince = at
		m.lastBlock = ""
		m.mu.Unlock()
		m.log.Info("idle-shutdown idle window started")
		return
	}
	elapsed := at.Sub(m.idleSince)
	m.committed = elapsed >= m.cfg.After
	m.mu.Unlock()
	if !m.committed {
		return
	}
	m.log.Warn("idle-shutdown decision", "idle_for", elapsed.String(), "lane", m.cfg.Lane)
	m.decide(ctx, at, elapsed)
}

// blockedReason returns "" only when every condition is clear. Any probe
// failure reports busy in the safe direction: a node that cannot see its own
// state must not power itself off.
func (m *idleShutdownManager) blockedReason(ctx context.Context) string {
	sessions, sessionsErr := m.deps.sessions(ctx)
	jobs, jobsErr := m.deps.jobLeases(ctx)
	if sessionsErr != nil {
		return "sessions_unavailable"
	}
	if jobsErr != nil {
		return "jobs_unavailable"
	}
	held := make(map[string]struct{}, len(jobs)*2)
	for _, job := range jobs {
		if job.Pane != "" {
			held[job.Pane] = struct{}{}
		}
		if job.AgentLabel != "" {
			held[job.AgentLabel] = struct{}{}
		}
	}
	for _, session := range sessions {
		// Fail-safe rule: only a positively-idle observation clears. "idle"
		// and "done" are the states herdr can prove carry no work; "working"
		// and "blocked" are visibly busy; "unknown", an empty status, or a
		// status this build does not recognise is not proof of idleness.
		switch session.Status {
		case "working", "blocked":
			return "pane_active"
		case "idle", "done":
		default:
			return "pane_unknown"
		}
		if _, busy := held[session.PaneID]; busy {
			return "pane_holding_job"
		}
		if _, busy := held[session.AgentName]; busy {
			return "pane_holding_job"
		}
		if _, busy := held[session.Label]; busy {
			return "pane_holding_job"
		}
	}
	if len(jobs) > 0 {
		return "jobs_active"
	}
	procs, err := m.deps.procs(ctx)
	if err != nil {
		return "procs_unavailable"
	}
	for _, line := range procs {
		if idleShutdownRunnerProc(line) {
			return "ci_runner"
		}
	}
	users, err := m.deps.interactive(ctx)
	if err != nil {
		return "users_unavailable"
	}
	if users > 0 {
		return "user_session"
	}
	if m.deps.keepAwake() {
		return "keep_awake"
	}
	return ""
}

// idleShutdownRunnerProc identifies the GitHub Actions worker by the basename
// of any argv token: an install path containing spaces splits argv[0] across
// fields, so only checking the first field would miss it. A stray
// "Runner.Worker" token (e.g. a user grepping for it) merely keeps the
// machine awake — the safe direction.
func idleShutdownRunnerProc(line string) bool {
	for _, field := range strings.Fields(line) {
		if filepath.Base(field) == "Runner.Worker" {
			return true
		}
	}
	return false
}

// decide emits the single durable lane.event, waits the notify settle, then
// invokes the configured command as a structured argv — never a shell line.
// The event is best-effort: a node that cannot reach its desk still performs
// the poweroff it was configured to do, and logs the gap.
func (m *idleShutdownManager) decide(ctx context.Context, at time.Time, idleFor time.Duration) {
	eventID := "idle-shutdown:" + at.UTC().Format(time.RFC3339Nano)
	body := map[string]any{"kind": "idle_shutdown", "idle_seconds": int64(idleFor.Seconds())}
	if m.deps.machineID != "" {
		body["machine"] = m.deps.machineID
	}
	textBytes, _ := json.Marshal(body)
	text := string(textBytes)
	record := emitRecord{Type: "lane.event", Epoch: 1, CreatedAt: at.Format(time.RFC3339), OwnerLane: m.cfg.Lane, EventID: eventID, Text: text}
	// One retry before accepting the gap: the durable record is the only
	// notice the desk gets before this machine powers off.
	_, writeErr := m.deps.writeRecord(m.inboxRoot, record)
	if writeErr != nil {
		_, writeErr = m.deps.writeRecord(m.inboxRoot, record)
	}
	if writeErr != nil {
		m.log.Error("idle-shutdown lane event write failed", "error", writeErr)
	} else if m.deps.enqueue != nil {
		m.deps.enqueue(hubScannedRelayEvent{
			Kind:         "lane.event",
			HubActiveJob: HubActiveJob{JobID: laneEventTransportID(m.cfg.Lane, eventID), Epoch: 1, OwnerLane: m.cfg.Lane},
			EventID:      eventID, Text: text,
		})
	}
	if m.cfg.NotifySettle > 0 {
		timer := time.NewTimer(m.cfg.NotifySettle)
		select {
		case <-ctx.Done():
			timer.Stop()
			m.mu.Lock()
			m.committed = false
			m.mu.Unlock()
			return
		case <-timer.C:
		}
	}
	// The settle gave the lane event time to leave; it also opened a window
	// for work to start, so the conditions are probed one last time before the
	// irreversible step.
	if blocker := m.blockedReason(ctx); blocker != "" {
		m.log.Warn("idle-shutdown aborted after settle", "reason", blocker)
		m.mu.Lock()
		m.committed = false
		m.idleSince = time.Time{}
		m.lastBlock = blocker
		m.mu.Unlock()
		return
	}
	if err := m.deps.shutdown(ctx); err != nil {
		m.log.Error("idle-shutdown command failed", "error", err)
		// A refused command restarts the idle window so the node tries again
		// only after another full quiet stretch, never in a tight loop. The
		// window restarts at the clock read after the command returned, not at
		// the decision, so a long-running failure cannot re-commit at once.
		m.mu.Lock()
		m.committed = false
		m.idleSince = m.deps.now()
		m.mu.Unlock()
		return
	}
	m.log.Warn("idle-shutdown command issued")
}

// collectIdleShutdownProcs is the production process probe: one argv line per
// process, matching the shape tests feed the fake.
func collectIdleShutdownProcs(ctx context.Context) ([]string, error) {
	command := exec.CommandContext(ctx, "ps", "-axo", "args=")
	output, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("process list unavailable")
	}
	lines := make([]string, 0, 128)
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		if line := strings.TrimSpace(scanner.Text()); line != "" {
			lines = append(lines, line)
		}
	}
	return lines, scanner.Err()
}

// collectIdleShutdownLogins counts utmp login sessions. Zero lines means no
// interactive session is holding the machine; a failed probe is a blocker,
// never a clearance.
func collectIdleShutdownLogins(ctx context.Context) (int, error) {
	command := exec.CommandContext(ctx, "who")
	output, err := command.Output()
	if err != nil {
		return 0, fmt.Errorf("login session list unavailable")
	}
	count := 0
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) != "" {
			count++
		}
	}
	return count, scanner.Err()
}

func idleShutdownKeepAwake(path string) bool {
	if path == "" {
		return false
	}
	_, err := os.Stat(path)
	// Any stat error other than a plain miss is unknown state and counts as
	// present: the marker fails toward staying awake.
	return err == nil || !os.IsNotExist(err)
}

func idleShutdownCommandRunner(argv []string) func(context.Context) error {
	return func(ctx context.Context) error {
		if len(argv) == 0 {
			return errors.New("idle shutdown command is empty")
		}
		command := exec.CommandContext(ctx, argv[0], argv[1:]...)
		command.Stdout, command.Stderr = io.Discard, io.Discard
		return command.Run()
	}
}

package panewire

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type CLIConfig struct{ SocketPath string }

func RunCLI(args []string, cfg CLIConfig) int {
	if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
		fmt.Fprintln(os.Stdout, "Usage: panewire COMMAND [arguments]")
		fmt.Fprintln(os.Stdout, "Commands: daemon, hub, hub-status, hub-emit, burst, place, prompt, deliveries, jobs, lanes, lanes-audit, fleet-census, session-reap, relay, emit, job, wait, version")
		fmt.Fprintln(os.Stdout)
		fmt.Fprintln(os.Stdout, lanesUsage)
		return ExitOK
	}
	if len(args) == 0 {
		return ExitUsage
	}
	if args[0] == "hub-status" {
		return runHubStatusCLI(args[1:], os.Stdout, os.Stderr, hubCLIDeps{})
	}
	if args[0] == "hub-emit" {
		return runHubEmitCLI(args[1:], os.Stdout, os.Stderr, hubCLIDeps{})
	}
	if args[0] == "burst" {
		return runBurstCLI(args[1:], os.Stdout, os.Stderr)
	}
	if args[0] == "place" {
		return runPlaceCLI(args[1:], os.Stdout, os.Stderr, hubCLIDeps{})
	}
	if args[0] == "prompt" {
		return runPromptCLI(args[1:], cfg)
	}
	if args[0] == "deliveries" {
		return runDeliveriesCLI(args[1:], os.Stdout, os.Stderr)
	}
	if args[0] == "jobs" {
		return runJobsCLI(args[1:], os.Stdout, os.Stderr, hubCLIDeps{})
	}
	if args[0] == "lanes" {
		return runLanesCLI(args[1:], os.Stdout, os.Stderr, hubCLIDeps{})
	}
	if args[0] == "lanes-audit" {
		return runLanesAuditCLI(args[1:], os.Stdout, os.Stderr, hubCLIDeps{})
	}
	if args[0] == "fleet-census" {
		return runFleetCensusCLI(args[1:], os.Stdout, os.Stderr, hubCLIDeps{})
	}
	if args[0] == "session-reap" {
		return runSessionReapCLI(args[1:], os.Stdout, os.Stderr, hubCLIDeps{})
	}
	if args[0] == "relay" {
		return runRelayCLI(args[1:], os.Stdout, os.Stderr)
	}
	if args[0] == "emit" {
		return runEmitCLI(args[1:], os.Stdout, os.Stderr, cfg)
	}
	if args[0] == "job" {
		return runJobCLI(args[1:], os.Stdout, os.Stderr, cfg)
	}
	if args[0] != "wait" {
		return ExitUsage
	}
	fs := flag.NewFlagSet("panewire wait", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	file := fs.String("file", "", "file path")
	agent := fs.String("agent", "", "agent target (name, label, or pane_id)")
	status := fs.String("status", "", "agent status")
	settle := fs.Duration("settle", 0, "settle duration")
	timeout := fs.Duration("timeout", 0, "overall timeout")
	if err := fs.Parse(args[1:]); err != nil {
		return ExitUsage
	}
	if *timeout <= 0 {
		return ExitUsage
	}
	if (*file == "") == (*agent == "") {
		return ExitUsage
	}
	if *file != "" && *status != "" {
		return ExitUsage
	}
	if *agent != "" && !validStatus(*status) {
		return ExitConditionInvalid
	}
	path := cfg.SocketPath
	if path == "" {
		path = socketPathFromEnv()
	}
	conn, err := net.Dial("unix", path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "panewired unavailable: %s\n", path)
		return ExitDaemonUnavailable
	}
	defer conn.Close()
	op := "wait.file"
	if *agent != "" {
		op = "wait.agent"
	}
	req := localRequest{Op: op, Path: *file, Target: *agent, Status: *status, SettleMS: settle.Milliseconds(), TimeoutMS: timeout.Milliseconds()}
	b, _ := json.Marshal(req)
	if _, err = fmt.Fprintf(conn, "%s\n", b); err != nil {
		return ExitDaemonUnavailable
	}
	scan := bufio.NewScanner(conn)
	if !scan.Scan() {
		return ExitDaemonUnavailable
	}
	var resp localResponse
	if json.Unmarshal(scan.Bytes(), &resp) != nil {
		return ExitInternal
	}
	if !resp.OK {
		if resp.Error != "" {
			fmt.Fprintln(os.Stderr, resp.Error)
		}
		return resp.Code
	}
	if result, ok := resp.Result.(map[string]any); ok {
		if cached, _ := result["cached"].(bool); cached {
			fmt.Fprintln(os.Stderr, "cached delivery; no prompt reinjected")
		}
	}
	return ExitOK
}

func runPromptCLI(args []string, cfg CLIConfig) int {
	fs := flag.NewFlagSet("panewire prompt", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	sender := fs.String("from", "", "sender provenance")
	target := fs.String("to", "", "recipient target")
	file := fs.String("file", "", "prompt file")
	uptake := fs.String("uptake", "", "uptake mode")
	timeout := fs.Duration("timeout", 2*time.Second, "overall timeout")
	storeBody := fs.Bool("store-prompt-body", false, "opt in to storing prompt body")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	if *sender == "" || *target == "" || *file == "" || *timeout <= 0 || (*uptake != "" && *uptake != "tool" && *uptake != "status-transition") {
		return ExitConditionInvalid
	}
	path := cfg.SocketPath
	if path == "" {
		path = socketPathFromEnv()
	}
	conn, err := net.Dial("unix", path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "panewired unavailable: %s\n", path)
		return ExitDaemonUnavailable
	}
	defer conn.Close()
	req := localRequest{Op: "prompt", Sender: *sender, Target: *target, Path: *file, Uptake: *uptake, StoreBody: *storeBody, TimeoutMS: timeout.Milliseconds()}
	b, _ := json.Marshal(req)
	if _, err = fmt.Fprintf(conn, "%s\n", b); err != nil {
		return ExitDaemonUnavailable
	}
	scan := bufio.NewScanner(conn)
	if !scan.Scan() {
		return ExitDaemonUnavailable
	}
	var resp localResponse
	if json.Unmarshal(scan.Bytes(), &resp) != nil {
		return ExitInternal
	}
	if !resp.OK {
		if resp.Error != "" {
			fmt.Fprintln(os.Stderr, resp.Error)
		}
		return resp.Code
	}
	return ExitOK
}

func socketPathFromEnv() string {
	if path := os.Getenv("PANEWIRE_SOCKET"); path != "" {
		return path
	}
	return defaultSocketPath()
}
func Main(args []string) int {
	return MainWithVersion(args, "panewire-dev")
}

// MainWithVersion is called by the command package so release builds can pass
// their ldflags-injected version into both `panewire version` and node hello.
func MainWithVersion(args []string, version string) int {
	if version == "" || !hubVersionPattern.MatchString(version) {
		version = "panewire-dev"
	}
	if len(args) > 0 && args[0] == "version" {
		if len(args) != 1 {
			return ExitUsage
		}
		fmt.Fprintln(os.Stdout, version)
		return ExitOK
	}
	if len(args) > 0 && args[0] == "daemon" {
		return runDaemonCLIWithDeps(args[1:], daemonCLIDeps{Version: version})
	}
	if len(args) > 0 && args[0] == "hub" {
		return runHubCLI(args[1:])
	}
	return RunCLI(args, CLIConfig{})
}

type daemonCLIDeps struct {
	HTTPClient            *http.Client
	AllowInsecureForTests bool
	TelegramBaseURL       string
	SchemaCommand         []string
	Logger                *slog.Logger
	Version               string
	ExecutablePath        string // fixture seam; production uses os.Executable.
}

func runDaemonCLI(args []string) int {
	return runDaemonCLIWithDeps(args, daemonCLIDeps{})
}

func runDaemonCLIWithDeps(args []string, deps daemonCLIDeps) int {
	d, code, err := newDaemonForCLI(args, deps)
	if err != nil {
		fmt.Fprintln(os.Stderr, "daemon configuration rejected:", err)
		return code
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := d.Start(ctx); err != nil {
		_ = d.Stop()
		fmt.Fprintln(os.Stderr, err)
		return ExitInternal
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	_ = d.Stop()
	return ExitOK
}

// newDaemonForCLI keeps daemon assembly independently testable: the production
// entrypoint passes no dependencies, while fixtures can use an httptest client
// and an explicitly permitted HTTP endpoint without weakening production TLS.
func newDaemonForCLI(args []string, deps daemonCLIDeps) (*Daemon, int, error) {
	fs := flag.NewFlagSet("panewire daemon", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	socket := fs.String("socket", socketPathFromEnv(), "local daemon socket")
	herdr := fs.String("herdr-socket", "", "herdr socket")
	db := fs.String("db", "", "SQLite path")
	inbox := fs.String("inbox-root", "", "inbox root")
	storeBody := fs.Bool("store-prompt-body", false, "opt in to storing prompt bodies")
	var hubURLs hubURLValues
	fs.Var(&hubURLs, "hub-url", "repeatable optional WSS hub base URL (first is preferred)")
	hubTokenEnv := fs.String("hub-token-env", "", "optional mode-0600 HUB_MACHINE_ID/HUB_TOKEN env file")
	hubCFEnv := fs.String("hub-cf-env", "", "optional mode-0600 CF_ACCESS_CLIENT_ID/CF_ACCESS_CLIENT_SECRET env file")
	hubAccepting := fs.Bool("hub-accepting", false, "advertise readiness for paper or standby jobs to the hub")
	hubJobsRoot := fs.String("hub-jobs-root", "", "local inbox root containing jobs/*/events for metadata-only heartbeats")
	idleWakeSettle := fs.Duration("idle-wake-settle", defaultIdleWakeSettle, "stable idle/done interval before owner notification")
	failoverWakeOn := fs.String("failover-wake-on", "", "fixed failover machine ID that may receive one Wake-on-LAN packet")
	failoverWakeMAC := fs.String("failover-wake-mac", "", "fixed Wake-on-LAN MAC address for --failover-wake-on")
	burstWakeMAC := fs.String("burst-wake-mac", "", "Wake-on-LAN MAC for hub burst events (defaults to failover MAC when configured)")
	burstPoweroffAllowed := fs.Bool("burst-poweroff-allowed", false, "allow authenticated hub burst down events to run sudo -n /usr/sbin/poweroff")
	checksConfig := fs.String("checks-config", "", "explicit local hub check JSON configuration")
	stallDetect := fs.Bool("stall-detect", false, "observe worker panes for stalls; records only")
	stallNotify := fs.Bool("stall-detect-notify", false, "emit lane notifications for stall incidents (default off: shadow)")
	stallHarness := fs.String("stall-detect-harness", "devin", "comma-separated harness families to observe (empty = all)")
	stallPoll := fs.Duration("stall-detect-poll", defaultStallPollInterval, "periodic pane read interval")
	if fs.Parse(args) != nil {
		return nil, ExitUsage, fmt.Errorf("invalid daemon flags")
	}
	wakeOnSet, wakeMACSet := false, false
	fs.Visit(func(item *flag.Flag) {
		switch item.Name {
		case "failover-wake-on":
			wakeOnSet = true
		case "failover-wake-mac":
			wakeMACSet = true
		}
	})
	if wakeOnSet != wakeMACSet || (wakeOnSet && (*failoverWakeOn == "" || *failoverWakeMAC == "")) {
		return nil, ExitConditionInvalid, fmt.Errorf("failover wake requires both --failover-wake-on and --failover-wake-mac")
	}
	cfg := Config{
		SocketPath:      *socket,
		HerdrSocket:     *herdr,
		DBPath:          *db,
		InboxRoot:       *inbox,
		StorePromptBody: *storeBody,
		IdleWakeSettle:  *idleWakeSettle,
		Logging:         LoggingConfig{StorePromptBody: *storeBody},
		SchemaCommand:   deps.SchemaCommand,
		Logger:          deps.Logger,
	}
	if *stallDetect {
		var harnesses []string
		for _, family := range strings.Split(*stallHarness, ",") {
			if family = strings.TrimSpace(family); family != "" {
				harnesses = append(harnesses, family)
			}
		}
		if *stallPoll <= 0 {
			return nil, ExitConditionInvalid, fmt.Errorf("stall-detect poll interval must be positive")
		}
		cfg.StallDetect = StallDetectConfig{Enabled: true, Notify: *stallNotify, Harnesses: harnesses, PollInterval: *stallPoll}
	}
	var hubClient *HubClient
	if hubFlagsProvided(args) {
		if *idleWakeSettle <= 0 {
			return nil, ExitConditionInvalid, fmt.Errorf("idle-wake settle must be positive")
		}
		if len(hubURLs) == 0 || *hubTokenEnv == "" {
			return nil, ExitConditionInvalid, fmt.Errorf("hub requires both --hub-url and --hub-token-env")
		}
		var checks []HubCheck
		if *checksConfig != "" {
			loadedChecks, err := LoadHubChecksConfig(*checksConfig)
			if err != nil {
				return nil, ExitConditionInvalid, fmt.Errorf("checks config is invalid")
			}
			checks = loadedChecks
		}
		configuredHub, err := buildHubDaemonClientWithBurst(strings.Join(hubURLs, ","), *hubTokenEnv, *hubCFEnv, checks, *hubAccepting, *failoverWakeOn, *failoverWakeMAC, *burstWakeMAC, *burstPoweroffAllowed, deps)
		if err != nil {
			return nil, ExitConditionInvalid, err
		}
		configuredHub.jobsInboxRoot = *hubJobsRoot
		hubClient = configuredHub
		cfg.Hub = HubDaemonConfig{Enabled: true, Client: hubClient}
	}
	if hubClient != nil && hubClient.jobsInboxRoot == "" {
		// Reuse the daemon's already-authorized local inbox when available; the
		// explicit hub flag remains useful for a dedicated jobs tree.
		hubClient.jobsInboxRoot = cfg.InboxRoot
	}
	return NewDaemon(cfg), ExitOK, nil
}

type hubURLValues []string

func (values *hubURLValues) String() string { return strings.Join(*values, ",") }
func (values *hubURLValues) Set(raw string) error {
	for _, value := range strings.Split(raw, ",") {
		value = strings.TrimSpace(value)
		if value == "" {
			return fmt.Errorf("hub URL is required")
		}
		*values = append(*values, value)
	}
	return nil
}

func hubFlagsProvided(args []string) bool {
	for _, name := range []string{"hub-url", "hub-token-env", "hub-cf-env", "hub-accepting", "hub-jobs-root", "idle-wake-settle", "failover-wake-on", "failover-wake-mac", "burst-wake-mac", "burst-poweroff-allowed", "checks-config"} {
		flagName := "--" + name
		for _, arg := range args {
			if arg == flagName || strings.HasPrefix(arg, flagName+"=") {
				return true
			}
		}
	}
	return false
}

func joinArgs(a []string) string { return strings.Join(a, " ") }

var _ = joinArgs

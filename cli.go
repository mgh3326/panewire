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
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/mgh3326/panewire/stage2/adapters/supabase"
	"github.com/mgh3326/panewire/stage2/adapters/wrkgate"
	"github.com/mgh3326/panewire/stage2/core"
)

type CLIConfig struct{ SocketPath string }

func RunCLI(args []string, cfg CLIConfig) int {
	if len(args) == 0 {
		return ExitUsage
	}
	if args[0] == "enroll-machine" {
		return runEnrollMachineCLI(args[1:], os.Stdout, os.Stderr, enrollDeps{})
	}
	if args[0] == "smoke-supabase" {
		return runSmokeSupabaseCLI(args[1:], os.Stdout, os.Stderr, smokeDeps{})
	}
	if args[0] == "hub-status" {
		return runHubStatusCLI(args[1:], os.Stdout, os.Stderr, hubCLIDeps{})
	}
	if args[0] == "hub-emit" {
		return runHubEmitCLI(args[1:], os.Stdout, os.Stderr, hubCLIDeps{})
	}
	if args[0] == "update" {
		return runUpdateCLI(args[1:], os.Stdout, os.Stderr, hubCLIDeps{})
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
	if args[0] == "submit" {
		return runSubmitCLI(args[1:])
	}
	if args[0] == "outbox" {
		return runOutboxCLI(args[1:])
	}
	if args[0] == "jobs" {
		return runJobsCLI(args[1:], os.Stdout, os.Stderr, hubCLIDeps{})
	}
	if args[0] == "relay" {
		return runRelayCLI(args[1:], os.Stdout, os.Stderr)
	}
	if args[0] != "wait" {
		return ExitUsage
	}
	fs := flag.NewFlagSet("panewire wait", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	file := fs.String("file", "", "file path")
	agent := fs.String("agent", "", "agent target")
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

// runSubmitCLI only durably records a stage2 outbox row. Publishing remains
// the explicit default-off daemon loop, so this command never opens a remote
// connection or loads a credential file.
func runSubmitCLI(args []string) int {
	return runSubmitCLIWithWriters(args, os.Stdout, os.Stderr)
}

func runSubmitCLIWithWriters(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("panewire submit", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dbPath := fs.String("db", "", "stage2 metadata SQLite path")
	file := fs.String("file", "", "canonical source file")
	from := fs.String("from-machine", "", "stable source machine identity")
	destinationMachine := fs.String("destination-machine", "", "stable destination machine identity (legacy long form)")
	to := fs.String("to", "", "stable destination machine identity")
	namespace := fs.String("namespace", "inbox", "approved destination inbox namespace")
	legacyLogicalPath := fs.String("logical-path", "", "normalized namespace-relative path (legacy long form)")
	logicalPath := fs.String("path", "", "normalized namespace-relative path")
	classification := fs.String("classification", string(core.ClassificationPersonalNonCompany), "public or personal_non_company")
	kind := fs.String("kind", string(core.MessageKindInbox), "inbox.delivery or workflow.completion")
	correlationID := fs.String("correlation-id", "", "original message ID for a workflow completion")
	causationID := fs.String("causation-id", "", "terminal outcome for a workflow completion")
	expiresAt := fs.String("expires-at", "", "RFC3339 UTC expiry (default 72h)")
	policy := fs.String("policy-version", "stage2-allowlist-v1", "classification policy version")
	requestWrk := fs.Bool("request-wrk", false, "request only the receiving wrk admission gate")
	wrkLabel := fs.String("wrk-label", "", "receiving spawn-policy label (required with --request-wrk)")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	destination, err := singleFlagValue(*destinationMachine, *to, "destination machine")
	if err != nil {
		fmt.Fprintln(stderr, err)
		return ExitConditionInvalid
	}
	logical, err := singleFlagValue(*legacyLogicalPath, *logicalPath, "logical path")
	if err != nil {
		fmt.Fprintln(stderr, err)
		return ExitConditionInvalid
	}
	if *dbPath == "" || *file == "" || *from == "" || destination == "" || *namespace == "" || logical == "" {
		return ExitUsage
	}
	if !machineIDPattern.MatchString(*from) || !machineIDPattern.MatchString(destination) {
		fmt.Fprintln(stderr, "submit rejected: machine IDs must be stable lowercase identifiers")
		return ExitConditionInvalid
	}
	if *classification != string(core.ClassificationPublic) && *classification != string(core.ClassificationPersonalNonCompany) {
		fmt.Fprintln(stderr, "submit rejected: classification must be public or personal_non_company")
		return ExitConditionInvalid
	}
	messageKind := core.MessageKind(*kind)
	if messageKind != core.MessageKindInbox && messageKind != core.MessageKindCompletion {
		fmt.Fprintln(stderr, "submit rejected: unsupported message kind")
		return ExitConditionInvalid
	}
	if messageKind == core.MessageKindCompletion && (*correlationID == "" || *causationID == "") {
		fmt.Fprintln(stderr, "submit rejected: workflow completion requires correlation-id and causation-id")
		return ExitConditionInvalid
	}
	if *requestWrk && *wrkLabel == "" {
		fmt.Fprintln(stderr, "submit rejected: --request-wrk requires --wrk-label")
		return ExitConditionInvalid
	}
	if !*requestWrk && *wrkLabel != "" {
		fmt.Fprintln(stderr, "submit rejected: --wrk-label requires --request-wrk")
		return ExitConditionInvalid
	}
	var expiry time.Time
	if *expiresAt != "" {
		expiry, err = time.Parse(time.RFC3339, *expiresAt)
		if err != nil {
			return ExitConditionInvalid
		}
	}
	store, err := core.OpenMetadataStore(*dbPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return ExitInternal
	}
	defer store.Close()
	sender := &core.Sender{Store: store}
	record, err := sender.Submit(context.Background(), core.Submission{
		SourcePath:     *file,
		Source:         core.Identity{MachineID: *from},
		Destination:    core.Destination{MachineID: destination, InboxNamespace: *namespace, LogicalPath: logical},
		Classification: core.Classification(*classification),
		PolicyVersion:  *policy,
		MessageKind:    messageKind,
		CorrelationID:  *correlationID,
		CausationID:    *causationID,
		ExpiresAt:      expiry,
		Spawn:          core.Spawn{Requested: *requestWrk, Label: *wrkLabel},
	})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return ExitConditionInvalid
	}
	fmt.Fprintln(stdout, record.MessageID)
	return ExitOK
}

func singleFlagValue(legacy, concise, label string) (string, error) {
	if legacy != "" && concise != "" && legacy != concise {
		return "", fmt.Errorf("submit rejected: conflicting %s flags", label)
	}
	if concise != "" {
		return concise, nil
	}
	return legacy, nil
}

func runOutboxCLI(args []string) int {
	return runOutboxCLIWithWriters(args, os.Stdout, os.Stderr)
}

func runOutboxCLIWithWriters(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "list" {
		return ExitUsage
	}
	fs := flag.NewFlagSet("panewire outbox list", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dbPath := fs.String("db", "", "stage2 metadata SQLite path")
	state := fs.String("state", "", "optional SUBMITTED, PUBLISHED, COMPLETED, or EXPIRED filter")
	if err := fs.Parse(args[1:]); err != nil {
		return ExitUsage
	}
	if *dbPath == "" {
		return ExitUsage
	}
	filter := core.OutboxState(*state)
	if filter != "" && filter != core.OutboxSubmitted && filter != core.OutboxPublished && filter != core.OutboxCompleted && filter != core.OutboxExpired {
		fmt.Fprintln(stderr, "outbox rejected: unknown state filter")
		return ExitConditionInvalid
	}
	store, err := core.OpenMetadataStore(*dbPath)
	if err != nil {
		fmt.Fprintln(stderr, "outbox unavailable")
		return ExitInternal
	}
	defer store.Close()
	records, err := store.ListOutbox(context.Background(), filter)
	if err != nil {
		fmt.Fprintln(stderr, "outbox unavailable")
		return ExitInternal
	}
	fmt.Fprintln(stdout, "MESSAGE_ID\tDESTINATION\tLOGICAL_PATH\tSTATE\tATTEMPTS\tUPDATED_AT")
	for _, record := range records {
		fmt.Fprintf(stdout, "%s\t%s\t%s\t%s\t%d\t%s\n", record.MessageID, record.DestinationMachineID, record.LogicalPath, record.State, record.Attempts, record.UpdatedAt.UTC().Format(time.RFC3339))
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
	if len(args) > 0 && args[0] == "daemon" {
		return runDaemonCLI(args[1:])
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
}

func runDaemonCLI(args []string) int {
	d, code, err := newDaemonForCLI(args, daemonCLIDeps{})
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
	stage2ClientEnv := fs.String("stage2-client-env", "", "explicit mode-0600 enrolled client env")
	stage2InboxRoot := fs.String("stage2-inbox-root", "", "stage2 logical-path namespace root")
	stage2DB := fs.String("stage2-db", "", "stage2 metadata SQLite path (defaults to --db or a sibling of stage2 inbox)")
	stage2Namespace := fs.String("stage2-namespace", "inbox", "approved stage2 inbox namespace name")
	stage2Poll := fs.Duration("stage2-poll", 30*time.Second, "stage2 publish/claim poll interval")
	stage2WrkGate := fs.Bool("stage2-wrk-gate", false, "attach the wrk admission gate for spawn requests")
	stage2SpawnPolicy := fs.String("stage2-spawn-policy", "", "receiving-machine JSON policy for wrk spawn context")
	var hubURLs hubURLValues
	fs.Var(&hubURLs, "hub-url", "repeatable optional WSS hub base URL (first is preferred)")
	hubTokenEnv := fs.String("hub-token-env", "", "optional mode-0600 HUB_MACHINE_ID/HUB_TOKEN env file")
	hubCFEnv := fs.String("hub-cf-env", "", "optional mode-0600 CF_ACCESS_CLIENT_ID/CF_ACCESS_CLIENT_SECRET env file")
	hubAccepting := fs.Bool("hub-accepting", false, "advertise readiness for paper or standby jobs to the hub")
	hubJobsRoot := fs.String("hub-jobs-root", "", "local inbox root containing jobs/*/events for metadata-only heartbeats")
	failoverWakeOn := fs.String("failover-wake-on", "", "fixed failover machine ID that may receive one Wake-on-LAN packet")
	failoverWakeMAC := fs.String("failover-wake-mac", "", "fixed Wake-on-LAN MAC address for --failover-wake-on")
	burstWakeMAC := fs.String("burst-wake-mac", "", "Wake-on-LAN MAC for hub burst events (defaults to failover MAC when configured)")
	burstPoweroffAllowed := fs.Bool("burst-poweroff-allowed", false, "allow authenticated hub burst down events to run sudo -n /usr/sbin/poweroff")
	checksConfig := fs.String("checks-config", "", "explicit local hub check JSON configuration")
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
		Logging:         LoggingConfig{StorePromptBody: *storeBody},
		SchemaCommand:   deps.SchemaCommand,
		Logger:          deps.Logger,
	}
	var hubClient *HubClient
	if hubFlagsProvided(args) {
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
	stage2Requested := stage2FlagsProvided(args)
	if stage2Requested {
		if *stage2ClientEnv == "" || *stage2InboxRoot == "" {
			return nil, ExitConditionInvalid, fmt.Errorf("stage2 requires both --stage2-client-env and --stage2-inbox-root")
		}
		if *stage2Poll <= 0 {
			return nil, ExitConditionInvalid, fmt.Errorf("stage2 poll interval must be positive")
		}
		if *stage2SpawnPolicy != "" && !*stage2WrkGate {
			return nil, ExitConditionInvalid, fmt.Errorf("--stage2-spawn-policy requires --stage2-wrk-gate")
		}
		stage2, resolvedInbox, err := buildStage2DaemonConfig(*stage2ClientEnv, *stage2InboxRoot, *stage2DB, *db, *stage2Namespace, *stage2Poll, *stage2WrkGate, *stage2SpawnPolicy, deps)
		if err != nil {
			return nil, ExitConditionInvalid, err
		}
		if *inbox != "" {
			legacyInbox, err := filepath.Abs(*inbox)
			if err != nil || legacyInbox != resolvedInbox {
				_ = stage2.Close()
				return nil, ExitConditionInvalid, fmt.Errorf("--inbox-root must match --stage2-inbox-root when stage2 is enabled")
			}
		}
		cfg.InboxRoot = resolvedInbox
		cfg.Stage2 = stage2
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

func stage2FlagsProvided(args []string) bool {
	for _, name := range []string{
		"stage2-client-env", "stage2-inbox-root", "stage2-db", "stage2-namespace", "stage2-poll", "stage2-wrk-gate", "stage2-spawn-policy",
	} {
		flagName := "--" + name
		for _, arg := range args {
			if arg == flagName || strings.HasPrefix(arg, flagName+"=") {
				return true
			}
		}
	}
	return false
}

func hubFlagsProvided(args []string) bool {
	for _, name := range []string{"hub-url", "hub-token-env", "hub-cf-env", "hub-accepting", "hub-jobs-root", "failover-wake-on", "failover-wake-mac", "burst-wake-mac", "burst-poweroff-allowed", "checks-config"} {
		flagName := "--" + name
		for _, arg := range args {
			if arg == flagName || strings.HasPrefix(arg, flagName+"=") {
				return true
			}
		}
	}
	return false
}

func loadStage2ClientEnv(path string) (clientCredentialEnv, error) {
	values, err := loadMode0600Env(path)
	if err != nil {
		return clientCredentialEnv{}, fmt.Errorf("stage2 client env must be a regular mode-0600 file")
	}
	credential := clientCredentialEnv{
		URL:            values["PANEWIRE_SUPABASE_URL"],
		MachineID:      values["PANEWIRE_MACHINE_ID"],
		AccessToken:    values["PANEWIRE_SUPABASE_ACCESS_TOKEN"],
		RefreshToken:   values["PANEWIRE_SUPABASE_REFRESH_TOKEN"],
		PublishableKey: values["PANEWIRE_SUPABASE_PUBLISHABLE_KEY"],
	}
	if credential.URL == "" || credential.AccessToken == "" || credential.RefreshToken == "" || credential.PublishableKey == "" || !machineIDPattern.MatchString(credential.MachineID) {
		return clientCredentialEnv{}, fmt.Errorf("stage2 client env is missing required enrolled values")
	}
	return credential, nil
}

func buildStage2DaemonConfig(clientEnvPath, inboxRoot, explicitMetadataDB, stage1DB, namespace string, poll time.Duration, wrkGateEnabled bool, spawnPolicyPath string, deps daemonCLIDeps) (Stage2Config, string, error) {
	credential, err := loadStage2ClientEnv(clientEnvPath)
	if err != nil {
		return Stage2Config{}, "", err
	}
	if namespace == "" || strings.ContainsAny(namespace, "/\\\x00") {
		return Stage2Config{}, "", fmt.Errorf("stage2 namespace is invalid")
	}
	resolvedInbox, err := filepath.Abs(inboxRoot)
	if err != nil {
		return Stage2Config{}, "", fmt.Errorf("resolve stage2 inbox root")
	}
	metadataDB := explicitMetadataDB
	if metadataDB == "" {
		metadataDB = stage1DB
	}
	if metadataDB == "" {
		metadataDB = filepath.Join(filepath.Dir(resolvedInbox), ".panewire-stage2.sqlite3")
	}
	metadata, err := core.OpenMetadataStore(metadataDB)
	if err != nil {
		return Stage2Config{}, "", fmt.Errorf("open stage2 metadata store")
	}
	closeMetadata := func() error { return metadata.Close() }
	adapter, err := supabase.New(supabase.Config{
		BaseURL:               credential.URL,
		AccessToken:           credential.AccessToken,
		RefreshToken:          credential.RefreshToken,
		APIKey:                credential.PublishableKey,
		ClientEnvPath:         clientEnvPath,
		Logger:                deps.Logger,
		HTTPClient:            deps.HTTPClient,
		AllowInsecureForTests: deps.AllowInsecureForTests,
	})
	if err != nil {
		_ = closeMetadata()
		return Stage2Config{}, "", fmt.Errorf("configure stage2 Supabase adapter")
	}
	var gate core.Gate
	if wrkGateEnabled {
		gate = wrkgate.New(wrkgate.Config{SpawnPolicyPath: spawnPolicyPath})
	}
	stagingRoot := filepath.Join(filepath.Dir(resolvedInbox), "."+filepath.Base(resolvedInbox)+"-stage2-staging")
	receiver, err := core.NewReceiver(core.ReceiverConfig{
		MachineID:   credential.MachineID,
		Namespaces:  map[string]string{namespace: resolvedInbox},
		InboxRoot:   resolvedInbox,
		StagingRoot: stagingRoot,
		Store:       metadata,
		Transport:   adapter,
		Gate:        gate,
	})
	if err != nil {
		_ = closeMetadata()
		return Stage2Config{}, "", fmt.Errorf("configure stage2 receiver")
	}
	return Stage2Config{
		Enabled:      true,
		Publisher:    &core.Sender{Store: metadata, Transport: adapter},
		Receiver:     receiver,
		PollInterval: poll,
		Close:        closeMetadata,
	}, resolvedInbox, nil
}
func joinArgs(a []string) string { return strings.Join(a, " ") }

var _ = joinArgs

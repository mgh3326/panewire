package panewire

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

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
	if args[0] == "prompt" {
		return runPromptCLI(args[1:], cfg)
	}
	if args[0] == "submit" {
		return runSubmitCLI(args[1:])
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
	fs := flag.NewFlagSet("panewire submit", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	dbPath := fs.String("db", "", "stage2 metadata SQLite path")
	file := fs.String("file", "", "canonical source file")
	from := fs.String("from-machine", "", "stable source machine identity")
	to := fs.String("destination-machine", "", "stable destination machine identity")
	namespace := fs.String("namespace", "", "approved destination inbox namespace")
	logicalPath := fs.String("logical-path", "", "normalized namespace-relative path")
	classification := fs.String("classification", "", "public or personal_non_company")
	expiresAt := fs.String("expires-at", "", "RFC3339 UTC expiry (default 72h)")
	policy := fs.String("policy-version", "stage2-allowlist-v1", "classification policy version")
	requestWrk := fs.Bool("request-wrk", false, "request only the receiving wrk admission gate")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	if *dbPath == "" || *file == "" || *from == "" || *to == "" || *namespace == "" || *logicalPath == "" || *classification == "" {
		return ExitUsage
	}
	var expiry time.Time
	var err error
	if *expiresAt != "" {
		expiry, err = time.Parse(time.RFC3339, *expiresAt)
		if err != nil {
			return ExitConditionInvalid
		}
	}
	store, err := core.OpenMetadataStore(*dbPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return ExitInternal
	}
	defer store.Close()
	sender := &core.Sender{Store: store}
	record, err := sender.Submit(context.Background(), core.Submission{
		SourcePath:     *file,
		Source:         core.Identity{MachineID: *from},
		Destination:    core.Destination{MachineID: *to, InboxNamespace: *namespace, LogicalPath: *logicalPath},
		Classification: core.Classification(*classification),
		PolicyVersion:  *policy,
		ExpiresAt:      expiry,
		Spawn:          core.Spawn{Requested: *requestWrk},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return ExitConditionInvalid
	}
	fmt.Fprintln(os.Stdout, record.MessageID)
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
	return RunCLI(args, CLIConfig{})
}
func runDaemonCLI(args []string) int {
	fs := flag.NewFlagSet("panewire daemon", flag.ContinueOnError)
	socket := fs.String("socket", socketPathFromEnv(), "local daemon socket")
	herdr := fs.String("herdr-socket", "", "herdr socket")
	db := fs.String("db", "", "SQLite path")
	inbox := fs.String("inbox-root", "", "inbox root")
	storeBody := fs.Bool("store-prompt-body", false, "opt in to storing prompt bodies")
	if fs.Parse(args) != nil {
		return ExitUsage
	}
	d := NewDaemon(Config{SocketPath: *socket, HerdrSocket: *herdr, DBPath: *db, InboxRoot: *inbox, StorePromptBody: *storeBody, Logging: LoggingConfig{StorePromptBody: *storeBody}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := d.Start(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return ExitInternal
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	_ = d.Stop()
	return ExitOK
}
func joinArgs(a []string) string { return strings.Join(a, " ") }

var _ = joinArgs

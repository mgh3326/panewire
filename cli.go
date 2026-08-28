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
)

type CLIConfig struct{ SocketPath string }

func RunCLI(args []string, cfg CLIConfig) int {
	if len(args) == 0 {
		return ExitUsage
	}
	if args[0] == "prompt" {
		return runPromptCLI(args[1:], cfg)
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
	return ExitOK
}

func runPromptCLI(args []string, cfg CLIConfig) int {
	fs := flag.NewFlagSet("panewire prompt", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	sender := fs.String("from", "", "sender provenance")
	target := fs.String("to", "", "recipient target")
	file := fs.String("file", "", "prompt file")
	uptake := fs.String("uptake", "", "uptake mode")
	timeout := fs.Duration("timeout", 30*time.Second, "overall timeout")
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

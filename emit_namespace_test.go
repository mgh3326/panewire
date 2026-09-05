package panewire

import (
	"bufio"
	"bytes"
	"encoding/json"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestT22EmitLaneNamespaceRejectionPreservesFileAndWarns(t *testing.T) {
	daemonRoot, givenRoot := t.TempDir(), t.TempDir()
	var logs bytes.Buffer
	daemon := t22NamespaceDaemon(daemonRoot, &logs)
	socket := t22EmitSocket(t, func(request localRequest) localResponse {
		err := daemon.emitRelayEvent(request)
		return localResponse{OK: err == nil, Code: ExitCode(err), Error: errorString(err)}
	})
	args := []string{"--kind", "lane.event", "--lane", "lane-a", "--event-id", "namespace-lane", "--text", "payload", "--inbox-root", givenRoot}
	var stderr bytes.Buffer
	if code := runEmitCLI(args, &bytes.Buffer{}, &stderr, CLIConfig{SocketPath: socket}); code != ExitUsage {
		t.Fatalf("namespace rejection code=%d, want ExitUsage", code)
	}
	if got := stderr.String(); !strings.Contains(got, "emit: inbox root mismatch") || !strings.Contains(got, daemonRoot) || !strings.Contains(got, givenRoot) {
		t.Fatalf("namespace rejection stderr=%q", got)
	}
	if entries, err := os.ReadDir(filepath.Join(givenRoot, "events-lane")); err != nil || len(entries) != 1 {
		t.Fatalf("namespace rejection files=%v err=%v", entries, err)
	}
	if got := logs.String(); strings.Count(got, "level=WARN") != 1 || !strings.Contains(got, daemonRoot) || !strings.Contains(got, givenRoot) {
		t.Fatalf("namespace rejection logs=%q", got)
	}

	matched := []string{"--kind", "lane.event", "--lane", "lane-a", "--event-id", "namespace-match", "--text", "payload", "--inbox-root", daemonRoot}
	stderr.Reset()
	if code := runEmitCLI(matched, &bytes.Buffer{}, &stderr, CLIConfig{SocketPath: socket}); code != ExitOK || stderr.Len() != 0 {
		t.Fatalf("matching namespace code=%d stderr=%q", code, stderr.String())
	}
}

func TestT22EmitJobNamespaceAndUnavailableBranches(t *testing.T) {
	daemonRoot, givenRoot := t.TempDir(), t.TempDir()
	daemon := t22NamespaceDaemon(daemonRoot, &bytes.Buffer{})
	socket := t22EmitSocket(t, func(request localRequest) localResponse {
		err := daemon.emitRelayEvent(request)
		return localResponse{OK: err == nil, Code: ExitCode(err), Error: errorString(err)}
	})
	args := func(root, job string) []string {
		return []string{"--kind", "job.completed", "--job", job, "--report", "report.md", "--owner-lane", "lane-a", "--inbox-root", root}
	}
	var stderr bytes.Buffer
	if code := runEmitCLI(args(givenRoot, "namespace-job"), &bytes.Buffer{}, &stderr, CLIConfig{SocketPath: socket}); code != ExitUsage {
		t.Fatalf("job namespace rejection code=%d, want ExitUsage", code)
	}
	if got := stderr.String(); !strings.Contains(got, "inbox root mismatch") || !strings.Contains(got, daemonRoot) || !strings.Contains(got, givenRoot) {
		t.Fatalf("job namespace rejection stderr=%q", got)
	}
	if files := r20EventFiles(t, givenRoot, "namespace-job"); len(files) != 1 {
		t.Fatalf("job namespace rejection files=%v", files)
	}
	stderr.Reset()
	if code := runEmitCLI(args(daemonRoot, "namespace-match-job"), &bytes.Buffer{}, &stderr, CLIConfig{SocketPath: socket}); code != ExitOK || stderr.Len() != 0 {
		t.Fatalf("matching job namespace code=%d stderr=%q", code, stderr.String())
	}
	offlineRoot := t.TempDir()
	stderr.Reset()
	if code := runEmitCLI(args(offlineRoot, "namespace-offline-job"), &bytes.Buffer{}, &stderr, CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent.sock")}); code != ExitOK {
		t.Fatalf("offline job code=%d, want ExitOK", code)
	}
	if got := stderr.String(); got != "emit: panewired unavailable; event recorded to file only\n" {
		t.Fatalf("offline job stderr=%q", got)
	}
}

func TestT22EmitReportsOtherDaemonRejection(t *testing.T) {
	socket := t22EmitSocket(t, func(localRequest) localResponse {
		return localResponse{Code: ExitUsage, Error: "fixture rejection"}
	})
	var stderr bytes.Buffer
	if code := runEmitCLI([]string{"--kind", "lane.event", "--lane", "lane-a", "--event-id", "other-rejection", "--text", "payload", "--inbox-root", t.TempDir()}, &bytes.Buffer{}, &stderr, CLIConfig{SocketPath: socket}); code != ExitUsage {
		t.Fatalf("other daemon rejection code=%d, want ExitUsage", code)
	}
	if got := stderr.String(); got != "emit: daemon rejected the event: fixture rejection\n" {
		t.Fatalf("other daemon rejection stderr=%q", got)
	}
}

func t22NamespaceDaemon(inboxRoot string, logs *bytes.Buffer) *Daemon {
	client := &HubClient{jobsInboxRoot: inboxRoot, completedJobs: map[string]uint64{}, completedReports: map[string]struct{}{}, assignedJobs: map[string]uint64{}, events: make(chan hubClientEvent, 4)}
	return NewDaemon(Config{InboxRoot: inboxRoot, Hub: HubDaemonConfig{Enabled: true, Client: client}, Logger: slog.New(slog.NewTextHandler(logs, nil))})
}

func t22EmitSocket(t *testing.T, respond func(localRequest) localResponse) string {
	t.Helper()
	socket := filepath.Join(r20SocketRoot(t), "emit.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer connection.Close()
				scanner := bufio.NewScanner(connection)
				for scanner.Scan() {
					var request localRequest
					if json.Unmarshal(scanner.Bytes(), &request) != nil {
						writeLocal(connection, localResponse{Code: ExitUsage, Error: "invalid request"})
						continue
					}
					writeLocal(connection, respond(request))
				}
			}()
		}
	}()
	return socket
}

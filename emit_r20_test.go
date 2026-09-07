package panewire

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// r20SocketRoot keeps the unix socket path inside the macOS sun_path limit,
// matching the shortRoot pattern the existing daemon fixtures use.
func r20SocketRoot(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "pw-r20-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func r20EventFiles(t *testing.T, inbox, jobID string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(inbox, "jobs", jobID, "events"))
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".json") {
			names = append(names, entry.Name())
		}
	}
	return names
}

// TE4: no daemon is not a failure. The file is the durable half of emit.
func TestR20EmitWithoutDaemonKeepsFileAndExitsOK(t *testing.T) {
	inbox := t.TempDir()
	var stderr bytes.Buffer
	code := runEmitCLI([]string{"--kind", "job.completed", "--job", "r20-offline", "--report", "report.md", "--owner-lane", "lane-a", "--inbox-root", inbox}, &bytes.Buffer{}, &stderr, CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent.sock")})
	if code != ExitOK {
		t.Fatalf("code=%d", code)
	}
	if names := r20EventFiles(t, inbox, "r20-offline"); len(names) != 1 {
		t.Fatalf("event files=%v", names)
	}
	if got := strings.Count(stderr.String(), "emit: panewired unavailable; event recorded to file only"); got != 1 {
		t.Fatalf("stderr=%q", stderr.String())
	}
}

// TE5: a second notification for the same durable event writes no second file
// but still reaches the socket. This is wrk's file-first immediate-notify
// contract, not a conflict.
func TestR20EmitRecordedEventStillPushes(t *testing.T) {
	inbox := t.TempDir()
	socket := filepath.Join(r20SocketRoot(t), "emit.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var mu sync.Mutex
	var pushes []localRequest
	done := make(chan struct{})
	go func() {
		defer close(done)
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
					if json.Unmarshal(scanner.Bytes(), &request) == nil {
						mu.Lock()
						pushes = append(pushes, request)
						mu.Unlock()
					}
					body, _ := json.Marshal(localResponse{OK: true})
					_, _ = connection.Write(append(body, '\n'))
				}
			}()
		}
	}()
	args := []string{"--kind", "job.completed", "--job", "r20-dupe", "--report", "report.md", "--owner-lane", "lane-a", "--inbox-root", inbox}
	if code := runEmitCLI(args, &bytes.Buffer{}, &bytes.Buffer{}, CLIConfig{SocketPath: socket}); code != ExitOK {
		t.Fatalf("first emit code=%d", code)
	}
	var stderr bytes.Buffer
	if code := runEmitCLI(args, &bytes.Buffer{}, &stderr, CLIConfig{SocketPath: socket}); code != ExitOK || stderr.Len() != 0 {
		t.Fatalf("repeat code=%d stderr=%q", code, stderr.String())
	}
	if names := r20EventFiles(t, inbox, "r20-dupe"); len(names) != 1 {
		t.Fatalf("a duplicate emit wrote a second file: %v", names)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		count := len(pushes)
		mu.Unlock()
		if count == 2 || time.Now().After(deadline) {
			if count != 2 {
				t.Fatalf("socket pushes=%d, want 2", count)
			}
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	first := pushes[0]
	second := pushes[1]
	mu.Unlock()
	if first.Op != "emit" || first.JobID != "r20-dupe" || first.Kind != "job.completed" || first.ReportPath != "report.md" {
		t.Fatalf("push=%+v", first)
	}
	if second != first {
		t.Fatalf("repeat push=%+v, want original=%+v", second, first)
	}
}

// AC1: the file exists before the socket is dialed, so a daemon that reads the
// inbox during the call already sees the record.
func TestR20EmitWritesFileBeforeSocketCall(t *testing.T) {
	inbox := t.TempDir()
	socket := filepath.Join(r20SocketRoot(t), "order.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	filesAtDial := make(chan int, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		defer connection.Close()
		entries, _ := os.ReadDir(filepath.Join(inbox, "jobs", "r20-order", "events"))
		filesAtDial <- len(entries)
		scanner := bufio.NewScanner(connection)
		for scanner.Scan() {
			body, _ := json.Marshal(localResponse{OK: true})
			_, _ = connection.Write(append(body, '\n'))
		}
	}()
	if code := runEmitCLI([]string{"--kind", "job.completed", "--job", "r20-order", "--report", "report.md", "--owner-lane", "lane-a", "--inbox-root", inbox}, &bytes.Buffer{}, &bytes.Buffer{}, CLIConfig{SocketPath: socket}); code != ExitOK {
		t.Fatalf("code=%d", code)
	}
	select {
	case count := <-filesAtDial:
		if count != 1 {
			t.Fatalf("event files visible when the socket was accepted=%d, want 1", count)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the daemon side never saw the connection")
	}
}

func TestR20EmitRejectsUnknownKindAndMissingArguments(t *testing.T) {
	inbox := t.TempDir()
	cases := [][]string{
		{"--kind", "job.unknown", "--job", "r20-bad", "--report", "report.md", "--inbox-root", inbox},
		{"--kind", "job.completed", "--report", "report.md", "--inbox-root", inbox},
		{"--kind", "job.completed", "--job", "r20-bad", "--inbox-root", inbox},
	}
	for index, args := range cases {
		if code := runEmitCLI(args, &bytes.Buffer{}, &bytes.Buffer{}, CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent.sock")}); code != ExitUsage {
			t.Fatalf("case %d code=%d, want ExitUsage", index, code)
		}
	}
	if _, err := os.Stat(filepath.Join(inbox, "jobs")); err == nil {
		t.Fatal("a rejected emit wrote into the inbox")
	}
}

// AC4: the daemon gains an emit op without renaming any wait/prompt field.
func TestR20LocalRequestRetainsWaitAndPromptFieldNames(t *testing.T) {
	body, err := json.Marshal(localRequest{Op: "prompt", Path: "p", Target: "t", Sender: "s", Uptake: "u", StoreBody: true, Status: "idle", SettleMS: 1, TimeoutMS: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{`"op"`, `"path"`, `"target"`, `"sender"`, `"uptake"`, `"store_body"`, `"status"`, `"settle_ms"`, `"timeout_ms"`} {
		if !strings.Contains(string(body), name) {
			t.Fatalf("request field %s is missing from %s", name, body)
		}
	}
}

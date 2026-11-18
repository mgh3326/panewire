package panewire

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeOmpExtension is the panewire omp extension's socket: it reads one line,
// records it, and answers with reply (empty: closes without an answer).
type fakeOmpExtension struct {
	path     string
	mu       sync.Mutex
	received [][]byte
	reply    string
	// hang keeps the connection open without answering until the test ends.
	hang bool
}

func newFakeOmpExtension(t *testing.T, reply string, hang bool) *fakeOmpExtension {
	t.Helper()
	dir, err := os.MkdirTemp("", "pwomp")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "s.sock")
	if len(path) > sessionSocketPathLimit {
		t.Skipf("socket path %q is longer than a Unix socket allows", path)
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	extension := &fakeOmpExtension{path: path, reply: reply, hang: hang}
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				line, _ := bufio.NewReader(conn).ReadBytes('\n')
				extension.mu.Lock()
				extension.received = append(extension.received, line)
				extension.mu.Unlock()
				if extension.hang {
					<-done
					return
				}
				if extension.reply != "" {
					_, _ = io.WriteString(conn, extension.reply)
				}
			}()
		}
	}()
	return extension
}

func (e *fakeOmpExtension) lines() [][]byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([][]byte(nil), e.received...)
}

func registerOmpSocket(t *testing.T, root, lane, socket string) (int, string) {
	t.Helper()
	t.Setenv("HERDR_PANE_ID", "w:p4")
	var stdout, stderr bytes.Buffer
	code := runOmpSocketCLI([]string{"register", "--lane", lane, "--inbox-root", root, "--socket", socket}, &stdout, &stderr, time.Now)
	if stdout.Len() != 0 {
		t.Fatalf("register printed %q", stdout.String())
	}
	return code, stderr.String()
}

func TestOmpWakePostsToExtensionAndWaitsForItsAck(t *testing.T) {
	logs := captureSlog(t)
	extension := newFakeOmpExtension(t, `{"ok":true}`+"\n", false)
	node := newSessionWakeNode(t, sessionMessageLanes{omp: []string{"routing-policy"}})
	if code, stderr := registerOmpSocket(t, node.root, "routing-policy", extension.path); code != ExitOK {
		t.Fatalf("register exited %d: %s", code, stderr)
	}
	message := wakeInject("routing-policy", 3, "order")
	node.client.relayBusyManager().offer(t.Context(), message)

	lines := extension.lines()
	if len(lines) != 1 {
		t.Fatalf("extension got %d lines, want 1", len(lines))
	}
	var got ompLine
	if err := json.Unmarshal(bytes.TrimSpace(lines[0]), &got); err != nil || got.Type != "panewire.message" || got.Text != "panewire-hub: "+message.Text {
		t.Fatalf("extension got %q (%v), want a panewire.message with the prefixed wake line", lines[0], err)
	}
	if len(*node.typed) != 0 {
		t.Fatalf("the wake line was also typed: %q", *node.typed)
	}
	if info, err := os.Stat(filepath.Join(node.root, ompSocketRegistryDir, "routing-policy.json")); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("registry stat %v err=%v: want mode 0600", info, err)
	}
	logged := logs.String()
	if !strings.Contains(logged, "stage="+sessionStageAcked) || !strings.Contains(logged, "harness=omp") || strings.Contains(logged, extension.path) || strings.Contains(logged, message.Text) {
		t.Fatalf("log must report the acked stage and leak neither path nor text: %s", logged)
	}
}

func TestOmpWakeFallsBackToTyping(t *testing.T) {
	for name, tc := range map[string]struct {
		reply  string
		hang   bool
		gone   bool
		other  bool
		reason string
	}{
		"extension closes without answering": {reason: "no_ack"},
		"extension answers not ok":           {reply: `{"ok":false}` + "\n", reason: "no_ack"},
		"extension answers garbage":          {reply: "hello\n", reason: "no_ack"},
		"extension never answers":            {hang: true, reason: "timeout"},
		"socket is gone":                     {gone: true, reason: "socket_gone"},
		"registered from another pane":       {reply: `{"ok":true}` + "\n", other: true, reason: "other_pane"},
	} {
		t.Run(name, func(t *testing.T) {
			logs := captureSlog(t)
			extension := newFakeOmpExtension(t, tc.reply, tc.hang)
			node := newSessionWakeNode(t, sessionMessageLanes{omp: []string{"routing-policy"}})
			if code, stderr := registerOmpSocket(t, node.root, "routing-policy", extension.path); code != ExitOK {
				t.Fatalf("register exited %d: %s", code, stderr)
			}
			if tc.gone {
				if err := os.Remove(extension.path); err != nil {
					t.Fatal(err)
				}
			}
			message := wakeInject("routing-policy", 2, "decision")
			if tc.other {
				message.Pane = "w:p9"
			}
			ctx := t.Context()
			if tc.hang {
				var cancel context.CancelFunc
				// The parent's deadline stands in for ompSessionPostTimeout.
				ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()
			}
			node.client.relayBusyManager().offer(ctx, message)

			if len(*node.typed) != 1 || !strings.HasSuffix((*node.typed)[0], message.Text) {
				t.Fatalf("typed = %q, want the wake line typed exactly once", *node.typed)
			}
			logged := logs.String()
			if !strings.Contains(logged, "stage="+sessionStageFellBack) || !strings.Contains(logged, "reason="+tc.reason) || strings.Contains(logged, extension.path) || strings.Contains(logged, message.Text) {
				t.Fatalf("log = %s, want fell_back_to_typed with reason=%s and no path or text", logged, tc.reason)
			}
		})
	}
}

func TestOmpWakeWithoutRegistrationOrOutsideTheTrialIsTyped(t *testing.T) {
	extension := newFakeOmpExtension(t, `{"ok":true}`+"\n", false)
	node := newSessionWakeNode(t, sessionMessageLanes{omp: []string{"routing-policy"}})
	// A claude-style registration under the other directory is not an omp one.
	if err := writeClaudeSocketRegistration(node.root, "routing-policy", "w:p4", extension.path, time.Now()); err != nil {
		t.Fatal(err)
	}
	node.client.relayBusyManager().offer(t.Context(), wakeInject("routing-policy", 1, "order"))
	// A lane the omp trial does not name keeps the typed line, registered or not.
	if code, stderr := registerOmpSocket(t, node.root, "other-lane", extension.path); code != ExitOK {
		t.Fatalf("register exited %d: %s", code, stderr)
	}
	node.client.relayBusyManager().offer(t.Context(), wakeInject("other-lane", 1, "order"))
	if len(extension.lines()) != 0 {
		t.Fatalf("extension got %q, want nothing", extension.lines())
	}
	if len(*node.typed) != 2 {
		t.Fatalf("typed = %q, want both lines typed", *node.typed)
	}
}

func TestOmpRegisterRefusals(t *testing.T) {
	extension := newFakeOmpExtension(t, "", false)
	regular := filepath.Join(t.TempDir(), "plain")
	if err := os.WriteFile(regular, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for name, args := range map[string][]string{
		"no socket":          {"register", "--lane", "routing-policy"},
		"relative socket":    {"register", "--lane", "routing-policy", "--socket", "s.sock"},
		"not a socket":       {"register", "--lane", "routing-policy", "--socket", regular},
		"missing socket":     {"register", "--lane", "routing-policy", "--socket", extension.path + "x"},
		"bad lane":           {"register", "--lane", "../x", "--socket", extension.path},
		"unknown flag":       {"register", "--lane", "routing-policy", "--socket", extension.path, "--auth", "t"},
		"unknown subcommand": {"unregister"},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			t.Setenv("HERDR_PANE_ID", "w:p4")
			t.Setenv("PANEWIRE_LANE", "")
			var stdout, stderr bytes.Buffer
			if code := runOmpSocketCLI(append(append([]string(nil), args...), "--inbox-root", root), &stdout, &stderr, time.Now); code == ExitOK {
				t.Fatalf("exit %d, want a refusal", code)
			}
			if _, err := os.Stat(filepath.Join(root, ompSocketRegistryDir)); !os.IsNotExist(err) {
				t.Fatalf("a refused registration left a registry (err %v)", err)
			}
		})
	}
}

// TestOmpExtensionEndToEnd runs the shipped extension under bun with a fake pi:
// the extension registers through the real `panewire omp-socket register`, the
// node posts, and pi.sendMessage gets the line with the documented options.
func TestOmpExtensionEndToEnd(t *testing.T) {
	bun, err := exec.LookPath("bun")
	if err != nil {
		t.Skip("bun is not installed; the extension is TypeScript run by omp's bun runtime")
	}
	work := t.TempDir()
	binary := filepath.Join(work, "bin", "panewire")
	if out, err := exec.Command("go", "build", "-o", binary, "./cmd/panewire").CombinedOutput(); err != nil {
		t.Fatalf("go build failed: %v\n%s", err, out)
	}
	extension, err := filepath.Abs(filepath.Join("docs", "examples", "omp-session-message.ts"))
	if err != nil {
		t.Fatal(err)
	}
	driver := filepath.Join(work, "driver.mjs")
	const script = `
import { spawnSync } from "node:child_process";
const ext = (await import(process.argv[2])).default;
const handlers = {}, sent = [];
const pi = {
  on: (name, handler) => { handlers[name] = handler; },
  sendMessage: (message, options) => { sent.push({ message, options }); },
  exec: async (command, args) => { const r = spawnSync(command, args, { encoding: "utf8" }); return { code: r.status ?? 1, stdout: r.stdout, stderr: r.stderr }; },
};
ext(pi);
if (!handlers.session_start) { console.log("NOEXT"); process.exit(0); }
await handlers.session_start({}, {});
console.log("READY");
for await (const _ of process.stdin) {}
await handlers.session_shutdown({}, {});
console.log("SENT " + JSON.stringify(sent));
`
	if err := os.WriteFile(driver, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	sockets, err := os.MkdirTemp("", "pwosd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockets) })
	inbox := filepath.Join(work, "inbox")
	// An explicit environment: a test run inside omp inherits OMPCODE=1, which
	// the extension reads as a nested session.
	env := []string{"PATH=" + filepath.Dir(binary) + string(os.PathListSeparator) + os.Getenv("PATH"), "HOME=" + work,
		"PANEWIRE_LANE=routing-policy", "HERDR_PANE_ID=w:p4", "PANEWIRE_INBOX_ROOT=" + inbox, "PANEWIRE_OMP_SOCKET_DIR=" + sockets}
	cmd := exec.Command(bun, driver, extension)
	cmd.Env = env
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	reader := bufio.NewReader(stdout)
	ready := make(chan string, 1)
	go func() { line, _ := reader.ReadString('\n'); ready <- strings.TrimSpace(line) }()
	select {
	case line := <-ready:
		if line != "READY" {
			t.Fatalf("driver said %q, stderr %s", line, stderr.String())
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("driver never became ready; stderr %s", stderr.String())
	}

	node := newSessionWakeNode(t, sessionMessageLanes{omp: []string{"routing-policy"}})
	node.client.jobsInboxRoot = inbox
	message := wakeInject("routing-policy", 4, "decision")
	node.client.relayBusyManager().offer(t.Context(), message)
	if len(*node.typed) != 0 {
		t.Fatalf("the extension did not take the line; it was typed: %q", *node.typed)
	}
	// A line the extension cannot use is answered not-ok.
	registration, err := readSessionRegistration(inbox, ompSocketRegistryDir, "routing-policy", "w:p4")
	if err != nil {
		t.Fatal(err)
	}
	if err := postOmpSessionMessage(t.Context(), registration.Socket, ""); !strings.Contains(sessionFailureReason(err), "no_ack") {
		t.Fatalf("an empty text was acknowledged: %v", err)
	}

	_ = stdin.Close()
	rest, _ := io.ReadAll(reader)
	var sent []struct {
		Message struct {
			CustomType string `json:"customType"`
			Content    string `json:"content"`
			Display    bool   `json:"display"`
		} `json:"message"`
		Options struct {
			TriggerTurn bool   `json:"triggerTurn"`
			DeliverAs   string `json:"deliverAs"`
		} `json:"options"`
	}
	payload, found := strings.CutPrefix(strings.TrimSpace(string(rest)), "SENT ")
	if !found || json.Unmarshal([]byte(payload), &sent) != nil {
		t.Fatalf("driver output %q, stderr %s", rest, stderr.String())
	}
	if len(sent) != 1 || sent[0].Message.Content != "panewire-hub: "+message.Text || sent[0].Message.CustomType != "panewire-hub" || !sent[0].Message.Display || !sent[0].Options.TriggerTurn || sent[0].Options.DeliverAs != "aside" {
		t.Fatalf("pi.sendMessage calls = %+v, want exactly one for the wake line, triggerTurn with deliverAs aside", sent)
	}
	if entries, _ := os.ReadDir(sockets); len(entries) != 0 {
		t.Fatalf("the extension left %d entries in its socket directory after session_shutdown", len(entries))
	}
}

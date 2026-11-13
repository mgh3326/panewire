package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const claudeTestTokenCanary = "canary-messaging-token-4258"

// fakeClaudeSession is a Claude Code inbox socket: it reads each connection to
// EOF, records the bytes, and closes, as a live session does.
type fakeClaudeSession struct {
	path     string
	listener net.Listener
	mu       sync.Mutex
	received [][]byte
}

// newFakeClaudeSession binds under a short path: a Unix socket path holds about
// 104 bytes, and a long t.TempDir (macOS) overflows it.
func newFakeClaudeSession(t *testing.T) *fakeClaudeSession {
	t.Helper()
	dir, err := os.MkdirTemp("", "pwcs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "s.sock")
	if len(path) > claudeSocketPathLimit {
		t.Skipf("socket path %q is longer than a Unix socket allows", path)
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	session := &fakeClaudeSession{path: path, listener: listener}
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			data, _ := io.ReadAll(conn)
			conn.Close()
			session.mu.Lock()
			session.received = append(session.received, data)
			session.mu.Unlock()
		}
	}()
	return session
}

func (s *fakeClaudeSession) messages() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]byte(nil), s.received...)
}

type claudeWakeNode struct {
	client *HubClient
	root   string
	typed  *[]string
}

// newClaudeWakeNode is a node whose typed path is a recording seam, with the
// trial switched on for lanes.
func newClaudeWakeNode(t *testing.T, lanes ...string) claudeWakeNode {
	t.Helper()
	typed := new([]string)
	var mu sync.Mutex
	client := &HubClient{jobsInboxRoot: t.TempDir(), relayInject: func(_ context.Context, pane, text string) bool {
		mu.Lock()
		defer mu.Unlock()
		*typed = append(*typed, pane+"\x00"+text)
		return true
	}}
	client.relayControl.sessionMessageLanes = lanes
	client.setRelayEmitter(func(hubClientEvent) {})
	return claudeWakeNode{client: client, root: client.jobsInboxRoot, typed: typed}
}

func registerClaudeSocket(t *testing.T, root, lane, socket string) int {
	t.Helper()
	t.Setenv(claudeSocketEnv, socket)
	t.Setenv("HERDR_PANE_ID", "w:p4")
	t.Setenv("CLAUDE_CODE_MESSAGING_TOKEN", claudeTestTokenCanary)
	var stdout, stderr bytes.Buffer
	code := runClaudeSocketCLI([]string{"register", "--lane", lane, "--inbox-root", root}, &stdout, &stderr, time.Now)
	if stdout.Len() != 0 {
		t.Fatalf("register printed %q: a SessionStart hook's stdout becomes session context", stdout.String())
	}
	if strings.Contains(stderr.String(), claudeTestTokenCanary) {
		t.Fatal("register echoed the messaging token")
	}
	return code
}

func wakeInject(lane string, unread int, urgent string) hubOutboundMessage {
	return hubOutboundMessage{Type: "relay.inject", JobID: laneEventTransportID(lane, "inbox-wake"), Pane: "w:p4", Text: inboxWakeText(lane, unread, urgent)}
}

// captureSlog routes slog to a buffer for the test.
func captureSlog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buf
}

func TestClaudeWakePostsToRegisteredSocket(t *testing.T) {
	logs := captureSlog(t)
	session := newFakeClaudeSession(t)
	node := newClaudeWakeNode(t, "routing-policy")
	if code := registerClaudeSocket(t, node.root, "routing-policy", session.path); code != ExitOK {
		t.Fatalf("register exited %d", code)
	}
	message := wakeInject("routing-policy", 3, "decision")
	node.client.relayBusyManager().offer(t.Context(), message)

	eventually(t, 2*time.Second, "the session socket to receive the wake line", func() bool { return len(session.messages()) == 1 })
	if len(*node.typed) != 0 {
		t.Fatalf("the wake line was also typed: %q", *node.typed)
	}
	raw := session.messages()[0]
	var line struct {
		Type    string `json:"type"`
		Message struct {
			Role, Content string
		} `json:"message"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(raw), &line); err != nil {
		t.Fatalf("socket got %q: %v", raw, err)
	}
	wantMessage := "panewire-hub: " + message.Text
	if line.Type != "user" || line.Message.Role != "user" || line.Message.Content != wantMessage {
		t.Fatalf("socket got %+v, want the wake line %q as a user message", line, wantMessage)
	}
	if bytes.Count(raw, []byte("\n")) != 1 || strings.Contains(string(raw), `"auth"`) || strings.Contains(string(raw), claudeTestTokenCanary) {
		t.Fatalf("the post must be one line with no auth frame and no token: %q", raw)
	}
	registry, err := os.ReadFile(claudeSocketRegistryPath(node.root, "routing-policy"))
	if err != nil || strings.Contains(string(registry), claudeTestTokenCanary) {
		t.Fatalf("registry %q err=%v", registry, err)
	}
	if info, _ := os.Stat(claudeSocketRegistryPath(node.root, "routing-policy")); info.Mode().Perm() != 0o600 {
		t.Fatalf("registry mode %v", info.Mode().Perm())
	}
	logged := logs.String()
	if !strings.Contains(logged, "stage="+claudeSessionStageReachedSocket) || strings.Contains(logged, session.path) || strings.Contains(logged, message.Text) || strings.Contains(logged, claudeTestTokenCanary) {
		t.Fatalf("log must report the honest stage and leak neither path, text nor token: %s", logged)
	}
}

func TestClaudeWakeFallsBackToTyping(t *testing.T) {
	for name, setup := range map[string]func(t *testing.T, node claudeWakeNode){
		"no registration": func(*testing.T, claudeWakeNode) {},
		"socket removed since registration": func(t *testing.T, node claudeWakeNode) {
			session := newFakeClaudeSession(t)
			registerClaudeSocket(t, node.root, "routing-policy", session.path)
			session.listener.Close()
			os.Remove(session.path)
		},
		"session gone but socket file left": func(t *testing.T, node claudeWakeNode) {
			session := newFakeClaudeSession(t)
			registerClaudeSocket(t, node.root, "routing-policy", session.path)
			session.listener.Close()
			// net.Listener.Close unlinks the file; put a dead socket back.
			dead, err := net.ListenUnix("unix", &net.UnixAddr{Name: session.path, Net: "unix"})
			if err != nil {
				t.Fatal(err)
			}
			dead.SetUnlinkOnClose(false)
			dead.Close()
		},
		"registration names a regular file": func(t *testing.T, node claudeWakeNode) {
			regular := filepath.Join(t.TempDir(), "x")
			if err := os.WriteFile(regular, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(node.root, claudeSocketRegistryDir)
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			body, _ := json.Marshal(claudeSocketRegistration{Lane: "routing-policy", Pane: "w:p4", Socket: regular})
			if err := os.WriteFile(claudeSocketRegistryPath(node.root, "routing-policy"), body, 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"registration is another lane's": func(t *testing.T, node claudeWakeNode) {
			session := newFakeClaudeSession(t)
			registerClaudeSocket(t, node.root, "other-lane", session.path)
			if err := os.Rename(claudeSocketRegistryPath(node.root, "other-lane"), claudeSocketRegistryPath(node.root, "routing-policy")); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			logs := captureSlog(t)
			node := newClaudeWakeNode(t, "routing-policy")
			setup(t, node)
			message := wakeInject("routing-policy", 2, "blocked")
			node.client.relayBusyManager().offer(t.Context(), message)
			// The typed path prefixes its own delivery nonce to the line.
			if len(*node.typed) != 1 || !strings.HasPrefix((*node.typed)[0], "w:p4\x00") || !strings.HasSuffix((*node.typed)[0], message.Text) {
				t.Fatalf("typed %q, want the wake line typed once into its pane", *node.typed)
			}
			if !strings.Contains(logs.String(), "stage="+claudeSessionStageFellBack) {
				t.Fatalf("the fallback was not logged: %s", logs)
			}
		})
	}
}

// The switch off is today's behaviour: a live registered socket is not used.
func TestClaudeWakeSwitchOffTypesTheLine(t *testing.T) {
	session := newFakeClaudeSession(t)
	node := newClaudeWakeNode(t)
	registerClaudeSocket(t, node.root, "routing-policy", session.path)
	message := wakeInject("routing-policy", 1, "info")
	node.client.relayBusyManager().offer(t.Context(), message)
	if len(*node.typed) != 1 || len(session.messages()) != 0 {
		t.Fatalf("typed=%q socket=%d", *node.typed, len(session.messages()))
	}
}

// Only the trial lane's own wake line is posted: every other inject keeps its
// typed path even when that lane has a live registered socket.
func TestClaudeWakeLeavesEveryOtherInjectTyped(t *testing.T) {
	session := newFakeClaudeSession(t)
	node := newClaudeWakeNode(t, "routing-policy")
	registerClaudeSocket(t, node.root, "routing-policy", session.path)
	wake := wakeInject("routing-policy", 1, "decision")
	cases := map[string]hubOutboundMessage{
		"another lane's wake line":               wakeInject("other-lane", 1, "decision"),
		"desk fallback line under the lane's id": {Type: "relay.inject", JobID: wake.JobID, Pane: "w:desk", Text: inboxWakeDeskText("routing-policy", 1, "decision")},
		"lane event with a durable id":           {Type: "relay.inject", JobID: wake.JobID, Pane: "w:p4", Lane: "routing-policy", EventID: 9, Kind: "lane.event", Text: wake.Text, DeliverPolicy: "now"},
		"lane's id with other text":              {Type: "relay.inject", JobID: wake.JobID, Pane: "w:p4", Text: "[inbox] routing-policy: 1 unread (most urgent: decision) | rm -rf /"},
		"another job's note":                     {Type: "relay.inject", JobID: "job-a", Pane: "w:p4", Text: wake.Text},
	}
	for name, message := range cases {
		node.client.relayBusyManager().offer(t.Context(), message)
		if got := len(*node.typed); got == 0 {
			t.Fatalf("%s: not typed", name)
		}
		*node.typed = nil
	}
	if got := len(session.messages()); got != 0 {
		t.Fatalf("%d non-wake injects reached the session socket", got)
	}
}

func TestClaudeSessionMessageSwitchParsing(t *testing.T) {
	t.Setenv(claudeSessionMessageLanesEnv, "")
	if lanes := claudeSessionMessageLanesFromEnv(); len(lanes) != 0 {
		t.Fatalf("an empty switch must be off: %v", lanes)
	}
	t.Setenv(claudeSessionMessageLanesEnv, " routing-policy , not a lane!, ,x")
	if lanes := claudeSessionMessageLanesFromEnv(); strings.Join(lanes, "|") != "routing-policy|x" {
		t.Fatalf("lanes %v", lanes)
	}
	t.Setenv(claudeSessionMessageLanesEnv, "routing-policy")
	if lanes := newClientRelayControlState(HubClientConfig{}).sessionMessageLanes; len(lanes) != 1 || lanes[0] != "routing-policy" {
		t.Fatalf("the node client did not read the switch: %v", lanes)
	}
}

func TestClaudeSocketRegisterRefusals(t *testing.T) {
	root := t.TempDir()
	t.Setenv("PANEWIRE_LANE", "")
	t.Setenv(claudeSocketEnv, "")
	var out bytes.Buffer
	if code := runClaudeSocketCLI([]string{"register", "--lane", "routing-policy", "--inbox-root", root}, &out, &out, time.Now); code != ExitUsage {
		t.Fatalf("a session without messaging exited %d", code)
	}
	regular := filepath.Join(t.TempDir(), "plain")
	if err := os.WriteFile(regular, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for name, socket := range map[string]string{"regular file": regular, "relative": "cc.sock", "missing": filepath.Join(t.TempDir(), "gone.sock")} {
		if code := registerClaudeSocket(t, root, "routing-policy", socket); code == ExitOK {
			t.Fatalf("%s socket was registered", name)
		}
	}
	if code := registerClaudeSocket(t, root, "../escape", newFakeClaudeSession(t).path); code != ExitUsage {
		t.Fatalf("a lane that is not a label exited %d", code)
	}
	if entries, _ := os.ReadDir(root); len(entries) != 0 {
		t.Fatalf("a refused registration left %v", entries)
	}
	// The lane defaults to PANEWIRE_LANE, as for the other pane-side commands.
	t.Setenv("PANEWIRE_LANE", "routing-policy")
	t.Setenv(claudeSocketEnv, newFakeClaudeSession(t).path)
	if code := runClaudeSocketCLI([]string{"register", "--inbox-root", root}, &out, &out, time.Now); code != ExitOK {
		t.Fatalf("PANEWIRE_LANE default exited %d: %s", code, out.String())
	}
	if _, err := readClaudeSocketRegistration(root, "routing-policy", "w:p4"); err != nil {
		t.Fatal(err)
	}
}

// The lane moved to another pane while its old session lives on: the live
// socket must not take the line meant for the routed pane (review of #447).
func TestClaudeWakeIgnoresRegistrationFromAnotherPane(t *testing.T) {
	logs := captureSlog(t)
	session := newFakeClaudeSession(t)
	node := newClaudeWakeNode(t, "routing-policy")
	t.Setenv(claudeSocketEnv, session.path)
	t.Setenv("HERDR_PANE_ID", "w:old")
	var out bytes.Buffer
	if code := runClaudeSocketCLI([]string{"register", "--lane", "routing-policy", "--inbox-root", node.root}, &out, &out, time.Now); code != ExitOK {
		t.Fatalf("register exited %d", code)
	}
	message := wakeInject("routing-policy", 1, "decision") // routed to w:p4
	node.client.relayBusyManager().offer(t.Context(), message)
	if len(*node.typed) != 1 || !strings.HasPrefix((*node.typed)[0], "w:p4\x00") {
		t.Fatalf("typed %q, want the line typed into the routed pane", *node.typed)
	}
	if got := len(session.messages()); got != 0 {
		t.Fatalf("the old pane's session received %d messages", got)
	}
	if !strings.Contains(logs.String(), "reason=other_pane") {
		t.Fatalf("log: %s", logs)
	}
	// Re-registering from the routed pane (the new session's hook) takes over.
	t.Setenv("HERDR_PANE_ID", "w:p4")
	if code := runClaudeSocketCLI([]string{"register", "--lane", "routing-policy", "--inbox-root", node.root}, &out, &out, time.Now); code != ExitOK {
		t.Fatalf("register exited %d", code)
	}
	*node.typed = nil
	node.client.relayBusyManager().offer(t.Context(), message)
	eventually(t, 2*time.Second, "the routed pane's session to receive the line", func() bool { return len(session.messages()) == 1 })
	if len(*node.typed) != 0 {
		t.Fatalf("typed %q", *node.typed)
	}
}

func TestClaudeSocketRegisterNeedsAPane(t *testing.T) {
	root := t.TempDir()
	t.Setenv("HERDR_PANE_ID", "")
	t.Setenv(claudeSocketEnv, newFakeClaudeSession(t).path)
	var out bytes.Buffer
	if code := runClaudeSocketCLI([]string{"register", "--lane", "routing-policy", "--inbox-root", root}, &out, &out, time.Now); code != ExitUsage {
		t.Fatalf("a registration with no pane exited %d", code)
	}
	if entries, _ := os.ReadDir(root); len(entries) != 0 {
		t.Fatalf("left %v", entries)
	}
	if code := runClaudeSocketCLI([]string{"register", "--lane", "routing-policy", "--pane", "w:p4", "--inbox-root", root}, &out, &out, time.Now); code != ExitOK {
		t.Fatalf("--pane exited %d", code)
	}
}

// A daemon started with no --inbox-root and no --hub-jobs-root has an empty
// client root; it must read the registration where the hook wrote it.
func TestClaudeWakeReadsDefaultRegistryRoot(t *testing.T) {
	session := newFakeClaudeSession(t)
	node := newClaudeWakeNode(t, "routing-policy")
	root := t.TempDir()
	t.Setenv("PANEWIRE_INBOX_ROOT", root)
	node.client.jobsInboxRoot = ""
	registerClaudeSocketAt(t, "routing-policy", session.path)
	node.client.relayBusyManager().offer(t.Context(), wakeInject("routing-policy", 1, "decision"))
	eventually(t, 2*time.Second, "the default-root registration to be used", func() bool { return len(session.messages()) == 1 })
	if len(*node.typed) != 0 {
		t.Fatalf("typed %q", *node.typed)
	}
}

// registerClaudeSocketAt registers through the hook's own default-root path
// (no --inbox-root), as a SessionStart hook does.
func registerClaudeSocketAt(t *testing.T, lane, socket string) {
	t.Helper()
	t.Setenv(claudeSocketEnv, socket)
	t.Setenv("HERDR_PANE_ID", "w:p4")
	var out bytes.Buffer
	if code := runClaudeSocketCLI([]string{"register", "--lane", lane}, &out, &out, time.Now); code != ExitOK {
		t.Fatalf("register exited %d: %s", code, out.String())
	}
}

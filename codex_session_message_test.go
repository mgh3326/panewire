package panewire

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	codexTestThread      = "01a1ee6e-efbe-7ae0-bc9e-03a6ec39ed41"
	codexTestParentID    = "019fffff-0000-7000-8000-000000000001"
	codexTestHookSession = `{"session_id":"` + codexTestThread + `","hook_event_name":"SessionStart","source":"startup","transcript_path":null}`
)

// fakeCodexQueue stands in for `codex queue`: it records each call and answers
// with err.
type fakeCodexQueue struct {
	mu    sync.Mutex
	calls []codexQueueCall
	err   error
	// block makes the call wait for its context, as a hung `codex queue` does.
	block bool
}

type codexQueueCall struct{ home, thread, text string }

func (f *fakeCodexQueue) run(ctx context.Context, home, thread, text string) error {
	f.mu.Lock()
	f.calls = append(f.calls, codexQueueCall{home, thread, text})
	f.mu.Unlock()
	if f.block {
		<-ctx.Done()
		return ctx.Err()
	}
	return f.err
}

func (f *fakeCodexQueue) seen() []codexQueueCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]codexQueueCall(nil), f.calls...)
}

func newCodexWakeNode(t *testing.T, lanes sessionMessageLanes) (claudeWakeNode, *fakeCodexQueue) {
	t.Helper()
	node := newSessionWakeNode(t, lanes)
	queue := &fakeCodexQueue{}
	node.client.codexQueue = queue.run
	return node, queue
}

// registerCodexThread runs the SessionStart hook's command: the hook input on
// stdin, the pane in the environment.
func registerCodexThread(t *testing.T, root, lane, stdin string, extra ...string) (int, string) {
	t.Helper()
	t.Setenv("HERDR_PANE_ID", "w:p4")
	t.Setenv("CODEX_THREAD_ID", codexTestParentID)
	var stdout, stderr bytes.Buffer
	args := append([]string{"register", "--lane", lane, "--inbox-root", root}, extra...)
	var in io.Reader // nil: a terminal, no hook input
	if stdin != "" {
		in = strings.NewReader(stdin)
	}
	code := runCodexThreadCLI(args, in, &stdout, &stderr, time.Now)
	if stdout.Len() != 0 {
		t.Fatalf("register printed %q: a SessionStart hook's stdout becomes session context", stdout.String())
	}
	return code, stderr.String()
}

func TestCodexWakeQueuesOnRegisteredThread(t *testing.T) {
	logs := captureSlog(t)
	node, queue := newCodexWakeNode(t, sessionMessageLanes{codex: []string{"routing-policy"}})
	t.Setenv("CODEX_HOME", "")
	if code, stderr := registerCodexThread(t, node.root, "routing-policy", codexTestHookSession); code != ExitOK {
		t.Fatalf("register exited %d: %s", code, stderr)
	}
	message := wakeInject("routing-policy", 3, "order")
	node.client.relayBusyManager().offer(t.Context(), message)

	calls := queue.seen()
	if len(calls) != 1 || calls[0].thread != codexTestThread || calls[0].text != "panewire-hub: "+message.Text || calls[0].home != "" {
		t.Fatalf("codex queue calls = %+v, want one on thread %s with the prefixed wake line", calls, codexTestThread)
	}
	if len(*node.typed) != 0 {
		t.Fatalf("the wake line was also typed: %q", *node.typed)
	}
	if info, err := os.Stat(filepath.Join(node.root, codexThreadRegistryDir, "routing-policy.json")); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("registry stat %v err=%v: want mode 0600", info, err)
	}
	logged := logs.String()
	if !strings.Contains(logged, "stage="+sessionStageQueued) || !strings.Contains(logged, "harness=codex") || strings.Contains(logged, codexTestThread) || strings.Contains(logged, message.Text) {
		t.Fatalf("log must report the queued stage and leak neither thread id nor text: %s", logged)
	}
}

func TestCodexRegistrationUsesTheHookSessionNotTheInheritedThread(t *testing.T) {
	node, queue := newCodexWakeNode(t, sessionMessageLanes{codex: []string{"routing-policy"}})
	// registerCodexThread exports the parent's thread as CODEX_THREAD_ID, as a
	// nested session inherits it; the hook input names this session's own.
	if code, stderr := registerCodexThread(t, node.root, "routing-policy", codexTestHookSession); code != ExitOK {
		t.Fatalf("register exited %d: %s", code, stderr)
	}
	node.client.relayBusyManager().offer(t.Context(), wakeInject("routing-policy", 1, "order"))
	if calls := queue.seen(); len(calls) != 1 || calls[0].thread != codexTestThread {
		t.Fatalf("queued on %+v, want the hook's session %s, never the inherited %s", calls, codexTestThread, codexTestParentID)
	}
}

func TestCodexRegistrationRecordsCodexHome(t *testing.T) {
	node, queue := newCodexWakeNode(t, sessionMessageLanes{codex: []string{"routing-policy"}})
	home := t.TempDir()
	t.Setenv("CODEX_HOME", home)
	if code, stderr := registerCodexThread(t, node.root, "routing-policy", codexTestHookSession); code != ExitOK {
		t.Fatalf("register exited %d: %s", code, stderr)
	}
	node.client.relayBusyManager().offer(t.Context(), wakeInject("routing-policy", 1, "order"))
	if calls := queue.seen(); len(calls) != 1 || calls[0].home != home {
		t.Fatalf("queue calls %+v, want CODEX_HOME %s carried to the queue", calls, home)
	}
}

func TestCodexRegisterRefusals(t *testing.T) {
	for name, tc := range map[string]struct {
		stdin string
		extra []string
	}{
		"no hook input and no --thread": {},
		"hook input without a session":  {stdin: `{"hook_event_name":"SessionStart"}`},
		"hook input is not json":        {stdin: `not json`},
		"thread is not a uuid":          {extra: []string{"--thread", "--help"}},
		"thread is a session name":      {extra: []string{"--thread", "routing-policy"}},
		"hook session is not a uuid":    {stdin: `{"session_id":"../../etc"}`},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			code, _ := registerCodexThread(t, root, "routing-policy", tc.stdin, tc.extra...)
			if code != ExitUsage {
				t.Fatalf("exit %d, want %d", code, ExitUsage)
			}
			if _, err := os.Stat(filepath.Join(root, codexThreadRegistryDir)); !os.IsNotExist(err) {
				t.Fatalf("a refused registration left %v (err %v)", filepath.Join(root, codexThreadRegistryDir), err)
			}
		})
	}
	t.Run("no pane", func(t *testing.T) {
		t.Setenv("HERDR_PANE_ID", "")
		var stdout, stderr bytes.Buffer
		if code := runCodexThreadCLI([]string{"register", "--lane", "routing-policy", "--inbox-root", t.TempDir(), "--thread", codexTestThread}, nil, &stdout, &stderr, time.Now); code != ExitUsage {
			t.Fatalf("exit %d, want %d", code, ExitUsage)
		}
	})
}

func TestCodexWakeFallsBackToTyping(t *testing.T) {
	for name, tc := range map[string]struct {
		setup  func(t *testing.T, node claudeWakeNode, queue *fakeCodexQueue)
		reason string
		timed  bool
		called bool
	}{
		"no registration": {setup: func(*testing.T, claudeWakeNode, *fakeCodexQueue) {}, reason: "not_registered"},
		"registered from another pane": {setup: func(t *testing.T, node claudeWakeNode, _ *fakeCodexQueue) {
			t.Setenv("HERDR_PANE_ID", "w:p9")
			var stdout, stderr bytes.Buffer
			args := []string{"register", "--lane", "routing-policy", "--inbox-root", node.root, "--thread", codexTestThread}
			if code := runCodexThreadCLI(args, nil, &stdout, &stderr, time.Now); code != ExitOK {
				t.Fatalf("register exited %d", code)
			}
		}, reason: "other_pane"},
		"registration names a flag": {setup: func(t *testing.T, node claudeWakeNode, _ *fakeCodexQueue) {
			writeCodexRegistration(t, node.root, `{"lane":"routing-policy","pane":"w:p4","thread":"--resume"}`)
		}, reason: "registration_invalid"},
		"registration is another lane's": {setup: func(t *testing.T, node claudeWakeNode, _ *fakeCodexQueue) {
			writeCodexRegistration(t, node.root, `{"lane":"other","pane":"w:p4","thread":"`+codexTestThread+`"}`)
		}, reason: "registration_invalid"},
		"registration has a relative CODEX_HOME": {setup: func(t *testing.T, node claudeWakeNode, _ *fakeCodexQueue) {
			writeCodexRegistration(t, node.root, `{"lane":"routing-policy","pane":"w:p4","thread":"`+codexTestThread+`","codex_home":"rel/home"}`)
		}, reason: "registration_invalid"},
		"codex queue refuses (no such active session)": {setup: func(t *testing.T, node claudeWakeNode, queue *fakeCodexQueue) {
			registerFixedCodexThread(t, node.root)
			queue.err = errors.New("exit status 1")
		}, reason: "queue_failed", called: true},
		"codex queue hangs": {setup: func(t *testing.T, node claudeWakeNode, queue *fakeCodexQueue) {
			registerFixedCodexThread(t, node.root)
			queue.block = true
		}, reason: "timeout", timed: true, called: true},
	} {
		t.Run(name, func(t *testing.T) {
			logs := captureSlog(t)
			node, queue := newCodexWakeNode(t, sessionMessageLanes{codex: []string{"routing-policy"}})
			tc.setup(t, node, queue)
			ctx := t.Context()
			if tc.timed {
				var cancel context.CancelFunc
				// The parent's deadline stands in for codexQueueTimeout.
				ctx, cancel = context.WithTimeout(ctx, 50*time.Millisecond)
				defer cancel()
			}
			message := wakeInject("routing-policy", 2, "decision")
			node.client.relayBusyManager().offer(ctx, message)

			if got := len(queue.seen()) == 1; got != tc.called {
				t.Fatalf("codex queue called = %v, want %v (calls %+v)", got, tc.called, queue.seen())
			}
			if len(*node.typed) != 1 || !strings.HasPrefix((*node.typed)[0], "w:p4\x00") || !strings.HasSuffix((*node.typed)[0], message.Text) {
				t.Fatalf("typed = %q, want the wake line typed exactly once", *node.typed)
			}
			logged := logs.String()
			if !strings.Contains(logged, "stage="+sessionStageFellBack) || !strings.Contains(logged, "reason="+tc.reason) || strings.Contains(logged, codexTestThread) || strings.Contains(logged, message.Text) {
				t.Fatalf("log = %s, want fell_back_to_typed with reason=%s and no thread id or text", logged, tc.reason)
			}
		})
	}
}

func writeCodexRegistration(t *testing.T, root, body string) {
	t.Helper()
	dir := filepath.Join(root, codexThreadRegistryDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "routing-policy.json"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func registerFixedCodexThread(t *testing.T, root string) {
	t.Helper()
	t.Setenv("CODEX_HOME", "")
	if code, stderr := registerCodexThread(t, root, "routing-policy", codexTestHookSession); code != ExitOK {
		t.Fatalf("register exited %d: %s", code, stderr)
	}
}

func TestCodexWakeOnlyForConfiguredLanesAndExactLine(t *testing.T) {
	node, queue := newCodexWakeNode(t, sessionMessageLanes{codex: []string{"routing-policy"}})
	registerFixedCodexThread(t, node.root)

	// A lane the codex trial does not name keeps the typed line.
	node.client.relayBusyManager().offer(t.Context(), wakeInject("other-lane", 1, "order"))
	// The desk's "no pane" line shares the lane's transport id but not its text.
	paneless := wakeInject("routing-policy", 1, "order")
	paneless.Text = "[inbox] routing-policy: has no pane; read it with panewire inbox list --lane routing-policy"
	node.client.relayBusyManager().offer(t.Context(), paneless)
	if calls := queue.seen(); len(calls) != 0 {
		t.Fatalf("queued %+v for lines that are not a configured lane's wake line", calls)
	}
	if len(*node.typed) != 2 {
		t.Fatalf("typed = %q, want both lines typed as before", *node.typed)
	}
}

func TestSessionMessageLaneServedByFirstHarnessOnly(t *testing.T) {
	// One lane named for claude and codex: the claude receiver serves it (and
	// here falls back, with no registration); codex never sees the line, so it
	// is never delivered twice.
	node, queue := newCodexWakeNode(t, sessionMessageLanes{claude: []string{"routing-policy"}, codex: []string{"routing-policy"}})
	registerFixedCodexThread(t, node.root)
	node.client.relayBusyManager().offer(t.Context(), wakeInject("routing-policy", 1, "order"))
	if calls := queue.seen(); len(calls) != 0 {
		t.Fatalf("codex queued %+v for a lane the claude receiver owns", calls)
	}
	if len(*node.typed) != 1 {
		t.Fatalf("typed = %q, want the claude receiver's fallback typing once", *node.typed)
	}
}

func TestSessionMessageLanesFromEnvPerHarness(t *testing.T) {
	t.Setenv(claudeSessionMessageLanesEnv, "a")
	t.Setenv(codexSessionMessageLanesEnv, " b , not a lane ,c")
	t.Setenv(ompSessionMessageLanesEnv, "d")
	lanes := newClientRelayControlState(HubClientConfig{}).sessionMessageLanes
	if strings.Join(lanes.claude, "|") != "a" || strings.Join(lanes.codex, "|") != "b|c" || strings.Join(lanes.omp, "|") != "d" {
		t.Fatalf("lanes = %+v", lanes)
	}
}

// writeFakeCodex installs a `codex` that records its argv and CODEX_HOME to
// out, then exits with status.
func writeFakeCodex(t *testing.T, out string, status int) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("the fake codex is a shell script")
	}
	bin := t.TempDir()
	script := "#!/bin/sh\n{ for a in \"$@\"; do printf '%s\\n' \"$a\"; done; printf 'HOME=%s\\n' \"$CODEX_HOME\"; } > '" + out + "'\nexit " + string(rune('0'+status)) + "\n"
	if err := os.WriteFile(filepath.Join(bin, "codex"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func TestRunCodexQueueArgv(t *testing.T) {
	out := filepath.Join(t.TempDir(), "argv")
	writeFakeCodex(t, out, 0)
	// A line that starts like a flag and carries shell and TOML metacharacters
	// must reach codex as one --message= argument.
	text := "--thread x; $(touch pwned) | [inbox] a: 1 unread"
	if err := runCodexQueue(t.Context(), "/codex/home", codexTestThread, text); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Join([]string{"queue", "--thread", codexTestThread, "--message=" + text, "HOME=/codex/home"}, "\n") + "\n"
	if string(got) != want {
		t.Fatalf("codex got\n%q\nwant\n%q", got, want)
	}
}

func TestRunCodexQueueReportsExitStatus(t *testing.T) {
	writeFakeCodex(t, filepath.Join(t.TempDir(), "argv"), 1)
	err := runCodexQueue(t.Context(), "", codexTestThread, "x")
	if err == nil || !strings.Contains(err.Error(), "exit status 1") {
		t.Fatalf("err = %v, want the exit status", err)
	}
	t.Setenv("PATH", t.TempDir())
	if err := runCodexQueue(t.Context(), "", codexTestThread, "x"); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("err = %v, want codex not found", err)
	}
}

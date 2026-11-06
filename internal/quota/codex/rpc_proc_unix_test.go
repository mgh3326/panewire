//go:build unix

package codex

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
)

// These tests run a fake codex, a shell script, through the real process
// transport: its argv and environment are recorded, and every process it
// started must be gone when Measure returns.

const fakeAppServer = `#!/bin/sh
echo "$$" > "$FAKE_DIR/pid"
printf '%s\n' "$@" > "$FAKE_DIR/argv"
pwd > "$FAKE_DIR/cwd"
env > "$FAKE_DIR/env"
if [ -n "$FAKE_HANG" ]; then
  sleep 300 &
  echo "$!" > "$FAKE_DIR/child"
  wait
fi
while read line; do
  case "$line" in
    *'"initialize"'*) echo '{"id":1,"result":{}}' ;;
    *'"account/rateLimits/read"'*) echo '{"id":2,"result":{"rateLimits":{"planType":"plus","primary":{"usedPercent":33,"windowDurationMins":300,"resetsAt":1789980000}}}}' ;;
    *'"account/read"'*) echo '{"id":3,"result":{"account":null}}' ;;
  esac
done
`

type fakeCodex struct {
	dir, binary, home string
}

// newFakeCodex writes the script and a codex home holding the test login.
func newFakeCodex(t *testing.T) fakeCodex {
	t.Helper()
	root := t.TempDir()
	f := fakeCodex{dir: filepath.Join(root, "record"), binary: filepath.Join(root, "codex"), home: filepath.Join(root, "home")}
	for _, dir := range []string{f.dir, f.home} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(f.binary, []byte(fakeAppServer), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.home, "auth.json"), []byte(`{"tokens":{"access_token":"`+token+`","account_id":"fixture-acct"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FAKE_DIR", f.dir)
	t.Setenv("FAKE_HANG", "")
	return f
}

func (f fakeCodex) config(api Doer) Config {
	return Config{CodexHome: f.home, Binary: f.binary, HTTP: api, Now: func() time.Time { return testNow }, LookupEnv: func(string) (string, bool) { return "", false }}
}

func (f fakeCodex) pid(t *testing.T, name string) int {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(f.dir, name))
	if err != nil {
		t.Fatalf("the fake app-server did not start: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }

// gone waits for pid to disappear. A killed grandchild is a zombie until init
// reaps it, which takes a moment.
func gone(t *testing.T, pid int) bool {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for alive(pid) {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond) // pacing a poll of the OS process table
	}
	return true
}

// killOnCleanup stops pid if the test failed before the code under test did.
func killOnCleanup(t *testing.T, f fakeCodex, name string) {
	t.Cleanup(func() {
		if data, err := os.ReadFile(filepath.Join(f.dir, name)); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil && alive(pid) {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	})
}

func neverCalled(t *testing.T) doFunc {
	return func(*http.Request) (*http.Response, error) {
		t.Error("usage API called although the app-server answered")
		return nil, errors.New("unexpected")
	}
}

// The real transport: argv is the closed app-server call, the codex home is
// the working directory and CODEX_HOME, no token is on argv or in the
// environment, and the process is gone when Measure returns.
func TestAppServerProcess(t *testing.T) {
	f := newFakeCodex(t)
	killOnCleanup(t, f, "pid")
	observed, err := Measure(t.Context(), f.config(neverCalled(t)))
	if err != nil || observed.Source != "usage-rpc" || *observed.Buckets[0].UsedPct != 33 || observed.AccountFP != fixtureFP {
		t.Fatalf("%+v %v", observed, err)
	}
	if alive(f.pid(t, "pid")) {
		t.Fatal("the app-server outlived Measure")
	}
	argv, _ := os.ReadFile(filepath.Join(f.dir, "argv"))
	if string(argv) != "-s\nread-only\n-a\nnever\napp-server\n" {
		t.Fatalf("argv %q", argv)
	}
	cwd, _ := os.ReadFile(filepath.Join(f.dir, "cwd"))
	if got, _ := filepath.EvalSymlinks(strings.TrimSpace(string(cwd))); got != mustEval(t, f.home) {
		t.Fatalf("cwd %q, want the codex home %q", cwd, f.home)
	}
	env, _ := os.ReadFile(filepath.Join(f.dir, "env"))
	if strings.Count(string(env), "CODEX_HOME=") != 1 || !strings.Contains(string(env), "CODEX_HOME="+f.home+"\n") || strings.Contains(string(argv)+string(env), token) {
		t.Fatalf("environment %q", env)
	}
}

func mustEval(t *testing.T, path string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

// A hung app-server, and the child it started, are killed at the timeout and
// the usage API answers.
func TestAppServerHangIsKilledAndReaped(t *testing.T) {
	f := newFakeCodex(t)
	t.Setenv("FAKE_HANG", "1")
	killOnCleanup(t, f, "pid")
	killOnCleanup(t, f, "child")
	cfg := f.config(reply(200, fixture(t, "wham_usage.json"), nil))
	cfg.RPCTimeout = 300 * time.Millisecond // simulated latency: the server never replies
	observed, err := Measure(t.Context(), cfg)
	if err != nil || observed.Source != "usage-api" {
		t.Fatalf("%+v %v", observed, err)
	}
	pid, child := f.pid(t, "pid"), f.pid(t, "child")
	if alive(pid) {
		t.Fatal("the hung app-server was not reaped")
	}
	if !gone(t, child) {
		t.Fatal("the app-server's child outlived it")
	}
}

// Cancelling the caller's context also ends the app-server.
func TestAppServerCancelled(t *testing.T) {
	f := newFakeCodex(t)
	t.Setenv("FAKE_HANG", "1")
	killOnCleanup(t, f, "pid")
	killOnCleanup(t, f, "child")
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := Measure(ctx, f.config(doFunc(func(r *http.Request) (*http.Response, error) { return nil, r.Context().Err() })))
		done <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(f.dir, "child")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the fake app-server never started")
		}
		time.Sleep(5 * time.Millisecond) // pacing a wait for the fake to start
	}
	cancel()
	err := <-done
	if quota.KindOf(err) != quota.ErrKindNetwork {
		t.Fatalf("err = %v", err)
	}
	if alive(f.pid(t, "pid")) || !gone(t, f.pid(t, "child")) {
		t.Fatal("the app-server survived the cancellation")
	}
}

// A codex that cannot start is an RPC failure, not a leak.
func TestAppServerMissingBinary(t *testing.T) {
	f := newFakeCodex(t)
	cfg := f.config(reply(200, fixture(t, "wham_usage.json"), nil))
	cfg.Binary = filepath.Join(f.dir, "not-there")
	observed, err := Measure(t.Context(), cfg)
	if err != nil || observed.Source != "usage-api" {
		t.Fatalf("%+v %v", observed, err)
	}
}

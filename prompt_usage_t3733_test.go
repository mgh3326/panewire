package panewire_test

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	panewire "github.com/mgh3326/panewire"
)

const cliUsageChildEnv = "PANEWIRE_CLI_USAGE_CHILD"

// TestCLIUsageChildProcess is the child half of runCLIChild: it runs the CLI
// with the arguments after "--" and prints the exit code on stdout, so the
// parent reads the real stderr without swapping its own os.Stderr.
func TestCLIUsageChildProcess(t *testing.T) {
	if os.Getenv(cliUsageChildEnv) != "1" {
		return
	}
	args := os.Args
	for i, arg := range args {
		if arg == "--" {
			args = args[i+1:]
			break
		}
	}
	fmt.Fprintf(os.Stdout, "child-exit=%d\n", panewire.RunCLI(args, panewire.CLIConfig{}))
}

var childExitPattern = regexp.MustCompile(`child-exit=(\d+)`)

// runCLIChild runs `panewire ARGS` in a child test process whose daemon socket
// is socket, and returns the exit code and everything written to stderr.
func runCLIChild(t *testing.T, socket string, args ...string) (int, string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], append([]string{"-test.run=^TestCLIUsageChildProcess$", "--"}, args...)...)
	cmd.Env = append(os.Environ(), cliUsageChildEnv+"=1", "PANEWIRE_SOCKET="+socket)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("child: %v\nstdout: %s\nstderr: %s", err, stdout.String(), stderr.String())
	}
	m := childExitPattern.FindStringSubmatch(stdout.String())
	if m == nil {
		t.Fatalf("child printed no exit code\nstdout: %s\nstderr: %s", stdout.String(), stderr.String())
	}
	code, _ := strconv.Atoi(m[1])
	return code, stderr.String()
}

// absentSocket is a daemon socket path that nothing listens on, so a CLI that
// gets past its usage checks fails with exit 4 instead of reaching a daemon.
func absentSocket(t *testing.T) string {
	return filepath.Join(t.TempDir(), "absent.sock")
}

// refusingDaemon answers every request with response, like a daemon that
// refuses without an error text.
func refusingDaemon(t *testing.T, response string) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "pw3733-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "d.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				if bufio.NewScanner(c).Scan() {
					_, _ = fmt.Fprintf(c, "%s\n", response)
				}
			}(conn)
		}
	}()
	return socket
}

// oneStderrLine requires stderr to be exactly one line with prefix and every
// one of wants in it.
func oneStderrLine(t *testing.T, stderr, prefix string, wants ...string) {
	t.Helper()
	line := strings.TrimSuffix(stderr, "\n")
	if line == "" || strings.Contains(line, "\n") || !strings.HasPrefix(line, prefix) {
		t.Fatalf("stderr is not one %q line: %q", prefix, stderr)
	}
	for _, want := range wants {
		if !strings.Contains(line, want) {
			t.Fatalf("stderr %q does not name %q", line, want)
		}
	}
}

func TestPromptUsageErrorsNameTheFlagT3733(t *testing.T) {
	file := filepath.Join(t.TempDir(), "prompt.md")
	if err := os.WriteFile(file, []byte("expect: name=orch\n\nbody\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		args  []string
		wants []string
	}{
		// The m1b design session's call (hk 3733): no --from, so the replies
		// were never sent and the exit said nothing.
		{"missing from", []string{"--file", file, "--directive", "--uptake", "status-transition", "--to", "wF:p22Q"}, []string{"missing --from"}},
		{"missing to", []string{"--from", "sender", "--file", file}, []string{"missing --to"}},
		{"missing file", []string{"--from", "sender", "--to", "orch"}, []string{"missing --file"}},
		{"all missing", []string{"--directive"}, []string{"missing --from", "--to", "--file"}},
		{"timeout", []string{"--from", "sender", "--to", "orch", "--file", file, "--timeout", "0s"}, []string{"--timeout must be > 0"}},
		{"uptake", []string{"--from", "sender", "--to", "orch", "--file", file, "--uptake", "landed"}, []string{"--uptake must be tool or status-transition"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, stderr := runCLIChild(t, absentSocket(t), append([]string{"prompt"}, tc.args...)...)
			if code != panewire.ExitConditionInvalid {
				t.Fatalf("exit=%d want %d; stderr=%q", code, panewire.ExitConditionInvalid, stderr)
			}
			oneStderrLine(t, stderr, "panewire prompt: ", tc.wants...)
		})
	}
}

func TestPromptRefusalWithoutErrorTextPrintsCodeT3733(t *testing.T) {
	socket := refusingDaemon(t, `{"ok":false,"code":6}`)
	file := filepath.Join(t.TempDir(), "prompt.md")
	if err := os.WriteFile(file, []byte("expect: name=orch\n\nbody\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	code, stderr := runCLIChild(t, socket, "prompt", "--from", "sender", "--to", "orch", "--file", file)
	if code != 6 {
		t.Fatalf("exit=%d want 6; stderr=%q", code, stderr)
	}
	oneStderrLine(t, stderr, "panewire prompt: ", "refused (code 6)")
}

// The wait CLI shares the prompt CLI's silent exit-5 and silent-refusal
// returns (hk 3733).
func TestWaitConditionErrorsAreNotSilentT3733(t *testing.T) {
	t.Run("invalid status", func(t *testing.T) {
		code, stderr := runCLIChild(t, absentSocket(t), "wait", "--agent", "orch", "--status", "landed", "--timeout", "1s")
		if code != panewire.ExitConditionInvalid {
			t.Fatalf("exit=%d want %d; stderr=%q", code, panewire.ExitConditionInvalid, stderr)
		}
		oneStderrLine(t, stderr, "panewire wait: ", "--status", "landed")
	})
	t.Run("refusal without error text", func(t *testing.T) {
		socket := refusingDaemon(t, `{"ok":false,"code":5}`)
		code, stderr := runCLIChild(t, socket, "wait", "--agent", "orch", "--status", "idle", "--timeout", "1s")
		if code != panewire.ExitConditionInvalid {
			t.Fatalf("exit=%d want %d; stderr=%q", code, panewire.ExitConditionInvalid, stderr)
		}
		oneStderrLine(t, stderr, "panewire wait: ", "refused (code 5)")
	})
}

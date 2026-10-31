package panewire

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// The per-OS rule, checked on synthetic homes so no real host path is read.
func TestDefaultInboxRootForEachOS(t *testing.T) {
	cases := []struct {
		goos, home, want string
	}{
		{"darwin", "/Users/fixture", filepath.Join("/Users/fixture", "Library", "Application Support", "panewire", "inbox")},
		{"linux", "/home/fixture", filepath.Join("/home/fixture", "work", "herdr-inbox")},
	}
	for _, tc := range cases {
		if got := defaultInboxRootFor(tc.goos, tc.home); got != tc.want {
			t.Errorf("defaultInboxRootFor(%q, %q) = %q, want %q", tc.goos, tc.home, got, tc.want)
		}
	}
	if got := defaultInboxRootFor("linux", "/home/fixture"); strings.Contains(got, "Library") {
		t.Errorf("linux default inbox root %q still carries the macOS data directory", got)
	}
}

// emit's default and the daemon's own fallback are one rule: with no flag and
// no PANEWIRE_INBOX_ROOT both resolve the per-OS default for this host's GOOS
// (CI runs this on both macOS and Linux).
func TestEmitDefaultInboxRootEqualsDaemonDefault(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("PANEWIRE_INBOX_ROOT", "")
	want := defaultInboxRootFor(runtime.GOOS, home)
	if got := emitInboxRoot(""); got != want {
		t.Fatalf("emit default inbox root = %q, want %q", got, want)
	}
	if got := NewDaemon(Config{}).stallInboxRoot(); got != want {
		t.Fatalf("daemon default inbox root = %q, want %q (emit resolves %q)", got, want, emitInboxRoot(""))
	}
}

func TestEmitInboxRootExplicitOverridesDefault(t *testing.T) {
	home, envRoot, flagRoot := t.TempDir(), t.TempDir(), t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("PANEWIRE_INBOX_ROOT", envRoot)
	if got := emitInboxRoot(""); got != envRoot {
		t.Fatalf("PANEWIRE_INBOX_ROOT: emit inbox root = %q, want %q", got, envRoot)
	}
	if got := emitInboxRoot(flagRoot); got != flagRoot {
		t.Fatalf("--inbox-root over PANEWIRE_INBOX_ROOT: emit inbox root = %q, want %q", got, flagRoot)
	}

	// End to end: the event file lands in the explicit root, not the default.
	absent := CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent.sock")}
	lane := func(eventID string, extra ...string) []string {
		return append([]string{"--kind", "lane.event", "--lane", "lane-a", "--event-id", eventID, "--text", "payload"}, extra...)
	}
	if code := runEmitCLI(lane("override-env"), &bytes.Buffer{}, &bytes.Buffer{}, absent); code != ExitOK {
		t.Fatalf("env override emit code=%d", code)
	}
	if code := runEmitCLI(lane("override-flag", "--inbox-root", flagRoot), &bytes.Buffer{}, &bytes.Buffer{}, absent); code != ExitOK {
		t.Fatalf("flag override emit code=%d", code)
	}
	for _, root := range []string{envRoot, flagRoot} {
		if entries, err := os.ReadDir(filepath.Join(root, "events-lane")); err != nil || len(entries) != 1 {
			t.Fatalf("explicit root %s: events=%v err=%v", root, entries, err)
		}
	}
	if _, err := os.Stat(defaultInboxRootFor(runtime.GOOS, home)); !os.IsNotExist(err) {
		t.Fatalf("an explicit root still wrote under the default inbox root: err=%v", err)
	}
}

// The incident shape: an emit relying on the default root reaches a daemon
// that relays from another root. Both paths must be in the error so the cause
// is visible in the caller's log.
func TestEmitDefaultRootMismatchNamesBothPaths(t *testing.T) {
	home, daemonRoot := t.TempDir(), t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("PANEWIRE_INBOX_ROOT", "")
	var logs bytes.Buffer
	daemon := t22NamespaceDaemon(daemonRoot, &logs)
	socket := t22EmitSocket(t, func(request localRequest) localResponse {
		err := daemon.emitRelayEvent(request)
		return localResponse{OK: err == nil, Code: ExitCode(err), Error: errorString(err)}
	})
	args := []string{"--kind", "lane.event", "--lane", "lane-a", "--event-id", "default-mismatch", "--text", "payload"}
	var stderr bytes.Buffer
	if code := runEmitCLI(args, &bytes.Buffer{}, &stderr, CLIConfig{SocketPath: socket}); code != ExitUsage {
		t.Fatalf("mismatch code=%d, want ExitUsage; stderr=%q", code, stderr.String())
	}
	given := defaultInboxRootFor(runtime.GOOS, home)
	want := "emit: inbox root mismatch (daemon=" + daemonRoot + ", given=" + given + ")\n"
	if got := stderr.String(); got != want {
		t.Fatalf("mismatch stderr=%q, want %q", got, want)
	}
	if got := logs.String(); !strings.Contains(got, daemonRoot) || !strings.Contains(got, given) {
		t.Fatalf("mismatch log does not name both roots: %q", got)
	}
}

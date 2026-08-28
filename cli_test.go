package panewire_test

import (
	"testing"

	panewire "github.com/mgh3326/panewire"
)

func TestNoDaemonFallbackForBothWaitModes(t *testing.T) {
	for _, args := range [][]string{
		{"wait", "--file", "/tmp/does-not-exist", "--settle", "0s", "--timeout", "20ms"},
		{"wait", "--agent", "build", "--status", "idle", "--settle", "0s", "--timeout", "20ms"},
	} {
		if code := panewire.RunCLI(args, panewire.CLIConfig{SocketPath: t.TempDir() + "/missing.sock"}); code != panewire.ExitDaemonUnavailable {
			t.Fatalf("args=%v code=%d, want daemon unavailable", args, code)
		}
	}
}

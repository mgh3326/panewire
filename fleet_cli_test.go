package panewire

import (
	"path/filepath"
	"testing"
)

func TestFleetCommandRouting(t *testing.T) {
	t.Setenv("FLEET_HOSTS_CONFIG", filepath.Join(t.TempDir(), "absent.toml"))
	t.Setenv("PANEWIRE_POLICY", "")
	for _, command := range []string{"spawn", "work", "reap", "done", "escalate", "delegate", "preflight"} {
		if code := RunCLI([]string{command, "--help"}, CLIConfig{}); code != ExitOK {
			t.Errorf("%s --help exit=%d", command, code)
		}
	}
	for _, args := range [][]string{{"lead", "42"}, {"lead", "--help"}} {
		if code := RunCLI(args, CLIConfig{}); code != ExitUsage {
			t.Errorf("retired %v exit=%d", args, code)
		}
	}
}

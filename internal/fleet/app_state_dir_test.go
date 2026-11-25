package fleet

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestNewWithRelativeXDGStateHomeUsesAbsoluteHomeForSpawnState(t *testing.T) {
	home := t.TempDir()
	config := filepath.Join(t.TempDir(), "hosts.toml")
	if err := os.WriteFile(config, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("XDG_STATE_HOME", "relative-state")
	t.Setenv("FLEET_HOSTS_CONFIG", config)
	t.Setenv("PANEWIRE_POLICY", "")

	a, err := New(io.Discard, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	stateDir := filepath.Join(home, ".local", "state", "panewire")
	a.workspaceLockRoot = filepath.Join(t.TempDir(), "host-locks")
	a.HerdrSession = "worker"
	release, err := a.lockWorkspace(context.Background(), "workers")
	if err != nil {
		t.Fatalf("spawn workspace lock with relative XDG_STATE_HOME: %v", err)
	}
	release()

	a.emitFleetEvent("test", "job-a", "placed", "", "")
	if _, err := os.Stat(filepath.Join(stateDir, "events", "fleet-events.jsonl")); err != nil {
		t.Fatalf("fleet event missing from absolute Home state directory: %v", err)
	}
}

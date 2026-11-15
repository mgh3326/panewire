package teststate

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestPinGoToolchainEnvFreezesTheResolvedValues proves the pin survives the
// move it guards against: after HOME and XDG_CACHE_HOME change, go still
// reports the paths resolved before the move. Pinning after the move is what
// put a build cache in every test state dir (hk 4733).
func TestPinGoToolchainEnvFreezesTheResolvedValues(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go is not on PATH: nothing to pin")
	}
	keys := []string{"GOCACHE", "GOMODCACHE", "GOPATH", "GOENV"}
	for _, key := range keys {
		t.Setenv(key, "")
	}
	PinGoToolchainEnv()
	pinned := make(map[string]string, len(keys))
	for _, key := range keys {
		pinned[key] = os.Getenv(key)
		if pinned[key] == "" {
			t.Fatalf("%s was not pinned", key)
		}
	}
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	t.Setenv("HOME", t.TempDir())
	for _, key := range keys {
		out, err := exec.Command("go", "env", key).Output()
		if err != nil {
			t.Fatalf("go env %s: %v", key, err)
		}
		if got := strings.TrimSpace(string(out)); got != pinned[key] {
			t.Fatalf("go env %s = %s after HOME moved, want the pinned %s", key, got, pinned[key])
		}
	}
}

// TestPinGoToolchainEnvKeepsAnExplicitValue: a developer who exported GOCACHE
// keeps precisely that cache.
func TestPinGoToolchainEnvKeepsAnExplicitValue(t *testing.T) {
	explicit := filepath.Join(t.TempDir(), "explicit-cache")
	t.Setenv("GOCACHE", explicit)
	PinGoToolchainEnv()
	if got := os.Getenv("GOCACHE"); got != explicit {
		t.Fatalf("GOCACHE = %q, want the explicit %q", got, explicit)
	}
}

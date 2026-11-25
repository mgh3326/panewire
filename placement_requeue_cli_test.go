package panewire

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func placementRequeueCLIFixture(t *testing.T, home string) {
	t.Helper()
	config := filepath.Join(t.TempDir(), "hosts.toml")
	if err := os.WriteFile(config, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("XDG_STATE_HOME", "")
	t.Setenv("FLEET_HOSTS_CONFIG", config)
	t.Setenv("PANEWIRE_POLICY", "")
}

func TestPlacementRequeueCLIResolvesRelativeStateDir(t *testing.T) {
	cwd := t.TempDir()
	t.Chdir(cwd)
	placementRequeueCLIFixture(t, filepath.Join(cwd, "home"))

	var stdout, stderr bytes.Buffer
	code := runPlacementRequeueCLI([]string{"--state-dir", "./state"}, &stdout, &stderr)
	wantDir := filepath.Join(cwd, "state", "placements-rejected")
	if code != ExitOK || !strings.Contains(stdout.String(), wantDir) || stderr.Len() != 0 {
		t.Fatalf("placement-requeue code=%d stdout=%q stderr=%q, want successful replay rooted at %q", code, stdout.String(), stderr.String(), wantDir)
	}
}

func TestPlacementRequeueCLIPrintsErrorWithoutResultDir(t *testing.T) {
	placementRequeueCLIFixture(t, "relative-home")

	var stdout, stderr bytes.Buffer
	code := runPlacementRequeueCLI(nil, &stdout, &stderr)
	want := "placement quarantine requires an absolute StateDir or Home"
	if code != ExitInternal || !strings.Contains(stderr.String(), want) || strings.Contains(stderr.String(), " is unreadable") {
		t.Fatalf("placement-requeue code=%d stdout=%q stderr=%q, want actual error %q", code, stdout.String(), stderr.String(), want)
	}
}

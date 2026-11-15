package panewire

import (
	"os"
	"strings"
	"testing"
)

// TestReleaseWorkflowPublishesComponents pins the release wiring of
// docs/release-components.md: the build fetches tags, names the components
// after the binaries and before verification, and the verified SHA256SUMS
// lists the four binaries plus components.json.
func TestReleaseWorkflowPublishesComponents(t *testing.T) {
	raw, err := os.ReadFile(".github/workflows/release.yml")
	if err != nil {
		t.Fatal(err)
	}
	wf := string(raw)
	for _, want := range []string{
		"fetch-depth: 0",
		"go run ./cmd/release-components",
		"-unknown < /dev/null > dist/release/components.json",
		"test -s dist/release/components.json",
		"test \"$(wc -l < dist/release/SHA256SUMS)\" -eq 5",
		"jq -r '.summary' dist/release/components.json",
		"--exclude-pre-releases",
		"git diff --no-renames --name-only",
	} {
		if !strings.Contains(wf, want) {
			t.Errorf("release.yml lacks %q", want)
		}
	}
	build := strings.Index(wf, "name: Build assets")
	name := strings.Index(wf, "name: Name the components this release changed")
	verify := strings.Index(wf, "name: Verify assets, SHA256SUMS and version output")
	if !(build >= 0 && build < name && name < verify) {
		t.Errorf("components must be named after the build and before verification: build=%d name=%d verify=%d", build, name, verify)
	}
}

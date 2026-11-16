package panewire

import (
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestDeployMacOSShardsCoverEveryRootTest pins that the macOS job of deploy.yml
// runs the root package in scripts/ci-test-shard.sh shards, and that those
// shards run every top-level test exactly once. One `go test ./...` of the root
// package hit the 10m default timeout on this runner (hk 4865), so the test
// also refuses a step that runs the root package whole again.
func TestDeployMacOSShardsCoverEveryRootTest(t *testing.T) {
	raw, err := os.ReadFile(".github/workflows/deploy.yml")
	if err != nil {
		t.Fatal(err)
	}
	var w struct {
		Jobs map[string]struct {
			Steps []struct {
				Run string `yaml:"run"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &w); err != nil {
		t.Fatal(err)
	}
	macos, ok := w.Jobs["macos"]
	if !ok {
		t.Fatal("deploy.yml has no macos job")
	}
	var specs []string
	packagesStep := false
	for _, step := range macos.Steps {
		if strings.Contains(step.Run, "go test ./...") {
			t.Errorf("macos step runs the root package whole again: %q", step.Run)
		}
		if strings.Contains(step.Run, "grep -v -x") && strings.Contains(step.Run, "xargs go test") {
			packagesStep = true
		}
		for _, line := range strings.Split(step.Run, "\n") {
			if spec, found := strings.CutPrefix(strings.TrimSpace(line), "scripts/ci-test-shard.sh "); found {
				specs = append(specs, spec)
			}
		}
	}
	if !packagesStep {
		t.Error("macos job has no step for the packages other than the root")
	}
	if len(specs) == 0 {
		t.Fatal("macos job runs no scripts/ci-test-shard.sh shard")
	}
	shardSpecsCoverRootTests(t, specs)
}

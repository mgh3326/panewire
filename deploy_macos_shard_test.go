package panewire

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// checkDeployMacOSShards verifies that deploy.yml configures macOS testing in
// parallel: a macos job for packages other than the root (plus race pins and
// platform checks) and a macos-shard matrix job running the root package
// across scripts/ci-test-shard.sh shards covering every top-level root test
// exactly once without running the root package whole again (hk 6197).
func checkDeployMacOSShards(raw []byte) error {
	var w struct {
		Jobs map[string]struct {
			RunsOn   string `yaml:"runs-on"`
			Strategy struct {
				Matrix map[string][]string `yaml:"matrix"`
			} `yaml:"strategy"`
			Steps []struct {
				Run string `yaml:"run"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &w); err != nil {
		return err
	}
	macos, ok := w.Jobs["macos"]
	if !ok {
		return errors.New("deploy.yml has no macos job")
	}
	if macos.RunsOn != "macos-latest" {
		return fmt.Errorf("macos job runs on %q, want macos-latest", macos.RunsOn)
	}
	packagesStep := false
	for _, step := range macos.Steps {
		if strings.Contains(step.Run, "go test ./...") {
			return fmt.Errorf("macos step runs the root package whole again: %q", step.Run)
		}
		if strings.Contains(step.Run, "grep -v -x") && strings.Contains(step.Run, "xargs go test") {
			packagesStep = true
		}
	}
	if !packagesStep {
		return errors.New("macos job has no step for the packages other than the root")
	}

	shardJob, ok := w.Jobs["macos-shard"]
	if !ok {
		return errors.New("deploy.yml has no macos-shard job")
	}
	if shardJob.RunsOn != "macos-latest" {
		return fmt.Errorf("macos-shard job runs on %q, want macos-latest", shardJob.RunsOn)
	}
	for _, step := range shardJob.Steps {
		if strings.Contains(step.Run, "go test ./...") {
			return fmt.Errorf("macos-shard step runs the root package whole again: %q", step.Run)
		}
	}
	specs := shardJob.Strategy.Matrix["shard"]
	if len(specs) == 0 {
		return errors.New("macos-shard job runs no scripts/ci-test-shard.sh shard")
	}
	hasShardRun := false
	for _, step := range shardJob.Steps {
		if strings.Contains(step.Run, "scripts/ci-test-shard.sh ${{ matrix.shard }}") {
			hasShardRun = true
		}
	}
	if !hasShardRun {
		return errors.New("macos-shard job does not run scripts/ci-test-shard.sh ${{ matrix.shard }}")
	}
	return verifyShardSpecsCoverEveryRootTest(specs)
}

func verifyShardSpecsCoverEveryRootTest(specs []string) error {
	list, err := exec.Command("go", "test", "-list", ".", ".").Output()
	if err != nil {
		return fmt.Errorf("go test -list: %w", err)
	}
	var names []string
	for _, line := range strings.Split(string(list), "\n") {
		if line != "" && !strings.HasPrefix(line, "ok ") {
			names = append(names, line)
		}
	}
	if len(names) < 100 {
		return fmt.Errorf("suspiciously few tests listed: %d", len(names))
	}
	tmpDir, err := os.MkdirTemp("", "deploy-shard-test-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	namesFile := filepath.Join(tmpDir, "names")
	if err := os.WriteFile(namesFile, []byte(strings.Join(names, "\n")+"\n"), 0o600); err != nil {
		return err
	}
	ran := map[string][]string{}
	seen := map[string]bool{}
	for _, spec := range specs {
		if seen[spec] {
			return fmt.Errorf("shard %s listed twice", spec)
		}
		seen[spec] = true
		cmd := exec.Command("sh", "scripts/ci-test-shard.sh", spec)
		cmd.Env = append(os.Environ(), "PANEWIRE_SHARD_DRY_RUN=1", "PANEWIRE_SHARD_NAMES_FILE="+namesFile)
		out, err := cmd.Output()
		if err != nil {
			return fmt.Errorf("shard %s: %w", spec, err)
		}
		pattern, err := regexp.Compile(strings.TrimSpace(string(out)))
		if err != nil {
			return fmt.Errorf("shard %s pattern %q: %w", spec, string(out), err)
		}
		for _, name := range names {
			if pattern.MatchString(name) {
				ran[name] = append(ran[name], spec)
			}
		}
	}
	for _, name := range names {
		switch len(ran[name]) {
		case 1:
		case 0:
			return fmt.Errorf("root test %s runs in no shard (matrix %v)", name, specs)
		default:
			return fmt.Errorf("root test %s runs in multiple shards: %v", name, ran[name])
		}
	}
	return nil
}

// TestDeployMacOSShardsCoverEveryRootTest pins that the macOS jobs of deploy.yml
// run the root package across scripts/ci-test-shard.sh shards in parallel,
// and that those shards run every top-level test exactly once.
func TestDeployMacOSShardsCoverEveryRootTest(t *testing.T) {
	raw, err := os.ReadFile(".github/workflows/deploy.yml")
	if err != nil {
		t.Fatal(err)
	}
	if err := checkDeployMacOSShards(raw); err != nil {
		t.Fatal(err)
	}
}

// TestDeployMacOSShardsDroppedShardMutantFails proves that dropping any shard
// from the macOS layout causes a failure because some root tests would not be run.
func TestDeployMacOSShardsDroppedShardMutantFails(t *testing.T) {
	raw, err := os.ReadFile(".github/workflows/deploy.yml")
	if err != nil {
		t.Fatal(err)
	}
	if err := checkDeployMacOSShards(raw); err != nil {
		t.Fatalf("unmutated deploy.yml is invalid: %v", err)
	}
	mutant := strings.Replace(string(raw), "'2/3'", "", 1)
	if mutant == string(raw) {
		t.Fatal("mutant did not apply")
	}
	if err := checkDeployMacOSShards([]byte(mutant)); err == nil {
		t.Fatal("dropping a root test shard went undetected")
	}
}

// TestDeployMacOSShardsFixedArgumentMutantFails proves that hardcoding a single
// shard argument instead of passing ${{ matrix.shard }} is rejected.
func TestDeployMacOSShardsFixedArgumentMutantFails(t *testing.T) {
	raw, err := os.ReadFile(".github/workflows/deploy.yml")
	if err != nil {
		t.Fatal(err)
	}
	if err := checkDeployMacOSShards(raw); err != nil {
		t.Fatalf("unmutated deploy.yml is invalid: %v", err)
	}
	mutant := strings.Replace(string(raw), "scripts/ci-test-shard.sh ${{ matrix.shard }}", "scripts/ci-test-shard.sh 0/3", 1)
	if mutant == string(raw) {
		t.Fatal("mutant did not apply")
	}
	if err := checkDeployMacOSShards([]byte(mutant)); err == nil {
		t.Fatal("hardcoding a fixed shard argument went undetected")
	}
}

package panewire

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// ciPRJob holds the fields of the pull request test path that readCI leaves out.
type ciPRJob struct {
	Name     string   `yaml:"name"`
	If       string   `yaml:"if"`
	Needs    []string `yaml:"needs"`
	Strategy struct {
		Matrix map[string][]string `yaml:"matrix"`
	} `yaml:"strategy"`
	Steps []struct {
		Run string `yaml:"run"`
	} `yaml:"steps"`
}

func readCIPRJobs(t *testing.T) map[string]ciPRJob {
	t.Helper()
	raw, err := os.ReadFile(".github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var w struct {
		Jobs map[string]ciPRJob `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &w); err != nil {
		t.Fatal(err)
	}
	return w.Jobs
}

func (j ciPRJob) runs() string {
	var runs string
	for _, step := range j.Steps {
		runs += step.Run + "\n"
	}
	return runs
}

// TestCIRequiredCheckGatesVetAndShards pins that `test (ubuntu-latest)` (the
// required check in .mergewake/policy.yaml) is the job that needs the vet job
// and the shards, and is red when either is not green.
func TestCIRequiredCheckGatesVetAndShards(t *testing.T) {
	jobs := readCIPRJobs(t)
	gate := jobs["test"]
	if gate.Name != "" || !slices.Equal(gate.Strategy.Matrix["os"], []string{"ubuntu-latest"}) {
		t.Fatalf("test must keep the check name `test (ubuntu-latest)`: name=%q matrix=%v", gate.Name, gate.Strategy.Matrix)
	}
	for _, need := range []string{"vet", "shard", "postgres"} {
		if !slices.Contains(gate.Needs, need) {
			t.Errorf("test must need %s", need)
		}
	}
	if !strings.HasPrefix(gate.If, "${{ always() && github.event_name == 'pull_request'") {
		t.Errorf("test must run after a failed need on pull requests: %s", gate.If)
	}
	if len(gate.Steps) != 1 {
		t.Fatalf("test gate has %d steps", len(gate.Steps))
	}
	for _, tc := range []struct {
		vet, shard, postgres string
		ok                   bool
	}{
		{"success", "success", "skipped", true},
		// An inbox-store PR runs postgres, and its failure fails the check.
		{"success", "success", "success", true},
		{"success", "success", "failure", false},
		{"success", "success", "cancelled", false},
		{"failure", "success", "skipped", false},
		{"success", "failure", "skipped", false},
		{"success", "cancelled", "skipped", false},
		{"success", "skipped", "skipped", false},
		{"skipped", "success", "skipped", false},
		{"", "success", "skipped", false},
	} {
		cmd := exec.Command("bash", "-e", "-c", gate.Steps[0].Run)
		cmd.Env = append(os.Environ(), "VET_RESULT="+tc.vet, "SHARD_RESULT="+tc.shard, "POSTGRES_RESULT="+tc.postgres)
		out, err := cmd.CombinedOutput()
		if (err == nil) != tc.ok {
			t.Errorf("%+v: %s %v", tc, out, err)
		}
	}
	for _, name := range []string{"vet", "shard"} {
		if jobs[name].If != "${{ github.event_name == 'pull_request' }}" {
			t.Errorf("%s must run on pull requests only: %s", name, jobs[name].If)
		}
	}
	for _, want := range []string{"gofmt -l .", "go vet ./...", "go build ./...", "go list ./..."} {
		if !strings.Contains(jobs["vet"].runs(), want) {
			t.Errorf("vet job missing %q", want)
		}
	}
}

// TestCIShardsCoverEveryRootTest proves the pull request shards run every
// top-level test of the root package exactly once. A removed shard matrix
// entry, or a selection rule that drops a name, leaves a test unrun and fails
// here.
func TestCIShardsCoverEveryRootTest(t *testing.T) {
	shard := readCIPRJobs(t)["shard"]
	specs := shard.Strategy.Matrix["shard"]
	if len(specs) == 0 {
		t.Fatal("no shards in the matrix")
	}
	if !slices.Contains(strings.Split(shard.runs(), "\n"), "scripts/ci-test-shard.sh ${{ matrix.shard }}") {
		t.Fatalf("shard job does not run scripts/ci-test-shard.sh with its matrix value: %s", shard.runs())
	}
	list, err := exec.Command("go", "test", "-list", ".", ".").Output()
	if err != nil {
		t.Fatalf("go test -list: %v", err)
	}
	var names []string
	for _, line := range strings.Split(string(list), "\n") {
		if line != "" && !strings.HasPrefix(line, "ok ") {
			names = append(names, line)
		}
	}
	if len(names) < 100 {
		t.Fatalf("suspiciously few tests listed: %d", len(names))
	}
	namesFile := filepath.Join(t.TempDir(), "names")
	if err := os.WriteFile(namesFile, []byte(strings.Join(names, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ran := map[string][]string{}
	seen := map[string]bool{}
	for _, spec := range specs {
		if seen[spec] {
			t.Fatalf("shard %s listed twice", spec)
		}
		seen[spec] = true
		cmd := exec.Command("sh", "scripts/ci-test-shard.sh", spec)
		cmd.Env = append(os.Environ(), "PANEWIRE_SHARD_DRY_RUN=1", "PANEWIRE_SHARD_NAMES_FILE="+namesFile)
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("shard %s: %v", spec, err)
		}
		pattern := regexp.MustCompile(strings.TrimSpace(string(out)))
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
			t.Errorf("%s runs in no shard (matrix %v)", name, specs)
		default:
			t.Errorf("%s runs in several shards: %v", name, ran[name])
		}
	}
}

// TestCIShardsAreBalancedByRecordedDurations pins that the shards are balanced
// by recorded durations (decision 178) so no shard monopolizes runtime.
func TestCIShardsAreBalancedByRecordedDurations(t *testing.T) {
	shard := readCIPRJobs(t)["shard"]
	specs := shard.Strategy.Matrix["shard"]
	if len(specs) != 3 {
		t.Fatalf("expected 3 shards, got %d", len(specs))
	}
	durationsRaw, err := os.ReadFile("scripts/ci-test-durations.json")
	if err != nil {
		t.Fatalf("reading scripts/ci-test-durations.json: %v", err)
	}
	var durations map[string]float64
	if err := json.Unmarshal(durationsRaw, &durations); err != nil {
		t.Fatalf("unmarshaling durations: %v", err)
	}
	if len(durations) == 0 {
		t.Fatal("ci-test-durations.json has no entries")
	}

	shardTotals := make([]float64, len(specs))
	for i, spec := range specs {
		cmd := exec.Command("sh", "scripts/ci-test-shard.sh", spec)
		cmd.Env = append(os.Environ(), "PANEWIRE_SHARD_DRY_RUN=1")
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("shard %s: %v", spec, err)
		}
		pattern := regexp.MustCompile(strings.TrimSpace(string(out)))
		for name, dur := range durations {
			if pattern.MatchString(name) {
				shardTotals[i] += dur
			}
		}
	}

	minTot, maxTot := shardTotals[0], shardTotals[0]
	for _, tot := range shardTotals {
		if tot < minTot {
			minTot = tot
		}
		if tot > maxTot {
			maxTot = tot
		}
	}
	t.Logf("Shard recorded duration totals: %v (min=%.2fs, max=%.2fs)", shardTotals, minTot, maxTot)
	// Dots observed 237s, 131s, 130s (spread > 100s).
	// With duration balancing, maxTot - minTot must be within 10 seconds.
	if maxTot-minTot > 10.0 {
		t.Errorf("shards are unbalanced: totals=%v max-min=%.2fs (>10s)", shardTotals, maxTot-minTot)
	}
}

// TestCIShardsHandleUnrecordedAndEmptyDurations verifies that a new test
// absent from recorded durations or an empty durations file still assigns
// every test to exactly one shard without failing.
func TestCIShardsHandleUnrecordedAndEmptyDurations(t *testing.T) {
	shard := readCIPRJobs(t)["shard"]
	specs := shard.Strategy.Matrix["shard"]
	names := []string{"TestExistingA", "TestExistingB", "TestUnrecordedNew", "TestExistingC"}
	namesFile := filepath.Join(t.TempDir(), "names")
	if err := os.WriteFile(namesFile, []byte(strings.Join(names, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	partialDurations := map[string]float64{
		"TestExistingA": 10.0,
		"TestExistingB": 5.0,
		"TestExistingC": 2.0,
	}
	durationsBytes, err := json.Marshal(partialDurations)
	if err != nil {
		t.Fatal(err)
	}
	durationsFile := filepath.Join(t.TempDir(), "partial-durations.json")
	if err := os.WriteFile(durationsFile, durationsBytes, 0o600); err != nil {
		t.Fatal(err)
	}

	ran := map[string][]string{}
	for _, spec := range specs {
		cmd := exec.Command("sh", "scripts/ci-test-shard.sh", spec)
		cmd.Env = append(os.Environ(),
			"PANEWIRE_SHARD_DRY_RUN=1",
			"PANEWIRE_SHARD_NAMES_FILE="+namesFile,
			"PANEWIRE_SHARD_DURATIONS_FILE="+durationsFile,
		)
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("shard %s: %v", spec, err)
		}
		pattern := regexp.MustCompile(strings.TrimSpace(string(out)))
		for _, name := range names {
			if pattern.MatchString(name) {
				ran[name] = append(ran[name], spec)
			}
		}
	}

	for _, name := range names {
		if len(ran[name]) != 1 {
			t.Errorf("%s assigned to %d shards: %v", name, len(ran[name]), ran[name])
		}
	}
}

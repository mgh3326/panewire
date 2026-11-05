package panewire

import (
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type ciWorkflow struct {
	Concurrency struct {
		Group  string `yaml:"group"`
		Cancel bool   `yaml:"cancel-in-progress"`
	} `yaml:"concurrency"`
	Jobs map[string]struct {
		If       string   `yaml:"if"`
		RunsOn   any      `yaml:"runs-on"`
		Strategy any      `yaml:"strategy"`
		Needs    []string `yaml:"needs"`
		Uses     string   `yaml:"uses"`
		Continue bool     `yaml:"continue-on-error"`
		Steps    []struct {
			Run string `yaml:"run"`
		} `yaml:"steps"`
	} `yaml:"jobs"`
}

func readCI(t *testing.T, path string) ciWorkflow {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var w ciWorkflow
	if err := yaml.Unmarshal(raw, &w); err != nil {
		t.Fatal(err)
	}
	return w
}
func TestCITiers(t *testing.T) {
	w := readCI(t, ".github/workflows/ci.yml")
	if !w.Concurrency.Cancel || !strings.Contains(w.Concurrency.Group, "ci-main") {
		t.Fatal("main concurrency must cancel older runs")
	}
	for name, job := range w.Jobs {
		if strings.Contains(job.If, "github.event_name != 'pull_request'") {
			continue
		}
		runner, _ := yaml.Marshal([]any{job.RunsOn, job.Strategy})
		if strings.Contains(string(runner), "vars.") || strings.Contains(string(runner), "self-hosted") {
			t.Errorf("PR-reachable job %s has unsafe runner: %s", name, runner)
		}
		for _, step := range job.Steps {
			if strings.Contains(step.Run, "-race") || strings.Contains(step.Run, "postgres:") {
				t.Errorf("PR-reachable job %s runs heavy check", name)
			}
		}
	}
	for _, name := range []string{"post-test", "postgres", "test-macos"} {
		job := w.Jobs[name]
		if !strings.Contains(job.If, "github.event_name != 'pull_request'") {
			t.Errorf("%s is not post-only", name)
		}
		runner, _ := yaml.Marshal(job.RunsOn)
		if !strings.Contains(string(runner), "vars.PANEWIRE_POST_") {
			t.Errorf("%s missing post runner", name)
		}
	}
	post := w.Jobs["post-test"]
	var runs string
	for _, step := range post.Steps {
		runs += step.Run + "\n"
	}
	for _, want := range []string{"-race -count=3", "TestQuiet", "TestAsyncSpawn", "sh -n"} {
		if !strings.Contains(runs, want) {
			t.Errorf("post-test missing %s", want)
		}
	}
	mac := w.Jobs["test-macos"]
	if !mac.Continue {
		t.Error("macOS must remain advisory")
	}
	var macRuns string
	for _, step := range mac.Steps {
		macRuns += step.Run
	}
	if !strings.Contains(macRuns, "gitleaks") {
		t.Error("macOS missing gitleaks")
	}
	gate := w.Jobs["ci-ok"]
	if gate.If != "${{ always() && (contains(needs.*.result, 'failure') || !contains(needs.*.result, 'cancelled')) }}" {
		t.Fatal("ci-ok cancellation rule changed")
	}
	if slices.Contains(gate.Needs, "test-macos") {
		t.Fatal("macOS must remain advisory")
	}
	raw, err := os.ReadFile(".github/workflows/ci-any-ref.yml")
	if err != nil {
		t.Fatal(err)
	}
	var any struct {
		Jobs map[string]struct {
			Uses string `yaml:"uses"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &any); err != nil {
		t.Fatal(err)
	}
	if any.Jobs["ci"].Uses != "./.github/workflows/ci.yml" {
		t.Fatal("dispatch must call post suite")
	}
}
func TestCIGateResults(t *testing.T) {
	gate := readCI(t, ".github/workflows/ci.yml").Jobs["ci-ok"]
	for _, tc := range []struct {
		event, suite, test, post, pg, auto string
		ok                                 bool
	}{
		{"pull_request", "all", "success", "skipped", "skipped", "success", true},
		{"pull_request", "all", "failure", "skipped", "skipped", "success", false},
		{"push", "all", "skipped", "success", "success", "success", true},
		{"push", "all", "skipped", "success", "failure", "success", false},
		{"workflow_dispatch", "test", "skipped", "success", "skipped", "success", true},
		{"workflow_dispatch", "postgres", "skipped", "skipped", "success", "success", true},
		{"push", "all", "skipped", "success", "success", "failure", false},
		{"push", "unknown", "skipped", "skipped", "skipped", "success", false},
	} {
		cmd := exec.Command("bash", "-e", "-c", gate.Steps[0].Run)
		cmd.Env = append(os.Environ(), "EVENT="+tc.event, "SUITE="+tc.suite, "TEST_RESULT="+tc.test, "POST_TEST_RESULT="+tc.post, "POSTGRES_RESULT="+tc.pg, "AUTO_TAG_RESULT="+tc.auto)
		out, err := cmd.CombinedOutput()
		if (err == nil) != tc.ok {
			t.Errorf("%+v: %s %v", tc, out, err)
		}
	}
}

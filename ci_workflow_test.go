package panewire

import (
	"encoding/json"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"testing"

	"gopkg.in/yaml.v3"
)

type ciWorkflow struct {
	Jobs map[string]struct {
		If    string   `yaml:"if"`
		Needs []string `yaml:"needs"`
		Steps []struct {
			Run string `yaml:"run"`
		} `yaml:"steps"`
	} `yaml:"jobs"`
}

// ciJobSuites returns the suites a job's `if: contains(fromJSON('[...]'),
// inputs.suite || 'all')` selects.
func ciJobSuites(t *testing.T, condition string) []string {
	t.Helper()
	m := regexp.MustCompile(`fromJSON\('(\[[^']*\])'\), inputs\.suite \|\| 'all'\)`).FindStringSubmatch(condition)
	if m == nil {
		t.Fatalf("unrecognised job condition %q", condition)
	}
	var suites []string
	if err := json.Unmarshal([]byte(m[1]), &suites); err != nil {
		t.Fatal(err)
	}
	return suites
}

// ci-ok runs the job list's own gate script: on a PR (suite all) both the
// test and the postgres job are required; a dispatched partial suite passes
// with the job it does not select skipped.
func TestCIOkRequiresOnlyTheSuitesJobs(t *testing.T) {
	raw, err := os.ReadFile(".github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var workflow ciWorkflow
	if err := yaml.Unmarshal(raw, &workflow); err != nil {
		t.Fatal(err)
	}
	gate := workflow.Jobs["ci-ok"]
	if !slices.Contains(gate.Needs, "test") || !slices.Contains(gate.Needs, "postgres") || len(gate.Steps) != 1 {
		t.Fatalf("ci-ok needs %v with %d steps", gate.Needs, len(gate.Steps))
	}
	selected := map[string][]string{}
	for _, job := range []string{"test", "postgres"} {
		selected[job] = ciJobSuites(t, workflow.Jobs[job].If)
	}
	// A PR has no inputs, so the suite is 'all': both jobs run.
	for job, suites := range selected {
		if !slices.Contains(suites, "all") {
			t.Fatalf("%s does not run on PRs (suites %v)", job, suites)
		}
	}
	result := func(job, suite string, outcome string) string {
		if !slices.Contains(selected[job], suite) {
			return "skipped"
		}
		return outcome
	}
	for _, tc := range []struct {
		suite, test, postgres string
		ok                    bool
	}{
		{"all", "success", "success", true},
		{"all", "success", "failure", false},
		{"all", "failure", "success", false},
		{"all", "success", "cancelled", false},
		{"test", "success", "", true},
		{"test", "failure", "", false},
		{"postgres", "", "success", true},
		{"postgres", "", "failure", false},
		// A suite no job knows runs nothing; it must not pass.
		{"lint", "", "", false},
	} {
		testResult, postgresResult := result("test", tc.suite, tc.test), result("postgres", tc.suite, tc.postgres)
		cmd := exec.Command("bash", "-e", "-c", gate.Steps[0].Run)
		cmd.Env = append(os.Environ(), "SUITE="+tc.suite, "TEST_RESULT="+testResult, "POSTGRES_RESULT="+postgresResult)
		out, err := cmd.CombinedOutput()
		if (err == nil) != tc.ok {
			t.Errorf("suite=%s test=%s postgres=%s: ok=%v, want %v (%s)", tc.suite, testResult, postgresResult, err == nil, tc.ok, out)
		}
	}
	// A skipped job the suite selects is a failure, not a pass.
	cmd := exec.Command("bash", "-e", "-c", gate.Steps[0].Run)
	cmd.Env = append(os.Environ(), "SUITE=all", "TEST_RESULT=success", "POSTGRES_RESULT=skipped")
	if err := cmd.Run(); err == nil {
		t.Fatal("ci-ok passed a PR whose postgres job was skipped")
	}
}

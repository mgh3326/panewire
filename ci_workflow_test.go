package panewire

import (
	"encoding/json"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strings"
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
		cmd.Env = append(os.Environ(), "SUITE="+tc.suite, "TEST_RESULT="+testResult, "POSTGRES_RESULT="+postgresResult, "AUTO_TAG_RESULT=success")
		out, err := cmd.CombinedOutput()
		if (err == nil) != tc.ok {
			t.Errorf("suite=%s test=%s postgres=%s: ok=%v, want %v (%s)", tc.suite, testResult, postgresResult, err == nil, tc.ok, out)
		}
	}

	cmdAuto := exec.Command("bash", "-e", "-c", gate.Steps[0].Run)
	cmdAuto.Env = append(os.Environ(), "SUITE=all", "TEST_RESULT=success", "POSTGRES_RESULT=success", "AUTO_TAG_RESULT=failure")
	if err := cmdAuto.Run(); err == nil {
		t.Fatal("ci-ok admitted failing auto-tag regression")
	}
	// A skipped job the suite selects is a failure, not a pass.
	cmd := exec.Command("bash", "-e", "-c", gate.Steps[0].Run)
	cmd.Env = append(os.Environ(), "SUITE=all", "TEST_RESULT=success", "POSTGRES_RESULT=skipped", "AUTO_TAG_RESULT=success")
	if err := cmd.Run(); err == nil {
		t.Fatal("ci-ok passed a PR whose postgres job was skipped")
	}
}

// macOS runs as an advisory job (operator decision 2026-11-05): hosted macOS
// runners are often not acquired, so neither ci-ok, mergewake's required
// checks nor the ci-any-ref status may wait on it.
func TestCIMacOSIsAdvisory(t *testing.T) {
	raw, err := os.ReadFile(".github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]struct {
			If              string   `yaml:"if"`
			Needs           []string `yaml:"needs"`
			RunsOn          string   `yaml:"runs-on"`
			ContinueOnError bool     `yaml:"continue-on-error"`
			Strategy        struct {
				Matrix struct {
					OS []string `yaml:"os"`
				} `yaml:"matrix"`
			} `yaml:"strategy"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &workflow); err != nil {
		t.Fatal(err)
	}
	mac, ok := workflow.Jobs["test-macos"]
	if !ok || mac.RunsOn != "macos-latest" || !mac.ContinueOnError {
		t.Fatalf("test-macos must exist on macos-latest with continue-on-error: %+v", mac)
	}
	// ci-any-ref passes a ref; its status must not wait on the advisory job.
	if !strings.HasSuffix(mac.If, "&& !inputs.ref }}") {
		t.Fatalf("test-macos must not run on ci-any-ref dispatches: if = %q", mac.If)
	}
	// Only-cancelled needs skip ci-ok (mergewake: unknown, hold); a failure
	// still runs the gate (hk 2924).
	if got := workflow.Jobs["ci-ok"].If; got != "${{ always() && (contains(needs.*.result, 'failure') || !contains(needs.*.result, 'cancelled')) }}" {
		t.Fatalf("ci-ok if = %q, want it skipped when needs are only cancelled", got)
	}
	if slices.Contains(workflow.Jobs["ci-ok"].Needs, "test-macos") {
		t.Fatal("ci-ok must not need the advisory test-macos job")
	}
	if matrix := workflow.Jobs["test"].Strategy.Matrix.OS; !slices.Equal(matrix, []string{"ubuntu-latest"}) {
		t.Fatalf("the required test matrix must be ubuntu only, got %v", matrix)
	}

	rawPolicy, err := os.ReadFile(".mergewake/policy.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var policy struct {
		RequiredChecks []string `yaml:"required_checks"`
	}
	if err := yaml.Unmarshal(rawPolicy, &policy); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(policy.RequiredChecks, "test (ubuntu-latest)") {
		t.Fatalf("required_checks lost the Linux test: %v", policy.RequiredChecks)
	}
	for _, check := range policy.RequiredChecks {
		if regexp.MustCompile(`(?i)macos`).MatchString(check) {
			t.Fatalf("required_checks must not wait on macOS: %v", policy.RequiredChecks)
		}
	}

	// The gate output must be a reusable-workflow output (on.workflow_call),
	// or ci-any-ref reads it empty.
	var trigger struct {
		On struct {
			WorkflowCall struct {
				Outputs map[string]struct {
					Value string `yaml:"value"`
				} `yaml:"outputs"`
			} `yaml:"workflow_call"`
		} `yaml:"on"`
	}
	if err := yaml.Unmarshal(raw, &trigger); err != nil {
		t.Fatal(err)
	}
	if got := trigger.On.WorkflowCall.Outputs["ok"].Value; got != "${{ jobs.ci-ok.outputs.ok }}" {
		t.Fatalf("on.workflow_call.outputs.ok = %q, want the ci-ok gate output", got)
	}

	anyRef, err := os.ReadFile(".github/workflows/ci-any-ref.yml")
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`RESULT: \$\{\{ needs\.ci\.outputs\.ok == 'true'`).Match(anyRef) {
		t.Fatal("ci-any-ref must post its status from the ci-ok gate output, not the whole run")
	}
}

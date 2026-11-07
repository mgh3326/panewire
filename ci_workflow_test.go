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
		Services any      `yaml:"services"`
		Needs    []string `yaml:"needs"`
		Uses     string   `yaml:"uses"`
		Continue bool     `yaml:"continue-on-error"`
		Steps    []struct {
			Name string `yaml:"name"`
			Run  string `yaml:"run"`
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
	// One group per ref (policy/fleet-ci-main-cancel, mergewake docs/ci-policy.md):
	// a constant group such as 'ci-main' is reported as not per ref.
	if !w.Concurrency.Cancel || !strings.Contains(w.Concurrency.Group, "format('ci-push-{0}', github.ref)") {
		t.Fatal("main concurrency must cancel older runs, grouped per ref")
	}
	if !strings.Contains(w.Concurrency.Group, "format('ci-pr-{0}', github.event.pull_request.number)") {
		t.Fatal("a pull request's new run must cancel its older run")
	}
	postOnly := map[string]string{
		"post-test": `${{ github.event_name != 'pull_request' && contains(fromJSON('["all","test"]'), inputs.suite || 'all') }}`,
		// postgres also runs on a pull request that touches the inbox store
		// (store-paths), because the production hub keeps its inbox there.
		"postgres": `${{ !cancelled() && ((github.event_name != 'pull_request' && contains(fromJSON('["all","postgres"]'), inputs.suite || 'all')) || (github.event_name == 'pull_request' && needs.store-paths.outputs.store == 'true')) }}`,
	}
	for name, job := range w.Jobs {
		if condition, ok := postOnly[name]; ok && job.If == condition {
			continue
		}
		if name == "store-paths" && job.If != "${{ github.event_name == 'pull_request' }}" {
			t.Errorf("store-paths must run on pull requests only: %s", job.If)
		}
		runner, _ := yaml.Marshal([]any{job.RunsOn, job.Strategy})
		if strings.Contains(string(runner), "vars.") || strings.Contains(string(runner), "vars[") || strings.Contains(strings.ToLower(string(runner)), "self-hosted") || strings.Contains(strings.ToLower(string(runner)), "macos") {
			t.Errorf("PR-reachable job %s has unsafe runner: %s", name, runner)
		}
		services, _ := yaml.Marshal(job.Services)
		if strings.Contains(strings.ToLower(string(services)), "postgres") {
			t.Errorf("PR-reachable job %s has Postgres service", name)
		}
		for _, step := range job.Steps {
			if strings.Contains(step.Run, "-race") || strings.Contains(step.Run, "postgres:") {
				t.Errorf("PR-reachable job %s runs heavy check", name)
			}
		}
	}
	if pg := w.Jobs["postgres"]; !slices.Contains(pg.Needs, "store-paths") {
		t.Error("postgres must need store-paths to decide pull request runs")
	}
	for _, name := range []string{"post-test", "postgres"} {
		job := w.Jobs[name]
		if job.If != postOnly[name] {
			t.Errorf("%s is not post-only (or, for postgres, not store-gated)", name)
		}
		runner, _ := yaml.Marshal(job.RunsOn)
		if !strings.Contains(string(runner), "vars.PANEWIRE_POST_") {
			t.Errorf("%s missing post runner", name)
		}
		// ci-any-ref runs any ref through workflow_call; only a reviewed
		// main push may reach a runner chosen by repository variable.
		if !strings.Contains(string(runner), "github.event_name == 'push' && github.ref == 'refs/heads/main' && vars.PANEWIRE_POST_") {
			t.Errorf("%s post runner is not limited to main pushes: %s", name, runner)
		}
	}
	post := w.Jobs["post-test"]
	var runs string
	for _, step := range post.Steps {
		runs += step.Run + "\n"
	}
	for _, want := range []string{"gofmt -l .", "go vet ./...", "go test ./...", "sh -n"} {
		if !strings.Contains(runs, want) {
			t.Errorf("post-test missing %s", want)
		}
	}
	// The heavy checks moved to deploy.yml (r2 §1): the main stage keeps the
	// basic set and must not run a race pin or a macOS runner.
	if strings.Contains(runs, "-race") {
		t.Error("post-test still runs a race pin; race belongs to deploy.yml")
	}
	if _, ok := w.Jobs["test-macos"]; ok {
		t.Error("macOS belongs to deploy.yml, not the main stage")
	}
	pg := w.Jobs["postgres"]
	var pgRuns string
	for _, step := range pg.Steps {
		pgRuns += step.Name + "\n" + step.Run + "\n"
	}
	if !strings.Contains(pgRuns, "go test -count=1 -v") {
		t.Error("main stage must run the Postgres suite without -race")
	}
	if !strings.Contains(pgRuns, "migration and startup") {
		t.Error("main stage must run the migration and startup check")
	}
	gate := w.Jobs["ci-ok"]
	if gate.If != "${{ always() && (contains(needs.*.result, 'failure') || !contains(needs.*.result, 'cancelled')) }}" {
		t.Fatal("ci-ok cancellation rule changed")
	}
	for _, required := range []string{"test", "post-test", "postgres", "auto-tag-test"} {
		if !slices.Contains(gate.Needs, required) {
			t.Errorf("ci-ok must need %s", required)
		}
	}
	if slices.Contains(gate.Needs, "test-macos") {
		t.Fatal("macOS is not a main-stage job")
	}
	raw, err := os.ReadFile(".github/workflows/ci-any-ref.yml")
	if err != nil {
		t.Fatal(err)
	}
	var any struct {
		Jobs map[string]struct {
			Uses string            `yaml:"uses"`
			Env  map[string]string `yaml:"env"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &any); err != nil {
		t.Fatal(err)
	}
	if any.Jobs["ci"].Uses != "./.github/workflows/ci.yml" {
		t.Fatal("dispatch must call post suite")
	}
	if !strings.Contains(any.Jobs["status"].Env["RESULT"], "needs.ci.outputs.ok == 'true'") {
		t.Fatal("dispatch status must use the gate output")
	}
	raw, err = os.ReadFile(".github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
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
	if trigger.On.WorkflowCall.Outputs["ok"].Value != "${{ jobs.ci-ok.outputs.ok }}" {
		t.Fatal("reusable workflow must expose the gate output")
	}
	raw, err = os.ReadFile(".mergewake/policy.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var policy struct {
		RequiredChecks []string `yaml:"required_checks"`
	}
	if err := yaml.Unmarshal(raw, &policy); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(policy.RequiredChecks, "test (ubuntu-latest)") {
		t.Fatal("Linux check must remain required")
	}
	for _, check := range policy.RequiredChecks {
		if strings.Contains(strings.ToLower(check), "macos") {
			t.Fatal("macOS must remain advisory")
		}
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

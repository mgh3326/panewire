package panewire

import (
	"errors"
	"os"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// needsList accepts both `needs: job` and `needs: [job, ...]`.
type needsList []string

func (n *needsList) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.ScalarNode:
		*n = []string{value.Value}
	case yaml.SequenceNode:
		var out []string
		if err := value.Decode(&out); err != nil {
			return err
		}
		*n = out
	}
	return nil
}

// deployWorkflow is the part of .github/workflows/deploy.yml this file pins:
// its triggers, its never-cancel concurrency rule, and the commit status it
// posts for the candidate sha.
type deployWorkflow struct {
	On struct {
		WorkflowRun struct {
			Workflows []string `yaml:"workflows"`
			Types     []string `yaml:"types"`
			Branches  []string `yaml:"branches"`
		} `yaml:"workflow_run"`
		WorkflowCall struct {
			Inputs map[string]struct {
				Required bool   `yaml:"required"`
				Default  string `yaml:"default"`
				Type     string `yaml:"type"`
			} `yaml:"inputs"`
		} `yaml:"workflow_call"`
		Push             any `yaml:"push"`
		PullRequest      any `yaml:"pull_request"`
		WorkflowDispatch any `yaml:"workflow_dispatch"`
	} `yaml:"on"`
	Permissions struct {
		Contents string `yaml:"contents"`
		Statuses string `yaml:"statuses"`
	} `yaml:"permissions"`
	Concurrency struct {
		Group  string `yaml:"group"`
		Cancel *bool  `yaml:"cancel-in-progress"`
	} `yaml:"concurrency"`
	Jobs map[string]struct {
		If    string    `yaml:"if"`
		Needs needsList `yaml:"needs"`
		Uses  string    `yaml:"uses"`
		Steps []struct {
			Name string            `yaml:"name"`
			Run  string            `yaml:"run"`
			Env  map[string]string `yaml:"env"`
			With map[string]string `yaml:"with"`
		} `yaml:"steps"`
	} `yaml:"jobs"`
}

// deployShape is the shape the deploy stage must keep. It is a plain function
// so the mutation test below can feed it an edited copy of the file and see
// the same assertion go RED.
func deployShape(raw []byte) error {
	var w deployWorkflow
	if err := yaml.Unmarshal(raw, &w); err != nil {
		return err
	}
	text := string(raw)

	// Triggers: only a completed CI run on main, plus the any-ref call.
	if !slices.Equal(w.On.WorkflowRun.Workflows, []string{"CI"}) {
		return errors.New("deploy must trigger on the CI workflow")
	}
	if !slices.Equal(w.On.WorkflowRun.Types, []string{"completed"}) {
		return errors.New("deploy must trigger on workflow_run completion")
	}
	if !slices.Equal(w.On.WorkflowRun.Branches, []string{"main"}) {
		return errors.New("deploy must trigger on main only")
	}
	for name, trigger := range map[string]any{
		"push": w.On.Push, "pull_request": w.On.PullRequest, "workflow_dispatch": w.On.WorkflowDispatch,
	} {
		if trigger != nil {
			return errors.New("deploy has an extra trigger: " + name)
		}
	}
	if _, ok := w.On.WorkflowCall.Inputs["ref"]; !ok {
		return errors.New("deploy must accept the resolved sha from an any-ref call")
	}

	// Same sha: the candidate comes from the triggering run's head, and every
	// checkout and the status use that resolved sha, never the deploy run's
	// own github.sha (which is the default branch).
	candidate, ok := w.Jobs["candidate"]
	if !ok {
		return errors.New("deploy has no candidate job")
	}
	if !strings.Contains(candidate.If, "github.event.workflow_run.conclusion == 'success'") ||
		!strings.Contains(candidate.If, "github.event.workflow_run.head_branch == 'main'") {
		return errors.New("candidate must run only on a successful main-stage run")
	}
	if !strings.Contains(candidate.If, "inputs.ref != ''") {
		return errors.New("candidate must recognise the any-ref call by its sha input")
	}
	if !strings.Contains(text, "RUN_SHA: ${{ github.event.workflow_run.head_sha }}") {
		return errors.New("candidate must take the triggering run's head_sha")
	}
	if strings.Contains(text, "github.sha") {
		return errors.New("deploy must never derive the candidate from github.sha")
	}
	checkouts := 0
	for name, job := range w.Jobs {
		for _, step := range job.Steps {
			if step.With["ref"] == "${{ needs.candidate.outputs.sha }}" {
				checkouts++
			}
		}
		if name == "candidate" {
			continue
		}
		if !slices.Contains(job.Needs, "candidate") {
			return errors.New("deploy job " + name + " does not need candidate")
		}
	}
	if checkouts == 0 {
		return errors.New("no checkout pins the candidate sha")
	}

	// Never cancelled: concurrency must say so explicitly, and the group must
	// be one per repository so a newer head replaces only a pending entry.
	if w.Concurrency.Cancel == nil || *w.Concurrency.Cancel {
		return errors.New("deploy concurrency must set cancel-in-progress: false")
	}
	if !strings.Contains(w.Concurrency.Group, "github.repository") {
		return errors.New("deploy concurrency group must be per repository")
	}

	if w.Permissions.Contents != "read" || w.Permissions.Statuses != "write" {
		return errors.New("deploy needs contents: read and statuses: write")
	}

	// Posts deploy-ok with the candidate evidence.
	status := w.Jobs["deploy-ok"]
	if status.If != "${{ always() && needs.candidate.result == 'success' }}" {
		return errors.New("deploy-ok must post even when a check fails, once a sha is resolved")
	}
	var statusRun string
	var statusEnv map[string]string
	for _, step := range status.Steps {
		if strings.Contains(step.Run, "context=deploy-ok") {
			statusRun, statusEnv = step.Run, step.Env
		}
	}
	if statusRun == "" {
		return errors.New("no step posts the deploy-ok commit status")
	}
	if !strings.Contains(statusRun, "statuses/${TARGET_SHA}") {
		return errors.New("deploy-ok must post on the candidate sha")
	}
	for _, field := range []string{"REPO", "TARGET_SHA", "POLICY_REVISION", "RUN_ID", "ARTIFACT_DIGEST"} {
		if _, ok := statusEnv[field]; !ok {
			return errors.New("deploy-ok evidence is missing " + field)
		}
	}
	for _, field := range []string{"repo", "sha", "policy_revision", "suite", "environment", "run_id", "result", "digest"} {
		if !strings.Contains(strings.ToLower(statusRun), field) {
			return errors.New("deploy-ok evidence never names " + field)
		}
	}
	return nil
}

func readDeployWorkflow(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile(".github/workflows/deploy.yml")
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestDeployWorkflowShape pins the deploy stage: it runs only from a
// successful main-stage run for the same sha, it never cancels, and it posts
// deploy-ok with the candidate evidence.
func TestDeployWorkflowShape(t *testing.T) {
	raw := readDeployWorkflow(t)
	if err := deployShape(raw); err != nil {
		t.Fatal(err)
	}
	// The one excluded flaky group is named with its hk id (r2 §1).
	if !strings.Contains(string(raw), "hk 1243") {
		t.Error("deploy.yml must name the excluded flaky test's hk id")
	}
}

// TestDeployWorkflowCancelMutantFails is the acceptance mutant: set
// cancel-in-progress true on deploy.yml and the shape check is RED.
func TestDeployWorkflowCancelMutantFails(t *testing.T) {
	raw := string(readDeployWorkflow(t))
	if err := deployShape([]byte(raw)); err != nil {
		t.Fatalf("unmutated deploy.yml is already invalid: %v", err)
	}
	mutant := strings.Replace(raw, "cancel-in-progress: false", "cancel-in-progress: true", 1)
	if mutant == raw {
		t.Fatal("mutant did not apply: deploy.yml has no cancel-in-progress: false")
	}
	if err := deployShape([]byte(mutant)); err == nil {
		t.Fatal("cancel-in-progress: true went undetected")
	}
}

// TestDeployWorkflowBranchMutantFails proves the main-only trigger is load
// bearing too: drop it and the shape check is RED.
func TestDeployWorkflowBranchMutantFails(t *testing.T) {
	raw := string(readDeployWorkflow(t))
	mutant := strings.Replace(raw, "    branches: [main]\n", "", 1)
	if mutant == raw {
		t.Fatal("mutant did not apply: deploy.yml has no main branch filter")
	}
	if err := deployShape([]byte(mutant)); err == nil {
		t.Fatal("a deploy without the main-only branch filter went undetected")
	}
}

// TestDeployStageLeavesTheMergeGateAlone pins r2 §4-3: this PR posts deploy-ok
// but keeps the ci-ok gate, so the policy and release.yml read the same thing
// before and after it.
func TestDeployStageLeavesTheMergeGateAlone(t *testing.T) {
	raw, err := os.ReadFile(".mergewake/policy.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var policy struct {
		RequiredChecks []string `yaml:"required_checks"`
		WatchChecks    []string `yaml:"watch_checks"`
	}
	if err := yaml.Unmarshal(raw, &policy); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(policy.RequiredChecks, []string{"test (ubuntu-latest)"}) {
		t.Errorf("required_checks changed: %v", policy.RequiredChecks)
	}
	if !slices.Equal(policy.WatchChecks, []string{"ci-ok"}) {
		t.Errorf("watch_checks must stay ci-ok: %v", policy.WatchChecks)
	}
	release, err := os.ReadFile(".github/workflows/release.yml")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(release), "deploy-ok") {
		t.Error("release.yml must keep reading the ci-ok gate for now")
	}
}

// TestCIAnyRefDeploySuite proves suite=deploy reaches the deploy stage with
// the resolved sha, and that ci.yml is not asked for a suite it does not have.
func TestCIAnyRefDeploySuite(t *testing.T) {
	raw, err := os.ReadFile(".github/workflows/ci-any-ref.yml")
	if err != nil {
		t.Fatal(err)
	}
	var wf struct {
		On struct {
			WorkflowDispatch struct {
				Inputs map[string]struct {
					Options []string `yaml:"options"`
				} `yaml:"inputs"`
			} `yaml:"workflow_dispatch"`
		} `yaml:"on"`
		Jobs map[string]struct {
			If   string `yaml:"if"`
			Uses string `yaml:"uses"`
			With struct {
				Ref string `yaml:"ref"`
			} `yaml:"with"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &wf); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(wf.On.WorkflowDispatch.Inputs["suite"].Options, "deploy") {
		t.Error("suite must offer deploy")
	}
	if wf.Jobs["ci"].If != "${{ inputs.suite != 'deploy' }}" {
		t.Errorf("the ci suite must not run for deploy: %s", wf.Jobs["ci"].If)
	}
	deploy := wf.Jobs["deploy"]
	if deploy.Uses != "./.github/workflows/deploy.yml" {
		t.Errorf("suite=deploy must call deploy.yml, got %q", deploy.Uses)
	}
	if deploy.With.Ref != "${{ needs.resolve.outputs.sha }}" {
		t.Errorf("suite=deploy must pass the resolved sha, got %q", deploy.With.Ref)
	}
	if !strings.Contains(wf.Jobs["status"].If, "inputs.suite != 'deploy'") {
		t.Error("the ci-ok status must not post for suite=deploy")
	}
}

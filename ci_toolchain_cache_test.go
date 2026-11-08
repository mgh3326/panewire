package panewire

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The toolchain cache rule (hk decision/2026-11-08/ci-toolchain-cache): every
// job that runs a Go command sets Go up through the local composite action,
// which restores the toolchain from the Actions cache before actions/setup-go,
// retries setup-go with backoff, and pins every action it uses to a full SHA.

const (
	toolchainCacheUse        = "./.github/actions/setup-go-cached"
	toolchainCacheActionPath = ".github/actions/setup-go-cached/action.yml"
)

var (
	toolchainCacheWorkflows = []string{
		".github/workflows/ci.yml",
		".github/workflows/deploy.yml",
		".github/workflows/release.yml",
	}
	toolchainCacheFullSHA = regexp.MustCompile(`^[0-9a-f]{40}$`)
	toolchainCacheExact   = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)
	toolchainCacheGoCmd   = regexp.MustCompile(`(^|[\s;&|(])(go (test|vet|build|run|mod|generate|env|version|list)\b|scripts/ci-test-shard\.sh\b)`)
)

// toolchainCacheActionOf returns "owner/repo" from a uses value "owner/repo@ref".
func toolchainCacheActionOf(uses string) string {
	name, _, _ := strings.Cut(uses, "@")
	return name
}

type toolchainCacheStep struct {
	ID              string         `yaml:"id"`
	If              string         `yaml:"if"`
	Name            string         `yaml:"name"`
	Uses            string         `yaml:"uses"`
	Run             string         `yaml:"run"`
	ContinueOnError bool           `yaml:"continue-on-error"`
	With            map[string]any `yaml:"with"`
}

func (s toolchainCacheStep) withString(key string) string {
	v, _ := s.With[key].(string)
	return v
}

type toolchainCacheWorkflow struct {
	Jobs map[string]struct {
		Steps []toolchainCacheStep `yaml:"steps"`
	} `yaml:"jobs"`
}

type toolchainCacheComposite struct {
	Inputs map[string]struct {
		Default string `yaml:"default"`
	} `yaml:"inputs"`
	Runs struct {
		Using string               `yaml:"using"`
		Steps []toolchainCacheStep `yaml:"steps"`
	} `yaml:"runs"`
}

func readToolchainCacheFile(t *testing.T, path string, into any) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(raw, into); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
}

// Every job that runs a Go command must set Go up through the composite
// action first, after its checkout. Other jobs (no Go command in their steps)
// are not constrained here.
func TestCIToolchainCacheReplacesEverySetupGo(t *testing.T) {
	for _, path := range toolchainCacheWorkflows {
		var w toolchainCacheWorkflow
		readToolchainCacheFile(t, path, &w)
		for name, job := range w.Jobs {
			checkoutAt, setupAt, goAt := -1, -1, -1
			for i, step := range job.Steps {
				switch {
				case strings.HasPrefix(step.Uses, "actions/setup-go"):
					t.Errorf("%s job %q step %d calls %s directly; use %s", path, name, i, step.Uses, toolchainCacheUse)
				case step.Uses == toolchainCacheUse:
					if setupAt < 0 {
						setupAt = i
					}
				case toolchainCacheActionOf(step.Uses) == "actions/checkout":
					if checkoutAt < 0 {
						checkoutAt = i
					}
				}
				if goAt < 0 && toolchainCacheGoCmd.MatchString(step.Run) {
					goAt = i
				}
			}
			if goAt < 0 {
				continue
			}
			switch {
			case setupAt < 0:
				t.Errorf("%s job %q runs a go command but never sets Go up through %s", path, name, toolchainCacheUse)
			case checkoutAt < 0 || checkoutAt > setupAt:
				t.Errorf("%s job %q sets Go up at step %d before its checkout (step %d)", path, name, setupAt, checkoutAt)
			case setupAt > goAt:
				t.Errorf("%s job %q runs a go command at step %d before setting Go up at step %d", path, name, goAt, setupAt)
			}
		}
	}
}

// The composite restores the exact toolchain before setup-go runs, keyed by
// OS, arch and version, and retries setup-go three times with backoff.
func TestCIToolchainCacheCompositeRestoresBeforeSetupGo(t *testing.T) {
	var a toolchainCacheComposite
	readToolchainCacheFile(t, toolchainCacheActionPath, &a)
	if a.Runs.Using != "composite" {
		t.Errorf("runs.using = %q, want composite", a.Runs.Using)
	}
	if v := a.Inputs["go-version"].Default; !toolchainCacheExact.MatchString(v) {
		t.Errorf("go-version default = %q, want an exact x.y.z", v)
	}

	restoreAt, firstSetupAt := -1, -1
	var attempts []toolchainCacheStep
	backoffs := 0
	for i, step := range a.Runs.Steps {
		switch {
		case strings.HasPrefix(step.Uses, "actions/cache@"):
			if restoreAt < 0 &&
				strings.HasPrefix(step.withString("path"), "${{ runner.tool_cache }}/go/") &&
				strings.HasPrefix(step.withString("key"), "setup-go-toolchain-${{ runner.os }}-${{ runner.arch }}-go") {
				restoreAt = i
			}
		case toolchainCacheActionOf(step.Uses) == "actions/setup-go":
			if firstSetupAt < 0 {
				firstSetupAt = i
			}
			attempts = append(attempts, step)
		case strings.Contains(step.Run, "sleep "):
			backoffs++
		}
	}
	if restoreAt < 0 {
		t.Fatalf("%s: no actions/cache step restores ${{ runner.tool_cache }}/go/<version>", toolchainCacheActionPath)
	}
	if restoreAt > firstSetupAt {
		t.Errorf("toolchain restore at step %d runs after setup-go at step %d", restoreAt, firstSetupAt)
	}
	if len(attempts) != 3 {
		t.Fatalf("setup-go attempts = %d, want 3", len(attempts))
	}
	if !attempts[0].ContinueOnError || !attempts[1].ContinueOnError || attempts[2].ContinueOnError {
		t.Errorf("setup-go attempts must continue-on-error on 1 and 2 only, last one fails the step")
	}
	for i, at := range attempts {
		if at.withString("go-version") != "${{ steps.version.outputs.version }}" || at.With["cache"] != true {
			t.Errorf("setup-go attempt %d: want go-version from the composite input and cache: true, got %v", i+1, at.With)
		}
	}
	if backoffs != 2 {
		t.Errorf("backoff steps = %d, want 2 (before attempts 2 and 3)", backoffs)
	}
}

// Actions across .github/ (both workflow files and composite actions) must be
// pinned to a full commit SHA, and each action must appear with only one SHA
// across the repository (hk decision/2026-11-08/ci-toolchain-cache rule 4).
func TestCIToolchainCachePinsActionsToFullSHA(t *testing.T) {
	composites, err := filepath.Glob(".github/actions/*/action.yml")
	if err != nil || len(composites) == 0 {
		t.Fatalf("no composite actions found: %v", err)
	}
	workflows, err := filepath.Glob(".github/workflows/*.yml")
	if err != nil || len(workflows) == 0 {
		t.Fatalf("no workflow files found: %v", err)
	}

	seen := make(map[string]string) // action -> first seen SHA

	check := func(where, uses string) {
		if uses == "" || strings.HasPrefix(uses, "./") || strings.HasPrefix(uses, "docker://") {
			return
		}
		action, ref, ok := strings.Cut(uses, "@")
		if !ok || !toolchainCacheFullSHA.MatchString(ref) {
			t.Errorf("%s: %s is not pinned to a full 40-hex SHA", where, uses)
			return
		}
		if first, exists := seen[action]; exists {
			if first != ref {
				t.Errorf("%s: %s uses SHA %s, but earlier use had %s", where, action, ref, first)
			}
		} else {
			seen[action] = ref
		}
	}

	for _, path := range composites {
		var a toolchainCacheComposite
		readToolchainCacheFile(t, path, &a)
		for _, step := range a.Runs.Steps {
			check(path, step.Uses)
		}
	}
	for _, path := range workflows {
		var w struct {
			Jobs map[string]struct {
				Uses  string               `yaml:"uses"`
				Steps []toolchainCacheStep `yaml:"steps"`
			} `yaml:"jobs"`
		}
		readToolchainCacheFile(t, path, &w)
		for name, job := range w.Jobs {
			if job.Uses != "" {
				check(path+" job "+name, job.Uses)
			}
			for _, step := range job.Steps {
				check(path+" job "+name, step.Uses)
			}
		}
	}
}

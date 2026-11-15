package fleet

// hk 4270 (decision 152): the spawn input mistakes that used to surface as a
// SPAWN_FAILED inbox event after the hub accepted the request fail at submit,
// in the caller's terminal: agent name, task title marker, task claim and
// host eligibility. hk being unreachable never blocks a submit.

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSubmitValidAgentNameFollowsHerdrRule_t4270(t *testing.T) {
	for label, want := range map[string]bool{
		"a": true, "mwr-2369-78e42fc-p2-1794465105": true, "worker_1-b": true, strings.Repeat("a", 32): true,
		"": false, strings.Repeat("a", 33): false, "Worker": false, "1worker": false, "-worker": false, "_w": false,
		"w.1": false, "w 1": false, "wörker": false, "w/1": false,
	} {
		if got := ValidAgentName(label); got != want {
			t.Errorf("ValidAgentName(%q)=%t, want %t", label, got, want)
		}
	}
}

// submitPreflightFixture is a hub submission for task 42 whose hk answer is task.
func submitPreflightFixture(t *testing.T, label, task string, extra ...string) (*App, Spawn) {
	t.Helper()
	a, s, _, _, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	a.Config.Local["machine_id"] = "source-a"
	if err := os.MkdirAll(s.CWD, 0700); err != nil {
		t.Fatal(err)
	}
	brief := filepath.Join(a.Home, "preflight-brief.md")
	if err := os.WriteFile(brief, []byte("brief"), 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"-c", s.CWD, "-p", brief, "-l", label, "--job", "job-a", "--t", "T1", "--task", "42", "--host", "h1", "--override-reason", "fixture", "--owner", "panewire-design", "--containment", "hk:12"}
	parsed, err := parseSpawn(append(args, extra...))
	if err != nil {
		t.Fatal(err)
	}
	if task != "" {
		useFakeHK(t).SetTask("42", task)
	}
	return a, parsed
}

func taskJSON(fields map[string]any) string {
	fields["id"] = "42"
	b, _ := json.Marshal(fields)
	return string(b)
}

func TestSubmitPrepareRefusesWhatTheNodeWouldRefuseLater_t4270(t *testing.T) {
	good := map[string]any{"state": "developing", "title": "task (grade A, T1)", "project": "panewire"}
	with := func(k string, v any) map[string]any {
		m := map[string]any{}
		for key, val := range good {
			m[key] = val
		}
		m[k] = v
		return m
	}
	for _, tc := range []struct {
		name, label string
		task        map[string]any
		want        string
	}{
		{"agent name too long", strings.Repeat("a", 33), good, "not a valid agent name"},
		{"agent name uppercase", "Job-A", good, "not a valid agent name"},
		{"title without marker", "job-a", with("title", "task without a marker"), "(grade X, Tn"},
		{"title scale differs from --t", "job-a", with("title", "task (grade A, T2)"), "scale does not match"},
		{"task parked", "job-a", with("state", "hold"), "task unavailable: hold"},
		{"task merged", "job-a", with("state", "merged"), "task unavailable: merged"},
		{"task bound to another job", "job-a", with("refs", map[string]any{"job_id": "someone-else"}), "bound to another job"},
		{"task has another claimant", "job-a", with("claimed_by", "other-lane"), "another claimant"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s := submitPreflightFixture(t, tc.label, taskJSON(tc.task))
			_, err := a.PrepareSubmission(context.Background(), s)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("PrepareSubmission err=%v, want a refusal naming %q", err, tc.want)
			}
		})
	}

	t.Run("a good request is accepted", func(t *testing.T) {
		a, s := submitPreflightFixture(t, "job-a", taskJSON(good))
		if _, err := a.PrepareSubmission(context.Background(), s); err != nil {
			t.Fatalf("PrepareSubmission refused a good request: %v", err)
		}
	})
	t.Run("a task hk does not have", func(t *testing.T) {
		a, s := submitPreflightFixture(t, "job-a", "")
		useFakeHK(t).Handle(hkFails("GET /v1/tasks/42", http.StatusNotFound, `{"error":"not_found"}`))
		if _, err := a.PrepareSubmission(context.Background(), s); err == nil || !strings.Contains(err.Error(), "task does not exist") {
			t.Fatalf("err=%v, want task does not exist", err)
		}
	})
	t.Run("hk down does not block the submit", func(t *testing.T) {
		a, s := submitPreflightFixture(t, "job-a", taskJSON(good))
		useFakeHK(t).Handle(hkFails("GET /v1/tasks/42", http.StatusBadGateway, `{"error":"bad_gateway"}`))
		if _, err := a.PrepareSubmission(context.Background(), s); err != nil {
			t.Fatalf("an hk outage blocked the submit: %v", err)
		}
	})
}

func TestSubmitHostRefusalNamesTheMissingCapability_t4270(t *testing.T) {
	hosts := map[string]map[string]any{
		"h1": {"via": "hub", "tags": []any{"gpu"}},
		"h2": {"via": "hub", "tags": []any{"unity"}},
	}
	a, _, _, _, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hosts)
	a.Config.Local["machine_id"] = "source-a"
	for _, tc := range []struct {
		name string
		s    Spawn
		want string // "" accepts
	}{
		{"no constraint", Spawn{}, ""},
		{"every host excluded", Spawn{ExcludeHosts: "h1,h2,source-a"}, "no host is eligible"},
		{"a host declares the cap", Spawn{Tags: "unity"}, ""},
		{"no host declares the cap", Spawn{Tags: "ios"}, "no host declares cap ios"},
		{"no single host has both", Spawn{Tags: "gpu,unity"}, "no single host declares all caps"},
		{"pinned host has the cap", Spawn{Tags: "unity", Host: "h2"}, ""},
		{"pinned host lacks the cap", Spawn{Tags: "unity", Host: "h1"}, "host h1 does not declare cap unity"},
		{"the only cap holder is excluded", Spawn{Tags: "unity", ExcludeHosts: "h2"}, "no host declares cap unity"},
		{"pinned and excluded", Spawn{Host: "h1", ExcludeHosts: "h1"}, "both pinned and excluded"},
		{"pinned host the config does not list", Spawn{Tags: "unity", Host: "node-only"}, ""},
		{"unlisted pinned host that is also excluded", Spawn{Host: "node-only", ExcludeHosts: "node-only"}, "both pinned and excluded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := a.SubmitHostRefusal(tc.s)
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("refused: %v", err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), "NO_ELIGIBLE_HOST") || !strings.Contains(err.Error(), tc.want)):
				t.Fatalf("err=%v, want NO_ELIGIBLE_HOST naming %q", err, tc.want)
			}
		})
	}
}

// A review pass of a claimed task names the builder, and the node accepts the
// task bound to that builder; the submit must not refuse what the node takes.
func TestSubmitPrepareAcceptsReviewPassOfBuilderBoundTask_t4270(t *testing.T) {
	task := taskJSON(map[string]any{"state": "developing", "title": "task (grade A, T1)", "claimed_by": "builder-label", "refs": map[string]any{"job_id": "builder-1"}})
	a, s := submitPreflightFixture(t, "job-a", task, "--role", "reviewer", "--author-family", "openai", "--builder-job", "builder-1", "--report-relative", "job-a/report.md")
	if err := a.submitPreflight(context.Background(), s); err != nil {
		t.Fatalf("a review pass of the builder's task was refused: %v", err)
	}
	// Without the builder the same task is bound to another job.
	a, s = submitPreflightFixture(t, "job-a", task)
	if err := a.submitPreflight(context.Background(), s); err == nil || !strings.Contains(err.Error(), "bound to another job") {
		t.Fatalf("err=%v, want the bound-to-another-job refusal", err)
	}
}

// Replaying an accepted submission reaches the hub's idempotent lookup even
// when the task has moved on since (merged, rebound, retitled).
func TestSubmitPrepareReplayIgnoresLaterTaskState_t4270(t *testing.T) {
	good := taskJSON(map[string]any{"state": "developing", "title": "task (grade A, T1)"})
	a, s := submitPreflightFixture(t, "job-a", good)
	request, err := a.PrepareSubmission(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.saveSubmission(request); err != nil {
		t.Fatal(err)
	}
	useFakeHK(t).SetTask("42", taskJSON(map[string]any{"state": "merged", "title": "task (grade A, T1)"}))
	if _, err = a.PrepareSubmission(context.Background(), s); err != nil {
		t.Fatalf("the replay of an accepted request was refused: %v", err)
	}
	// A different job id with the merged task is still refused.
	s.Job, s.Label = "job-b", "job-b"
	if _, err = a.PrepareSubmission(context.Background(), s); err == nil || !strings.Contains(err.Error(), "task unavailable") {
		t.Fatalf("err=%v, want a fresh job refused on the merged task", err)
	}
}

// A counsel is a consult and needs no title marker, and a reviewer that names
// only the author's family takes any task: the node accepts both, so the
// submit must too.
func TestSubmitPrepareAcceptsWhatTheNodeExempts_t4270(t *testing.T) {
	merged := taskJSON(map[string]any{"state": "merged", "title": "task (grade A, T1)"})
	for name, extra := range map[string][]string{
		"consult":                   {"--role", "consult"},
		"reviewer with family only": {"--role", "reviewer", "--author-family", "openai"},
	} {
		t.Run(name, func(t *testing.T) {
			task := merged
			if name == "consult" {
				task = taskJSON(map[string]any{"state": "developing", "title": "task without a marker"})
			}
			a, s := submitPreflightFixture(t, "job-a", task, extra...)
			if err := a.submitPreflight(context.Background(), s); err != nil {
				t.Fatalf("refused what the node accepts: %v", err)
			}
		})
	}
}

// With local spawning off and no remote host configured, even an
// unconstrained request has no host to run on.
func TestSubmitHostRefusalWhenNoHostCanSpawn_t4270(t *testing.T) {
	a, _, _, _, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", map[string]map[string]any{})
	a.Config.Local["machine_id"] = "source-a"
	a.Config.Local["spawn"] = false
	if err := a.SubmitHostRefusal(Spawn{}); err == nil || !strings.Contains(err.Error(), "NO_ELIGIBLE_HOST") {
		t.Fatalf("err=%v, want NO_ELIGIBLE_HOST", err)
	}
}

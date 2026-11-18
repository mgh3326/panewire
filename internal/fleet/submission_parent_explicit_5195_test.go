package fleet

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// parentExplicitFixture is a hub submission with no --task whose parent job
// resolves locally, so the only variable is how the parent reached the spawn.
func parentExplicitFixture(t *testing.T, extra ...string) (*App, Spawn) {
	t.Helper()
	a, s, _, _, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	a.Config.Local["machine_id"] = "source-a"
	if err := os.MkdirAll(s.CWD, 0700); err != nil {
		t.Fatal(err)
	}
	brief := filepath.Join(a.Home, "parent-brief.md")
	if err := os.WriteFile(brief, []byte("brief"), 0600); err != nil {
		t.Fatal(err)
	}
	putEvent(t, a, "parent-job", "job.claim", map[string]any{"owner_lane": "panewire-design", "task_id": "42", "t_level": "T1", "role": "implementer"})
	args := append([]string{"-c", s.CWD, "-p", brief, "-l", "job-a", "--job", "job-a", "--t", "T1", "--host", "h1", "--override-reason", "fixture", "--owner", "panewire-design", "--containment", "hk:12"}, extra...)
	parsed, err := parseSpawn(args)
	if err != nil {
		t.Fatal(err)
	}
	return a, parsed
}

// The submission records whether --parent-job was typed on the command line;
// a parent filled from ARBITER_JOB is not explicit, though both send the same
// --parent-job argument to the hub (hk 5195).
func TestSubmissionRecordsWhetherParentJobWasExplicit_5195(t *testing.T) {
	t.Run("flag", func(t *testing.T) {
		t.Setenv("ARBITER_JOB", "")
		a, s := parentExplicitFixture(t, "--parent-job", "parent-job")
		request, err := a.PrepareSubmission(context.Background(), s)
		if err != nil {
			t.Fatal(err)
		}
		if !request.ParentJobExplicit || !slices.Contains(request.Args, "parent-job") {
			t.Fatalf("explicit=%v args=%v", request.ParentJobExplicit, request.Args)
		}
	})
	t.Run("ARBITER_JOB", func(t *testing.T) {
		t.Setenv("ARBITER_JOB", "parent-job")
		a, s := parentExplicitFixture(t)
		request, err := a.PrepareSubmission(context.Background(), s)
		if err != nil {
			t.Fatal(err)
		}
		if request.ParentJobExplicit {
			t.Fatal("a parent filled from ARBITER_JOB must not be explicit")
		}
		if i := slices.Index(request.Args, "--parent-job"); i < 0 || request.Args[i+1] != "parent-job" {
			t.Fatalf("the hub still gets the inherited parent: %v", request.Args)
		}
	})
	t.Run("an empty flag falls back to ARBITER_JOB and is not explicit", func(t *testing.T) {
		t.Setenv("ARBITER_JOB", "parent-job")
		a, s := parentExplicitFixture(t, "--parent-job", "")
		request, err := a.PrepareSubmission(context.Background(), s)
		if err != nil {
			t.Fatal(err)
		}
		if request.ParentJobExplicit {
			t.Fatal("an empty --parent-job names nothing; the inherited parent is not explicit")
		}
	})
	t.Run("the wire field", func(t *testing.T) {
		t.Setenv("ARBITER_JOB", "")
		a, s := parentExplicitFixture(t, "--parent-job", "parent-job")
		request, err := a.PrepareSubmission(context.Background(), s)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ValidateSubmission(request); err != nil {
			t.Fatalf("an explicit request is valid: %v", err)
		}
		for i, arg := range request.Args {
			if arg == "--parent-job" {
				request.Args = slices.Delete(request.Args, i, i+2)
				break
			}
		}
		request.Args = append(request.Args, "--task", "42")
		if _, err := ValidateSubmission(request); err == nil {
			t.Fatal("parent_job_explicit without a --parent-job must be refused")
		}
	})
}

package fleet

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestQuotaE6EnvUsesPanewireName(t *testing.T) {
	if got := quotaE6Env("omp-dsflash41@high"); !slices.Equal(got, []string{"PANEWIRE_E6_ARM=omp-dsflash41@high"}) {
		t.Fatalf("quota E6 env = %v", got)
	}
}

// prepareE6Submission runs the submitter half over the hub fixture: a linked
// worktree on a pushed branch, an explicit profile and the marker named by the
// environment.
func prepareE6Submission(t *testing.T, profile, arm string) (Submission, error) {
	t.Helper()
	a, s, _, _, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	a.Config.Local["machine_id"] = "source-a"
	if err := os.MkdirAll(s.CWD, 0700); err != nil {
		t.Fatal(err)
	}
	s.Prompt = filepath.Join(a.Home, "submission-brief.md")
	if err := os.WriteFile(s.Prompt, []byte("brief marker"), 0600); err != nil {
		t.Fatal(err)
	}
	s.Label, s.T, s.Owner, s.Host, s.OverrideReason, s.Containment = "job-a", "T1", "parent", "auto", "fixture", "hk:12"
	s.Profile = profile
	t.Setenv("PANEWIRE_E6_ARM", arm)
	return a.PrepareSubmission(t.Context(), s)
}

func TestSubmissionCarriesE6Arm(t *testing.T) {
	t.Run("matching marker", func(t *testing.T) {
		req, err := prepareE6Submission(t, "sonnet", "sonnet@high")
		if err != nil {
			t.Fatal(err)
		}
		if req.E6Arm != "sonnet@high" {
			t.Fatalf("submission e6_arm=%q", req.E6Arm)
		}
		parsed, err := ValidateSubmission(req)
		if err != nil {
			t.Fatal(err)
		}
		if parsed.E6Arm != "sonnet@high" || parsed.Profile != "sonnet" {
			t.Fatalf("hub spawn e6_arm=%q profile=%q", parsed.E6Arm, parsed.Profile)
		}
	})
	t.Run("no marker", func(t *testing.T) {
		req, err := prepareE6Submission(t, "sonnet", "")
		if err != nil {
			t.Fatal(err)
		}
		if req.E6Arm != "" {
			t.Fatalf("submission e6_arm=%q", req.E6Arm)
		}
		parsed, err := ValidateSubmission(req)
		if err != nil || parsed.E6Arm != "" {
			t.Fatalf("hub spawn e6_arm=%q err=%v", parsed.E6Arm, err)
		}
	})
	t.Run("marker for another profile", func(t *testing.T) {
		_, err := prepareE6Submission(t, "sonnet", "opus@high")
		if ErrorCode(err) != exitUsage || !strings.Contains(err.Error(), "sonnet") {
			t.Fatalf("marker for another profile was not refused at submit: %v", err)
		}
	})
}

// e6Quota refuses a catalog resolution that lacks the exact experiment marker,
// as pacelever's policy launch does for an unrated rung.
type e6Quota struct {
	*fakeQuota
	require  string
	launched []QuotaRequest
}

func (q *e6Quota) Launch(ctx context.Context, r QuotaRequest) (Launch, error) {
	q.launched = append(q.launched, r)
	if r.E6Arm != q.require {
		return Launch{}, fail(exitConfig, "quota catalog: dependency failed")
	}
	return q.fakeQuota.Launch(ctx, r)
}

func e6AdmissionSubmission(t *testing.T, arm string) Submission {
	t.Helper()
	s := Spawn{CWD: "/work/project", Profile: "omp-dsflash41", Prompt: "/work/brief.md", Job: "job-a", Label: "job-a", T: "T1", Task: "42", Role: "implementer", Owner: "parent", Host: "auto", Effort: "high", Containment: "hk:12"}
	return Submission{
		RequestID:       "00000000-0000-4000-8000-000000000001",
		Args:            submissionArgs(s),
		Brief:           "brief",
		Repository:      Repository{Name: "repo-a", Origin: "https://git.invalid/org/repo-a.git", LocalRoot: "/work/project"},
		OriginMachine:   "source-a",
		DeadlineSeconds: 600,
		E6Arm:           arm,
	}
}

// TestHubAdmissionUsesSubmittedE6Arm is acceptance (a) and its mutant: the
// submission field is the only source of the marker at the hub, and the
// catalog step refuses an unrated rung without it.
func TestHubAdmissionUsesSubmittedE6Arm(t *testing.T) {
	a := testApp(t)
	a.Config.Local = map[string]any{"omp_commandcode": true}
	seat := a.Policy.Profiles["omp-dsflash41"]
	q := &e6Quota{
		fakeQuota: &fakeQuota{
			LaunchValue: Launch{Profile: "omp-dsflash41", ModelID: "provider/example-model", Effort: "high", Grade: "A+", Gate: "default", Pool: "example"},
			GateDo:      func(QuotaRequest) (string, error) { return "pool=example profile=omp-dsflash41\n", nil },
		},
		require: "omp-dsflash41@high",
	}
	a.Quota = q

	request := e6AdmissionSubmission(t, "omp-dsflash41@high")
	parsed, err := ValidateSubmission(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := a.admitLaunch(t.Context(), parsed, seat, "high", "high", "B", "", false); err != nil {
		t.Fatalf("hub admission refused the carried marker: %v", err)
	}
	if len(q.launched) == 0 || q.launched[0].E6Arm != "omp-dsflash41@high" {
		t.Fatalf("catalog step did not receive the marker: %+v", q.launched)
	}

	mutant := request
	mutant.E6Arm = ""
	parsed, err = ValidateSubmission(mutant)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := a.admitLaunch(t.Context(), parsed, seat, "high", "high", "B", "", false); err == nil {
		t.Fatal("dropping the submission field still admitted the unrated rung")
	} else if !strings.Contains(err.Error(), "dependency failed") && !strings.Contains(err.Error(), "requires PANEWIRE_E6_ARM") {
		t.Fatalf("mutant refused for another reason: %v", err)
	}
}

// TestSpawnRecordsE6Arm is acceptance (d): the marker reaches the job record,
// and a spawn with no marker records none.
func TestSpawnRecordsE6Arm(t *testing.T) {
	for _, tc := range []struct{ name, arm string }{{"carried", "cmd-dsflash41@high"}, {"none", ""}} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, _, _ := cmdFixture(t)
			s.E6Arm = tc.arm
			if err := a.Spawn(t.Context(), s); err != nil {
				t.Fatal(err)
			}
			job, err := a.durable(t.Context(), s.Job)
			if err != nil {
				t.Fatal(err)
			}
			if got := str(job.Receipt, "e6_arm"); got != tc.arm {
				t.Fatalf("job record e6_arm=%q want %q", got, tc.arm)
			}
			if _, ok := job.Receipt["e6_arm"]; ok && tc.arm == "" {
				t.Fatal("a spawn with no marker recorded an e6_arm")
			}
		})
	}
}

// TestCLIQuotaPassesE6ArmToPacelever keeps the catalog and gate steps reading
// the carried marker from the environment a local spawn would have supplied.
func TestCLIQuotaPassesE6ArmToPacelever(t *testing.T) {
	f := &fakeRunner{Do: func(Command) Result {
		return Result{Out: `{"profile":"omp-dsflash41","model_id":"provider/example-model","effort":"high","grade":"A+","gate":"default","pool":"example"}`}
	}}
	q := CLIQuota{Runner: f, Binary: "pacelever"}
	if _, err := q.Launch(context.Background(), QuotaRequest{Profile: "omp-dsflash41", Effort: "high", E6Arm: "omp-dsflash41@high"}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Gate(context.Background(), QuotaRequest{Profile: "omp-dsflash41", Effort: "high", E6Arm: "omp-dsflash41@high"}); err != nil {
		t.Fatal(err)
	}
	if len(f.Calls) != 2 {
		t.Fatalf("pacelever calls=%d", len(f.Calls))
	}
	for _, call := range f.Calls {
		if !slices.Contains(call.Env, "PANEWIRE_E6_ARM=omp-dsflash41@high") {
			t.Fatalf("%v did not receive the marker: %v", call.Args, call.Env)
		}
	}
}

// TestHubArgsCarryE6Arm keeps the node launch on the same marker: the hub's
// allowlisted argv carries it and the node parses it back.
func TestHubArgsCarryE6Arm(t *testing.T) {
	args, err := hubArgs("/jobs", Spawn{E6Arm: "sonnet@high"})
	if err != nil {
		t.Fatal(err)
	}
	i := slices.Index(args, "--e6-arm")
	if i < 0 || i+1 >= len(args) || args[i+1] != "sonnet@high" {
		t.Fatalf("hub args do not carry the marker: %v", args)
	}
	parsed, err := parseSpawn(args)
	if err != nil || parsed.E6Arm != "sonnet@high" {
		t.Fatalf("node spawn e6_arm=%q err=%v", parsed.E6Arm, err)
	}
}

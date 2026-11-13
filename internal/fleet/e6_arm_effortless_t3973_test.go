package fleet

import (
	"context"
	"strings"
	"testing"
)

// hk 3973: an unrated rung whose catalog effort is empty is armed by exactly
// <canon>@ (nothing after the @). Submission, the hub, the node and the seat
// gate read one validator for the form of the marker; the gate decides whether
// the rung it names is armed.

// TestE6ArmValidatorEffortlessRung_t3973 pins the form rule: <canon>@ and
// <canon>@<effort> are well formed; a missing canon, a foreign canon, a second
// @, whitespace and an empty canon are not.
func TestE6ArmValidatorEffortlessRung_t3973(t *testing.T) {
	for _, tc := range []struct {
		arm, canon string
		want       bool
	}{
		{"devin-ds41-max@", "devin-ds41-max", true},
		{"devin-ds41-max@high", "devin-ds41-max", true},
		{"omp-dsflash41@high", "omp-dsflash41", true},
		{"devin-ds41-max", "devin-ds41-max", false},
		{"@", "", false},
		{"@high", "", false},
		{"other@", "devin-ds41-max", false},
		{"devin-ds41-max@@", "devin-ds41-max", false},
		{"devin-ds41-max@a@b", "devin-ds41-max", false},
		{"devin-ds41-max@ ", "devin-ds41-max", false},
		{"devin-ds41-max@ high", "devin-ds41-max", false},
		{"devin-ds41-max@high\n", "devin-ds41-max", false},
		{"devin-ds41-max@../x", "devin-ds41-max", false},
		{"", "devin-ds41-max", false},
	} {
		if got := ValidE6Arm(tc.arm, tc.canon); got != tc.want {
			t.Errorf("ValidE6Arm(%q, %q)=%v want %v", tc.arm, tc.canon, got, tc.want)
		}
	}
}

// TestEffortlessRungArmedAtSubmitAndSeatGate_t3973 is acceptance (a): a pinned
// worker spawn of the unrated effortless devin rung with PANEWIRE_E6_ARM=<canon>@
// passes submission, the hub's validation of the carried request, and the seat gate.
func TestEffortlessRungArmedAtSubmitAndSeatGate_t3973(t *testing.T) {
	const canon = "devin-ds41-max"
	const arm = canon + "@"

	req, err := prepareE6Submission(t, canon, arm)
	if err != nil {
		t.Fatalf("submission refused the effortless arm: %v", err)
	}
	if req.E6Arm != arm {
		t.Fatalf("submission e6_arm=%q want %q", req.E6Arm, arm)
	}
	parsed, err := ValidateSubmission(req)
	if err != nil {
		t.Fatalf("hub validation refused the carried arm: %v", err)
	}
	if parsed.E6Arm != arm || parsed.Profile != canon {
		t.Fatalf("hub spawn e6_arm=%q profile=%q", parsed.E6Arm, parsed.Profile)
	}

	a := testApp(t)
	seat := a.Policy.Profiles[canon]
	unrated := Launch{ModelID: "provider/example-model", Effort: "", Grade: "unrated", Gate: "default"}
	if err := seat.Check(unrated, "implementer", "B", "", arm, nil); err != nil {
		t.Fatalf("seat gate refused the armed effortless rung: %v", err)
	}
}

// TestEffortlessRungRefusesOtherArms_t3973 is acceptance (b): an effortless rung
// is not armed by canon@<effort>, and an effort rung is not armed by canon@.
// The seat gate refuses both and names the marker the rung needs.
func TestEffortlessRungRefusesOtherArms_t3973(t *testing.T) {
	a := testApp(t)
	seat := a.Policy.Profiles["devin-ds41-max"]
	unrated := Launch{ModelID: "provider/example-model", Effort: "", Grade: "unrated", Gate: "default"}
	for _, arm := range []string{"", "devin-ds41-max@high", "devin-ds41-max@low"} {
		err := seat.Check(unrated, "implementer", "B", "", arm, nil)
		if code(err) != exitSeatRefused {
			t.Errorf("effortless rung with arm %q: code=%d err=%v", arm, code(err), err)
		}
	}
	err := seat.Check(unrated, "implementer", "B", "", "", nil)
	if err == nil || !strings.Contains(err.Error(), "requires PANEWIRE_E6_ARM=devin-ds41-max@") {
		t.Fatalf("effortless rung without arm did not name its marker: %v", err)
	}

	effortRung := Seat{Canon: "omp-dsflash41", Kind: "omp", Family: "deepseek", Capability: "standard", Roles: []string{"implementer"}, E6: true}
	high := Launch{ModelID: "provider/example-model", Effort: "high", Grade: "unrated", Gate: "default"}
	if err := effortRung.Check(high, "implementer", "B", "", "omp-dsflash41@", nil); code(err) != exitSeatRefused {
		t.Fatalf("effort rung armed by bare canon@: code=%d err=%v", code(err), err)
	}
	if err := effortRung.Check(high, "implementer", "B", "", "omp-dsflash41@high", nil); err != nil {
		t.Fatalf("effort rung with its own arm refused: %v", err)
	}
}

// TestEffortlessRungSubmissionRefusesForeignArms_t3973 keeps the malformed arms
// refused at submit, including the effortless form against another canon.
func TestEffortlessRungSubmissionRefusesForeignArms_t3973(t *testing.T) {
	for _, arm := range []string{"other@", "devin-ds41-max", "devin-ds41-max@ ", "devin-ds41-max@@"} {
		_, err := prepareE6Submission(t, "devin-ds41-max", arm)
		if ErrorCode(err) != exitUsage {
			t.Errorf("submission arm %q: code=%d err=%v; want exitUsage", arm, ErrorCode(err), err)
		}
	}
}

// TestEffortlessRungDryRunAndHubAdmissionAgree_t3973 is acceptance (c): the dry-run
// and the hub's admission of the same spawn give the same verdict for the armed
// effortless rung (accept) and for the rung armed at an effort (refuse).
func TestEffortlessRungDryRunAndHubAdmissionAgree_t3973(t *testing.T) {
	const canon = "devin-ds41-max"
	for _, tc := range []struct {
		arm    string
		accept bool
	}{
		{canon + "@", true},
		{canon + "@high", false},
		{"", false},
	} {
		t.Run("arm="+tc.arm, func(t *testing.T) {
			launch := Launch{Profile: canon, ModelID: "provider/example-model", Effort: "", Grade: "unrated", Gate: "default", Pool: "example"}
			t.Setenv("PANEWIRE_E6_ARM", tc.arm)

			// Dry-run: the local preview on the spawn path.
			dry, s, f, _ := cmdFixture(t)
			dry.Quota = &fakeQuota{LaunchValue: launch, GateDo: func(QuotaRequest) (string, error) { return "pool=example profile=" + canon + "\n", nil }}
			s.Profile, s.Effort, s.Host, s.DryRun = canon, "", "local", true
			dryErr := dry.Spawn(context.Background(), s)
			for _, c := range f.Calls {
				if strings.HasPrefix(strings.Join(c.Args, " "), "tab create") {
					t.Fatalf("dry run created a pane: %v", c.Args)
				}
			}

			// Hub admission of the same arm, as the submission carries it.
			hub := testApp(t)
			hub.Config.Local = map[string]any{}
			seat := hub.Policy.Profiles[canon]
			q := &e6Quota{fakeQuota: &fakeQuota{LaunchValue: launch, GateDo: func(QuotaRequest) (string, error) { return "pool=example profile=" + canon + "\n", nil }}, require: tc.arm}
			hub.Quota = q
			request := effortlessSubmission(tc.arm)
			parsed, err := ValidateSubmission(request)
			if err != nil {
				t.Fatalf("hub validation refused: %v", err)
			}
			_, _, hubErr := hub.admitLaunch(context.Background(), parsed, seat, "", "", "B", "", false)

			if (dryErr == nil) != tc.accept || (hubErr == nil) != tc.accept {
				t.Fatalf("verdicts disagree or differ from want=%v: dry-run err=%v hub err=%v", tc.accept, dryErr, hubErr)
			}
		})
	}
}

// effortlessSubmission is the hub request for an effortless pinned worker spawn.
func effortlessSubmission(arm string) Submission {
	s := Spawn{CWD: "/work/project", Profile: "devin-ds41-max", Prompt: "/work/brief.md", Job: "job-a", Label: "job-a", T: "T1", Task: "42", Role: "implementer", Owner: "parent", Host: "auto", Containment: "hk:12"}
	return Submission{
		RequestID:       "00000000-0000-4000-8000-000000000002",
		Args:            submissionArgs(s),
		Brief:           "brief",
		Repository:      Repository{Name: "repo-a", Origin: "https://git.invalid/org/repo-a.git", LocalRoot: "/work/project"},
		OriginMachine:   "source-a",
		DeadlineSeconds: 600,
		E6Arm:           arm,
	}
}

package fleet

import (
	"slices"
	"testing"
)

func TestParseSpawnReviewPass(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		pass string
		ok   bool
	}{
		{"reviewer p1", []string{"--role", "reviewer", "--review-pass", "1"}, "1", true},
		{"reviewer p9", []string{"--role", "reviewer", "--review-pass", "9"}, "9", true},
		{"no flag", []string{"--role", "reviewer"}, "", true},
		{"implementer", []string{"--role", "implementer", "--review-pass", "1"}, "", false},
		{"default role", []string{"--review-pass", "1"}, "", false},
		{"consult", []string{"--role", "consult", "--review-pass", "1"}, "", false},
		{"zero", []string{"--role", "reviewer", "--review-pass", "0"}, "", false},
		{"ten", []string{"--role", "reviewer", "--review-pass", "10"}, "", false},
		{"label form", []string{"--role", "reviewer", "--review-pass", "p1"}, "", false},
		{"empty", []string{"--role", "reviewer", "--review-pass", ""}, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := parseSpawn(tc.args)
			if tc.ok != (err == nil) || err != nil && exitCode(err) != exitUsage {
				t.Fatalf("err=%v exit=%d, want ok=%v", err, exitCode(err), tc.ok)
			}
			if tc.ok && s.ReviewPass != tc.pass {
				t.Fatalf("ReviewPass=%q want %q", s.ReviewPass, tc.pass)
			}
		})
	}
}

func TestSpawnRoleKey(t *testing.T) {
	for _, tc := range []struct {
		s    Spawn
		want string
	}{
		{Spawn{Task: "42", Role: "implementer"}, "42:implementer"},
		{Spawn{Task: "42", Role: "reviewer"}, "42:reviewer"},
		{Spawn{Task: "42", Role: "reviewer", ReviewPass: "1"}, "42:reviewer:p1"},
		{Spawn{Task: "42", Role: "reviewer", ReviewPass: "2"}, "42:reviewer:p2"},
	} {
		if got := tc.s.RoleKey(); got != tc.want {
			t.Errorf("RoleKey(%+v)=%q want %q", tc.s, got, tc.want)
		}
	}
}

// The pass survives both wire renderings and the hub's parse of them.
func TestReviewPassCrossesWires(t *testing.T) {
	s := Spawn{Profile: "auto", Job: "rev", Label: "rev", T: "T1", Owner: "o", Role: "reviewer", Task: "42", ReviewPass: "2"}
	sub := submissionArgs(s)
	if i := slices.Index(sub, "--review-pass"); i < 0 || sub[i+1] != "2" {
		t.Fatalf("submissionArgs=%v", sub)
	}
	parsed, err := parseSpawn(sub)
	if err != nil || parsed.ReviewPass != "2" {
		t.Fatalf("round trip: %+v %v", parsed, err)
	}
	args, err := hubArgs("/jobs", s)
	if err != nil {
		t.Fatal(err)
	}
	if i := slices.Index(args, "--review-pass"); i < 0 || args[i+1] != "2" {
		t.Fatalf("hubArgs=%v", args)
	}
	s.ReviewPass = ""
	if args, _ = hubArgs("/jobs", s); slices.Contains(args, "--review-pass") {
		t.Fatalf("hubArgs without a pass sent the flag: %v", args)
	}
}

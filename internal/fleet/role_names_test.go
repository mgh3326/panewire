package fleet

import (
	"context"
	"strings"
	"testing"
)

// The spawn roles are implementer, reviewer and consult (hk 2875, decision
// 209): one vocabulary for the seat, the routing table and the job records.
// The names before the rename are no roles and no alias maps them.
func TestSpawnRolesAreImplementerReviewerConsult(t *testing.T) {
	for _, role := range []string{"worker", "tester", "counsel", "captain", "lead", "explorer"} {
		t.Run("refuses "+role, func(t *testing.T) {
			a, s, _, _ := cmdFixture(t)
			s.Role = role
			err := a.Spawn(context.Background(), s)
			if exitCode(err) != exitUsage || err == nil || !strings.Contains(err.Error(), "invalid job, label or role") {
				t.Fatalf("role %s: want a usage refusal naming the role, got %v", role, err)
			}
		})
	}
	if got := strings.Join(spawnRoles, ","); got != "implementer,reviewer,consult" {
		t.Fatalf("spawn roles %q", got)
	}
	s, err := parseSpawn(nil)
	if err != nil || s.Role != "implementer" {
		t.Fatalf("the default spawn role is implementer: %q %v", s.Role, err)
	}
}

// Every retired flag and role value is a usage error on the CLI surface that
// took it, and --author-family replaces --builder-family on each of them.
func TestRetiredRoleFlagsAreUsageErrors(t *testing.T) {
	for name, args := range map[string][]string{
		"spawn --builder-family":   {"spawn", "--builder-family", "anthropic", "--task", "12", "--role", "reviewer"},
		"spawn --routing-role":     {"spawn", "--routing-role", "consult"},
		"plan --builder-family":    {"plan", "--grade", "A", "--t", "T1", "--role", "reviewer", "--builder-family", "anthropic"},
		"plan --routing-role":      {"plan", "--grade", "A", "--t", "T1", "--routing-role", "consult"},
		"plan --role worker":       {"plan", "--grade", "A", "--t", "T1", "--role", "worker"},
		"plan --role counsel":      {"plan", "--t", "T1", "--role", "counsel"},
		"resolve --builder-family": {"resolve", "--role", "reviewer", "--builder-family", "anthropic"},
		"reap --include-captains":  {"reap", "--include-captains"},
		"delegate":                 {"delegate", "--role", "reader", "--prompt", "brief.md"},
	} {
		t.Run(name, func(t *testing.T) {
			a := testApp(t)
			a.Runner = &fakeRunner{}
			if err := a.Run(context.Background(), args); exitCode(err) != exitUsage {
				t.Fatalf("%v: want a usage error, got %v", args, err)
			}
		})
	}
	s, err := parseSpawn([]string{"--author-family", "anthropic", "--task", "12", "--role", "reviewer"})
	if err != nil || s.AuthorFamily != "anthropic" {
		t.Fatalf("--author-family on spawn: %+v %v", s, err)
	}
}

// A reviewer whose only admissible seat shares the author's family is refused
// with exit 77 (seat refusal). Nothing retries it as another role: no pane is
// created and no task is claimed.
func TestReviewerSeatRefusalIsNotRetriedAsAnotherRole(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	s.Role, s.ParentFamily = "reviewer", "deepseek" // cmd-dsflash41 is a deepseek seat
	err := a.Spawn(context.Background(), s)
	if exitCode(err) != exitSeatRefused {
		t.Fatalf("want exit %d, got %v", exitSeatRefused, err)
	}
	for _, c := range f.Calls {
		if c.Name == "herdr" && len(c.Args) > 1 && c.Args[0] == "tab" && c.Args[1] == "create" {
			t.Fatalf("a refused reviewer reached pane creation: %v", c.Args)
		}
	}
	if n := claimCalls(t); n != 0 {
		t.Fatalf("a refused reviewer claimed the task %d times", n)
	}
}

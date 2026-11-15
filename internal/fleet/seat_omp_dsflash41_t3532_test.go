package fleet

import (
	"slices"
	"testing"
)

// hk 3532: omp-dsflash41 is a regular seat. Its catalog row is graded A+, so
// a launch needs no E6 arm, and the omp_commandcode capability still gates it.
func TestOmpDsflash41RegularSeatNeedsNoArm(t *testing.T) {
	p, e := LoadPolicy("")
	if e != nil {
		t.Fatal(e)
	}
	s := p.Profiles["omp-dsflash41"]
	if s.E6 {
		t.Fatal("omp-dsflash41 still carries the E6 flag")
	}
	if want := []string{"implementer", "reviewer"}; !slices.Equal(s.Roles, want) {
		t.Fatalf("roles %v, want %v", s.Roles, want)
	}

	l := Launch{ModelID: "deepseek/deepseek-v4.1-flash", Effort: "high", Grade: "A+", Gate: "default"}
	capabilityOn := true
	for _, grade := range []string{"A", "B"} {
		t.Run("worker on grade "+grade+" without an arm", func(t *testing.T) {
			if err := s.Check(l, "implementer", grade, "", "", capabilityOn); err != nil {
				t.Fatalf("regular seat refused a grade %s worker: %v", grade, err)
			}
		})
	}

	t.Run("host without omp_commandcode is refused", func(t *testing.T) {
		if got := code(s.Check(l, "implementer", "B", "", "", false)); got != exitConfig {
			t.Fatalf("capability absent: code %d, want %d", got, exitConfig)
		}
	})

	t.Run("reviewer needs an independent author family", func(t *testing.T) {
		if got := code(s.Check(l, "reviewer", "B", "openai", "", capabilityOn)); got != 0 {
			t.Fatalf("independent reviewer: code %d, want 0", got)
		}
		if got := code(s.Check(l, "reviewer", "B", "deepseek", "", capabilityOn)); got != exitSeatRefused {
			t.Fatalf("same-family reviewer: code %d, want %d", got, exitSeatRefused)
		}
	})
}

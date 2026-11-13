package fleet

import (
	"strings"
	"testing"
)

// hk 3520: an E6 arm lets the seat check admit an unrated rung for a graded
// task, but only for the exact <canon>@<effort> of that rung.
func TestSeatCheckUnratedRungAdmittedByMatchingArm(t *testing.T) {
	s := Seat{Canon: "omp-dsflash41", Kind: "omp", Family: "deepseek", Capability: "standard", Roles: []string{"implementer"}, E6: true}
	unrated := Launch{ModelID: "ds-m", Effort: "high", Grade: "unrated", Gate: "default"}
	rated := Launch{ModelID: "ds-m", Effort: "high", Grade: "C", Gate: "default"}

	t.Run("unrated rung with matching arm passes a grade B task", func(t *testing.T) {
		if err := s.Check(unrated, "implementer", "B", "", "omp-dsflash41@high", nil); err != nil {
			t.Fatalf("matching arm refused: %v", err)
		}
	})

	t.Run("unrated rung without arm names the arm it needs", func(t *testing.T) {
		err := s.Check(unrated, "implementer", "B", "", "", nil)
		if code(err) != exitSeatRefused || !strings.Contains(err.Error(), "requires PANEWIRE_E6_ARM=omp-dsflash41@high") {
			t.Fatalf("no arm: code=%d err=%v", code(err), err)
		}
	})

	t.Run("unrated rung with a different arm is refused", func(t *testing.T) {
		err := s.Check(unrated, "implementer", "B", "", "omp-dsflash41@low", nil)
		if code(err) != exitSeatRefused {
			t.Fatalf("different arm: code=%d err=%v", code(err), err)
		}
	})

	t.Run("unrated rung without E6 flag and no arm is refused with the arm named", func(t *testing.T) {
		plain := Seat{Canon: "omp-dsflash41", Kind: "omp", Family: "deepseek", Capability: "standard", Roles: []string{"implementer"}}
		err := plain.Check(unrated, "implementer", "B", "", "", nil)
		if code(err) != exitSeatRefused || !strings.Contains(err.Error(), "canon grade unrated does not cover task grade B") || !strings.Contains(err.Error(), "requires PANEWIRE_E6_ARM=omp-dsflash41@high") {
			t.Fatalf("no arm, no flag: code=%d err=%v", code(err), err)
		}
	})

	t.Run("rated rung stays graded even with matching arm", func(t *testing.T) {
		err := s.Check(rated, "implementer", "B", "", "omp-dsflash41@high", nil)
		if code(err) != exitSeatRefused || !strings.Contains(err.Error(), "canon grade C does not cover task grade B") {
			t.Fatalf("rated C with arm: code=%d err=%v", code(err), err)
		}
	})
}

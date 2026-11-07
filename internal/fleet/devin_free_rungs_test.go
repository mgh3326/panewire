package fleet

import "testing"

// Every SWE-2 free rung must have a launchable devin seat, or a rung the
// catalog and routing name can never be spawned (devin-swe2-flash, 2026-11-07).
func TestDevinFreeRungsHaveDevinSeats(t *testing.T) {
	p, err := LoadPolicy("")
	if err != nil {
		t.Fatal(err)
	}
	for _, profile := range []string{"devin-swe2-flash", "devin-swe2-flash-medium"} {
		if _, free := devinFreeRungs[profile]; !free {
			t.Errorf("%s is not a SWE-2 free rung", profile)
		}
	}
	for profile, bucket := range devinFreeRungs {
		seat, ok := p.Profiles[profile]
		if !ok || seat.Kind != "devin" || seat.Family != "cognition" {
			t.Errorf("free rung %s (bucket %s) has no cognition devin seat: %+v", profile, bucket, seat)
		}
	}
}

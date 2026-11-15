package fleet

import (
	"testing"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// hk 4644: Devin's request-rate-limit text does not say which rung hit it, so
// the running profile decides the pool. A stop the screen named is untouched.
func TestQuotaStopPoolFromProfile(t *testing.T) {
	open := quotastop.Stop{Pool: "devin", PoolFromProfile: true}
	for profile, want := range map[string]string{
		"devin-swe2":              "devin-free",
		"devin-swe2-medium":       "devin-free",
		"devin-swe2-flash-max":    "devin-free",
		"devin-ds41":              "devin",
		"devin-fusion-sol61-high": "devin",
		"":                        "devin",
		"unplaced-rung":           "devin",
	} {
		if got := QuotaStopPool(open, profile).Pool; got != want {
			t.Fatalf("profile %q: pool = %s, want %s", profile, got, want)
		}
	}
	if got := QuotaStopPool(quotastop.Stop{Pool: "devin-free"}, "devin-ds41").Pool; got != "devin-free" {
		t.Fatalf("a screen-named devin-free stop became %s from the profile", got)
	}
}

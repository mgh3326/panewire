package fleet

import (
	"strings"
	"testing"
	"time"
)

// hk 3926: a devin pool under policy class "exclude" refuses every devin rung
// except the SWE-2 free rungs, which draw on the free bucket, not the paid pool.
func excludedDevinInputs(free *admissionFree) admissionInputs {
	return admissionInputs{
		Now:           switchNow,
		AccountScoped: true,
		Policy: admissionPolicy{Pools: map[string]admissionPoolPolicy{
			"devin":  {Class: "exclude"},
			"claude": {Class: "exclude"},
		}},
		Rows: []admissionRow{
			{Profile: "devin-swe2-medium", Effort: "medium", Pool: "devin", Grade: "A", Gate: defaultGate, Placed: true},
			{Profile: "devin-swe2-flash", Effort: "high", Pool: "devin", Grade: "B", Gate: defaultGate, Placed: true},
			{Profile: "devin-ds41", Effort: "high", Pool: "devin", Grade: "A+", Gate: defaultGate, Placed: true},
			{Profile: "sonnet", Effort: "high", Pool: "claude", Grade: "A", Gate: defaultGate, Placed: true},
		},
		Readings: map[string]admissionReading{"devin": {Free: free}},
	}
}

// (a) An excluded devin pool with the free bucket available admits the SWE-2
// free rungs on the same verdict as an unexcluded pool.
func TestExcludedDevinPoolAdmitsFreeRungs(t *testing.T) {
	in := excludedDevinInputs(&admissionFree{State: "available"})
	for _, req := range []admissionRequest{
		{Profile: "devin-swe2-medium", Effort: "medium", Role: "implementer"},
		{Profile: "devin-swe2-flash", Effort: "high", Role: "implementer"},
	} {
		d := in.admit(req)
		if !d.OK || d.Exit != gateAdmitted || !strings.HasPrefix(d.Basis, freeBasisPrefix) {
			t.Fatalf("%s under exclude with free bucket available: want admitted on the free basis, got %+v", req.Profile, d)
		}
		if d.Class != "exclude" {
			t.Fatalf("%s: verdict class = %q, want the pool's own exclude class recorded", req.Profile, d.Class)
		}
	}
}

// (b) An excluded devin pool with the free bucket exhausted or unknown refuses
// the free rung on the free-bucket reason, never on "pool excluded by policy".
func TestExcludedDevinPoolFreeRungJudgedOnFreeBucket(t *testing.T) {
	resetAt := switchNow.Add(2 * time.Hour)
	for _, tc := range []struct {
		name string
		free *admissionFree
		want string
	}{
		{"exhausted until reset", &admissionFree{State: "exhausted", Reset: &resetAt}, "free limit exceeded until"},
		{"unknown state", &admissionFree{State: "unknown"}, "shared free bucket state unavailable"},
		{"no reading", nil, "shared free bucket state unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := excludedDevinInputs(tc.free)
			d := in.admit(admissionRequest{Profile: "devin-swe2-medium", Effort: "medium", Role: "implementer"})
			if d.OK {
				t.Fatalf("free rung admitted with %s bucket: %+v", tc.name, d)
			}
			if strings.Contains(d.Reason, "pool excluded") {
				t.Fatalf("free rung refused by the pool class, not the free bucket: %+v", d)
			}
			if !strings.Contains(d.Reason, tc.want) {
				t.Fatalf("reason = %q, want it to name the free bucket (%q)", d.Reason, tc.want)
			}
		})
	}
}

// (c) A paid devin rung under exclude stays refused on the pool class.
func TestExcludedDevinPoolKeepsPaidRungsRefused(t *testing.T) {
	in := excludedDevinInputs(&admissionFree{State: "available"})
	d := in.admit(admissionRequest{Profile: "devin-ds41", Effort: "high", Role: "implementer"})
	if d.OK || d.Reason != "pool excluded by policy" {
		t.Fatalf("devin-ds41 under exclude: want refused with \"pool excluded by policy\", got %+v", d)
	}
}

// (d) A non-devin excluded pool is refused exactly as before.
func TestExcludedNonDevinPoolUnchanged(t *testing.T) {
	in := excludedDevinInputs(&admissionFree{State: "available"})
	d := in.admit(admissionRequest{Profile: "sonnet", Effort: "high", Role: "implementer"})
	if d.OK || d.Reason != "pool excluded by policy" {
		t.Fatalf("sonnet on an excluded claude pool: want refused with \"pool excluded by policy\", got %+v", d)
	}
}

// Auto selection tiers a free rung by its row before the pool class is read,
// so an excluded devin pool still ranks the SWE-2 free rungs as free and the
// paid devin rung as nothing.
func TestExcludedDevinPoolAutoTiersFreeRungs(t *testing.T) {
	policy := PoolPolicy{Free: []FreeSet{{Pool: "devin", Models: []string{"swe-2", "swe-2-medium"}}}}
	excluded := PoolView{Status: "ok", Class: "exclude"}
	free := CatalogRow{Profile: "devin-swe2-medium", Pool: "devin", ModelID: "swe-2-medium"}
	paid := CatalogRow{Profile: "devin-ds41", Pool: "devin", ModelID: "deepseek-v4-1-flash-high"}
	if tier, idx := policy.tierOf(free, poolBands, excluded); tier != "free" || idx < 0 {
		t.Fatalf("free rung under exclude: tier=%q idx=%d, want free", tier, idx)
	}
	if tier, idx := policy.tierOf(paid, poolBands, excluded); tier != "" || idx != -1 {
		t.Fatalf("paid devin under exclude: tier=%q idx=%d, want no tier", tier, idx)
	}
}

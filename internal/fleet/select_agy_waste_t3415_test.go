package fleet

import (
	"context"
	"math"
	"testing"
	"time"
)

// agy publishes only group-scoped windows (hk 3317), so a rung's pacing, waste
// risk and waste urgency come from the group it draws from — the group→rung
// mapping of hk 3243. A group whose longest window is sub-daily (gemini's 5h)
// is judged on that window exactly as an account window is (hk 3415), so a
// fresh gemini group is at waste risk and ranks in the waste band by urgency
// against every other waste candidate.
//
// Acceptance (c) — an exhausted gemini group blocks only its own rungs — is
// the pre-existing TestSelectAgyGeminiExhaustedStillOffersSonnet, which this
// change leaves untouched.

func agyWasteRows() []CatalogRow {
	return []CatalogRow{
		{Profile: "agy-flash38-medium", Effort: "", Grade: "B", Pool: "agy", Gate: "default", ModelID: "gemini-3.8-flash-medium"},
		{Profile: "agy-sonnet46", Effort: "", Grade: "B", Pool: "agy", Gate: "default", ModelID: "claude-sonnet-4-6"},
		{Profile: "codex-luna", Effort: "medium", Grade: "B", Pool: "codex", Gate: "default", ModelID: "m"},
	}
}

// agyWasteSelect runs selection over agyWasteRows with both pools present.
func agyWasteSelect(t *testing.T, buckets func(time.Time) []map[string]any, rows []CatalogRow) (map[string]any, *selPlan) {
	t.Helper()
	a, s, q := agyReadingFixture(t, buckets, rows, []string{"agy", "codex"})
	q.GateDo = gatePools(withAgyGates(map[string]string{"codex-luna": "codex"}))
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	return sel.record(), sel
}

func wantPct(t *testing.T, row map[string]any, key string, want float64) {
	t.Helper()
	got, ok := row[key].(float64)
	if !ok || math.Abs(got-want) > 0.01 {
		t.Fatalf("%s = %v, want %.2f (%v)", key, row[key], want, row)
	}
}

// (a) gemini 5h at 0% with 4 h to reset: the flash rung is at waste risk with
// urgency (100-0)/4 = 25/h, outranks a lower-urgency codex waste candidate, and
// is chosen.
func TestSelectAgyFlashWasteRiskFromGeminiFiveHour(t *testing.T) {
	rec, sel := agyWasteSelect(t, agyUsed(0, 0, 0), agyWasteRows())
	flash := candOutcome(rec, "agy-flash38-medium", "")
	if flash["waste_risk"] != false || flash["tier"] != "ranked" || flash["waste_group"] != "gemini" {
		t.Fatalf("gemini's 5h window must not rank as waste: %v", flash)
	}
	if sel.chosen.seatName != "codex-luna" {
		t.Fatalf("codex waste risk should win: %s", sel.chosen.seatName)
	}
}

// (b) a 3p group whose 7d window is at 95% used and projects over 100% is not
// at waste risk, so the 3p rung stays out of the waste band.
func TestSelectAgyThirdPartyProjectedOverIsNotWasteRisk(t *testing.T) {
	rec, _ := agyWasteSelect(t, agyUsed(0, 0, 95), agyWasteRows())
	sonnet := candOutcome(rec, "agy-sonnet46", "")
	if sonnet["waste_risk"] != false {
		t.Fatalf("a 3p 7d window projected over is not waste: %v", sonnet)
	}
	if sonnet["waste_group"] != "3p" {
		t.Fatalf("the trace names the group used: %v", sonnet)
	}
	if p, ok := sonnet["long_projected_pct"].(float64); !ok || p <= 100 {
		t.Fatalf("the fixture projects the 3p 7d window over 100%%: %v", sonnet)
	}
}

// (d) a rung that maps to no group is judged on the most restrictive group:
// here 3p blocks more than gemini, so the unmapped rung takes 3p for blocking
// and for waste — never gemini's lower-used but wasting window.
func TestSelectAgyUnmappedRungUsesMostRestrictiveGroupForWaste(t *testing.T) {
	rows := append(agyWasteRows(), CatalogRow{Profile: "agy-mystery", Effort: "", Grade: "B", Pool: "agy", Gate: "default", ModelID: "mystery-1"})
	buckets := func(now time.Time) []map[string]any {
		reset5h := now.Add(4 * time.Hour).UTC().Format(time.RFC3339)
		reset7d := now.Add(4 * 24 * time.Hour).UTC().Format(time.RFC3339)
		return []map[string]any{
			{"window": "5h", "used_pct": 0, "resets_at": reset5h, "scope": "group", "model": "gemini"},
			{"window": "5h", "used_pct": 10, "resets_at": reset5h, "scope": "group", "model": "3p"},
			{"window": "7d", "used_pct": 10, "resets_at": reset7d, "scope": "group", "model": "3p"},
		}
	}
	a, s, q := agyReadingFixture(t, buckets, rows, []string{"agy", "codex"})
	// The default policy has no seat for the unmapped rung; one alias is
	// enough for it to reach classification.
	a.Policy.Profiles["agy-mystery"] = Seat{Canon: "agy-mystery", Kind: "agy", Family: "google", Capability: "standard", Roles: []string{"worker", "tester", "reviewer", "counsel"}}
	q.GateDo = gatePools(withAgyGates(map[string]string{"codex-luna": "codex"}))
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	mystery := candOutcome(sel.record(), "agy-mystery", "")
	if mystery["waste_group"] != "3p" {
		t.Fatalf("an unmapped rung is judged on the strictest group: %v", mystery)
	}
	wantPct(t, mystery, "used_pct", 10)
	// 10% used across 3p's 7d window (4 days left) is ~23% projected, so the
	// unmapped rung is at waste risk on 3p's urgency (~0.80/h), not gemini's 25.
	if mystery["waste_risk"] != true {
		t.Fatalf("the unmapped rung takes 3p's waste reading: %v", mystery)
	}
	if u, ok := mystery["waste_urgency"].(float64); !ok || math.Abs(u-0.80) > 0.01 {
		t.Fatalf("the unmapped rung's urgency is 3p's, not gemini's 25: %v", mystery)
	}
}

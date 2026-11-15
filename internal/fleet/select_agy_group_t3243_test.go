package fleet

import (
	"context"
	"strings"
	"testing"
	"time"
)

// agy meters per group, and the operator confirmed on 2026-11-07 that the
// quotas are separate: agy-sonnet46 and agy-opus46 draw from the third-party
// group ("3p"), agy-flash* from the gemini group (hk 3243). Placement ranks and
// blocks a rung on its own group's windows only.

// agyUsed is the production shape of the agy reading (gemini 5h, 3p 5h, 3p 7d,
// an "unknown" 5h with no reset) with the given used percentages.
func agyUsed(gemini5h, third5h, third7d float64) func(time.Time) []map[string]any {
	return func(now time.Time) []map[string]any {
		reset5h := now.Add(4 * time.Hour).UTC().Format(time.RFC3339)
		reset7d := now.Add(4 * 24 * time.Hour).UTC().Format(time.RFC3339)
		return []map[string]any{
			{"window": "5h", "used_pct": gemini5h, "resets_at": reset5h, "scope": "group", "model": "gemini"},
			{"window": "5h", "used_pct": third5h, "resets_at": reset5h, "scope": "group", "model": "3p"},
			{"window": "7d", "used_pct": third7d, "resets_at": reset7d, "scope": "group", "model": "3p"},
			{"window": "5h", "used_pct": 0, "scope": "group", "model": "unknown"},
		}
	}
}

func agyBothGroupsRows() []CatalogRow {
	return []CatalogRow{
		{Profile: "agy-flash38-medium", Effort: "medium", Grade: "B", Pool: "agy", Gate: "default", ModelID: "gemini-3.8-flash-medium"},
		{Profile: "agy-sonnet46", Effort: "medium", Grade: "B", Pool: "agy", Gate: "default", ModelID: "claude-sonnet-4-6"},
	}
}

func agySelect(t *testing.T, buckets func(time.Time) []map[string]any, rows []CatalogRow) (map[string]any, *selPlan) {
	t.Helper()
	a, s, q := agyReadingFixture(t, buckets, rows, []string{"agy"})
	q.GateDo = gatePools(withAgyGates(nil))
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatalf("one agy group still has quota, so a rung is offered: %v", e)
	}
	return sel.record(), sel
}

func dropped(row map[string]any) bool {
	outcome, _ := row["outcome"].(string)
	return strings.HasPrefix(outcome, "dropped:")
}

// Gemini exhausted: the gemini rungs are blocked, the 3p rung is still offered.
func TestSelectAgyGeminiExhaustedStillOffersSonnet(t *testing.T) {
	rec, sel := agySelect(t, agyUsed(100, 10, 10), agyBothGroupsRows())
	if row := candOutcome(rec, "agy-flash38-medium", "medium"); row == nil || !dropped(row) {
		t.Fatalf("a gemini rung must be blocked by the exhausted gemini window: %v", row)
	}
	if sel.chosen.seatName != "agy-sonnet46" {
		t.Fatalf("the 3p rung must still be offered: chosen %s", sel.chosen.seatName)
	}
}

// 3p exhausted (either window): the reverse.
func TestSelectAgyThirdPartyExhaustedStillOffersFlash(t *testing.T) {
	for name, buckets := range map[string]func(time.Time) []map[string]any{
		"5h": agyUsed(10, 100, 10),
		"7d": agyUsed(10, 10, 100),
	} {
		t.Run(name, func(t *testing.T) {
			rec, sel := agySelect(t, buckets, agyBothGroupsRows())
			if row := candOutcome(rec, "agy-sonnet46", "medium"); row == nil || !dropped(row) {
				t.Fatalf("a 3p rung must be blocked by the exhausted 3p window: %v", row)
			}
			if sel.chosen.seatName != "agy-flash38-medium" {
				t.Fatalf("the gemini rung must still be offered: chosen %s", sel.chosen.seatName)
			}
		})
	}
}

// A rung ranks on its own group's usage, not the strictest one's: with 3p at
// 90% a gemini rung reports its own 10%.
func TestSelectAgyRungReportsItsOwnGroupUsage(t *testing.T) {
	rec, _ := agySelect(t, agyUsed(10, 90, 40), agyBothGroupsRows())
	flash := candOutcome(rec, "agy-flash38-medium", "medium")
	sonnet := candOutcome(rec, "agy-sonnet46", "medium")
	if flash["used_pct"] != 10.0 || sonnet["used_pct"] != 90.0 {
		t.Fatalf("used_pct gemini=%v 3p=%v, want 10 and 90", flash["used_pct"], sonnet["used_pct"])
	}
}

// A rung that maps to no group keeps the conservative rule: the strictest
// group blocks it.
func TestSelectAgyUnmappedRungKeepsConservativeRule(t *testing.T) {
	rows := append(agyBothGroupsRows(), CatalogRow{Profile: "agy-mystery", Effort: "medium", Grade: "B", Pool: "agy", Gate: "default", ModelID: "mystery-1"})
	rec, sel := agySelect(t, agyUsed(100, 10, 10), rows)
	if row := candOutcome(rec, "agy-mystery", "medium"); row == nil || !dropped(row) {
		t.Fatalf("an unmapped rung is blocked while any group is exhausted: %v", row)
	}
	if sel.chosen.seatName != "agy-sonnet46" {
		t.Fatalf("chosen %s", sel.chosen.seatName)
	}
}

// The catalog's own group wins over the model id, and a rung that names a
// group the reading has no window for is unknown, never borrowed from another.
func TestSelectAgyGroupMissingFromReadingIsUnknown(t *testing.T) {
	only3p := func(now time.Time) []map[string]any {
		return []map[string]any{{"window": "5h", "used_pct": 10, "resets_at": now.Add(4 * time.Hour).UTC().Format(time.RFC3339), "scope": "group", "model": "3p"}}
	}
	rec, sel := agySelect(t, only3p, agyBothGroupsRows())
	if row := candOutcome(rec, "agy-flash38-medium", "medium"); row == nil || row["outcome"] != "dropped:quota_unknown" {
		t.Fatalf("a rung whose group has no window is quota_unknown: %v", row)
	}
	if sel.chosen.seatName != "agy-sonnet46" {
		t.Fatalf("chosen %s", sel.chosen.seatName)
	}
}

func TestCatalogRowMeteringGroup(t *testing.T) {
	for _, c := range []struct {
		row  CatalogRow
		want string
	}{
		{CatalogRow{Pool: "agy", ModelID: "gemini-3.7-flash"}, "gemini"},
		{CatalogRow{Pool: "agy", ModelID: "gemini-3.8-flash-low"}, "gemini"},
		{CatalogRow{Pool: "agy", ModelID: "claude-sonnet-4-6"}, "3p"},
		{CatalogRow{Pool: "agy", ModelID: "claude-opus-4-6-thinking"}, "3p"},
		{CatalogRow{Pool: "agy", ModelID: "gpt-oss-120b-medium"}, "3p"},
		{CatalogRow{Pool: "agy", ModelID: "mystery-1"}, ""},
		{CatalogRow{Pool: "agy", ModelID: "mystery-1", Group: "3p"}, "3p"},
		{CatalogRow{Pool: "agy", ModelID: "gemini-3.7-flash", Group: "unknown"}, "gemini"},
		{CatalogRow{Pool: "codex", ModelID: "gemini-3.7-flash", Group: "gemini"}, ""},
	} {
		if got := c.row.meteringGroup(); got != c.want {
			t.Errorf("%+v: group %q, want %q", c.row, got, c.want)
		}
	}
}

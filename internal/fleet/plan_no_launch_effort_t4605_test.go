package fleet

// hk 4605: one catalog row admission cannot launch (it names no effort and its
// profile has no named rung) must not abort the whole plan. Selection drops it
// with its own outcome and tries the next candidate; a pinned profile still
// refuses with the catalog's message.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// noLaunchEffortRows are two agy rungs of the same grade: one rung-less row
// whose profile names no effort, and one named rung that launches. The
// rung-less row ranks first (effort "" sorts before "high").
func noLaunchEffortRows() []CatalogRow {
	return []CatalogRow{
		{Profile: "agy-flash38-medium", Grade: "B", Pool: "agy", Gate: "default", ModelID: "gemini-3.8-flash-medium"},
		{Profile: "agy-flash38-high", Effort: "high", Grade: "B", Pool: "agy", Gate: "default", ModelID: "gemini-3.8-flash-high"},
	}
}

func TestPlanDropsNoLaunchEffortRung_t4605(t *testing.T) {
	a, _, q := selFixture(t, "B", noLaunchEffortRows(), map[string]PoolView{"agy": {Status: "ok", Class: "spend"}}, []string{"agy"})
	q.GateDo = gatePools(map[string]string{"agy-flash38-high": "agy", "agy-flash38-medium": "agy"})
	q.LaunchValue = Launch{Profile: "agy-flash38-high", ModelID: "gemini-3.8-flash-high", Effort: "high", Grade: "B", Gate: "default", Pool: "agy"}
	if e := a.planCmd(context.Background(), []string{"--grade", "B", "--t", "T2", "--json"}); e != nil {
		t.Fatalf("plan failed on a rung-less catalog row: %v", e)
	}
	var doc map[string]any
	if e := json.Unmarshal(a.Out.(interface{ Bytes() []byte }).Bytes(), &doc); e != nil {
		t.Fatalf("plan --json: %v", e)
	}
	chosen, _ := doc["chosen"].(map[string]any)
	if chosen["seat"] != "agy-flash38-high" || chosen["effort"] != "high" {
		t.Fatalf("plan chose %v; want the next candidate agy-flash38-high@high", chosen)
	}
	sel, _ := doc["selection"].(map[string]any)
	cands, _ := sel["candidates"].([]any)
	dropped := ""
	for _, v := range cands {
		row := v.(map[string]any)
		if row["profile"] == "agy-flash38-medium" {
			dropped, _ = row["outcome"].(string)
		}
	}
	if dropped != "dropped:no_launch_effort" {
		t.Fatalf("rung-less row outcome = %q; want dropped:no_launch_effort", dropped)
	}
}

// An unrated named rung resolves an effort only when admission keeps it: the
// implementer role without the E6 arm excludes it, so the profile's rung-less
// row still has no launch effort and drops; armed, the rung counts and the row
// stays a candidate (hk 4605).
func TestNoLaunchEffortCountsOnlyEligibleRungs_t4605(t *testing.T) {
	rows := []CatalogRow{
		{Profile: "devin-ds41", Grade: "B", Pool: "devin", Gate: "default", ModelID: "deepseek-v4-1-flash"},
		{Profile: "devin-ds41", Effort: "high", Grade: "unrated", Pool: "devin", Gate: "default", ModelID: "deepseek-v4-1-flash"},
		{Profile: "sonnet", Effort: "high", Grade: "C", Pool: "claude", Gate: "default", ModelID: "m"},
	}
	pools := map[string]PoolView{"devin": {Status: "ok", Class: "spend"}, "claude": {Status: "ok", Class: "spend"}}
	a, s, q := selFixture(t, "C", rows, pools, []string{"devin", "claude"})
	q.GateDo = gatePools(map[string]string{"devin-ds41": "devin", "sonnet": "claude"})
	rec := func() map[string]any {
		sel, e := a.selectProfile(context.Background(), s)
		if e != nil {
			t.Fatal(e)
		}
		return sel.record()
	}
	if got := candOutcome(rec(), "devin-ds41", "")["outcome"]; got != "dropped:no_launch_effort" {
		t.Fatalf("an unrated-only named rung resolves no effort: %v", got)
	}
	s.E6Arm = "devin-ds41@high"
	if got := candOutcome(rec(), "devin-ds41", "")["outcome"]; got == "dropped:no_launch_effort" {
		t.Fatalf("the armed unrated rung resolves an effort: %v", got)
	}
}

func TestPinnedNoLaunchEffortProfileRefuses_t4605(t *testing.T) {
	a := testApp(t)
	a.BenchCatalog = func(context.Context) ([]CatalogRow, error) {
		return []CatalogRow{{Profile: "agy-flash38-medium", Grade: "B", Pool: "agy", Gate: "default", ModelID: "gemini-3.8-flash-medium"}}, nil
	}
	_, err := (admissionQuota{CLIQuota{}, a}).Launch(context.Background(), QuotaRequest{Profile: "agy-flash38-medium", Role: "implementer"})
	if err == nil || !strings.Contains(err.Error(), "catalog row has no launch effort") {
		t.Fatalf("pinned rung-less profile: err = %v; want the catalog's no-launch-effort refusal", err)
	}
	if got := code(err); got != gateBlocked {
		t.Fatalf("pinned rung-less profile exits %d; want %d", got, gateBlocked)
	}
}

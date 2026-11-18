package fleet

import (
	"context"
	"errors"
	"slices"
	"testing"
)

// A placed selection reports the SWE-2 candidates it dropped, one entry each,
// for the counted reasons, on the account each ran on. A counted drop is never
// an admitted launch, a paid rung, or a reason outside the three counted ones.
func TestSelectionDropsCountsOnlyDroppedSWE2(t *testing.T) {
	sel := map[string]any{"candidates": []any{
		map[string]any{"profile": "devin-swe2", "outcome": "", "account_key": "devin:aaaaaaaaaaaaaaaa"},
		map[string]any{"profile": "devin-swe2", "outcome": "dropped:pool_at_cap", "reason": "pool_at_cap", "account_key": "devin:aaaaaaaaaaaaaaaa"},
		map[string]any{"profile": "devin-swe2-medium", "outcome": "dropped:free_limit", "reason": "free_limit", "account_key": "devin:bbbbbbbbbbbbbbbb"},
		map[string]any{"profile": "devin-swe2", "outcome": "dropped:pool_cooling", "reason": "pool_cooling"},
		map[string]any{"profile": "devin-ds41", "outcome": "dropped:pool_at_cap", "reason": "pool_at_cap", "account_key": "devin:aaaaaaaaaaaaaaaa"},
		map[string]any{"profile": "devin-swe2", "outcome": "dropped:quota_exhausted", "reason": "quota_exhausted", "account_key": "devin:aaaaaaaaaaaaaaaa"},
	}}
	want := []SelectionDrop{
		{Reason: "pool_at_cap", Account: "devin:aaaaaaaaaaaaaaaa"},
		{Reason: "free_limit", Account: "devin:bbbbbbbbbbbbbbbb"},
		{Reason: "pool_cooling", Account: "unknown"},
	}
	if got := selectionDrops(sel); !slices.Equal(got, want) {
		t.Fatalf("selectionDrops = %v, want %v", got, want)
	}
}

// The selection a real placement makes counts its drop once, and an admitted
// first launch counts nothing: the burst's second launch on the same account is
// the only drop.
func TestSelectionDropsFromSelectionCountOnlyTheBlockedLaunch(t *testing.T) {
	a, s := devinBandFixture(t)
	live := map[string]map[string]int{}
	a.LivePlacedByAccount = func(context.Context, string) (map[string]map[string]int, error) { return live, nil }

	first, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if drops := selectionDrops(first.record()); len(drops) != 0 {
		t.Fatalf("admitted first launch counted drops %v, want none", drops)
	}
	account := selectionAccountKey(t, first)
	live[account] = map[string]int{"devin-swe2": 1}

	second, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	want := []SelectionDrop{{Reason: "pool_at_cap", Account: account}}
	if got := selectionDrops(second.record()); !slices.Equal(got, want) {
		t.Fatalf("blocked launch drops = %v, want %v", got, want)
	}
}

// A dry run never reaches the placed event, so it counts nothing; a placed
// spawn reports its selection's drops to the hook once.
func TestSelectionDropsReportedOnceOnPlacedEventOnly(t *testing.T) {
	a, s := devinBandFixture(t)
	live := map[string]map[string]int{}
	a.LivePlacedByAccount = func(context.Context, string) (map[string]map[string]int, error) { return live, nil }
	first, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	account := selectionAccountKey(t, first)
	live[account] = map[string]int{"devin-swe2": 1}
	blocked, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}

	var reported [][]SelectionDrop
	a.OnSelectionDrops = func(drops []SelectionDrop) { reported = append(reported, drops) }
	placed := s
	placed.Sel = blocked.record()
	rec := map[string]any{"chose": "local", "ranking": "score", "candidates": []any{}}

	dry := placed
	dry.DryRun = true
	a.placementEvent(dry, rec, nil)
	if len(reported) != 0 {
		t.Fatalf("dry run reported %v, want nothing", reported)
	}
	a.placementEvent(placed, rec, errors.New("refused"))
	if len(reported) != 0 {
		t.Fatalf("refused placement reported %v, want nothing", reported)
	}
	a.placementEvent(placed, rec, nil)
	want := []SelectionDrop{{Reason: "pool_at_cap", Account: account}}
	if len(reported) != 1 || !slices.Equal(reported[0], want) {
		t.Fatalf("placed reported %v, want one report %v", reported, want)
	}
}

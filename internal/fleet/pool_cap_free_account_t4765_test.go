package fleet

import (
	"context"
	"errors"
	"slices"
	"testing"
)

// Three submissions bursting onto the same account's free band place at most
// one there: the first finds nothing live and takes the free rung, and with
// that launch live the next two already move to another pool, so the burst
// does not trip the free limit (hk 4765). The three selections stand in for
// three submissions landing inside the hub's live window.
func TestFreeBandAdmitsOneConcurrentLaunchPerAccount(t *testing.T) {
	a, s := devinBandFixture(t)
	live := map[string]map[string]int{}
	a.LivePlacedByAccount = func(context.Context, string) (map[string]map[string]int, error) { return live, nil }

	first, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if first.chosen.seatName != "devin-swe2" {
		t.Fatalf("first of the burst chose %s, want the free rung devin-swe2", first.chosen.seatName)
	}
	chosen, _ := first.record()["chosen"].(map[string]any)
	account, _ := chosen["account_key"].(string)
	if account == "" {
		t.Fatal("first selection names no account")
	}
	live[account] = map[string]int{first.chosen.row.Profile: 1}

	for i := range 2 {
		sel, err := a.selectProfile(context.Background(), s)
		if err != nil {
			t.Fatal(err)
		}
		row := candOutcome(sel.record(), "devin-swe2", "high")
		if row["outcome"] != "dropped:pool_at_cap" {
			t.Fatalf("burst selection %d: free rung outcome = %v, want dropped:pool_at_cap", i+2, row["outcome"])
		}
		if row["cap"] != "devin-free" || row["cap_live"] != 1 || row["cap_limit"] != 1 {
			t.Fatalf("burst selection %d: free rung evidence = %v, want devin-free with 1 live and limit 1", i+2, row)
		}
		if sel.chosen.seatName != "devin-ds41" {
			t.Fatalf("burst selection %d chose %s, want the paid rung devin-ds41", i+2, sel.chosen.seatName)
		}
	}
}

// Another account's free launch does not hold this one: the gate is per
// account, not fleet-wide.
func TestFreeBandPerAccountGateIgnoresOtherAccounts(t *testing.T) {
	a, s := devinBandFixture(t)
	a.LivePlacedByAccount = func(context.Context, string) (map[string]map[string]int, error) {
		return map[string]map[string]int{"devin:ffff0000ffff0000": {"devin-swe2": 1}}, nil
	}
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if sel.chosen.seatName != "devin-swe2" {
		t.Fatalf("chosen = %s, want the free rung: another account's launch must not hold this one", sel.chosen.seatName)
	}
}

// An unreadable launch record fails the gate open: the free rung stays
// placeable and the record says the count was unavailable.
func TestFreeBandPerAccountGateFailsOpenOnUnreadableCount(t *testing.T) {
	a, s := devinBandFixture(t)
	a.LivePlacedByAccount = func(context.Context, string) (map[string]map[string]int, error) {
		return nil, errors.New("store unavailable")
	}
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if sel.chosen.seatName != "devin-swe2" {
		t.Fatalf("chosen = %s, want the free rung: an unreadable count must not cap it", sel.chosen.seatName)
	}
	if !slices.Contains(sel.degraded, degradedPoolCapCount) {
		t.Fatalf("degraded = %v, want %q", sel.degraded, degradedPoolCapCount)
	}
}

// A launch placed through a seat counts too: builder-devin runs devin-swe2,
// so it holds the account's free band like a catalog-named launch.
func TestFreeBandPerAccountGateCountsSeatAliases(t *testing.T) {
	a, s := devinBandFixture(t)
	live := map[string]map[string]int{}
	a.LivePlacedByAccount = func(context.Context, string) (map[string]map[string]int, error) { return live, nil }

	first, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	chosen, _ := first.record()["chosen"].(map[string]any)
	account, _ := chosen["account_key"].(string)
	if account == "" {
		t.Fatal("first selection names no account")
	}
	live[account] = map[string]int{"builder-devin": 1}

	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	row := candOutcome(sel.record(), "devin-swe2", "high")
	if row["outcome"] != "dropped:pool_at_cap" || row["cap"] != "devin-free" || row["cap_live"] != 1 {
		t.Fatalf("seat-named free launch must hold the band: %v", row)
	}
	if sel.chosen.seatName != "devin-ds41" {
		t.Fatalf("chose %s, want the paid rung devin-ds41", sel.chosen.seatName)
	}
}

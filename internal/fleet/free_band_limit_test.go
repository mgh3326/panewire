package fleet

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// [quota].free_band_per_account raises the concurrent free-band launches one
// account holds above the default of one (decision 427). With the key at two,
// the second launch on the account is admitted and the third is dropped
// pool_at_cap, naming the reserved devin-free cap and its limit of two.
func TestFreeBandLimitKeyAdmitsUpToItsCount(t *testing.T) {
	a, s := devinBandFixture(t)
	a.Config.Quota = map[string]any{"free_band_per_account": int64(2)}
	live := map[string]map[string]int{}
	a.LivePlacedByAccount = func(context.Context, string) (map[string]map[string]int, error) { return live, nil }

	first, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if first.chosen.seatName != "devin-swe2" {
		t.Fatalf("first launch chose %s, want the free rung devin-swe2", first.chosen.seatName)
	}
	account := selectionAccountKey(t, first)
	live[account] = map[string]int{"devin-swe2": 1}

	second, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if second.chosen.seatName != "devin-swe2" {
		t.Fatalf("second launch chose %s, want devin-swe2: the key at two admits it", second.chosen.seatName)
	}
	live[account] = map[string]int{"devin-swe2": 2}

	third, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	row := candOutcome(third.record(), "devin-swe2", "high")
	if row["outcome"] != "dropped:pool_at_cap" || row["cap"] != "devin-free" || row["cap_live"] != 2 || row["cap_limit"] != 2 {
		t.Fatalf("third launch free rung = %v, want dropped:pool_at_cap at devin-free with 2 live and limit 2", row)
	}
	if third.chosen.seatName != "devin-ds41" {
		t.Fatalf("third launch chose %s, want the paid rung devin-ds41", third.chosen.seatName)
	}
}

// [quota].free_band_per_account is refused at load unless it is an integer
// from 1 to 8, naming the key. An absent key reads as the default of one.
func TestFreeBandLimitConfigRefusedAtLoad(t *testing.T) {
	const refusal = "quota.free_band_per_account must be an integer from 1 to 8"
	for _, tc := range []struct {
		name, body string
		limit      int
		want       string
	}{
		{"absent", "", 1, ""},
		{"minimum", "free_band_per_account = 1", 1, ""},
		{"maximum", "free_band_per_account = 8", 8, ""},
		{"zero", "free_band_per_account = 0", 0, refusal},
		{"nine", "free_band_per_account = 9", 0, refusal},
		{"string_value", `free_band_per_account = "2"`, 0, refusal},
		{"float_value", "free_band_per_account = 2.5", 0, refusal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "hosts.toml")
			if err := os.WriteFile(path, []byte("[quota]\n"+tc.body+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(path)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				if got := freeBandPerAccount(cfg.Quota); got != tc.limit {
					t.Fatalf("free_band_per_account = %d, want %d", got, tc.limit)
				}
				return
			}
			if exitCode(err) != exitConfig || err == nil || err.Error() != tc.want {
				t.Fatalf("want exit %d %q, got %v", exitConfig, tc.want, err)
			}
		})
	}
}

// selectionAccountKey returns the account key the chosen rung ran on.
func selectionAccountKey(t *testing.T, sel *selPlan) string {
	t.Helper()
	chosen, _ := sel.record()["chosen"].(map[string]any)
	account, _ := chosen["account_key"].(string)
	if account == "" {
		t.Fatal("selection names no account")
	}
	return account
}

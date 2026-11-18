package fleet

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

// hk 5138 (decision 403): a commandcode account whose reading reports fewer
// than one remaining credit is refused by the gate, with the reason naming the
// balance; one credit or more is admitted, and a reading with no balance gates
// nothing (the 5h/7d windows decide as before).
func TestCommandcodeBalanceGate_t5138(t *testing.T) {
	for _, tc := range []struct {
		name    string
		balance *float64
		refused bool
		why     string
	}{
		{"no balance unchanged", nil, false, ""},
		{"half a credit", ptrF(0.5), true, "credit_exhausted balance=0.5"},
		{"exactly one credit", ptrF(1), false, ""},
		{"many credits", ptrF(17.5330931265), false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app, _, _ := switchApp(t, gateModeNative, nil, Result{Code: 99})
			app.Config.Local = map[string]any{"presence_id": "mac-personal"}
			app.Source = &fakePlacementSource{presence: []PresenceRecord{{HostID: "mac-personal", AccountKey: ccKeyA, LastSeen: switchNow.Add(-time.Minute)}}}
			reading := switchReading(10, 30)
			reading.Balance = tc.balance
			app.SharedLatest = func(context.Context, string) (shared.Latest, error) {
				return shared.Latest{Observation: reading}, nil
			}
			res := app.nativeGate(context.Background(), QuotaRequest{Profile: "cmd-dsflash41", Effort: "high", Role: "implementer"}, true)
			if tc.refused {
				if res.Code != gateBlocked || !strings.Contains(res.Out, tc.why) {
					t.Fatalf("gate = %d %s, want blocked with %q", res.Code, res.Out, tc.why)
				}
				return
			}
			if strings.Contains(res.Out, "credit_exhausted") {
				t.Fatalf("gate refused on a balance that is not exhausted: %d %s", res.Code, res.Out)
			}
			if res.Code != 0 {
				t.Fatalf("gate = %d %s, want admitted", res.Code, res.Out)
			}
		})
	}
}

// Selection takes the same per-account path: a balance below one drops the
// account with credit_exhausted balance=<n>, leaving the pool's other accounts
// placeable; a mark from a pane rejection is lifted by a reading with credits.
func TestCommandcodeBalanceSelection_t5138(t *testing.T) {
	a0, _ := ccAccountsFixture(t, map[string]*PoolView{})
	now := a0.Now()
	withBalance := func(view *PoolView, balance *float64) *PoolView {
		view.Balance = balance
		return view
	}

	t.Run("below one credit drops only that account", func(t *testing.T) {
		a, s := ccAccountsFixture(t, map[string]*PoolView{
			ccKeyA: withBalance(ccView(now, 10, 30, 3), ptrF(0.5)),
			ccKeyB: withBalance(ccView(now, 10, 40, 3), ptrF(17)),
		})
		sel, err := a.selectProfile(context.Background(), s)
		if err != nil || sel.chosen == nil {
			t.Fatalf("no choice: %v", err)
		}
		if c := ccCandidates(sel)[ccKeyA]; c.outcome != "dropped:credit_exhausted balance=0.5" {
			t.Fatalf("account A outcome = %q, want dropped:credit_exhausted balance=0.5", c.outcome)
		}
		if sel.chosen.accountKey != ccKeyB {
			t.Fatalf("chosen account %s, want %s", sel.chosen.accountKey, ccKeyB)
		}
	})

	t.Run("one credit is admitted", func(t *testing.T) {
		a, s := ccAccountsFixture(t, map[string]*PoolView{
			ccKeyA: withBalance(ccView(now, 10, 30, 3), ptrF(1)),
			ccKeyB: withBalance(ccView(now, 10, 40, 3), ptrF(17)),
		})
		sel, err := a.selectProfile(context.Background(), s)
		if err != nil || sel.chosen == nil {
			t.Fatalf("no choice: %v", err)
		}
		if c := ccCandidates(sel)[ccKeyA]; strings.HasPrefix(c.outcome, "dropped:") {
			t.Fatalf("one credit was dropped: %s", c.outcome)
		}
	})

	t.Run("a reading with no balance leaves the mark in force", func(t *testing.T) {
		a, s := ccAccountsFixture(t, map[string]*PoolView{
			ccKeyA: ccView(now, 10, 30, 3),
			ccKeyB: ccView(now, 10, 40, 3),
		})
		a.State = &fakeStateReader{snap: FleetSnapshot{V: 1, At: now, AccountCreditExhausted: map[string]time.Time{ccKeyA: now.Add(-time.Hour)}}}
		sel, err := a.selectProfile(context.Background(), s)
		if err != nil || sel.chosen == nil {
			t.Fatalf("no choice: %v", err)
		}
		if c := ccCandidates(sel)[ccKeyA]; c.outcome != "dropped:credit_exhausted account="+ccKeyA {
			t.Fatalf("account A outcome = %q, want the mark in force", c.outcome)
		}
	})

	t.Run("a stale reading before the mark does not lift it", func(t *testing.T) {
		a, s := ccAccountsFixture(t, map[string]*PoolView{
			ccKeyA: withBalance(ccView(now, 10, 30, 3), ptrF(12)),
			ccKeyB: withBalance(ccView(now, 10, 40, 3), ptrF(17)),
		})
		// The fixture's reading is measured now-1m; a mark 30 s ago is newer,
		// so the cached balance predates the rejection and must not lift it.
		a.State = &fakeStateReader{snap: FleetSnapshot{V: 1, At: now, AccountCreditExhausted: map[string]time.Time{ccKeyA: now.Add(-30 * time.Second)}}}
		sel, err := a.selectProfile(context.Background(), s)
		if err != nil || sel.chosen == nil {
			t.Fatalf("no choice: %v", err)
		}
		if c := ccCandidates(sel)[ccKeyA]; c.outcome != "dropped:credit_exhausted account="+ccKeyA {
			t.Fatalf("a pre-mark reading lifted the mark: %s", c.outcome)
		}
	})

	t.Run("a reading with credits lifts the mark", func(t *testing.T) {
		a, s := ccAccountsFixture(t, map[string]*PoolView{
			ccKeyA: withBalance(ccView(now, 10, 30, 3), ptrF(2)),
			ccKeyB: withBalance(ccView(now, 10, 40, 3), ptrF(17)),
		})
		a.State = &fakeStateReader{snap: FleetSnapshot{V: 1, At: now, AccountCreditExhausted: map[string]time.Time{ccKeyA: now.Add(-time.Hour)}}}
		sel, err := a.selectProfile(context.Background(), s)
		if err != nil || sel.chosen == nil {
			t.Fatalf("no choice: %v", err)
		}
		if c := ccCandidates(sel)[ccKeyA]; strings.HasPrefix(c.outcome, "dropped:") {
			t.Fatalf("a reading with credits did not lift the mark: %s", c.outcome)
		}
	})
}

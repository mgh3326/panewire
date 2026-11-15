package fleet

import (
	"context"
	"slices"
	"testing"
	"time"
)

// Credit pacing (decision/2026-11-04/pool-order-owner §3): a subscription
// pool whose longest window is on pace to be used up by its reset ranks behind
// a credit pool that has a recorded balance. The free seat is refused here so
// the subscription pool and the credit pool compete directly.
func TestCreditPacingRanksPacedPoolBehindCredit(t *testing.T) {
	month := 30 * 24 * time.Hour
	// claude's week projects to 120% at reset; its 5h window is not exhausted.
	paced := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(60), Projected: ptrF(120), RankProjected: ptrF(120), ShortUsed: ptrF(40)}
	// claude's week projects to 90%: it leaves quota unused at reset.
	unused := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(60), Projected: ptrF(90), RankProjected: ptrF(90), ShortUsed: ptrF(40)}
	// The 5h window paces to 450% but the week projects to 60%: sub-daily
	// windows never pace a pool.
	fastShort := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(30), Projected: ptrF(450), RankProjected: ptrF(60), ShortUsed: ptrF(90)}
	noFree := map[string]string{"sonnet": "claude", "omp-opus": "anthropic-api"}
	cases := []struct {
		name         string
		credit       func(time.Time) *CreditBalance
		claude       PoolView
		gates        map[string]string
		want         string
		paced        bool
		behindCredit bool
	}{
		{"paced pool, credit balance recorded → credit first", balance(80, month), paced, noFree, "omp-opus", true, true},
		{"paced pool, no credit balance recorded → credit still ranks in the free band", noBalance, paced, noFree, "omp-opus", true, false},
		{"paced pool, credit balance zero → credit excluded", balance(0, month), paced, noFree, "sonnet", true, false},
		{"paced pool, credit gate refuses → the paced pool is next", balance(80, month), paced, map[string]string{"sonnet": "claude"}, "sonnet", true, true},
		{"pool leaves quota unused without urgency → credit first", balance(80, month), unused, noFree, "omp-opus", false, false},
		{"fast 5h window, week leaves quota → not paced", balance(80, month), fastShort, noFree, "omp-opus", false, false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			a, s := creditFixture(t, tt.credit, tt.claude, "", tt.gates)
			sel, err := a.selectProfile(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			if sel.chosen.seatName != tt.want {
				t.Fatalf("want %s, got %s (ranked %v)", tt.want, sel.chosen.seatName, rankedSeats(sel))
			}
			row := candOutcome(sel.record(), "sonnet", "high")
			if got, _ := row["paced"].(bool); got != tt.paced {
				t.Fatalf("claude paced: want %t, got %v", tt.paced, row)
			}
			if tt.paced && row["behind_credit"] != tt.behindCredit {
				t.Fatalf("claude behind_credit: want %t, got %v", tt.behindCredit, row)
			}
			if !tt.paced && row["behind_credit"] != nil {
				t.Fatalf("an unpaced pool records no behind_credit: %v", row)
			}
			if r, _ := row["reason"].(string); r != "" {
				t.Fatalf("a pool on pace is never excluded: %v", row)
			}
			seats := rankedSeats(sel)
			// Credit ranks in the free band, so a paced subscription pool
			// never ranks ahead of a present credit rung, whether or not the
			// credit-pacing flag demoted it.
			if credit, sub := slices.Index(seats, "omp-opus"), slices.Index(seats, "sonnet"); tt.paced && credit >= 0 && sub >= 0 && credit > sub {
				t.Fatalf("ranked order %v: paced pool must not precede credit", seats)
			}
		})
	}
}

// A window already used up still excludes a pool, paced or not.
func TestCreditPacingExhaustedWindowStillExcludes(t *testing.T) {
	for name, claude := range map[string]PoolView{
		"5h used up":   {Status: "ok", Class: "spend", Blocking: ptrF(100), Projected: ptrF(120), RankProjected: ptrF(80), ShortUsed: ptrF(100)},
		"week used up": {Status: "ok", Class: "spend", Blocking: ptrF(100), Projected: ptrF(100), RankProjected: ptrF(100)},
	} {
		t.Run(name, func(t *testing.T) {
			a, s := creditFixture(t, noBalance, claude, "", map[string]string{"sonnet": "claude", "omp-opus": "anthropic-api"})
			sel, err := a.selectProfile(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			if got := candOutcome(sel.record(), "sonnet", "high")["outcome"]; got != "dropped:window_exhausted" {
				t.Fatalf("a used-up window excludes the pool: %v", got)
			}
			if sel.chosen.seatName != "omp-opus" {
				t.Fatalf("credit is the fallback once subscription quota is used up, got %s", sel.chosen.seatName)
			}
		})
	}
}

func rankedSeats(sel *selPlan) []string {
	var out []string
	for _, c := range sel.ranked {
		out = append(out, c.seatName)
	}
	return out
}

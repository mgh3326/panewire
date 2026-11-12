package fleet

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestManualCreditBalanceStalesAfterFourHours(t *testing.T) {
	now := time.Now().UTC()
	measured := now.Add(-4 * time.Hour)
	entry := map[string]any{
		"manual_observation_id": "a1", "event": "set", "pool": "anthropic-api", "window": "credit",
		"used_pct": 50.0, "measured_at": measured.Format(time.RFC3339Nano), "expires_at": now.Add(24 * time.Hour).Format(time.RFC3339Nano),
		"credit": map[string]any{"remaining_usd": 100.0, "total_usd": 200.0, "expires_on": now.Add(24 * time.Hour).Format("2006-01-02")},
	}
	latest := map[string]string{"anthropic-api:credit": "a1"}
	store, _ := jsonCreditStore(latest, []map[string]any{entry})
	q := CLIQuota{Binary: "pacelever", Now: func() time.Time { return now }, Runner: &fakeRunner{Do: func(c Command) Result { return Result{Out: store} }}}
	balances, err := q.creditBalances(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	credit := balances["anthropic-api"]
	if credit == nil || credit.BalanceUSD != nil {
		t.Fatalf("stale balance = %+v", credit)
	}
}

func TestPercentOnlyCreditDoesNotBecomeStale(t *testing.T) {
	now := time.Date(2026, 11, 12, 6, 0, 0, 0, time.UTC)
	entry := map[string]any{
		"manual_observation_id": "a1", "event": "set", "pool": "anthropic-api", "window": "credit",
		"used_pct": 50.0, "measured_at": now.Add(-5 * time.Hour).Format(time.RFC3339Nano),
		"expires_at": now.Add(24 * time.Hour).Format(time.RFC3339Nano),
	}
	store, _ := jsonCreditStore(map[string]string{"anthropic-api:credit": "a1"}, []map[string]any{entry})
	q := CLIQuota{Binary: "pacelever", Now: func() time.Time { return now }, Runner: &fakeRunner{Do: func(Command) Result { return Result{Out: store} }}}
	balances, err := q.creditBalances(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	credit := balances["anthropic-api"]
	if credit == nil || !credit.StaleAt.IsZero() || credit.BalanceUSD != nil {
		t.Fatalf("percent-only credit = %+v, want no dollar balance and no stale timestamp", credit)
	}
}

func TestStaleCreditDoesNotRankAsDollarsAtRisk(t *testing.T) {
	a, s, _ := usdCreditFixture(t, func(now time.Time) []map[string]any {
		return []map[string]any{usdCreditEntry("a1", "anthropic-api", 25, ptrF(300), now.Add(-5*time.Hour), onDay(now, 9*24*time.Hour))}
	}, ptrPool(PoolView{Status: "ok", Class: "spend", RankProjected: ptrF(100)}), false)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if sel.chosen.seatName != "sonnet" {
		t.Fatalf("stale dollar balance ranked ahead of subscription: chose %s", sel.chosen.seatName)
	}
	credit := creditTrace(t, sel.record(), "omp-opus")
	if credit["state"] != "stale" || credit["stale_since"] != a.Now().Add(-time.Hour).Format(time.RFC3339) || credit["balance_usd"] != nil || credit["at_risk_usd"] != nil {
		t.Fatalf("stale credit trace = %v", credit)
	}
}

func TestStaleZeroDollarCreditRemainsExhausted(t *testing.T) {
	a, s, _ := usdCreditFixture(t, func(now time.Time) []map[string]any {
		return []map[string]any{usdCreditEntry("a1", "anthropic-api", 100, ptrF(0), now.Add(-5*time.Hour), onDay(now, 9*24*time.Hour))}
	}, ptrPool(PoolView{Status: "ok", Class: "spend", RankProjected: ptrF(50)}), false)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if sel.chosen.seatName != "sonnet" {
		t.Fatalf("stale zero-left credit remained a candidate: chose %s", sel.chosen.seatName)
	}
	row := candOutcome(sel.record(), "omp-opus", "high")
	if row["reason"] != "credit_exhausted" || row["outcome"] != "dropped:credit_exhausted" {
		t.Fatalf("zero-left plan outcome = %v, want credit_exhausted", row)
	}
}

func jsonCreditStore(latest map[string]string, history []map[string]any) (string, error) {
	b, err := json.Marshal(map[string]any{"schema": "pacelever.manual.v1", "latest": latest, "history": history})
	return string(b), err
}

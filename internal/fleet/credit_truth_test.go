package fleet

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

func TestCreditExhaustionScreenStopsWorkerImmediately(t *testing.T) {
	now := time.Date(2026, 11, 12, 6, 0, 0, 0, time.UTC)
	screen := "API Error: 400 {\"type\":\"error\",\"error\":{\"message\":\"Your credit balance is too low to access the Anthropic API\"}}\n❯ Ask Claude"
	a, _, h, _ := quotaStopFixture(t, "worker", "idle", screen)
	putEvent(t, a, "j", "job.spawned", map[string]any{"pane_id": "w:p1", "profile": "omp-opus", "pool": "anthropic-api", "role": "worker"})
	a.Now = func() time.Time { return now }
	j, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	stop, ok := a.quotaStopped(context.Background(), j, Agent{Pane: "w:p1", Status: "idle"}, &quotaStopWatch{})
	if !ok || !stop.CreditExhausted || stop.Pool != "anthropic-api" {
		t.Fatalf("provider stop = %+v, %t", stop, ok)
	}
	if err := a.quotaStopLost(context.Background(), j, stop, true); err != nil {
		t.Fatal(err)
	}
	if got := str(h.record(t, "j"), "end_reason"); got != "quota_stop pool=anthropic-api provider_credit_exhausted=true exhausted_at=2026-11-12T06:00:00Z" {
		t.Fatalf("end_reason = %q", got)
	}
}

func TestProviderCreditExhaustionTexts(t *testing.T) {
	for _, tc := range []struct {
		name, text, pool string
	}{
		{"Anthropic", "Your credit balance is too low to access the Anthropic API", "anthropic-api"},
		{"OpenAI insufficient quota", "insufficient_quota", "openai-api"},
		{"OpenAI current quota", "You exceeded your current quota", "openai-api"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			screen := "API Error: " + tc.text + "\n❯ Ask Claude"
			stop, ok := quotastop.Classify(screen, time.Now().UTC(), time.UTC)
			if !ok || !stop.CreditExhausted || stop.Pool != tc.pool {
				t.Fatalf("classified stop = %+v, %t", stop, ok)
			}
		})
	}
}

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

func TestCreditExhaustionBlocksUntilFreshManualEntry(t *testing.T) {
	for _, tc := range []struct {
		name      string
		measured  time.Duration
		wantSeat  string
		wantState string
	}{
		{name: "exhausted", measured: -time.Hour, wantSeat: "sonnet", wantState: "provider_credit_exhausted"},
		{name: "fresh manual entry clears", measured: -10 * time.Minute, wantSeat: "omp-opus"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, _ := usdCreditFixture(t, func(now time.Time) []map[string]any {
				return []map[string]any{usdCreditEntry("a1", "anthropic-api", 50, ptrF(100), now.Add(tc.measured), onDay(now, 9*24*time.Hour))}
			}, ptrPool(riskPool(20, 8)), false)
			exhaustedAt := a.Now().Add(-30 * time.Minute)
			a.State = &fakeStateReader{snap: FleetSnapshot{V: 1, At: a.Now(), CreditExhausted: map[string]time.Time{"anthropic-api": exhaustedAt}}}
			sel, err := a.selectProfile(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			if sel.chosen.seatName != tc.wantSeat {
				t.Fatalf("chosen %s, want %s", sel.chosen.seatName, tc.wantSeat)
			}
			row := candOutcome(sel.record(), "omp-opus", "high")
			if tc.wantState != "" {
				if row["reason"] != tc.wantState || row["credit_exhausted_since"] != exhaustedAt.Format(time.RFC3339) {
					t.Fatalf("exhausted plan row = %v", row)
				}
			} else if row["reason"] == "provider_credit_exhausted" {
				t.Fatalf("fresh manual entry did not clear exhaustion: %v", row)
			} else if credit := creditTrace(t, sel.record(), "omp-opus"); credit["state"] != "live" || credit["balance_usd"] != 100.0 {
				t.Fatalf("fresh manual entry was not restored: %v", credit)
			}
		})
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

func jsonCreditStore(latest map[string]string, history []map[string]any) (string, error) {
	b, err := json.Marshal(map[string]any{"schema": "pacelever.manual.v1", "latest": latest, "history": history})
	return string(b), err
}

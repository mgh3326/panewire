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
		name, status, text, pool string
	}{
		{"Anthropic", "400", "Your credit balance is too low to access the Anthropic API", "anthropic-api"},
		{"OpenAI insufficient quota", "429", "insufficient_quota", "openai-api"},
		{"OpenAI current quota", "429", "You exceeded your current quota", "openai-api"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			screen := "API Error: " + tc.status + " " + tc.text + "\n❯ Ask Claude"
			stop, ok := quotastop.Classify(screen, time.Now().UTC(), time.UTC)
			if !ok || !stop.CreditExhausted || stop.Pool != tc.pool {
				t.Fatalf("classified stop = %+v, %t", stop, ok)
			}
		})
	}
}

func TestQuotedProviderCreditErrorIsNotExhaustion(t *testing.T) {
	screen := "Here is the error I saw earlier: API Error: 429 insufficient_quota\n❯ Ask Codex"
	if stop, ok := quotastop.Classify(screen, time.Now().UTC(), time.UTC); ok {
		t.Fatalf("quoted provider error classified as stop = %+v", stop)
	}
}

func TestWrappedProviderCreditErrorClassifies(t *testing.T) {
	for _, tc := range []struct {
		name, screen, pool string
	}{
		{
			name:   "Anthropic with border prefix",
			screen: "└ API Error: 400 invalid_request_error {\"type\":\"error\",\n└ \"error\":{\"message\":\"Your credit balance is too low to access\n\n└ ─────────\n└ the Anthropic API\"}}\n└ ❯ Ask Claude",
			pool:   "anthropic-api",
		},
		{
			name:   "Anthropic plain text wrapped by border",
			screen: "│ API Error: 400 Your credit balance is too low to access the\n│ Anthropic API. Please go to Plans & Billing to upgrade or purchase credits.\n│ ❯ Ask Claude",
			pool:   "anthropic-api",
		},
		{
			name:   "Anthropic text and standard continuation wrapped by border",
			screen: "│ API Error: 400 Your credit balance is too low to access the Anthropic API. Please go to\n│ Plans & Billing to upgrade.\n│ ❯ Ask Claude",
			pool:   "anthropic-api",
		},
		{
			name:   "Anthropic request annotation wrapped without JSON",
			screen: "API Error: 400 Your credit balance is too low to access the Anthropic API [request:\nreq_abc]\n❯ Ask Claude",
			pool:   "anthropic-api",
		},
		{
			name:   "Anthropic JSON scalar continuation",
			screen: "API Error: 400 {\"error\":{\"message\":\"Your credit balance is too low to access the Anthropic API\",\"status\":\n30\n}}\n❯ Ask Claude",
			pool:   "anthropic-api",
		},
		{
			name:   "Anthropic unbordered JSON wrap",
			screen: "API Error: 400 invalid_request_error {\"type\":\"error\",\"error\":{\"message\":\"Your credit balance is too low to access\nthe Anthropic API\"}}\n❯ Ask Claude",
			pool:   "anthropic-api",
		},
		{
			name:   "OpenAI wrapped body",
			screen: "| API Error: 429 {\"error\":{\n| \"type\":\"insufficient_\n| quota\"}}\n| › Ask Codex",
			pool:   "openai-api",
		},
		{
			name:   "OpenAI unbordered token wrap",
			screen: "API Error: 429 {\"error\":{\"type\":\"insufficient_\nquota\"}}\n› Ask Codex",
			pool:   "openai-api",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stop, ok := quotastop.Classify(tc.screen, time.Now().UTC(), time.UTC)
			if !ok || !stop.CreditExhausted || stop.Pool != tc.pool {
				t.Fatalf("wrapped provider stop = %+v, %t", stop, ok)
			}
		})
	}
}

func TestProviderCreditErrorRequiresLastActivity(t *testing.T) {
	for _, screen := range []string{
		"API Error: 429 {\"error\":{\"type\":\"insufficient_quota\"}}\nRetried; the call succeeded\n❯ Ask Claude",
		`API Error: 400 {"error":{"message":"Your credit balance is too low to access the Anthropic API"
Retried; the call succeeded
}}
❯ Ask Claude`,
		"│ API Error: 429 insufficient_quota\n│ Retried the request and it succeeded before\n│ ❯ Ask Codex",
		"API Error: 429 insufficient_quota\n[retry] succeeded\n› Ask Codex",
		"| API Error: 429 insufficient_quota\n| Retried; the call succeeded\n| ⎿ file1\n| › Ask Codex",
		"API Error: 429 insufficient_quota\n}\n❯ Ask Codex",
		"│ API Error: 429 rate_limit_error\n│ note: your credit balance is too low to access the Anthropic API\n│ ❯ Ask Claude",
		"API Error: 400 invalid_request_error {\"message\":\"Your credit balance is too low to access the Anthropic API\"}\nAPI Error: 500 internal_error\n❯ Ask Claude",
		"API Error: 429 {\"error\":{\"type\":\"insufficient_quota\"}}",
		"┌ API Error: 429 insufficient_quota\n│ boxed quote text\n└ ❯ Ask Codex",
	} {
		if stop, ok := quotastop.Classify(screen, time.Now().UTC(), time.UTC); ok {
			t.Errorf("superseded or quoted provider error classified as stop = %+v", stop)
		}
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
			}, ptrPool(PoolView{Status: "ok", Class: "spend", RankProjected: ptrF(50)}), false)
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

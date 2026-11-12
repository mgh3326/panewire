package fleet

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestCreditExhaustionScreenStopsWorkerImmediately(t *testing.T) {
	now := time.Date(2026, 11, 12, 6, 0, 0, 0, time.UTC)
	screen := "API Error: 400 {\"type\":\"error\",\"error\":{\"message\":\"Your credit balance is too low to access the Anthropic API\"}}\n❯ Ask omp"
	a, _, h, _ := quotaStopFixture(t, "worker", "idle", screen)
	a.Now = func() time.Time { return now }
	a.QuotaHandOffGrace = 10 * time.Minute
	if err := a.watch(context.Background(), []string{"j"}); err != nil {
		t.Fatal(err)
	}
	if got := str(h.record(t, "j"), "end_reason"); got != "quota_stop pool=anthropic-api provider_credit_exhausted=true" {
		t.Fatalf("end_reason = %q", got)
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
	q := CLIQuota{Binary: "pacelever", Runner: &fakeRunner{Do: func(c Command) Result { return Result{Out: store} }}}
	balances, err := q.creditBalances(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	credit := balances["anthropic-api"]
	if credit == nil || credit.BalanceUSD != nil {
		t.Fatalf("stale balance = %+v", credit)
	}
}

func jsonCreditStore(latest map[string]string, history []map[string]any) (string, error) {
	b, err := json.Marshal(map[string]any{"schema": "pacelever.manual.v1", "latest": latest, "history": history})
	return string(b), err
}

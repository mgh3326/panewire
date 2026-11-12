package fleet

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestCreditExhaustionScreenStopsWorkerImmediately(t *testing.T) {
	now := time.Date(2026, 11, 12, 6, 0, 0, 0, time.UTC)
	screen := "API Error: 400 {\"type\":\"error\",\"error\":{\"message\":\"Your credit balance is too low to access the Anthropic API\"}}\n❯ Ask Claude"
	a, _, _, _ := quotaStopFixture(t, "worker", "idle", screen)
	putEvent(t, a, "j", "job.spawned", map[string]any{"pane_id": "w:p1", "profile": "omp-opus", "pool": "anthropic-api", "role": "worker"})
	a.Now = func() time.Time { return now }
	j, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	stop, ok := a.quotaStopped(context.Background(), j, Agent{Pane: "w:p1", Status: "idle"}, &quotaStopWatch{})
	if !ok || stop.Pool != "anthropic-api" {
		t.Fatalf("provider stop = %+v, %t", stop, ok)
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
	store, _ := json.Marshal(map[string]any{"schema": "pacelever.manual.v1", "latest": latest, "history": []map[string]any{entry}})
	q := CLIQuota{Binary: "pacelever", Runner: &fakeRunner{Do: func(c Command) Result { return Result{Out: string(store)} }}}
	balances, err := q.creditBalances(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	credit := balances["anthropic-api"]
	if credit == nil || credit.BalanceUSD != nil {
		t.Fatalf("stale balance = %+v", credit)
	}
}

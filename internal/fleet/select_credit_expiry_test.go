package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// creditFixture offers the omp-opus credit seat (anthropic-api), the free
// SWE-2 seat with room and the sonnet subscription seat (claude); every gate
// admits unless gates says otherwise.
func creditFixture(t *testing.T, credit func(now time.Time) *CreditBalance, claude PoolView, policyExtra string, gates map[string]string) (*App, Spawn) {
	t.Helper()
	rows := append(riskRows(), ompRow("omp-opus", "anthropic/claude-opus-5-5", "anthropic-api"))
	a, s, q := selFixture(t, "A", rows, nil, []string{"devin", "claude", "anthropic-api"})
	serveOMPRouting(t, a)
	q.PoolViews = map[string]PoolView{"devin": {Status: "ok", Class: "spend", Blocking: ptrF(4)}, "claude": claude, "anthropic-api": {Status: "ok", Class: "spend", Credit: credit(a.Now())}}
	if gates == nil {
		gates = map[string]string{"devin-swe2": "devin", "sonnet": "claude", "omp-opus": "anthropic-api"}
	}
	q.GateDo = gatePools(gates)
	path := filepath.Join(t.TempDir(), "pool-order.json")
	if e := os.WriteFile(path, []byte(riskPolicy(policyExtra)), 0o600); e != nil {
		t.Fatal(e)
	}
	a.Config.Local["pool_policy"] = path
	return a, s
}

func balance(left float64, in time.Duration) func(time.Time) *CreditBalance {
	return func(now time.Time) *CreditBalance {
		return &CreditBalance{LeftPct: left, BalanceUSD: ptrF(left * 2), ExpiresAt: now.Add(in)}
	}
}

func noBalance(time.Time) *CreditBalance { return nil }

// pctOnly is a credit entry with no recorded dollars: the percentage rule
// still ranks it, after any credit that has dollars.
func pctOnly(left float64, in time.Duration) func(time.Time) *CreditBalance {
	return func(now time.Time) *CreditBalance {
		return &CreditBalance{LeftPct: left, ExpiresAt: now.Add(in)}
	}
}

// A waste-risk subscription pool goes first whatever the credit's expiry
// urgency: credit ranks in the free band, after waste.
func TestCreditUrgencyRanksInsideFreeBand(t *testing.T) {
	for _, hours := range []float64{100, 300} {
		// 77/220 = 0.35; the subscription's unused share is 80%.
		a, s := creditFixture(t, pctOnly(77, 220*time.Hour), riskPool(20, hours), `, "credit_final_days": 30`, nil)
		sel, err := a.selectProfile(context.Background(), s)
		if err != nil {
			t.Fatal(err)
		}
		if sel.chosen.seatName != "sonnet" || sel.chosen.tierName != "waste" {
			t.Fatalf("hours=%g: chosen %s in %s, want the waste pool", hours, sel.chosen.seatName, sel.chosen.tierName)
		}
		row := candOutcome(sel.record(), "omp-opus", "high")
		if row["waste_urgency"] != 0.35 || row["waste_risk"] != false || row["tier"] != "credit" {
			t.Fatalf("credit urgency in the free band: %v", row)
		}
	}
}

// A credit entry without an expiry still ranks in the free band, ahead of a
// ranked subscription pool. It sits behind the free rung on the in-tier
// projection tie-break (devin's blocking share is lower here).
func TestCreditWithoutExpiryRanksWithFree(t *testing.T) {
	noExpiry := func(time.Time) *CreditBalance { return &CreditBalance{LeftPct: 77} }
	ranked := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(10)}
	a, s := creditFixture(t, noExpiry, ranked, "", nil)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if got := candOutcome(sel.record(), "omp-opus", "high")["tier"]; got != "credit" {
		t.Fatalf("credit without expiry must rank in the free band: %v", got)
	}
	seats := rankedSeats(sel)
	if credit, sub := slices.Index(seats, "omp-opus"), slices.Index(seats, "sonnet"); credit < 0 || sub < 0 || credit > sub {
		t.Fatalf("ranked order %v: credit must precede the ranked pool", seats)
	}
}

func TestPoolPolicyCreditFinalDaysDeprecated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p.json")
	if err := os.WriteFile(path, []byte(riskPolicy(`, "credit_final_days": 14`)), 0o600); err != nil {
		t.Fatal(err)
	}
	a, _ := creditFixture(t, balance(77, 220*time.Hour), riskPool(20, 100), `, "credit_final_days": 14`, nil)
	var warning bytes.Buffer
	a.Err = &warning
	if _, err := a.poolPolicy(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(warning.String(), "deprecated") {
		t.Fatalf("warning: %s", warning.String())
	}
}

func TestPlanCreditAt(t *testing.T) {
	a, _ := creditFixture(t, balance(80, 240*time.Hour), riskPool(32.6, 8), "", nil)
	out := &bytes.Buffer{}
	a.Out = out
	at := a.Now().Add(8 * 24 * time.Hour).UTC().Format(time.RFC3339)
	_ = a.planCmd(context.Background(), []string{"--grade", "A", "--t", "T1", "--credit-at", at})
	if !strings.Contains(out.String(), "credit_expiring days_left=2.0") {
		t.Fatalf("plan: %s", out.String())
	}
}

// The balance comes from pacelever's manual store: the current credit entry
// and its expiry.
func TestCLIQuotaPoolsReadsCreditBalance(t *testing.T) {
	now := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	status, _ := json.Marshal(map[string]any{"schema": "scopefuel.v1", "generated_at": now.Format(time.RFC3339Nano), "providers": []map[string]any{
		{"id": "anthropic-api", "status": "ok", "pool_class": "spend", "buckets": []map[string]any{{"window": "credit", "used_pct": 25, "scope": map[string]any{"kind": "account"}}}},
	}})
	store, _ := json.Marshal(map[string]any{
		"schema": "pacelever.manual.v1",
		"latest": map[string]string{"anthropic-api:credit": "a2", "openai-api:credit": "o1", "claude:7d": "c1"},
		"history": []map[string]any{
			{"manual_observation_id": "a1", "event": "set", "pool": "anthropic-api", "window": "credit", "used_pct": 20, "measured_at": now.Add(-2 * time.Hour).Format(time.RFC3339Nano), "expires_at": "2026-11-16T00:00:00Z"},
			{"manual_observation_id": "a2", "event": "set", "pool": "anthropic-api", "window": "credit", "used_pct": 25, "measured_at": now.Add(-time.Hour).Format(time.RFC3339Nano), "expires_at": "2026-11-16T00:00:00Z",
				"credit": map[string]any{"remaining_usd": 150, "total_usd": 200, "expires_on": "2026-11-16"}},
			// A shared entry: no dollars.
			{"manual_observation_id": "o1", "event": "set", "pool": "openai-api", "window": "credit", "used_pct": 5, "measured_at": now.Add(-time.Hour).Format(time.RFC3339Nano), "expires_at": "2026-11-18T00:00:00Z"},
			{"manual_observation_id": "c1", "event": "set", "pool": "claude", "window": "7d", "used_pct": 30, "expires_at": now.Add(time.Hour).Format(time.RFC3339Nano)},
		},
	})
	run := func(manualOK bool) map[string]PoolView {
		q := CLIQuota{Binary: "pacelever", Now: func() time.Time { return now }, Runner: &fakeRunner{Do: func(c Command) Result {
			if strings.Join(c.Args, " ") == "manual list" {
				if !manualOK {
					return Result{Code: 2, Err: "shared manual observations unavailable"}
				}
				return Result{Out: string(store)}
			}
			return Result{Out: string(status)}
		}}}
		pools, err := q.Pools(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return pools
	}
	pools := run(true)
	if c := pools["anthropic-api"].Credit; c == nil || c.LeftPct != 75 || c.BalanceUSD == nil || *c.BalanceUSD != 150 || !c.ExpiresAt.Equal(time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("anthropic-api current entry: %+v", c)
	}
	if c := pools["openai-api"].Credit; c == nil || c.LeftPct != 95 || c.BalanceUSD != nil || !c.ExpiresAt.Equal(time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("shared openai-api entry: %+v", c)
	}
	if pools["claude"].Credit != nil {
		t.Fatal("only credit-window entries carry a balance")
	}
	if pools := run(false); pools["anthropic-api"].Credit != nil || pools["anthropic-api"].Status != "ok" {
		t.Fatalf("an unreadable store leaves the status view without a balance: %+v", pools["anthropic-api"])
	}
}

// Final-window credit affects rung order inside a host, never host rank.
func TestCreditFinalWindowHostFirst(t *testing.T) {
	const creditKey = "anthropic-api:00000000000000d4"
	for _, tc := range []struct {
		name               string
		in                 time.Duration
		wantHost, wantSeat string
		outcomes           map[string]string
	}{
		{"final window", 72 * time.Hour, "mac-b", "sonnet", map[string]string{"mac-b": "chosen", "cloud-small": "not_tried"}},
		{"outside the window", 240 * time.Hour, "mac-b", "sonnet", map[string]string{"mac-b": "chosen", "cloud-small": "not_tried"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := []CatalogRow{ompRow("sonnet", "claude-sonnet-5-5", "claude"), ompRow("omp-opus", "anthropic/claude-opus-5-5", "anthropic-api")}
			a, s, q, _ := hostFirstFixture(t, "implementer", "A", rows, map[string]string{"mac-b": testClaudeA, "cloud-small": creditKey}, map[string]float64{"mac-b": 0.1, "cloud-small": 0.5})
			serveOMPRouting(t, a)
			q.PoolViews = map[string]PoolView{"anthropic-api": {Status: "ok", Class: "spend", Credit: balance(80, tc.in)(a.Now())}}
			q.GateDo = gatePools(map[string]string{"sonnet": "claude", "omp-opus": "anthropic-api"})
			sel, err := a.selectProfile(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			if sel.chosen.host != tc.wantHost || sel.chosen.seatName != tc.wantSeat {
				t.Fatalf("placed %s on %s, want %s on %s", sel.chosen.seatName, sel.chosen.host, tc.wantSeat, tc.wantHost)
			}
			for host, want := range tc.outcomes {
				if got := hostRow(t, sel, host)["outcome"]; got != want {
					t.Fatalf("host %s outcome %v, want %s", host, got, want)
				}
			}
		})
	}
}

package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
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

// (3) In the final window before expiry (default 4 days) a credit pool with a
// balance left ranks first, ahead of subscription waste and the free seat.
func TestCreditFinalWindowRanksFirst(t *testing.T) {
	cases := []struct {
		name   string
		credit func(time.Time) *CreditBalance
		policy string
	}{
		{"3 days left, default 4-day window", balance(80, 72*time.Hour), ""},
		{"a little balance is still used up", balance(0.5, 24*time.Hour), ""},
		{"credit_final_days 14 takes in 10 days left", balance(80, 240*time.Hour), `, "credit_final_days": 14`},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			// claude is at waste risk with urgency 67.4/8 = 8.4 per hour.
			a, s := creditFixture(t, tt.credit, riskPool(32.6, 8), tt.policy, nil)
			sel, err := a.selectProfile(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			if sel.chosen.seatName != "omp-opus" {
				t.Fatalf("final-window credit ranks first, got %s", sel.chosen.seatName)
			}
			if second := sel.ranked[1].seatName; second != "sonnet" {
				t.Fatalf("subscription waste ranks next, got %s", second)
			}
			row := candOutcome(sel.record(), "omp-opus", "high")
			c, _ := row["credit"].(map[string]any)
			if row["tier"] != "waste" || row["waste_reason"] != "credit_expiring" || c == nil || c["days_left"] == nil || c["final_window_days"] == nil || c["balance_pct"] == nil {
				t.Fatalf("record states the final-window reason, days left and balance: %v", row)
			}
			if !strings.HasPrefix(creditNote(sel.chosen), " credit_expiring final_window_days=") {
				t.Fatalf("plan text: %q", creditNote(sel.chosen))
			}
		})
	}
}

// (1) Outside the final window credit ranks below every admitted pool,
// however much of it is left and however little has been spent.
func TestCreditOutsideFinalWindowRanksLast(t *testing.T) {
	cases := []struct {
		name   string
		credit func(time.Time) *CreditBalance
		claude PoolView
		policy string
	}{
		{"10 days left", balance(95, 240*time.Hour), riskPool(32.6, 8), ""},
		{"10 days left, subscription ranked (not waste)", balance(95, 240*time.Hour), riskPool(85, 100), ""},
		{"no balance or expiry recorded", noBalance, riskPool(32.6, 8), ""},
		{"balance used up in the window", balance(0, 24*time.Hour), riskPool(32.6, 8), ""},
		{"already expired", balance(80, -time.Hour), riskPool(32.6, 8), ""},
		{"credit_final_days 0 disables the window", balance(80, 24*time.Hour), riskPool(32.6, 8), `, "credit_final_days": 0`},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			a, s := creditFixture(t, tt.credit, tt.claude, tt.policy, nil)
			sel, err := a.selectProfile(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			if sel.chosen.seatName == "omp-opus" || sel.ranked[len(sel.ranked)-1].seatName != "omp-opus" {
				t.Fatalf("credit ranks last: chosen %s", sel.chosen.seatName)
			}
			row := candOutcome(sel.record(), "omp-opus", "high")
			if row["tier"] != "credit" || row["waste_risk"] != false || row["waste_reason"] != nil {
				t.Fatalf("outside the final window credit stays tier credit: %v", row)
			}
		})
	}
}

// (2) When no subscription or free pool is admitted the credit pool is used,
// as before.
func TestCreditUsedWhenNoSubscriptionAdmitted(t *testing.T) {
	a, s := creditFixture(t, balance(95, 240*time.Hour), riskPool(32.6, 8), "", map[string]string{"omp-opus": "anthropic-api"})
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if sel.chosen.seatName != "omp-opus" || sel.chosen.tierName != "credit" {
		t.Fatalf("credit is the fallback: %s tier=%s", sel.chosen.seatName, sel.chosen.tierName)
	}
}

func TestPoolPolicyCreditFinalDays(t *testing.T) {
	embedded, e := LoadPoolPolicy("")
	if e != nil || *embedded.CreditFinalDays != 4 {
		t.Fatalf("embedded credit_final_days is 4: %v", e)
	}
	path := filepath.Join(t.TempDir(), "p.json")
	for extra, ok := range map[string]bool{"": true, `, "credit_final_days": 0`: true, `, "credit_final_days": -1`: false} {
		if e := os.WriteFile(path, []byte(riskPolicy(extra)), 0o600); e != nil {
			t.Fatal(e)
		}
		p, e := LoadPoolPolicy(path)
		if (e == nil) != ok {
			t.Fatalf("%q: err=%v", extra, e)
		}
		if extra == "" && *p.CreditFinalDays != 4 {
			t.Fatalf("absent credit_final_days defaults to 4: %v", *p.CreditFinalDays)
		}
	}
}

// `plan --credit-at` judges the credit window as of another date, read-only.
func TestPlanCreditAt(t *testing.T) {
	a, _ := creditFixture(t, balance(80, 240*time.Hour), riskPool(32.6, 8), "", nil)
	run := func(args ...string) (string, error) {
		out := &bytes.Buffer{}
		a.Out = out
		e := a.planCmd(context.Background(), append([]string{"--grade", "A", "--t", "T1"}, args...))
		return out.String(), e
	}
	// The fixture has no launch data, so placement after the selection
	// refuses; the selection lines are what --credit-at changes.
	text, _ := run()
	if !strings.Contains(text, "chosen host=none seat=sonnet@high") || strings.Contains(text, "credit_expiring") {
		t.Fatalf("10 days out: no final window\n%s", text)
	}
	at := a.Now().Add(8 * 24 * time.Hour).UTC().Format(time.RFC3339)
	text, _ = run("--credit-at", at)
	if !strings.Contains(text, "seat=omp-opus@high model=anthropic/claude-opus-5-5 pool=anthropic-api grade=A tier=waste waste_risk=true credit_expiring final_window_days=4 days_left=2.0 balance_pct=80.0 balance_usd=160.00") {
		t.Fatalf("--credit-at %s: final window\n%s", at, text)
	}
	if _, e := run("--credit-at", "soon"); code(e) != exitUsage {
		t.Fatalf("invalid --credit-at: %v", e)
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
			{"manual_observation_id": "a1", "event": "set", "pool": "anthropic-api", "window": "credit", "used_pct": 20, "expires_at": "2026-11-16T00:00:00Z"},
			{"manual_observation_id": "a2", "event": "set", "pool": "anthropic-api", "window": "credit", "used_pct": 25, "expires_at": "2026-11-16T00:00:00Z",
				"credit": map[string]any{"remaining_usd": 150, "total_usd": 200, "expires_on": "2026-11-16"}},
			// A shared entry: no dollars.
			{"manual_observation_id": "o1", "event": "set", "pool": "openai-api", "window": "credit", "used_pct": 5, "expires_at": "2026-11-18T00:00:00Z"},
			{"manual_observation_id": "c1", "event": "set", "pool": "claude", "window": "7d", "used_pct": 30, "expires_at": now.Add(time.Hour).Format(time.RFC3339Nano)},
		},
	})
	run := func(manualOK bool) map[string]PoolView {
		q := CLIQuota{Binary: "pacelever", Runner: &fakeRunner{Do: func(c Command) Result {
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
			a, s, q, _ := hostFirstFixture(t, "worker", "A", rows, map[string]string{"mac-b": testClaudeA, "cloud-small": creditKey}, map[string]float64{"mac-b": 0.1, "cloud-small": 0.5})
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

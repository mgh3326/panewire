package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

// Acceptance tests for hk 5113: The quota gate must not seat commandcode on a
// nearly spent weekly window.
//
// 1. Commandcode default cutoff 90 (both windows) when pools.commandcode.cutoff
//    is unset; explicit config override wins; other pools default 99.
// 2. One-seat-in-flight limit per nearly spent account: when an account's 7d
//    window is >= 75% used, admit a new commandcode seat on that account only
//    if no other commandcode seat is running on it.
// 3. plan and quota status explain it: refused account shows the reason
//    (window_exhausted cutoff=90 or account_busy_near_weekly_limit) on its
//    account … line and in candidate outcome.

func TestCommandcodeWeeklyWindow1117ReadingsRefused_t5113(t *testing.T) {
	a0, _ := ccAccountsFixture(t, map[string]*PoolView{})
	now := a0.Now()
	// The 2026-11-17 readings: Account A at 95.5%, Account B at 99.9%.
	// On 844ae0cc today's code admitted A (< 99); with cutoff 90 both are refused.
	a, s := ccAccountsFixture(t, map[string]*PoolView{
		ccKeyA: ccView(now, 10, 95.5, 3),
		ccKeyB: ccView(now, 10, 99.9, 3),
	})
	var out bytes.Buffer
	a.Out = &out

	sel, err := a.selectProfile(context.Background(), s)
	if err == nil || sel == nil || sel.chosen != nil {
		t.Fatalf("both accounts >= 90%% used: chosen=%v err=%v, want refused", sel.chosen, err)
	}
	cands := ccCandidates(sel)
	for _, key := range []string{ccKeyA, ccKeyB} {
		c, ok := cands[key]
		if !ok {
			t.Fatalf("candidate %s missing", key)
		}
		if c.outcome != "dropped:window_exhausted cutoff=90" {
			t.Fatalf("%s outcome = %q, want %q", key, c.outcome, "dropped:window_exhausted cutoff=90")
		}
		if c.code != outcomeQuotaExhausted {
			t.Fatalf("%s code = %q, want %q", key, c.code, outcomeQuotaExhausted)
		}
	}

	// Verify plan output shows the refusal reason on each account line
	a.printPlan(false, map[string]any{}, sel, nil, nil, "", err)
	planText := out.String()
	wantA := fmt.Sprintf("account %s host=mac-personal 5h=10.0%% 7d=95.5%% age=1m0s window_exhausted cutoff=90", ccKeyA)
	wantB := fmt.Sprintf("account %s host=mac-personal 5h=10.0%% 7d=99.9%% age=1m0s window_exhausted cutoff=90", ccKeyB)
	if !strings.Contains(planText, wantA) {
		t.Fatalf("plan output missing Account A refusal line:\nwant: %s\ngot:\n%s", wantA, planText)
	}
	if !strings.Contains(planText, wantB) {
		t.Fatalf("plan output missing Account B refusal line:\nwant: %s\ngot:\n%s", wantB, planText)
	}

	// Also verify native gate refusal directly
	gateApp, _, _ := switchApp(t, gateModeNative, nil, Result{Code: 99})
	gateApp.Config.Local = map[string]any{"presence_id": "mac-personal"}
	gateApp.Source = &fakePlacementSource{presence: []PresenceRecord{
		{HostID: "mac-personal", AccountKey: ccKeyA, LastSeen: switchNow.Add(-time.Minute)},
		{HostID: "mac-personal", AccountKey: ccKeyB, LastSeen: switchNow.Add(-2 * time.Minute)},
	}}
	gateApp.SharedLatest = func(_ context.Context, key string) (shared.Latest, error) {
		used := 95.5
		if key == ccKeyB {
			used = 99.9
		}
		return shared.Latest{Observation: switchReading(10, used)}, nil
	}
	res := gateApp.nativeGate(context.Background(), QuotaRequest{Profile: "cmd-dsflash41", Effort: "high", Role: "implementer"}, true)
	if res.Code != gateBlocked {
		t.Fatalf("native gate code = %d, want blocked (%d): %s", res.Code, gateBlocked, res.Out)
	}
	var receipt gateReceipt
	if err := json.Unmarshal([]byte(res.Out), &receipt); err != nil {
		t.Fatalf("unmarshal receipt: %v", err)
	}
	if !strings.Contains(receipt.Reason, "cutoff=90") {
		t.Fatalf("native gate receipt reason %q, want cutoff=90", receipt.Reason)
	}
}

func TestCommandcodeWeeklyWindow89PercentAdmitted_t5113(t *testing.T) {
	a0, _ := ccAccountsFixture(t, map[string]*PoolView{})
	now := a0.Now()
	// 89% on 7d window, no running seat: admitted (< 90).
	a, s := ccAccountsFixture(t, map[string]*PoolView{
		ccKeyA: ccView(now, 10, 89, 3),
		ccKeyB: ccView(now, 10, 95, 3), // B is exhausted at 95%
	})
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen == nil {
		t.Fatalf("choice failed: %v", err)
	}
	if sel.chosen.accountKey != ccKeyA {
		t.Fatalf("chosen account %s, want %s (the 89%% account)", sel.chosen.accountKey, ccKeyA)
	}
}

func TestCommandcodeWeeklyWindow80PercentWithRunningSeat_t5113(t *testing.T) {
	a0, _ := ccAccountsFixture(t, map[string]*PoolView{})
	now := a0.Now()

	t.Run("80% with running seat on that account refused with busy reason", func(t *testing.T) {
		a, s := ccAccountsFixture(t, map[string]*PoolView{
			ccKeyA: ccView(now, 10, 80, 3),
			ccKeyB: ccView(now, 10, 30, 3),
		})
		const runningJobID = "pa-redlight-pass-5031"
		a.LiveAccountJobs = func(_ context.Context, _ string) (map[string]string, error) {
			return map[string]string{ccKeyA: runningJobID}, nil
		}
		var out bytes.Buffer
		a.Out = &out

		sel, err := a.selectProfile(context.Background(), s)
		if err != nil || sel.chosen == nil {
			t.Fatalf("choice failed: %v", err)
		}
		// Account B (at 30%, no running seat) must be chosen
		if sel.chosen.accountKey != ccKeyB {
			t.Fatalf("chosen account %s, want %s", sel.chosen.accountKey, ccKeyB)
		}
		cands := ccCandidates(sel)
		candA := cands[ccKeyA]
		wantBusy := fmt.Sprintf("account_busy_near_weekly_limit account=%s job=%s", ccKeyA, runningJobID)
		if candA.outcome != "dropped:"+wantBusy {
			t.Fatalf("Account A outcome = %q, want %q", candA.outcome, "dropped:"+wantBusy)
		}

		// Verify plan output shows busy reason on Account A line
		a.printPlan(false, map[string]any{}, sel, nil, nil, "", nil)
		planText := out.String()
		wantLineA := fmt.Sprintf("account %s host=mac-personal 5h=10.0%% 7d=80.0%% age=1m0s %s", ccKeyA, wantBusy)
		if !strings.Contains(planText, wantLineA) {
			t.Fatalf("plan output missing Account A busy line:\nwant: %s\ngot:\n%s", wantLineA, planText)
		}
	})

	t.Run("80% reading with running seat on other account admitted", func(t *testing.T) {
		a, s := ccAccountsFixture(t, map[string]*PoolView{
			ccKeyA: ccView(now, 10, 80, 3),
			ccKeyB: ccView(now, 10, 95, 3), // B is exhausted (95 >= 90)
		})
		// Running job is on ccKeyB, not ccKeyA
		a.LiveAccountJobs = func(_ context.Context, _ string) (map[string]string, error) {
			return map[string]string{ccKeyB: "some-job-on-b"}, nil
		}
		sel, err := a.selectProfile(context.Background(), s)
		if err != nil || sel.chosen == nil {
			t.Fatalf("choice failed: %v", err)
		}
		// Account A (at 80% with no running seat on A) must be admitted and chosen
		if sel.chosen.accountKey != ccKeyA {
			t.Fatalf("chosen account %s, want %s", sel.chosen.accountKey, ccKeyA)
		}
	})

	t.Run("native gate refuses with busy reason when 7d >= 75% and seat is running", func(t *testing.T) {
		gateApp, _, _ := switchApp(t, gateModeNative, nil, Result{Code: 99})
		gateApp.Config.Local = map[string]any{"presence_id": "mac-personal"}
		gateApp.Source = &fakePlacementSource{presence: []PresenceRecord{
			{HostID: "mac-personal", AccountKey: ccKeyA, LastSeen: switchNow.Add(-time.Minute)},
		}}
		gateApp.SharedLatest = func(_ context.Context, _ string) (shared.Latest, error) {
			return shared.Latest{Observation: switchReading(10, 80)}, nil
		}
		gateApp.LiveAccountJobs = func(_ context.Context, _ string) (map[string]string, error) {
			return map[string]string{ccKeyA: "job-busy-42"}, nil
		}
		res := gateApp.nativeGate(context.Background(), QuotaRequest{Profile: "cmd-dsflash41", Effort: "high", Role: "implementer"}, true)
		if res.Code != gateBlocked {
			t.Fatalf("native gate code = %d, want %d: %s", res.Code, gateBlocked, res.Out)
		}
		var receipt gateReceipt
		if err := json.Unmarshal([]byte(res.Out), &receipt); err != nil {
			t.Fatalf("unmarshal receipt: %v", err)
		}
		wantReason := fmt.Sprintf("account_busy_near_weekly_limit account=%s job=job-busy-42", ccKeyA)
		if receipt.Reason != wantReason {
			t.Fatalf("receipt reason = %q, want %q", receipt.Reason, wantReason)
		}
	})
}

func TestCommandcodeConfigCutoffOverrideWins_t5113(t *testing.T) {
	cfgFile := filepath.Join(t.TempDir(), "config.toml")
	toml := "[pools.commandcode]\ncutoff = 95\n"
	if err := os.WriteFile(cfgFile, []byte(toml), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PANEWIRE_CONFIG", cfgFile)

	a0, _ := ccAccountsFixture(t, map[string]*PoolView{})
	now := a0.Now()

	// 92% is admitted under cutoff 95 (would be refused under default 90)
	a, s := ccAccountsFixture(t, map[string]*PoolView{
		ccKeyA: ccView(now, 10, 92, 3),
		ccKeyB: ccView(now, 10, 96, 3), // 96% is refused under cutoff 95
	})
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen == nil {
		t.Fatalf("choice failed: %v", err)
	}
	if sel.chosen.accountKey != ccKeyA {
		t.Fatalf("chosen account %s, want %s (the 92%% account)", sel.chosen.accountKey, ccKeyA)
	}
	cands := ccCandidates(sel)
	if c := cands[ccKeyB]; c.outcome != "dropped:window_exhausted cutoff=95" {
		t.Fatalf("ccKeyB outcome = %q, want %q", c.outcome, "dropped:window_exhausted cutoff=95")
	}
}

func TestOtherPoolAt95PercentUnchanged_t5113(t *testing.T) {
	// Codex at 95%: unchanged (admitted at default 99)
	rows := []CatalogRow{{Profile: "codex-luna", Effort: "high", Grade: "A", Pool: "codex", Gate: "default", ModelID: "openai/codex-5.2"}}
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	reset := now.Add(3 * 24 * time.Hour).Format(time.RFC3339Nano)
	codexView := accountPoolView([]accountBucket{
		{Window: "7d", UsedPct: ptrF(95), ResetsAt: reset, Scope: "account"},
	}, now)

	a, s, q := selFixture(t, "A", rows, map[string]PoolView{"codex": *codexView}, []string{"codex"})
	q.GateDo = gatePools(map[string]string{"codex-luna": "codex"})
	a.Config.Hosts = map[string]map[string]any{"local": {"codex_luna": true}}
	a.Config.HostOrder = []string{"local"}
	a.planOnly = true

	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen == nil {
		t.Fatalf("codex at 95%% must be admitted under default 99 cutoff, got err=%v sel=%v", err, sel)
	}
	if sel.chosen.row.Pool != "codex" {
		t.Fatalf("chosen pool = %q, want codex", sel.chosen.row.Pool)
	}
}

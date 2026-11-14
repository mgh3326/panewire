package fleet

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

// hk 4626: a plan dropped Claude rows as quota-exhausted while the host's own
// record read "weekly 2%, stale". Two hypotheses: H2 says a host whose own
// account reading is stale (or whose binding lapses) is judged by another
// account's reading or a pool-wide view; H1 says the exhausted rows belonged
// to the other account all along and the output just did not say which.
//
// This test is the decider: host mac-b holds account A with a stale low
// reading, host cloud-small holds account B fresh at 100%, and the plan's
// grade reaches both. If any A-bound row is dropped as exhausted, H2 holds.
func TestSelectQuotaDropAttributesToItsOwnAccount(t *testing.T) {
	low := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(2)}
	full := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(100)}

	t.Run("stale own reading never borrows another account's exhaustion", func(t *testing.T) {
		a, s, _, src := hostFirstFixture(t, "implementer", "B", workerRows,
			map[string]string{"mac-b": testClaudeA, "cloud-small": testClaudeB},
			map[string]float64{"mac-b": 0.1, "cloud-small": 0.1})
		src.accounts[testClaudeA] = SharedAccount{Key: testClaudeA, Observed: true, Fresh: false, MeasuredAt: a.Now().Add(-20 * time.Minute), Quota: &low}
		src.accounts[testClaudeB] = SharedAccount{Key: testClaudeB, Observed: true, Fresh: true, MeasuredAt: a.Now(), Quota: &full}
		a.planOnly = true

		sel, err := a.selectProfile(context.Background(), s)
		if sel == nil {
			t.Fatalf("no plan: %v", err)
		}
		var aRow, bRow *selCand
		for _, c := range sel.cands {
			switch c.accountKey {
			case testClaudeA:
				aRow = c
			case testClaudeB:
				bRow = c
			}
			// H2 fails here: a row bound to account A must never be
			// dropped by account B's exhaustion.
			if c.accountKey == testClaudeA && c.code == outcomeQuotaExhausted {
				t.Fatalf("H2: A's row dropped on B's exhausted reading: %+v", c)
			}
		}
		if aRow == nil || bRow == nil {
			t.Fatalf("want one row per account, got %+v", sel.cands)
		}
		if aRow.code != outcomeQuotaUnknown || aRow.outcome != "dropped:quota_unknown" {
			t.Fatalf("a stale reading must drop quota_unknown, got code=%s outcome=%s", aRow.code, aRow.outcome)
		}
		if len(aRow.judgments) != 1 || aRow.judgments[0].Reason != "would_measure" {
			t.Fatalf("a dry run names the measurement it skipped: %+v", aRow.judgments)
		}
		if bRow.code != outcomeQuotaExhausted || bRow.outcome != "dropped:window_exhausted" {
			t.Fatalf("B's fresh 100%% reading is the real exhaustion: code=%s outcome=%s", bRow.code, bRow.outcome)
		}

		// Every quota drop names its account, host and reading age/stale.
		rec := sel.record()
		byKey := map[string]map[string]any{}
		for _, v := range rec["candidates"].([]any) {
			row := v.(map[string]any)
			byKey[row["account_key"].(string)] = row
		}
		if got := byKey[testClaudeA]; got["host"] != "mac-b" || got["stale"] != true || got["reading_age_s"] != 1200.0 {
			t.Fatalf("A's drop must name host, staleness and reading age: %v", got)
		}
		if got := byKey[testClaudeB]; got["host"] != "cloud-small" || got["stale"] != nil || got["reading_age_s"] != 0.0 {
			t.Fatalf("B's drop must name its own host and fresh reading: %v", got)
		}
		if got := byKey[testClaudeB]["outcome"]; got != "dropped:window_exhausted" {
			t.Fatalf("B's exhausted reading decides its own row: %v", got)
		}
	})

	t.Run("fresh low reading competes while the other account exhausts", func(t *testing.T) {
		a, s, _, src := hostFirstFixture(t, "implementer", "B", workerRows,
			map[string]string{"mac-b": testClaudeA, "cloud-small": testClaudeB},
			map[string]float64{"mac-b": 0.1, "cloud-small": 0.1})
		src.accounts[testClaudeA] = SharedAccount{Key: testClaudeA, Observed: true, Fresh: true, MeasuredAt: a.Now(), Quota: &low}
		src.accounts[testClaudeB] = SharedAccount{Key: testClaudeB, Observed: true, Fresh: true, MeasuredAt: a.Now(), Quota: &full}

		sel, err := a.selectProfile(context.Background(), s)
		if err != nil || sel.chosen == nil {
			t.Fatalf("A's fresh low reading must admit: %v", err)
		}
		if sel.chosen.accountKey != testClaudeA || sel.chosen.host != "mac-b" {
			t.Fatalf("admitted %s on %s, want A on mac-b", sel.chosen.accountKey, sel.chosen.host)
		}
		for _, c := range sel.cands {
			if c.accountKey == testClaudeB && (c.code != outcomeQuotaExhausted || c.outcome != "dropped:window_exhausted") {
				t.Fatalf("B's own exhaustion drops its row: code=%s outcome=%s", c.code, c.outcome)
			}
		}
	})
}

// The human line carries the same attribution: account, host and reading age.
func TestPlanDroppedLineNamesAccountAndReading(t *testing.T) {
	low := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(2)}
	full := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(100)}
	a, s, _, src := hostFirstFixture(t, "implementer", "B", workerRows,
		map[string]string{"mac-b": testClaudeA, "cloud-small": testClaudeB},
		map[string]float64{"mac-b": 0.1, "cloud-small": 0.1})
	src.accounts[testClaudeA] = SharedAccount{Key: testClaudeA, Observed: true, Fresh: false, MeasuredAt: a.Now().Add(-20 * time.Minute), Quota: &low}
	src.accounts[testClaudeB] = SharedAccount{Key: testClaudeB, Observed: true, Fresh: true, MeasuredAt: a.Now(), Quota: &full}
	a.planOnly = true

	sel, err := a.selectProfile(context.Background(), s)
	a.printPlan(false, map[string]any{}, sel, nil, nil, "", err)
	text := a.Out.(*bytes.Buffer).String()
	if !strings.Contains(text, "account="+testClaudeB+" host=cloud-small") {
		t.Fatalf("B's exhausted drop must name its account and host:\n%s", text)
	}
	if !strings.Contains(text, "account="+testClaudeA+" host=mac-b age=20m0s stale") {
		t.Fatalf("A's drop must name its account, host, reading age and staleness:\n%s", text)
	}
}

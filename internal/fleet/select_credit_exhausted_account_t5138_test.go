package fleet

import (
	"context"
	"strings"
	"testing"
	"time"
)

// hk 5138: the hub marks the one commandcode account whose pane showed the
// provider's credit rejection. Selection drops that account with the same
// reason a 7d-exhausted account takes, and every other account of the pool
// stays a candidate.
func TestSelectionSkipsCreditExhaustedCommandcodeAccount(t *testing.T) {
	a0, _ := ccAccountsFixture(t, map[string]*PoolView{})
	now := a0.Now()
	a, s := ccAccountsFixture(t, map[string]*PoolView{
		ccKeyA: ccView(now, 10, 30, 3),
		ccKeyB: ccView(now, 10, 40, 3),
	})
	marked := now.Add(-time.Hour)
	a.State = &fakeStateReader{snap: FleetSnapshot{V: 1, At: now, AccountCreditExhausted: map[string]time.Time{ccKeyA: marked}}}

	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen == nil {
		t.Fatalf("no choice: %v", err)
	}
	cands := ccCandidates(sel)
	if c := cands[ccKeyA]; c.outcome != "dropped:credit_exhausted account="+ccKeyA {
		t.Fatalf("marked account outcome = %q, want dropped:credit_exhausted account=%s", c.outcome, ccKeyA)
	} else if c.code != outcomeQuotaExhausted {
		t.Fatalf("marked account code = %q, want %q", c.code, outcomeQuotaExhausted)
	}
	row := ccRecordRow(t, sel.record(), ccKeyA)
	if got, _ := row["since"].(string); got != marked.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("marked account since = %q, want %q", got, marked.UTC().Format(time.RFC3339Nano))
	}
	if c := cands[ccKeyB]; strings.HasPrefix(c.outcome, "dropped:") {
		t.Fatalf("the other account was dropped: %s", c.outcome)
	}
	if sel.chosen.accountKey != ccKeyB {
		t.Fatalf("chosen account %s, want %s", sel.chosen.accountKey, ccKeyB)
	}
}

// The mark lifts after AccountCreditMarkTTL even when no reading of the account
// arrived, so a stale hub mark cannot strand an account for good.
func TestSelectionLiftsCreditExhaustedMarkAfterTTL(t *testing.T) {
	a0, _ := ccAccountsFixture(t, map[string]*PoolView{})
	now := a0.Now()
	a, s := ccAccountsFixture(t, map[string]*PoolView{
		ccKeyA: ccView(now, 10, 30, 3),
		ccKeyB: ccView(now, 10, 40, 3),
	})
	a.State = &fakeStateReader{snap: FleetSnapshot{V: 1, At: now, AccountCreditExhausted: map[string]time.Time{ccKeyA: now.Add(-AccountCreditMarkTTL)}}}

	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if c := ccCandidates(sel)[ccKeyA]; strings.HasPrefix(c.outcome, "dropped:") {
		t.Fatalf("an expired mark still dropped the account: %s", c.outcome)
	}
}

// ccRecordRow finds the selection record row of account key.
func ccRecordRow(t *testing.T, rec map[string]any, key string) map[string]any {
	t.Helper()
	cands, _ := rec["candidates"].([]any)
	for _, v := range cands {
		row, _ := v.(map[string]any)
		if row["account_key"] == key {
			return row
		}
	}
	t.Fatalf("no selection record row for %s", key)
	return nil
}

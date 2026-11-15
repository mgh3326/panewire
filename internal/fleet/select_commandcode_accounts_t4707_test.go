package fleet

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

// hk 4707 (decisions 288-290): omp may hold several Command Code accounts on a
// host. Each is its own account key and its own candidate; the pool is usable
// while one has room, and the weekly window orders the accounts as it orders any
// other subscription pool's 7-day window.

const (
	ccKeyA = "commandcode:7d279b7dcc317cda"
	ccKeyB = "commandcode:a1fd7fc5ae227218"
)

// ccWindows is an omp reading's 5h and 7d windows as the hub serves them, the 7d
// one resetting in weekResetDays.
func ccView(now time.Time, used5h, used7d, weekResetDays float64) *PoolView {
	week := now.Add(time.Duration(weekResetDays * float64(24*time.Hour))).UTC().Format(time.RFC3339Nano)
	five := now.Add(3 * time.Hour).UTC().Format(time.RFC3339Nano)
	return accountPoolView([]accountBucket{
		{Window: "5h", UsedPct: &used5h, ResetsAt: five, Scope: "account"},
		{Window: "7d", UsedPct: &used7d, ResetsAt: week, Scope: "account"},
	}, now)
}

// ccAccountsFixture is mac-personal holding the two accounts, with the views
// given per key; fresh says which account's reading is current.
func ccAccountsFixture(t *testing.T, views map[string]*PoolView, stale ...string) (*App, Spawn) {
	t.Helper()
	rows := []CatalogRow{{Profile: "cmd-dsflash41", Effort: "high", Grade: "A+", Pool: "commandcode", Gate: "default", ModelID: "deepseek/deepseek-v4.1-flash"}}
	a, s, q := selFixture(t, "C", rows, nil, nil)
	src := &fakePlacementSource{hosts: []HostView{{Name: "mac-personal", Connected: true}}, accounts: map[string]SharedAccount{}}
	for key, view := range views {
		src.presence = append(src.presence, PresenceRecord{HostID: "mac-personal", AccountKey: key, LastSeen: a.Now()})
		src.accounts[key] = SharedAccount{Key: key, Observed: true, Fresh: !slices.Contains(stale, key), MeasuredAt: a.Now().Add(-time.Minute), Quota: view}
	}
	a.Source = src
	a.Config.Hosts = map[string]map[string]any{"mac-personal": {"cmd_dsflash41": true}}
	a.Config.HostOrder = []string{"mac-personal"}
	q.GateDo = gatePools(map[string]string{"cmd-dsflash41": "commandcode"})
	a.planOnly = true
	return a, s
}

func ccCandidates(sel *selPlan) map[string]*selCand {
	out := map[string]*selCand{}
	for _, c := range sel.cands {
		if c.accountKey != "" {
			out[c.accountKey] = c
		}
	}
	return out
}

func TestCommandcodePoolIsUsableWhileOneAccountHasRoom(t *testing.T) {
	a0, _ := ccAccountsFixture(t, map[string]*PoolView{})
	now := a0.Now()
	a, s := ccAccountsFixture(t, map[string]*PoolView{
		ccKeyA: ccView(now, 10, 100, 3), // weekly window used up
		ccKeyB: ccView(now, 10, 30, 3),
	})
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen == nil {
		t.Fatalf("no choice: %v", err)
	}
	cands := ccCandidates(sel)
	if len(cands) != 2 {
		t.Fatalf("want one candidate per account, got %v", cands)
	}
	if sel.chosen.accountKey != ccKeyB {
		t.Fatalf("chosen account %s, want %s (the one with room)", sel.chosen.accountKey, ccKeyB)
	}
	if c := cands[ccKeyA]; c.outcome != "dropped:window_exhausted" || c.code != outcomeQuotaExhausted {
		t.Fatalf("the exhausted account must drop on its own window: %s %s", c.outcome, c.code)
	}
}

func TestCommandcodePoolIsDroppedWhenEveryAccountIsExhausted(t *testing.T) {
	a0, _ := ccAccountsFixture(t, map[string]*PoolView{})
	now := a0.Now()
	a, s := ccAccountsFixture(t, map[string]*PoolView{
		ccKeyA: ccView(now, 10, 100, 3),
		ccKeyB: ccView(now, 100, 20, 3), // the 5-hour window gates admission
	})
	sel, err := a.selectProfile(context.Background(), s)
	if err == nil || sel == nil || sel.chosen != nil {
		t.Fatalf("both accounts exhausted: chosen=%v err=%v, want the pool dropped", sel.chosen, err)
	}
	for key, c := range ccCandidates(sel) {
		if c.outcome != "dropped:window_exhausted" {
			t.Fatalf("%s outcome %s, want window_exhausted", key, c.outcome)
		}
	}
}

// A reading that is not current is unknown, never room: the account is dropped
// as quota_unknown (a spawn would measure it first) and the other is chosen.
func TestCommandcodeStaleAccountIsUnknownAndTheOtherIsChosen(t *testing.T) {
	a0, _ := ccAccountsFixture(t, map[string]*PoolView{})
	now := a0.Now()
	a, s := ccAccountsFixture(t, map[string]*PoolView{
		ccKeyA: ccView(now, 0, 0, 3),
		ccKeyB: ccView(now, 10, 40, 3),
	}, ccKeyA)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen == nil || sel.chosen.accountKey != ccKeyB {
		t.Fatalf("chosen %v err=%v, want account B", sel.chosen, err)
	}
	if c := ccCandidates(sel)[ccKeyA]; c.code != outcomeQuotaUnknown || !c.stale {
		t.Fatalf("stale account A: code=%s stale=%v, want quota_unknown", c.code, c.stale)
	}
}

// The weekly window orders the accounts: the one whose week ends with the most
// quota unused is at waste risk and goes first; otherwise the one with the most
// room. No month enters the order.
func TestCommandcodeAccountsAreOrderedOnTheirWeeklyWindow(t *testing.T) {
	a0, _ := ccAccountsFixture(t, map[string]*PoolView{})
	now := a0.Now()
	for name, tc := range map[string]struct {
		a, b *PoolView
		want string
		risk bool
	}{
		// A's week ends in a day with 80% unused; B's has six days left.
		"waste risk before room": {ccView(now, 5, 20, 1), ccView(now, 5, 20, 6), ccKeyA, true},
		"B at risk, A is not":    {ccView(now, 5, 20, 6), ccView(now, 5, 20, 1), ccKeyB, true},
		// Neither week is about to end unused: the account with more room wins.
		"more room": {ccView(now, 5, 60, 3.5), ccView(now, 5, 45, 3.5), ccKeyB, false},
	} {
		t.Run(name, func(t *testing.T) {
			a, s := ccAccountsFixture(t, map[string]*PoolView{ccKeyA: tc.a, ccKeyB: tc.b})
			sel, err := a.selectProfile(context.Background(), s)
			if err != nil || sel.chosen == nil {
				t.Fatalf("no choice: %v", err)
			}
			if sel.chosen.accountKey != tc.want || sel.chosen.risk != tc.risk {
				t.Fatalf("chosen %s risk=%v, want %s risk=%v (urgency %v)", sel.chosen.accountKey, sel.chosen.risk, tc.want, tc.risk, sel.chosen.urgency)
			}
		})
	}
}

func TestHostAccountsListsEveryCommandcodeAccountButOneAccountForOtherPools(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	const claudeKey = "claude:f0245b3a03eeff12"
	other := "claude:0123456789abcdef"
	presence := []PresenceRecord{
		{HostID: "mac", AccountKey: ccKeyB, LastSeen: now.Add(-2 * time.Minute)},
		{HostID: "mac", AccountKey: ccKeyA, LastSeen: now.Add(-time.Minute)},
		{HostID: "mac", AccountKey: "commandcode:0000000000000001", LastSeen: now.Add(-3 * time.Hour)}, // an account omp stopped listing
		{HostID: "other", AccountKey: "commandcode:0000000000000002", LastSeen: now},
		{HostID: "mac", AccountKey: other, LastSeen: now.Add(-time.Hour)},
		{HostID: "mac", AccountKey: claudeKey, LastSeen: now.Add(-time.Minute)},
	}
	if got := hostAccounts(presence, "commandcode", "mac", now, 24*time.Hour); !slices.Equal(got, []string{ccKeyA, ccKeyB}) {
		t.Fatalf("commandcode accounts %v, want %s then %s (newest first; the one not announced for 3 h is gone)", got, ccKeyA, ccKeyB)
	}
	// A host that removed its last account stops announcing: its old record must
	// not keep the account "held" for the presence TTL, however old the newest is.
	hours := []PresenceRecord{{HostID: "mac", AccountKey: ccKeyA, LastSeen: now.Add(-5 * time.Hour)}, {HostID: "mac", AccountKey: ccKeyB, LastSeen: now.Add(-6 * time.Hour)}}
	if got := hostAccounts(hours, "commandcode", "mac", now, 24*time.Hour); len(got) != 0 {
		t.Fatalf("announcements hours old still name accounts: %v", got)
	}
	if got := hostAccountFor(hours, "commandcode", "mac", now, 24*time.Hour, ccKeyA); got != "" {
		t.Fatalf("a chosen account announced hours ago is not held: %q", got)
	}
	if got := hostAccounts(presence, "claude", "mac", now, 24*time.Hour); !slices.Equal(got, []string{claudeKey}) {
		t.Fatalf("claude accounts %v, want only the newest", got)
	}
	if got := hostAccountFor(presence, "commandcode", "mac", now, 24*time.Hour, ccKeyB); got != ccKeyB {
		t.Fatalf("a chosen account the host holds must be kept, got %s", got)
	}
	if got := hostAccountFor(presence, "commandcode", "mac", now, 24*time.Hour, "commandcode:ffffffffffffffff"); got != ccKeyA {
		t.Fatalf("a chosen account the host does not hold falls back to the newest, got %s", got)
	}
	// Two accounts announced in the same instant: a multi-account pool still has
	// a deterministic first account (key order) for a launch that chose none,
	// while a single-account pool keeps refusing the tie.
	tied := []PresenceRecord{
		{HostID: "mac", AccountKey: ccKeyB, LastSeen: now}, {HostID: "mac", AccountKey: ccKeyA, LastSeen: now},
		{HostID: "mac", AccountKey: claudeKey, LastSeen: now}, {HostID: "mac", AccountKey: other, LastSeen: now},
	}
	if got := hostAccountFor(tied, "commandcode", "mac", now, 24*time.Hour, ""); got != ccKeyA {
		t.Fatalf("tied commandcode accounts with no choice: got %q, want %s", got, ccKeyA)
	}
	if got := hostAccountFor(tied, "claude", "mac", now, 24*time.Hour, ""); got != "" {
		t.Fatalf("a tied single-account pool must stay unresolved, got %q", got)
	}
	if got := hostAccountFor(presence, "claude", "mac", now, 24*time.Hour, other); got != claudeKey {
		t.Fatalf("a single-account pool keeps the newest account, got %s", got)
	}
}

// A node that still runs the earlier API measurement publishes a 30d bucket. The
// month is not read, projected or gated on (decision 289), so the bucket is
// ignored by the gate, the ranking and the status band until that node is
// replaced; other pools keep their own 30d windows.
func TestCommandcodeMonthlyBucketFromAnOlderNodeIsIgnored(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	reset := func(d time.Duration) *time.Time { at := now.Add(d); return &at }
	observation := &shared.Observation{Kind: shared.KindAuto, MeasuredAt: now.Add(-time.Minute), Buckets: []shared.Bucket{
		{Window: "5h", UsedPct: 10, Scope: "account", ResetsAt: reset(3 * time.Hour)},
		{Window: "7d", UsedPct: 20, Scope: "account", ResetsAt: reset(3 * 24 * time.Hour)},
		{Window: "30d", UsedPct: 100, Scope: "account", ResetsAt: reset(10 * 24 * time.Hour)},
	}}
	reading, ok := admissionAuto(observation, "commandcode", ccKeyA, now, 5*time.Minute, false)
	if !ok || len(reading.Buckets) != 2 {
		t.Fatalf("the gate read %+v ok=%v, want the 5h and 7d windows only", reading.Buckets, ok)
	}
	if kept, ok := admissionAuto(observation, "kiro", "kiro:0123456789abcdef", now, 5*time.Minute, false); !ok || len(kept.Buckets) != 3 {
		t.Fatalf("another pool keeps its 30d window: %+v ok=%v", kept.Buckets, ok)
	}
	buckets := []accountBucket{
		{Window: "5h", UsedPct: ptrF(10), ResetsAt: now.Add(3 * time.Hour).Format(time.RFC3339Nano), Scope: "account"},
		{Window: "30d", UsedPct: ptrF(100), ResetsAt: now.Add(10 * 24 * time.Hour).Format(time.RFC3339Nano), Scope: "account"},
	}
	if got := readableBuckets("commandcode", buckets); len(got) != 1 || got[0].Window != "5h" || len(buckets) != 2 {
		t.Fatalf("readableBuckets = %+v (input %+v must be untouched)", got, buckets)
	}
	if got := readableBuckets("kiro", buckets); len(got) != 2 {
		t.Fatalf("another pool's windows are all read: %+v", got)
	}
}

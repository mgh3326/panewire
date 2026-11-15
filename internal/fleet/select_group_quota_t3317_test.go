package fleet

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// agy publishes only group-scoped buckets (pacelever --json, 2026-11-06:
// gemini 5h, 3p 5h, 3p 7d and an "unknown" 5h with no reset, no account
// bucket). Placement ranked account windows only, so every agy rung read as
// quota_unknown (hk 3317).

// agyGroupBuckets is the production shape of the agy reading, with the 7d
// window `elapsed` into its seven days.
func agyGroupBuckets(now time.Time, elapsed time.Duration) []map[string]any {
	reset7d := now.Add(7*24*time.Hour - elapsed).UTC().Format(time.RFC3339)
	reset5h := now.Add(4 * time.Hour).UTC().Format(time.RFC3339)
	return []map[string]any{
		{"window": "5h", "used_pct": 0, "resets_at": reset5h, "scope": "group", "model": "gemini"},
		{"window": "5h", "used_pct": 0, "resets_at": reset5h, "scope": "group", "model": "3p"},
		{"window": "7d", "used_pct": 0, "resets_at": reset7d, "scope": "group", "model": "3p"},
		{"window": "5h", "used_pct": 0, "scope": "group", "model": "unknown"},
	}
}

// hubObservedSource serves every account's shared reading from an HTTP
// handoffkeep fake through the production decoder (hkQuota.sharedAccount), so
// the buckets reach placement the way the wire carries them.
type hubObservedSource struct {
	*fakePlacementSource
	hk *hkQuota
}

func (s hubObservedSource) SharedAccount(ctx context.Context, key string) (SharedAccount, error) {
	return s.hk.sharedAccount(ctx, key)
}

func agyGroupFixture(t *testing.T, elapsed time.Duration, rows []CatalogRow, present []string) (*App, Spawn, *fakeQuota) {
	t.Helper()
	return agyReadingFixture(t, func(now time.Time) []map[string]any { return agyGroupBuckets(now, elapsed) }, rows, present)
}

// agyReadingFixture is agyGroupFixture with the agy reading's buckets chosen by
// the caller.
func agyReadingFixture(t *testing.T, agyBuckets func(now time.Time) []map[string]any, rows []CatalogRow, present []string) (*App, Spawn, *fakeQuota) {
	t.Helper()
	a, s, q := selFixture(t, "B", rows, nil, present)
	now := a.Now()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/quota/accounts/"), "/latest")
		buckets := []map[string]any{{"window": "7d", "used_pct": 20, "resets_at": now.Add(24 * time.Hour).UTC().Format(time.RFC3339), "scope": "account"}}
		if strings.HasPrefix(key, "agy:") {
			buckets = agyBuckets(now)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"observation": map[string]any{"kind": "auto", "measured_at": now.Add(-time.Minute), "buckets": buckets}})
	}))
	t.Cleanup(server.Close)
	a.Source = hubObservedSource{a.Source.(*fakePlacementSource), &hkQuota{base: server.URL, token: "t", now: a.Now, freshTTL: a.sharedFreshTTL}}
	return a, s, q
}

func agyGroupRows() []CatalogRow {
	return []CatalogRow{
		{Profile: "agy-flash38-medium", Effort: "medium", Grade: "B", Pool: "agy", Gate: "default", ModelID: "gemini-3.8-flash-medium"},
		{Profile: "codex-luna", Effort: "medium", Grade: "B", Pool: "codex", Gate: "default", ModelID: "m"},
	}
}

func TestSelectOffersAgyWithGroupOnlyReading(t *testing.T) {
	a, s, q := agyGroupFixture(t, 0, agyGroupRows(), []string{"agy"})
	q.GateDo = gatePools(withAgyGates(nil))
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatalf("agy with a measured group reading must be admitted: %v", e)
	}
	if sel.chosen.seatName != "agy-flash38-medium" {
		t.Fatalf("chosen %s", sel.chosen.seatName)
	}
	row := candOutcome(sel.record(), "agy-flash38-medium", "medium")
	if row["outcome"] != "chosen" || row["tier"] == nil || row["tier"] == "" {
		t.Fatalf("agy must be a ranked candidate with a tier, not quota_unknown: %v", row)
	}
}

// Midway through the 7d window an unused agy 3p rung has waste urgency, as the
// operator's confirmed order needs. The 7d window belongs to the 3p group: a
// gemini rung ranks on the gemini group's 5h window alone and has none.
func TestSelectRanksAgyByWasteUrgencyMidWindow(t *testing.T) {
	rows := []CatalogRow{{Profile: "agy-sonnet46", Effort: "medium", Grade: "B", Pool: "agy", Gate: "default", ModelID: "claude-sonnet-4-6"}}
	a, s, q := agyGroupFixture(t, 3*24*time.Hour, rows, []string{"agy"})
	q.GateDo = gatePools(withAgyGates(nil))
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	row := candOutcome(sel.record(), "agy-sonnet46", "medium")
	if row["tier"] != "waste" || row["waste_risk"] != true || row["waste_urgency"] == nil {
		t.Fatalf("unused agy mid-window is waste with an urgency: %v", row)
	}
}

func TestAccountPoolViewGroupBuckets(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	reset := now.Add(24 * time.Hour).Format(time.RFC3339)
	group := accountBucket{Scope: "group", Model: "3p", UsedPct: ptrF(30), Window: "7d", ResetsAt: reset}
	account := accountBucket{Scope: "account", UsedPct: ptrF(10), Window: "7d", ResetsAt: reset}
	if v := accountPoolView([]accountBucket{group}, now); v == nil || v.Blocking == nil || *v.Blocking != 30 {
		t.Fatalf("a group-only reading ranks on its group buckets: %+v", v)
	}
	if v := accountPoolView([]accountBucket{group, account}, now); v == nil || v.Blocking == nil || *v.Blocking != 10 {
		t.Fatalf("account buckets win over group buckets: %+v", v)
	}
	model := accountBucket{Scope: "model", Model: "m", UsedPct: ptrF(30), Window: "7d", ResetsAt: reset}
	if v := accountPoolView([]accountBucket{model}, now); v == nil || v.Blocking != nil {
		t.Fatalf("model buckets stay with the profile's gate: %+v", v)
	}
}

// productionShapeRows are the evidence doc's reviewer rungs on one host: the
// three agy rungs that passed role and grade, commandcode and codex.
func productionShapeRows() []CatalogRow {
	return []CatalogRow{
		{Profile: "agy-flash38-high", Effort: "high", Grade: "A", Pool: "agy", Gate: "default", ModelID: "gemini-3.8-flash-high"},
		{Profile: "agy-flash38-medium", Effort: "medium", Grade: "B", Pool: "agy", Gate: "default", ModelID: "gemini-3.8-flash-medium"},
		{Profile: "agy-sonnet46", Effort: "medium", Grade: "B", Pool: "agy", Gate: "default", ModelID: "claude-sonnet-4-6"},
		{Profile: "cmd-dsflash41", Effort: "high", Grade: "A+", Pool: "commandcode", Gate: "default", ModelID: "deepseek/deepseek-v4.1-flash"},
		{Profile: "codex-luna", Effort: "medium", Grade: "B", Pool: "codex", Gate: "default", ModelID: "gpt-6-luna"},
	}
}

// The dry-run placement of the 2026-11-06 reviewer evidence: one host holds a
// codex, a commandcode and an agy account. Every agy rung and the commandcode
// rung is a candidate with a tier, and none is quota_unknown or absent from
// the host's tried list.
func TestSelectDryRunProductionShapeOffersAgyAndCommandcode(t *testing.T) {
	a, s, q := agyGroupFixture(t, 3*24*time.Hour, productionShapeRows(), nil)
	src := a.Source.(hubObservedSource).fakePlacementSource
	src.hosts = []HostView{{Name: "mac-dev", Connected: true}}
	for _, key := range []string{"codex:00000000000000a1", "commandcode:00000000000000b2", "agy:78046f9a722ebaff"} {
		src.presence = append(src.presence, PresenceRecord{HostID: "mac-dev", AccountKey: key, LastSeen: a.Now()})
	}
	a.Config.Hosts = map[string]map[string]any{"mac-dev": {"cmd_dsflash41": true}}
	a.Config.HostOrder = []string{"mac-dev"}
	q.GateDo = gatePools(withAgyGates(map[string]string{"cmd-dsflash41": "commandcode", "codex-luna": "codex"}))
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	rec := sel.record()
	for _, c := range []struct {
		profile, effort string
		urgency         bool
	}{{"agy-flash38-high", "high", false}, {"agy-flash38-medium", "medium", false}, {"agy-sonnet46", "medium", true}, {"cmd-dsflash41", "high", true}} {
		row := candOutcome(rec, c.profile, c.effort)
		if row == nil || row["outcome"] == "dropped:quota_unknown" || row["tier"] == nil || row["tier"] == "" {
			t.Fatalf("%s must be a ranked candidate with a tier: %v", c.profile, row)
		}
		// 3p paces on its 7d window and gemini on its 5h one (hk 3415); the
		// commandcode pool paces on its account window.
		if c.urgency && row["waste_urgency"] == nil {
			t.Fatalf("%s must carry its waste urgency: %v", c.profile, row)
		}
	}
	tried := map[string]bool{}
	for _, h := range rec["hosts"].([]any) {
		row := h.(map[string]any)
		if row["host"] != "mac-dev" {
			continue
		}
		for _, x := range row["tried"].([]any) {
			tried[x.(map[string]any)["profile"].(string)] = true
		}
	}
	if !tried[sel.chosen.seatName] {
		t.Fatalf("the chosen rung is on the tried list: %v", tried)
	}
}

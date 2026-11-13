package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// selFixture builds an app with a fake catalog, pool status table and
// placement source for exercising selectProfile. Local presence keys are
// registered per pool on demand.
func selFixture(t *testing.T, grade string, rows []CatalogRow, pools map[string]PoolView, presentPools []string) (*App, Spawn, *fakeQuota) {
	t.Helper()
	a := testApp(t)
	now := a.Now()
	var presence []PresenceRecord
	accounts := map[string]SharedAccount{}
	for i, pool := range presentPools {
		key := fmt.Sprintf("%s:%016x", pool, i+1)
		presence = append(presence, PresenceRecord{HostID: "local", AccountKey: key, LastSeen: now.Add(-time.Hour)})
		accounts[key] = SharedAccount{Key: key, Observed: true, Fresh: true, MeasuredAt: now.Add(-time.Hour)}
	}
	q := &fakeQuota{CatalogRows: rows, PoolViews: pools}
	a.Source = &fakePlacementSource{presence: presence, accounts: accounts, quotaDo: func(key string) *PoolView {
		pool, _, _ := strings.Cut(key, ":")
		v, ok := q.PoolViews[pool]
		if !ok && pool == "devin" {
			v, ok = PoolView{Status: "ok", Class: "spend", Blocking: ptrF(0)}, true
		}
		if !ok || q.PoolsErr != nil && pool != "devin" {
			return nil
		}
		if v.Status == "ok" && v.Class == "spend" && v.Blocking == nil && v.Projected == nil {
			v.Blocking = ptrF(0)
		}
		return &v
	}}
	a.Quota = q
	a.Config.Local = map[string]any{}
	a.Runner = &fakeRunner{}
	useFakeHK(t).SetTask("42", fmt.Sprintf(`{"id":42,"title":"work (grade %s, T1)","state":"ready","refs":{}}`, grade))
	s := Spawn{CWD: a.Home + "/repo", Job: "job-a", Task: "42", Role: "implementer", Host: "auto", T: "T1"}
	return a, s, q
}

func gatePools(pools map[string]string) func(QuotaRequest) (string, error) {
	return func(r QuotaRequest) (string, error) {
		name := firstNonempty(r.GateProfile, r.Profile)
		pool, ok := pools[name]
		if !ok {
			return "", fail(4, "quota refused for %s", name)
		}
		if r.AccountKey != "" {
			b, _ := json.Marshal(map[string]any{"schema": "scopefuel.gate.v1", "provider_id": pool, "account_key": r.AccountKey, "profile": name, "ok": true, "exit_code": 0})
			return string(b), nil
		}
		return fmt.Sprintf("pool=%s profile=%s\n", pool, name), nil
	}
}

func candOutcome(rec map[string]any, profile, effort string) map[string]any {
	cands, _ := rec["candidates"].([]any)
	for _, v := range cands {
		row := v.(map[string]any)
		if row["profile"] == profile && row["effort"] == effort {
			return row
		}
	}
	return nil
}

func TestSelectGradeFilterPrefersCheapestCovering(t *testing.T) {
	rows := []CatalogRow{
		{Profile: "devin-swe2", Effort: "high", Grade: "A+", Pool: "devin", Gate: "default", ModelID: "swe-2"},
		{Profile: "devin-swe2-medium", Effort: "medium", Grade: "A", Pool: "devin", Gate: "default", ModelID: "swe-2-medium"},
		{Profile: "devin-swe2-max", Effort: "max", Grade: "S+", Pool: "devin", Gate: "default", ModelID: "swe-2-max"},
		{Profile: "sonnet", Effort: "high", Grade: "C", Pool: "claude", Gate: "default", ModelID: "m"},
	}
	a, s, q := selFixture(t, "B", rows, nil, []string{"devin"})
	q.GateDo = gatePools(map[string]string{"devin-swe2-medium": "devin"})
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	if sel.chosen.seatName != "devin-swe2-medium" || sel.chosen.row.Grade != "A" {
		t.Fatalf("lowest covering grade must win: %+v", sel.chosen.row)
	}
	rec := sel.record()
	if got := candOutcome(rec, "sonnet", "high")["outcome"]; got != "dropped:grade_insufficient" {
		t.Fatalf("grade C must drop out: %v", got)
	}
	if got := candOutcome(rec, "devin-swe2", "high")["outcome"]; got != "below_choice" {
		t.Fatalf("a higher free rung ranks below the chosen rung: %v", got)
	}
}

func TestSelectDevinFlashRungsUseFreeTier(t *testing.T) {
	t.Setenv("PANEWIRE_POOL_POLICY", "")
	rows := []CatalogRow{
		{Profile: "devin-swe2-flash", Effort: "high", Grade: "B", Pool: "devin", Gate: "default", ModelID: "swe-2-flash-high"},
		{Profile: "devin-swe2-flash-medium", Effort: "medium", Grade: "C", Pool: "devin", Gate: "default", ModelID: "swe-2-flash-medium"},
	}
	a, s, q := selFixture(t, "C", rows, nil, []string{"devin"})
	serveRouting(t, strings.Replace(routingBlockV1,
		`"devin-swe2-max", "cmd-dsflash41"`,
		`"devin-swe2-max", "devin-swe2-flash", "devin-swe2-flash-medium", "devin-swe2-flash-max", "cmd-dsflash41"`, 1))
	q.GateDo = gatePools(map[string]string{"devin-swe2-flash": "devin", "devin-swe2-flash-medium": "devin"})
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"devin-swe2-flash": "high", "devin-swe2-flash-medium": "medium"}
	for profile, effort := range want {
		row := candOutcome(sel.record(), profile, effort)
		if row == nil {
			t.Fatalf("missing candidate %s@%s", profile, effort)
		}
		if row["tier"] != "free" {
			t.Errorf("%s@%s tier = %v, want free", profile, effort, row["tier"])
		}
		if row["outcome"] == "dropped:gate_refused" || row["reason"] == "gate_blocked:pool_excluded_by_policy" {
			t.Errorf("%s@%s was refused by pool policy: %v", profile, effort, row)
		}
	}
}

func TestSelectNonDevinGradeBPreservesCandidate(t *testing.T) {
	t.Setenv("PANEWIRE_POOL_POLICY", "")
	rows := []CatalogRow{
		{Profile: "devin-swe2-flash", Effort: "high", Grade: "B", Pool: "devin", Gate: "default", ModelID: "swe-2-flash-high"},
		{Profile: "devin-swe2-flash-medium", Effort: "medium", Grade: "C", Pool: "devin", Gate: "default", ModelID: "swe-2-flash-medium"},
		{Profile: "codex-sol", Effort: "high", Grade: "B", Pool: "codex", Gate: "default", ModelID: "gpt-6.1-sol"},
	}
	a, s, q := selFixture(t, "B", rows, nil, []string{"devin", "codex"})
	serveRouting(t, strings.Replace(routingBlockV1,
		`"devin-swe2-max", "cmd-dsflash41"`,
		`"devin-swe2-max", "devin-swe2-flash", "devin-swe2-flash-medium", "devin-swe2-flash-max", "cmd-dsflash41"`, 1))
	s.ExcludePools = "devin" // keeps the codex candidate the test follows
	q.GateDo = gatePools(map[string]string{"devin-swe2-flash": "devin", "devin-swe2-flash-medium": "devin", "codex-sol": "codex"})
	q.PoolViews = map[string]PoolView{"codex": {Status: "ok", Class: "spend", Blocking: ptrF(0)}}
	basePolicy := map[string]any{}
	if err := json.Unmarshal(defaultPoolOrder, &basePolicy); err != nil {
		t.Fatal(err)
	}
	freeSets := basePolicy["free"].([]any)
	freeSets[0].(map[string]any)["models"] = []any{"swe-2", "swe-2-medium", "swe-2-max"}
	baseBytes, err := json.Marshal(basePolicy)
	if err != nil {
		t.Fatal(err)
	}
	basePath := filepath.Join(t.TempDir(), "pool-order.json")
	if err := os.WriteFile(basePath, baseBytes, 0600); err != nil {
		t.Fatal(err)
	}
	a.Config.Local["pool_policy"] = basePath
	baseSel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	a.Config.Local["pool_policy"] = ""
	headSel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if baseSel.chosen.seatName != "codex-sol" || headSel.chosen.seatName != baseSel.chosen.seatName || headSel.chosen.row.ModelID != baseSel.chosen.row.ModelID {
		t.Fatalf("non-Devin grade B candidate changed: base=%s/%s head=%s/%s", baseSel.chosen.seatName, baseSel.chosen.row.ModelID, headSel.chosen.seatName, headSel.chosen.row.ModelID)
	}
}

func TestSelectPolicyOrderWasteFreeCreditRanked(t *testing.T) {
	waste, noWaste := true, false
	mk := func() []CatalogRow {
		return []CatalogRow{
			{Profile: "devin-swe2", Effort: "high", Grade: "S", Pool: "devin", Gate: "default", ModelID: "swe-2"},
			{Profile: "sonnet", Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "m"},
			{Profile: "opus", Effort: "high", Grade: "S", Pool: "anthropic-api", Gate: "default", ModelID: "m"},
			{Profile: "codex-sol", Effort: "high", Grade: "S", Pool: "codex", Gate: "default", ModelID: "m"},
			{Profile: "codex-sol", Effort: "max", Grade: "S", Pool: "codex", Gate: "default", ModelID: "m"},
		}
	}
	pools := map[string]PoolView{
		"claude":        {Status: "ok", Class: "spend", Waste: &waste, Blocking: ptrF(95)},
		"anthropic-api": {Status: "ok", Class: "spend", Waste: &noWaste, Blocking: ptrF(25)},
		"codex":         {Status: "ok", Class: "spend", Waste: &noWaste, Blocking: ptrF(10)},
	}
	gp := gatePools(map[string]string{"sonnet": "claude", "opus": "anthropic-api", "codex-sol": "codex", "devin-swe2": "devin"})
	cases := []struct {
		name    string
		rows    []CatalogRow
		pools   map[string]PoolView
		present []string
		want    string
	}{
		{"waste beats free", mk(), pools, []string{"devin", "claude", "anthropic-api", "codex"}, "sonnet"},
		{"waste beats credit and ranked", mk()[1:], pools, []string{"claude", "anthropic-api", "codex"}, "sonnet"},
		{"credit beside free beats ranked", mk()[2:], pools, []string{"anthropic-api", "codex"}, "opus"},
		{"ranked by blocking", mk()[3:], pools, []string{"codex"}, "codex-sol"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			a, s, q := selFixture(t, "S", tt.rows, tt.pools, tt.present)
			q.GateDo = gp
			sel, e := a.selectProfile(context.Background(), s)
			if e != nil {
				t.Fatal(e)
			}
			if sel.chosen.seatName != tt.want {
				t.Fatalf("want %s chosen, got %s (%s)", tt.want, sel.chosen.seatName, sel.chosen.row.Effort)
			}
			if sel.chosen.tierName == "" {
				t.Fatal("chosen candidate must carry its policy tier")
			}
		})
	}
}

func TestSelectWasteNeedsVerdict(t *testing.T) {
	// A spend pool without a verdict flag is ranked, not waste-promoted. It
	// ranks behind the free band, where the credit pool sits.
	waste, noWaste := true, false
	rows := []CatalogRow{
		{Profile: "sonnet", Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "m"},
		{Profile: "opus", Effort: "high", Grade: "S", Pool: "anthropic-api", Gate: "default", ModelID: "m"},
	}
	pools := map[string]PoolView{
		"claude":        {Status: "ok", Class: "spend", Blocking: ptrF(5)},
		"anthropic-api": {Status: "ok", Class: "spend", Waste: &noWaste, Blocking: ptrF(90)},
	}
	a, s, q := selFixture(t, "S", rows, pools, []string{"claude", "anthropic-api"})
	q.GateDo = gatePools(map[string]string{"sonnet": "claude", "opus": "anthropic-api"})
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	if got := candOutcome(sel.record(), "sonnet", "high")["tier"]; got != "ranked" {
		t.Fatalf("a spend pool without a verdict flag must stay ranked: %v", got)
	}
	if sel.chosen.seatName != "opus" || sel.chosen.tierName != "credit" {
		t.Fatalf("credit ranks in the free band before the ranked pool: %s in %s", sel.chosen.seatName, sel.chosen.tierName)
	}
	// With the flag set the same pool moves into the waste tier and wins.
	pools["claude"] = PoolView{Status: "ok", Class: "spend", Waste: &waste, Blocking: ptrF(5)}
	sel, e = a.selectProfile(context.Background(), s)
	if e != nil || sel.chosen.seatName != "sonnet" {
		t.Fatalf("waste flag must promote the pool: %v", e)
	}
}

func TestSelectUnknownOrDegradedQuotaNotAdmitted(t *testing.T) {
	rows := []CatalogRow{
		{Profile: "sonnet", Effort: "high", Grade: "S", Pool: "mystery", Gate: "default", ModelID: "m"},
		{Profile: "devin-swe2", Effort: "high", Grade: "S", Pool: "devin", Gate: "default", ModelID: "swe-2"},
	}
	a, s, q := selFixture(t, "S", rows, map[string]PoolView{}, []string{"devin", "mystery"})
	q.GateDo = gatePools(map[string]string{"sonnet": "mystery", "devin-swe2": "devin"})
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil || sel.chosen.seatName != "devin-swe2" {
		t.Fatalf("%v", e)
	}
	if got := candOutcome(sel.record(), "sonnet", "high")["outcome"]; got != "dropped:policy_unavailable" {
		t.Fatalf("a pool without a measurement source must not be guessed admissible: %v", got)
	}
	// Degraded, stale and excluded pools drop the same way.
	for _, pv := range []PoolView{
		{Status: "degraded", Class: "spend", Stale: true},
		{Status: "ok", Class: "spend", Stale: true},
		{Status: "ok", Class: "exclude"},
		{Status: "error", Class: "spend"},
	} {
		a2, s2, _ := selFixture(t, "S", rows[:1], map[string]PoolView{"mystery": pv}, []string{"mystery"})
		sel, e = a2.selectProfile(context.Background(), s2)
		if e == nil || sel == nil {
			t.Fatalf("status %+v must refuse", pv)
		}
		if got := candOutcome(sel.record(), "sonnet", "high")["outcome"]; got != "dropped:policy_unavailable" {
			t.Fatalf("status %+v must remain unmeasurable: %v", pv, got)
		}
	}
	// When the status read itself fails, free candidates still run.
	a, s, q = selFixture(t, "S", rows, nil, []string{"devin", "mystery"})
	q.PoolsErr = errString("status down")
	q.GateDo = gatePools(map[string]string{"devin-swe2": "devin"})
	sel, e = a.selectProfile(context.Background(), s)
	if e != nil || sel.chosen.seatName != "devin-swe2" {
		t.Fatalf("%v", e)
	}
	rec := sel.record()
	if got := candOutcome(rec, "sonnet", "high")["outcome"]; got != "dropped:policy_unavailable" {
		t.Fatalf("%v", got)
	}
	if rec["outcome_code"] != outcomeAdmitted {
		t.Fatalf("account evidence survives an unrelated local status failure: %v", rec)
	}
}

func TestSelectRequiresFreshPresence(t *testing.T) {
	rows := []CatalogRow{
		{Profile: "sonnet", Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "m"},
		{Profile: "opus", Effort: "high", Grade: "S", Pool: "anthropic-api", Gate: "default", ModelID: "m"},
	}
	waste := true
	noWaste := false
	pools := map[string]PoolView{
		"claude":        {Status: "ok", Class: "spend", Waste: &waste, Blocking: ptrF(5)},
		"anthropic-api": {Status: "ok", Class: "spend", Waste: &noWaste},
	}
	// claude presence exists only on a host that is not configured.
	a, s, q := selFixture(t, "S", rows, pools, []string{"anthropic-api"})
	src := a.Source.(*fakePlacementSource)
	src.presence = append(src.presence, PresenceRecord{HostID: "ghost", AccountKey: "claude:00000000000000ff", LastSeen: a.Now()})
	src.accounts["claude:00000000000000ff"] = SharedAccount{Key: "claude:00000000000000ff", Observed: true, Fresh: true}
	q.GateDo = gatePools(map[string]string{"sonnet": "claude", "opus": "anthropic-api"})
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil || sel.chosen.seatName != "opus" {
		t.Fatalf("%v", e)
	}
	if got := candOutcome(sel.record(), "sonnet", "high")["outcome"]; got != "dropped:no_presence" {
		t.Fatalf("presence on a non-spawn host must not admit: %v", got)
	}
	// Stale presence drops the same way.
	a2, s2, q2 := selFixture(t, "S", rows, pools, []string{"anthropic-api"})
	src2 := a2.Source.(*fakePlacementSource)
	src2.presence = append(src2.presence, PresenceRecord{HostID: "local", AccountKey: "claude:00000000000000ff", LastSeen: a2.Now().Add(-48 * time.Hour)})
	src2.accounts["claude:00000000000000ff"] = SharedAccount{Key: "claude:00000000000000ff", Observed: true, Fresh: true}
	q2.GateDo = q.GateDo
	sel, e = a2.selectProfile(context.Background(), s2)
	if e != nil || sel.chosen.seatName != "opus" {
		t.Fatalf("%v", e)
	}
	if got := candOutcome(sel.record(), "sonnet", "high")["outcome"]; got != "dropped:no_presence" {
		t.Fatalf("stale presence must not admit: %v", got)
	}
}

func TestSelectGateRefusalMovesToNextCandidate(t *testing.T) {
	waste := true
	rows := []CatalogRow{
		{Profile: "sonnet", Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "m", Score: ptrF(90)},
		{Profile: "opus", Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "m", Score: ptrF(80)},
	}
	pools := map[string]PoolView{"claude": {Status: "ok", Class: "spend", Waste: &waste, Blocking: ptrF(5)}}
	a, s, q := selFixture(t, "S", rows, pools, []string{"claude"})
	q.GateDo = func(r QuotaRequest) (string, error) {
		if r.Profile == "sonnet" {
			return "", fail(4, "pool exhausted")
		}
		return "pool=claude profile=" + r.Profile + "\n", nil
	}
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil || sel.chosen.seatName != "opus" {
		t.Fatalf("%v", e)
	}
	row := candOutcome(sel.record(), "sonnet", "high")
	if row["outcome"] != "dropped:gate_refused" || row["reason"] != "gate_refused:4" {
		t.Fatalf("the refusal code must be recorded: %v", row)
	}
}

func TestSelectDeterministic(t *testing.T) {
	waste := true
	rows := []CatalogRow{
		{Profile: "codex-sol", Effort: "high", Grade: "S", Pool: "codex", Gate: "default", ModelID: "m"},
		{Profile: "sonnet", Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "m"},
		{Profile: "opus", Effort: "high", Grade: "S", Pool: "anthropic-api", Gate: "default", ModelID: "m"},
		{Profile: "devin-swe2", Effort: "high", Grade: "S", Pool: "devin", Gate: "default", ModelID: "swe-2"},
	}
	pools := map[string]PoolView{
		"claude":        {Status: "ok", Class: "spend", Waste: &waste, Blocking: ptrF(5)},
		"anthropic-api": {Status: "ok", Class: "spend", Blocking: ptrF(25)},
		"codex":         {Status: "ok", Class: "spend", Blocking: ptrF(17)},
	}
	gp := gatePools(map[string]string{"sonnet": "claude", "opus": "anthropic-api", "codex-sol": "codex", "devin-swe2": "devin"})
	var first string
	for i := 0; i < 3; i++ {
		a, s, q := selFixture(t, "S", rows, pools, []string{"devin", "claude", "anthropic-api", "codex"})
		q.GateDo = gp
		sel, e := a.selectProfile(context.Background(), s)
		if e != nil {
			t.Fatal(e)
		}
		b, _ := encode(sel.record())
		if i == 0 {
			first = string(b)
			continue
		}
		if string(b) != first {
			t.Fatal("selection must be deterministic")
		}
	}
}

func TestSelectEscalationGateNeedsFlag(t *testing.T) {
	rows := []CatalogRow{
		{Profile: "sonnet", Effort: "high", Grade: "S", Pool: "claude", Gate: "escalation", ModelID: "m"},
	}
	waste := true
	pools := map[string]PoolView{"claude": {Status: "ok", Class: "spend", Waste: &waste, Blocking: ptrF(5)}}
	a, s, _ := selFixture(t, "S", rows, pools, []string{"claude"})
	sel, e := a.selectProfile(context.Background(), s)
	if e == nil {
		t.Fatal("an escalation rung must not be admitted silently")
	}
	if got := candOutcome(sel.record(), "sonnet", "high")["outcome"]; got != "dropped:gate_class" {
		t.Fatalf("%v", got)
	}
	a, s, q := selFixture(t, "S", rows, pools, []string{"claude"})
	s.AllowEscalation = true
	q.GateDo = gatePools(map[string]string{"sonnet": "claude"})
	sel, e = a.selectProfile(context.Background(), s)
	if e != nil || sel.chosen.seatName != "sonnet" {
		t.Fatalf("--allow-escalation must admit the rung: %v", e)
	}
}

func TestSelectReviewerExcludesAuthorFamily(t *testing.T) {
	// A reviewer routes to the reviewer role, which excludes the author's family.
	rows := []CatalogRow{
		{Profile: "opus", Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "m"},
		{Profile: "codex-terra61", Effort: "high", Grade: "S", Pool: "codex", Gate: "default", ModelID: "m"},
	}
	pools := map[string]PoolView{
		"claude": {Status: "ok", Class: "spend", Blocking: ptrF(5)},
		"codex":  {Status: "ok", Class: "spend", Blocking: ptrF(90)},
	}
	a, s, q := selFixture(t, "S", rows, pools, []string{"claude", "codex"})
	serveRouting(t, routingDocV1)
	s.Role = "reviewer"
	s.ParentFamily = "anthropic"
	q.GateDo = gatePools(map[string]string{"codex-terra61": "codex"})
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil || sel.chosen.seatName != "codex-terra61" {
		t.Fatalf("%v", e)
	}
	if got := candOutcome(sel.record(), "opus", "high")["outcome"]; got != "dropped:family_excluded" {
		t.Fatalf("the author's family must be excluded for reviewers: %v", got)
	}
}

func TestSelectRetiredAndNoSeatRowsDropped(t *testing.T) {
	ret := "2026-01-01T00:00:00Z"
	rows := []CatalogRow{
		{Profile: "sonnet", Grade: "S", Pool: "claude", Gate: "default", ModelID: "m", RetiredAt: &ret},
		{Profile: "no-such-profile", Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "m"},
		{Profile: "devin-swe2", Effort: "high", Grade: "S", Pool: "devin", Gate: "default", ModelID: "swe-2"},
	}
	a, s, q := selFixture(t, "S", rows, nil, []string{"devin"})
	q.GateDo = gatePools(map[string]string{"devin-swe2": "devin"})
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	if got := candOutcome(sel.record(), "sonnet", "")["outcome"]; got != "dropped:retired" {
		t.Fatalf("retired rows must not be candidates: %v", got)
	}
	if got := candOutcome(sel.record(), "no-such-profile", "high")["outcome"]; got != "dropped:no_seat" {
		t.Fatalf("catalog profiles without a seat must drop: %v", got)
	}
}

func TestSelectRecordsPolicyVersionAndOverride(t *testing.T) {
	a, s, q := selFixture(t, "B", []CatalogRow{{Profile: "devin-swe2", Effort: "high", Grade: "A+", Pool: "devin", Gate: "default", ModelID: "swe-2"}}, nil, []string{"devin"})
	q.GateDo = gatePools(map[string]string{"devin-swe2": "devin"})
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	rec := sel.record()
	if rec["policy_version"] != float64(3) && rec["policy_version"] != 3 {
		t.Fatalf("policy version must be recorded: %v", rec["policy_version"])
	}
	if rec["policy_source"] != "policy/quota-stop" {
		t.Fatalf("%v", rec["policy_source"])
	}
	req, _ := rec["requested"].(map[string]any)
	if req["grade"] != "B" || req["t"] != "T1" {
		t.Fatalf("the request must be recorded: %v", req)
	}
	if q != nil && len(q.Calls) == 0 {
		t.Fatal("the chosen candidate must pass the quota gate")
	}
}

func TestPoolsPrefersUsableDuplicateRows(t *testing.T) {
	doc := `{"schema":"scopefuel.v1","providers":[
	  {"id":"p","status":"stale","stale":true,"pool_class":"spend","verdict":{"waste":false,"blocking_pct":90}},
	  {"id":"p","status":"ok","stale":false,"pool_class":"spend","verdict":{"waste":true,"blocking_pct":5}},
	  {"id":"q","status":"error","stale":false,"pool_class":"exclude","verdict":{"waste":true,"blocking_pct":1}}
	]}`
	q := CLIQuota{Runner: &fakeRunner{Do: func(Command) Result { return Result{Out: doc} }}, Binary: "quota"}
	pools, e := q.Pools(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if pools["p"].Status != "ok" || pools["p"].Stale || pools["p"].Waste == nil || !*pools["p"].Waste || *pools["p"].Blocking != 5 {
		t.Fatalf("the fresh row must win: %+v", pools["p"])
	}
	if pools["q"].Status != "error" {
		t.Fatalf("%+v", pools["q"])
	}
}

func TestSpawnOverrideReasonRequired(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	s.OverrideReason = ""
	if code(a.Spawn(context.Background(), s)) != 2 {
		t.Fatal("explicit -m without --override-reason must refuse")
	}
	s.OverrideReason = "  "
	if code(a.Spawn(context.Background(), s)) != 2 {
		t.Fatal("a blank override reason must refuse")
	}
}

func TestSpawnOverrideRecorded(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	var placement map[string]any
	evs, _ := a.events(s.Job)
	for _, ev := range evs {
		if ev.Kind == "job.placement" {
			placement = ev.Payload
		}
	}
	_ = f
	if placement == nil {
		t.Fatal("placement record missing")
	}
	if placement["override_reason"] != "fixture" {
		t.Fatalf("override reason must reach the record: %v", placement)
	}
	sel, _ := placement["selection"].(map[string]any)
	if sel["mode"] != "override" || sel["profile"] != "cmd-dsflash41" {
		t.Fatalf("explicit choices must be marked as overrides: %v", sel)
	}
}

func TestSpawnAutoDryRunPrintsSelection(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	s.Profile = "auto"
	s.Host = "auto"
	s.DryRun = true
	q := a.Quota.(*fakeQuota)
	q.CatalogRows = []CatalogRow{{Profile: "cmd-dsflash41", Effort: "high", Grade: "A+", Pool: "example", Gate: "default", ModelID: "provider/example-model"}}
	q.PoolViews = map[string]PoolView{"example": {Status: "ok", Class: "spend"}}
	now := a.Now()
	key := "example:0123456789abcdef"
	a.Source = &fakePlacementSource{
		presence: []PresenceRecord{{HostID: "local", AccountKey: key, LastSeen: now}},
		accounts: map[string]SharedAccount{key: {Key: key, Observed: true, Fresh: true, Quota: &PoolView{Status: "ok", Class: "spend", Blocking: ptrF(0)}}},
	}
	q.GateDo = func(r QuotaRequest) (string, error) { return "pool=example\n", nil }
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	out := a.Out.(interface{ String() string }).String()
	if !strings.Contains(out, `"chosen"`) || !strings.Contains(out, `"seat":"cmd-dsflash41"`) {
		t.Fatalf("the decision record must be printed: %s", out)
	}
	joined := ""
	for _, c := range f.Calls {
		joined += c.Name + " " + strings.Join(c.Args, " ") + "\n"
	}
	for _, bad := range []string{"tab create", "pane "} {
		if strings.Contains(joined, bad) {
			t.Fatalf("dry-run must emit no side effects: %s", joined)
		}
	}
	if hkClaimed(t) {
		t.Fatal("dry-run must not claim the task")
	}
	if _, e := os.Stat(a.ledger().DB); e == nil {
		t.Fatal("dry-run must not touch the ledger")
	}
}

func TestSelectDropsRunglessRows(t *testing.T) {
	rows := []CatalogRow{
		{Profile: "devin-swe2", Grade: "S", Pool: "devin", Gate: "default", ModelID: "swe-2"},
		{Profile: "devin-swe2", Effort: "high", Grade: "S", Pool: "devin", Gate: "default", ModelID: "swe-2"},
	}
	a, s, q := selFixture(t, "S", rows, nil, []string{"devin"})
	q.GateDo = gatePools(map[string]string{"devin-swe2": "devin"})
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil || sel.chosen.row.Effort != "high" {
		t.Fatalf("the runged row must win: %v", e)
	}
	if got := candOutcome(sel.record(), "devin-swe2", "")["outcome"]; got != "dropped:effort_empty" {
		t.Fatalf("a rung-less row can never launch: %v", got)
	}
}

// A default-rung row (effort "") sorts before a runged row on the effort
// tiebreak; without the effort_empty drop it would be chosen and its launch
// could never pass Seat.Check. The runged sibling must win the dry-run.
func TestSpawnAutoSkipsDefaultRung(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	s.Profile = "auto"
	s.Host = "auto"
	s.DryRun = true
	q := a.Quota.(*fakeQuota)
	q.CatalogRows = []CatalogRow{
		{Profile: "cmd-dsflash41", Grade: "A+", Pool: "example", Gate: "default", ModelID: "provider/example-model"},
		{Profile: "cmd-dsflash41", Effort: "high", Grade: "A+", Pool: "example", Gate: "default", ModelID: "provider/example-model"},
	}
	q.PoolViews = map[string]PoolView{"example": {Status: "ok", Class: "spend"}}
	now := a.Now()
	key := "example:0123456789abcdef"
	a.Source = &fakePlacementSource{
		presence: []PresenceRecord{{HostID: "local", AccountKey: key, LastSeen: now}},
		accounts: map[string]SharedAccount{key: {Key: key, Observed: true, Fresh: true, Quota: &PoolView{Status: "ok", Class: "spend", Blocking: ptrF(0)}}},
	}
	q.GateDo = func(r QuotaRequest) (string, error) { return "pool=example\n", nil }
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	out := a.Out.(interface{ String() string }).String()
	var sel map[string]any
	for _, line := range strings.Split(out, "\n") {
		if v, ok := strings.CutPrefix(line, "selection="); ok {
			if e := json.Unmarshal([]byte(v), &sel); e != nil {
				t.Fatal(e)
			}
		}
	}
	chosen, _ := sel["chosen"].(map[string]any)
	if chosen["effort"] != "high" {
		t.Fatalf("the runged sibling must be chosen over the default rung: %v", chosen)
	}
	_ = f
}

func TestSpawnWaitValidatesTimeoutBeforePane(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	s.Wait = true
	s.WaitTimeout = "nonsense"
	if code(a.Spawn(context.Background(), s)) != 2 {
		t.Fatal("an unparseable --wait-timeout must refuse before spawning")
	}
	joined := ""
	for _, c := range f.Calls {
		joined += c.Name + " " + strings.Join(c.Args, " ") + "\n"
	}
	if strings.Contains(joined, "tab create") || strings.Contains(joined, "pane ") {
		t.Fatalf("no pane may launch after a timeout usage error: %s", joined)
	}
}

// withAgyGates adds every agy profile to a gatePools map as its own gate name:
// no agy profile is gated under another profile's name any more (hk 3859).
func withAgyGates(pools map[string]string) map[string]string {
	out := map[string]string{}
	for _, name := range []string{"agy-flash38-high", "agy-flash38-medium", "agy-flash38-low", "agy-sonnet46", "agy-opus46", "agy-oss120"} {
		out[name] = "agy"
	}
	for name, pool := range pools {
		out[name] = pool
	}
	return out
}

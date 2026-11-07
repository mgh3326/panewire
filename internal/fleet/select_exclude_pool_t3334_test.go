package fleet

import (
	"context"
	"slices"
	"strings"
	"testing"
)

// --exclude-pool drops the named pool's candidates with reason pool_excluded
// (hk 3305, 3334): the dry-run decision record shows the drop and the other
// pool wins.
func TestSpawnDryRunExcludePoolDropsPoolWithReason(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	_ = f
	s.Profile, s.Host, s.DryRun = "auto", "auto", true
	s.ExcludePools = "example"
	q := a.Quota.(*fakeQuota)
	q.CatalogRows = []CatalogRow{
		{Profile: "cmd-dsflash41", Effort: "high", Grade: "A+", Pool: "example", Gate: "default", ModelID: "provider/example-model"},
		{Profile: "devin-swe2", Effort: "high", Grade: "A+", Pool: "devin", Gate: "default", ModelID: "swe-2"},
	}
	q.PoolViews = map[string]PoolView{"example": {Status: "ok", Class: "spend"}, "devin": {Status: "ok", Class: "spend"}}
	now := a.Now()
	keys := map[string]string{"example": "example:0123456789abcdef", "devin": "devin:0123456789abcdef"}
	a.Source = &fakePlacementSource{
		presence: []PresenceRecord{{HostID: "local", AccountKey: keys["example"], LastSeen: now}, {HostID: "local", AccountKey: keys["devin"], LastSeen: now}},
		accounts: map[string]SharedAccount{
			keys["example"]: {Key: keys["example"], Observed: true, Fresh: true, Quota: &PoolView{Status: "ok", Class: "spend", Blocking: ptrF(0)}},
			keys["devin"]:   {Key: keys["devin"], Observed: true, Fresh: true, Quota: &PoolView{Status: "ok", Class: "spend", Blocking: ptrF(0)}},
		},
	}
	q.GateDo = gatePools(map[string]string{"cmd-dsflash41": "example", "devin-swe2": "devin"})
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	out := a.Out.(interface{ String() string }).String()
	if !strings.Contains(out, "dropped:pool_excluded") {
		t.Fatalf("the dry-run must show the excluded pool's candidate with reason pool_excluded: %s", out)
	}
	if !strings.Contains(out, `"seat":"devin-swe2"`) || strings.Contains(out, `"seat":"cmd-dsflash41"`) {
		t.Fatalf("the other pool must be the one chosen: %s", out)
	}
}

func TestSelectExcludePoolIsNotAPin(t *testing.T) {
	// A standing exclusion needs no containment, and the flag reaches the
	// hub submission args beside --exclude-host.
	s, e := parseSpawn([]string{"--exclude-pool", "claude,codex", "--exclude-host", "h1"})
	if e != nil {
		t.Fatal(e)
	}
	if e := validateContainment(s); e != nil {
		t.Fatalf("--exclude-pool must not need containment: %v", e)
	}
	args := submissionArgs(s)
	i := slices.Index(args, "--exclude-pool")
	if i < 0 || args[i+1] != "claude,codex" {
		t.Fatalf("submission args must carry --exclude-pool: %v", args)
	}
	if _, e := parseSpawn([]string{"--exclude-pool", "a b"}); e != nil {
		t.Fatalf("parse keeps the raw value; spawn validates it: %v", e)
	}
	if _, e := parsePools("good,bad pool"); e == nil {
		t.Fatal("an invalid pool name must be a usage error")
	}
}

package fleet

import (
	"context"
	"os"
	"strings"
	"testing"
)

// routingBlockPublished is routingBlockV1 under the key hk policy/model-routing
// actually publishes: the map kept its runtug name after #155 renamed the
// field, and every --role worker spawn then ran without the implementer filter.
var routingBlockPublished = strings.Replace(routingBlockV1, `"panewire_role_map"`, `"runtug_role_map"`, 1)

// A worker routed by the published document is an implementer: profiles the
// implementer role does not allow (codex-luna, haiku) never reach the ranking.
func TestRoutingPublishedRuntugRoleMapFiltersWorkers(t *testing.T) {
	rows := append(riskRows(),
		CatalogRow{Profile: "haiku", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "haiku-m"},
		CatalogRow{Profile: "codex-luna", Effort: "medium", Grade: "B", Pool: "codex", Gate: "default", ModelID: "luna-m"},
		CatalogRow{Profile: "codex-sol", Effort: "high", Grade: "S", Pool: "codex", Gate: "default", ModelID: "sol-m"},
	)
	pools := map[string]PoolView{"devin": {Status: "ok", Class: "spend", Blocking: ptrF(4)}, "claude": {Status: "ok", Class: "spend", Blocking: ptrF(10), Projected: ptrF(50)}, "codex": {Status: "ok", Class: "spend", Blocking: ptrF(40)}}
	a, s, q := selFixture(t, "A", rows, pools, []string{"devin", "claude", "codex"})
	q.GateDo = gatePools(map[string]string{"devin-swe2": "devin", "sonnet": "claude", "haiku": "claude", "codex-luna": "codex", "codex-sol": "codex"})
	serveRouting(a, routingDoc(routingBlockPublished))
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	if rt := routingStamp(t, sel); rt["routing_role"] != "implementer" {
		t.Fatalf("the published runtug_role_map must map worker to implementer: %v", rt)
	}
	for _, profile := range []string{"haiku", "codex-luna"} {
		effort := "high"
		if profile == "codex-luna" {
			effort = "medium"
		}
		if got := candOutcome(sel.record(), profile, effort)["reason"]; got != "role_filtered" {
			t.Errorf("%s is not an implementer profile: %v", profile, got)
		}
	}
	if p := sel.chosen.row.Profile; p == "haiku" || p == "codex-luna" {
		t.Fatalf("a profile outside implementer.allowed was chosen: %s", p)
	}
}

func TestRoutingRoleMapKeys(t *testing.T) {
	if p, e := parseRoutingDoc(routingDoc(routingBlockPublished)); e != nil || p.RoleMap["worker"] != "implementer" {
		t.Fatalf("runtug_role_map is read into the role map: %v %v", p.RoleMap, e)
	}
	both := strings.Replace(routingBlockV1, `"panewire_role_map": {`, `"runtug_role_map": {"builder": "lead", "worker": "implementer", "tester": "verifier", "reviewer": "reviewer", "counsel": "lead"}, "panewire_role_map": {`, 1)
	if _, e := parseRoutingDoc(routingDoc(both)); e != nil {
		t.Fatalf("two equal maps are one map: %v", e)
	}
	disagree := strings.Replace(both, `"runtug_role_map": {"builder": "lead", "worker": "implementer"`, `"runtug_role_map": {"builder": "lead", "worker": "explorer"`, 1)
	if _, e := parseRoutingDoc(routingDoc(disagree)); e == nil {
		t.Fatal("two maps that disagree must be refused, not resolved by precedence")
	}
	legacyUnknown := strings.Replace(routingBlockPublished, `"builder": "lead"`, `"builder": "boss"`, 1)
	if _, e := parseRoutingDoc(routingDoc(legacyUnknown)); e == nil {
		t.Fatal("the runtug_role_map is validated like panewire_role_map")
	}
}

// A seat role the published table does not map takes its builtin routing role
// instead of running unfiltered.
func TestRoutingUnmappedSeatRoleUsesBuiltinRole(t *testing.T) {
	block := strings.Replace(routingBlockV1, `, "panewire_role_map": {"builder": "lead", "worker": "implementer", "tester": "verifier", "reviewer": "reviewer", "counsel": "lead"}`, ``, 1)
	block = strings.Replace(block, "  },\n  \"panewire_role_map\": {\"builder\": \"lead\", \"worker\": \"implementer\", \"tester\": \"verifier\", \"reviewer\": \"reviewer\", \"counsel\": \"lead\"}\n", "  }\n", 1)
	if strings.Contains(block, "role_map") {
		t.Fatalf("fixture still carries a role map:\n%s", block)
	}
	a := testApp(t)
	a.Runner = &fakeRunner{}
	serveRouting(a, routingDoc(block))
	r, e := a.routeFor(context.Background(), "worker", "")
	if e != nil {
		t.Fatal(e)
	}
	if r.role != "implementer" || r.admits("codex-luna") || !r.admits("sonnet") {
		t.Fatalf("an unmapped worker must route as implementer: role=%q allowed=%v", r.role, r.allowed)
	}
}

// The cache keeps one spelling: a policy read under runtug_role_map is cached
// as panewire_role_map, and the cached copy still routes workers as implementers.
func TestRoutingCacheStoresPanewireRoleMap(t *testing.T) {
	ctx := context.Background()
	a := testApp(t)
	a.Runner = &fakeRunner{}
	serveRouting(a, routingDoc(routingBlockPublished))
	if _, e := a.routeFor(ctx, "worker", ""); e != nil {
		t.Fatal(e)
	}
	b, e := os.ReadFile(a.routingCachePath())
	if e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(string(b), "panewire_role_map") || strings.Contains(string(b), "runtug_role_map") {
		t.Fatalf("cache must carry only panewire_role_map:\n%s", b)
	}
	a.Runner = &fakeRunner{}
	serveRouting(a, "")
	r, e := a.routeFor(ctx, "worker", "")
	if e != nil || r.role != "implementer" || r.admits("codex-luna") {
		t.Fatalf("the cached policy routes workers as implementers: %+v %v", r, e)
	}
}

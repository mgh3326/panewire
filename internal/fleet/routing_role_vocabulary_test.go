package fleet

import (
	"context"
	"strings"
	"testing"
)

// routingBlockWithRoleMap is routingBlockV1 plus the seat-to-routing role map
// hk policy/model-routing used to publish. A seat role is its routing role
// now, so the map is read as an unknown field and changes nothing.
var routingBlockWithRoleMap = strings.Replace(routingBlockV1, "\n  }\n}", "\n  },\n  \"panewire_role_map\": {\"implementer\": \"consult\", \"reviewer\": \"verifier\", \"worker\": \"implementer\"}\n}", 1)

// An implementer spawn is routed as the implementer role: profiles the role
// does not allow (codex-luna, haiku) never reach the ranking.
func TestRoutingImplementerRoleFiltersProfiles(t *testing.T) {
	rows := append(riskRows(),
		CatalogRow{Profile: "haiku", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "haiku-m"},
		CatalogRow{Profile: "codex-luna", Effort: "medium", Grade: "B", Pool: "codex", Gate: "default", ModelID: "luna-m"},
		CatalogRow{Profile: "codex-sol", Effort: "high", Grade: "S", Pool: "codex", Gate: "default", ModelID: "sol-m"},
	)
	pools := map[string]PoolView{"devin": {Status: "ok", Class: "spend", Blocking: ptrF(4)}, "claude": {Status: "ok", Class: "spend", Blocking: ptrF(10), Projected: ptrF(50)}, "codex": {Status: "ok", Class: "spend", Blocking: ptrF(40)}}
	a, s, q := selFixture(t, "A", rows, pools, []string{"devin", "claude", "codex"})
	q.GateDo = gatePools(map[string]string{"devin-swe2": "devin", "sonnet": "claude", "haiku": "claude", "codex-luna": "codex", "codex-sol": "codex"})
	serveRouting(t, routingDoc(routingBlockWithRoleMap))
	if s.Role != "implementer" {
		t.Fatalf("fixture spawn role %q, want implementer", s.Role)
	}
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	if rt := routingStamp(t, sel); rt["routing_role"] != "implementer" {
		t.Fatalf("an implementer spawn must route as implementer, whatever role map the table carries: %v", rt)
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

// A seat role routes as itself. The table's retired role map cannot relabel
// it, and the pre-rename seat names are no roles at all.
func TestRoutingSeatRoleIsRoutingRole(t *testing.T) {
	a := testApp(t)
	a.Runner = &fakeRunner{}
	serveRouting(t, routingDoc(routingBlockWithRoleMap))
	for _, role := range []string{"implementer", "reviewer", "consult"} {
		r, e := a.routeFor(context.Background(), role, "")
		if e != nil || r.role != role {
			t.Fatalf("seat role %s routed as %+v: %v", role, r, e)
		}
	}
	if r, _ := a.routeFor(context.Background(), "implementer", ""); r.admits("codex-luna") || !r.admits("sonnet") {
		t.Fatalf("implementer must keep the implementer allow list: %v", r.allowed)
	}
	// A routing override only names a routing role; consult cannot be
	// relabelled to escape its admission.
	if _, e := a.routeFor(context.Background(), "consult", "implementer"); exitCode(e) != exitSeatRefused {
		t.Fatalf("consult relabelled as implementer: %v", e)
	}
	if _, e := a.routeFor(context.Background(), "implementer", "worker"); exitCode(e) != exitUsage {
		t.Fatalf("a retired role name is no routing role: %v", e)
	}
}

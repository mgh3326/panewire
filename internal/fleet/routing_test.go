package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
)

// routingBlockV1 is the machine-readable block of hk policy/model-routing as
// the operator recorded it on 2026-11-01.
const routingBlockV1 = `{
  "version": 1,
  "roles": {
    "consult":     {"allowed": ["opus", "fable", "codex-sol", "codex-astra"], "effort": "high", "on_exhaustion": "wait"},
    "implementer": {"allowed": ["sonnet", "opus", "codex-terra61", "codex-sol", "devin-swe2", "devin-swe2-medium", "devin-swe2-max", "cmd-dsflash41"], "effort": "high", "on_exhaustion": "wait"},
    "explorer":    {"allowed": ["sonnet", "haiku", "codex-luna", "devin-swe2-medium", "agy-flash38-high"], "effort": "medium", "on_exhaustion": "downgrade_to", "downgrade_to": ["haiku", "devin-swe2-medium"]},
    "reviewer":    {"allowed": ["*"], "effort": "high", "on_exhaustion": "escalate", "family_rule": "not_author"},
    "verifier":    {"allowed": ["opus", "codex-terra61"], "effort": "high", "on_exhaustion": "wait", "fixed": true},
    "judge":       {"allowed": ["*"], "effort": "high", "on_exhaustion": "escalate", "family_rule": "not_parent"}
  }
}`

var routingDocV1 = routingDoc(routingBlockV1)

func routingDoc(block string) string {
	return "# policy/model-routing\n\nRules in prose.\n\n## Machine-readable block (v1)\n```json\n" + block + "\n```\nProfile names are pacelever launch profiles.\n"
}

// serveRouting makes the test's fake hk answer GET
// /v1/documents/policy%2Fmodel-routing with body from now on; an empty body
// makes hk unavailable (503).
func serveRouting(t *testing.T, body string) {
	t.Helper()
	useFakeHK(t).Handle(func(r hkRequest) (int, string, bool) {
		if r.Route() != "GET /v1/documents/policy%2Fmodel-routing" {
			return 0, "", false
		}
		if body == "" {
			return http.StatusServiceUnavailable, `{"error":"unavailable"}`, true
		}
		b, _ := json.Marshal(map[string]any{"key": routingDocKey, "body": body})
		return http.StatusOK, string(b), true
	})
}

func routingStamp(t *testing.T, sel *selPlan) map[string]any {
	t.Helper()
	if sel == nil {
		t.Fatal("no selection plan")
	}
	rt, ok := sel.record()["routing"].(map[string]any)
	if !ok {
		t.Fatalf("selection record has no routing stamp: %v", sel.record())
	}
	return rt
}

// consultFixture is the incident: a grade A task, the claude pool wasted quota
// with sonnet its cheapest rung, and lead profiles (opus, fable, codex-sol)
// available above it.
func consultFixture(t *testing.T) (*App, Spawn, *fakeQuota) {
	t.Helper()
	rows := []CatalogRow{
		{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "sonnet-m"},
		{Profile: "opus", Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "opus-m"},
		{Profile: "fable", Effort: "high", Grade: "S+", Pool: "claude", Gate: "default", ModelID: "fable-m"},
		{Profile: "codex-sol", Effort: "high", Grade: "S", Pool: "codex", Gate: "default", ModelID: "sol-m"},
	}
	pools := map[string]PoolView{"claude": riskPool(30, 8), "codex": {Status: "ok", Class: "spend", Blocking: ptrF(40)}}
	a, s, q := selFixture(t, "A", rows, pools, []string{"claude", "codex"})
	q.GateDo = gatePools(map[string]string{"sonnet": "claude", "opus": "claude", "fable": "claude", "codex-sol": "codex"})
	serveRouting(t, routingDocV1)
	return a, s, q
}

// Acceptance 1: role lead, grade A, wasted claude pool with sonnet cheapest
// resolves to an allowed lead profile or refuses role_exhausted_wait; never
// sonnet.
func TestRoutingLeadNeverDowngradesToSonnet(t *testing.T) {
	ctx := context.Background()

	// Control: without routing the incident reproduces — sonnet wins.
	a, s, _ := consultFixture(t)
	s.Role = "consult"
	sel, e := a.selectFor(ctx, s, selReq{grade: "A", t: s.T, role: s.Role, urgency: urgencyNormal, urgencySource: "default"})
	if e != nil || sel.chosen.seat.Canon != "sonnet" {
		t.Fatalf("control: unrouted builder must pick the cheapest rung, sonnet: %v %+v", e, sel)
	}

	for _, tc := range []struct{ name, role, override string }{
		{"resolve --role consult (implementer seat)", "implementer", "consult"},
		{"--role consult", "consult", ""},
	} {
		a, s, _ := consultFixture(t)
		s.Role, s.RoutingRole = tc.role, tc.override
		sel, e := a.selectProfile(ctx, s)
		if e != nil {
			t.Fatalf("%s: %v", tc.name, e)
		}
		if got := sel.chosen.seat.Canon; got != "opus" {
			t.Fatalf("%s: lead must resolve to the cheapest allowed lead profile (opus), got %s", tc.name, got)
		}
		if got := candOutcome(sel.record(), "sonnet", "high")["reason"]; got != "role_filtered" {
			t.Fatalf("%s: sonnet must be role_filtered, got %v", tc.name, got)
		}
		rt := routingStamp(t, sel)
		if rt["routing_role"] != "consult" || rt["routing_version"] != 1 || rt["policy_source"] != "hk" {
			t.Fatalf("%s: routing stamp: %v", tc.name, rt)
		}
	}

	// Every lead profile refused at its gate while sonnet would still admit:
	// refuse role_exhausted_wait with the earliest reset, never sonnet.
	a, s, q := consultFixture(t)
	s.Role = "consult"
	q.GateDo = exhaustedGatePools(map[string]string{"sonnet": "claude"})
	sel, e = a.selectProfile(ctx, s)
	if exitCode(e) != exitRoleWait || !strings.Contains(e.Error(), reasonRoleExhaustWait) {
		t.Fatalf("want exit %d %s, got %v", exitRoleWait, reasonRoleExhaustWait, e)
	}
	if !strings.Contains(e.Error(), "earliest reset 2026-01-01T08:00:00Z (in 8.0h)") {
		t.Fatalf("the refusal names the earliest reset: %v", e)
	}
	if sel.chosen != nil {
		t.Fatalf("nothing may be chosen: %+v", sel.chosen.row)
	}
	if rt := routingStamp(t, sel); rt["refused"] != reasonRoleExhaustWait {
		t.Fatalf("routing stamp names the refusal: %v", rt)
	}
	for _, c := range q.Calls {
		if c.Profile == "sonnet" {
			t.Fatalf("sonnet's gate must never be asked for a lead: %+v", q.Calls)
		}
	}
}

func TestExplorerRoleRefusedWithImplementerHint(t *testing.T) {
	a := &App{Out: &bytes.Buffer{}}
	err := a.resolveCmd(context.Background(), []string{"--role", "explorer"})
	if code(err) != exitUsage || !strings.Contains(err.Error(), "use --role implementer") {
		t.Fatalf("resolve explorer: got %v, want usage error recommending implementer", err)
	}
}

// Acceptance 2: the implementer role keeps today's ordering among its allowed
// profiles — quota that would be wasted precedes free SWE-2.
func TestRoutingImplementerKeepsPoolOrder(t *testing.T) {
	rows := append(riskRows(),
		CatalogRow{Profile: "haiku", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "haiku-m"},
		CatalogRow{Profile: "codex-sol", Effort: "high", Grade: "S", Pool: "codex", Gate: "default", ModelID: "sol-m"},
	)
	pools := map[string]PoolView{"devin": {Status: "ok", Class: "spend", Blocking: ptrF(4)}, "claude": {Status: "ok", Class: "spend", Blocking: ptrF(10), Projected: ptrF(50)}, "codex": {Status: "ok", Class: "spend", Blocking: ptrF(40)}}
	gates := map[string]string{"devin-swe2": "devin", "sonnet": "claude", "haiku": "claude", "codex-sol": "codex"}
	run := func(unrouted bool) *selPlan {
		a, s, q := selFixture(t, "A", rows, pools, []string{"devin", "claude", "codex"})
		q.GateDo = gatePools(gates)
		serveRouting(t, routingDocV1)
		var sel *selPlan
		var e error
		if unrouted {
			sel, e = a.selectFor(context.Background(), s, selReq{grade: "A", t: s.T, role: s.Role, urgency: urgencyNormal, urgencySource: "default"})
		} else {
			sel, e = a.selectProfile(context.Background(), s)
		}
		if e != nil {
			t.Fatal(e)
		}
		return sel
	}
	names := func(sel *selPlan) []string {
		var out []string
		for _, c := range sel.ranked {
			out = append(out, c.row.Profile)
		}
		return out
	}
	routed, unrouted := run(false), run(true)
	if routed.chosen.row.Profile != "sonnet" || routed.chosen.tierName != "waste" {
		t.Fatalf("waste quota precedes free SWE-2 for implementers: %+v", routed.chosen.row)
	}
	want := slices.DeleteFunc(names(unrouted), func(p string) bool { return p == "haiku" })
	if got := names(routed); !slices.Equal(got, want) {
		t.Fatalf("implementer order must equal today's order minus non-allowed profiles: got %v want %v", got, want)
	}
	if got := candOutcome(routed.record(), "haiku", "high")["reason"]; got != "role_filtered" {
		t.Fatalf("haiku is not an implementer profile: %v", got)
	}
	if rt := routingStamp(t, routed); rt["routing_role"] != "implementer" {
		t.Fatalf("implementer routes as implementer: %v", rt)
	}
}

// Acceptance 3: hk unreachable with a cache uses the cache and stamps
// cache@v1; without a cache lead is refused and implementer proceeds
// unfiltered with builtin_default.
func TestRoutingCacheFallback(t *testing.T) {
	ctx := context.Background()
	a, s, _ := consultFixture(t)
	s.Role = "consult"
	if _, e := a.selectProfile(ctx, s); e != nil {
		t.Fatal(e)
	}
	b, e := os.ReadFile(a.routingCachePath())
	if e != nil || !strings.Contains(string(b), `"version": 1`) {
		t.Fatalf("a good hk read caches the policy with its version: %v %s", e, b)
	}

	// hk goes away: the cache still filters and is stamped.
	serveRouting(t, "")
	sel, e := a.selectProfile(ctx, s)
	if e != nil || sel.chosen.seat.Canon != "opus" {
		t.Fatalf("cached policy must still filter the lead: %v", e)
	}
	if rt := routingStamp(t, sel); rt["policy_source"] != "cache@v1" || rt["routing_version"] != 1 {
		t.Fatalf("cache stamp: %v", rt)
	}
	if !strings.Contains(a.Err.(*bytes.Buffer).String(), "using the cached routing policy v1") {
		t.Fatalf("cache use is warned: %s", a.Err)
	}

	// An unreadable document falls back the same way.
	serveRouting(t, "# policy/model-routing\n\nno block\n")
	if sel, e = a.selectProfile(ctx, s); e != nil || routingStamp(t, sel)["policy_source"] != "cache@v1" {
		t.Fatalf("an unreadable document uses the cache: %v", e)
	}

	// No cache: consult (resolve --role consult) and verifier are
	// refused; implementer proceeds unfiltered.
	if e := os.Remove(a.routingCachePath()); e != nil {
		t.Fatal(e)
	}
	serveRouting(t, "")
	for _, tc := range []struct{ role, override string }{{"consult", ""}, {"implementer", "consult"}, {"implementer", "verifier"}} {
		s.Role, s.RoutingRole, s.ParentFamily = tc.role, tc.override, "openai"
		if _, e := a.selectProfile(ctx, s); exitCode(e) != exitConfig {
			t.Fatalf("%s/%s without hk or cache must exit %d, got %v", tc.role, tc.override, exitConfig, e)
		}
	}
	s.Role, s.RoutingRole, s.ParentFamily = "implementer", "", ""
	sel, e = a.selectProfile(ctx, s)
	if e != nil {
		t.Fatal(e)
	}
	rt := routingStamp(t, sel)
	if rt["policy_source"] != routingSourceBuiltin || rt["routing_role"] != "implementer" || rt["allowed"] != nil {
		t.Fatalf("implementer proceeds with builtin_default and no filter: %v", rt)
	}
	if sel.chosen.seat.Canon != "sonnet" {
		t.Fatalf("builtin_default filters nothing: %s", sel.chosen.seat.Canon)
	}
	s.RoutingRole = "implementer"
	if sel, e = a.selectProfile(ctx, s); e != nil || routingStamp(t, sel)["policy_source"] != routingSourceBuiltin {
		t.Fatalf("implementer proceeds with builtin_default: %v", e)
	}
}

// Acceptance 4: downgrade_to retries with only its profiles and stamps
// downgraded_from; wait and escalate refuse with their reasons.
func TestRoutingOnExhaustion(t *testing.T) {
	ctx := context.Background()
	rows := []CatalogRow{
		{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "sonnet-m"},
		{Profile: "haiku", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "haiku-m"},
		{Profile: "codex-luna", Effort: "high", Grade: "A", Pool: "codex", Gate: "default", ModelID: "luna-m"},
	}
	pools := map[string]PoolView{"claude": {Status: "ok", Class: "spend", Blocking: ptrF(10)}, "codex": riskPool(120, 30)}
	block := strings.Replace(routingBlockV1, `"implementer": {"allowed": ["sonnet", "opus", "codex-terra61", "codex-sol", "devin-swe2", "devin-swe2-medium", "devin-swe2-max", "cmd-dsflash41"], "effort": "high", "on_exhaustion": "wait"}`, `"implementer": {"allowed": ["sonnet", "codex-luna"], "effort": "high", "on_exhaustion": "downgrade_to", "downgrade_to": ["haiku", "devin-swe2-medium"]}`, 1)
	fixture := func(gates map[string]string) (*App, Spawn) {
		a, s, q := selFixture(t, "A", rows, pools, []string{"claude", "codex"})
		q.GateDo = exhaustedGatePools(gates)
		serveRouting(t, routingDoc(block))
		s.RoutingRole = "implementer"
		return a, s
	}

	// Implementer: sonnet refused at its gate, codex-luna's pool exhausted →
	// downgrade to haiku.
	a, s := fixture(map[string]string{"haiku": "claude"})
	sel, e := a.selectProfile(ctx, s)
	if e != nil || sel.chosen.row.Profile != "haiku" {
		t.Fatalf("downgrade_to must choose haiku: %v", e)
	}
	rt := routingStamp(t, sel)
	if rt["downgraded_from"] != "implementer" || !slices.Equal(rt["allowed"].([]string), []string{"haiku", "devin-swe2-medium"}) {
		t.Fatalf("downgrade stamp: %v", rt)
	}
	if before, _ := rt["before_downgrade"].([]any); len(before) != len(rows) {
		t.Fatalf("the exhausted role attempt is kept: %v", rt["before_downgrade"])
	}

	// The downgrade profiles are exhausted too → wait.
	a, s = fixture(map[string]string{})
	if _, e := a.selectProfile(ctx, s); exitCode(e) != exitRoleWait || !strings.Contains(e.Error(), reasonRoleExhaustWait) {
		t.Fatalf("an exhausted downgrade waits: %v", e)
	}

	// Lead: wait.
	a, s = fixture(map[string]string{"sonnet": "claude", "haiku": "claude"})
	rows = append(rows, CatalogRow{Profile: "opus", Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "opus-m"})
	a.Quota.(*fakeQuota).CatalogRows = rows
	s.RoutingRole = "consult"
	if sel, e = a.selectProfile(ctx, s); exitCode(e) != exitRoleWait || !strings.HasPrefix(e.Error(), reasonRoleExhaustWait) {
		t.Fatalf("lead waits: %v", e)
	}
	if routingStamp(t, sel)["refused"] != reasonRoleExhaustWait {
		t.Fatal("wait refusal is stamped")
	}

	// Reviewer ("*", escalate): every candidate exhausted → escalate.
	a, s = fixture(map[string]string{})
	s.RoutingRole, s.Role, s.ParentFamily = "", "reviewer", "xai"
	if sel, e = a.selectProfile(ctx, s); exitCode(e) != exitRoleEscalate || !strings.HasPrefix(e.Error(), reasonRoleExhaustEscal) {
		t.Fatalf("reviewer escalates: %v", e)
	}
	if routingStamp(t, sel)["refused"] != reasonRoleExhaustEscal {
		t.Fatal("escalate refusal is stamped")
	}

	// A role whose profiles all fail the grade (opus is S) is not exhausted: it refuses
	// role_no_candidate and never waits or downgrades.
	a, s = fixture(map[string]string{"sonnet": "claude", "opus": "claude"})
	s.RoutingRole, s.Grade = "verifier", "S+"
	if _, e := a.selectProfile(ctx, s); exitCode(e) != exitSeatRefused || !strings.HasPrefix(e.Error(), reasonRoleNoCandidate) {
		t.Fatalf("no verifier profile in the catalog: %v", e)
	}
}

// Acceptance 5: `panewire resolve --role consult --grade A --json` prints the
// profile `plan` chooses and spawns nothing.
func TestResolveMatchesPlanAndSpawnsNothing(t *testing.T) {
	setup := func() (*App, *fakeRunner, *bytes.Buffer) {
		a, q, fr, _, out := planFixture(t)
		q.CatalogRows = append(q.CatalogRows,
			CatalogRow{Profile: "opus", Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "opus-m"},
			CatalogRow{Profile: "codex-sol", Effort: "high", Grade: "S", Pool: "codex", Gate: "default", ModelID: "sol-m"},
		)
		gate := q.GateDo
		q.GateDo = func(r QuotaRequest) (string, error) {
			if firstNonempty(r.GateProfile, r.Profile) == "opus" {
				r.Profile, r.GateProfile = "sonnet", "sonnet" // same claude pool
			}
			return gate(r)
		}
		serveRouting(t, routingDocV1)
		return a, fr, out
	}

	a, _, out := setup()
	if e := a.Run(context.Background(), []string{"plan", "--t", "T1", "--role", "consult", "--grade", "A", "--json"}); e != nil {
		t.Fatalf("plan: %v\n%s", e, out)
	}
	var plan map[string]any
	if e := json.Unmarshal(out.Bytes(), &plan); e != nil {
		t.Fatal(e)
	}
	planChosen := plan["selection"].(map[string]any)["chosen"].(map[string]any)
	if planChosen["canon"] != "opus" || plan["chosen"].(map[string]any)["seat"] != "opus" {
		t.Fatalf("plan for a lead picks opus: %v", planChosen)
	}
	if rt := plan["selection"].(map[string]any)["routing"].(map[string]any); rt["routing_role"] != "consult" || rt["policy_source"] != "hk" {
		t.Fatalf("plan stamps the routing: %v", rt)
	}

	a, fr, out := setup()
	if e := a.Run(context.Background(), []string{"resolve", "--role", "consult", "--grade", "A", "--json"}); e != nil {
		t.Fatalf("resolve: %v\n%s", e, out)
	}
	var r resolveResult
	if e := json.Unmarshal(out.Bytes(), &r); e != nil {
		t.Fatalf("%v\n%s", e, out)
	}
	if r.Profile != planChosen["canon"] || r.Seat != plan["chosen"].(map[string]any)["seat"] || r.Effort != "high" {
		t.Fatalf("resolve must print plan's choice: %+v", r)
	}
	if r.RoutingRole != "consult" || r.RoutingVersion != 1 || r.PolicySource != "hk" {
		t.Fatalf("resolve stamps the routing: %+v", r)
	}
	// The answer is last.
	if body := strings.TrimSpace(out.String()); !strings.HasSuffix(body, `"effort": "high"
}`) {
		t.Fatalf("the answer is the last thing printed:\n%s", body)
	}
	if len(fr.Calls) != 0 {
		t.Fatalf("resolve runs nothing: %v", fr.Calls)
	}
	for _, route := range useFakeHK(t).Routes() {
		if route != "GET /v1/documents/policy%2Fmodel-routing" {
			t.Fatalf("resolve only reads the routing table: %v", useFakeHK(t).Routes())
		}
	}

	// resolve --t passes the task scale to selection exactly as plan does:
	// at each scale both choose the same profile and record the same request.
	for _, scale := range []string{"T1", "T2"} {
		a, _, out := setup()
		if e := a.Run(context.Background(), []string{"plan", "--t", scale, "--role", "consult", "--grade", "A", "--json"}); e != nil {
			t.Fatalf("plan --t %s: %v\n%s", scale, e, out)
		}
		var plan map[string]any
		if e := json.Unmarshal(out.Bytes(), &plan); e != nil {
			t.Fatal(e)
		}
		planSel := plan["selection"].(map[string]any)
		a, _, out = setup()
		if e := a.Run(context.Background(), []string{"resolve", "--role", "consult", "--t", scale, "--grade", "A", "--json"}); e != nil {
			t.Fatalf("resolve --t %s: %v\n%s", scale, e, out)
		}
		var r resolveResult
		if e := json.Unmarshal(out.Bytes(), &r); e != nil {
			t.Fatal(e)
		}
		if r.T != scale || r.Profile != planSel["chosen"].(map[string]any)["canon"] || r.Seat != plan["chosen"].(map[string]any)["seat"] {
			t.Fatalf("--t %s: resolve %+v disagrees with plan %v", scale, r, planSel["chosen"])
		}
		if got, want := r.Selection["requested"].(map[string]any)["t"], planSel["requested"].(map[string]any)["t"]; got != scale || want != scale {
			t.Fatalf("--t %s reaches selection: resolve %v plan %v", scale, got, want)
		}
	}

	// Text form: the last line is the answer.
	a, _, out = setup()
	if e := a.Run(context.Background(), []string{"resolve", "--role", "consult", "--grade", "A"}); e != nil {
		t.Fatal(e)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if lines[len(lines)-1] != "profile=opus effort=high" {
		t.Fatalf("text answer last:\n%s", out)
	}

	for _, args := range [][]string{{"resolve"}, {"resolve", "--role", "boss"}, {"resolve", "--role", "reviewer"}, {"resolve", "--role", "implementer", "--grade", "Z"}, {"resolve", "--role", "consult", "--t", "T9"}} {
		if e := a.Run(context.Background(), args); exitCode(e) != exitUsage {
			t.Fatalf("%v: want usage exit, got %v", args, e)
		}
	}
}

func TestRoutingPolicyValidation(t *testing.T) {
	if _, e := parseRoutingDoc(routingDocV1); e != nil {
		t.Fatalf("the recorded v1 document parses: %v", e)
	}
	for name, block := range map[string]string{
		"version":    strings.Replace(routingBlockV1, `"version": 1`, `"version": 2`, 1),
		"exhaust":    strings.Replace(routingBlockV1, `"on_exhaustion": "escalate", "family_rule": "not_author"`, `"on_exhaustion": "panic"`, 1),
		"downgrade":  strings.Replace(routingBlockV1, `, "downgrade_to": ["haiku", "devin-swe2-medium"]`, ``, 1),
		"dup key":    strings.Replace(routingBlockV1, `"version": 1,`, `"version": 1, "version": 1,`, 1),
		"min_effort": strings.Replace(routingBlockV1, `"effort": "high", "on_exhaustion": "escalate"`, `"effort": "high", "min_effort": "turbo", "on_exhaustion": "escalate"`, 1),
	} {
		if _, e := parseRoutingDoc(routingDoc(block)); e == nil {
			t.Errorf("%s: invalid policy accepted", name)
		}
	}
	a := testApp(t)
	a.Runner = &fakeRunner{}
	serveRouting(t, routingDocV1)
	if _, e := a.routeFor(context.Background(), "implementer", "boss"); exitCode(e) != exitUsage {
		t.Fatalf("an unknown routing role is a usage error: %v", e)
	}
}

// The spawned job record carries the routing stamp, and a lead whose role
// admits nothing in the catalog never spawns.
func TestSpawnRecordsRouting(t *testing.T) {
	setup := func() (*App, Spawn, *fakeRunner) {
		a, s, f, _ := cmdFixture(t)
		s.Profile, s.Host = "auto", "auto"
		q := a.Quota.(*fakeQuota)
		q.CatalogRows = []CatalogRow{{Profile: "cmd-dsflash41", Effort: "high", Grade: "A+", Pool: "example", Gate: "default", ModelID: "provider/example-model"}}
		q.PoolViews = map[string]PoolView{"example": {Status: "ok", Class: "spend"}}
		key := "example:0123456789abcdef"
		a.Source = &fakePlacementSource{
			presence: []PresenceRecord{{HostID: "local", AccountKey: key, LastSeen: a.Now()}},
			accounts: map[string]SharedAccount{key: {Key: key, Observed: true, Fresh: true, Quota: &PoolView{Status: "ok", Class: "spend", Blocking: ptrF(0)}}},
		}
		q.GateDo = gatePools(map[string]string{"cmd-dsflash41": "example"})
		serveRouting(t, routingDocV1)
		return a, s, f
	}
	a, s, _ := setup()
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
	rt, _ := placement["selection"].(map[string]any)["routing"].(map[string]any)
	if rt["routing_role"] != "implementer" || rt["routing_version"] != float64(1) || rt["policy_source"] != "hk" {
		t.Fatalf("job.placement selection carries the routing stamp: %v", placement["selection"])
	}

	a, s, f := setup()
	s.RoutingRole = "consult"
	if e := a.Spawn(context.Background(), s); exitCode(e) != exitSeatRefused || !strings.Contains(e.Error(), reasonRoleNoCandidate) {
		t.Fatalf("a lead with no lead profile in the catalog is refused: %v", e)
	}
	for _, c := range f.Calls {
		if c.Name == "herdr" {
			t.Fatalf("a refused lead must not spawn: %v", f.Calls)
		}
	}
}

func TestClipKeepsMultibyteTextValid(t *testing.T) {
	if got := clip("a가나", 3); got != "a…" {
		t.Fatalf("clip=%q, want %q", got, "a…")
	}
	if got := clip("short", 10); got != "short" {
		t.Fatalf("clip changed short text: %q", got)
	}
}

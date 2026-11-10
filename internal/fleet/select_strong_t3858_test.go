package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// strongBlock is routingBlockV1 with agy-flash38-high admitted to implementer
// (so only the strong list can drop it), an optional implementer min_effort,
// and the strong_profiles list; "" leaves the field out.
func strongBlock(t *testing.T, strongProfiles, minEffort string) string {
	t.Helper()
	block := routingBlockV1
	swap := func(old, repl string) {
		t.Helper()
		if strings.Count(block, old) != 1 {
			t.Fatalf("fixture drifted: %q is not in routingBlockV1 exactly once", old)
		}
		block = strings.Replace(block, old, repl, 1)
	}
	swap(`"implementer": {"allowed": ["sonnet",`, `"implementer": {"allowed": ["agy-flash38-high", "sonnet",`)
	if minEffort != "" {
		swap(`"cmd-dsflash41"], "effort": "high"`, `"cmd-dsflash41"], "effort": "high", "min_effort": "`+minEffort+`"`)
	}
	if strongProfiles != "" {
		swap(`"panewire_role_map"`, `"strong_profiles": `+strongProfiles+`, "panewire_role_map"`)
	}
	return block
}

// strongFixture: a grade A task; opus is a ranked paid rung, agy-flash38-high a
// grade A rung on a pool at waste risk. Without strong the agy rung wins.
func strongFixture(t *testing.T, block string, extra ...CatalogRow) (*App, Spawn, *fakeQuota) {
	t.Helper()
	rows := append([]CatalogRow{
		{Profile: "opus", Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "opus-m"},
		{Profile: "agy-flash38-high", Effort: "high", Grade: "A", Pool: "agy", Gate: "default", ModelID: "flash-m"},
	}, extra...)
	pools := map[string]PoolView{"claude": {Status: "ok", Class: "spend", Blocking: ptrF(40)}, "agy": riskPool(30, 8), "codex": {Status: "ok", Class: "spend", Blocking: ptrF(40)}}
	a, s, q := selFixture(t, "A", rows, pools, []string{"claude", "agy", "codex"})
	q.GateDo = gatePools(map[string]string{"opus": "claude", "agy-flash": "agy", "codex-sol": "codex"})
	serveRouting(t, routingDoc(block))
	return a, s, q
}

func TestStrongMarkerParse(t *testing.T) {
	for _, tc := range []struct {
		title        string
		strong       bool
		grade, scale string
	}{
		{"work (grade A, T2, strong)", true, "A", "T2"},
		{"work (Grade A+, t2, STRONG) tail", true, "A+", "T2"},
		{"work (grade A, T2)", false, "A", "T2"},
		{"work (grade A, T2, strongish)", false, "A", "T2"},
		{"work (grade A, T2, strong-ish)", false, "A", "T2"},
		{"work (grade A, T2, weak)", false, "A", "T2"},
		{"Grade S+, T3, strong", true, "S+", "T3"},
	} {
		m := taskMarker.FindStringSubmatch(tc.title)
		if m == nil {
			t.Errorf("%q: no marker", tc.title)
			continue
		}
		if markerStrong(m) != tc.strong || !strings.EqualFold(m[1], tc.grade) || !strings.EqualFold(m[2], tc.scale) {
			t.Errorf("%q: strong=%v grade=%s scale=%s", tc.title, markerStrong(m), m[1], m[2])
		}
	}
}

func TestStrongMarkerSelectsFromStrongProfiles(t *testing.T) {
	ctx := context.Background()
	a, s, _ := strongFixture(t, strongBlock(t, `["opus"]`, ""))
	useFakeHK(t).SetTask("42", `{"id":42,"title":"work (grade A, T1, strong)","state":"ready","refs":{}}`)
	sel, e := a.selectProfile(ctx, s)
	if e != nil {
		t.Fatal(e)
	}
	if got := sel.chosen.seat.Canon; got != "opus" {
		t.Fatalf("a strong-marked task must not land on the waste-tier agy rung: %s", got)
	}
	// The marker's scale is still checked against --t.
	s.T = "T2"
	if _, e := a.selectProfile(ctx, s); exitCode(e) != exitSeatRefused || !strings.Contains(e.Error(), "does not match --t") {
		t.Fatalf("the --t check is unchanged: %v", e)
	}
	// The flag and the marker are OR-ed: --strong on an unmarked title.
	a, s, _ = strongFixture(t, strongBlock(t, `["opus"]`, ""))
	s.Strong = true
	if sel, e = a.selectProfile(ctx, s); e != nil || sel.chosen.seat.Canon != "opus" {
		t.Fatalf("--strong with an unmarked title: %v", e)
	}
}

func TestStrongDropsCandidatesOutsideTheList(t *testing.T) {
	ctx := context.Background()
	a, s, q := strongFixture(t, strongBlock(t, `["opus"]`, ""))
	s.Strong = true
	sel, e := a.selectProfile(ctx, s)
	if e != nil {
		t.Fatal(e)
	}
	if got := sel.chosen.seat.Canon; got != "opus" {
		t.Fatalf("strong must choose opus over the grade A waste rung: %s", got)
	}
	rec := sel.record()
	agy := candOutcome(rec, "agy-flash38-high", "high")
	if agy["reason"] != reasonNotStrong || agy["outcome"] != "dropped:not_strong" {
		t.Fatalf("the agy rung is recorded not_strong: %v", agy)
	}
	if rec["strong"] != true {
		t.Fatalf("the selection record names strong: %v", rec["strong"])
	}
	if rt := routingStamp(t, sel); rt["routing_role"] != "implementer" || rt["strong"] != true || !reflect.DeepEqual(rt["strong_profiles"], []string{"opus"}) {
		t.Fatalf("a strong spawn keeps its routing role and stamps the list: %v", rt)
	}
	for _, c := range q.Calls {
		if firstNonempty(c.GateProfile, c.Profile) == "agy-flash" {
			t.Fatalf("a dropped profile's gate must never be asked: %+v", q.Calls)
		}
	}
}

// A strong list names where a spawn may run, not where it must queue: a listed
// profile in the waste tier still goes first, and the role's min_effort floor
// still drops its cheaper rung.
func TestStrongKeepsTierOrderAndRoleEffortFloor(t *testing.T) {
	a, s, _ := strongFixture(t, strongBlock(t, `["opus", "codex-sol"]`, "high"))
	q := a.Quota.(*fakeQuota)
	q.CatalogRows = []CatalogRow{
		{Profile: "opus", Effort: "medium", Grade: "S", Pool: "claude", Gate: "default", ModelID: "opus-m"},
		{Profile: "opus", Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "opus-m"},
		{Profile: "codex-sol", Effort: "high", Grade: "S", Pool: "codex", Gate: "default", ModelID: "sol-m"},
		{Profile: "agy-flash38-high", Effort: "high", Grade: "A", Pool: "agy", Gate: "default", ModelID: "flash-m"},
	}
	q.PoolViews["claude"] = riskPool(30, 8) // opus's pool wastes quota; codex does not
	s.Strong = true
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	c := sel.chosen
	if c.seat.Canon != "opus" || c.row.Effort != "high" || c.tierName != "waste" {
		t.Fatalf("the listed waste-tier rung goes first at the effort floor: %s@%s tier=%s", c.seat.Canon, c.row.Effort, c.tierName)
	}
	rec := sel.record()
	if got := candOutcome(rec, "opus", "medium")["reason"]; got != reasonBelowRoleMinEffort {
		t.Fatalf("min_effort still applies: %v", got)
	}
	if got := candOutcome(rec, "agy-flash38-high", "high")["reason"]; got != reasonNotStrong {
		t.Fatalf("agy is outside the list: %v", got)
	}
	if got := candOutcome(rec, "codex-sol", "high")["outcome"]; got != "below_choice" {
		t.Fatalf("a listed ranked rung stays a lower choice: %v", got)
	}
}

func TestStrongRefusedWithoutStrongProfiles(t *testing.T) {
	ctx := context.Background()
	check := func(name string, a *App, s Spawn, q *fakeQuota) {
		t.Helper()
		s.Strong = true
		sel, e := a.selectProfile(ctx, s)
		if exitCode(e) != exitConfig || e == nil || !strings.Contains(e.Error(), "POLICY_UNAVAILABLE") || !strings.Contains(e.Error(), "strong") {
			t.Fatalf("%s: want the strict-role refusal, got %v", name, e)
		}
		if sel != nil && sel.chosen != nil {
			t.Fatalf("%s: nothing may be placed: %+v", name, sel.chosen.row)
		}
		if len(q.Calls) != 0 {
			t.Fatalf("%s: no quota gate may be asked: %+v", name, q.Calls)
		}
	}
	a, s, q := strongFixture(t, strongBlock(t, "", ""))
	check("no strong_profiles in hk", a, s, q)
	a, s, q = strongFixture(t, strongBlock(t, `[]`, ""))
	check("empty strong_profiles", a, s, q)
	a, s, q = strongFixture(t, strongBlock(t, `["opus"]`, ""))
	serveRouting(t, "") // hk down, no cache yet
	check("hk unavailable and no cache", a, s, q)

	// A cached list serves a strong spawn while hk is down.
	a, s, _ = strongFixture(t, strongBlock(t, `["opus"]`, ""))
	s.Strong = true
	if sel, e := a.selectProfile(ctx, s); e != nil || sel.chosen.seat.Canon != "opus" {
		t.Fatalf("hk read: %v", e)
	}
	serveRouting(t, "")
	sel, e := a.selectProfile(ctx, s)
	if e != nil || sel.chosen.seat.Canon != "opus" || routingStamp(t, sel)["policy_source"] != "cache@v1" {
		t.Fatalf("strong_profiles is cached with the table: %v", e)
	}
	// A later hk table without the list replaces the cached one: refuse.
	serveRouting(t, routingDoc(strongBlock(t, "", "")))
	if _, e := a.selectProfile(ctx, s); exitCode(e) != exitConfig {
		t.Fatalf("a table without the list refuses even after a listing was cached: %v", e)
	}
}

// Preservation: without the marker or --strong the strong list is never read,
// whatever the table carries.
func TestStrongAbsentKeepsTodaysSelection(t *testing.T) {
	var base string
	for _, block := range []string{strongBlock(t, "", ""), strongBlock(t, `["opus"]`, "")} {
		a, s, _ := strongFixture(t, block)
		sel, e := a.selectProfile(context.Background(), s)
		if e != nil {
			t.Fatal(e)
		}
		rec := sel.record()
		if _, ok := rec["strong"]; ok {
			t.Fatalf("a non-strong record carries no strong field: %v", rec["strong"])
		}
		if _, ok := routingStamp(t, sel)["strong"]; ok {
			t.Fatal("a non-strong routing stamp carries no strong field")
		}
		for _, c := range rec["candidates"].([]any) {
			if c.(map[string]any)["reason"] == reasonNotStrong {
				t.Fatalf("nothing is dropped not_strong: %v", c)
			}
		}
		got := fmt.Sprintf("%s@%s", sel.chosen.seat.Canon, sel.chosen.row.Effort)
		if base == "" {
			base = got
		}
		if got != base || got != "agy-flash38-high@high" {
			t.Fatalf("grade A non-strong keeps the waste-risk agy rung: %s (first %s)", got, base)
		}
	}
}

func TestPlanStrongShowsRestrictedCandidates(t *testing.T) {
	run := func(args ...string) map[string]any {
		t.Helper()
		a, q, _, _, out := planFixture(t)
		q.CatalogRows = append(q.CatalogRows, CatalogRow{Profile: "opus", Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "opus-m"})
		gate := q.GateDo
		q.GateDo = func(r QuotaRequest) (string, error) {
			if firstNonempty(r.GateProfile, r.Profile) == "opus" {
				r.Profile, r.GateProfile = "sonnet", "sonnet" // same claude pool
			}
			return gate(r)
		}
		serveRouting(t, routingDoc(strings.Replace(routingBlockV1, `"panewire_role_map"`, `"strong_profiles": ["opus"], "panewire_role_map"`, 1)))
		if e := a.Run(context.Background(), append([]string{"plan", "--grade", "A", "--t", "T1", "--json"}, args...)); e != nil {
			t.Fatalf("plan %v: %v\n%s", args, e, out)
		}
		var plan map[string]any
		if e := json.Unmarshal(out.Bytes(), &plan); e != nil {
			t.Fatal(e)
		}
		return plan["selection"].(map[string]any)
	}
	reasons := func(sel map[string]any) []string {
		var r []string
		for _, c := range sel["candidates"].([]any) {
			if why, _ := c.(map[string]any)["reason"].(string); why == reasonNotStrong {
				r = append(r, c.(map[string]any)["profile"].(string))
			}
		}
		slices.Sort(r)
		return r
	}
	strong := run("--strong")
	if got := reasons(strong); !reflect.DeepEqual(got, []string{"devin-swe2", "sonnet"}) {
		t.Fatalf("plan --strong lists the dropped rungs as not_strong: %v", got)
	}
	if strong["strong"] != true || strong["chosen"].(map[string]any)["canon"] != "opus" {
		t.Fatalf("plan --strong chooses from the list: %v", strong["chosen"])
	}
	plain := run()
	if got := reasons(plain); len(got) != 0 || plain["strong"] != nil || plain["chosen"].(map[string]any)["canon"] == "opus" {
		t.Fatalf("plan without --strong is unrestricted: %v %v", got, plain["chosen"])
	}
}

// The hub re-runs spawn from its args: --strong must survive both transports,
// or a strong spawn would be placed unrestricted on the far side.
func TestStrongFlagCrossesHubArgs(t *testing.T) {
	s, err := parseSpawn([]string{"-c", "/r", "-p", "/b", "-w", "w", "-l", "l", "--t", "T1", "--owner", "o", "--strong"})
	if err != nil || !s.Strong {
		t.Fatalf("spawn --strong: %v %+v", err, s.Strong)
	}
	if args, e := hubArgs("/r", s); e != nil || !slices.Contains(args, "--strong") {
		t.Fatalf("hubArgs dropped --strong: %v %v", args, e)
	}
	if !slices.Contains(submissionArgs(s), "--strong") {
		t.Fatalf("submissionArgs dropped --strong: %v", submissionArgs(s))
	}
	if args, _ := hubArgs("/r", Spawn{}); slices.Contains(args, "--strong") {
		t.Fatalf("a non-strong spawn carries no flag: %v", args)
	}
}

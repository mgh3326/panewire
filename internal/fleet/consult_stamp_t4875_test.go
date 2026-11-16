package fleet

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

// hk 4875: the planner pins the consult seat, so selection never runs where the
// job's placement record is written. The planner carries the stamp it selected
// with, and that record states the same consult object an auto-selected
// consult's does.

// plannedConsultStamp is the stamp the consult floor writes for an A asker in
// anthropic's family: one rung up (A+), another family, no fallback.
var plannedConsultStamp = map[string]string{
	"asker_grade": "A", "asker_family": "anthropic", "grade_source": "flag",
	"floor": "A+", "used_floor": "A+", "fallback": "",
}

func consultStampFixture(t *testing.T) (*App, ConsultPlan) {
	t.Helper()
	rows := []CatalogRow{consultFloorRung("opus", "high", "A+", "claude"), consultFloorRung("codex-sol", "high", "A+", "codex"), consultFloorRung("codex-astra", "high", "SS", "codex")}
	a, _, q := selFixture(t, "A", rows, map[string]PoolView{"claude": riskPool(30, 8), "codex": {Status: "ok", Class: "spend", Blocking: ptrF(40)}}, consultFloorBothPools)
	q.GateDo = gatePools(map[string]string{"opus": "claude", "codex-sol": "codex", "codex-astra": "codex"})
	serveRouting(t, routingDoc(routingBlockV1))
	useFakeHK(t).SetDoc("design/consult-q", "## Question\nWhy did the round-3 findings happen?")
	repo := &Repository{
		Name: "project", Origin: "https://github.com/example/project.git", LocalRoot: t.TempDir(),
		Worktree: "author-checkout",
		Checkout: &HubCheckout{Origin: "https://github.com/example/project.git", Mode: "branch", Branch: "wk/x", Sha: strings.Repeat("a", 40), Dir: "author-checkout"},
	}
	plan, err := a.PlanConsult(context.Background(), ConsultPlanInput{
		Task: "42", Question: "hk:doc/design/consult-q", AuthorFamily: "anthropic",
		Requester: "mergewake/example/project#7", Owner: "operator-desk", Repository: repo,
	})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	return a, plan
}

func assertPlannedStamp(t *testing.T, kind string, stamp map[string]string) {
	t.Helper()
	if len(stamp) != len(plannedConsultStamp) {
		t.Fatalf("%s stamp = %v, want %v", kind, stamp, plannedConsultStamp)
	}
	for key, want := range plannedConsultStamp {
		if got := stamp[key]; got != want {
			t.Fatalf("%s stamp %s = %q, want %q (stamp %v)", kind, key, got, want, stamp)
		}
	}
}

// TestPlannedConsultCarriesSelectionStamp_t4875: the planner's real output
// carries the stamp its own seat choice wrote, the hub binds that stamp to the
// spawn it dispatches, and the record writer states it in the consult object.
func TestPlannedConsultCarriesSelectionStamp_t4875(t *testing.T) {
	a, plan := consultStampFixture(t)
	at := indexArg(plan.Request.Args, ConsultStampArg)
	if at < 0 {
		t.Fatalf("the planned consult carries no %s: %v", ConsultStampArg, plan.Request.Args)
	}
	stamp, err := parseConsultStamp(plan.Request.Args[at+1])
	if err != nil {
		t.Fatalf("stamp: %v", err)
	}
	assertPlannedStamp(t, "planned", stamp)

	s, cleanup, err := a.hubSpawn(plan.Request)
	if err != nil {
		t.Fatalf("hub binds the planned consult: %v", err)
	}
	defer cleanup()
	assertPlannedStamp(t, "hub-bound", s.ConsultStamp)

	// attachSelection is what writes the job.placement record (spawn.go) and the
	// hub's placement trace (placement.go) from the spawn.
	rec := map[string]any{"source": "forced", "profile": s.Profile, "host": "local"}
	a.attachSelection(rec, s)
	selection, _ := rec["selection"].(map[string]any)
	consult, _ := selection["consult"].(map[string]any)
	if len(consult) != len(plannedConsultStamp) {
		t.Fatalf("the placement record carries no consult stamp: %v", selection)
	}
	for key, want := range plannedConsultStamp {
		if got := consult[key]; got != want {
			t.Fatalf("recorded %s = %v, want %v", key, got, want)
		}
	}
}

// TestConsultStampRoundTrip_t4875: a consult request's hyphenated family and a
// long policy family round-trip, an asker without a task grade carries an empty
// asker_grade, and the encoder never emits a value its reader refuses.
func TestConsultStampRoundTrip_t4875(t *testing.T) {
	for _, family := range []string{"unknown", "acme-consult", "acme.consult.v2", strings.Repeat("family", 8), strings.Repeat("<", 64), strings.Repeat("&", 64), strings.Repeat(`"`, 32)} {
		stamp := map[string]string{"asker_grade": "", "asker_family": family, "grade_source": "without_task", "floor": "S+", "used_floor": "S+", "fallback": ""}
		got, err := parseConsultStamp(consultStampValue(stamp))
		if err != nil {
			t.Fatalf("family %q: %v", family, err)
		}
		for key, want := range stamp {
			if got[key] != want {
				t.Fatalf("family %q: %s = %q, want %q", family, key, got[key], want)
			}
		}
	}
	// raw encodes without the encoder's own check, so the reader's rule is
	// tested on values it must refuse even though no writer would make them.
	raw := func(stamp map[string]string) string {
		encoded, err := json.Marshal(stamp)
		if err != nil {
			t.Fatal(err)
		}
		return base64.RawURLEncoding.EncodeToString(encoded)
	}
	base := func() map[string]string {
		return map[string]string{"asker_grade": "A", "asker_family": "anthropic", "grade_source": "flag", "floor": "A+", "used_floor": "A+", "fallback": ""}
	}
	bad := map[string]map[string]string{}
	unencodable := func(name string, edit func(map[string]string)) {
		stamp := base()
		edit(stamp)
		bad[name] = stamp
	}
	unencodable("empty family", func(s map[string]string) { s["asker_family"] = "" })
	unencodable("control char", func(s map[string]string) { s["asker_family"] = "anthropic\n" })
	unencodable("tab", func(s map[string]string) { s["asker_family"] = "anthropic\t" })
	unencodable("c1 control", func(s map[string]string) { s["asker_family"] = "acme\u0080" })
	unencodable("value too long", func(s map[string]string) { s["asker_family"] = strings.Repeat("f", consultStampValueMax+1) })
	unencodable("unknown key", func(s map[string]string) { s["extra"] = "x" })
	for name, stamp := range bad {
		if value := consultStampValue(stamp); value != "" {
			t.Fatalf("%s: encoder emitted %q, want no stamp", name, value)
		}
		if _, err := parseConsultStamp(raw(stamp)); err == nil {
			t.Fatalf("%s: reader accepted %q", name, raw(stamp))
		}
	}
	// encoding/json replaces invalid UTF-8 with U+FFFD, so an encoded bad input
	// still reads back; only the encoder can refuse it (hk 4875 review).
	invalid := base()
	invalid["asker_family"] = "acme\xff"
	if value := consultStampValue(invalid); value != "" {
		t.Fatalf("invalid UTF-8: encoder emitted %q, want no stamp", value)
	}
	for name, value := range map[string]string{
		"empty value":   "",
		"not base64url": "eyJh*b",
		"not an object": "W10",
		"missing key":   raw(map[string]string{"asker_grade": "A"}),
	} {
		if _, err := parseConsultStamp(value); err == nil {
			t.Fatalf("%s: reader accepted %q", name, value)
		}
	}
}

// TestManualConsultStampUnchanged_t4875: a spawn with no planned stamp writes
// the selection it always wrote; the carry never invents a consult object.
func TestManualConsultStampUnchanged_t4875(t *testing.T) {
	sel, err := consultFloorSpawnSelect(t, []CatalogRow{consultFloorRung("opus", "high", "A+", "claude"), consultFloorRung("codex-sol", "high", "A+", "codex")}, consultFloorBothPools, "A", "anthropic", "operator-desk")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	// The auto-selected consult's own record is the baseline this change leaves
	// alone; the same record is what attachSelection writes for it.
	want := consultFloorStamp(t, sel)
	s := Spawn{Role: "consult", Profile: sel.chosen.seatName}
	s.Sel = sel.record()
	a := testApp(t)
	rec := map[string]any{}
	a.attachSelection(rec, s)
	got, _ := rec["selection"].(map[string]any)["consult"].(map[string]any)
	if len(got) != len(want) {
		t.Fatalf("auto-selected consult stamp = %v, want %v", got, want)
	}
	for key, value := range want {
		if got[key] != value {
			t.Fatalf("auto-selected consult %s = %v, want %v", key, got[key], value)
		}
	}
}

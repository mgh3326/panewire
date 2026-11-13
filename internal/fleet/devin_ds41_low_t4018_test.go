package fleet

import (
	"context"
	"reflect"
	"testing"
)

// hk 4018: devin-ds41-low seat. Shaped like devin-ds41-max, draws on the devin pool,
// model id deepseek-v4-1-flash-1017-low, unrated until routing-policy grades it.
const (
	ds41LowProfile = "devin-ds41-low"
	ds41LowModelID = "deepseek-v4-1-flash-1017-low"
	ds41LowFamily  = "deepseek"
)

func ds41LowRow() CatalogRow {
	return CatalogRow{
		Profile: ds41LowProfile,
		Effort:  "high",
		ModelID: ds41LowModelID,
		Pool:    "devin",
		Grade:   gradeUnrated,
		Gate:    defaultGate,
	}
}

// (a) With an unrated high-effort row for devin-ds41-low (pool devin, the chosen model id),
// a pinned worker spawn with PANEWIRE_E6_ARM=devin-ds41-low@high resolves to that
// model and the devin launch argv that devin-ds41-max gets.
func TestDevinDS41LowPinnedSpawnResolvesLikeDS41Max(t *testing.T) {
	a := testApp(t)
	wantArgv := func(model string) []string {
		return []string{"--model", model, "--permission-mode", "dangerous", "--respect-workspace-trust", "false"}
	}
	dsSeat := a.Policy.Profiles["devin-ds41-max"]
	dsArgv, _, err := a.launchArgs(dsSeat, Launch{ModelID: "deepseek-v4-1-flash-max"})
	if err != nil || !reflect.DeepEqual(dsArgv, wantArgv("deepseek-v4-1-flash-max")) {
		t.Fatalf("devin-ds41-max argv %v err %v", dsArgv, err)
	}

	seat, ok := a.Policy.Profiles[ds41LowProfile]
	if !ok || seat.Kind != "devin" || seat.Family != ds41LowFamily || seat.Capability != dsSeat.Capability || !reflect.DeepEqual(seat.Roles, dsSeat.Roles) {
		t.Fatalf("policy seat for %s: %+v", ds41LowProfile, seat)
	}

	rows := []CatalogRow{ds41LowRow()}
	catalog := overlayLaunchCatalog(rows, false)
	req := admissionRequest{Profile: ds41LowProfile, Effort: "high", Role: "implementer", E6Arm: ds41LowProfile + "@high"}
	resolved, effort, why := catalog.resolveRow(req, true, a.Policy.Profiles)
	if why != "" || resolved.ModelID != ds41LowModelID || resolved.Pool != "devin" || effort != "high" {
		t.Fatalf("resolved row=%+v effort=%q why=%q", resolved, effort, why)
	}

	in := admissionInputs{Rows: admissionRows(rows), Seats: a.Policy.Profiles}
	if _, _, _, denial := in.resolveRow(req, true); denial != nil {
		t.Fatalf("admission refused the pinned arm: %+v", denial)
	}

	argv, env, err := a.launchArgs(seat, Launch{ModelID: resolved.ModelID, Effort: effort})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(argv, wantArgv(ds41LowModelID)) || env != nil {
		t.Fatalf("launch args %v env %v", argv, env)
	}
}

// (a, control) The pin is the only way in: for a graded task the unrated seat
// is refused without PANEWIRE_E6_ARM and admitted with its own marker.
func TestDevinDS41LowRequiresTheArm(t *testing.T) {
	a := testApp(t)
	seat, ok := a.Policy.Profiles[ds41LowProfile]
	if !ok {
		t.Fatalf("policy seat for %s does not exist", ds41LowProfile)
	}
	l := Launch{Profile: ds41LowProfile, ModelID: ds41LowModelID, Pool: "devin", Grade: gradeUnrated, Gate: defaultGate}
	if err := seat.Check(l, "implementer", "A", "", "", nil); err == nil {
		t.Fatalf("%s admitted a graded task without PANEWIRE_E6_ARM", ds41LowProfile)
	}
	if err := seat.Check(l, "implementer", "A", "", "some-other-seat@", nil); err == nil {
		t.Fatalf("%s admitted a graded task on another seat's marker", ds41LowProfile)
	}
	if err := seat.Check(l, "implementer", "A", "", ds41LowProfile+"@", nil); err != nil {
		t.Fatalf("%s refused its own marker: %v", ds41LowProfile, err)
	}
}

// (b) A graded -m auto spawn does not pick the unrated seat; the graded
// claude rung takes the task.
func TestDevinDS41LowNotAutoSelectedWhileUnrated(t *testing.T) {
	a0 := testApp(t)
	if seat, ok := a0.Policy.Profiles[ds41LowProfile]; !ok || seat.Kind != "devin" {
		t.Fatalf("policy has no devin seat %s; the auto check would prove nothing", ds41LowProfile)
	}
	rows := []CatalogRow{ds41LowRow(), ompRow("sonnet", "claude-sonnet-5-5", "claude")}
	pools := map[string]PoolView{
		"claude": {Status: "ok", Class: "spend", Blocking: ptrF(0)},
		"devin":  {Status: "ok", Class: "spend", Blocking: ptrF(0)},
	}
	a, s, q := selFixture(t, "A", rows, pools, []string{"claude", "devin"})
	q.GateDo = gatePools(map[string]string{"sonnet": "claude", ds41LowProfile: "devin"})
	serveOMPRouting(t, a)
	s.Profile = "auto"
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if chosen := sel.record()["chosen"].(map[string]any)["canon"]; chosen != "sonnet" {
		t.Fatalf("auto selected %v, want sonnet", chosen)
	}
}

// (c) Existing devin seats keep their kind, family, capability, roles and
// launch argv (preservation test).
func TestDevinDS41LowExistingDevinSeatsUnchanged(t *testing.T) {
	a := testApp(t)
	for _, tc := range []struct{ profile, model, family string }{
		{"devin-ds41", "deepseek-v4-1-flash-high", "deepseek"},
		{"devin-ds41-max", "deepseek-v4-1-flash-max", "deepseek"},
		{"devin-argon4-high", "gemini-4-argon-high", "google"},
		{"devin-glm53-high", "glm-5-3-high", "other"},
		{"devin-swe2-flash-max", "swe-2-flash-max", "cognition"},
		{"devin-fusion-opus55", "fusion-claude-opus-5-5-high-sidekick-swe-2-medium", "anthropic"},
	} {
		seat, ok := a.Policy.Profiles[tc.profile]
		if !ok || seat.Kind != "devin" || seat.Family != tc.family || seat.Capability != "standard" || !reflect.DeepEqual(seat.Roles, []string{"implementer", "reviewer", "consult"}) {
			t.Fatalf("existing devin seat %s changed: %+v", tc.profile, seat)
		}
		argv, _, err := a.launchArgs(seat, Launch{ModelID: tc.model})
		if err != nil || !reflect.DeepEqual(argv, []string{"--model", tc.model, "--permission-mode", "dangerous", "--respect-workspace-trust", "false"}) {
			t.Fatalf("existing %s argv %v err %v", tc.profile, argv, err)
		}
	}
}

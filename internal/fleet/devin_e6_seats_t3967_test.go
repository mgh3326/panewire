package fleet

import (
	"context"
	"reflect"
	"testing"
)

// hk 3967: experiment 6 devin arms. Each seat is shaped like devin-ds41-max,
// draws on the devin pool, and its catalog row is unrated until routing-policy
// grades it. The rows below are the ones the desk publishes after deploy.
var devinE6Seats = map[string]struct{ model, family string }{
	"devin-argon4-high": {"gemini-4-argon-high", "google"},
	"devin-glm53-high":  {"glm-5-3-high", "other"},
}

func devinE6Rows() []CatalogRow {
	var rows []CatalogRow
	for profile, want := range devinE6Seats {
		rows = append(rows, CatalogRow{Profile: profile, Effort: "high", ModelID: want.model, Pool: "devin", Grade: gradeUnrated, Gate: defaultGate})
	}
	return rows
}

// (a) A pinned worker spawn with the E6 arm resolves each new seat to its
// model id and to the devin launch argv that devin-ds41-max gets.
func TestDevinE6SeatsPinnedSpawnResolvesLikeDS41Max(t *testing.T) {
	a := testApp(t)
	wantArgv := func(model string) []string {
		return []string{"--model", model, "--permission-mode", "dangerous", "--respect-workspace-trust", "false"}
	}
	dsSeat := a.Policy.Profiles["devin-ds41-max"]
	dsArgv, _, err := a.launchArgs(dsSeat, Launch{ModelID: "deepseek-v4-1-flash-max"})
	if err != nil || !reflect.DeepEqual(dsArgv, wantArgv("deepseek-v4-1-flash-max")) {
		t.Fatalf("devin-ds41-max argv %v err %v", dsArgv, err)
	}
	for profile, want := range devinE6Seats {
		t.Run(profile, func(t *testing.T) {
			seat, ok := a.Policy.Profiles[profile]
			if !ok || seat.Kind != "devin" || seat.Family != want.family || seat.Capability != dsSeat.Capability || !reflect.DeepEqual(seat.Roles, dsSeat.Roles) {
				t.Fatalf("policy seat for %s: %+v", profile, seat)
			}
			var found []CatalogRow
			for _, row := range devinE6Rows() {
				if row.Profile == profile {
					found = append(found, row)
				}
			}
			if len(found) != 1 {
				t.Fatalf("catalog rows for %s: %+v", profile, found)
			}
			catalog := overlayLaunchCatalog(found, false)
			req := admissionRequest{Profile: profile, Effort: "high", Role: "implementer", E6Arm: profile + "@high"}
			resolved, effort, why := catalog.resolveRow(req, true, a.Policy.Profiles)
			if why != "" || resolved.ModelID != want.model || resolved.Pool != "devin" || effort != "high" {
				t.Fatalf("resolved row=%+v effort=%q why=%q", resolved, effort, why)
			}
			in := admissionInputs{Rows: admissionRows(found), Seats: a.Policy.Profiles}
			if _, _, _, denial := in.resolveRow(req, true); denial != nil {
				t.Fatalf("admission refused the pinned arm: %+v", denial)
			}
			argv, env, err := a.launchArgs(seat, Launch{ModelID: resolved.ModelID, Effort: effort})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(argv, wantArgv(want.model)) || env != nil {
				t.Fatalf("launch args %v env %v", argv, env)
			}
		})
	}
}

// (a, control) The pin is the only way in: for a graded task the unrated seat
// is refused without PANEWIRE_E6_ARM and admitted with its own marker.
func TestDevinE6SeatsRequireTheArm(t *testing.T) {
	a := testApp(t)
	for profile, want := range devinE6Seats {
		t.Run(profile, func(t *testing.T) {
			seat := a.Policy.Profiles[profile]
			l := Launch{Profile: profile, ModelID: want.model, Pool: "devin", Grade: gradeUnrated, Gate: defaultGate}
			if err := seat.Check(l, "implementer", "A", "", "", nil); err == nil {
				t.Fatalf("%s admitted a graded task without PANEWIRE_E6_ARM", profile)
			}
			if err := seat.Check(l, "implementer", "A", "", "some-other-seat@", nil); err == nil {
				t.Fatalf("%s admitted a graded task on another seat's marker", profile)
			}
			if err := seat.Check(l, "implementer", "A", "", profile+"@", nil); err != nil {
				t.Fatalf("%s refused its own marker: %v", profile, err)
			}
		})
	}
}

// (b) A graded -m auto spawn does not pick either unrated seat; the graded
// claude rung takes the task.
func TestDevinE6SeatsNotAutoSelectedWhileUnrated(t *testing.T) {
	a0 := testApp(t)
	for profile := range devinE6Seats {
		if seat, ok := a0.Policy.Profiles[profile]; !ok || seat.Kind != "devin" {
			t.Fatalf("policy has no devin seat %s; the auto check would prove nothing", profile)
		}
	}
	rows := append(devinE6Rows(), ompRow("sonnet", "claude-sonnet-5-5", "claude"))
	pools := map[string]PoolView{
		"claude": {Status: "ok", Class: "spend", Blocking: ptrF(0)},
		"devin":  {Status: "ok", Class: "spend", Blocking: ptrF(0)},
	}
	a, s, q := selFixture(t, "A", rows, pools, []string{"claude", "devin"})
	q.GateDo = gatePools(map[string]string{"sonnet": "claude", "devin-argon4-high": "devin", "devin-glm53-high": "devin"})
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

// (c) Neither seat is a SWE-2 free rung, and the free list is unchanged.
func TestDevinE6SeatsAreNotFreeRungs(t *testing.T) {
	p, err := LoadPolicy("")
	if err != nil {
		t.Fatal(err)
	}
	pools, err := LoadPoolPolicy("")
	if err != nil {
		t.Fatal(err)
	}
	for profile := range devinE6Seats {
		if bucket, free := devinFreeRungs[profile]; free {
			t.Errorf("%s is listed as a free rung on bucket %s", profile, bucket)
		}
		if _, ok := p.Profiles[profile]; !ok {
			t.Errorf("%s has no policy seat", profile)
		}
	}
	if len(devinFreeRungs) != 6 {
		t.Errorf("devinFreeRungs has %d entries, want the six SWE-2 rungs", len(devinFreeRungs))
	}
	var devinFree []string
	for _, f := range pools.Free {
		if f.Pool == "devin" {
			devinFree = f.Models
		}
	}
	want := []string{"swe-2", "swe-2-medium", "swe-2-max", "swe-2-flash-high", "swe-2-flash-medium", "swe-2-flash-max"}
	if !reflect.DeepEqual(devinFree, want) {
		t.Fatalf("devin free list %v, want %v", devinFree, want)
	}
}

// (d) Existing devin seats keep their kind, family, capability, roles and
// launch argv.
func TestDevinE6SeatsExistingDevinSeatsUnchanged(t *testing.T) {
	a := testApp(t)
	for _, tc := range []struct{ profile, model, family string }{
		{"devin-ds41", "deepseek-v4-1-flash-high", "deepseek"},
		{"devin-ds41-max", "deepseek-v4-1-flash-max", "deepseek"},
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

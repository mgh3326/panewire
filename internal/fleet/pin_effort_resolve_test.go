package fleet

import "testing"

func TestPinnedProfileWithoutEffortResolvesSingleLiveRow(t *testing.T) {
	a := testApp(t)
	seat := a.Policy.Profiles["opus"]
	rows := []launchRow{{admissionRow: admissionRow{Profile: "opus", Effort: "high", Grade: "S", Placed: true, Pool: "claude", Gate: "default", ModelID: "claude-opus-5-5"}}}
	launch, resolved, why := (launchCatalog{Rows: rows}).resolveRow(admissionRequest{Profile: "opus", Role: "worker"}, false, a.Policy.Profiles)
	if why != "" {
		t.Fatalf("single live row was refused: %s", why)
	}
	if launch.Profile != seat.Canon || launch.Effort != "high" || resolved != "high" {
		t.Fatalf("launch = %+v resolved effort = %q; want profile %q effort high", launch, resolved, seat.Canon)
	}
}

func TestPinnedProfileWithoutEffortRefusesRunglessSingleRow(t *testing.T) {
	a := testApp(t)
	rows := []launchRow{{admissionRow: admissionRow{Profile: "opus", Effort: "", Grade: "S", Placed: true, Pool: "claude", Gate: "default", ModelID: "claude-opus-5-5"}}}
	_, _, why := (launchCatalog{Rows: rows}).resolveRow(admissionRequest{Profile: "opus", Role: "worker"}, false, a.Policy.Profiles)
	if why != "catalog row has no launch effort" {
		t.Fatalf("rungless row refusal = %q; want %q", why, "catalog row has no launch effort")
	}
}

func TestPinnedProfileWithoutEffortLaunchesLowestOrdinaryEffort(t *testing.T) {
	a := testApp(t)
	rows := []launchRow{
		{admissionRow: admissionRow{Profile: "fable", Effort: "max", Grade: "S", Placed: true, Pool: "claude", Gate: "default", ModelID: "claude-fable-5-1"}},
		{admissionRow: admissionRow{Profile: "fable", Effort: "high", Grade: "S", Placed: true, Pool: "claude", Gate: "default", ModelID: "claude-fable-5-1"}},
	}
	launch, resolved, why := (launchCatalog{Rows: rows}).resolveRow(admissionRequest{Profile: "fable", Role: "worker"}, false, a.Policy.Profiles)
	if why != "" {
		t.Fatalf("effortless multi-row profile was refused: %s", why)
	}
	if launch.Effort != "high" || resolved != "high" {
		t.Fatalf("launch effort = %q resolved = %q; want lowest ordinary effort high", launch.Effort, resolved)
	}
}

package fleet

import (
	"strings"
	"testing"
)

func TestPinnedProfileWithoutEffortResolvesSingleLiveRow(t *testing.T) {
	a := testApp(t)
	seat := a.Policy.Profiles["opus"]
	rows := []launchRow{{admissionRow: admissionRow{Profile: "opus", Effort: "high", Grade: "S", Placed: true, Pool: "claude", Gate: "default", ModelID: "claude-opus-5-5"}}}
	for _, defaults := range []map[string]string{{"opus": "high"}, {"opus": "max"}} {
		launch, resolved, why := (launchCatalog{Rows: rows, Defaults: defaults}).resolveRow(admissionRequest{Profile: "opus", Role: "worker"}, false, a.Policy.Profiles)
		if why != "" {
			t.Fatalf("single live row was refused with defaults %v: %s", defaults, why)
		}
		if launch.Profile != seat.Canon || launch.Effort != "high" || resolved != "high" {
			t.Fatalf("launch = %+v resolved effort = %q; want profile %q effort high", launch, resolved, seat.Canon)
		}
	}
}

func TestPinnedProfileWithoutEffortNamesEffortsWhenNoDefaultRow(t *testing.T) {
	a := testApp(t)
	rows := []launchRow{
		{admissionRow: admissionRow{Profile: "fable", Effort: "high", Grade: "S+", Placed: true, Pool: "claude", Gate: "default", ModelID: "claude-fable-5-1"}},
		{admissionRow: admissionRow{Profile: "fable", Effort: "max", Grade: "SS", Placed: true, Pool: "claude", Gate: "default", ModelID: "claude-fable-5-1"}},
	}
	_, _, why := (launchCatalog{Rows: rows, Defaults: map[string]string{"fable": "low"}}).resolveRow(admissionRequest{Profile: "fable", Role: "worker"}, false, a.Policy.Profiles)
	if why == "" || !strings.Contains(why, "--effort") || !strings.Contains(why, "high") || !strings.Contains(why, "max") {
		t.Fatalf("ambiguous effortless profile refusal = %q; want --effort and both live efforts", why)
	}
}

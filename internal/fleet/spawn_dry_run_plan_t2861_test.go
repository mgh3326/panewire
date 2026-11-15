package fleet

import (
	"context"
	"strings"
	"testing"
)

// A dry run that plans on this machine must say so. The host and profile it
// prints are this machine's choice, which the hub may not share.
func TestDryRunLocalPlanIsLabelled(t *testing.T) {
	a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	s.DryRun = true
	a.Quota = &fakeQuota{LaunchValue: Launch{Profile: "sonnet", Effort: "high", Grade: "A", ModelID: "sonnet-m", Gate: "default", Pool: "claude"}, GateDo: gatePools(map[string]string{"sonnet": "claude"})}
	if _, _, e := a.placeAuto(context.Background(), s, "brief"); e != nil {
		t.Fatal(e)
	}
	if len(hub.spawns) != 0 {
		t.Fatalf("dry run must not spawn: %v", hub.spawns)
	}
	out := a.Out.(interface{ String() string }).String()
	if !strings.Contains(out, "local plan") {
		t.Fatalf("local dry run must carry the local plan label: %q", out)
	}
}

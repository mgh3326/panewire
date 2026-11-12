package fleet

import (
	"context"
	"strings"
	"testing"
)

// A dry run previews a spawn and starts nothing, so local spawns being
// disabled must not refuse it (the preview ran before that gate until hk 3973).
func TestSpawnDryRunAllowedWhenLocalSpawnDisabled_t3973(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	a.Config.Local["spawn"] = false
	s.DryRun = true
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatalf("dry run refused on a host with local spawns disabled: %v", err)
	}
	out := a.Out.(interface{ String() string }).String()
	if !strings.Contains(out, "dry-run host=") {
		t.Fatalf("dry-run printed no preview: %q", out)
	}
	for _, c := range f.Calls {
		if strings.HasPrefix(strings.Join(c.Args, " "), "tab create") {
			t.Fatalf("dry run created a pane: %v", c.Args)
		}
	}
}

func TestSpawnDryRunRequiresEscalationOptIn(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	s.Profile, s.DryRun, s.OverrideReason = "agy-sonnet46", true, "fixture"
	q := a.Quota.(*fakeQuota)
	q.LaunchValue = Launch{Profile: "agy-sonnet46", ModelID: "claude-sonnet-4-6", Grade: "B", Pool: "agy", Gate: escalationGate}
	q.GateDo = gatePools(map[string]string{"agy-sonnet46": "agy"})

	err := a.Spawn(context.Background(), s)
	if err == nil || exitCode(err) != exitSeatRefused || !strings.Contains(err.Error(), "agy-sonnet46 is a escalation rung; pass --allow-escalation") {
		t.Fatalf("dry-run without escalation opt-in must match node refusal: %v", err)
	}

	a, s, _, _ = cmdFixture(t)
	s.Profile, s.DryRun, s.OverrideReason, s.AllowEscalation = "agy-sonnet46", true, "fixture", true
	q = a.Quota.(*fakeQuota)
	q.LaunchValue = Launch{Profile: "agy-sonnet46", ModelID: "claude-sonnet-4-6", Grade: "B", Pool: "agy", Gate: escalationGate}
	q.GateDo = gatePools(map[string]string{"agy-sonnet46": "agy"})
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatalf("dry-run with escalation opt-in was refused: %v", err)
	}
	if out := a.Out.(interface{ String() string }).String(); !strings.Contains(out, "dry-run host=") {
		t.Fatalf("admitted dry-run did not print its preview: %q", out)
	}
}

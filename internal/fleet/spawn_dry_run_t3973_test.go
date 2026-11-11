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

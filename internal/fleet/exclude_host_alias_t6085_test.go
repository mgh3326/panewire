package fleet

import (
	"context"
	"strings"
	"testing"
)

// hk 6085: --exclude-host names a host by an alias (hosts.<machine>.aliases)
// the same way --host does. A stored or re-placed request must keep its
// exclusion on the canonical hosts key, or the alias never matches and the
// excluded host is placed.
func TestExcludeHostAliasIsNeverPlaced_t6085(t *testing.T) {
	a := placementApp(t, &fakePlacementSource{})
	a.Config.Local = map[string]any{"machine_id": "home"}
	a.Config.Hosts = map[string]map[string]any{
		"machine-a": {"aliases": []any{"desktop"}},
		"desk-2":    {},
	}
	a.Config.HostOrder = []string{"machine-a", "desk-2"}
	p, e := a.plan(context.Background(), Spawn{ExcludeHosts: "home,desktop"}, "", 0)
	if e != nil {
		t.Fatal(e)
	}
	if got := strings.Join(orderNames(p), ","); got != "desk-2" {
		t.Fatalf("--exclude-host desktop names machine-a by its alias; it must not be ordered: got %q", got)
	}
	for _, c := range p.candidates {
		if c.name == "machine-a" && c.outcome != "excluded_host" {
			t.Fatalf("the aliased host must record the exclusion on its candidate: %+v", c)
		}
	}
}

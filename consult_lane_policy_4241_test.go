package panewire

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

// The consult floor's design-lane classifier follows the loaded relay policy
// (hk 4241): a custom list, an explicit empty list, and no policy at all.
func TestFleetDesignLaneClassifierFollowsPolicyList(t *testing.T) {
	custom := writeConsultPolicy(t, "default: interrupt\nrelay:\n  design_lanes: [\"routing-policy\"]\n  desk_lanes: [\"operator-desk\"]\n")
	isDesign := fleetDesignLaneClassifier(custom)
	if isDesign == nil {
		t.Fatal("custom policy loaded no classifier")
	}
	if !isDesign("routing-policy") || isDesign("panewire-design") || isDesign("operator-desk") {
		t.Fatalf("custom list classifier: routing=%t default=%t desk=%t", isDesign("routing-policy"), isDesign("panewire-design"), isDesign("operator-desk"))
	}

	empty := writeConsultPolicy(t, "default: interrupt\nrelay:\n  design_lanes: []\n")
	none := fleetDesignLaneClassifier(empty)
	if none == nil {
		t.Fatal("explicit empty list loaded no classifier")
	}
	if none("panewire-design") || none("routing-policy") {
		t.Fatal("explicit empty design_lanes must name no design lane")
	}
}

// Without a policy the classifier is nil, so the fleet app keeps the built-in
// list: "off" (the hub's kill switch) and a policy file that cannot be read.
func TestFleetDesignLaneClassifierAbsentPolicyKeepsBuiltIn(t *testing.T) {
	if fleetDesignLaneClassifier("off") != nil {
		t.Fatal(`"off" must load no classifier`)
	}
	missing := filepath.Join(t.TempDir(), "absent.yaml")
	if fleetDesignLaneClassifier(missing) != nil {
		t.Fatal("an unreadable policy must load no classifier")
	}
}

// The hub's spawn app classifies with the hub's loaded policy, not the
// built-in list.
func TestHubSpawnAppUsesLoadedDesignLanePolicy(t *testing.T) {
	policy := quietDesignPolicyOf(t, "default: interrupt\nrelay:\n  design_lanes: [\"custom-design\"]\n  desk_lanes: [\"operator-desk\"]\n")
	h := &HubServer{
		inbox: &hubInbox{policy: policy},
		spawnNewApp: func(out, errout io.Writer) (*fleet.App, error) {
			return &fleet.App{Out: out, Err: errout}, nil
		},
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("new spawn app: %v", err)
	}
	if app.IsDesignLane == nil || !app.IsDesignLane("custom-design") || app.IsDesignLane("panewire-design") || app.IsDesignLane("operator-desk") {
		t.Fatal("spawn app classifier does not match the loaded policy")
	}
}

// A hub with no inbox has no loaded policy, so its spawn app keeps the
// built-in list (nil classifier).
func TestHubSpawnAppWithoutPolicyKeepsBuiltIn(t *testing.T) {
	h := &HubServer{
		spawnNewApp: func(out, errout io.Writer) (*fleet.App, error) {
			return &fleet.App{Out: out, Err: errout}, nil
		},
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("new spawn app: %v", err)
	}
	if app.IsDesignLane != nil {
		t.Fatal("a hub without a loaded policy must leave the classifier nil")
	}
}

func writeConsultPolicy(t *testing.T, raw string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "delivery-policy.yaml")
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

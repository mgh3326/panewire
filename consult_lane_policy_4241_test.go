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

// Under the default policy (embedded, or relay.design_lanes absent) the consult
// floor keeps today's design lanes: routing-policy and fleet-policy stay SS
// (hk 4241). Mutant-checked: classifying by the policy's own *-design list fails it.
func TestConsultDesignLanesDefaultPolicyKeepsBuiltInList(t *testing.T) {
	embedded, err := loadDeliveryPolicy("default")
	if err != nil {
		t.Fatal(err)
	}
	policies := map[string]*deliveryPolicy{
		"embedded default":    embedded,
		"absent design_lanes": quietDesignPolicyOf(t, "default: interrupt\n"),
	}
	for name, p := range policies {
		isDesign := consultDesignLanes(p)
		if isDesign == nil {
			t.Fatalf("%s: no classifier loaded", name)
		}
		for _, lane := range []string{"panewire-design", "routing-policy", "fleet-policy"} {
			if !isDesign(lane) {
				t.Fatalf("%s: %s is not a design-session lane, so it loses the SS floor", name, lane)
			}
		}
		if isDesign("operator-desk") || isDesign("work-operator-desk") {
			t.Fatalf("%s: a desk lane classified as design", name)
		}
	}
}

// The hub's spawn app under the default policy classifies the built-in lanes.
func TestHubSpawnAppDefaultPolicyKeepsBuiltInList(t *testing.T) {
	embedded, err := loadDeliveryPolicy("default")
	if err != nil {
		t.Fatal(err)
	}
	h := &HubServer{
		inbox: &hubInbox{policy: embedded},
		spawnNewApp: func(out, errout io.Writer) (*fleet.App, error) {
			return &fleet.App{Out: out, Err: errout}, nil
		},
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("new spawn app: %v", err)
	}
	if app.IsDesignLane == nil || !app.IsDesignLane("routing-policy") || !app.IsDesignLane("fleet-policy") {
		t.Fatal("hub spawn app under the default policy must keep routing-policy and fleet-policy as design lanes")
	}
}

// escalate-author picks its consult seat from the job's owner_lane through the
// same classifier as spawn and resolve (mergewake finding on cli.go, hk 4241).
// policyAwareFleetCommand is new on this head, so the test cannot build on the
// previous head; removing "escalate-author" from it fails this test.
func TestPolicyAwareFleetCommandsIncludeEscalateAuthor(t *testing.T) {
	for _, c := range []string{"spawn", "work", "delegate", "resolve", "escalate-author"} {
		if !policyAwareFleetCommand(c) {
			t.Fatalf("%s must receive the loaded relay policy classifier", c)
		}
	}
}

// A lane the loaded policy names in relay.desk_lanes is never a design lane,
// even under the default *-design list that keeps the built-in lanes (hk 4241;
// mergewake finding on consult_design_lanes.go). Mutant-checked: dropping the
// desk check classifies routing-policy as design and fails this test.
func TestConsultDesignLanesDefaultPatternHonoursDeskLanes(t *testing.T) {
	for name, raw := range map[string]string{
		"explicit *-design":   "default: interrupt\nrelay:\n  design_lanes: [\"*-design\"]\n  desk_lanes: [\"routing-policy\"]\n",
		"absent design_lanes": "default: interrupt\nrelay:\n  desk_lanes: [\"routing-policy\"]\n",
	} {
		isDesign := consultDesignLanes(quietDesignPolicyOf(t, raw))
		if isDesign == nil {
			t.Fatalf("%s: no classifier loaded", name)
		}
		if isDesign("routing-policy") {
			t.Fatalf("%s: routing-policy is a desk lane and must not be a design lane", name)
		}
		if !isDesign("panewire-design") || !isDesign("fleet-policy") {
			t.Fatalf("%s: built-in design lanes lost outside the desk list", name)
		}
	}
}

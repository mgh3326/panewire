package panewire

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A machine that owns its whole lane row cap cannot register the lane row a new
// pane needs: the next lane write is refused `lane_quota_exceeded`, so a spawn
// placed there comes up LANE_UNREGISTERED (hk 4658). Placement must not choose
// such a machine, and when every machine is capped the decision is no
// placement at all rather than a fallback that spends a pane and loses it.
func TestPlacementTreatsLaneCappedMachineAsNotEligible(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, `{"lanes":{"rev-a1":{"machine":"machine-a","pane":"w1:p1"},"rev-a2":{"machine":"machine-a","pane":"w1:p2"}}}`)
	hub := lanesNodeHub(t, path, func(config *HubServerConfig) { config.LanesNodeRowCap = 2 })
	hub.connect("machine-a", "fixture", "fixture", &hubAgent{}, true)
	hub.connect("machine-b", "fixture", "fixture", &hubAgent{}, true)
	policy := memoryPlacementPolicy("machine-a", "machine-b")

	result := hub.makePlacement(policy, placementMetrics{}, "hub-only", time.Now())
	if result.Decision != "machine-b" {
		t.Fatalf("decision %q, want the uncapped spill target: %+v", result.Decision, result)
	}
	capped := memoryCandidate(t, result, "machine-a")
	if !strings.Contains(capped.Reason, "lanes_full") {
		t.Fatalf("capped machine reason %q lacks lanes_full", capped.Reason)
	}
	if placementUsable(capped, policy, "hub-only") {
		t.Fatal("a machine at its lane cap must not be usable")
	}

	lanesProjectionWrite(t, path, `{"lanes":{"rev-a1":{"machine":"machine-a","pane":"w1:p1"},"rev-a2":{"machine":"machine-a","pane":"w1:p2"},"rev-b1":{"machine":"machine-b","pane":"w9:p1"},"rev-b2":{"machine":"machine-b","pane":"w9:p2"}}}`)
	result = hub.makePlacement(policy, placementMetrics{}, "hub-only", time.Now())
	if result.Decision != "unavailable" || result.Reason != "lanes_full" {
		t.Fatalf("decision %q reason %q, want unavailable/lanes_full: %+v", result.Decision, result.Reason, result)
	}
}

// A decided placement whose lane row does not exist yet counts against the cap,
// so two decisions made before either row lands cannot both take the last slot.
// A pending placement whose row already exists is not counted twice.
func TestPlacementCountsAPendingPlacementAgainstTheLaneCap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, `{"lanes":{"rev-a1":{"machine":"machine-a","pane":"w1:p1"}}}`)
	hub := lanesNodeHub(t, path, func(config *HubServerConfig) { config.LanesNodeRowCap = 2 })
	hub.connect("machine-a", "fixture", "fixture", &hubAgent{}, true)
	policy := memoryPlacementPolicy("machine-a")

	if result := hub.makePlacement(policy, placementMetrics{}, "hub-only", time.Now()); result.Decision != "machine-a" {
		t.Fatalf("decision %q, want the host with one free row", result.Decision)
	}
	hub.countPlacement("pending-job", "machine-a", "pending-lane")
	result := hub.makePlacement(policy, placementMetrics{}, "hub-only", time.Now())
	if result.Decision != "unavailable" || result.Reason != "lanes_full" {
		t.Fatalf("decision %q reason %q, want the pending placement to fill the cap", result.Decision, result.Reason)
	}
	// The same decision, now for the label whose row already exists: no second
	// count, so the free row is still free.
	hub.countPlacement("pending-job", "machine-a", "rev-a1")
	if result := hub.makePlacement(policy, placementMetrics{}, "hub-only", time.Now()); result.Decision != "machine-a" {
		t.Fatalf("decision %q, want the existing row not to be counted twice", result.Decision)
	}
}

// The lane-full reason must survive a decision another rule already turned
// unavailable: a lane-capped machine with no fresh Prometheus load sample is
// still lanes_full, not a bare unavailable.
func TestPlacementKeepsLaneFullReasonWithoutLoadSample(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, `{"lanes":{"rev-a1":{"machine":"machine-a","pane":"w1:p1"},"rev-a2":{"machine":"machine-a","pane":"w1:p2"}}}`)
	hub := lanesNodeHub(t, path, func(config *HubServerConfig) { config.LanesNodeRowCap = 2 })
	hub.connect("machine-a", "fixture", "fixture", &hubAgent{}, true)
	policy := memoryPlacementPolicy("machine-a")

	result := hub.makePlacement(policy, placementMetrics{}, "prometheus", time.Now())
	if result.Decision != "unavailable" || result.Reason != "lanes_full" {
		t.Fatalf("decision %q reason %q, want unavailable/lanes_full: %+v", result.Decision, result.Reason, result)
	}
}

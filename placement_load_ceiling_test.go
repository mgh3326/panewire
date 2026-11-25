package panewire

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// loadCeilingPolicy builds a policy from JSON, so these tests also compile
// against a tree that does not yet read the ceiling key: the key is then an
// ignored config key and the rule is absent.
func loadCeilingPolicy(t *testing.T, score string) PlacementPolicy {
	t.Helper()
	policy, err := ParsePlacementPolicy([]byte(`{"local_machine":"mac-work","spill_targets":["desktop"],"max_active_jobs":50,"score":` + score + `}`))
	if err != nil {
		t.Fatalf("policy with score %s: %v", score, err)
	}
	return policy
}

// loadCeilingCandidate is a usable host with a known load1 per core. Its
// free is 0.25, above the default free floor, so only the ceiling can move it.
func loadCeilingCandidate(machine string, load, score float64) PlacementCandidate {
	reading := load
	return PlacementCandidate{Machine: machine, Score: score, Connected: true, Signals: &PlacementSignals{
		Load1PerCore: &reading,
		Free:         0.25,
		Power:        100,
		Terms:        &PlacementTerms{CPU: 1, Mem: 0.5, IO: 1, Load: 0.5, Jobs: 1},
	}}
}

// hk 6482: a host above the load ceiling is left out of the decision while
// another usable host is at or under it — the policy-order winner included,
// and a far higher score does not let it take the decision as a challenger.
func TestPlacementLoadCeilingLeavesOutHostAboveIt(t *testing.T) {
	k := loadCeilingPolicy(t, `{"load_ceiling_per_core":2.5}`).scoreK()
	usable := []PlacementCandidate{
		loadCeilingCandidate("mac-work", 3.2, 90),
		loadCeilingCandidate("desktop", 1.1, 10),
	}
	if got, reason := k.decide(usable, 0); got != 1 || reason != "load_over_ceiling" {
		t.Fatalf("decide=%d %q, want desktop load_over_ceiling", got, reason)
	}
	// The over-ceiling host's far higher score cannot take the decision from
	// a winner under the ceiling.
	usable[0].Score = 1000
	if got, reason := k.decide(usable, 1); got != 1 || reason != "" {
		t.Fatalf("decide=%d %q, want the under-ceiling winner kept", got, reason)
	}
}

// The default ceiling is on (2.5) and a configured value replaces it.
func TestPlacementLoadCeilingDefaultsAndOverride(t *testing.T) {
	usable := []PlacementCandidate{
		loadCeilingCandidate("mac-work", 3.2, 90),
		loadCeilingCandidate("desktop", 1.0, 10),
	}
	if got, reason := (PlacementPolicy{}).scoreK().decide(usable, 0); got != 1 || reason != "load_over_ceiling" {
		t.Fatalf("default ceiling: decide=%d %q, want desktop load_over_ceiling", got, reason)
	}
	if got, reason := loadCeilingPolicy(t, `{"load_ceiling_per_core":5}`).scoreK().decide(usable, 0); got != 0 || reason != "" {
		t.Fatalf("ceiling 5: decide=%d %q, want the policy-order winner kept", got, reason)
	}
}

// hk 6482: every usable host over the ceiling → nobody is left out and the
// same winner as today takes the decision.
func TestPlacementLoadCeilingLeavesEveryHostInWhenAllAreOver(t *testing.T) {
	on := loadCeilingPolicy(t, `{"load_ceiling_per_core":2.5}`).scoreK()
	off := loadCeilingPolicy(t, `{"load_ceiling_per_core":0}`).scoreK()
	usable := []PlacementCandidate{
		loadCeilingCandidate("mac-work", 3.2, 40),
		loadCeilingCandidate("desktop", 2.9, 90),
	}
	gotOn, reasonOn := on.decide(usable, 0)
	gotOff, reasonOff := off.decide(usable, 0)
	if gotOn != gotOff || reasonOn != reasonOff {
		t.Fatalf("ceiling changed an all-over decision: on=(%d %q) off=(%d %q)", gotOn, reasonOn, gotOff, reasonOff)
	}
	// Today's rule: desktop's 90 clears 1.25 × 40.
	if gotOn != 1 || reasonOn != "challenger" {
		t.Fatalf("all-over decide=%d %q, want desktop challenger", gotOn, reasonOn)
	}
}

// A host at exactly the ceiling is not over it, and a host whose load is
// unknown is neither over nor under it.
func TestPlacementLoadCeilingBoundaryAndUnknownLoad(t *testing.T) {
	k := loadCeilingPolicy(t, `{"load_ceiling_per_core":2.5}`).scoreK()
	// Exactly at the ceiling: the winner keeps the decision.
	at := []PlacementCandidate{loadCeilingCandidate("mac-work", 2.5, 90), loadCeilingCandidate("desktop", 1.0, 10)}
	if got, reason := k.decide(at, 0); got != 0 || reason != "" {
		t.Fatalf("at the ceiling: decide=%d %q, want the policy-order winner kept", got, reason)
	}
	// An unknown reading does not lift the ceiling, so an over-ceiling winner
	// is not left out for it: no host is known at or under the ceiling.
	unknown := loadCeilingCandidate("desktop", 0, 10)
	unknown.Signals.Load1PerCore = nil
	if got, reason := k.decide([]PlacementCandidate{loadCeilingCandidate("mac-work", 3.2, 90), unknown}, 0); got != 0 || reason != "" {
		t.Fatalf("unknown sibling: decide=%d %q, want the policy-order winner kept", got, reason)
	}
	// An unknown winner is not left out while another host is under it.
	unknownWinner := loadCeilingCandidate("mac-work", 0, 90)
	unknownWinner.Signals.Load1PerCore = nil
	if got, reason := k.decide([]PlacementCandidate{unknownWinner, loadCeilingCandidate("desktop", 1.0, 10)}, 0); got != 0 || reason != "" {
		t.Fatalf("unknown winner: decide=%d %q, want the policy-order winner kept", got, reason)
	}
}

// hk 6482: a ceiling of 0 turns the rule off.
func TestPlacementLoadCeilingZeroDisablesIt(t *testing.T) {
	k := loadCeilingPolicy(t, `{"load_ceiling_per_core":0}`).scoreK()
	usable := []PlacementCandidate{
		loadCeilingCandidate("mac-work", 10, 90),
		loadCeilingCandidate("desktop", 0.1, 10),
	}
	if got, reason := k.decide(usable, 0); got != 0 || reason != "" {
		t.Fatalf("ceiling 0: decide=%d %q, want the over-ceiling winner kept", got, reason)
	}
}

// hk 6482: the hub decision carries the reason onto the wire, --explain names
// it, and the decision log line names it. A hub-level all-over case keeps the
// decision it made before the rule.
func TestPlacementLoadCeilingDecisionReasonOnWireLogAndExplain(t *testing.T) {
	policy := loadCeilingPolicy(t, `{"load_ceiling_per_core":2.5}`)
	tables := map[string]map[string]string{
		placementCPUBusyQuery:      {"mac-work": "0.2", "desktop": "0.2"},
		placementCPUBusy1mQuery:    {"mac-work": "0.2", "desktop": "0.2"},
		placementMemAvailableQuery: {"mac-work": "0.8", "desktop": "0.8"},
		placementSwapInQuery:       {"mac-work": "0", "desktop": "0"},
		placementThermalQuery:      idleThermal(),
	}
	over := make(map[string]map[string]string, len(tables)+1)
	for query, values := range tables {
		over[query] = values
	}
	over[placementLoad1Query] = map[string]string{"mac-work": "3.4", "desktop": "0.6"}
	hub := measuredLoadHub(t, over, policy)
	var logs bytes.Buffer
	hub.logger = slog.New(slog.NewTextHandler(&logs, nil))
	got := hub.placement(t.Context(), "worker", "repo")
	if got.Decision != "desktop" || got.Reason != "load_over_ceiling" {
		t.Fatalf("decision=%q reason=%q, want desktop load_over_ceiling", got.Decision, got.Reason)
	}
	var explain bytes.Buffer
	renderPlacementExplain(&explain, got)
	if !strings.Contains(explain.String(), "REASON\tload_over_ceiling") {
		t.Fatalf("explain does not name the reason:\n%s", explain.String())
	}
	if !strings.Contains(logs.String(), "decision_reason=load_over_ceiling") {
		t.Fatalf("log does not name the reason: %q", logs.String())
	}

	// Every usable host over the ceiling: the policy-order winner keeps the
	// decision, as it did before the rule.
	allOver := make(map[string]map[string]string, len(tables)+1)
	for query, values := range tables {
		allOver[query] = values
	}
	allOver[placementLoad1Query] = map[string]string{"mac-work": "3.4", "desktop": "3.1"}
	hub = measuredLoadHub(t, allOver, policy)
	got = hub.placement(t.Context(), "worker", "repo")
	if got.Decision != "mac-work" || got.Reason != "" {
		t.Fatalf("all over: decision=%q reason=%q, want mac-work and no reason", got.Decision, got.Reason)
	}
}

// hk 6482: a tagged request takes the highest score among the hosts at or
// under the ceiling, and names the reason when the ceiling moved it.
func TestPlacementLoadCeilingAppliesToTaggedPlacement(t *testing.T) {
	policy := loadCeilingPolicy(t, `{"load_ceiling_per_core":2.5}`)
	policy.Power = map[string]float64{"mac-work": 500, "desktop": 100}
	policy.Tags = map[string][]string{"mac-work": {"gpu"}, "desktop": {"gpu"}}
	hub := measuredLoadHub(t, map[string]map[string]string{
		placementCPUBusyQuery:      {"mac-work": "0.2", "desktop": "0.2"},
		placementCPUBusy1mQuery:    {"mac-work": "0.2", "desktop": "0.2"},
		placementMemAvailableQuery: {"mac-work": "0.8", "desktop": "0.8"},
		placementSwapInQuery:       {"mac-work": "0", "desktop": "0"},
		placementThermalQuery:      idleThermal(),
		placementLoad1Query:        {"mac-work": "3.4", "desktop": "0.6"},
	}, policy)
	got := hub.placement(t.Context(), "worker", "repo", "gpu")
	if got.Decision != "desktop" || got.Reason != "load_over_ceiling" {
		t.Fatalf("tagged decision=%q reason=%q, want desktop load_over_ceiling (mac-work has the higher score)", got.Decision, got.Reason)
	}
}

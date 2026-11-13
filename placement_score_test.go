package panewire

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

func scoredCandidate(machine string, score, mem float64) PlacementCandidate {
	return PlacementCandidate{Machine: machine, Score: score, Signals: &PlacementSignals{MemAvailable: &mem, Terms: &PlacementTerms{CPU: 1, Mem: mem, IO: 1, Load: 1, Jobs: 1}}}
}

// Acceptance 2: the policy-order winner keeps the decision unless a challenger
// scores at least 1.25 times as much.
func TestPlacementHysteresisKeepsWinnerUnlessChallengerClearsMargin(t *testing.T) {
	k := (PlacementPolicy{}).scoreK()
	cases := []struct {
		name       string
		winner     float64
		challenger float64
		wantIndex  int
		wantReason string
	}{
		{"challenger a little ahead", 60, 74, 0, ""},
		{"challenger one point under the margin", 60, 74.99, 0, ""},
		{"challenger exactly at the margin", 60, 75, 1, "challenger"},
		{"challenger far ahead", 60, 100, 1, "challenger"},
		{"challenger behind", 60, 40, 0, ""},
		{"winner scored zero", 20, 0, 0, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			usable := []PlacementCandidate{scoredCandidate("first", tc.winner, 1), scoredCandidate("second", tc.challenger, 1)}
			got, reason := k.decide(usable, 0)
			if got != tc.wantIndex || reason != tc.wantReason {
				t.Fatalf("decide=%d %q, want %d %q", got, reason, tc.wantIndex, tc.wantReason)
			}
		})
	}
}

func TestPlacementHysteresisMarginComesFromPolicy(t *testing.T) {
	policy, err := ParsePlacementPolicy([]byte(`{"local_machine":"a","max_active_jobs":5,"score":{"hysteresis":2}}`))
	if err != nil {
		t.Fatal(err)
	}
	usable := []PlacementCandidate{scoredCandidate("a", 40, 1), scoredCandidate("b", 79, 1)}
	if got, _ := policy.scoreK().decide(usable, 0); got != 0 {
		t.Fatalf("2x margin was not honoured: %d", got)
	}
	usable[1].Score = 80
	if got, reason := policy.scoreK().decide(usable, 0); got != 1 || reason != "challenger" {
		t.Fatalf("got %d %q", got, reason)
	}
}

// freeCandidate is a usable host with a given free fraction (carried by the
// cpu term) and power, for the free-floor rules.
func freeCandidate(machine string, score, free, power float64) PlacementCandidate {
	return PlacementCandidate{Machine: machine, Score: score, Signals: &PlacementSignals{Free: free, Power: power, Terms: &PlacementTerms{CPU: free, Mem: 1, IO: 1, Load: 1, Jobs: 1}}}
}

// Rule 1 (hk 4465): a host under the free floor is not chosen while another
// usable host is at or above it.
func TestPlacementFreeFloorSkipsHostsBelowIt(t *testing.T) {
	k := (PlacementPolicy{}).scoreK()
	// The policy-order winner is under the floor with the best score: it still
	// loses to the first host at the floor.
	usable := []PlacementCandidate{freeCandidate("first", 40, .04, 100), freeCandidate("second", 5, .05, 12), freeCandidate("third", 4, .9, 30)}
	if got, reason := k.decide(usable, 0); got != 1 || reason != "below_free_floor" {
		t.Fatalf("decide=%d %q, want second (free .05 is at the floor) below_free_floor", got, reason)
	}
	// A winner at the floor keeps the decision; a below-floor rival with a far
	// higher score cannot take it.
	usable = []PlacementCandidate{freeCandidate("first", 3, .06, 12), freeCandidate("second", 90, .049, 100)}
	if got, reason := k.decide(usable, 0); got != 0 || reason != "" {
		t.Fatalf("decide=%d %q, want the winner kept", got, reason)
	}
	// Hysteresis still applies among the hosts above the floor, after the floor
	// moved the decision: second (the new winner) is kept at 1.2x, taken over
	// at 1.25x.
	usable = []PlacementCandidate{freeCandidate("first", 99, .01, 100), freeCandidate("second", 10, .5, 20), freeCandidate("third", 12, .5, 20)}
	if got, reason := k.decide(usable, 0); got != 1 || reason != "below_free_floor" {
		t.Fatalf("decide=%d %q", got, reason)
	}
	usable[2].Score = 12.5
	if got, reason := k.decide(usable, 0); got != 2 || reason != "challenger" {
		t.Fatalf("decide=%d %q", got, reason)
	}
	// A host with no terms has an unknown free, never a low one.
	usable = []PlacementCandidate{freeCandidate("first", 9, .01, 100), {Machine: "second", Score: 1}}
	if got, reason := k.decide(usable, 0); got != 1 || reason != "below_free_floor" {
		t.Fatalf("decide=%d %q", got, reason)
	}
}

// Rule 2: every usable host under the floor, the highest free*power takes it,
// whatever the scores and the memory.
func TestPlacementAllBelowFloorChoosesHighestFreeTimesPower(t *testing.T) {
	k := (PlacementPolicy{}).scoreK()
	usable := []PlacementCandidate{freeCandidate("first", 30, .04, 50), freeCandidate("second", -20, .03, 100), freeCandidate("third", 0, .0017, 30)}
	if got, reason := k.decide(usable, 0); got != 1 || reason != "all_loaded" {
		t.Fatalf("decide=%d %q, want second (3 beats 2 and 0.05)", got, reason)
	}
	// Equal free*power: the higher score, then policy order.
	usable = []PlacementCandidate{freeCandidate("first", 5, .02, 100), freeCandidate("second", 9, .04, 50), freeCandidate("third", 9, .04, 50)}
	if got, reason := k.decide(usable, 0); got != 1 || reason != "all_loaded" {
		t.Fatalf("decide=%d %q", got, reason)
	}
	usable = []PlacementCandidate{freeCandidate("first", 9, .02, 100), freeCandidate("second", 5, .04, 50)}
	if got, _ := k.decide(usable, 0); got != 0 {
		t.Fatalf("decide=%d, want the higher score at a tie", got)
	}
}

// The constants come from the policy's score object, with the defaults the
// ticket names.
func TestPlacementFreeFloorAndBridgeCostFromPolicy(t *testing.T) {
	k := (PlacementPolicy{}).scoreK()
	if k.freeFloor != .05 || k.bridgeCost != 10 {
		t.Fatalf("defaults free_floor=%v bridge_cost=%v", k.freeFloor, k.bridgeCost)
	}
	policy, err := ParsePlacementPolicy([]byte(`{"local_machine":"a","max_active_jobs":5,"score":{"free_floor":0.2,"bridge_cost":20}}`))
	if err != nil {
		t.Fatal(err)
	}
	k = policy.scoreK()
	if k.freeFloor != .2 || k.bridgeCost != 20 {
		t.Fatalf("free_floor=%v bridge_cost=%v", k.freeFloor, k.bridgeCost)
	}
	if got := k.scoreBase(0, 1, 50); got != -40 {
		t.Fatalf("a claim on power 50 costs %v, want 40", -got)
	}
	usable := []PlacementCandidate{freeCandidate("a", 1, .1, 100), freeCandidate("b", 0, .3, 100)}
	if got, reason := k.decide(usable, 0); got != 1 || reason != "below_free_floor" {
		t.Fatalf("a floor of .2 left a at .1: decide=%d %q", got, reason)
	}
	// The retired keys still load and change nothing.
	old, err := ParsePlacementPolicy([]byte(`{"local_machine":"a","max_active_jobs":5,"score":{"claim_cost":50,"all_loaded_below":40}}`))
	if err != nil || old.scoreK() != (PlacementPolicy{}).scoreK() {
		t.Fatalf("retired score keys: %+v err=%v", old.scoreK(), err)
	}
	for _, bad := range []string{`{"free_floor":1.5}`, `{"free_floor":-0.1}`, `{"bridge_cost":-1}`} {
		if _, err := ParsePlacementPolicy([]byte(`{"local_machine":"a","max_active_jobs":5,"score":` + bad + `}`)); err == nil {
			t.Fatalf("score %s accepted", bad)
		}
	}
}

// A typical moment: mac-dev is half free with one job decided onto it
// (0.5*100 - 10 = 40) and an idle oci is 0.9*30 = 27. Neither is under the
// floor, so the scores decide, whichever is first in policy order: 27 does not
// reach 1.25*40, and 40 clears 1.25*27 = 33.75.
func TestPlacementTypicalMomentChoosesMacDev(t *testing.T) {
	k := (PlacementPolicy{}).scoreK()
	macDev := freeCandidate("mac-dev", k.scoreBase(.5, 1, 100), .5, 100)
	oci := freeCandidate("oci", k.scoreBase(.9, 0, 30), .9, 30)
	if macDev.Score != 40 || oci.Score != 27 {
		t.Fatalf("scores %v and %v, want 40 and 27", macDev.Score, oci.Score)
	}
	if got, reason := k.decide([]PlacementCandidate{macDev, oci}, 0); got != 0 || reason != "" {
		t.Fatalf("mac-dev first: decide=%d %q", got, reason)
	}
	if got, reason := k.decide([]PlacementCandidate{oci, macDev}, 0); got != 1 || reason != "challenger" {
		t.Fatalf("oci first: decide=%d %q", got, reason)
	}
}

// The rules run on the hub's real placement: a loaded local host that is
// first in policy order, a second host with free memory.
func TestPlacementDecisionRulesOnHubPlacement(t *testing.T) {
	policy := measuredLoadPolicy()
	run := func(macBusy, deskBusy, macMem, deskMem string) PlacementResult {
		cpu := map[string]string{"mac-work": macBusy, "desktop": deskBusy}
		hub := measuredLoadHub(t, map[string]map[string]string{
			placementCPUBusyQuery: cpu, placementCPUBusy1mQuery: cpu, placementThermalQuery: idleThermal(),
			placementMemAvailableQuery: {"mac-work": macMem, "desktop": deskMem},
		}, policy)
		return hub.placement(t.Context(), "worker", "repo")
	}
	// Scores 80 vs 90: 90 < 1.25*80, so the policy-order winner stays.
	if got := run("0.2", "0.1", "0.9", "0.9"); got.Decision != "mac-work" || got.Reason != "" {
		t.Fatalf("hysteresis: %+v", got)
	}
	// Scores 50 vs 90: the challenger clears the margin.
	if got := run("0.5", "0.1", "0.9", "0.9"); got.Decision != "desktop" || got.Reason != "challenger" {
		t.Fatalf("challenger: %+v", got)
	}
	// free .03*.5 = .015 and .03*1: both under the .05 floor, so the higher
	// free*power takes it (all loaded).
	got := run("0.97", "0.97", "0.30", "0.90")
	if got.Decision != "desktop" || got.Reason != "all_loaded" {
		t.Fatalf("all_loaded: %+v", got)
	}
	// The wire carries the terms behind the decision for a caller that asks.
	desktop, _ := t607Candidate(got, "desktop")
	if desktop.Signals.Terms == nil || desktop.Signals.Terms.Mem != 1 || !closeTerms(*desktop.Signals.Terms, PlacementTerms{.03, 1, 1, 1, 1}) {
		t.Fatalf("terms=%+v", desktop.Signals.Terms)
	}
	// The hub's answer reaches the spawn engine's ranking as the decided host.
	rawBytes, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	views, err := fleet.ParseHubHosts(string(rawBytes))
	if err != nil {
		t.Fatal(err)
	}
	decided := ""
	for _, view := range views {
		if view.Decided {
			decided = view.Name + ":" + view.DecisionReason
		}
	}
	if decided != "desktop:all_loaded" {
		t.Fatalf("decided=%q views=%+v", decided, views)
	}
}

// A weak host that happens to hold the decision is not forced on a caller when
// it is unusable: the rules only reorder usable candidates.
func TestPlacementDecisionRulesIgnoreUnusableHosts(t *testing.T) {
	policy := measuredLoadPolicy()
	cpu := map[string]string{"mac-work": "0.1", "desktop": "0.3"}
	hub := measuredLoadHub(t, map[string]map[string]string{placementCPUBusyQuery: cpu, placementCPUBusy1mQuery: cpu, placementThermalQuery: idleThermal()}, policy)
	hub.nodes["mac-work"].accepting = false
	got := hub.placement(t.Context(), "worker", "repo")
	if got.Decision != "desktop" || got.Reason != "" || !strings.Contains(got.Candidates[0].Reason+got.Candidates[1].Reason, "not_accepting") {
		t.Fatalf("%+v", got)
	}
}

// Acceptance 3 (node_spawn): a successful hub spawn writes a node_spawn event
// with the request id, job, profile and duration; no screen text, brief or argv.
func TestSuccessfulSpawnWritesNodeSpawnEvent(t *testing.T) {
	var events []fleetEvent
	clock := time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)
	client := &HubClient{
		spawnConfigPath: t14SpawnConfig(t, t.TempDir()),
		now:             func() time.Time { clock = clock.Add(1500 * time.Millisecond); return clock },
		fleetEvents:     func(_ time.Time, ev fleetEvent) bool { events = append(events, ev); return true },
		spawnEngine: func(_ context.Context, cwd, brief, workspace, session string, args []string) (*hubSpawnOutput, fleet.Execution) {
			output := &hubSpawnOutput{}
			_, _ = output.stderr.Write([]byte("SECRET SCREEN TEXT"))
			return output, fleet.Execution{Receipt: fleet.ExecutionReceipt{Job: "job-ok", Pane: "w:p1", Profile: "claude-opus-high"}}
		},
	}
	requestID := t14RequestID(2869)
	message := hubOutboundMessage{RequestID: requestID, CWDKey: "repo-a", BriefInline: "the brief text", WaitSeconds: 5, Args: []string{"--job", "job-ok", "--prompt", "argv text"}}
	result := client.runHubSpawn(t.Context(), message)
	if result.Error != "" || result.RC != 0 {
		t.Fatalf("result=%+v", result)
	}
	if len(events) != 1 {
		t.Fatalf("want one event, got %+v", events)
	}
	ev := events[0]
	if ev.Kind != "node_spawn" || ev.Outcome != "ok" || ev.Reason != "spawned" || ev.RequestID != requestID || ev.Job != "job-ok" || ev.Profile != "claude-opus-high" || ev.DurationMS == nil || *ev.DurationMS < 1500 {
		t.Fatalf("event=%+v", ev)
	}
	if strings.Contains(ev.Detail, "SECRET") || strings.Contains(ev.Detail, "brief") || strings.Contains(ev.Detail, "argv") {
		t.Fatalf("detail leaks: %q", ev.Detail)
	}
}

// The success event reaches the file Alloy tails, with the host label and the
// optional fields, through the production writer.
func TestSuccessfulSpawnEventReachesTheEventsFile(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	client := &HubClient{spawnConfigPath: t14SpawnConfig(t, t.TempDir()), spawnEngine: func(context.Context, string, string, string, string, []string) (*hubSpawnOutput, fleet.Execution) {
		return &hubSpawnOutput{}, fleet.Execution{Receipt: fleet.ExecutionReceipt{Job: "job-ok", Profile: "codex-high"}}
	}}
	requestID := t14RequestID(2870)
	client.runHubSpawn(t.Context(), hubOutboundMessage{RequestID: requestID, CWDKey: "repo-a", BriefInline: "brief", WaitSeconds: 5, Args: []string{"--job", "job-ok"}})
	raw, events := readFleetEventsFile(t)
	if len(events) != 1 || events[0]["kind"] != "node_spawn" || events[0]["outcome"] != "ok" || events[0]["request_id"] != requestID || events[0]["profile"] != "codex-high" || events[0]["host"] == "" {
		t.Fatalf("events=%s", raw)
	}
}

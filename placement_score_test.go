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
	return PlacementCandidate{Machine: machine, Score: score, Signals: &PlacementSignals{Terms: &PlacementTerms{CPU: 1, Mem: mem, IO: 1, Load: 1, Jobs: 1}}}
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

// Acceptance 2: when the best admitted score is under 15 every host counts as
// loaded and the decision goes to the host with the most free memory.
func TestPlacementAllLoadedChoosesHighestMemTerm(t *testing.T) {
	k := (PlacementPolicy{}).scoreK()
	usable := []PlacementCandidate{scoredCandidate("first", 14, .2), scoredCandidate("second", 9, .9), scoredCandidate("third", 12, .5)}
	if got, reason := k.decide(usable, 0); got != 1 || reason != "all_loaded" {
		t.Fatalf("decide=%d %q, want the highest mem term (second)", got, reason)
	}
	// Equal memory: the higher score, then policy order, decides.
	usable = []PlacementCandidate{scoredCandidate("first", 5, .5), scoredCandidate("second", 9, .5), scoredCandidate("third", 9, .5)}
	if got, reason := k.decide(usable, 0); got != 1 || reason != "all_loaded" {
		t.Fatalf("decide=%d %q", got, reason)
	}
	// One host at 15 is enough: the normal rules apply.
	usable = []PlacementCandidate{scoredCandidate("first", 14, .2), scoredCandidate("second", 15, .9)}
	if got, reason := k.decide(usable, 0); reason == "all_loaded" || got != 0 {
		t.Fatalf("decide=%d %q: 15 is not loaded, and 15 < 1.25*14 keeps the winner", got, reason)
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
	// Scores 100*.12*.5 = 6 and 100*.12*1 = 12: all loaded; desktop has the memory.
	got := run("0.88", "0.88", "0.30", "0.90")
	if got.Decision != "desktop" || got.Reason != "all_loaded" {
		t.Fatalf("all_loaded: %+v", got)
	}
	// The wire carries the terms behind the decision for a caller that asks.
	desktop, _ := t607Candidate(got, "desktop")
	if desktop.Signals.Terms == nil || desktop.Signals.Terms.Mem != 1 || !closeTerms(*desktop.Signals.Terms, PlacementTerms{.12, 1, 1, 1, 1}) {
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

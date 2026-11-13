package panewire

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// liveHeartbeat is a node heartbeat with the placement inputs. withNew adds the
// fields this change records: host_memory.swap_out_per_s and host_activity.
func liveHeartbeat(withNew bool) []byte {
	memory := map[string]any{"free_pct": 85, "compressed_mb": nil, "swap_used_mb": 0, "psi_some_avg10": nil, "swap_in_per_s": 2, "pressure": 0.5, "source": "proc_meminfo"}
	payload := map[string]any{"status": "alive", "host_signals": map[string]any{"power_source": "battery"}, "host_memory": memory}
	if withNew {
		memory["swap_out_per_s"] = 500
		payload["host_activity"] = map[string]any{"user_idle_s": 0}
	}
	encoded, err := json.Marshal(map[string]any{"type": "event", "kind": "heartbeat", "payload": payload})
	if err != nil {
		panic(err)
	}
	return encoded
}

func sendLiveHeartbeat(hub *HubServer, machine string, withNew bool) {
	hub.handleAgentMessage(machine, "fixture", hub.nodes[machine].agent, liveHeartbeat(withNew))
}

// (c) A candidate carries the node's live readings, the heartbeat age, and the
// 1-minute CPU busy the hk record calls cpu_busy_short, and the signals object
// that reaches the hk record names them.
func TestPlacementCandidateRecordsNodeLiveSignals(t *testing.T) {
	hub := memoryPlacementHub(t, "host-a")
	sendLiveHeartbeat(hub, "host-a", true)
	received := hub.nodes["host-a"].lastHeartbeat
	metrics := placementMetrics{cpu1m: map[string]float64{"host-a": .17}}
	result := hub.makePlacement(memoryPlacementPolicy("host-a"), metrics, "hub-only", received.Add(7*time.Second))

	candidate := memoryCandidate(t, result, "host-a")
	signals := candidate.Signals
	if signals == nil || signals.CPUBusyShort == nil || *signals.CPUBusyShort != .17 ||
		signals.SignalAgeS == nil || *signals.SignalAgeS != 7 || signals.PowerSource != "battery" ||
		!sameMemoryFloat(signals.SwapInPerS, 2) || !sameMemoryFloat(signals.SwapOutPerS, 500) ||
		!sameMemoryFloat(signals.Pressure, .5) || signals.UserIdleS == nil || *signals.UserIdleS != 0 {
		t.Fatalf("candidate signals=%+v", signals)
	}

	wire := placementWire(result, true, true)
	encoded, err := json.Marshal(wire.Candidates[0].Signals)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{`"cpu_busy_short":0.17`, `"signal_age_s":7`, `"power_source":"battery"`, `"swap_out_per_s":500`, `"user_idle_s":0`} {
		if !strings.Contains(string(encoded), name) {
			t.Fatalf("signals object omitted %s: %s", name, encoded)
		}
	}
	if without, _ := json.Marshal(placementWire(result, false, false).Candidates[0]); strings.Contains(string(without), "cpu_busy_short") {
		t.Fatalf("a caller that did not ask for signals=1 got them: %s", without)
	}
}

// (e) The new fields are recorded only. A heartbeat with them and the same one
// without them give the same decision, score and reasons.
func TestNodeLiveSignalsDoNotChangePlacementDecisionOrScore(t *testing.T) {
	now := time.Unix(2_000_000, 0)
	metrics := placementMetrics{
		load:     map[string]float64{"host-a": .4, "host-b": .1},
		cpu1m:    map[string]float64{"host-a": .3, "host-b": .05},
		memAvail: map[string]float64{"host-a": .5, "host-b": .8},
		swapIn:   map[string]float64{"host-a": 1, "host-b": 0},
	}
	run := func(withNew bool) PlacementResult {
		hub := memoryPlacementHub(t, "host-a", "host-b")
		sendLiveHeartbeat(hub, "host-a", withNew)
		sendLiveHeartbeat(hub, "host-b", false)
		return hub.makePlacement(memoryPlacementPolicy("host-a", "host-b"), metrics, "prometheus", now)
	}
	without, with := run(false), run(true)
	if without.Decision != with.Decision {
		t.Fatalf("decision changed: without=%q with=%q", without.Decision, with.Decision)
	}
	for _, machine := range []string{"host-a", "host-b"} {
		left, right := memoryCandidate(t, without, machine), memoryCandidate(t, with, machine)
		if left.Score != right.Score || left.Reason != right.Reason || left.Connected != right.Connected || left.Throttled != right.Throttled || left.Tasks != right.Tasks {
			t.Fatalf("%s changed: without=%+v with=%+v", machine, left, right)
		}
	}
	if got := memoryCandidate(t, with, "host-a").Signals; got == nil || got.SwapOutPerS == nil {
		t.Fatalf("the new fields were not recorded in the fixture that carries them: %+v", got)
	}
}

package panewire

import (
	"encoding/json"
	"testing"
)

// spawnResultWireWithStages is a job.spawn.result frame carrying a coarse stage
// and a stage_ms map, the two fields the hub validates separately (hk 5693).
func spawnResultWireWithStages(t *testing.T, stage string, stageMS map[string]int64) []byte {
	t.Helper()
	wire := map[string]any{
		"type": "job.spawn.result", "request_id": t14RequestID(93), "rc": 0,
		"job_id": "job-stage", "pane": "pane-stage", "stdout_tail": "",
		"stage": stage, "stage_ms": stageMS,
	}
	raw, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// A coarse stage is never submit: submit is a timing key, so a result that
// names it as its stage is refused like any other unknown stage (hk 5693).
func TestSpawnResultRejectsSubmitAsCoarseStage_t5693(t *testing.T) {
	wire := spawnResultWireWithStages(t, "submit", map[string]int64{"submit": 7})
	if _, ok := parseHubSpawnResult(wire); ok {
		t.Fatal("a result with stage \"submit\" must be rejected")
	}
}

// Only stage_ms takes submit: a landing coarse stage with a submit timing key
// is still a valid result (hk 5693).
func TestSpawnResultAcceptsSubmitInStageMS_t5693(t *testing.T) {
	wire := spawnResultWireWithStages(t, "landing", map[string]int64{"launch": 4, "submit": 7, "landing": 3})
	parsed, ok := parseHubSpawnResult(wire)
	if !ok {
		t.Fatal("stage_ms carrying submit must still be accepted")
	}
	if parsed.StageMS["submit"] != 7 {
		t.Fatalf("submit timing lost: %v", parsed.StageMS)
	}
}

// Every coarse stage other than submit stays accepted, so the split changes
// only what submit may be (hk 5693).
func TestSpawnCoarseStagesStillAccepted_t5693(t *testing.T) {
	for _, stage := range []string{"claim", "placement", "checkout", "launch", "landing"} {
		if _, ok := parseHubSpawnResult(spawnResultWireWithStages(t, stage, nil)); !ok {
			t.Fatalf("coarse stage %q must be accepted", stage)
		}
	}
}

package panewire

// spawnStages are the timing keys a node may report in a spawn result's
// stage_ms. "submit" is the brief's write, typing and record (hk 5585); a node
// sends it only to a hub that listed hubFeatureSpawnStageSubmit.
var spawnStages = []string{"claim", "placement", "checkout", "launch", "submit", "landing"}

// hubFeatureSpawnStageSubmit is listed in hub.features by a hub that accepts
// the "submit" key in stage_ms (hk 5585). A strict hub rejects the whole result
// for an unknown key, so a node folds submit into landing for any other hub
// (spawnResultForHub).
const hubFeatureSpawnStageSubmit = "spawn_stage_submit"

func validSpawnStage(stage string) bool {
	for _, known := range spawnStages {
		if stage == known {
			return true
		}
	}
	return false
}
func validSpawnStages(stages map[string]int64) bool {
	for stage, ms := range stages {
		if !validSpawnStage(stage) || ms < 0 || ms > 3600000 {
			return false
		}
	}
	return true
}

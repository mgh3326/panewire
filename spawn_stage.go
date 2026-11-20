package panewire

// spawnStages are the timing keys a node may report in a spawn result's
// stage_ms. "submit" is the brief's write, typing and record (hk 5585); a node
// sends it only to a hub that listed hubFeatureSpawnStageSubmit.
var spawnStages = []string{"claim", "placement", "checkout", "launch", "submit", "landing"}

// spawnCoarseStages are the values a spawn result may carry in its coarse
// stage field. "submit" is never one of them: it is a timing key only, and
// CoarseSpawnStage reports it as landing (hk 5643). Recovery rules such as
// postLaunch read this field, so it stays a closed set of its own.
var spawnCoarseStages = []string{"claim", "placement", "checkout", "launch", "landing"}

// hubFeatureSpawnStageSubmit is listed in hub.features by a hub that accepts
// the "submit" key in stage_ms (hk 5585). A strict hub rejects the whole result
// for an unknown key, so a node folds submit into landing for any other hub
// (spawnResultForHub).
const hubFeatureSpawnStageSubmit = "spawn_stage_submit"

// validSpawnStage reports whether stage is a coarse spawn stage (the result's
// stage field).
func validSpawnStage(stage string) bool {
	for _, known := range spawnCoarseStages {
		if stage == known {
			return true
		}
	}
	return false
}

// validSpawnTimingKey reports whether key is a stage_ms timing key.
func validSpawnTimingKey(key string) bool {
	for _, known := range spawnStages {
		if key == known {
			return true
		}
	}
	return false
}

func validSpawnStages(stages map[string]int64) bool {
	for stage, ms := range stages {
		if !validSpawnTimingKey(stage) || ms < 0 || ms > 3600000 {
			return false
		}
	}
	return true
}

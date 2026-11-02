package panewire

var spawnStages = []string{"claim", "placement", "checkout", "launch", "landing"}

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

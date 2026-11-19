package fleet

import (
	"testing"
	"time"
)

// TestSpawnReportsBriefSubmitBetweenLaunchAndLanding holds the split of the
// old landing stage (hk 5585): the brief's write, typing and record report as
// "submit" once, after the pane starts and before the landing proof, and that
// timing's coarse stage is landing, as before.
func TestSpawnReportsBriefSubmitBetweenLaunchAndLanding(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	var stages []string
	a.ObserveStage = func(stage string, _ time.Duration) { stages = append(stages, stage) }
	if err := a.Spawn(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	index := func(name string) int {
		found, at := 0, -1
		for i, stage := range stages {
			if stage == name {
				found++
				at = i
			}
		}
		if found != 1 {
			t.Fatalf("%q reported %d times in %v", name, found, stages)
		}
		return at
	}
	launch, submit, landing := index("launch"), index(StageBriefSubmit), index("landing")
	if !(launch < submit && submit < landing) {
		t.Fatalf("order launch=%d submit=%d landing=%d in %v", launch, submit, landing, stages)
	}
}

func TestCoarseSpawnStageKeepsHubStageRules(t *testing.T) {
	if got := CoarseSpawnStage(StageBriefSubmit); got != "landing" {
		t.Fatalf("submit reports coarse stage %q, want landing", got)
	}
	for _, stage := range []string{"claim", "placement", "checkout", "launch", "landing"} {
		if got := CoarseSpawnStage(stage); got != stage {
			t.Fatalf("%s reports coarse stage %q", stage, got)
		}
	}
}

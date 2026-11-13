package fleet

import (
	"slices"
	"testing"
)

// Fable 5.2 (claude-fable-5-2) is the fable profile's model; 5.1 stays
// accepted as the rollback path. These cases pin the three places the fable
// model id is read: the omp top-profile ranking and launch argv built from a
// live catalog row.

func TestOMPFableSelectorsRankAsFableTopProfile(t *testing.T) {
	want := slices.Index(topProfiles, "fable")
	if want < 0 {
		t.Fatal("fable is not a top profile")
	}
	for _, model := range []string{"anthropic/claude-fable-5-2", "anthropic/claude-fable-5-1"} {
		row := CatalogRow{Profile: "omp-" + model, ModelID: model}
		if got := topProfileRank(row); got != want {
			t.Errorf("omp rung %s ranks %d, want fable top rank %d", model, got, want)
		}
	}
}

func TestFableRowLaunchArgvCarriesFable52Model(t *testing.T) {
	a := &App{}
	row := launchRowFrom(CatalogRow{Profile: "fable", Effort: "high", ModelID: "claude-fable-5-2", Pool: "claude", Grade: "SS", Gate: defaultGate})
	argv, _, err := a.launchArgs(Seat{Kind: "claude"}, Launch{ModelID: row.ModelID, Effort: row.Effort})
	if err != nil {
		t.Fatal(err)
	}
	i := slices.Index(argv, "--model")
	if i < 0 || i+1 >= len(argv) || argv[i+1] != "claude-fable-5-2" {
		t.Fatalf("fable launch argv lacks --model claude-fable-5-2: %v", argv)
	}
}

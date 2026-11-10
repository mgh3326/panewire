package fleet

import (
	"encoding/json"
	"slices"
	"testing"
)

// Fable 5.2 (claude-fable-5-2) is the fable profile's model; 5.1 stays
// accepted as the rollback path. These cases pin the three places the fable
// model id is read: the omp top-profile ranking, the embedded catalog
// snapshot, and the launch argv built from the snapshot row.

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

func TestEmbeddedSnapshotFableRowNamesFable52(t *testing.T) {
	var doc struct {
		Rows []struct {
			Profile    string `json:"profile"`
			Effort     string `json:"effort"`
			ModelID    string `json:"model_id"`
			Grade      string `json:"grade"`
			Gate       string `json:"gate"`
			GateReason string `json:"gate_reason"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(catalogSnapshotJSON, &doc); err != nil {
		t.Fatal(err)
	}
	var fable []string
	for _, r := range doc.Rows {
		if r.Profile != "fable" {
			continue
		}
		fable = append(fable, r.ModelID)
		if r.ModelID != "claude-fable-5-2" {
			t.Errorf("fable row model_id = %q, want claude-fable-5-2", r.ModelID)
		}
		if r.Grade != "S+" || r.Gate != "consult_only" || r.Effort != "" {
			t.Errorf("fable row grade/gate/effort = %q/%q/%q, want S+/consult_only/\"\"", r.Grade, r.Gate, r.Effort)
		}
		if r.GateReason != "Opus 대비 2배 가격 — 운영자 명시 요청(consult-only)일 때만. hk:doc note/2026-09-23/model-refresh-opus55-gpt6-grok47" {
			t.Errorf("fable row gate_reason changed: %q", r.GateReason)
		}
	}
	if len(fable) != 1 {
		t.Fatalf("snapshot has %d fable rows, want 1: %v", len(fable), fable)
	}
}

func TestFableRowLaunchArgvCarriesFable52Model(t *testing.T) {
	var row *launchRow
	for _, r := range catalogSnapshot().Rows {
		if r.Profile == "fable" {
			row = &r
			break
		}
	}
	if row == nil {
		t.Fatal("snapshot has no fable row")
	}
	a := &App{}
	argv, _, err := a.launchArgs(Seat{Kind: "claude"}, Launch{ModelID: row.ModelID, Effort: row.Effort})
	if err != nil {
		t.Fatal(err)
	}
	i := slices.Index(argv, "--model")
	if i < 0 || i+1 >= len(argv) || argv[i+1] != "claude-fable-5-2" {
		t.Fatalf("fable launch argv lacks --model claude-fable-5-2: %v", argv)
	}
}

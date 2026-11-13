package fleet

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestGradeSSScaleAndMarkers(t *testing.T) {
	if !validCatalogGrade("SS") || gradeOrder["SS"] <= gradeOrder["S+"] {
		t.Errorf("catalog scale does not place SS above S+: %v", gradeOrder)
	}
	if admissionGradeRank("SS") >= admissionGradeRank("S+") {
		t.Errorf("admission scale does not place SS before S+: %v", admissionGrades)
	}
	if !validRepGrade("SS") || repGradeRank("SS") >= repGradeRank("S+") {
		t.Errorf("representation scale does not place SS above S+")
	}
	for _, tc := range []struct {
		title string
		grade string
		tier  string
	}{
		{"work (grade SS, T2)", "SS", "T2"},
		{"Work item: Grade SS, T2", "SS", "T2"},
		{"work (grade S, T2)", "S", "T2"},
		{"work (grade S+, T2)", "S+", "T2"},
	} {
		m := taskMarker.FindStringSubmatch(tc.title)
		if len(m) != 3 || m[1] != tc.grade || m[2] != tc.tier {
			t.Errorf("marker %q = %v, want %s/%s", tc.title, m, tc.grade, tc.tier)
		}
	}
	for _, title := range []string{"work (grade SSS, T2)", "work (grade SS+, T2)"} {
		if m := taskMarker.FindStringSubmatch(title); m != nil {
			t.Errorf("invalid grade marker %q matched: %v", title, m)
		}
	}
	if grade, tier := outcomeGrade("work (grade SS, T2)"); grade != "SS" || tier != "T2" {
		t.Errorf("outcomeGrade = %q/%q, want SS/T2", grade, tier)
	}
}

func TestGradeSSCatalogAndMonotonicity(t *testing.T) {
	raw := []json.RawMessage{json.RawMessage(`{"profile":"opus","effort":"high","model_id":"m","pool":"claude","grade":"SS","gate":"default"}`)}
	rows, _, _, err := decodeCatalogRows(raw)
	if err != nil || len(rows) != 1 || rows[0].Grade != "SS" {
		t.Errorf("decode SS catalog: rows=%+v err=%v", rows, err)
	}
	entry := CatalogEntry{Profile: "opus", ModelID: "m", Effort: "high", Pool: "claude", Grade: "SS", Gate: "default", DecidedAt: "2026-11-01T00:00:00Z", DecidedBy: "operator", DeviationRef: "hk:task/4044"}
	if err := ValidateCatalog([]CatalogEntry{entry}); err != nil {
		t.Errorf("validate SS catalog push: %v", err)
	}
	low := entry
	low.Effort, low.Grade = "high", "S+"
	high := entry
	high.Effort, high.Grade = "xhigh", "SS"
	if err := ValidateCatalog([]CatalogEntry{low, high}); err != nil {
		t.Fatalf("grade improvement with effort was refused: %v", err)
	}
	high.Grade = "S+"
	low.Grade = "SS"
	if err := ValidateCatalog([]CatalogEntry{low, high}); err == nil || !strings.Contains(err.Error(), "not monotonic") {
		t.Fatalf("grade downgrade with effort accepted: %v", err)
	}
}

func gradeSSSelectionRows() []CatalogRow {
	return []CatalogRow{
		{Profile: "opus", Effort: "high", Grade: "S+", Pool: "claude", Gate: "default", ModelID: "opus-m"},
		{Profile: "sonnet", Effort: "high", Grade: "SS", Pool: "claude", Gate: "default", ModelID: "sonnet-m"},
	}
}

func TestGradeSSSelection(t *testing.T) {
	serveRouting(t, routingDocV1)
	rows := gradeSSSelectionRows()
	for _, tc := range []struct {
		grade string
		want  string
	}{
		{"SS", "sonnet"},
		{"S+", "opus"},
	} {
		t.Run(tc.grade, func(t *testing.T) {
			pools := map[string]PoolView{"claude": {Status: "ok", Class: "spend", Blocking: ptrF(0)}}
			a, s, q := selFixture(t, tc.grade, rows, pools, []string{"claude"})
			q.GateDo = gatePools(map[string]string{"opus": "claude", "sonnet": "claude"})
			sel, err := a.selectProfile(context.Background(), s)
			if err != nil {
				t.Fatalf("grade %s selection: %v", tc.grade, err)
			}
			if got := sel.chosen.row.Profile; got != tc.want {
				t.Errorf("grade %s chose %s, want %s", tc.grade, got, tc.want)
			}
			if tc.grade == "SS" {
				if got := candOutcome(sel.record(), "opus", "high")["outcome"]; got != "dropped:grade_insufficient" {
					t.Errorf("S+ row outcome = %v, want dropped:grade_insufficient", got)
				}
			}
		})
	}
	t.Run("SS covers S+", func(t *testing.T) {
		a, s, q := selFixture(t, "S+", rows[1:], map[string]PoolView{"claude": {Status: "ok", Class: "spend", Blocking: ptrF(0)}}, []string{"claude"})
		q.GateDo = gatePools(map[string]string{"sonnet": "claude"})
		sel, err := a.selectProfile(context.Background(), s)
		if err != nil || sel.chosen.row.Grade != "SS" {
			t.Fatalf("SS-only row should cover S+: selection=%+v err=%v", sel, err)
		}
	})
}

func TestGradeSSSeatGate(t *testing.T) {
	for _, tc := range []struct {
		launch string
		task   string
		want   int
	}{{"SS", "S+", 0}, {"S+", "SS", exitSeatRefused}} {
		p, err := LoadPolicy("")
		if err != nil {
			t.Fatal(err)
		}
		seat := p.Profiles["sonnet"]
		got := code(seat.Check(Launch{ModelID: "m", Effort: "high", Grade: tc.launch, Gate: "default"}, "implementer", tc.task, "", "", map[string]any{seat.Capability: true}))
		if got != tc.want {
			t.Errorf("seat grade %s covering %s: got %d want %d", tc.launch, tc.task, got, tc.want)
		}
	}
}

func TestGradeSSTaskSpawn(t *testing.T) {
	taskApp, taskSpawn, taskQuota := selFixture(t, "SS", gradeSSSelectionRows(), map[string]PoolView{"claude": {Status: "ok", Class: "spend", Blocking: ptrF(0)}}, []string{"claude"})
	taskQuota.GateDo = gatePools(map[string]string{"opus": "claude", "sonnet": "claude"})
	useFakeHK(t).SetTask("42", `{"id":42,"title":"work (grade SS, T2)","state":"ready","refs":{}}`)
	taskSpawn.T = "T1"
	if _, err := taskApp.selectProfile(context.Background(), taskSpawn); err == nil || !strings.Contains(err.Error(), "task scale does not match --t") {
		t.Errorf("mismatched --t was not refused with the task scale reason: %v", err)
	}
	taskSpawn.T = "T2"
	taskSelection, taskErr := taskApp.selectProfile(context.Background(), taskSpawn)
	if taskErr != nil || taskSelection.chosen.row.Grade != "SS" {
		t.Fatalf("task marker SS selection: chosen=%+v err=%v", taskSelection, taskErr)
	}
	if grade, tier := outcomeGrade("work (grade SS, T2)"); grade != "SS" || tier != "T2" {
		t.Fatalf("task trailer marker = %s/%s", grade, tier)
	}
	brief, err := taskApp.prepareBrief(Spawn{Task: "4044", TaskGrade: "SS", T: "T2"}, Seat{Canon: "sonnet", Family: "anthropic"}, Launch{Effort: "high"}, "BODY")
	if err != nil || !strings.Contains(brief, "Mergewake-Grade: SS\nMergewake-Tier: T2") {
		t.Fatalf("task brief grade trailer missing: %q (%v)", brief, err)
	}
}

func TestGradeSSPlanAndCommandInputs(t *testing.T) {
	a, q, _, _, out := planFixture(t)
	q.CatalogRows = []CatalogRow{
		{Profile: "devin-swe2", Effort: "high", Grade: "S+", Pool: "devin", Gate: "default", ModelID: "swe-2"},
		{Profile: "sonnet", Effort: "high", Grade: "SS", Pool: "claude", Gate: "default", ModelID: "m"},
	}
	q.LaunchValue = Launch{Profile: "sonnet", ModelID: "m", Effort: "high", Grade: "SS", Gate: "default", Pool: "claude"}
	q.GateDo = gatePools(map[string]string{"devin-swe2": "devin", "sonnet": "claude"})
	planErr := a.planCmd(context.Background(), []string{"--grade", "SS", "--t", "T1", "--json"})
	if planErr != nil {
		t.Errorf("plan --grade SS: %v\n%s", planErr, out.String())
	} else if !strings.Contains(out.String(), `"grade": "SS"`) || !strings.Contains(out.String(), `"reason": "grade_insufficient"`) {
		t.Fatalf("plan did not select SS and report the lower row as insufficient: %s", out.String())
	}

	if err := a.resolveCmd(context.Background(), []string{"--role", "implementer", "--grade", "SS", "--t", "T1"}); err != nil {
		t.Errorf("resolve --grade SS: %v", err)
	}
	selection := `{"now":"2026-11-12T00:00:00Z","grade":"SS","role":"implementer","pools":[{"id":"claude","status":"ok","class":"spend","used_pct":0}],"candidates":[{"profile":"sonnet","effort":"high","grade":"SS","pool":"claude","gate":"default","model_id":"m"}]}`
	path := t.TempDir() + "/selection.json"
	if err := os.WriteFile(path, []byte(selection), 0600); err != nil {
		t.Fatal(err)
	}
	if err := a.simulate(context.Background(), []string{"--selection", path}); err != nil {
		t.Errorf("simulate SS selection: %v", err)
	}
}

func TestGradeSSHostConfig(t *testing.T) {
	path := t.TempDir() + "/hosts.toml"
	if err := os.WriteFile(path, []byte("[hosts.pi]\ngrades = [\"SS\"]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); err != nil {
		t.Fatalf("host grades SS: %v", err)
	}
}

func TestGradeSSRepresentations(t *testing.T) {
	if !validRepGrade("SS") || repGradeRank("SS") >= repGradeRank("S+") {
		t.Errorf("SS is not accepted and ordered in reps")
	}
	a := testApp(t)
	var out strings.Builder
	if err := a.repsAdd(context.Background(), []string{"--profile", "opus", "--model", "m", "--task", "hk4044", "--tier", "T2", "--role", "impl", "--grade", "SS", "--rounds", "1", "--blockers-found", "0", "--completed", "1", "--dry-run"}, &out); err != nil {
		t.Errorf("reps add SS: %v", err)
	}
	if !strings.Contains(out.String(), `"grade":"SS"`) {
		t.Fatalf("reps add did not preserve SS: %s", out.String())
	}
}

package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// runSimulate invokes the simulate command on a fixture path and returns the
// printed decision record. The fake runner proves nothing external ran.
func runSimulate(t *testing.T, args ...string) (map[string]any, *fakeRunner, error) {
	t.Helper()
	a := testApp(t)
	fr := &fakeRunner{}
	a.Runner = fr
	a.Quota = CLIQuota{Runner: fr, Binary: "pacelever"}
	a.Source = &fakePlacementSource{}
	out := &bytes.Buffer{}
	a.Out = out
	err := a.simulate(context.Background(), args)
	var rec map[string]any
	if out.Len() > 0 {
		if e := json.Unmarshal(out.Bytes(), &rec); e != nil {
			t.Fatalf("simulate output is not JSON: %v\n%s", e, out.String())
		}
	}
	return rec, fr, err
}

func simulateFixturePath(t *testing.T) string {
	t.Helper()
	return filepath.Join("testdata", "simulate-claude-codex-2026-10-31.json")
}

func TestSimulateRanksWasteUrgency(t *testing.T) {
	rec, fr, err := runSimulate(t, "--selection", simulateFixturePath(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(fr.Calls) != 0 {
		t.Fatalf("simulate must run no external command: %v", fr.Calls)
	}
	chosen := rec["chosen"].(map[string]any)
	if chosen["seat"] != "sonnet" || chosen["tier"] != "waste" {
		t.Fatalf("the 2026-10-31 inputs choose the claude rung: %v", chosen)
	}
	if rec["mode"] != "simulate" {
		t.Fatalf("mode: %v", rec["mode"])
	}
	if got := candOutcome(rec, "devin-swe2", "high")["outcome"]; got != "dropped:free_limit" {
		t.Fatalf("the held free rung is refused by its observed free limit: %v", got)
	}
	if got := candOutcome(rec, "sonnet", "high")["waste_urgency"]; got == nil {
		t.Fatalf("ranked candidates carry waste_urgency: %v", got)
	}
}

func TestSimulateSetChangesChoice(t *testing.T) {
	// After claude's reset it reads 0% used, projected 0 with +168 h to the
	// next reset (a fresh window cannot be paced, so the projection is given).
	rec, fr, err := runSimulate(t, "--selection", simulateFixturePath(t), "--set", "claude:used=0,reset=+168h,projected=0")
	if err != nil {
		t.Fatal(err)
	}
	if len(fr.Calls) != 0 {
		t.Fatalf("simulate must run no external command: %v", fr.Calls)
	}
	if got := rec["chosen"].(map[string]any)["seat"]; got != "codex-luna" {
		t.Fatalf("--set must change the choice: %v", got)
	}
}

func TestSimulateNowAndJobEnvelope(t *testing.T) {
	// A job event envelope wraps the same record under payload.selection.
	b, err := os.ReadFile(simulateFixturePath(t))
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	env := map[string]any{"kind": "job.placement", "created_at": "2026-10-31T07:24:32Z", "job_id": "p4-pw6", "payload": map[string]any{"selection": doc}}
	eb, _ := json.Marshal(env)
	path := filepath.Join(t.TempDir(), "event.json")
	if err := os.WriteFile(path, eb, 0600); err != nil {
		t.Fatal(err)
	}
	rec, _, err := runSimulate(t, "--selection", path, "--now", "2026-10-31T07:24:00Z")
	if err != nil {
		t.Fatal(err)
	}
	if got := rec["chosen"].(map[string]any)["seat"]; got != "sonnet" {
		t.Fatalf("job envelope selects the claude rung: %v", got)
	}
}

func TestSimulateRejectsMissingSelection(t *testing.T) {
	if _, _, err := runSimulate(t); err == nil {
		t.Fatal("simulate without --selection must fail")
	}
	if _, _, err := runSimulate(t, "--selection", simulateFixturePath(t), "--set", "claude:used=oops"); err == nil {
		t.Fatal("a malformed --set must fail")
	}
}

func TestSimulateRejectsNonFiniteSet(t *testing.T) {
	for _, v := range []string{"claude:used=NaN", "claude:used=Inf", "claude:used=-Inf", "claude:projected=NaN", "claude:projected=Inf"} {
		if _, _, err := runSimulate(t, "--selection", simulateFixturePath(t), "--set", v); err == nil || exitCode(err) != exitUsage {
			t.Fatalf("--set %s must fail with exitUsage, got %v", v, err)
		}
	}
}

// A recorded job.placement carries its selection record under
// payload.selection: the request under `requested`, no pools array, and the
// pool reading on each candidate row. simulate rebuilds the pool views from
// those rows; a recorded reset instant exists nowhere, so pools rank by the
// projected fallback until --set supplies a reset.
func TestSimulateReplaysRecordedEnvelope(t *testing.T) {
	path := filepath.Join("testdata", "simulate-recorded-p4-pw6.json")
	rec, fr, err := runSimulate(t, "--selection", path)
	if err != nil {
		t.Fatal(err)
	}
	if len(fr.Calls) != 0 {
		t.Fatalf("simulate must run no external command: %v", fr.Calls)
	}
	if rec["chosen"] == nil {
		t.Fatal("a recorded envelope must replay to a choice")
	}
	if got := candOutcome(rec, "sonnet", "high")["waste_order"]; got != "projected_fallback" {
		t.Fatalf("recorded rows carry no reset instant: %v", got)
	}
	// Giving claude a reset 9h out makes its ~68% remainder urgent and ranks
	// the claude rung by reset order.
	rec, _, err = runSimulate(t, "--selection", path, "--set", "claude:reset=+9h")
	if err != nil {
		t.Fatal(err)
	}
	row := candOutcome(rec, "sonnet", "high")
	if row["waste_order"] != "reset" || row["waste_urgency"] == nil || row["reset_hours"] == nil {
		t.Fatalf("--set claude:reset must produce an urgency: %v", row)
	}
	if u := row["waste_urgency"].(float64); u < 5 || u > 9 {
		t.Fatalf("claude urgency after --set reset=+9h: %v", u)
	}
	if got := candOutcome(rec, "codex-luna", "high")["waste_order"]; got != "projected_fallback" {
		t.Fatalf("codex still has no reset and keeps the fallback: %v", got)
	}
}

// A dropped rung is not a ranked candidate: none of the waste-ranking fields
// may appear on its record row, even when its pool is paceable.
func TestSimulateDroppedRowHasNoWasteFields(t *testing.T) {
	doc := `{"now":"2026-10-31T07:24:00Z","grade":"A",
	  "pools":[{"id":"claude","used_pct":31,"resets_at":"2026-10-31T16:48:00Z","window":"7d"}],
	  "candidates":[
	    {"profile":"sonnet","effort":"high","grade":"A","pool":"claude","gate":"default","model_id":"m"},
	    {"profile":"weak","effort":"high","grade":"B","pool":"claude","gate":"default","model_id":"m"}]}`
	path := filepath.Join(t.TempDir(), "sel.json")
	if err := os.WriteFile(path, []byte(doc), 0600); err != nil {
		t.Fatal(err)
	}
	rec, _, err := runSimulate(t, "--selection", path)
	if err != nil {
		t.Fatal(err)
	}
	dropped := candOutcome(rec, "weak", "high")
	if dropped["outcome"] != "dropped:grade_insufficient" {
		t.Fatalf("the B rung must drop: %v", dropped["outcome"])
	}
	for _, k := range []string{"reset_hours", "waste_urgency", "waste_order"} {
		if _, ok := dropped[k]; ok {
			t.Fatalf("a dropped candidate records no %s: %v", k, dropped)
		}
	}
	if candOutcome(rec, "sonnet", "high")["reset_hours"] == nil {
		t.Fatal("the ranked candidate keeps reset_hours")
	}
}

// --set pool:used=N without a reset= term keeps the reset instant the input
// recorded — also for a sub-daily window whose reset never feeds urgency but
// still paces the projection.
func TestSimulateSetKeepsRecordedReset(t *testing.T) {
	doc := `{"now":"2026-10-31T07:24:00Z","grade":"A",
	  "pools":[
	    {"id":"fast","used_pct":20,"resets_at":"2026-10-31T09:24:00Z","window":"5h"},
	    {"id":"codex","used_pct":13,"projected_pct":30}],
	  "candidates":[
	    {"profile":"fast-cand","effort":"high","grade":"A","pool":"fast","gate":"default","model_id":"m"},
	    {"profile":"codex-luna","effort":"high","grade":"A","pool":"codex","gate":"default","model_id":"m"}]}`
	path := filepath.Join(t.TempDir(), "sel.json")
	if err := os.WriteFile(path, []byte(doc), 0600); err != nil {
		t.Fatal(err)
	}
	rec, _, err := runSimulate(t, "--selection", path, "--set", "fast:used=40")
	if err != nil {
		t.Fatal(err)
	}
	// 3h of the 5h window elapsed: 40% used paces to 66.7% at reset.
	got := candOutcome(rec, "fast-cand", "high")["projected_pct"]
	if p, ok := got.(float64); !ok || p < 66 || p > 67 {
		t.Fatalf("the recorded sub-daily reset must survive --set used=: %v", got)
	}
}

func TestSimulateSetKeepsRecordedProjectionWithoutReset(t *testing.T) {
	doc := `{"now":"2026-10-31T07:24:00Z","grade":"A",
	  "pools":[{"id":"codex","used_pct":13,"projected_pct":30}],
	  "candidates":[{"profile":"codex-luna","effort":"high","grade":"A","pool":"codex","gate":"default","model_id":"m"}]}`
	path := filepath.Join(t.TempDir(), "sel.json")
	if err := os.WriteFile(path, []byte(doc), 0600); err != nil {
		t.Fatal(err)
	}
	rec, _, err := runSimulate(t, "--selection", path, "--set", "codex:used=5")
	if err != nil {
		t.Fatal(err)
	}
	if got := candOutcome(rec, "codex-luna", "high")["projected_pct"]; got != 30.0 {
		t.Fatalf("a --set without projected= and without a reset must keep the recorded projection: %v", got)
	}
}

func TestSimulateReplaysBareRecordedSelection(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("testdata", "simulate-recorded-p4-pw6.json"))
	if err != nil {
		t.Fatal(err)
	}
	var env map[string]any
	if err := json.Unmarshal(b, &env); err != nil {
		t.Fatal(err)
	}
	sel := env["payload"].(map[string]any)["selection"]
	for name, input := range map[string]any{"bare": sel, "payload": map[string]any{"payload": sel}} {
		path := filepath.Join(t.TempDir(), name+".json")
		if err := os.WriteFile(path, mustJSON(input), 0600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := runSimulate(t, "--selection", path); err != nil {
			t.Fatalf("%s recorded selection must replay: %v", name, err)
		}
	}
}

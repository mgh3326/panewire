package fleet

import (
	"os"
	"path/filepath"
	"testing"
)

func riskFixturePath() string {
	return filepath.Join("testdata", "simulate-waste-risk-2026-10-31.json")
}

func TestSimulateWasteRiskBeatsFree(t *testing.T) {
	rec, fr, err := runSimulate(t, "--selection", riskFixturePath())
	if err != nil {
		t.Fatal(err)
	}
	if len(fr.Calls) != 0 {
		t.Fatalf("simulate must run no external command: %v", fr.Calls)
	}
	chosen := rec["chosen"].(map[string]any)
	if chosen["seat"] != "sonnet" || chosen["waste_risk"] != true {
		t.Fatalf("claude at waste risk beats SWE-2 on 2026-10-31: %v", chosen)
	}
	// After the reset the fresh window cannot be paced: no risk, SWE-2 first.
	rec, _, err = runSimulate(t, "--selection", riskFixturePath(), "--set", "claude:used=0,reset=+168h")
	if err != nil {
		t.Fatal(err)
	}
	if got := rec["chosen"].(map[string]any)["seat"]; got != "devin-swe2" {
		t.Fatalf("after claude's reset SWE-2 is first again: %v", got)
	}
}

func TestSimulatePolicyOverridesChangeChoice(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.json")
	if e := os.WriteFile(path, []byte(riskPolicy(`, "waste_risk": {"min_unused_pct": 80}`)), 0o600); e != nil {
		t.Fatal(e)
	}
	rec, _, err := runSimulate(t, "--selection", riskFixturePath(), "--policy", path)
	if err != nil {
		t.Fatal(err)
	}
	if got := rec["chosen"].(map[string]any)["seat"]; got != "devin-swe2" {
		t.Fatalf("--policy with min_unused_pct 80 must keep free first: %v", got)
	}
	if rec["policy_file"] != path {
		t.Fatalf("the record names the policy file: %v", rec["policy_file"])
	}
	for _, set := range []string{"waste_risk.min_unused_pct=80", "waste_risk.horizon_hours=4"} {
		rec, _, err = runSimulate(t, "--selection", riskFixturePath(), "--policy", path, "--policy-set", set)
		if err != nil {
			t.Fatal(err)
		}
		if got := rec["chosen"].(map[string]any)["seat"]; got != "devin-swe2" {
			t.Fatalf("--policy-set %s must keep free first: %v", set, got)
		}
		if ov, _ := rec["policy_overrides"].([]any); len(ov) != 1 {
			t.Fatalf("the record lists the overrides: %v", rec["policy_overrides"])
		}
	}
	for _, bad := range []string{"waste_risk.min_unused_pct=abc", "nope=1"} {
		if _, _, err := runSimulate(t, "--selection", riskFixturePath(), "--policy-set", bad); exitCode(err) != exitUsage {
			t.Fatalf("--policy-set %s: want exit %d, got %v", bad, exitUsage, err)
		}
	}
	if _, _, err := runSimulate(t, "--selection", riskFixturePath(), "--policy-set", "waste_risk.min_unused_pct=150"); exitCode(err) != exitConfig {
		t.Fatalf("an out-of-range threshold is a config error: %v", err)
	}
}

func TestSimulateRejectsMalformedPoolInput(t *testing.T) {
	cases := map[string]string{
		"resets_at": `{"id": "claude", "class": "spend", "used_pct": 31, "resets_at": "tomorrow", "window": "7d"}`,
		"window":    `{"id": "claude", "class": "spend", "used_pct": 31, "resets_at": "2026-10-31T16:50:00Z", "window": "weekly"}`,
	}
	for name, pool := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "in.json")
			doc := `{"now": "2026-10-31T08:50:00Z", "grade": "A", "pools": [` + pool + `],
			  "candidates": [{"profile": "sonnet", "effort": "high", "grade": "A", "pool": "claude", "gate": "default", "model_id": "m"}]}`
			if e := os.WriteFile(path, []byte(doc), 0o600); e != nil {
				t.Fatal(e)
			}
			if _, _, err := runSimulate(t, "--selection", path); exitCode(err) != exitConfig {
				t.Fatalf("malformed %s: want exit %d, got %v", name, exitConfig, err)
			}
		})
	}
}

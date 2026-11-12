package fleet

import (
	"path/filepath"
	"strings"
	"testing"
)

// The 2026-11-11 order on a fixture: a free SWE-2 rung, a live omp credit rung
// and a ranked subscription rung. Free is chosen before credit and ranked.
func TestSimulatePoolOrderCreditBesideFree_T4006(t *testing.T) {
	rec, fr, err := runSimulate(t, "--selection", filepath.Join("testdata", "simulate-pool-order-credit-free.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(fr.Calls) != 0 {
		t.Fatalf("simulate must run no external command: %v", fr.Calls)
	}
	var tiers []string
	for _, tier := range rec["tiers"].([]any) {
		tiers = append(tiers, tier.(string))
	}
	if got := strings.Join(tiers, ","); got != "waste,free,ranked" {
		t.Fatalf("tiers = %s", got)
	}
	chosen := rec["chosen"].(map[string]any)
	if chosen["seat"] != "devin-swe2" || chosen["tier"] != "free" {
		t.Fatalf("the free rung is chosen before credit: %v", chosen)
	}
	if got := candOutcome(rec, "sonnet", "high")["outcome"]; got != "below_choice" {
		t.Fatalf("the ranked pool is below the choice: %v", got)
	}
	if got := candOutcome(rec, "devin-swe2", "high")["tier"]; got != "free" {
		t.Fatalf("the SWE-2 rung keeps its free tier: %v", got)
	}
}

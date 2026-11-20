package fleet

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// A login_missing need the hub has not seen cleared drops its (host, kind) pair
// from automatic placement, whatever the node's own report says about the
// login: a launch that stopped at the login screen is the evidence, and the
// node's probe can read the login present meanwhile (hk 5686). The drop names
// login_missing, and once the hub clears the need the pair is placed again.

func TestLoginNeedDropsOnlyItsKindWhileItStands(t *testing.T) {
	now := time.Date(2026, 11, 20, 1, 0, 0, 0, time.UTC)
	// readyNode reads the claude login present, so only the need can drop it.
	standing := ReadinessState{Readiness: readyNode(), LoginNeeds: []string{"claude"}}
	if got := standing.Drop(ReadinessNeed{Kind: "claude"}, now); got != ReadyLoginMissing {
		t.Fatalf("standing need: drop=%q want %q", got, ReadyLoginMissing)
	}
	if got := standing.Drop(ReadinessNeed{Kind: "codex"}, now); got != "" {
		t.Fatalf("the need reached a sibling kind: %q", got)
	}
	// A node that never reported is skipped by the hub's need too.
	unreported := ReadinessState{Unknown: true, LoginNeeds: []string{"claude"}}
	if got := unreported.Drop(ReadinessNeed{Kind: "claude"}, now); got != ReadyLoginMissing {
		t.Fatalf("unreported node: drop=%q want %q", got, ReadyLoginMissing)
	}
	// cmd has no login, so a need on it never drops it.
	cmd := ReadinessState{Readiness: readyNode(), LoginNeeds: []string{"cmd"}}
	if got := cmd.Drop(ReadinessNeed{Kind: "cmd"}, now); got != "" {
		t.Fatalf("cmd dropped by a login need: %q", got)
	}
	cleared := ReadinessState{Readiness: readyNode()}
	if got := cleared.Drop(ReadinessNeed{Kind: "claude"}, now); got != "" {
		t.Fatalf("cleared need still drops the pair: %q", got)
	}
}

// The skip reaches the placement pass: the other host takes the seat, and the
// skipped host's row names login_missing (the request's tried list). When the
// need is cleared the host is the best one again and is chosen.
func TestLoginNeedMovesPlacementAndClearsBack(t *testing.T) {
	standing := ReadinessState{Readiness: readyNode(), LoginNeeds: []string{"claude"}}
	ready := ReadinessState{Readiness: readyNode()}

	a, s := readinessFixture(t, map[string]ReadinessState{"mac-b": standing, "cloud-small": ready})
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatalf("refused: %v", err)
	}
	if sel.chosen.host != "cloud-small" || sel.chosen.accountKey != testClaudeB {
		t.Fatalf("placed %s with %s, want cloud-small with %s", sel.chosen.host, sel.chosen.accountKey, testClaudeB)
	}
	row := hostRow(t, sel, "mac-b")
	if row["outcome"] != ReadyLoginMissing {
		t.Fatalf("mac-b outcome=%v want %s", row["outcome"], ReadyLoginMissing)
	}
	raw, _ := json.Marshal(row["readiness_drops"])
	if !strings.Contains(string(raw), `"code":"`+ReadyLoginMissing+`"`) {
		t.Fatalf("drops=%s", raw)
	}

	a, s = readinessFixture(t, map[string]ReadinessState{"mac-b": ready, "cloud-small": ready})
	sel, err = a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatalf("refused after the need cleared: %v", err)
	}
	if sel.chosen.host != "mac-b" || sel.chosen.accountKey != testClaudeA {
		t.Fatalf("after the need cleared placed %s with %s, want mac-b with %s", sel.chosen.host, sel.chosen.accountKey, testClaudeA)
	}
}

// When the skipped host is the only one holding the account, the candidate's
// own reason is login_missing, not a missing login read from the report.
func TestLoginNeedNamesTheReasonWhenNoHostIsLeft(t *testing.T) {
	standing := ReadinessState{Readiness: readyNode(), LoginNeeds: []string{"claude"}}
	a, s, _, _ := hostFirstFixture(t, "consult", "A", consultRows, map[string]string{"mac-b": testClaudeA}, map[string]float64{"mac-b": 0.1})
	a.Repository = &Repository{Name: "panewire"}
	a.State = readinessSnapshot(t, a, map[string]ReadinessState{"mac-b": standing}, a.Now())
	sel, err := a.selectProfile(context.Background(), s)
	if err == nil || sel == nil {
		t.Fatalf("expected a refusal with a record, got %v", err)
	}
	raw, _ := json.Marshal(sel.record())
	if !strings.Contains(string(raw), `"reason":"`+ReadyLoginMissing+`"`) || !strings.Contains(string(raw), `"outcome":"`+ReadyLoginMissing+`"`) {
		t.Fatalf("record does not name the skip: %s", raw)
	}
}

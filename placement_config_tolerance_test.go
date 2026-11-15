package panewire

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// A placement.json written before wake_on_spill was removed (#476, the v0.0.332
// refusal) must load: the hub starts and names the ignored key and its file.
func TestPlacementPolicyRetiredKeyLoadsWithWarning(t *testing.T) {
	path := filepath.Join(t.TempDir(), "placement.json")
	if err := os.WriteFile(path, []byte(`{"local_machine":"mac-work","spill_targets":["desktop"],"max_active_jobs":5,"wake_on_spill":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": r6OperatorToken, "mac-work": r6NodeAToken}, PlacementPolicyPath: path, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	if err != nil {
		t.Fatalf("hub refused a placement.json with an unknown key: %v", err)
	}
	if hub == nil {
		t.Fatal("hub is nil")
	}
	if out := logs.String(); !strings.Contains(out, "key=wake_on_spill") || !strings.Contains(out, "file="+path) {
		t.Fatalf("unknown key not reported with its key and file: %q", out)
	}
}

// A malformed value for a known key still refuses the file.
func TestPlacementPolicyMalformedKnownValueStillRefuses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "placement.json")
	if err := os.WriteFile(path, []byte(`{"local_machine":"mac-work","max_active_jobs":"five"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewHubServer(HubServerConfig{PlacementPolicyPath: path}); err == nil {
		t.Fatal("placement.json with a non-integer max_active_jobs was accepted")
	}
}

func TestParsePlacementPolicyRecordsOnlyUnreadKeys(t *testing.T) {
	// capacity_weight is retired but still a known field, so it is decoded and
	// warned on reload rather than listed as an unknown key.
	policy, err := ParsePlacementPolicy([]byte(`{"local_machine":"mac-work","max_active_jobs":5,"wake_on_spill":true,"capacity_weight":2}`))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(policy.ignoredKeys, []string{"wake_on_spill"}) {
		t.Fatalf("ignoredKeys=%v, want [wake_on_spill]", policy.ignoredKeys)
	}
}

// Unknown keys inside a known object (score, a machines entry) load too and are
// recorded by dotted path, so an operator can see which key is not read.
func TestPlacementPolicyNestedUnknownKeysLoadAndAreRecorded(t *testing.T) {
	policy, err := ParsePlacementPolicy([]byte(`{"local_machine":"mac-work","max_active_jobs":5,"score":{"unknown":1},"machines":{"desktop":{"task_slots":1,"extra":1}}}`))
	if err != nil {
		t.Fatalf("unknown nested keys refused: %v", err)
	}
	for _, want := range []string{"score.unknown", "machines.desktop.extra"} {
		if !slices.Contains(policy.ignoredKeys, want) {
			t.Fatalf("ignoredKeys=%v, missing %s", policy.ignoredKeys, want)
		}
	}
}

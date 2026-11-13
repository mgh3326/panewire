package panewire

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// Unknown keys at the file level and inside an override entry are ignored and
// listed for the startup warning, with the entry key named by its machine.
func TestAcceptingOverridesUnknownKeysIgnoredAndRecorded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accepting.json")
	if err := os.WriteFile(path, []byte(`{"overrides":{"mac-work":{"mode":"on","typo_key":1}},"bogus":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	overrides, ignored, err := loadAcceptingOverrides(path)
	if err != nil {
		t.Fatalf("override file with unknown keys refused: %v", err)
	}
	if want := []string{"bogus", "overrides.mac-work.typo_key"}; !slices.Equal(ignored, want) {
		t.Fatalf("ignored=%v, want %v", ignored, want)
	}
	if overrides["mac-work"].Mode != "on" {
		t.Fatalf("override mode=%q, want on", overrides["mac-work"].Mode)
	}
}

// A malformed value for a known key still refuses the file.
func TestAcceptingOverridesMalformedKnownValueStillRefuses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accepting.json")
	if err := os.WriteFile(path, []byte(`{"overrides":{"mac-work":{"mode":"sideways"}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadAcceptingOverrides(path); err == nil {
		t.Fatal("override with an unknown mode was accepted")
	}
}

// A rewrite carries the keys the loader ignores, so a hub-side override change
// does not erase data a newer release wrote at the root or in an entry.
func TestAcceptingOverridesRewriteKeepsIgnoredKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accepting.json")
	if err := os.WriteFile(path, []byte(`{"overrides":{"mac-work":{"mode":"on","note":"kept"}},"future":{"a":1}}`), 0600); err != nil {
		t.Fatal(err)
	}
	h := &HubServer{}
	h.relayControl.acceptingOverridesPath = path
	next := map[string]acceptingOverride{
		"mac-work": {Mode: "off", Containment: "hk:7", ContainmentVerified: true},
		"desk":     {Mode: "auto"},
	}
	if err := h.persistAcceptingOverridesLocked(next); err != nil {
		t.Fatal(err)
	}
	overrides, ignored, err := loadAcceptingOverrides(path)
	if err != nil {
		t.Fatalf("rewritten override file refused: %v", err)
	}
	if want := []string{"future", "overrides.mac-work.note"}; !slices.Equal(ignored, want) {
		t.Fatalf("ignored=%v, want %v", ignored, want)
	}
	if got := overrides["mac-work"]; got.Mode != "off" || got.Containment != "hk:7" || !got.ContainmentVerified {
		t.Fatalf("mac-work=%+v", got)
	}
	if got := overrides["desk"]; got.Mode != "auto" {
		t.Fatalf("desk=%+v", got)
	}
}

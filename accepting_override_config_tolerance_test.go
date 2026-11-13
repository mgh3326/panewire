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

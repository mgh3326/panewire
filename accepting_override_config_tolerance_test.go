package panewire

import (
	"encoding/json"
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

// A containment entry's unknown keys survive a rewrite that clears the
// containment, so a later mode change does not erase data a newer release wrote.
func TestAcceptingOverridesRewriteKeepsContainmentLeftovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accepting.json")
	if err := os.WriteFile(path, []byte(`{"overrides":{"mac-work":"off"},"containments":{"mac-work":{"containment":"hk:7","containment_verified":true,"future":2}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	h := &HubServer{}
	h.relayControl.acceptingOverridesPath = path
	if err := h.persistAcceptingOverridesLocked(map[string]acceptingOverride{"mac-work": {Mode: "auto"}}); err != nil {
		t.Fatal(err)
	}
	_, ignored, err := loadAcceptingOverrides(path)
	if err != nil {
		t.Fatalf("rewritten override file refused: %v", err)
	}
	if !slices.Contains(ignored, "containments.mac-work.future") {
		t.Fatalf("containment leftover lost: ignored=%v", ignored)
	}
}

// Members match case-insensitively, as the loader decodes them: a file with
// OVERRIDES and Containments keeps its leftovers and is rewritten under the
// canonical names, with no duplicate member.
func TestAcceptingOverridesRewriteKeepsCaseVariantMembers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accepting.json")
	if err := os.WriteFile(path, []byte(`{"OVERRIDES":{"mac-work":{"mode":"on","note":"kept"}},"Containments":{"mac-work":{"containment":"hk:7","containment_verified":true,"future":2}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	h := &HubServer{}
	h.relayControl.acceptingOverridesPath = path
	next := map[string]acceptingOverride{"mac-work": {Mode: "off", Containment: "hk:7", ContainmentVerified: true}}
	if err := h.persistAcceptingOverridesLocked(next); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil || len(doc) != 2 || doc["overrides"] == nil || doc["containments"] == nil {
		t.Fatalf("rewritten root members: %s %v", raw, err)
	}
	overrides, ignored, err := loadAcceptingOverrides(path)
	if err != nil {
		t.Fatalf("rewritten override file refused: %v", err)
	}
	if want := []string{"containments.mac-work.future", "overrides.mac-work.note"}; !slices.Equal(ignored, want) {
		t.Fatalf("ignored=%v, want %v", ignored, want)
	}
	if got := overrides["mac-work"]; got.Mode != "off" || got.Containment != "hk:7" || !got.ContainmentVerified {
		t.Fatalf("mac-work=%+v", got)
	}
}

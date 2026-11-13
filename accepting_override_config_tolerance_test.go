package panewire

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
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

// A rewrite whose encoded file would exceed the loader limit is refused, and
// the existing file is left as it was. Marshaling escapes each '<' to six
// bytes, so a file under the limit can grow past it.
func TestAcceptingOverridesRewriteRefusesOversizedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accepting.json")
	original := []byte(`{"overrides":{"mac-work":"auto"},"future":"` + strings.Repeat("<", 20000) + `"}`)
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadAcceptingOverrides(path); err != nil {
		t.Fatalf("fixture refused: %v", err)
	}
	h := &HubServer{}
	h.relayControl.acceptingOverridesPath = path
	if err := h.persistAcceptingOverridesLocked(map[string]acceptingOverride{"mac-work": {Mode: "on"}}); !errors.Is(err, errAcceptingOverrideTooLarge) {
		t.Fatalf("persist err = %v, want errAcceptingOverrideTooLarge", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(after, original) {
		t.Fatal("refused rewrite changed the override file")
	}
}

// Two case variants of overrides that both define an unknown key for one
// machine keep the value of the later member in the file, which is the value
// the loader's decode keeps for the shared machine entry. The rewrite must not
// depend on map iteration order, so the check repeats the rewrite many times.
func TestAcceptingOverridesRewriteCaseVariantLeftoverFollowsSourceOrder(t *testing.T) {
	const original = `{"overrides":{"mac-work":{"mode":"on","note":"first"}},"OVERRIDES":{"mac-work":{"mode":"on","note":"second"}}}`
	next := map[string]acceptingOverride{"mac-work": {Mode: "on"}}
	for run := range 50 {
		path := filepath.Join(t.TempDir(), "accepting.json")
		if err := os.WriteFile(path, []byte(original), 0600); err != nil {
			t.Fatal(err)
		}
		h := &HubServer{}
		h.relayControl.acceptingOverridesPath = path
		if err := h.persistAcceptingOverridesLocked(next); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var doc struct {
			Overrides map[string]map[string]string `json:"overrides"`
		}
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("run %d: rewritten file %s: %v", run, raw, err)
		}
		if got := doc.Overrides["mac-work"]["note"]; got != "second" {
			t.Fatalf("run %d: kept note=%q, want %q (later member wins): %s", run, got, "second", raw)
		}
	}
}

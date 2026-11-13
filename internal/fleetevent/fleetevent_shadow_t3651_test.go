package fleetevent

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A non-finite quota figure on a waste_first choice is dropped, and the
// placement_shadow line is still written (encoding/json cannot write NaN).
func TestWasteFirstNonFiniteQuotaKeepsLine_T3651(t *testing.T) {
	dir := t.TempDir()
	w := New(dir, "panewire", time.Now)
	nan := math.NaN()
	inf := math.Inf(1)
	ev := Event{Kind: "placement_shadow", Outcome: "agree", Shadow: &PlacementShadow{
		Role: "implementer", WorkClass: "heavy",
		WasteFirst: &WasteFirst{
			Outcome:     "differs",
			Choice:      &ShadowChoice{Host: "cloud-tiny", AccountKey: "claude:00000000000000a1", UnusedAtResetPct: &nan, ResetHours: &inf},
			LeftUnspent: &ShadowAccount{Host: "cloud-tiny", AccountKey: "claude:00000000000000a1", UnusedAtResetPct: &inf},
		},
	}}
	if !w.EmitEvent(ev) {
		t.Fatal("the line was not written")
	}
	b, err := os.ReadFile(filepath.Join(dir, "events", FileName))
	if err != nil {
		t.Fatal(err)
	}
	line := string(b)
	if !strings.Contains(line, `"waste_first"`) || !strings.Contains(line, `"account_key":"claude:00000000000000a1"`) {
		t.Fatalf("line lacks the waste_first choice: %s", line)
	}
	if !json.Valid([]byte(strings.TrimSpace(line))) || strings.Contains(line, "NaN") || strings.Contains(line, "Inf") {
		t.Fatalf("the line is not valid JSON or carries a non-finite figure: %s", line)
	}
}

package panewire

import (
	"os"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// hk 4680: a Devin pane's explain reply names agent "devin" with no quota rule
// (herdr 0.9.5 has none), and the observer then classifies the real screen with
// that kind. The indented banner is a stop only with that kind.
func TestParseHerdrExplainDevinAgentT4680(t *testing.T) {
	rule, agent := parseHerdrExplain([]byte(`{"agent":"devin","matched_rule":null}`))
	if rule != "" || agent != "devin" {
		t.Fatalf("rule=%q agent=%q, want empty rule and devin", rule, agent)
	}
}

func TestDetectRealDevinScreenWithExplainAgentT4680(t *testing.T) {
	raw, err := os.ReadFile("internal/quotastop/testdata/devin_dot_cwd_screen_t4680.txt")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 15, 16, 48, 0, 0, time.UTC)
	stop, basis, ok := quotastop.Detect(string(raw), "devin", "", now, time.UTC)
	if !ok || basis != quotastop.BasisClassifier || stop.Pool != "devin-free" || stop.Reset.IsZero() {
		t.Fatalf("Detect(devin) = (%+v, %q, %v), want a devin-free classifier stop with a reset", stop, basis, ok)
	}
	if _, _, ok := quotastop.Detect(string(raw), "", "", now, time.UTC); ok {
		t.Fatal("Detect with no agent kind read the indented banner")
	}
}

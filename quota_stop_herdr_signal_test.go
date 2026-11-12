package panewire

import (
	"os"
	"testing"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// herdr 0.9.5 returns agent and matched_rule at the top level of
// `agent explain --json`; the result-wrapped shape stays accepted. Both
// fixtures carry the keys this code reads, and the rule id is the one
// quotastop.HerdrQuotaRule names (herdr 0.9.5 itself ships no such rule).
func TestParseHerdrExplainShapes(t *testing.T) {
	for _, tc := range []struct {
		name, file, rule, agent string
	}{
		{"top-level", "testdata/herdr-explain/top-level.json", quotastop.HerdrQuotaRule, "codex"},
		{"result-wrapped with object rule", "testdata/herdr-explain/wrapped.json", quotastop.HerdrQuotaRule, "codex"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := os.ReadFile(tc.file)
			if err != nil {
				t.Fatal(err)
			}
			rule, agent := parseHerdrExplain(out)
			if rule != tc.rule || agent != tc.agent {
				t.Fatalf("rule=%q agent=%q, want rule=%q agent=%q", rule, agent, tc.rule, tc.agent)
			}
		})
	}
}

func TestParseHerdrExplainNoMatchedRuleKeepsAgent(t *testing.T) {
	rule, agent := parseHerdrExplain([]byte(`{"agent":"claude","matched_rule":null}`))
	if rule != "" || agent != "claude" {
		t.Fatalf("rule=%q agent=%q, want empty rule and claude", rule, agent)
	}
}

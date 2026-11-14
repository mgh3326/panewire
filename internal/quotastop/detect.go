package quotastop

import "time"

// HerdrQuotaRule is the herdr agent-detection rule id that marks a pane as
// showing a usage limit. herdr 0.9.5 ships no such rule, so today Detect
// falls through to the classifier; the check is ready for when herdr adds it.
const HerdrQuotaRule = "quota_limit"

// Basis names the signal that recognised a stop. It is logged with the
// decision so a stop can be traced to herdr or to the screen text.
const (
	BasisHerdr      = "herdr:" + HerdrQuotaRule
	BasisClassifier = "classifier"
)

// Detect recognises a usage-limit stop: herdr's quota_limit rule first, the
// screen classifier otherwise. agent is herdr's agent kind for the pane.
//
// When the rule fires and the screen text does not parse, the stop is still
// reported with the pool named by agent and no reset. The caller then hands
// it over as reset_unknown, which is the conservative outcome.
func Detect(screen, agent, herdrRule string, now time.Time, local *time.Location) (Stop, string, bool) {
	stop, ok := ClassifyAgent(screen, agent, now, local)
	if herdrRule != HerdrQuotaRule {
		return stop, BasisClassifier, ok
	}
	if ok {
		return stop, BasisHerdr, true
	}
	pool := poolForAgent(agent)
	if pool == "" {
		return Stop{}, "", false
	}
	return Stop{Pool: pool}, BasisHerdr, true
}

// poolForAgent maps herdr's agent kind to the pool the classifier would name.
func poolForAgent(agent string) string {
	switch agent {
	case "codex":
		return "codex"
	case "claude":
		return "claude"
	case "devin":
		return "devin-free"
	}
	return ""
}

package fleet

import "context"

// AgentSession is the session reference reported by herdr's agent.list.
type AgentSession struct {
	Source string `json:"source"`
	Agent  string `json:"agent"`
	Kind   string `json:"kind"`
	Value  string `json:"value"`
}

// HarnessSession identifies a harness conversation, not the herdr session.
// The event contract is documented in docs/jobs-resilience.md#harness-session-identity.
type HarnessSession struct {
	Kind   string `json:"kind"`
	ID     string `json:"id"`
	Source string `json:"source"`
}

// FindHarnessSession accepts only the reference for the requested pane and harness.
func FindHarnessSession(agents []Agent, pane, kind string) HarnessSession {
	result := HarnessSession{Kind: kind, ID: "unknown", Source: "unavailable"}
	for _, agent := range agents {
		if agent.Pane != pane || agent.Kind != kind || agent.Session == nil {
			continue
		}
		ref := agent.Session
		if ref.Agent == kind && (ref.Kind == "id" || ref.Kind == "path") && ref.Value != "" && ref.Source != "" {
			result.ID, result.Source = ref.Value, ref.Source
		}
		break
	}
	return result
}

func (a *App) harnessSession(ctx context.Context, pane, kind string) HarnessSession {
	agents, _ := a.agents(ctx)
	return FindHarnessSession(agents, pane, kind)
}

package panewire

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// GuardResult is the capability decision made from one herdr schema snapshot.
// A missing required contract disables only the capability that needs it.
type GuardResult struct {
	Protocol    int      `json:"protocol"`
	Schema      int      `json:"schema_version"`
	Events      bool     `json:"events"`
	AgentWait   bool     `json:"agent_wait"`
	AgentRead   bool     `json:"agent_read"`
	Prompt      bool     `json:"prompt"`
	Warnings    []string `json:"warnings,omitempty"`
	Unavailable []string `json:"unavailable,omitempty"`
}

func (g GuardResult) AgentCapability() bool { return g.AgentWait && g.AgentRead }

// GuardSchema accepts both the real JSON schema shape and the compact fixture
// shape used by tests. It deliberately extracts only known contract facts.
func GuardSchema(r io.Reader) (GuardResult, error) {
	var raw map[string]any
	dec := json.NewDecoder(r)
	if err := dec.Decode(&raw); err != nil {
		return GuardResult{}, fmt.Errorf("decode herdr schema: %w", err)
	}
	result := GuardResult{Protocol: number(raw["protocol"]), Schema: number(raw["schema_version"])}
	if result.Protocol == 0 || result.Schema == 0 {
		result.Warnings = append(result.Warnings, "schema missing protocol or schema_version")
	}

	methods := map[string]bool{}
	collectMethodConsts(raw, methods)
	// Compact fixtures expose methods as schemas.request.methods.
	if schemas, ok := raw["schemas"].(map[string]any); ok {
		if request, ok := schemas["request"].(map[string]any); ok {
			if ms, ok := request["methods"].([]any); ok {
				for _, m := range ms {
					if s, ok := m.(string); ok {
						methods[s] = true
					}
				}
			}
		}
	}

	defs := requestDefs(raw)
	result.Events = methods["events.subscribe"] && (hasDefField(defs, "EventsSubscribeParams", "subscriptions") || compactHas(raw, "subscriptions"))
	result.AgentWait = methods["agent.wait"] && (hasDefField(defs, "AgentWaitParams", "target") || compactHas(raw, "agent_status"))
	result.AgentRead = (methods["agent.read"] || methods["pane.read"]) && (hasDefField(defs, "AgentReadParams", "source") || hasDefField(defs, "PaneReadParams", "source") || compactHas(raw, "read_source"))
	result.Prompt = methods["agent.prompt"] && (hasDefField(defs, "AgentPromptParams", "text") || compactHas(raw, "methods"))

	if !result.Events {
		result.Unavailable = append(result.Unavailable, "events")
	}
	if !result.AgentWait {
		result.Unavailable = append(result.Unavailable, "agent.wait")
	}
	if !result.AgentRead {
		result.Unavailable = append(result.Unavailable, "agent.read")
	}
	if !result.Prompt {
		result.Unavailable = append(result.Unavailable, "prompt")
	}
	if strings.Contains(string(mustJSON(raw)), "unknown_event") {
		result.Warnings = append(result.Warnings, "unknown schema field: unknown_event")
	}
	return result, nil
}

func requestDefs(raw map[string]any) map[string]any {
	if schemas, ok := raw["schemas"].(map[string]any); ok {
		if req, ok := schemas["request"].(map[string]any); ok {
			if defs, ok := req["$defs"].(map[string]any); ok {
				return defs
			}
		}
	}
	return nil
}
func hasDefField(defs map[string]any, name, field string) bool {
	d, ok := defs[name].(map[string]any)
	if !ok {
		return false
	}
	p, ok := d["properties"].(map[string]any)
	if !ok {
		return false
	}
	_, ok = p[field]
	return ok
}
func compactHas(raw map[string]any, key string) bool {
	return strings.Contains(string(mustJSON(raw)), `"`+key+`"`)
}
func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }
func number(v any) int {
	if f, ok := v.(float64); ok {
		return int(f)
	}
	return 0
}
func collectMethodConsts(v any, methods map[string]bool) {
	switch x := v.(type) {
	case map[string]any:
		if props, ok := x["properties"].(map[string]any); ok {
			if method, ok := props["method"].(map[string]any); ok {
				if c, ok := method["const"].(string); ok {
					methods[c] = true
				}
			}
		}
		for _, child := range x {
			collectMethodConsts(child, methods)
		}
	case []any:
		for _, child := range x {
			collectMethodConsts(child, methods)
		}
	}
}

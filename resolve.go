package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// resolveTarget is the single pane-target resolver for `prompt --to` and
// `wait --agent` (#497): the same input string resolves to the same pane from
// either command. Matching is exact-only — partial match is deliberately not
// offered here (`sessions find --contains` covers fleet search), so the
// not-found error says "exact" rather than reading as "does not exist".
//
// Two precedence tiers mirror wrk find's contract (name before tab label):
// tier 1 identity fields (pane_id, agent, name), tier 2 label fields (tab
// label, tab_id, title). Candidates dedupe by pane_id — two agent rows on one
// pane are one target. More than one distinct pane in a tier is ambiguous:
// every candidate is listed and resolution fails; nothing is auto-selected.
// Inputs outside wrk's name/label domain (pane_id, tab_id, title, harness
// name) are panewire extensions wrk find reports as "no exact name or label
// match".
func resolveTarget(ctx context.Context, c *HerdrClient, target string) (paneIdentity, error) {
	raw, err := c.Call(ctx, "agent.list", map[string]any{})
	if err != nil {
		return paneIdentity{}, &codedError{ExitDaemonUnavailable, err}
	}
	var top struct {
		Agents []map[string]any `json:"agents"`
	}
	if json.Unmarshal(raw, &top) != nil {
		return paneIdentity{}, &codedError{ExitConditionInvalid, fmt.Errorf("invalid herdr agent list")}
	}
	labels := tabLabels(ctx, c)
	var identity, labeled []paneIdentity
	for _, a := range top.Agents {
		p := identityFromMap(a)
		// displayLabel is the tab.list label, wrk find's only label source.
		// It is matched on its own so a record label cannot shadow it.
		displayLabel := labels[p.TabID]
		if p.Label == "" {
			p.Label = displayLabel
		}
		if p.PaneID == target || p.Agent == target || p.Name == target {
			identity = append(identity, p)
		} else if p.Label == target || displayLabel == target || p.TabID == target || p.Title == target || aString(a, "tab_label") == target {
			labeled = append(labeled, p)
		}
	}
	if pane, found, err := uniqueTarget(identity, "identity", target); found || err != nil {
		return pane, err
	}
	if pane, found, err := uniqueTarget(labeled, "label", target); found || err != nil {
		return pane, err
	}
	return paneIdentity{}, &codedError{ExitConditionInvalid, fmt.Errorf("no exact match for agent target %q (targets match pane_id, name, or label exactly)", target)}
}

// uniqueTarget applies the ambiguous-means-fail rule to one tier: zero
// candidates falls through, exactly one resolves, and several distinct panes
// fail with every candidate spelled out.
func uniqueTarget(candidates []paneIdentity, tier, target string) (paneIdentity, bool, error) {
	seen := map[string]bool{}
	unique := make([]paneIdentity, 0, len(candidates))
	for _, p := range candidates {
		if p.PaneID != "" && seen[p.PaneID] {
			continue
		}
		seen[p.PaneID] = true
		unique = append(unique, p)
	}
	if len(unique) == 0 {
		return paneIdentity{}, false, nil
	}
	if len(unique) > 1 {
		sort.Slice(unique, func(i, j int) bool { return unique[i].PaneID < unique[j].PaneID })
		desc := make([]string, 0, len(unique))
		for _, p := range unique {
			desc = append(desc, fmt.Sprintf("pane_id=%s name=%s label=%s agent=%s status=%s", orDash(p.PaneID), orDash(p.Name), orDash(p.Label), orDash(p.Agent), orDash(p.Status)))
		}
		return paneIdentity{}, false, &codedError{ExitConditionInvalid, fmt.Errorf("ambiguous agent target %q (%d %s candidates): %s", target, len(unique), tier, strings.Join(desc, "; "))}
	}
	return unique[0], true, nil
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// agentSessionValue reads the discriminating session id out of an agent
// record's agent_session field, which current herdr sends as an object
// ({kind,source,value}) and older snapshots carried as a plain string.
func agentSessionValue(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case map[string]any:
		return aString(s, "value")
	}
	return ""
}

// paneOccupant reports whether paneID still holds an agent and, when herdr
// reports one, that agent's session id. wait uses it to pin a resolved target
// to the same occupant instead of trusting a bare pane_id forever.
func paneOccupant(ctx context.Context, c *HerdrClient, paneID string) (session string, occupied bool, err error) {
	raw, err := c.Call(ctx, "agent.list", map[string]any{})
	if err != nil {
		return "", false, err
	}
	var top struct {
		Agents []map[string]any `json:"agents"`
	}
	if json.Unmarshal(raw, &top) != nil {
		return "", false, fmt.Errorf("invalid herdr agent list")
	}
	for _, a := range top.Agents {
		if aString(a, "pane_id") == paneID {
			return agentSessionValue(a["agent_session"]), true, nil
		}
	}
	return "", false, nil
}

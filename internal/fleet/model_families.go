package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

const modelFamiliesDocKey = "policy/model-families"

// ModelFamiliesPolicy is the machine-readable v1 block of
// handoffkeep's policy/model-families document.
type ModelFamiliesPolicy struct {
	Version   int                    `json:"version"`
	CountedBy string                 `json:"counted_by"`
	Families  map[string]ModelFamily `json:"families"`
	Ladder    json.RawMessage        `json:"ladder"`
	Consult   json.RawMessage        `json:"consult"`
}

// ModelFamily describes a quota-pool family. EntryEffort is the ladder entry
// for a model; launch resolves effort from the selected live catalog row.
type ModelFamily struct {
	Harness          string            `json:"harness"`
	Billing          string            `json:"billing"`
	Models           []string          `json:"models"`
	EffortVia        string            `json:"effort_via"`
	Efforts          []string          `json:"efforts"`
	LadderCap        string            `json:"ladder_cap"`
	LadderCapByModel map[string]string `json:"ladder_cap_by_model,omitempty"`
	EntryEffort      string            `json:"entry_effort,omitempty"`
}

// parseModelFamiliesDoc reads the machine-readable v1 block from the HK doc.
func parseModelFamiliesDoc(body string) (ModelFamiliesPolicy, error) {
	_, rest, ok := strings.Cut(body, "```json")
	if !ok {
		return ModelFamiliesPolicy{}, fmt.Errorf("no json block")
	}
	block, _, ok := strings.Cut(rest, "```")
	if !ok {
		return ModelFamiliesPolicy{}, fmt.Errorf("unterminated json block")
	}
	return parseModelFamiliesJSON([]byte(block))
}

func parseModelFamiliesJSON(b []byte) (ModelFamiliesPolicy, error) {
	var p ModelFamiliesPolicy
	if err := uniqueJSON(b); err != nil {
		return p, err
	}
	if err := json.Unmarshal(b, &p); err != nil {
		return p, err
	}
	if p.Version != 1 {
		return p, fmt.Errorf("unsupported model families policy version %d", p.Version)
	}
	if p.CountedBy != "pool" || len(p.Families) == 0 {
		return p, fmt.Errorf("model families policy needs pool counting and families")
	}
	for pool, family := range p.Families {
		if pool == "" || family.Harness == "" || family.Billing == "" || len(family.Models) == 0 {
			return p, fmt.Errorf("model family %q is incomplete", pool)
		}
		switch family.EffortVia {
		case "flag", "profile_suffix", "none":
		default:
			return p, fmt.Errorf("model family %s has unknown effort_via %q", pool, family.EffortVia)
		}
		if family.EntryEffort != "" && !slices.Contains(family.Efforts, family.EntryEffort) {
			return p, fmt.Errorf("model family %s has unknown entry_effort %q", pool, family.EntryEffort)
		}
	}
	return p, nil
}

// modelFamiliesPolicy reads the canonical family policy from handoffkeep.
func (a *App) modelFamiliesPolicy(ctx context.Context) (ModelFamiliesPolicy, error) {
	if a.modelFamilies != nil {
		return a.modelFamilies(ctx)
	}
	doc, err := hkGetDocument(ctx, modelFamiliesDocKey)
	if err != nil {
		return ModelFamiliesPolicy{}, fmt.Errorf("hk doc get %s: %s", modelFamiliesDocKey, clip(strings.TrimSpace(err.Error()), 200))
	}
	p, err := parseModelFamiliesDoc(doc.Body)
	if err != nil {
		return ModelFamiliesPolicy{}, fmt.Errorf("unreadable %s: %s", modelFamiliesDocKey, err)
	}
	return p, nil
}

// familyForPool uses the policy's pool family where it has one; a catalog
// family remains authoritative for pools the policy does not describe.
func (p ModelFamiliesPolicy) familyForPool(pool, catalogFamily string) string {
	if _, ok := p.Families[pool]; ok {
		return pool
	}
	return catalogFamily
}

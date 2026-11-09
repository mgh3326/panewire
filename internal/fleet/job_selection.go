package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
)

const maxJobSelectionBytes = 32 * 1024

func jobSelectionSource(s Spawn) string {
	switch {
	case s.Delegated:
		return "delegate"
	case s.Host != "" && s.Host != "auto" && s.Host != "local":
		return "forced"
	case s.placement != nil && s.placement.plan != nil:
		return "score"
	default:
		return "local"
	}
}

// jobSelection maps the private placement trace onto the small, safe hk
// selection contract. Candidate arrays keep their trace order, with the
// chosen entry promoted to the front before the byte bound is applied.
func jobSelection(s Spawn, source string) map[string]any {
	selection := map[string]any{}
	if s.Sel != nil {
		if chosen, ok := s.Sel["chosen"].(map[string]any); ok {
			selection["chosen"] = copySelectionFields(chosen, "why", "grade", "tier", "projected_pct", "reset_in_s", "waste_urgency", "model_id")
		}
		if urgency := s.Sel["urgency_source"]; urgency != nil {
			selection["policy"] = map[string]any{"urgency_source": urgency}
		}
		if candidates, ok := s.Sel["candidates"].([]any); ok {
			selection["candidates"] = jobSelectionCandidates(candidates, s.Sel["chosen"])
		}
	}
	if s.OverrideReason != "" {
		selection["override_reason"] = s.OverrideReason
	}
	host := map[string]any{"chosen": firstNonempty(func() string {
		if s.placement != nil {
			return s.placement.execution.Host
		}
		return ""
	}(), s.Host, "local")}
	if host["chosen"] == "local" {
		host["chosen"] = firstNonempty(s.Host, "local")
	}
	host["source"] = source
	if s.placement != nil && s.placement.plan != nil {
		plan := s.placement.plan
		rows := make([]any, 0, len(plan.candidates))
		chosen := host["chosen"].(string)
		appendCandidate := func(c placeCandidate) {
			row := map[string]any{"machine": c.name}
			outcome := c.outcome
			if c.excluded != "" {
				outcome = "excluded:" + c.excluded
			} else if c.preflight != nil {
				for _, check := range c.preflight.Checks {
					if !check.OK && check.Reason != "" {
						outcome = "excluded:" + check.Reason
						break
					}
				}
			} else if outcome == "" && c.name == chosen {
				outcome = "admitted"
			} else if outcome == "" {
				outcome = "below_choice"
			}
			if outcome != "" {
				row["outcome"] = outcome
			}
			rows = append(rows, row)
		}
		for _, c := range plan.candidates {
			if c.name == chosen {
				appendCandidate(c)
				break
			}
		}
		for _, c := range plan.candidates {
			if c.name != chosen {
				appendCandidate(c)
			}
		}
		host["candidates"] = rows
	}
	selection["host"] = host
	trimJobSelection(selection)
	return selection
}

func copySelectionFields(src map[string]any, keys ...string) map[string]any {
	out := map[string]any{}
	for _, key := range keys {
		if value, ok := src[key]; ok && value != nil {
			out[key] = value
		}
	}
	return out
}

func jobSelectionCandidates(in []any, chosenValue any) []any {
	out := make([]any, 0, len(in))
	chosen, _ := chosenValue.(map[string]any)
	chosenID := firstNonempty(str(chosen, "seat"), str(chosen, "canon"), str(chosen, "model_id"))
	for _, item := range in {
		candidate, ok := item.(map[string]any)
		if !ok {
			continue
		}
		row := copySelectionFields(candidate, "seat", "canon", "model_id", "outcome")
		if len(row) > 0 {
			out = append(out, row)
		}
	}
	for i, row := range out {
		candidate := row.(map[string]any)
		if firstNonempty(str(candidate, "seat"), str(candidate, "canon"), str(candidate, "model_id")) == chosenID && chosenID != "" {
			copy(out[1:i+1], out[0:i])
			out[0] = row
			break
		}
	}
	return out
}

func trimJobSelection(selection map[string]any) {
	encoded, _ := json.Marshal(selection)
	for len(encoded) > maxJobSelectionBytes {
		trimmed := false
		for _, key := range []string{"candidates"} {
			rows, ok := selection[key].([]any)
			if ok && len(rows) > 1 {
				selection[key] = rows[:len(rows)-1]
				trimmed = true
				break
			}
		}
		if !trimmed {
			if host, ok := selection["host"].(map[string]any); ok {
				if rows, ok := host["candidates"].([]any); ok && len(rows) > 1 {
					host["candidates"] = rows[:len(rows)-1]
					trimmed = true
				}
			}
		}
		if !trimmed {
			// Internal reasons are short, but keep the wire guard absolute if a
			// future trace introduces an unexpectedly large chosen field.
			if chosen, ok := selection["chosen"].(map[string]any); ok {
				for _, key := range []string{"why", "model_id"} {
					if _, exists := chosen[key]; exists {
						delete(chosen, key)
						trimmed = true
						break
					}
				}
			}
			if !trimmed {
				if _, exists := selection["override_reason"]; exists {
					delete(selection, "override_reason")
					trimmed = true
				}
			}
			if !trimmed {
				if _, ok := selection["candidates"]; ok {
					delete(selection, "candidates")
					trimmed = true
				}
			}
			if !trimmed {
				if host, ok := selection["host"].(map[string]any); ok {
					if _, ok := host["candidates"]; ok {
						delete(host, "candidates")
						trimmed = true
					}
				}
			}
			if !trimmed {
				if _, ok := selection["policy"]; ok {
					delete(selection, "policy")
					trimmed = true
				}
			}
			if !trimmed {
				if _, ok := selection["chosen"]; ok {
					delete(selection, "chosen")
					trimmed = true
				}
			}
			if !trimmed {
				host, _ := selection["host"].(map[string]any)
				clear(selection)
				selection["host"] = host
				trimmed = true
			}
		}
		if !trimmed {
			break
		}
		encoded, _ = json.Marshal(selection)
	}
}

func putSpawnedJobRecord(ctx context.Context, a *App, id string, rec map[string]any) {
	if err := a.putJobRecord(ctx, id, rec); err == nil {
		return
	} else if _, hasSelection := rec["selection"]; !hasSelection || !isBadRequest(err) {
		a.jobRecordFailed(ctx, id, "spawned", err)
		return
	} else {
		a.jobRecordFailed(ctx, id, "selection", err)
		fallback := make(map[string]any, len(rec)-1)
		for key, value := range rec {
			if key != "selection" {
				fallback[key] = value
			}
		}
		if retryErr := a.putJobRecord(ctx, id, fallback); retryErr != nil {
			a.jobRecordFailed(ctx, id, "spawned", retryErr)
		}
	}
}

func isBadRequest(err error) bool {
	var backend *BackendError
	return errors.As(err, &backend) && backend.Status == http.StatusBadRequest
}

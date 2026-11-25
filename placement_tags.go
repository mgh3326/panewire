package panewire

import (
	"errors"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"
)

var placementTagPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

func parsePlacementTags(raw string) ([]string, error) {
	if raw == "" {
		return nil, nil
	}
	if len(raw) > 16*64+15 {
		return nil, errors.New("at most 16 tags of 64 characters")
	}
	parts := strings.Split(raw, ",")
	if len(parts) > 16 {
		return nil, errors.New("at most 16 tags")
	}
	tags := make([]string, 0, len(parts))
	for _, part := range parts {
		tag := strings.TrimSpace(part)
		if !placementTagPattern.MatchString(tag) {
			return nil, errors.New("tags must contain lowercase letters, digits, dot, underscore or hyphen")
		}
		tags = append(tags, tag)
	}
	slices.Sort(tags)
	return slices.Compact(tags), nil
}
func validPlacementHostTags(tags map[string][]string) bool {
	if len(tags) > 128 {
		return false
	}
	for machine, values := range tags {
		if !machineIDPattern.MatchString(machine) || machine == hubOperatorMachineID || len(values) > 16 {
			return false
		}
		seen := map[string]bool{}
		for _, tag := range values {
			if !placementTagPattern.MatchString(tag) || seen[tag] {
				return false
			}
			seen[tag] = true
		}
	}
	return true
}
func placementTagsMatch(have, required []string) bool {
	for _, tag := range required {
		if !slices.Contains(have, tag) {
			return false
		}
	}
	return true
}

// A tagged query is a load-ranked recommendation for movable work. Keep the
// legacy policy-order query unchanged, and never widen a requested capability
// pool to the local fallback, an unknown host, or a disconnected wake target.
func (h *HubServer) makeTaggedPlacement(policy PlacementPolicy, metrics placementMetrics, source string, now time.Time, tags []string) PlacementResult {
	candidates := h.placementCandidates(policy, metrics, source, now)
	eligible := make([]PlacementCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if placementTagsMatch(policy.Tags[candidate.Machine], tags) && placementUsable(candidate, policy, source) {
			eligible = append(eligible, candidate)
		}
	}
	sort.SliceStable(eligible, func(i, j int) bool { return eligible[i].Score > eligible[j].Score })
	reason := "no_tagged_capacity"
	if len(eligible) > 0 {
		reason = ""
	}
	// A tagged request takes the highest score instead of a policy-order
	// winner, but the load ceiling still applies: a host above it is left out
	// while another tagged host is at or under it (operator decision 515).
	if k := policy.scoreK(); k.loadCeiling > 0 && loadCeilingApplies(eligible, k.loadCeiling) {
		best := eligible[0].Machine
		kept := eligible[:0]
		for _, candidate := range eligible {
			if !loadOverCeiling(candidate, k.loadCeiling) {
				kept = append(kept, candidate)
			}
		}
		if kept[0].Machine != best {
			reason = "load_over_ceiling"
		}
		eligible = kept
	}
	result := PlacementResult{Decision: "unavailable", Candidates: eligible, Source: source, Asof: now, Reason: reason}
	if len(eligible) > 0 {
		result.Decision = eligible[0].Machine
	}
	return result
}

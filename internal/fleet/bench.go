package fleet

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
)

// Rep is one bench rep. The wire fields match pacelever's Rep so mergewake can
// switch binaries without changing its call.
type Rep struct {
	FirstCIGreen       *bool          `json:"first_ci_green,omitempty"`
	FindingsBySeverity map[string]int `json:"findings_by_severity"`
	RevertedWithin7d   *bool          `json:"reverted_within_7d,omitempty"`
	TimeToMergeS       *float64       `json:"time_to_merge_s,omitempty"`
	ExperimentID       string         `json:"experiment_id,omitempty"`
	ExperimentArm      string         `json:"experiment_arm,omitempty"`
	OutcomeFinalized   *bool          `json:"outcome_finalized,omitempty"`
	OutcomeDecision    string         `json:"outcome_decision,omitempty"`
	LocalID            int64          `json:"local_id,omitempty"`
	ID                 int64          `json:"id,omitempty"`
	OriginID           int64          `json:"origin_id"`
	Profile            string         `json:"profile"`
	ModelID            string         `json:"model_id"`
	TaskRef            string         `json:"task_ref"`
	Tier               string         `json:"tier"`
	Role               string         `json:"role"`
	Rounds             int            `json:"rounds"`
	BlockersFound      int            `json:"blockers_found"`
	Completed          *int           `json:"completed"`
	InputTokens        *int64         `json:"input_tokens"`
	OutputTokens       *int64         `json:"output_tokens"`
	Notes              string         `json:"notes"`
	RecordedAt         string         `json:"recorded_at"`
	Effort             string         `json:"effort"`
	Grade              string         `json:"grade"`
	TableGrade         string         `json:"table_grade"`
	Pending            bool           `json:"pending,omitempty"`

	CriticalFindings *int  `json:"critical_findings,omitempty"`
	HighFindings     *int  `json:"high_findings,omitempty"`
	Merged           *bool `json:"merged,omitempty"`
	SpawnToMergeS    *int  `json:"spawn_to_merge_s,omitempty"`
}

var repGrades = func() map[string]int {
	grades := gradeOrderKeys()
	reps := make(map[string]int, len(grades)+1)
	for rank := range grades {
		reps[grades[len(grades)-1-rank]] = rank
	}
	reps[gradeUnrated] = len(grades)
	return reps
}()

func validRepGrade(grade string) bool {
	_, ok := repGrades[grade]
	return ok
}

func repGradeRank(grade string) int {
	if rank, ok := repGrades[grade]; ok {
		return rank
	}
	return 1 << 30
}

func normalizeRepKey(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

// repWire is pacelever's exact 17-key rep envelope: outcome fields travel inside
// notes so an older bench server keeps them.
func repWire(r Rep) map[string]any {
	return map[string]any{
		"origin_id": r.OriginID, "profile": r.Profile, "model_id": r.ModelID,
		"task_ref": r.TaskRef, "tier": r.Tier, "role": r.Role,
		"rounds": r.Rounds, "blockers_found": r.BlockersFound,
		"completed": r.Completed, "input_tokens": r.InputTokens,
		"output_tokens": r.OutputTokens, "notes": repNotesForWire(r),
		"recorded_at": r.RecordedAt, "effort": r.Effort,
		"grade": r.Grade, "table_grade": r.TableGrade,
	}
}

// outcomeWire adds the schema-40 outcome columns for panewire's own automatic
// records. mergewake's reps add keeps the older envelope byte for byte.
func outcomeWire(r Rep) map[string]any {
	wire := repWire(r)
	if r.CriticalFindings != nil {
		wire["critical_findings"] = *r.CriticalFindings
	}
	if r.HighFindings != nil {
		wire["high_findings"] = *r.HighFindings
	}
	if r.Merged != nil {
		wire["merged"] = *r.Merged
	}
	if r.SpawnToMergeS != nil {
		wire["spawn_to_merge_s"] = *r.SpawnToMergeS
	}
	return wire
}

func repNotesForWire(r Rep) string {
	if !hasRepOutcomeFields(r) && r.ExperimentID == "" && r.ExperimentArm == "" {
		return r.Notes
	}
	raw := map[string]any{}
	if json.Unmarshal([]byte(r.Notes), &raw) != nil || raw == nil {
		raw = map[string]any{}
		if r.Notes != "" {
			raw["text"] = r.Notes
		}
	}
	if r.FirstCIGreen != nil {
		raw["first_ci_green"] = *r.FirstCIGreen
	}
	if r.FindingsBySeverity != nil {
		raw["findings_by_severity"] = r.FindingsBySeverity
	}
	if r.RevertedWithin7d != nil {
		raw["reverted_within_7d"] = *r.RevertedWithin7d
	}
	if r.TimeToMergeS != nil {
		raw["time_to_merge_s"] = *r.TimeToMergeS
	}
	if r.ExperimentID != "" {
		raw["experiment_id"] = r.ExperimentID
	}
	if r.ExperimentArm != "" {
		raw["experiment_arm"] = r.ExperimentArm
	}
	if r.OutcomeFinalized != nil {
		raw["finalized"] = *r.OutcomeFinalized
	}
	if r.OutcomeDecision != "" {
		raw["decision"] = r.OutcomeDecision
	}
	data, _ := json.Marshal(raw)
	return string(data)
}

func hasRepOutcomeFields(r Rep) bool {
	return r.FirstCIGreen != nil || r.FindingsBySeverity != nil || r.RevertedWithin7d != nil ||
		r.TimeToMergeS != nil || r.OutcomeFinalized != nil || r.OutcomeDecision != ""
}

func hydrateRepOutcomes(r *Rep) error {
	note := strings.TrimSpace(r.Notes)
	if strings.HasPrefix(note, "{") {
		var raw map[string]json.RawMessage
		if json.Unmarshal([]byte(note), &raw) != nil {
			return fmt.Errorf("invalid structured notes JSON")
		}
		sources := []map[string]json.RawMessage{raw}
		for _, key := range []string{"outcome", "outcomes"} {
			if data := raw[key]; len(data) > 0 && string(data) != "null" {
				var nested map[string]json.RawMessage
				if json.Unmarshal(data, &nested) != nil {
					return fmt.Errorf("invalid structured outcome object")
				}
				sources = append(sources, nested)
			}
		}
		for sourceIndex, source := range sources {
			for _, key := range []string{"first_ci_green", "reverted_within_7d"} {
				data, ok := source[key]
				if !ok || string(data) == "null" {
					continue
				}
				var value bool
				if json.Unmarshal(data, &value) != nil {
					return fmt.Errorf("invalid outcome field %s", key)
				}
				target := &r.FirstCIGreen
				if key == "reverted_within_7d" {
					target = &r.RevertedWithin7d
				}
				if *target != nil && **target != value {
					return fmt.Errorf("conflicting outcome field %s", key)
				}
				*target = &value
			}
			if data, ok := source["findings_by_severity"]; ok && string(data) != "null" {
				var findings map[string]int
				if json.Unmarshal(data, &findings) != nil || findings == nil {
					return fmt.Errorf("invalid findings_by_severity map")
				}
				normalized := map[string]int{}
				for severity, count := range findings {
					key := normalizeRepKey(severity)
					if key == "" || strings.ContainsAny(key, "\r\n\x00") || len(key) > 64 || count < 0 || count > 1000000 {
						return fmt.Errorf("invalid findings_by_severity entry")
					}
					normalized[key] += count
				}
				if r.FindingsBySeverity != nil && !reflect.DeepEqual(r.FindingsBySeverity, normalized) {
					return fmt.Errorf("conflicting findings_by_severity")
				}
				r.FindingsBySeverity = normalized
			}
			if data, ok := source["time_to_merge_s"]; ok && string(data) != "null" {
				var value float64
				if json.Unmarshal(data, &value) != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
					return fmt.Errorf("invalid time_to_merge_s")
				}
				if r.TimeToMergeS != nil && *r.TimeToMergeS != value {
					return fmt.Errorf("conflicting time_to_merge_s")
				}
				r.TimeToMergeS = &value
			}
			for _, key := range []string{"experiment_id", "experiment_arm", "arm"} {
				data, ok := source[key]
				if !ok || string(data) == "null" {
					continue
				}
				var value string
				if json.Unmarshal(data, &value) != nil {
					return fmt.Errorf("invalid outcome field %s", key)
				}
				target := &r.ExperimentID
				if key != "experiment_id" {
					target = &r.ExperimentArm
				}
				if *target != "" && value != "" && *target != value {
					return fmt.Errorf("conflicting outcome field %s", key)
				}
				if value != "" {
					*target = value
				}
			}
			if sourceIndex > 0 && hasRepOutcomeFields(*r) {
				for _, key := range []string{"profile", "model", "model_id", "effort", "task_ref"} {
					data, exists := source[key]
					if !exists || string(data) == "null" {
						continue
					}
					var value string
					if json.Unmarshal(data, &value) != nil {
						return fmt.Errorf("invalid outcome attribution field %s", key)
					}
					if value == "" {
						continue
					}
					expected := ""
					switch key {
					case "profile":
						expected = r.Profile
					case "model", "model_id":
						expected = r.ModelID
					case "effort":
						expected = r.Effort
						if expected == "" {
							r.Effort = value
						}
					case "task_ref":
						expected = r.TaskRef
						if normalizeRepKey(value) == normalizeRepKey(expected) {
							continue
						}
					}
					if expected != "" && expected != value {
						return fmt.Errorf("outcome attribution conflicts with rep field %s", key)
					}
				}
			}
			if data, ok := source["finalized"]; ok && string(data) != "null" {
				var value bool
				if json.Unmarshal(data, &value) != nil {
					return fmt.Errorf("invalid finalized outcome field")
				}
				if r.OutcomeFinalized != nil && *r.OutcomeFinalized != value {
					return fmt.Errorf("conflicting finalized outcome field")
				}
				r.OutcomeFinalized = &value
			}
			if data, ok := source["decision"]; ok && string(data) != "null" {
				var value string
				if json.Unmarshal(data, &value) != nil {
					return fmt.Errorf("invalid outcome decision")
				}
				r.OutcomeDecision = value
			}
		}
	}
	return validateRepOutcomes(*r)
}

func validateRepOutcomes(r Rep) error {
	if r.ExperimentArm != "" && r.ExperimentArm != "baseline" && r.ExperimentArm != "downshift" {
		return fmt.Errorf("experiment_arm must be baseline or downshift")
	}
	if len(r.ExperimentID) > 128 || strings.ContainsAny(r.ExperimentID, "\r\n\t\x00 ") {
		return fmt.Errorf("invalid experiment_id")
	}
	if r.ExperimentArm == "downshift" && r.ExperimentID == "" {
		return fmt.Errorf("downshift requires experiment_id")
	}
	if r.TimeToMergeS != nil && (math.IsNaN(*r.TimeToMergeS) || math.IsInf(*r.TimeToMergeS, 0) || *r.TimeToMergeS < 0) {
		return fmt.Errorf("invalid time_to_merge_s")
	}
	for severity, count := range r.FindingsBySeverity {
		if normalizeRepKey(severity) == "" || len(severity) > 64 || strings.ContainsAny(severity, "\r\n\x00") || count < 0 || count > 1000000 {
			return fmt.Errorf("invalid findings_by_severity entry")
		}
	}
	return nil
}

func severeRepFinding(severity string) bool {
	switch normalizeRepKey(severity) {
	case "blocking", "blocker", "critical", "high", "major", "p0", "p1", "s0", "s1":
		return true
	}
	return false
}

func repOutcomeClass(r Rep) string {
	if !hasRepOutcomeFields(r) {
		return "legacy"
	}
	if strings.Contains(strings.ToLower(r.Notes), "[rollback]") || strings.Contains(strings.ToLower(r.Notes), "[post-merge-blocker]") {
		return "fail"
	}
	if r.RevertedWithin7d != nil && *r.RevertedWithin7d {
		return "fail"
	}
	unknown := false
	for severity, count := range r.FindingsBySeverity {
		if count > 0 && severeRepFinding(severity) {
			return "fail"
		}
		if count > 0 && !severeRepFinding(severity) {
			switch normalizeRepKey(severity) {
			case "medium", "moderate", "low", "minor", "info", "informational", "nit", "nits", "style", "suggestion", "non_blocking", "non-blocking", "nonblocking", "p2", "p3", "p4", "s2", "s3", "s4":
			default:
				unknown = true
			}
		}
	}
	if r.OutcomeFinalized != nil && !*r.OutcomeFinalized {
		return "pending"
	}
	merged := r.TimeToMergeS != nil || (r.Completed != nil && *r.Completed == 1) || r.OutcomeDecision == "landed" || r.OutcomeDecision == "merged"
	if r.OutcomeDecision != "" && r.OutcomeDecision != "landed" && r.OutcomeDecision != "merged" {
		merged = false
	}
	if !merged || r.FirstCIGreen == nil || r.RevertedWithin7d == nil || r.FindingsBySeverity == nil {
		return "pending"
	}
	if !*r.FirstCIGreen || unknown {
		return "with_fixes"
	}
	return "pass"
}

func repRef(r Rep) string {
	if r.LocalID > 0 {
		return fmt.Sprintf("local:%d", r.LocalID)
	}
	if r.ID > 0 {
		return fmt.Sprintf("srv:%d", r.ID)
	}
	return fmt.Sprintf("origin:%d", r.OriginID)
}

func newRepOriginID() (int64, error) {
	var v [8]byte
	if _, err := rand.Read(v[:]); err != nil {
		return 0, err
	}
	id := int64(binary.BigEndian.Uint64(v[:]) & ((1 << 53) - 1))
	if id == 0 {
		id = 1
	}
	return id, nil
}

func stableRepOriginID(seed string) int64 {
	var hash uint64 = 1469598103934665603
	for i := 0; i < len(seed); i++ {
		hash ^= uint64(seed[i])
		hash *= 1099511628211
	}
	id := int64(hash & ((1 << 53) - 1))
	if id == 0 {
		id = 1
	}
	return id
}

func repTableGrade(catalog []CatalogRow, profile, effort string) string {
	for _, row := range catalog {
		if row.Profile == profile && row.Effort == effort {
			return row.Grade
		}
	}
	if effort == "" {
		for _, row := range catalog {
			if row.Profile == profile && row.Effort == "" {
				return row.Grade
			}
		}
	}
	return ""
}

// resolveRepRung is pacelever's rung rule without its alias table: a recorded
// effort always wins; an empty effort falls back to the profile's default row.
func resolveRepRung(catalog []CatalogRow, r Rep) (profile, effort string) {
	profile, effort = r.Profile, r.Effort
	if effort != "" {
		return profile, effort
	}
	for _, row := range catalog {
		if row.Profile == profile && row.Effort == "" {
			return profile, ""
		}
	}
	for _, row := range catalog {
		if row.Profile == profile {
			return profile, row.Effort
		}
	}
	return profile, ""
}

func uniqueRepRows(rows []Rep) []Rep {
	slices.SortStableFunc(rows, func(a, b Rep) int {
		switch {
		case a.RecordedAt > b.RecordedAt:
			return -1
		case a.RecordedAt < b.RecordedAt:
			return 1
		}
		return 0
	})
	return rows
}

package fleet

import (
	"encoding/json"
	"regexp"
	"sort"
)

// Per-job skill use rides the same hk terminal update as usage (hk 3164). Only
// a skill's name and call count leave the harness record: arguments, prompts,
// tool output and every other string are never decoded, retained or sent.
//
// Record formats, from the harnesses' public docs and source:
//   - claude: an assistant entry's message.content carries
//     {type:"tool_use", id, name:"Skill", input:{skill:"<name>", args:"..."}}.
//     The input is decoded into a struct holding only `skill`, so arguments
//     never enter memory. One API response is written once per content block
//     with the same message.id, so a tool_use id seen again is the same call
//     and is counted once.
//   - codex, omp, devin, agy, cmd and every other kind: no documented,
//     bounded skill record; they report skills_supported=false.

const (
	// hkSkillsMax bounds the unique name/source pairs one update may carry.
	hkSkillsMax = 100
	// skillSourceClaude names where the observation came from: claude's
	// Skill tool. The record names no package or version.
	skillSourceClaude = "claude-skill-tool"
)

// usageSkillNameRE is the character set a Skill tool's name must match to
// cross. A name is a skill id, never free text: claude's own skill spec allows
// lowercase letters, digits, hyphens, underscores and a plugin namespace.
var usageSkillNameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{0,127}$`)

// SkillUse is one skill the session called, in hk's `skills` element shape.
// VersionOrHash stays empty: the record names no version.
type SkillUse struct {
	Name   string `json:"name"`
	Source string `json:"source"`
	Calls  int64  `json:"calls"`
}

// SkillUsage is what one harness record says about skill use. Supported is
// true when the harness has a skill mechanism at all; Known is true when the
// record was read, so Skills is the current list.
type SkillUsage struct {
	Skills    []SkillUse
	Supported bool
	Known     bool
}

// ReadSkills returns the skill calls of the session ref names. Only claude
// has a record it can read the same bounded way as usage; every other kind
// is unsupported and returns no skills.
func (r UsageReader) ReadSkills(ref HarnessSession, cwd string) SkillUsage {
	if ref.Kind != "claude" {
		return SkillUsage{}
	}
	out := SkillUsage{Supported: true}
	// "unknown" is FindHarnessSession's sentinel for no reference, not an id:
	// it must never name a file.
	if ref.ID == "unknown" {
		return out
	}
	path, ok := r.claudePath(ref, cwd)
	if !ok {
		return out
	}
	skills, ok := readClaudeSkills(path)
	if !ok {
		return out
	}
	out.Skills, out.Known = skills, true
	return out
}

func readClaudeSkills(path string) ([]SkillUse, bool) {
	counts := map[string]int64{}
	seen := map[string]bool{}
	overflow, skipped := false, false
	ok := scanUsageLinesNotify(path, func(line []byte) {
		var entry struct {
			Type    string `json:"type"`
			Message struct {
				Content []struct {
					Type  string `json:"type"`
					ID    string `json:"id"`
					Name  string `json:"name"`
					Input struct {
						Skill string `json:"skill"`
					} `json:"input"`
				} `json:"content"`
			} `json:"message"`
		}
		if json.Unmarshal(line, &entry) != nil || entry.Type != "assistant" {
			return
		}
		for _, block := range entry.Message.Content {
			if block.Type != "tool_use" || block.Name != "Skill" {
				continue
			}
			name := block.Input.Skill
			if !usageSkillNameRE.MatchString(name) {
				continue
			}
			if block.ID != "" {
				if seen[block.ID] {
					continue
				}
				seen[block.ID] = true
			}
			if _, known := counts[name]; !known && len(counts) >= hkSkillsMax {
				overflow = true
				continue
			}
			counts[name]++
		}
	}, func() { skipped = true })
	// A partial list is worse than none: hk replaces the whole array, and an
	// overflowed or skipped-over record cannot prove the calls it dropped.
	if !ok || overflow || skipped {
		return nil, false
	}
	out := make([]SkillUse, 0, len(counts))
	for name, calls := range counts {
		out = append(out, SkillUse{Name: name, Source: skillSourceClaude, Calls: calls})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, true
}

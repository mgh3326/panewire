package fleet

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"regexp"
	"slices"
	"strings"

	"github.com/mgh3326/panewire/internal/configkeys"
	"github.com/mgh3326/panewire/internal/grade"
)

//go:embed policy.json
var defaultPolicy []byte

type Policy struct {
	Version  int             `json:"version"`
	Profiles map[string]Seat `json:"profiles"`
}
type Seat struct {
	Proof      string          `json:"proof,omitempty"`
	Canon      string          `json:"canon"`
	Kind       string          `json:"kind"`
	Family     string          `json:"family"`
	Effort     string          `json:"effort,omitempty"`
	Pin        bool            `json:"pin,omitempty"`
	Capability string          `json:"capability"`
	E6         bool            `json:"e6,omitempty"`
	Roles      []string        `json:"roles"`
	Rungs      map[string]Rung `json:"rungs,omitempty"`
	// Gate names the quota gate profile when it differs from the launch
	// canon (several agy spellings share the one agy pool gate).
	Gate string `json:"gate,omitempty"`
	// Route redirects a claude-kind harness through a named model route;
	// such a seat has no effort rung and its launcher passes no effort flag.
	Route string `json:"route,omitempty"`
	// ContextTokens caps CLAUDE_CODE_MAX_CONTEXT_TOKENS for a routed seat.
	ContextTokens int `json:"context_tokens,omitempty"`
	// NoThinking marks a model with no controllable thinking levels. Its
	// launcher passes no effort, the gate admits only its "off" rung, and the
	// landing proof accepts a footer without an effort glyph. The property
	// belongs to the seat; a catalog row does not carry it (hk 3974).
	NoThinking bool `json:"no_thinking,omitempty"`
}
type Rung struct {
	Roles []string `json:"roles"`
	E6    *bool    `json:"e6,omitempty"`
}

// gateName is the profile the quota gate and arbiter lease know this seat by.
func (s Seat) gateName() string {
	if s.Gate != "" {
		return s.Gate
	}
	return s.Canon
}

// needsEffort reports whether the seat's launcher has to be told an effort.
// An empty catalog effort is the profile's default rung; only launchers that
// pass the effort to the harness refuse it. devin, opencode and agy launch by
// model id alone, and a routed claude harness runs at its own default.
func (s Seat) needsEffort() bool {
	switch s.Kind {
	case "claude":
		return s.Route == ""
	case "codex", "omp", "cmd", "kiro":
		return true
	}
	return false
}

func LoadPolicy(path string) (Policy, error) {
	b := defaultPolicy
	var e error
	if path != "" {
		b, e = os.ReadFile(path)
		if e != nil {
			return Policy{}, e
		}
	}
	var p Policy
	if e = decode(b, &p); e != nil {
		return p, e
	}
	if p.Version != 1 || len(p.Profiles) == 0 {
		return p, fail(exitConfig, "invalid seat policy version or empty profiles")
	}
	for _, name := range slices.Sorted(maps.Keys(p.Profiles)) {
		s := p.Profiles[name]
		if s.Canon == "" || s.Kind == "" || s.Family == "" || s.Capability == "" || len(s.Roles) == 0 {
			return p, fail(exitConfig, "missing seat data for %s", name)
		}
		if s.Gate != "" && !validID(s.Gate) {
			return p, fail(exitConfig, "invalid gate alias for %s", name)
		}
		if s.Route != "" && (s.Kind != "claude" || routes[s.Route].baseURLEnv == "") {
			return p, fail(exitConfig, "invalid route for %s", name)
		}
		if s.Route != "" && (s.Effort != "" || s.Pin || len(s.Rungs) > 0) {
			return p, fail(exitConfig, "routed seat %s has no effort rung", name)
		}
	}
	return p, nil
}

var gradeOrder = func() map[string]int {
	grades := grade.Grades()
	ranks := make(map[string]int, len(grades))
	for rank, value := range grades {
		ranks[value] = rank
	}
	return ranks
}()

const gradeUnrated = "unrated"

func validCatalogGrade(grade string) bool {
	_, rated := gradeOrder[grade]
	return rated || grade == gradeUnrated
}

func taskMarkerPattern() string {
	grades := grade.Grades()
	for left, right := 0, len(grades)-1; left < right; left, right = left+1, right-1 {
		grades[left], grades[right] = grades[right], grades[left]
	}
	for i := range grades {
		grades[i] = regexp.QuoteMeta(grades[i])
	}
	return `(?i)(?:\(|\b)grade (` + strings.Join(grades, `|`) + `),\s*(T[0-3])\b`
}

// Task grades are C, B, A, A+, S, S+ and SS. Titles declare a marker as
// "(grade B, T1)" or the bare sentence form "Grade SS, T3"; both are accepted.
// Grade and scale must be adjacent.
var taskMarker = regexp.MustCompile(taskMarkerPattern())

// Check admits one launch row for a seat role.
func (s Seat) Check(l Launch, role, grade, builderFamily, arm string, host map[string]any) error {
	if !validCatalogGrade(l.Grade) || l.ModelID == "" || (l.Effort == "" && s.needsEffort()) || l.Gate == "" {
		return fail(exitConfig, "canon launch data incomplete")
	}
	if !boolean(host, s.Capability, false) { // Standard harnesses predate capability flags; only their explicit policy sentinel is implicit.
		if s.Capability != "standard" {
			return fail(exitConfig, "host lacks capability %s", s.Capability)
		}
	}
	if s.Route != "" && l.Effort != "" {
		return fail(exitConfig, "routed seat %s takes no effort rung", s.Canon)
	}
	roles := s.Roles
	e6 := s.E6
	if len(s.Rungs) > 0 {
		r, ok := s.Rungs[l.Effort]
		if !ok {
			return fail(exitConfig, "missing policy rung %s@%s", s.Canon, l.Effort)
		}
		roles = r.Roles
		if r.E6 != nil {
			e6 = *r.E6
		}
	}
	if !slices.Contains(roles, role) {
		return fail(exitSeatRefused, "seat does not allow role %s", role)
	}
	armed := arm == s.Canon+"@"+l.Effort
	if e6 && !armed {
		return fail(exitSeatRefused, "requires PANEWIRE_E6_ARM=%s@%s", s.Canon, l.Effort)
	}
	if grade != "" && role != "counsel" {
		n, ok := gradeOrder[grade]
		if ok && l.Grade == gradeUnrated {
			// An E6 arm admits an unrated rung for a graded task; it never raises a grade.
			if !armed {
				return fail(exitSeatRefused, "canon grade unrated does not cover task grade %s; requires PANEWIRE_E6_ARM=%s@%s", grade, s.Canon, l.Effort)
			}
		} else if !ok || l.Grade == gradeUnrated || gradeOrder[l.Grade] < n {
			return fail(exitSeatRefused, "canon grade %s does not cover task grade %s", l.Grade, grade)
		}
	}
	if role == "tester" || role == "reviewer" {
		if builderFamily == "" {
			return fail(exitSeatRefused, "tester requires readable builder family")
		}
		if s.Family == builderFamily {
			return fail(exitSeatRefused, "tester and builder share family %s", s.Family)
		}
	}
	return nil
}
func encode(v any) ([]byte, error) { return json.Marshal(v) }
func decode(b []byte, v any) error {
	if e := uniqueJSON(b); e != nil {
		return e
	}
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.DisallowUnknownFields()
	return d.Decode(v)
}

// decodeConfig is decode for a config document that may carry keys this
// release does not read: they come back for the caller to warn about instead
// of refusing the file. A malformed value for a key the struct reads still
// fails.
func decodeConfig(b []byte, v any) ([]string, error) {
	if e := uniqueJSON(b); e != nil {
		return nil, e
	}
	if e := json.NewDecoder(strings.NewReader(string(b))).Decode(v); e != nil {
		return nil, e
	}
	return configkeys.JSON(b, v)
}
func uniqueJSON(b []byte) error {
	d := json.NewDecoder(strings.NewReader(string(b)))
	var value func() error
	value = func() error {
		token, e := d.Token()
		if e != nil {
			return e
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				k, e := d.Token()
				if e != nil {
					return e
				}
				key, ok := k.(string)
				if !ok || seen[key] {
					return fmt.Errorf("duplicate or invalid JSON key %q", key)
				}
				seen[key] = true
				if e = value(); e != nil {
					return e
				}
			}
		case '[':
			for d.More() {
				if e = value(); e != nil {
					return e
				}
			}
		default:
			return fmt.Errorf("unexpected JSON delimiter")
		}
		_, e = d.Token()
		return e
	}
	if e := value(); e != nil {
		return e
	}
	if _, e := d.Token(); e != io.EOF {
		return fmt.Errorf("trailing JSON data")
	}
	return nil
}

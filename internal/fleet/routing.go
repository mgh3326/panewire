package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/durable"
	"github.com/mgh3326/panewire/internal/textbound"
)

// Role routing (hk document policy/model-routing, operator 2026-11-01).
//
// The v1 table filters profiles before account quota ordering. Consult
// ignores task grade and requires native top profiles or OMP top models.
// Other roles retain grade, gate and seat constraints. Exhaustion never escapes the role.
//
// A valid hk read refreshes the disk cache. If neither hk nor the cache is
// usable, consult and verifier refuse; other roles retain v1's
// builtin_default fallback.

const routingDocKey = "policy/model-routing"

// RoutingPolicy is the machine-readable v1 block of policy/model-routing.
// Unknown fields are ignored so the desk can extend the document.
type RoutingPolicy struct {
	Version int                    `json:"version"`
	Roles   map[string]RoutingRule `json:"roles"`
	RoleMap map[string]string      `json:"panewire_role_map"`
	// LegacyRoleMap is the map's name before the runtug absorption (#155).
	// hk policy/model-routing still publishes it, so parseRoutingJSON reads it
	// into RoleMap; it is never kept or cached under this name.
	LegacyRoleMap map[string]string `json:"runtug_role_map,omitempty"`
}

// RoutingRule is one routing role's entry. Allowed names catalog profiles
// (pacelever launch profiles); "*" admits any profile.
type RoutingRule struct {
	Allowed []string `json:"allowed"`
	Effort  string   `json:"effort"`
	// MinEffort is the cheapest effort rung the role admits. Selection drops
	// a candidate rung below it as below_role_min_effort; "" leaves today's
	// ordering untouched. Only consult applies Effort itself (roleEffortCap),
	// so this is how every other role sets an effort floor.
	MinEffort    string   `json:"min_effort,omitempty"`
	OnExhaustion string   `json:"on_exhaustion"`
	DowngradeTo  []string `json:"downgrade_to,omitempty"`
	FamilyRule   string   `json:"family_rule,omitempty"`
	Fixed        bool     `json:"fixed,omitempty"`
	// FamilyEffort maps a profile, or a top family, to the role-effort rung
	// it leads at instead of Effort (consult only). That exact rung
	// is admitted even when its catalog gate is escalation: the entry is the
	// operator's opt-in. See routing.familyEffortKey for the family rule.
	FamilyEffort map[string]string `json:"family_effort,omitempty"`
}

// routingRoles are the values --routing-role accepts.
var routingRoles = []string{"consult", "implementer", "verifier", "reviewer", "judge"}

// routingStrictRoles are refused when neither hk nor a cached copy can say
// which profiles they may run on.
var routingStrictRoles = []string{"consult", "verifier"}

// builtinRoleMap maps seat roles to routing roles when no table is readable,
// so a counsel is still known to be a consult (and refused) without one.
var builtinRoleMap = map[string]string{"worker": "implementer", "tester": "verifier", "reviewer": "reviewer", "counsel": "consult"}

// retiredLeadRole is the routing role panewire no longer reads from the table.
const retiredLeadRole = "lead"

var topProfiles = []string{"opus", "fable", "codex-sol", "codex-astra"}

var ompTopModels = map[string]string{
	"anthropic/claude-opus-5-5":  "opus",
	"anthropic/claude-fable-5-2": "fable",
	"anthropic/claude-fable-5-1": "fable", // kept for rollback
	"openai/gpt-6.1-sol":         "codex-sol",
	"openai/gpt-6-astra":         "codex-astra",
}

func topProfileRank(row CatalogRow) int {
	profile := row.Profile
	if native, ok := ompTopModels[row.ModelID]; ok {
		profile = native
	}
	if i := slices.Index(topProfiles, profile); i >= 0 {
		return i
	}
	return len(topProfiles)
}

func topRoutingRole(role string) bool { return role == "consult" }

func (r *routing) top() bool { return r != nil && topRoutingRole(r.role) }

func (r *routing) admitsSeat(s Seat, model string) bool {
	if !r.admits(s.Canon) {
		return false
	}
	if !r.top() {
		return true
	}
	if s.Kind == "omp" {
		_, ok := ompTopModels[model]
		return ok
	}
	switch s.Canon {
	case "opus", "fable":
		return s.Kind == "claude"
	case "codex-sol", "codex-astra":
		return s.Kind == "codex"
	}
	return false
}

const (
	exhaustWait      = "wait"
	exhaustEscalate  = "escalate"
	exhaustDowngrade = "downgrade_to"
)

// Policy sources stamped on the selection record.
const (
	routingSourceHK      = "hk"
	routingSourceBuiltin = "builtin_default"
)

func (p RoutingPolicy) validate() error {
	if p.Version != 1 {
		return fmt.Errorf("unsupported routing policy version %d", p.Version)
	}
	if len(p.Roles) == 0 {
		return fmt.Errorf("routing policy has no roles")
	}
	for name, r := range p.Roles {
		if len(r.Allowed) == 0 {
			return fmt.Errorf("routing role %s allows nothing", name)
		}
		if r.MinEffort != "" {
			if _, ok := effortRank[r.MinEffort]; !ok {
				return fmt.Errorf("routing role %s has unknown min_effort %q", name, r.MinEffort)
			}
		}
		switch r.OnExhaustion {
		case exhaustWait, exhaustEscalate:
			if len(r.DowngradeTo) > 0 {
				return fmt.Errorf("routing role %s lists downgrade_to without on_exhaustion downgrade_to", name)
			}
		case exhaustDowngrade:
			if len(r.DowngradeTo) == 0 || slices.Contains(r.DowngradeTo, "*") {
				return fmt.Errorf("routing role %s needs explicit downgrade_to profiles", name)
			}
		default:
			return fmt.Errorf("routing role %s has unknown on_exhaustion %q", name, r.OnExhaustion)
		}
	}
	for name, r := range p.Roles {
		if e := r.validateFamilyEffort(name); e != nil {
			return e
		}
	}
	for seat, role := range p.RoleMap {
		if role == "explorer" {
			return fmt.Errorf("panewire_role_map maps %s to removed role explorer; use implementer", seat)
		}
		if _, ok := p.Roles[role]; !ok {
			return fmt.Errorf("panewire_role_map maps %s to unknown role %s", seat, role)
		}
	}
	return nil
}

// reasonFamilyEffortInvalid names a family_effort entry the table refuses.
const reasonFamilyEffortInvalid = "family_effort_invalid"

// validateFamilyEffort refuses a family_effort panewire would otherwise ignore:
// on a role without the consult cap, keyed by a profile the role cannot
// name, or naming no known effort.
func (r RoutingRule) validateFamilyEffort(role string) error {
	if len(r.FamilyEffort) == 0 {
		return nil
	}
	if !topRoutingRole(role) {
		return fmt.Errorf("%s: routing role %s has family_effort; only consult takes one", reasonFamilyEffortInvalid, role)
	}
	for key, effort := range r.FamilyEffort {
		known := slices.Contains(topProfiles, key) || slices.Contains(r.Allowed, key) || slices.Contains(r.Allowed, "*") && validID(key)
		if !known {
			return fmt.Errorf("%s: routing role %s family_effort names unknown profile %q (want a top profile or one the role allows)", reasonFamilyEffortInvalid, role, key)
		}
		if _, ok := effortRank[effort]; !ok || effort == "" {
			return fmt.Errorf("%s: routing role %s family_effort %s has unknown effort %q", reasonFamilyEffortInvalid, role, key, effort)
		}
	}
	return nil
}

// parseRoutingDoc extracts and validates the first ```json block of the
// document body.
func parseRoutingDoc(body string) (RoutingPolicy, error) {
	var p RoutingPolicy
	_, rest, ok := strings.Cut(body, "```json")
	if !ok {
		return p, fmt.Errorf("no json block")
	}
	block, _, ok := strings.Cut(rest, "```")
	if !ok {
		return p, fmt.Errorf("unterminated json block")
	}
	return parseRoutingJSON([]byte(block))
}
func parseRoutingJSON(b []byte) (RoutingPolicy, error) {
	var p RoutingPolicy
	if e := uniqueJSON(b); e != nil {
		return p, e
	}
	if e := json.Unmarshal(b, &p); e != nil {
		return p, e
	}
	if len(p.LegacyRoleMap) > 0 {
		if len(p.RoleMap) > 0 && !maps.Equal(p.RoleMap, p.LegacyRoleMap) {
			return p, fmt.Errorf("panewire_role_map and runtug_role_map disagree; publish one map")
		}
		p.RoleMap = p.LegacyRoleMap
	}
	p.LegacyRoleMap = nil
	// The retired lead entry is never read, so it is never validated: a table
	// that still carries one (with or without family_effort) stays usable, and
	// a role map that names lead is refused as an unknown role.
	delete(p.Roles, retiredLeadRole)
	return p, p.validate()
}

// stateDir is panewire's local state directory ($XDG_STATE_HOME/panewire, else
// ~/.local/state/panewire); New sets StateDir from the environment.
func (a *App) stateDir() string {
	if a.StateDir != "" {
		return a.StateDir
	}
	return filepath.Join(a.Home, ".local", "state", "panewire")
}
func (a *App) routingCachePath() string { return filepath.Join(a.stateDir(), "model-routing.json") }

// routingCache is the on-disk last good copy of the policy.
type routingCache struct {
	Version   int             `json:"version"`
	FetchedAt string          `json:"fetched_at"`
	Policy    json.RawMessage `json:"policy"`
}

func (a *App) readRoutingCache() (RoutingPolicy, error) {
	b, e := os.ReadFile(a.routingCachePath())
	if e != nil {
		return RoutingPolicy{}, e
	}
	var c routingCache
	if e = json.Unmarshal(b, &c); e != nil {
		return RoutingPolicy{}, e
	}
	p, e := parseRoutingJSON(c.Policy)
	if e == nil && p.Version != c.Version {
		e = fmt.Errorf("cache version %d does not match its policy v%d", c.Version, p.Version)
	}
	return p, e
}

// writeRoutingCache replaces the cache atomically when the policy changed.
func (a *App) writeRoutingCache(p RoutingPolicy) error {
	policy, e := json.Marshal(p)
	if e != nil {
		return e
	}
	path := a.routingCachePath()
	if old, e := os.ReadFile(path); e == nil {
		var c routingCache
		if json.Unmarshal(old, &c) == nil && c.Version == p.Version && bytes.Equal(c.Policy, policy) {
			return nil
		}
	}
	b, e := json.MarshalIndent(routingCache{Version: p.Version, FetchedAt: a.Now().UTC().Format(time.RFC3339), Policy: policy}, "", "  ")
	if e != nil {
		return e
	}
	if e = os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return e
	}
	return durable.ReplaceFileAtomically(path, ".model-routing-*.json", append(b, '\n'), 0600)
}

// routingPolicy reads the table from hk over HTTP (hkGetDocument), refreshing
// the cache, else falls back to the cache: hk unreachable, the credential
// unconfigured, or a doc that fails parseRoutingDoc, which never overwrites
// the last good copy. The source is "hk" or "cache@v<N>".
func (a *App) routingPolicy(ctx context.Context) (RoutingPolicy, string, error) {
	var why string
	doc, e := hkGetDocument(ctx, routingDocKey)
	if e == nil {
		var p RoutingPolicy
		if p, e = parseRoutingDoc(doc.Body); e == nil {
			if e := a.writeRoutingCache(p); e != nil {
				a.warn("routing policy cache not written: %v", e)
			}
			return p, routingSourceHK, nil
		}
		why = "unreadable " + routingDocKey + ": " + e.Error()
	} else {
		why = fmt.Sprintf("hk doc get %s: %s", routingDocKey, clip(strings.TrimSpace(e.Error()), 200))
	}
	p, e := a.readRoutingCache()
	if e != nil {
		return p, "", fmt.Errorf("%s; no usable cache (%v)", why, e)
	}
	a.warn("%s; using the cached routing policy v%d", why, p.Version)
	return p, fmt.Sprintf("cache@v%d", p.Version), nil
}

// clip bounds s to n bytes on a UTF-8 boundary, marking a cut with "…".
func clip(s string, n int) string {
	if len(s) > n {
		return textbound.PrefixWithin(s, n) + "…"
	}
	return s
}

// routing is the role decision applied to one selection. A nil *routing or
// a nil allowed list filters nothing.
type routing struct {
	role    string // routing role; "" when the seat role is unmapped
	version int    // table version; 0 for builtin_default
	source  string // hk, cache@vN or builtin_default
	rule    *RoutingRule
	allowed []string // nil = any profile
	// downgradedFrom names the role whose exhaustion led to this downgrade
	// selection.
	downgradedFrom string
}

func (r *routing) admits(profile string) bool {
	return r == nil || r.allowed == nil || slices.Contains(r.allowed, profile)
}

// routeFor resolves the routing role for a seat role (or the explicit
// override) against the policy table.
func (a *App) routeFor(ctx context.Context, seatRole, override string) (*routing, error) {
	if override != "" && !slices.Contains(routingRoles, override) {
		if override == "explorer" {
			return nil, fail(exitUsage, "explorer role was removed; use --routing-role implementer")
		}
		return nil, fail(exitUsage, "--routing-role must be one of %s", strings.Join(routingRoles, "|"))
	}
	// Execution authority cannot be relabelled to escape consult admission.
	if required := builtinRoleMap[seatRole]; topRoutingRole(required) {
		if override != "" && override != required {
			return nil, fail(exitSeatRefused, "NO_ROLE_CANDIDATE: --role %s requires routing role %s", seatRole, required)
		}
		override = required
	}
	p, source, e := a.routingPolicy(ctx)
	if e != nil {
		role := firstNonempty(override, builtinRoleMap[seatRole])
		if slices.Contains(routingStrictRoles, role) {
			return nil, fail(exitConfig, "POLICY_UNAVAILABLE: routing role %s refused: %s unavailable (%v)", role, routingDocKey, e)
		}
		a.warn("%s unavailable (%v); %s proceeds unfiltered (builtin_default)", routingDocKey, e, firstNonempty(role, seatRole))
		return &routing{role: role, source: routingSourceBuiltin}, nil
	}
	// A seat role the table does not map takes the builtin routing role: a
	// missing or renamed map must not leave the role unfiltered (hk 2837).
	role := firstNonempty(override, p.RoleMap[seatRole], builtinRoleMap[seatRole])
	r := &routing{role: role, version: p.Version, source: source}
	if role == "" {
		return r, nil
	}
	rule, ok := p.Roles[role]
	if !ok {
		return nil, fail(exitConfig, "POLICY_UNAVAILABLE: %s v%d has no role %s", routingDocKey, p.Version, role)
	}
	if topRoutingRole(role) {
		rule.OnExhaustion, rule.DowngradeTo = exhaustWait, nil
	}
	r.rule = &rule
	if !slices.Contains(rule.Allowed, "*") {
		r.allowed = slices.Clone(rule.Allowed)
	}
	return r, nil
}

// effort is the effort the chosen rung launches at. A consult rung at or
// below the role effort reports that effort, unless family_effort or an
// escalation sets its own. Every other role's Effort does not shape
// selection (roleEffortCap is ""), so it reports the rung itself.
func (r *routing) effort(row CatalogRow) string {
	if r != nil && r.top() {
		if row.Effort != "" && r.familyEffortKey(row) != "" {
			return row.Effort
		}
		if r.rule != nil && r.rule.Effort != "" && !aboveEffort(row.Effort, r.roleEffortCap()) {
			return r.rule.Effort
		}
	}
	return row.Effort
}

// minEffort is the role's effort floor; "" admits every rung.
func (r *routing) minEffort() string {
	if r == nil || r.rule == nil {
		return ""
	}
	return r.rule.MinEffort
}

// roleEffortCap is the effort a consult never climbs above without
// --allow-escalation (policy/model-routing: max only on a recorded
// escalation); "" for every other role, which keeps grade-driven selection.
// roleEffortFor is the same cap for one catalog row, after family_effort.
func (r *routing) roleEffortCap() string {
	if !r.top() || r.rule == nil {
		return ""
	}
	return r.rule.Effort
}

// familyEffortKey is the family_effort key that sets a consult row's
// role effort: the row's own profile, else the native top profile its
// catalog model is (omp-sol runs openai/gpt-6.1-sol, so it follows a
// codex-sol entry; omp-opus follows opus). An exact profile key wins, and a
// key naming an OMP profile never applies to the native profile. "" when the
// map names neither.
func (r *routing) familyEffortKey(row CatalogRow) string {
	if !r.top() || r.rule == nil || len(r.rule.FamilyEffort) == 0 {
		return ""
	}
	if _, ok := r.rule.FamilyEffort[row.Profile]; ok {
		return row.Profile
	}
	if native, ok := ompTopModels[row.ModelID]; ok {
		if _, ok := r.rule.FamilyEffort[native]; ok {
			return native
		}
	}
	return ""
}

func (r *routing) roleEffortFor(row CatalogRow) string {
	if key := r.familyEffortKey(row); key != "" {
		return r.rule.FamilyEffort[key]
	}
	return r.roleEffortCap()
}

// familyAdmitsGate reports whether family_effort opts this exact escalation
// rung in without --allow-escalation. Rungs above it stay excluded.
func (r *routing) familyAdmitsGate(row CatalogRow) bool {
	key := r.familyEffortKey(row)
	return key != "" && row.Gate == "escalation" && row.Effort == r.rule.FamilyEffort[key]
}

// aboveEffort reports whether a named rung ranks above the cap. The catalog's
// default rung ("") is never above it.
func aboveEffort(rung, cap string) bool {
	return cap != "" && rung != "" && rankEffort(rung) > rankEffort(cap)
}

// record renders the routing stamp carried in the selection record.
func (r *routing) record(refusal string) map[string]any {
	m := map[string]any{"routing_role": r.role, "routing_version": r.version, "policy_source": r.source}
	if r.allowed != nil {
		m["allowed"] = r.allowed
	} else if r.rule != nil {
		m["allowed"] = []string{"*"}
	}
	if r.rule != nil {
		m["on_exhaustion"] = r.rule.OnExhaustion
		if r.rule.Effort != "" {
			m["effort"] = r.rule.Effort
		}
		if r.rule.MinEffort != "" {
			m["min_effort"] = r.rule.MinEffort
		}
		if r.top() && len(r.rule.FamilyEffort) > 0 {
			m["family_effort"] = r.rule.FamilyEffort
		}
	}
	if r.downgradedFrom != "" {
		m["downgraded_from"] = r.downgradedFrom
	}
	if refusal != "" {
		m["refused"] = refusal
	}
	return m
}

// Refusal reasons a role selection can end with.
const (
	reasonRoleNoCandidate  = "role_no_candidate"
	reasonRoleExhaustWait  = "role_exhausted_wait"
	reasonRoleExhaustEscal = "role_exhausted_escalate"
)

// Candidate drop reasons of the consult role-effort cap.
const (
	reasonAboveRoleEffort    = "above_role_effort"
	reasonRoleEffortUnplaced = "role_effort_unplaced"
)

// reasonBelowRoleMinEffort drops a rung below the role's min_effort.
const reasonBelowRoleMinEffort = "below_role_min_effort"

// roleExhausted handles a routed selection that admitted nothing. With no
// in-role candidate left after the static filters it refuses
// role_no_candidate; otherwise every in-role candidate was quota-exhausted
// and the role's on_exhaustion decides. A downgrade retries selection once
// with only the downgrade_to profiles.
func (a *App) roleExhausted(ctx context.Context, s Spawn, req selReq, p *selPlan) (*selPlan, error) {
	rt := req.route
	var earliest *float64
	exhausted := 0
	for _, c := range p.cands {
		if c.code != outcomeQuotaExhausted {
			continue
		}
		exhausted++
		if c.resetHrs != nil && (earliest == nil || *c.resetHrs < *earliest) {
			earliest = c.resetHrs
		}
	}
	allowed := "*"
	if rt.allowed != nil {
		allowed = strings.Join(rt.allowed, ",")
	}
	if exhausted == 0 {
		p.routeRefusal = reasonRoleNoCandidate
		return p, fail(exitSeatRefused, "%s: no %s profile (%s) passes grade %s, gate and seat rules", reasonRoleNoCandidate, rt.role, allowed, req.grade)
	}
	reset := "unknown"
	if earliest != nil {
		reset = fmt.Sprintf("%s (in %.1fh)", p.at.Add(time.Duration(*earliest*float64(time.Hour))).UTC().Format(time.RFC3339), *earliest)
	}
	if rt.downgradedFrom != "" {
		p.routeRefusal = reasonRoleExhaustWait
		return p, fail(exitRoleWait, "%s: QUOTA_EXHAUSTED: role %s and its downgrade_to profiles (%s) are exhausted; earliest reset %s%s", reasonRoleExhaustWait, rt.downgradedFrom, allowed, reset, hostScope(s))
	}
	switch rt.rule.OnExhaustion {
	case exhaustDowngrade:
		d := *rt
		d.allowed, d.downgradedFrom = slices.Clone(rt.rule.DowngradeTo), rt.role
		next := req
		next.route = &d
		q, e := a.selectFor(ctx, s, next)
		if q != nil {
			q.before = p
		}
		return q, e
	case exhaustEscalate:
		p.routeRefusal = reasonRoleExhaustEscal
		return p, fail(exitRoleEscalate, "%s: QUOTA_EXHAUSTED: every eligible %s account (%s) is measured exhausted; ask the parent lane%s", reasonRoleExhaustEscal, rt.role, allowed, hostScope(s))
	default:
		p.routeRefusal = reasonRoleExhaustWait
		return p, fail(exitRoleWait, "%s: QUOTA_EXHAUSTED: every eligible %s account (%s) is measured exhausted; earliest reset %s%s", reasonRoleExhaustWait, rt.role, allowed, reset, hostScope(s))
	}
}

package fleet

import (
	"cmp"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

//go:embed pool-order.json
var defaultPoolOrder []byte

// PoolPolicy is the versioned model-selection order from
// internal/fleet/pool-order.json. It encodes the operator's paid/credit pool
// order (source_doc) without embedding model IDs or grades: "free" matches
// catalog rows by (pool, model) so the SWE-2 free bucket follows quota that would be wasted,
// "credit_pools" names spend pools drawn on credit, and a subscription pool
// whose projected use at reset stays below "waste_below_projected_pct" is
// promoted to waste because the rest is lost at reset. A pool projected at or
// above "exclude_at_projected_pct" is never selected. Both default to 100
// when absent; an explicit 0 waste threshold disables projected waste.
//
// "waste_risk" names when a paid pool is about to lose quota at its longest
// window's reset: the projected unused share at that reset is at least
// min_unused_pct (default 20), however far the reset is. An optional
// horizon_hours (default 0 = no limit) additionally requires the reset to
// fall within that many hours. Such a pool ranks ahead of the free seats by
// waste urgency, and its sub-daily windows refuse only when exhausted.
type PoolPolicy struct {
	Version     int              `json:"version"`
	SourceDoc   string           `json:"source_doc"`
	Order       []string         `json:"order"`
	Free        []FreeSet        `json:"free"`
	CreditPools []string         `json:"credit_pools"`
	WasteBelow  *float64         `json:"waste_below_projected_pct"`
	ExcludeAt   *float64         `json:"exclude_at_projected_pct"`
	WasteRisk   *WasteRiskPolicy `json:"waste_risk"`
	// CreditFinalDays is deprecated and has no effect.
	CreditFinalDays *float64 `json:"credit_final_days"`
}

// WasteRiskPolicy holds the waste-risk override thresholds; absent fields
// take their defaults.
type WasteRiskPolicy struct {
	HorizonHours *float64 `json:"horizon_hours"`
	MinUnusedPct *float64 `json:"min_unused_pct"`
}

const (
	defaultWasteRiskHorizonHours = 0.0
	defaultWasteRiskMinUnusedPct = 20.0
)

type FreeSet struct {
	Pool   string   `json:"pool"`
	Models []string `json:"models"`
}

var poolTiers = []string{"waste", "free", "ranked", "credit"}

func LoadPoolPolicy(path string) (PoolPolicy, error) {
	b := defaultPoolOrder
	if path == "" {
		path = os.Getenv("PANEWIRE_POOL_POLICY")
	}
	if path != "" {
		var e error
		b, e = os.ReadFile(expand(path))
		if e != nil {
			return PoolPolicy{}, e
		}
	}
	var p PoolPolicy
	if e := decode(b, &p); e != nil {
		return p, e
	}
	// validate fills defaults in place; call it before p is copied out.
	e := p.validate()
	return p, e
}

// validate checks a decoded pool policy and fills absent thresholds with
// their defaults. It is rerun after simulate's --policy-set overrides.
func (p *PoolPolicy) validate() error {
	if p.Version != 2 {
		return fail(exitConfig, "pool order policy version %d is unsupported; migrate to version 2 with order waste,free,ranked,credit and credit_pools anthropic-api,openai-api (docs/config.md)", p.Version)
	}
	if len(p.Order) == 0 {
		return fail(exitConfig, "invalid pool order policy")
	}
	for _, tier := range poolTiers {
		if !slices.Contains(p.Order, tier) {
			return fail(exitConfig, "pool order policy missing tier %q", tier)
		}
	}
	for _, tier := range p.Order {
		if !slices.Contains(poolTiers, tier) {
			return fail(exitConfig, "unknown pool order tier %q", tier)
		}
	}
	if slices.Index(p.Order, "credit") != len(p.Order)-1 {
		return fail(exitConfig, "pool order policy v2 requires credit last, after ranked")
	}
	for _, fs := range p.Free {
		if !validID(fs.Pool) {
			return fail(exitConfig, "invalid free pool name %q", fs.Pool)
		}
	}
	for _, pool := range p.CreditPools {
		if !validID(pool) {
			return fail(exitConfig, "invalid credit pool name %q", pool)
		}
	}
	for _, x := range []**float64{&p.WasteBelow, &p.ExcludeAt} {
		if *x == nil {
			v := 100.0
			*x = &v
		}
	}
	if *p.WasteBelow < 0 || *p.ExcludeAt <= 0 || *p.WasteBelow > *p.ExcludeAt {
		return fail(exitConfig, "invalid projected-at-reset thresholds")
	}
	if p.CreditFinalDays != nil && (!finite(*p.CreditFinalDays) || *p.CreditFinalDays < 0) {
		return fail(exitConfig, "invalid credit_final_days")
	}
	if p.WasteRisk == nil {
		p.WasteRisk = &WasteRiskPolicy{}
	}
	if p.WasteRisk.HorizonHours == nil {
		v := defaultWasteRiskHorizonHours
		p.WasteRisk.HorizonHours = &v
	}
	if p.WasteRisk.MinUnusedPct == nil {
		v := defaultWasteRiskMinUnusedPct
		p.WasteRisk.MinUnusedPct = &v
	}
	if h, m := *p.WasteRisk.HorizonHours, *p.WasteRisk.MinUnusedPct; !finite(h) || !finite(m) || h < 0 || m < 0 || m > 100 {
		return fail(exitConfig, "invalid waste_risk thresholds")
	}
	return nil
}

func finite(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }
func (a *App) poolPolicy() (PoolPolicy, error) {
	p, err := LoadPoolPolicy(str(a.Config.Local, "pool_policy"))
	if err == nil && p.CreditFinalDays != nil {
		a.warn("credit_final_days is deprecated and has no effect")
	}
	return p, err
}

// selCand is one profile/rung/account choice. Static refusals have no account.
// Reason records a drop; Tier and TierName record its quota-policy rank.
type selCand struct {
	row       CatalogRow
	seat      *Seat
	seatName  string
	tier      int
	tierName  string
	blocking  *float64
	projected *float64
	urgency   *float64
	resetHrs  *float64
	reason    string
	// cap names the [quota].pool_max_concurrent key that dropped the
	// candidate: its catalog pool, or the reserved devin-free key for a
	// SWE-2 free rung.
	cap       string
	outcome   string
	judgments []quotaJudgment
	// exhausted marks a free rung whose account projection reaches the
	// exclusion threshold: only an admit on a free bucket may keep it.
	exhausted bool
	// risk marks a paid pool at waste risk (PoolPolicy.wasteRisk); unused is
	// its projected unused share at the longest window's reset.
	risk   bool
	unused *float64
	// creditPool identifies a prepaid pool; credit records its expiry reading.
	creditPool bool
	credit     *creditFinal
	// usdUrgency is a credit pool's dollars at risk per hour until expiry;
	// nil for a credit entry without a dollar balance.
	usdUrgency *float64
	// creditLeft marks a credit pool with a recorded balance above zero; only
	// then may a paced pool rank behind credit.
	creditLeft bool
	// longProj is the pool's projection at reset over windows of a day or
	// more; sub-daily windows never rank a pool.
	longProj *float64
	// paced marks a subscription pool on pace to use up its longest window
	// by that window's reset (PoolPolicy.paced); behindCredit records that
	// orderSelCands ranked it behind credit because a credit pool with a
	// balance was a candidate.
	paced        bool
	behindCredit bool
	// wasteGroup names the metering group this rung's pacing and waste were
	// judged on, for a pool that publishes only group windows; empty for every
	// other pool.
	wasteGroup  string
	accountKey  string
	host        string
	hosts       []selHost
	holders     []selHost
	code        string
	ignoreGrade bool
}

type selPlan struct {
	policy   PoolPolicy
	order    []string
	req      selReq
	cands    []*selCand
	ranked   []*selCand // tiered candidates in rank order
	chosen   *selCand
	degraded []string
	// poolCaps is [quota].pool_max_concurrent as this selection read it; the
	// refusal names the caps that dropped candidates.
	poolCaps map[string]int
	at       time.Time
	// routeRefusal names the role refusal (role_no_candidate,
	// role_exhausted_wait, role_exhausted_escalate) when routing ended the
	// selection; before is the role's own exhausted attempt when this plan
	// is its downgrade_to retry.
	routeRefusal string
	before       *selPlan
	outcome      string
	// roleSkips names the consult families skipped role_effort_unplaced;
	// escalation records a chosen rung above the role effort;
	// familyEffort records a chosen rung whose role effort family_effort set.
	roleSkips    []map[string]any
	escalation   map[string]any
	familyEffort map[string]any
	// hosts explains host-first admission per host (host_fit.go); capsGap
	// names required caps no usable host declares, for the refusal text.
	hosts   []hostTrace
	capsGap string
	// creditMissing names credit pools whose entry records no dollars, logged
	// once per plan so the operator knows to record them.
	creditMissing []string
}
type selReq struct {
	grade           string
	t               string
	role            string
	tags            []string
	effort          string
	allowEscalation bool
	builderFamily   string
	urgency         string
	urgencySource   string
	// excludeFamilies drops seats of those families; onlyFamily keeps just that
	// one (delegate --panel picks one seat per distinct family).
	excludeFamilies []string
	onlyFamily      string
	// excludePools drops candidates of those quota pools (--exclude-pool).
	excludePools []string
	harness      string
	// creditAt judges credit expiry as of this time instead of the plan's
	// clock (`plan --credit-at`, read-only); zero means the plan's clock.
	creditAt time.Time
	// route is the role routing decision (routing.go); nil filters nothing.
	route *routing
}

// effortRank orders rung efforts cheap-first; an empty effort is the catalog's
// default rung and ranks before named rungs.
var effortRank = map[string]int{"": 0, "low": 1, "medium": 2, "high": 3, "xhigh": 4, "max": 5, "ultra": 6}

func rankEffort(e string) int {
	if n, ok := effortRank[e]; ok {
		return n
	}
	return len(effortRank)
}

// seatRoles resolves the role list a seat admits for one catalog effort,
// mirroring Seat.Check's rung lookup. A missing rung is a refusal.
func (s Seat) seatRoles(effort string) ([]string, bool) {
	if len(s.Rungs) == 0 {
		return s.Roles, true
	}
	r, ok := s.Rungs[effort]
	if !ok {
		return nil, false
	}
	return r.Roles, true
}
func (s Seat) seatE6(effort string) bool {
	e6 := s.E6
	if len(s.Rungs) > 0 {
		if r, ok := s.Rungs[effort]; ok && r.E6 != nil {
			e6 = *r.E6
		}
	}
	return e6
}

// seatFor picks the seat alias that should launch one catalog rung. Seat
// candidates share the rung's catalog profile as their canon; an exact
// name match wins, then the lexically smallest alias. Role, rung, pinned
// effort, builder-family and E6 rules mirror Seat.Check.
func (a *App) seatFor(row CatalogRow, req selReq) (string, *Seat, string) {
	var names []string
	for name, seat := range a.Policy.Profiles {
		if seat.Canon == row.Profile {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return "", nil, "no_seat"
	}
	slices.Sort(names)
	if exact, ok := a.Policy.Profiles[row.Profile]; ok && row.Effort == "" && exact.Canon == row.Profile && (exact.needsEffort() || exact.Effort != "") {
		// The profile's own seat launches it at a declared rung, so an alias
		// seat cannot stand in for the rung-less default row.
		return "", nil, "effort_empty"
	}
	if i := slices.Index(names, row.Profile); i > 0 {
		names = append([]string{row.Profile}, slices.Concat(names[:i], names[i+1:])...)
	}
	firstReason := ""
	for _, name := range names {
		seat := a.Policy.Profiles[name]
		reason := ""
		if !req.route.admitsSeat(seat, row.ModelID) {
			reason = "role_filtered"
		} else if req.harness != "" && seat.Kind != req.harness {
			reason = "harness_filtered"
		} else if row.Effort != "" && seat.Route != "" {
			// A routed harness runs at its own default; it has no named rung.
			reason = "seat_rung"
		} else if row.Effort == "" && (seat.needsEffort() || seat.Effort != "") {
			// A rung-less row is the profile's default rung. A seat whose
			// launcher needs an effort, or that launches at its own default
			// rung, cannot represent it.
			reason = "effort_empty"
		} else if seat.Pin && seat.Effort != row.Effort {
			reason = "effort_mismatch"
		} else if roles, ok := seat.seatRoles(row.Effort); !ok {
			reason = "seat_rung"
		} else {
			switch {
			case slices.Contains(req.excludeFamilies, seat.Family):
				reason = "family_excluded"
			case req.onlyFamily != "" && seat.Family != req.onlyFamily:
				reason = "family_filtered"
			case !slices.Contains(roles, req.role):
				reason = "seat_role"
			case (req.role == "tester" || req.role == "reviewer") && req.builderFamily == "":
				reason = "builder_family_unreadable"
			case (req.role == "tester" || req.role == "reviewer") && seat.Family == req.builderFamily:
				reason = "family_excluded"
			case seat.seatE6(row.Effort) && a.envOr("PANEWIRE_E6_ARM", "SCOPEFUEL_E6_ARM") != seat.Canon+"@"+row.Effort:
				reason = "e6_marker"
			}
		}
		if reason == "" {
			return name, &seat, ""
		}
		if firstReason == "" {
			firstReason = reason
		}
	}
	return "", nil, firstReason
}

func (p PoolPolicy) isFree(row CatalogRow) bool {
	for _, fs := range p.Free {
		if fs.Pool == row.Pool && (len(fs.Models) == 0 || slices.Contains(fs.Models, row.ModelID)) {
			return true
		}
	}
	return false
}

func (p PoolPolicy) isCredit(pool string) bool {
	return slices.Contains(p.CreditPools, pool)
}

// tierOf assigns a tier from this account's view and the pool policy.
// Cross-candidate ordering belongs to orderSelCands, not tier assignment.
func (p PoolPolicy) tierOf(row CatalogRow, order []string, v PoolView) (string, int) {
	if p.isFree(row) {
		return "free", slices.Index(order, "free")
	}
	if v.Class == "exclude" {
		return "", -1
	}
	if p.isCredit(row.Pool) || v.Class == "credit" {
		return "credit", slices.Index(order, "credit")
	}
	if v.Status != "ok" || v.Stale || v.Class != "spend" {
		return "", -1
	}
	tier := "ranked"
	if long := longProjection(v); long != nil && *long < *p.WasteBelow || long == nil && v.Waste != nil && *v.Waste {
		tier = "waste"
	}
	return tier, slices.Index(order, tier)
}

// longProjection is the pool's projected use at reset over its windows of a
// day or more: LongProjected, else the longest window's waste urgency times
// its hours to reset, else Projected when the pool reported no sub-daily
// window (so Projected can only come from longer ones). It is nil when no
// such window can be paced. Sub-daily windows never rank an account-scoped
// pool; a group view's own longest window ranks even when it is sub-daily
// (PoolView.Groups), and LongProjected then carries it.
func longProjection(v PoolView) *float64 {
	if v.LongProjected != nil {
		return v.LongProjected
	}
	if v.Urgency != nil && v.ResetHours != nil {
		x := 100 - *v.Urgency**v.ResetHours
		return &x
	}
	if v.ShortUsed == nil {
		return v.Projected
	}
	return nil
}

// paced reports a subscription pool on pace to use up its longest window by
// that window's reset (credit pacing, decision/2026-11-04/pool-order-owner):
// that quota will be spent anyway, so a credit pool with a balance takes the
// work first. Free seats, credit pools and pools at waste risk are never
// paced. There is no threshold beyond "used up": 100%.
func (p PoolPolicy) paced(row CatalogRow, free, risk bool, v PoolView) bool {
	if free || risk || p.isCredit(row.Pool) || v.Class == "credit" {
		return false
	}
	long := longProjection(v)
	return long != nil && *long >= 100
}

// wasteRisk reports whether a paid pool is about to lose quota: its longest
// window's (a day or more) projected unused share at reset is at least the
// policy minimum and, when the policy sets a horizon above 0, that reset
// falls within it. The unused
// share is (100 − projected) of that window, i.e. waste urgency × hours to
// reset. Free seats and credit pools are never at waste risk here (an
// expiring credit balance is judged in selPlan.classify), and a pool
// without a readable ok spend status, or whose longest window cannot be
// paced, is not either. The returned share is set whenever it is readable.
func (p PoolPolicy) wasteRisk(row CatalogRow, free bool, v PoolView, ok bool) (bool, *float64) {
	if !ok || v.Urgency == nil || v.ResetHours == nil || *v.ResetHours <= 0 {
		return false, nil
	}
	unused := *v.Urgency * *v.ResetHours
	if free || p.isCredit(row.Pool) || v.Status != "ok" || v.Stale || v.Class != "spend" {
		return false, &unused
	}
	wr := p.WasteRisk
	if h := *wr.HorizonHours; h > 0 && *v.ResetHours > h {
		return false, &unused
	}
	return unused >= *wr.MinUnusedPct, &unused
}

// exhaustReason names why a non-free candidate's pool excludes it, or "".
// Only a window already used up excludes a pool: a sub-daily window or the
// blocking share at or above 100%. A pool merely on pace to use up its
// longest window stays a candidate and ranks behind credit (paced, credit
// pacing); sub-daily windows never project. A waste-risk pool is also
// excluded once its longest window projects to the exclusion threshold.
func (p PoolPolicy) exhaustReason(v PoolView, risk bool) string {
	if v.ShortUsed != nil && *v.ShortUsed >= 100 || v.Blocking != nil && *v.Blocking >= 100 {
		return "window_exhausted"
	}
	if !risk {
		return ""
	}
	if long := longProjection(v); long != nil && *long >= *p.ExcludeAt {
		return "projected_exhaust"
	}
	return ""
}

// Urgency values and the sources a resolved urgency can come from.
const (
	urgencyNormal = "normal"
	urgencyUrgent = "urgent"
)

// urgentPriorities are the hk task priorities that make a spawn urgent when
// --urgency is omitted (policy/quota-stop: urgent_priorities [1, 2]).
var urgentPriorities = []int{1, 2}

// resolveUrgency applies the explicit flag first, then the bound task's hk
// priority, then the normal default. The returned source names which won.
func resolveUrgency(flag string, task Task) (string, string) {
	if flag != "" {
		return flag, "flag"
	}
	if n, ok := taskPriority(task.Priority); ok && slices.Contains(urgentPriorities, n) {
		return urgencyUrgent, "task-priority"
	}
	return urgencyNormal, "default"
}
func taskPriority(v any) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), n == float64(int(n))
	case string:
		i, e := strconv.Atoi(strings.TrimSpace(n))
		return i, e == nil
	}
	return 0, false
}

// selHost is one spawn host as the presence rule sees it.
type selHost struct {
	name     string
	local    bool
	cfg      map[string]any
	presence string
	awake    bool
	wakeable bool
	eligible bool
	excluded bool
	// restricted marks a host excluded only by --host or --exclude-host.
	// It is never placed or gated, but it may still measure an account it
	// holds: the reading belongs to the account, so a restricted spawn
	// judges the same evidence as the unrestricted plan.
	restricted bool
	load       *float64
	score      *float64
	// decided marks the host the hub's placement named (HostView.Decided).
	decided bool
	// over marks a host at or above its load threshold (local
	// max_load_ratio, else placement.load_threshold); it ranks last among
	// awake hosts. fit names why the host cannot take this work at all
	// (host_fit.go), "" when it can.
	over bool
	fit  string
}

// selPresence snapshots eligible hosts and their current account bindings for
// one selection round. Account reads and bounded refreshes are cached by key.
type selPresence struct {
	a         *App
	hub       map[string]HostView
	presErr   bool
	presence  []PresenceRecord
	accounts  map[string]SharedAccount
	hosts     []selHost
	presTTL   time.Duration
	now       time.Time
	degraded  []string
	refreshed map[string]accountRefresh
	// unreachable names hosts a refresh could not reach this round;
	// host-first admission skips them (a holder that completed a refresh is
	// known reachable, one that failed is not).
	unreachable map[string]string
	// ready is each host's readiness from the hub (readiness.go); a host not
	// in it is unknown and never dropped by a readiness check. repo, brief
	// and toolchains describe what this spawn needs of a host. drops records
	// every (host, profile) pair the filter removed before ranking.
	ready readinessView
	drops []readinessDrop
}

func (a *App) newSelPresence(ctx context.Context, s Spawn, req selReq) *selPresence {
	required := req.tags
	sp := &selPresence{a: a, now: a.Now(), presTTL: a.placementTTL("presence_ttl_s", 86400), accounts: map[string]SharedAccount{}, refreshed: map[string]accountRefresh{}, unreachable: map[string]string{}}
	sp.hub, sp.degraded = a.hostViews(ctx, s.CWD)
	sp.ready = a.newReadinessView(ctx, s.CWD)
	if a.Source == nil {
		sp.presErr = true
		sp.degraded = append(sp.degraded, "presence_unavailable")
	} else if records, e := a.Source.Presence(ctx); e != nil {
		sp.presErr = true
		sp.degraded = append(sp.degraded, "presence_degraded")
	} else {
		sp.presence = records
	}
	machine := a.machine()
	excluded, _ := parseHosts(s.ExcludeHosts)
	threshold := a.loadThreshold()
	// A consult has no grade, whatever the task title says; the
	// placement plan clears it the same way (placeAuto, planHosts).
	grade := req.grade
	if req.route.top() {
		grade = ""
	}
	add := func(name string, cfg map[string]any, local bool) {
		h := selHost{name: name, local: local, cfg: cfg, presence: firstNonempty(str(cfg, "presence_id"), name)}
		if local {
			h.presence = firstNonempty(str(cfg, "presence_id"), machine)
			h.awake = true
			h.load = a.localLoad(ctx)
			if hv, reported := sp.hub[name]; reported {
				h.score, h.decided = hv.Score, hv.Decided
			}
			if !boolean(cfg, "spawn", true) {
				h.excluded = true
			}
		} else {
			h.wakeable = str(cfg, "wake") == "panewire"
			if hv, reported := sp.hub[name]; reported {
				h.awake = hv.Connected
				h.load, h.score, h.decided = hv.LoadRatio, hv.Score, hv.Decided
				if strings.Contains(hv.Reason, "not_accepting") {
					h.excluded = true
				}
			} else {
				h.awake = !h.wakeable || sp.hub == nil
			}
		}
		if slices.Contains(excluded, name) || s.Host != "" && s.Host != "auto" && s.Host != name && !(local && s.Host == "local") {
			h.restricted = !h.excluded
			h.excluded = true
		}
		// The same pressure rule as the placement plan: the local
		// max_load_ratio guard (an unreadable load counts as pressured),
		// then the fleet load threshold for every host.
		if maxRatio, guard := number(cfg, "max_load_ratio"); local && guard && (h.load == nil || *h.load >= maxRatio) {
			h.over = true
		}
		if h.load != nil && *h.load >= threshold {
			h.over = true
		}
		missing := missingCaps(cfg, required)
		refusal := roleRefusal(cfg, req.role, grade)
		h.eligible = len(missing) == 0 && refusal == ""
		switch {
		case h.excluded:
			h.fit = "excluded"
		case len(missing) > 0:
			h.fit = "missing_caps:" + strings.Join(missing, ",")
		case refusal != "":
			h.fit = refusal
		case !h.awake && !h.wakeable:
			h.fit = "disconnected"
		}
		sp.hosts = append(sp.hosts, h)
	}
	add(machine, a.Config.Local, true)
	names := slices.Clone(a.Config.HostOrder)
	var extra []string
	for name := range a.Config.Hosts {
		if !slices.Contains(names, name) {
			extra = append(extra, name)
		}
	}
	slices.Sort(extra)
	for _, name := range append(names, extra...) {
		add(name, a.Config.Hosts[name], false)
	}
	return sp
}

// selectProfile runs automatic model selection when -m is omitted or "auto":
// the bench catalog's live rungs are filtered to the task's grade, admission
// gate, role and effort constraints. Eligible hosts are ranked first, then
// their own accounts and rungs are ordered by the versioned pool policy and
// admitted through the existing quota gate. The first admitted pair wins.
// Every row's outcome is recorded. See docs/placement.md.
//
// The tier order is the policy order for every urgency (pool-order.json;
// panewire owns it, decision/2026-11-04/pool-order-owner): expiring credit,
// paid pools at waste risk, waste, free, ranked, credit, then pools on pace to
// use up their longest window when a credit pool with a balance is a
// candidate. Inside a tier the lowest grade that satisfies the task
// wins, then the pool with the higher waste urgency — the unused projected
// share per hour until its longest window's reset; a pool whose reset can be
// paced always ranks ahead of one whose cannot — then the smaller effort.
// Sub-daily windows (e.g. 5h) never rank a pool; they gate admission only.
// A pool whose reset cannot be paced ranks by its projected use at reset. A
// pool with a window already used up is never a candidate.
func (a *App) selectProfile(ctx context.Context, s Spawn) (*selPlan, error) {
	if a.Quota == nil {
		return nil, fail(exitConfig, "POLICY_UNAVAILABLE: selection needs the quota catalog")
	}
	req := selReq{t: s.T, role: s.Role, effort: s.Effort, allowEscalation: s.AllowEscalation, builderFamily: s.ParentFamily, excludeFamilies: s.ExcludeFamilies, onlyFamily: s.OnlyFamily}
	tags, e := parseTags(s.Tags)
	if e != nil {
		return nil, e
	}
	req.tags = tags
	if req.excludePools, e = parsePools(s.ExcludePools); e != nil {
		return nil, e
	}
	task, _, e := a.taskPrecheck(ctx, s)
	if e != nil {
		return nil, e
	}
	if req.route, e = a.routeFor(ctx, s.Role, s.RoutingRole); e != nil {
		return nil, e
	}
	if s.Grade != "" {
		if req.route.top() {
			return nil, fail(exitConfig, "%s: --grade is not accepted for routing role %s", noLeadGradeRule, req.route.role)
		}
		req.grade = s.Grade
	} else if m := taskMarker.FindStringSubmatch(task.Title); m != nil {
		req.grade = strings.ToUpper(m[1])
		if strings.ToUpper(m[2]) != s.T {
			return nil, fail(exitSeatRefused, "task scale does not match --t")
		}
	} else if !req.route.top() {
		return nil, fail(exitSeatRefused, "auto selection needs a readable task grade")
	}
	if _, ok := gradeOrder[req.grade]; !ok && !req.route.top() {
		return nil, fail(exitSeatRefused, "unreadable task grade %q", req.grade)
	}
	req.urgency, req.urgencySource = resolveUrgency(s.Urgency, task)
	return a.selectFor(ctx, s, req)
}

// selectFor ranks and admits the catalog rungs for a resolved request. It is
// the selection core shared by spawn and `panewire plan`: it reads the catalog,
// account observations, presence and quota gates, and writes no job state.
func (a *App) selectFor(ctx context.Context, s Spawn, req selReq) (*selPlan, error) {
	policy, e := a.poolPolicy()
	if e != nil {
		return nil, fail(exitConfig, "POLICY_UNAVAILABLE: %v", e)
	}
	p := &selPlan{policy: policy, order: slices.Clone(policy.Order), req: req, at: a.Now()}
	// Hub-only cooling fails open when the snapshot is unavailable; record the gap.
	snapshot, coolingState := a.fleetSnapshot(ctx)
	if snapshot == nil {
		p.degraded = append(p.degraded, "pool_cooling_"+coolingState)
	}
	rows, e := a.Quota.Catalog(ctx)
	if e != nil {
		return nil, fail(exitConfig, "POLICY_UNAVAILABLE: bench catalog unreadable: %v", e)
	}
	pres := a.newSelPresence(ctx, s, req)
	p.capsGap = capsGap(pres.hosts, req.tags)
	p.degraded = append(p.degraded, pres.degraded...)
	// API credit is ranked from pacelever's hand-entered balance when one is
	// recorded (credit_expiry.go). Pool classes still carry the operator's
	// exclusion, and the account-bound gate remains authoritative.
	var creditViews map[string]PoolView
	var creditErr error
	if slices.ContainsFunc(rows, func(row CatalogRow) bool { return policy.isCredit(row.Pool) }) {
		creditViews, creditErr = a.Quota.Pools(ctx)
		if creditErr != nil {
			p.degraded = append(p.degraded, "credit_pool_status_unavailable")
		}
	}
	var pending []*selCand
	capGate := a.newPoolCapGate(rows)
	p.poolCaps = capGate.caps
	for _, row := range rows {
		c := &selCand{row: row, tier: -1, ignoreGrade: req.route.top()}
		roleCap := req.route.roleEffortFor(row)
		switch {
		case row.RetiredAt != nil:
			c.reason = "retired"
		case row.Profile == "" || row.ModelID == "" || row.Pool == "":
			c.reason = "catalog_incomplete"
		case !req.route.admits(row.Profile):
			c.reason = "role_filtered"
		case !validCatalogGrade(row.Grade):
			c.reason = "grade_invalid"
		case row.Grade == gradeUnrated && !req.route.top():
			c.reason = "grade_unrated"
		case !req.route.top() && gradeOrder[row.Grade] < gradeOrder[req.grade]:
			c.reason = "grade_insufficient"
		case row.Gate != "default" && row.Gate != "escalation" && row.Gate != "consult_only":
			c.reason = "gate_unknown"
		case row.Gate != "default" && !req.allowEscalation && !req.route.familyAdmitsGate(row):
			c.reason = "gate_class"
		case aboveEffort(row.Effort, roleCap) && !req.allowEscalation:
			c.reason = reasonAboveRoleEffort
		case req.effort != "" && row.Effort != req.effort:
			c.reason = "effort_mismatch"
		case !validID(row.Pool):
			c.reason = "pool_invalid"
		case slices.Contains(req.excludePools, row.Pool):
			c.reason = "pool_excluded"
		case snapshot != nil && snapshot.poolCooling(row.Pool, p.at):
			c.reason = "pool_cooling"
		default:
			if name, seat, reason := a.seatFor(row, req); seat == nil {
				c.reason = reason
			} else {
				c.seat = seat
				c.seatName = name
				// Checked after the seat rules so a candidate those rules
				// drop keeps its own reason.
				if capKey := capGate.full(ctx, row); capKey != "" {
					c.reason, c.cap = reasonPoolAtCap, capKey
				}
			}
		}
		if c.reason != "" {
			c.outcome = "dropped:" + c.reason
			p.cands = append(p.cands, c)
			continue
		}
		// A candidate is a profile/rung/account, never a provider-level pool.
		bindings := pres.bindings(c.seat, row.Pool)
		if len(bindings) == 0 {
			c.reason, c.code = "no_presence", outcomeAuthUnavailable
			if drop := pres.firstDrop(c.seat); drop != "" {
				// Every host that held the account was dropped by readiness.
				c.reason, c.code = drop, outcomeNoEligibleHost
			} else if !pres.hasHost(c.seat) {
				c.reason, c.code = "no_eligible_host", outcomeNoEligibleHost
			}
			c.outcome = "dropped:" + c.reason
			p.cands = append(p.cands, c)
			continue
		}
		for _, binding := range bindings {
			x := *c
			x.accountKey, x.hosts, x.holders = binding.key, binding.hosts, binding.holders
			p.cands = append(p.cands, &x)
			if policy.isCredit(row.Pool) {
				if creditErr != nil {
					x.reason, x.code, x.outcome = "quota_unknown", outcomeQuotaUnknown, "dropped:quota_unknown"
					continue
				}
				p.classify(&x, creditViews[row.Pool])
				if x.reason == "" {
					pending = append(pending, &x)
				}
				continue
			}
			acc := pres.account(ctx, binding.key, row.Pool, binding.holders, false)
			if why := sharedReason(binding.key, acc); why != "" || acc.Quota == nil {
				x.code = pres.accountFailureCode(binding.key)
				x.reason = strings.ToLower(x.code)
				x.outcome = "dropped:" + x.reason
				x.judgments = []quotaJudgment{pres.unknownJudgment(binding, why)}
				continue
			}
			p.classifyRung(&x, acc.Quota)
			if x.code == outcomeQuotaUnknown {
				acc = pres.account(ctx, binding.key, row.Pool, binding.holders, true)
				if sharedReason(binding.key, acc) == "" && acc.Quota != nil {
					x.reason, x.code, x.outcome = "", "", ""
					p.classifyRung(&x, acc.Quota)
				} else {
					x.code = pres.accountFailureCode(binding.key)
					x.reason, x.outcome = strings.ToLower(x.code), "dropped:"+strings.ToLower(x.code)
				}
			}
			if x.reason == "" {
				pending = append(pending, &x)
			}
		}
	}
	p.degraded = append(p.degraded, capGate.degraded()...)
	orderSelCands(pending)
	for _, pool := range p.creditMissing {
		a.warn("credit_usd_missing pool=%s", pool)
	}
	p.ranked = pending
	pres.admitHostFirst(ctx, s, p, pending)
	p.markRoleEffort()
	if p.chosen == nil {
		return a.selectionRefused(ctx, s, req, p)
	}
	return p, nil
}

// markRoleEffort records how the consult role-effort cap shaped the
// selection. A family's role effort is the role's effort unless family_effort
// names the family. Without --allow-escalation a rung above it is never
// a candidate (above_role_effort). When its family's role-effort rung was not
// placed either — dropped by seat rules, refused at its gate (an unplaced E6
// seat), or absent from the catalog — the family is skipped
// role_effort_unplaced with that cause, so the choice falls through to the
// next family at the role effort instead of climbing. With
// --allow-escalation a chosen rung above the role effort is the escalation.
func (p *selPlan) markRoleEffort() {
	rt := p.req.route
	if rt.roleEffortCap() == "" {
		return
	}
	placed := map[string]bool{}
	causes := map[string]string{}
	for _, c := range p.cands {
		if c.row.Effort != rt.roleEffortFor(c.row) {
			continue
		}
		if c.outcome == "chosen" || c.outcome == "below_choice" {
			placed[c.row.Profile] = true
		} else if _, ok := causes[c.row.Profile]; !ok {
			causes[c.row.Profile] = c.reason
		}
	}
	cause := func(profile string) string {
		if why, ok := causes[profile]; ok && why != "" {
			return why
		}
		return "no_role_effort_rung"
	}
	if c := p.chosen; c != nil {
		roleCap := rt.roleEffortFor(c.row)
		if aboveEffort(c.row.Effort, roleCap) {
			p.escalation = map[string]any{"profile": c.row.Profile, "role_effort": roleCap, "chosen_effort": c.row.Effort, "by": "allow_escalation", "role_effort_cause": cause(c.row.Profile)}
			if placed[c.row.Profile] {
				p.escalation["role_effort_cause"] = "ranked_after"
			}
		} else if key := rt.familyEffortKey(c.row); key != "" && c.row.Effort == roleCap {
			p.familyEffort = map[string]any{"profile": c.row.Profile, "key": key, "effort": roleCap, "gate": c.row.Gate}
		}
	}
	if p.req.allowEscalation {
		return
	}
	for _, c := range p.cands {
		roleCap := rt.roleEffortFor(c.row)
		// An explicit --effort pins another rung: the role-effort rung was
		// never asked, so it cannot be called unplaced.
		if c.reason != reasonAboveRoleEffort || placed[c.row.Profile] || p.req.effort != "" && p.req.effort != roleCap {
			continue
		}
		c.reason, c.outcome = reasonRoleEffortUnplaced, "dropped:"+reasonRoleEffortUnplaced
		if !slices.ContainsFunc(p.roleSkips, func(m map[string]any) bool { return m["profile"] == c.row.Profile }) {
			p.roleSkips = append(p.roleSkips, map[string]any{"profile": c.row.Profile, "reason": reasonRoleEffortUnplaced, "role_effort": roleCap, "cause": cause(c.row.Profile)})
		}
	}
}

// orderSelCands ranks tiered candidates: waste-risk pools first, then policy
// tier, then the lowest grade that fits, then waste urgency — the unused
// projected share per hour until the pool's longest window's reset, highest
// first. A waste-risk credit pool with a dollar balance ranks by the dollars
// at risk per hour until expiry and ahead of percent-ranked waste risk; a
// credit pool without dollars keeps the percentage rule and ranks after any
// credit that has dollars. A candidate whose pool reset can be paced always
// ranks ahead of one whose cannot; candidates without urgency keep the lower
// projected use at reset (the used percentage when it cannot be paced yet),
// then smaller effort, catalog score, profile.
func orderSelCands(pending []*selCand) {
	// Credit pacing: a pool on pace to use up its longest window ranks behind
	// credit, but only when a credit pool with a recorded balance is a
	// candidate; a credit pool with no balance (or none recorded) never takes
	// work ahead of subscription quota.
	creditLeft := slices.ContainsFunc(pending, func(c *selCand) bool { return c.creditPool && c.creditLeft })
	for _, c := range pending {
		c.behindCredit = creditLeft && c.paced
	}
	slices.SortStableFunc(pending, func(x, y *selCand) int {
		// Waste-risk pools rank ahead of every tier, free included, and among
		// themselves by waste urgency first; everything else keeps the tier
		// order.
		if x.risk != y.risk {
			if x.risk {
				return -1
			}
			return 1
		}
		if x.behindCredit != y.behindCredit {
			if y.behindCredit {
				return -1
			}
			return 1
		}
		if x.risk {
			// A dollar credit ranks ahead of percent-ranked waste risk, and
			// within each group the higher urgency wins.
			if (x.usdUrgency != nil) != (y.usdUrgency != nil) {
				if x.usdUrgency != nil {
					return -1
				}
				return 1
			}
			if x.usdUrgency != nil {
				if c := cmp.Compare(*y.usdUrgency, *x.usdUrgency); c != 0 {
					return c
				}
			} else if c := cmp.Compare(*y.urgency, *x.urgency); c != 0 {
				return c
			}
		} else {
			if x.tier != y.tier {
				return x.tier - y.tier
			}
			// Among credit pools a dollar balance outranks a percentage-only
			// entry and is ordered by dollars at risk per hour.
			if x.creditPool && y.creditPool {
				if (x.usdUrgency != nil) != (y.usdUrgency != nil) {
					if x.usdUrgency != nil {
						return -1
					}
					return 1
				}
				if x.usdUrgency != nil {
					if c := cmp.Compare(*y.usdUrgency, *x.usdUrgency); c != 0 {
						return c
					}
				}
			}
		}
		// Grade fit: the lowest grade that satisfies the task.
		if gx, gy := gradeOrder[x.row.Grade], gradeOrder[y.row.Grade]; !x.ignoreGrade && gx != gy {
			return gx - gy
		}
		// Paceable resets rank ahead of unpaceable ones: mixing the urgency
		// and projected orders in one comparison is not a strict weak
		// ordering.
		if (x.urgency != nil) != (y.urgency != nil) {
			if x.urgency != nil {
				return -1
			}
			return 1
		}
		if x.urgency != nil {
			if c := cmp.Compare(*y.urgency, *x.urgency); c != 0 {
				return c
			}
		}
		if c := cmp.Compare(rankBlocking(firstPct(x.longProj, x.blocking)), rankBlocking(firstPct(y.longProj, y.blocking))); c != 0 {
			return c
		}
		if x.ignoreGrade {
			if c := cmp.Compare(topProfileRank(x.row), topProfileRank(y.row)); c != 0 {
				return c
			}
		}
		if ex, ey := rankEffort(x.row.Effort), rankEffort(y.row.Effort); ex != ey {
			return ex - ey
		}
		if c := cmp.Compare(rankScore(x.row.Score), rankScore(y.row.Score)); c != 0 {
			return c
		}
		if c := cmp.Compare(x.row.Profile, y.row.Profile); c != 0 {
			return c
		}
		return cmp.Compare(x.row.Effort, y.row.Effort)
	})
}

// horizonClause renders the optional waste-risk horizon for the rule text.
func horizonClause(h float64) string {
	if h > 0 {
		return fmt.Sprintf(", resetting within %gh", h)
	}
	return ", any reset distance"
}

func firstPct(a, b *float64) *float64 {
	if a != nil {
		return a
	}
	return b
}
func rankBlocking(b *float64) float64 {
	if b == nil {
		return 1e9
	}
	return *b
}
func rankScore(s *float64) float64 {
	if s == nil {
		return -1e9
	}
	return -(*s)
}
func gatePool(gate string) string {
	if strings.HasPrefix(strings.TrimSpace(gate), "{") {
		var receipt struct {
			Pool string `json:"provider_id"`
		}
		if json.Unmarshal([]byte(gate), &receipt) == nil {
			return receipt.Pool
		}
		return ""
	}
	for _, word := range strings.Fields(strings.SplitN(gate, "\n", 2)[0]) {
		if w, ok := strings.CutPrefix(word, "pool="); ok {
			return w
		}
	}
	return ""
}

// record renders the selection decision embedded in the job.placement event:
// the request, the policy version, every catalog rung's outcome and the
// chosen seat.
func (p *selPlan) record() map[string]any {
	cands := []any{}
	for _, c := range p.cands {
		row := map[string]any{"profile": c.row.Profile, "effort": c.row.Effort, "grade": c.row.Grade, "pool": c.row.Pool, "gate": c.row.Gate, "model_id": c.row.ModelID, "outcome": c.outcome}
		row["account_key"], row["host"], row["outcome_code"] = c.accountKey, c.host, c.code
		if c.seatName != "" {
			row["seat"] = c.seatName
		}
		if c.tierName != "" {
			row["tier"] = c.tierName
		}
		if c.reason != "" {
			row["reason"] = c.reason
		}
		if c.cap != "" {
			row["cap"] = c.cap
		}
		if c.projected != nil {
			row["projected_pct"] = *c.projected
		}
		if c.blocking != nil {
			row["used_pct"] = *c.blocking
		}
		row["waste_risk"] = c.risk
		if c.wasteGroup != "" {
			row["waste_group"] = c.wasteGroup
		}
		if c.longProj != nil {
			row["long_projected_pct"] = *c.longProj
		}
		if c.paced {
			row["paced"] = true
			row["behind_credit"] = c.behindCredit
		}
		if c.unused != nil {
			row["unused_at_reset_pct"] = *c.unused
		}
		if c.credit != nil {
			row["waste_reason"] = "credit_expiring"
			row["credit"] = c.credit.record()
		}
		if c.tier >= 0 {
			// Ranked candidates state how they were ordered: by dollars at
			// risk per hour for a dollar credit, else by waste urgency when the
			// pool's longest window's reset was paceable, else by the
			// projected-at-reset fallback.
			switch {
			case c.usdUrgency != nil:
				row["usd_urgency"] = *c.usdUrgency
				row["waste_order"] = "usd"
			case c.urgency != nil:
				row["waste_urgency"] = *c.urgency
				row["waste_order"] = "reset"
			default:
				row["waste_order"] = "projected_fallback"
			}
			if c.resetHrs != nil {
				row["reset_hours"] = *c.resetHrs
			}
		}
		if len(c.judgments) > 0 {
			row["quota_gates"] = c.judgments
		}
		cands = append(cands, row)
	}
	tags := p.req.tags
	if tags == nil {
		tags = []string{}
	}
	rec := map[string]any{
		"routing_role":   routingRole(p.req),
		"outcome_code":   p.outcome,
		"mode":           "auto",
		"decision":       fmt.Sprintf("urgency=%s source=%s tiers=%s", p.req.urgency, p.req.urgencySource, strings.Join(p.order, ">")),
		"rule":           fmt.Sprintf("hosts first; within each host, waste-risk pools rank by waste urgency (unused share per hour until reset) and expiring credit by dollars at risk per hour until expiry, then tier, lowest grade >= %s, waste urgency and smaller effort; waste < %g%%, excluded >= %g%% projected", p.req.grade, *p.policy.WasteBelow, *p.policy.ExcludeAt),
		"waste_risk":     map[string]any{"horizon_hours": *p.policy.WasteRisk.HorizonHours, "min_unused_pct": *p.policy.WasteRisk.MinUnusedPct},
		"policy_version": p.policy.Version,
		"policy_source":  p.policy.SourceDoc,
		"requested":      map[string]any{"grade": p.req.grade, "t": p.req.t, "role": p.req.role, "tags": tags, "effort": p.req.effort, "allow_escalation": p.req.allowEscalation},
		"urgency":        p.req.urgency,
		"urgency_source": p.req.urgencySource,
		"tiers":          p.order,
		"candidates":     cands,
		"at":             p.at.Format(time.RFC3339Nano),
	}
	if c := p.chosen; c != nil {
		canon := c.row.Profile
		if c.seat != nil {
			canon = c.seat.Canon
		}
		chosen := map[string]any{"seat": c.seatName, "canon": canon, "effort": c.row.Effort, "model_id": c.row.ModelID, "pool": c.row.Pool, "gate": c.row.Gate, "tier": c.tierName, "grade": c.row.Grade, "waste_risk": c.risk}
		if c.wasteGroup != "" {
			chosen["waste_group"] = c.wasteGroup
		}
		chosen["account_key"], chosen["host"], chosen["outcome_code"] = c.accountKey, c.host, c.code
		rec["account_key"], rec["host"] = c.accountKey, c.host
		why := fmt.Sprintf("tier=%s grade=%s for task %s", c.tierName, c.row.Grade, p.req.grade)
		if c.risk {
			why = "waste_risk " + why
		}
		if c.credit != nil {
			why += " " + c.credit.note()
			chosen["waste_reason"], chosen["credit"] = "credit_expiring", c.credit.record()
		}
		if c.projected != nil {
			chosen["projected_pct"] = *c.projected
			why += fmt.Sprintf(" projected=%.0f%%", *c.projected)
		}
		if c.urgency != nil {
			chosen["waste_urgency"] = *c.urgency
			why += fmt.Sprintf(" waste_urgency=%.2f", *c.urgency)
		}
		if c.usdUrgency != nil {
			chosen["usd_urgency"] = *c.usdUrgency
		}
		chosen["why"] = why
		rec["chosen"] = chosen
	}
	if len(p.degraded) > 0 {
		rec["degraded"] = p.degraded
	}
	if p.hosts != nil {
		hosts := []any{}
		order := []string{}
		for _, t := range p.hosts {
			hosts = append(hosts, t.record())
			if t.rank > 0 && !slices.Contains(order, t.host.name) {
				order = append(order, t.host.name)
			}
		}
		rec["hosts"] = hosts
		rec["host_order"] = order
		rec["host_rule"] = "hosts that fit (caps, roles, grades, hub availability) ranked by load bucket, then hub score or load; on each host its present accounts in pool order; first admitted pair wins; credit, and pools ranked behind credit, are a last band tried on the hosts again (pass 2) only when no other account was admitted on any host (pass 1); expiring credit with dollars at risk is waste-risk and stays in pass 1"
	}
	if p.req.route != nil {
		rt := p.req.route.record(p.routeRefusal)
		if p.before != nil {
			// The role's own attempt, every profile exhausted, before the
			// downgrade_to retry this record chose from.
			rt["before_downgrade"] = p.before.record()["candidates"]
		}
		if len(p.roleSkips) > 0 {
			rt["skipped"] = p.roleSkips
		}
		if p.escalation != nil {
			rt["escalation"] = p.escalation
		}
		if p.familyEffort != nil {
			rt["family_effort_chosen"] = p.familyEffort
		}
		rec["routing"] = rt
	}
	return rec
}

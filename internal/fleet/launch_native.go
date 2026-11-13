package fleet

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
)

const launchModeNative = "native"
const launchModeShadow = "shadow" // retired config value retained for migration diagnostics

func (a *App) launchMode() string { return launchModeNative }

func validateLaunchMode(quota map[string]any) error {
	v, ok := quota["launch"]
	if !ok {
		return nil
	}
	if s, isString := v.(string); !isString || s != launchModeNative {
		return fail(exitConfig, "quota.launch에는 %q만 지정할 수 있습니다", launchModeNative)
	}
	return nil
}

// Launch resolves each request with panewire's native lookup.
func (q admissionQuota) Launch(ctx context.Context, r QuotaRequest) (Launch, error) {
	return q.app.nativeLaunch(ctx, r)
}

// nativeLaunch resolves a request from the live handoffkeep catalog and
// policy/model-families. A missing profile refuses; family policy is a
// best-effort pool-family override and falls back to the live row's family if
// the document cannot be read. A stale catalog is refused after its row rules
// are checked. No pacelever process or bundled catalog is used.
//
// panewire's config.toml is loaded and validated with the rules ported from
// pacelever (launch_config.go); [bench] catalog_ttl_s and
// catalog_stale_max_s set the catalog's freshness, as they do pacelever's.
// The catalog is panewire's client and cache (catalog.go). A lookup a cached
// catalog refuses reads handoffkeep once more before it refuses, so a rung
// added there is found by the next lookup instead of an hour later; that
// refresh replaces the cache the selection also reads, which is the host's
// choice in setting native.
func (a *App) nativeLaunch(ctx context.Context, r QuotaRequest) (Launch, error) {
	if r.Profile == "" {
		return Launch{}, fail(gateInvalid, "quota catalog: profile required")
	}
	policy, err := loadLaunchPolicy(os.Getenv, a.Home)
	if err != nil {
		return Launch{}, fail(gateInvalid, "quota catalog: %v", err)
	}
	families, err := a.modelFamiliesPolicy(ctx)
	if err != nil {
		if a.Err != nil {
			a.warn("quota catalog: model-family policy unavailable; using live catalog family: %v", err)
		}
		families = ModelFamiliesPolicy{}
	}
	catalog := a.launchCatalog(ctx, policy.Freshness, false, families)
	return decideLaunch(policy.admissionPolicy, a.Policy.Profiles, launchRequest(r), catalog, func() launchCatalog {
		return a.launchCatalog(ctx, policy.Freshness, true, families)
	})
}

// offlineLaunch is the native lookup the shadow compares with pacelever's:
// nativeLaunch over panewire's catalog cache alone, read only, with no
// refresh. The cache's freshness is pacelever's [bench] settings, as
// nativeLaunch reads them, so the two agree on which cache is fresh. found is
// false when there is no fresh cache entry to resolve against.
func (a *App) offlineLaunch(r QuotaRequest) (l Launch, found bool, err error) {
	if r.Profile == "" {
		return Launch{}, true, fail(gateInvalid, "quota catalog: profile required")
	}
	policy, err := loadLaunchPolicy(os.Getenv, a.Home)
	if err != nil {
		return Launch{}, true, fail(gateInvalid, "quota catalog: %v", err)
	}
	catalog, found := a.cachedLaunchCatalog(policy.Freshness)
	if !found {
		return Launch{}, false, nil
	}
	l, err = decideLaunch(policy.admissionPolicy, a.Policy.Profiles, launchRequest(r), catalog, nil)
	return l, true, err
}

// launchRequest applies the request's marker, then the panewire environment
// override and its retired pacelever name as fallback. A locked request (a
// hub submission or a node-delivered spawn) reads no environment: its marker
// is already authoritative.
func launchRequest(r QuotaRequest) admissionRequest {
	e6Arm := ""
	if !r.E6ArmLocked {
		e6Arm = strings.TrimSpace(os.Getenv("PANEWIRE_E6_ARM"))
		if e6Arm == "" {
			e6Arm = os.Getenv("PACELEVER_E6_ARM")
		}
	}
	return admissionRequest{Profile: r.Profile, Effort: r.Effort, Role: r.Role, Purpose: r.Purpose, OperatorRequest: r.OperatorRequest, RequestedBy: r.RequestedBy, E6Arm: strings.TrimSpace(firstNonempty(r.E6Arm, e6Arm))}
}

// decideLaunch resolves req against catalog. A refusal from a cached catalog
// is resolved once more against refetch's catalog when refetch is set. A
// refusal exits 3; a launch from a stale catalog exits 70.
func decideLaunch(policy admissionPolicy, seats map[string]Seat, req admissionRequest, catalog launchCatalog, refetch func() launchCatalog) (Launch, error) {
	l, why := catalog.resolve(policy, seats, req)
	if why != "" && catalog.cached && refetch != nil {
		catalog = refetch()
		l, why = catalog.resolve(policy, seats, req)
	}
	if why != "" {
		return Launch{}, fail(gateBlocked, "quota catalog: %s", why)
	}
	if l.Catalog.Stale {
		return l, fail(exitConfig, "quota catalog is stale")
	}
	return l, nil
}

// launchCatalog is the live handoffkeep catalog with placement derived from
// grade and family identity read from policy/model-families.
type launchCatalog struct {
	Rows  []launchRow
	Stale bool
	// cached marks rows served from a cache entry younger than the TTL,
	// without asking handoffkeep.
	cached bool
}

// launchRow is one live bench-catalog rung as launch reads it.
type launchRow struct {
	admissionRow
	Family string
}

func launchRowFrom(r CatalogRow) launchRow {
	return launchRow{admissionRow: admissionRow{Profile: r.Profile, Effort: r.Effort, ModelID: r.ModelID, Pool: r.Pool, Grade: r.Grade, Gate: r.Gate, Placed: r.Grade != gradeUnrated, Retired: r.RetiredAt != nil}, Family: r.Family}
}

// launchCatalog reads the catalog: a.BenchCatalog when set (a fixture), else
// handoffkeep through the catalog client and its cache, with freshness's
// limits. fresh skips the cache.
func (a *App) launchCatalog(ctx context.Context, freshness CatalogFreshness, fresh bool, families ModelFamiliesPolicy) launchCatalog {
	stale := launchCatalog{Stale: true}
	if a.BenchCatalog != nil {
		rows, err := a.BenchCatalog(ctx)
		if err != nil {
			return stale
		}
		return overlayLaunchCatalog(rows, false, families)
	}
	client, err := NewCatalogClient(CatalogClientConfig{Now: a.Now, Freshness: freshness})
	if err != nil {
		return stale
	}
	data, err := client.Read(ctx, fresh)
	if err != nil {
		return stale
	}
	if data.Info.Source == catalogSourceServer {
		for _, warning := range data.Warnings {
			if strings.HasPrefix(warning, "catalog_row_invalid:") {
				a.warn("%s", warning)
			}
		}
	}
	c := overlayLaunchCatalog(catalogDataRows(data), data.Info.Stale, families)
	c.cached = data.Info.Source == catalogSourceCache && data.Info.AgeS != nil && client.fresh(*data.Info.AgeS)
	return c
}

// cachedLaunchCatalog is the launch catalog from panewire's catalog cache
// alone, when it holds an entry fresh under freshness (catalog.go
// ReadCached). It never asks handoffkeep and never writes, and it ignores
// a.BenchCatalog: it is what the shadow comparison resolves against.
func (a *App) cachedLaunchCatalog(freshness CatalogFreshness) (launchCatalog, bool) {
	client, err := NewCatalogClient(CatalogClientConfig{Now: a.Now, Freshness: freshness})
	if err != nil {
		return launchCatalog{}, false
	}
	data, ok := client.ReadCached()
	if !ok {
		return launchCatalog{}, false
	}
	c := overlayLaunchCatalog(catalogDataRows(data), false, ModelFamiliesPolicy{})
	c.cached = true
	return c, true
}

func catalogDataRows(data CatalogData) []CatalogRow {
	rows := make([]CatalogRow, len(data.Rows))
	for i, entry := range data.Rows {
		rows[i] = entry.row()
	}
	return rows
}

// overlayLaunchCatalog projects live catalog rows into launch rows. Placement
// is derived from grade; the family policy names families by quota pool.
func overlayLaunchCatalog(rows []CatalogRow, stale bool, familyPolicies ...ModelFamiliesPolicy) launchCatalog {
	c := launchCatalog{Stale: stale}
	var families ModelFamiliesPolicy
	if len(familyPolicies) > 0 {
		families = familyPolicies[0]
	}
	for _, r := range rows {
		row := launchRowFrom(r)
		row.Family = families.familyForPool(r.Pool, row.Family)
		c.Rows = append(c.Rows, row)
	}
	return c
}

func (c launchCatalog) profileRows(profile string) []launchRow {
	var rows []launchRow
	for _, r := range c.Rows {
		if r.Profile == profile && !r.Retired {
			rows = append(rows, r)
		}
	}
	return rows
}

// profileCredit follows admissionCredit: a pi- profile or any profile with a
// commandcode-pool rung is a credit profile.
func (c launchCatalog) profileCredit(profile string) bool {
	if admissionCredit(profile, "") || profile == "cmd-dsflash41" {
		return true
	}
	return slices.ContainsFunc(c.profileRows(profile), func(r launchRow) bool {
		return admissionCredit(profile, r.Pool)
	})
}

// resolve returns the launch a request resolves to, or the reason it is
// refused. seats is the embedded seat policy: a profile's
// no-thinking property is read there, never from a catalog row.
func (c launchCatalog) resolve(policy admissionPolicy, seats map[string]Seat, req admissionRequest) (Launch, string) {
	row, resolved, why := c.resolveRow(req, admissionUnratedRole(req), seats)
	if why != "" {
		return Launch{}, why
	}
	if why := launchRoleCheck(req, row); why != "" {
		return Launch{}, why
	}
	if req.OperatorRequest != "" && !admissionValidRef(req.OperatorRequest) {
		return Launch{}, "operator_request_ref_invalid"
	}
	if !policy.subscribed(req.Profile, row.Pool) {
		return Launch{}, "profile is unsubscribed"
	}
	if row.Gate == consultOnlyGate && req.OperatorRequest == "" && req.Profile != "codex-astra" {
		return Launch{}, "profile is consult_only; pass --operator-request"
	}
	if c.Stale && row.Gate != defaultGate && row.Gate != consultOnlyGate && req.OperatorRequest == "" {
		return Launch{}, "catalog is stale; non-default gate requires --operator-request"
	}
	l := Launch{Profile: req.Profile, ModelID: row.ModelID, Effort: resolved, Grade: row.Grade, Gate: row.Gate, Pool: row.Pool, Family: row.Family}
	l.Catalog.Stale = c.Stale
	return l, ""
}

// resolveRow is pacelever's resolveRow: the rung a request names and the
// effort it resolves to.
func (c launchCatalog) resolveRow(req admissionRequest, allowUnrated bool, seats map[string]Seat) (launchRow, string, string) {
	profile := req.Profile
	requested := admissionNormalize(req.Effort)
	if c.profileCredit(profile) && requested == "" {
		return launchRow{}, "", "explicit --effort required for credit profile"
	}
	if requested != "" && admissionEffortRank(requested) == 99 && !slices.ContainsFunc(c.Rows, func(r launchRow) bool { return r.Profile == profile && r.Effort == requested }) {
		return launchRow{}, "", "unknown effort rung"
	}
	if c.profileCredit(profile) {
		allowed := []string{"low", "high", "max"}
		if requested == "off" {
			allowed = nil
			if seats[profile].NoThinking && slices.ContainsFunc(c.profileRows(profile), func(r launchRow) bool { return r.Effort == "off" && r.Pool == commandcodePool }) {
				allowed = []string{"off"}
			}
		}
		if strings.HasPrefix(profile, "pi-") {
			allowed = []string{"off", "minimal", "low", "medium", "high", "xhigh", "max"}
		}
		if !slices.Contains(allowed, requested) {
			return launchRow{}, "", "credit effort is not placed"
		}
	}
	if slices.Contains([]string{"codex-med", "builder-terra-high", "builder-terra-xhigh", "builder-terra-max"}, profile) ||
		slices.ContainsFunc(c.Rows, func(r launchRow) bool { return r.Profile == profile && r.ModelID == "gpt-5.6-terra" }) {
		return launchRow{}, "", "model_refused: retired model identity"
	}
	rows := c.profileRows(profile)
	if len(rows) == 0 {
		return launchRow{}, "", fmt.Sprintf("profile %q is unknown or retired", profile)
	}
	if requested == "" && strings.HasPrefix(req.E6Arm, profile+"@") && !slices.ContainsFunc(rows, func(r launchRow) bool { return r.Placed }) {
		requested = strings.TrimPrefix(req.E6Arm, profile+"@")
	}
	var eligible []launchRow
	for _, r := range rows {
		marker := admissionMarkerRequired(r.admissionRow, allowUnrated)
		if requested == r.Effort && r.Grade == gradeUnrated && marker && req.E6Arm != profile+"@"+r.Effort {
			return launchRow{}, "", "e6_arm_required: exact measurement marker required"
		}
		if !marker || req.E6Arm == profile+"@"+r.Effort {
			eligible = append(eligible, r)
		}
	}
	if len(eligible) == 0 {
		return launchRow{}, "", "e6_arm_required: unplaced profile requires exact measurement marker"
	}
	fallback := launchDefaultEffort(eligible)
	if fallback == "" {
		return launchRow{}, "", "catalog row has no launch effort"
	}
	resolved := requested
	if resolved == "" {
		resolved = fallback
	}
	if slices.ContainsFunc(c.Rows, func(r launchRow) bool { return r.Profile == profile && r.Effort == resolved && r.Retired }) {
		return launchRow{}, "", "rung is retired in the catalog"
	}
	i := slices.IndexFunc(eligible, func(r launchRow) bool { return r.Effort == resolved })
	if i < 0 {
		if c.profileCredit(profile) || profile == "codex-terra61" {
			return launchRow{}, "", "e6_arm_required: rung is not placed"
		}
		return launchRow{}, "", "rung is not in the catalog"
	}
	if i < 0 {
		return launchRow{}, "", "rung is not in the catalog"
	}
	chosen := eligible[i]
	if admissionMarkerRequired(chosen.admissionRow, allowUnrated) && req.E6Arm != profile+"@"+chosen.Effort {
		return launchRow{}, "", "e6_arm_required"
	}
	if chosen.Effort == "" {
		return launchRow{}, "", "catalog row has no launch effort"
	}
	return chosen, chosen.Effort, ""
}

// launchDefaultEffort selects the best graded ordinary rung at the lowest
// effort. If no ordinary rung exists, it selects the best available live row.
func launchDefaultEffort(rows []launchRow) (fallback string) {
	var ordinary []launchRow
	for _, r := range rows {
		if r.Effort != "" && r.Gate == defaultGate && (r.Placed || r.Grade == gradeUnrated) {
			ordinary = append(ordinary, r)
		}
	}
	if len(ordinary) == 0 {
		for _, r := range rows {
			if r.Effort != "" {
				ordinary = append(ordinary, r)
			}
		}
	}
	sort.SliceStable(ordinary, func(i, j int) bool {
		a, b := ordinary[i], ordinary[j]
		if admissionGradeRank(a.Grade) != admissionGradeRank(b.Grade) {
			return admissionGradeRank(a.Grade) < admissionGradeRank(b.Grade)
		}
		return admissionEffortRank(a.Effort) < admissionEffortRank(b.Effort)
	})
	if len(ordinary) == 0 {
		return ""
	}
	return ordinary[0].Effort
}

// launchRoleCheck is pacelever's roleCheck with its credit() rule.
func launchRoleCheck(req admissionRequest, row launchRow) string {
	role := admissionNormalize(req.Role)
	if admissionCredit(req.Profile, row.Pool) && role != "worker" && role != "tester" && (role != "builder" || !row.Placed || row.Gate != defaultGate) {
		return "role_restricted: credit rung requires worker/tester; placed default rungs also allow builder"
	}
	if strings.Contains(req.Profile, "astra") && req.Profile != "codex-astra" {
		return "role_restricted: removed astra seat alias"
	}
	return ""
}

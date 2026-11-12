package fleet

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
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

// nativeLaunch is `pacelever policy launch PROFILE --json` as CLIQuota.Launch
// reads it, answered from panewire's catalog: pacelever v0.3.28's
// resolveLaunch (catalog.go) over the catalog its policy launch reads
// (bench.go). A refusal exits 3 with pacelever's reason, as pacelever exits 3
// with that error; a missing profile or an unreadable pool policy exits 2; a
// stale catalog is refused as CLIQuota.Launch refuses it. No pacelever process
// runs.
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
	catalog := a.launchCatalog(ctx, policy.Freshness, false)
	return decideLaunch(policy.admissionPolicy, a.Policy.Profiles, launchRequest(r), catalog, func() launchCatalog {
		return a.launchCatalog(ctx, policy.Freshness, true)
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
// override and its retired pacelever name as fallback.
func launchRequest(r QuotaRequest) admissionRequest {
	e6Arm := strings.TrimSpace(os.Getenv("PANEWIRE_E6_ARM"))
	if e6Arm == "" {
		e6Arm = os.Getenv("PACELEVER_E6_ARM")
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

// launchCatalog is the catalog pacelever's policy launch resolves against:
// handoffkeep's rows with the local fields pacelever's bundled snapshot adds
// (bench.go decodeCatalog), the snapshot's rows for profiles handoffkeep does
// not list (bench.go ReadCatalog), and the snapshot's effort defaults and
// aliases (catalog.go). Without a readable catalog it is the snapshot alone,
// stale, as in pacelever.
type launchCatalog struct {
	Rows     []launchRow
	Defaults map[string]string
	Aliases  map[string]string
	Stale    bool
	// cached marks rows served from a cache entry younger than the TTL,
	// without asking handoffkeep.
	cached bool
}

// launchRow is one rung as the lookup reads it: the gate's row plus the
// family pacelever's snapshot names for it.
type launchRow struct {
	admissionRow
	Family string
}

// catalogSnapshotJSON is pacelever v0.3.28's catalog_snapshot.json, byte for
// byte: the local fields (placement, family), effort defaults and aliases that
// pacelever's own catalog adds to handoffkeep's rows. Its agy-flash row
// (Gemini 3.7) is dropped: that rung is retired (hk 3859).
//
//go:embed catalog-snapshot.json
var catalogSnapshotJSON []byte

var catalogSnapshot = sync.OnceValue(func() launchCatalog {
	var doc struct {
		Rows []struct {
			CatalogRow
			Placed bool   `json:"placed"`
			Family string `json:"family"`
		} `json:"rows"`
		Defaults map[string]string `json:"defaults"`
		Aliases  map[string]string `json:"aliases"`
	}
	if err := json.Unmarshal(catalogSnapshotJSON, &doc); err != nil {
		panic(err)
	}
	c := launchCatalog{Defaults: doc.Defaults, Aliases: doc.Aliases}
	for _, r := range doc.Rows {
		row := launchRowFrom(r.CatalogRow)
		row.Placed, row.Family = r.Placed, r.Family
		c.Rows = append(c.Rows, row)
	}
	return c
})

func launchRowFrom(r CatalogRow) launchRow {
	return launchRow{admissionRow: admissionRow{Profile: r.Profile, Effort: r.Effort, ModelID: r.ModelID, Pool: r.Pool, Grade: r.Grade, Gate: r.Gate, Placed: r.Grade != gradeUnrated, Retired: r.RetiredAt != nil}, Family: r.Family}
}

// launchCatalog reads the catalog: a.BenchCatalog when set (a fixture), else
// handoffkeep through the catalog client and its cache, with freshness's
// limits. fresh skips the cache.
func (a *App) launchCatalog(ctx context.Context, freshness CatalogFreshness, fresh bool) launchCatalog {
	stale := catalogSnapshot()
	stale.Stale = true
	if a.BenchCatalog != nil {
		rows, err := a.BenchCatalog(ctx)
		if err != nil {
			return stale
		}
		return overlayLaunchCatalog(rows, false)
	}
	client, err := NewCatalogClient(CatalogClientConfig{Now: a.Now, Freshness: freshness})
	if err != nil {
		return stale
	}
	data, err := client.Read(ctx, fresh)
	if err != nil {
		return stale
	}
	c := overlayLaunchCatalog(catalogDataRows(data), data.Info.Stale)
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
	c := overlayLaunchCatalog(catalogDataRows(data), false)
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

// overlayLaunchCatalog is handoffkeep's rows with the bundled snapshot's
// local fields and the snapshot's rows for profiles the rows do not list.
func overlayLaunchCatalog(rows []CatalogRow, stale bool) launchCatalog {
	snapshot := catalogSnapshot()
	c := launchCatalog{Defaults: snapshot.Defaults, Aliases: snapshot.Aliases, Stale: stale}
	listed := map[string]bool{}
	for _, r := range rows {
		row := launchRowFrom(r)
		// A rung the snapshot knows takes its family, and stays unplaced
		// when the snapshot holds it unplaced at the same grade.
		if i := slices.IndexFunc(snapshot.Rows, func(old launchRow) bool { return old.Profile == r.Profile && old.Effort == r.Effort }); i >= 0 {
			old := snapshot.Rows[i]
			row.Family = old.Family
			if !old.Placed && r.Grade == old.Grade {
				row.Placed = false
			}
		}
		listed[r.Profile] = true
		c.Rows = append(c.Rows, row)
	}
	for _, old := range snapshot.Rows {
		if !listed[old.Profile] {
			c.Rows = append(c.Rows, old)
		}
	}
	return c
}

func (c launchCatalog) canonical(profile string) string {
	if v := c.Aliases[profile]; v != "" {
		return v
	}
	return profile
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

// resolve is pacelever's resolveLaunch: the launch a request resolves to, or
// the reason it is refused. seats is the embedded seat policy: a profile's
// no-thinking property is read there, never from a catalog row.
func (c launchCatalog) resolve(policy admissionPolicy, seats map[string]Seat, req admissionRequest) (Launch, string) {
	row, resolved, why := c.resolveRow(req, admissionUnratedRole(req), seats)
	if why != "" {
		return Launch{}, why
	}
	req.Profile = c.canonical(req.Profile)
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
	profile := c.canonical(req.Profile)
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
	fallback, resolved := launchDefaultEffort(profile, eligible, c.Defaults)
	if requested != "" {
		resolved = requested
	}
	if slices.ContainsFunc(c.Rows, func(r launchRow) bool { return r.Profile == profile && r.Effort == resolved && r.Retired }) {
		return launchRow{}, "", "rung is retired in the catalog"
	}
	i := slices.IndexFunc(eligible, func(r launchRow) bool { return r.Effort == resolved })
	if i < 0 {
		if c.profileCredit(profile) || profile == "codex-terra61" {
			return launchRow{}, "", "e6_arm_required: rung is not placed"
		}
		i = slices.IndexFunc(eligible, func(r launchRow) bool { return r.Effort == fallback })
	}
	if i < 0 {
		return launchRow{}, "", "rung is not in the catalog"
	}
	chosen := eligible[i]
	if admissionMarkerRequired(chosen.admissionRow, allowUnrated) && req.E6Arm != profile+"@"+resolved {
		return launchRow{}, "", "e6_arm_required"
	}
	return chosen, resolved, ""
}

// launchDefaultEffort is pacelever's defaultEffort: the effort an effortless
// request resolves to — the profile's default from the snapshot when it is an
// ordinary rung, else the best graded ordinary rung at the lowest effort. A
// rungless best rung resolves to the default effort but falls back to itself.
func launchDefaultEffort(profile string, rows []launchRow, defaults map[string]string) (fallback, resolved string) {
	preferred := defaults[profile]
	var ordinary []launchRow
	for _, r := range rows {
		if r.Gate == defaultGate && (r.Placed || r.Grade == gradeUnrated) {
			ordinary = append(ordinary, r)
			if preferred != "" && r.Effort == preferred {
				return preferred, preferred
			}
		}
	}
	if len(ordinary) == 0 {
		ordinary = append(ordinary, rows...)
	}
	sort.SliceStable(ordinary, func(i, j int) bool {
		a, b := ordinary[i], ordinary[j]
		if admissionGradeRank(a.Grade) != admissionGradeRank(b.Grade) {
			return admissionGradeRank(a.Grade) < admissionGradeRank(b.Grade)
		}
		return admissionEffortRank(a.Effort) < admissionEffortRank(b.Effort)
	})
	if len(ordinary) == 0 {
		return "", ""
	}
	if ordinary[0].Effort == "" {
		return "", preferred
	}
	return ordinary[0].Effort, ordinary[0].Effort
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

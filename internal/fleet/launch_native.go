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
	"time"
)

// The launch lookup switch (docs/quota.md §Launch). `[quota].launch` in the
// host's fleet config picks who resolves a launch rung — the profile, effort,
// model, grade, gate, pool and family a spawn runs:
//
//   - shadow (the default): `pacelever policy launch` resolves it. After it
//     answers, panewire resolves the same request offline — from pacelever's
//     config.toml and panewire's catalog cache, read only — and records
//     whether the two agree (fleet event quota_launch_shadow). The comparison
//     never asks handoffkeep, never writes the cache, never runs on the
//     caller's context and never changes the spawn's outcome.
//   - native: panewire resolves it and `pacelever policy launch` is never run.
//
// panewire-design flips the switch once the shadow events show agreement.
const (
	launchModeShadow = "shadow"
	launchModeNative = "native"
)

// shadowBound bounds a shadow comparison. The comparison reads only local
// files, so it takes milliseconds; the bound keeps a hung filesystem from
// holding up the spawn it rides on.
const shadowBound = 2 * time.Second

// shadowReadHook, when set, runs in each shadow comparison's goroutine just
// before its local reads. Tests use it to hold a comparison as a hung
// filesystem would and to count the comparisons started; production leaves it
// nil.
var shadowReadHook func()

// shadowRun runs read for a shadow comparison on ctx — which the caller
// detaches from its own context — bounded by shadowBound. ok is false when the
// bound passes first; read then runs on to its end and its answer is dropped.
func shadowRun[T any](ctx context.Context, read func() T) (answer T, ok bool) {
	ctx, cancel := context.WithTimeout(ctx, shadowBound)
	defer cancel()
	done := make(chan T, 1)
	go func() {
		if hook := shadowReadHook; hook != nil {
			hook()
		}
		done <- read()
	}()
	select {
	case answer = <-done:
		return answer, true
	case <-ctx.Done():
		return answer, false
	}
}

func validateLaunchMode(quota map[string]any) error {
	v, ok := quota["launch"]
	if !ok {
		return nil
	}
	if s, isString := v.(string); !isString || s != launchModeShadow && s != launchModeNative {
		return fail(exitConfig, "quota.launch must be %q or %q", launchModeShadow, launchModeNative)
	}
	return nil
}

func (a *App) launchMode() string {
	if str(a.Config.Quota, "launch") == launchModeNative {
		return launchModeNative
	}
	return launchModeShadow
}

// Launch resolves the launch rung following the switch. In shadow mode the
// answer is pacelever's, computed before the comparison starts; the
// comparison gets a context detached from the caller's.
func (q admissionQuota) Launch(ctx context.Context, r QuotaRequest) (Launch, error) {
	if q.app.launchMode() == launchModeNative {
		return q.app.nativeLaunch(ctx, r)
	}
	x := q.CLIQuota.runLaunch(ctx, r)
	l, err := launchResult(x)
	q.app.shadowLaunch(context.WithoutCancel(ctx), r, x)
	return l, err
}

// nativeLaunch is `pacelever policy launch PROFILE --json` as CLIQuota.Launch
// reads it, answered from panewire's catalog: pacelever v0.3.28's
// resolveLaunch (catalog.go) over the catalog its policy launch reads
// (bench.go). A refusal exits 3 with pacelever's reason, as pacelever exits 3
// with that error; a missing profile or an unreadable pool policy exits 2; a
// stale catalog is refused as CLIQuota.Launch refuses it. No pacelever process
// runs.
//
// pacelever's config.toml is loaded and validated as pacelever loads it
// (pacelever_config.go), and its [bench] catalog_ttl_s and
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
	return decideLaunch(policy.admissionPolicy, launchRequest(r), catalog, func() launchCatalog {
		return a.launchCatalog(ctx, policy.Freshness, true)
	})
}

// offlineLaunch is the native lookup the shadow compares with pacelever's:
// nativeLaunch over panewire's catalog cache alone, read only, with no
// refresh. found is false when there is no cache entry younger than
// panewire's TTL to resolve against.
func (a *App) offlineLaunch(r QuotaRequest) (l Launch, found bool, err error) {
	if r.Profile == "" {
		return Launch{}, true, fail(gateInvalid, "quota catalog: profile required")
	}
	catalog, found := a.cachedLaunchCatalog()
	if !found {
		return Launch{}, false, nil
	}
	policy, err := loadLaunchPolicy(os.Getenv, a.Home)
	if err != nil {
		return Launch{}, true, fail(gateInvalid, "quota catalog: %v", err)
	}
	l, err = decideLaunch(policy.admissionPolicy, launchRequest(r), catalog, nil)
	return l, true, err
}

// launchRequest is the request as pacelever's policy launch reads it.
// pacelever reads the marker from PACELEVER_E6_ARM, which CLIQuota sets from
// the request and a local spawn otherwise inherits.
func launchRequest(r QuotaRequest) admissionRequest {
	return admissionRequest{Profile: r.Profile, Effort: r.Effort, Role: r.Role, Purpose: r.Purpose, OperatorRequest: r.OperatorRequest, RequestedBy: r.RequestedBy, E6Arm: strings.TrimSpace(firstNonempty(r.E6Arm, os.Getenv("PACELEVER_E6_ARM")))}
}

// decideLaunch resolves req against catalog. A refusal from a cached catalog
// is resolved once more against refetch's catalog when refetch is set. A
// refusal exits 3; a launch from a stale catalog exits 70.
func decideLaunch(policy admissionPolicy, req admissionRequest, catalog launchCatalog, refetch func() launchCatalog) (Launch, error) {
	l, why := catalog.resolve(policy, req)
	if why != "" && catalog.cached && refetch != nil {
		catalog = refetch()
		l, why = catalog.resolve(policy, req)
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
// pacelever's own catalog adds to handoffkeep's rows.
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
	return launchRow{admissionRow: admissionRow{Profile: r.Profile, Effort: r.Effort, ModelID: r.ModelID, Pool: r.Pool, Grade: r.Grade, Gate: r.Gate, Placed: r.Grade != gradeUnrated, Retired: r.RetiredAt != nil}}
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
// alone, when it holds an entry younger than panewire's TTL (catalog.go
// ReadCached). It never asks handoffkeep and never writes, and it ignores
// a.BenchCatalog: it is what the shadow comparison resolves against.
func (a *App) cachedLaunchCatalog() (launchCatalog, bool) {
	client, err := NewCatalogClient(CatalogClientConfig{Now: a.Now})
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

// launchCredit is pacelever's credit(): a pi- profile or cmd-dsflash41. The
// gate decides a credit rung by its pool instead (admissionCredit, hk 3532);
// the lookup keeps pacelever's rule so it refuses exactly what pacelever does.
func launchCredit(profile string) bool {
	return strings.HasPrefix(profile, "pi-") || profile == "cmd-dsflash41"
}

// resolve is pacelever's resolveLaunch: the launch a request resolves to, or
// the reason it is refused.
func (c launchCatalog) resolve(policy admissionPolicy, req admissionRequest) (Launch, string) {
	row, resolved, why := c.resolveRow(req, admissionUnratedRole(req))
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
func (c launchCatalog) resolveRow(req admissionRequest, allowUnrated bool) (launchRow, string, string) {
	profile := c.canonical(req.Profile)
	requested := admissionNormalize(req.Effort)
	if launchCredit(profile) && requested == "" {
		return launchRow{}, "", "explicit --effort required for credit profile"
	}
	if requested != "" && admissionEffortRank(requested) == 99 && !slices.ContainsFunc(c.Rows, func(r launchRow) bool { return r.Profile == profile && r.Effort == requested }) {
		return launchRow{}, "", "unknown effort rung"
	}
	if launchCredit(profile) {
		allowed := []string{"low", "high", "max"}
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
		if launchCredit(profile) || profile == "codex-terra61" {
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
	role, purpose := admissionNormalize(req.Role), admissionNormalize(req.Purpose)
	if launchCredit(req.Profile) && role != "worker" && role != "tester" && (role != "builder" || !row.Placed || row.Gate != defaultGate) {
		return "role_restricted: credit rung requires worker/tester; placed default rungs also allow builder"
	}
	if req.Profile == "codex-astra" || req.Profile == "fable" {
		if role == "worker" || role == "captain" {
			return "role_restricted: consult model cannot hold this seat"
		}
		if (role == "builder" || role == "tester" || role == "reviewer") && (purpose != "round-extension" || !strings.HasPrefix(req.OperatorRequest, "hk:doc/round-ext/")) {
			return "role_restricted: round-extension and a round-ext document required"
		}
	}
	if strings.Contains(req.Profile, "astra") && req.Profile != "codex-astra" {
		return "role_restricted: removed astra seat alias"
	}
	if req.Profile == "codex-astra" && purpose != "director" && purpose != "architect" && purpose != "operator-request" && purpose != "round-extension" {
		return "role_restricted: astra requires director, architect, or operator-request purpose"
	}
	return ""
}

// launchSide is one lookup's answer as the spawn would see it: the exit
// status, the launch fields and, on a refusal, the reason.
type launchSide struct {
	code int
	l    Launch
	why  string
}

// paceleverLaunchSide reads pacelever's run. Its refusal reason is the
// `error: ` line pacelever prints on stderr, which CLIQuota does not keep.
func paceleverLaunchSide(x Result) launchSide {
	l, err := launchResult(x)
	s := launchSide{code: exitCode(err), l: l}
	if err == nil {
		return s
	}
	s.why = strings.TrimPrefix(err.Error(), "quota catalog: ")
	for _, line := range strings.Split(x.Err, "\n") {
		if reason, ok := strings.CutPrefix(strings.TrimSpace(line), "error: "); ok {
			s.why = reason
			break
		}
	}
	return s
}

func nativeLaunchSide(l Launch, err error) launchSide {
	s := launchSide{code: exitCode(err), l: l}
	if err != nil {
		s.why = strings.TrimPrefix(err.Error(), "quota catalog: ")
	}
	return s
}

func (s launchSide) String() string {
	return fmt.Sprintf("exit=%d profile=%s model=%s effort=%s grade=%s gate=%s pool=%s family=%s stale=%t reason=%q", s.code, s.l.Profile, s.l.ModelID, s.l.Effort, s.l.Grade, s.l.Gate, s.l.Pool, s.l.Family, s.l.Catalog.Stale, s.why)
}

// diff names the fields where two answers differ.
func (s launchSide) diff(o launchSide) []string {
	var out []string
	for _, f := range []struct {
		name string
		same bool
	}{
		{"exit", s.code == o.code}, {"profile", s.l.Profile == o.l.Profile}, {"model_id", s.l.ModelID == o.l.ModelID},
		{"effort", s.l.Effort == o.l.Effort}, {"grade", s.l.Grade == o.l.Grade}, {"gate", s.l.Gate == o.l.Gate},
		{"pool", s.l.Pool == o.l.Pool}, {"family", s.l.Family == o.l.Family}, {"stale", s.l.Catalog.Stale == o.l.Catalog.Stale},
	} {
		if !f.same {
			out = append(out, f.name)
		}
	}
	return out
}

// shadowLaunch records how the offline native lookup compares with
// pacelever's run for one request (fleet event quota_launch_shadow). Outcome
// agree: the same exit status, launch fields and reason; reason_differs: the
// same exit status and fields, other refusal words; disagree: anything else,
// with the differing fields listed; skipped: nothing compared, because the
// catalog cache holds no entry younger than panewire's TTL (reason
// no_fresh_cache) or the local reads outlasted shadowBound (reason
// shadow_timeout). The event is best effort.
func (a *App) shadowLaunch(ctx context.Context, r QuotaRequest, pacelever Result) {
	type answer struct {
		l     Launch
		err   error
		found bool
	}
	native, ok := shadowRun(ctx, func() answer {
		l, found, err := a.offlineLaunch(r)
		return answer{l, err, found}
	})
	p := paceleverLaunchSide(pacelever)
	request := fmt.Sprintf("profile=%s effort=%s role=%s purpose=%s", r.Profile, r.Effort, r.Role, r.Purpose)
	switch {
	case !ok:
		a.emitFleetEvent("quota_launch_shadow", "", "skipped", "shadow_timeout", fmt.Sprintf("%s pacelever{%s}", request, p))
		return
	case !native.found:
		a.emitFleetEvent("quota_launch_shadow", "", "skipped", "no_fresh_cache", fmt.Sprintf("%s pacelever{%s}", request, p))
		return
	}
	n := nativeLaunchSide(native.l, native.err)
	diff := p.diff(n)
	outcome := "agree"
	switch {
	case len(diff) > 0:
		outcome = "disagree"
	case p.why != n.why:
		outcome = "reason_differs"
	}
	detail := fmt.Sprintf("%s pacelever{%s} native{%s} diff=%s", request, p, n, strings.Join(diff, ","))
	a.emitFleetEvent("quota_launch_shadow", "", outcome, firstNonempty(n.l.Pool, p.l.Pool, "unknown"), detail)
}

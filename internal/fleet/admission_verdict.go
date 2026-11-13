package fleet

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleetevent"
	"github.com/mgh3326/panewire/internal/grade"
	"github.com/mgh3326/panewire/internal/textbound"
)

func bestFirstGrades() []string {
	grades := gradeOrderKeys()
	for left, right := 0, len(grades)-1; left < right; left, right = left+1, right-1 {
		grades[left], grades[right] = grades[right], grades[left]
	}
	return append(grades, gradeUnrated)
}

func gradeOrderKeys() []string {
	return grade.Grades()
}

// The admission verdict is panewire's port of pacelever v0.3.28's spawn gate
// (gate.go gateWithUnrated and Gate, catalog.go roleCheck and resolveRow,
// manual.go RequiredWindows, gate.go recommendBase). It judges one request from
// stored readings, the bench catalog and pacelever's pool policy, and never
// runs pacelever. docs/quota.md §Admission names what it reads, the rules it
// keeps and where it differs from pacelever on purpose.
//
// The verdict codes are the gate's exit statuses, so a refusal surfaces
// exactly as the `pacelever gate` exit it replaces (CLIQuota.gateResult).
const (
	gateAdmitted     = 0
	gateInvalid      = 2
	gateBlocked      = quotaGateBlocked
	gateUnmeasurable = 4
	gateRoleDenied   = quotaGateRoleDenied
	gateUnsubscribed = 6
)

const (
	freeAvailable   = "available (free)"
	sourceOperator  = "operator"
	escalationGate  = "escalation"
	defaultGate     = "default"
	consultOnlyGate = "consult_only"
)

// admissionGrades orders the catalog grades, best first.
var admissionGrades = bestFirstGrades()

// creditAccountPools are pacelever's hand-recorded API credit pools.
var creditAccountPools = []string{"anthropic-api", "openai-api"}

// admissionRow is one catalog rung as the gate reads it. Placed and Group are
// derived the way pacelever's catalog decoder derives them for a remote row.
type admissionRow struct {
	Profile, Effort, ModelID, Pool, Grade, Gate, Group string
	Placed                                             bool
	Retired                                            bool
}

// admissionRows reads the bench catalog rows. A rung is placed when it has a
// grade; an agy rung meters against its group (CatalogRow.meteringGroup).
func admissionRows(rows []CatalogRow) []admissionRow {
	out := make([]admissionRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, admissionRow{Profile: r.Profile, Effort: r.Effort, ModelID: r.ModelID, Pool: r.Pool, Grade: r.Grade, Gate: r.Gate, Group: r.meteringGroup(), Placed: r.Grade != gradeUnrated, Retired: r.RetiredAt != nil})
	}
	return out
}

// admissionRequest is one gate question: the gate profile and the seat it
// asks for. E6Arm is the exact measurement marker, PANEWIRE_E6_ARM (with the
// retired PACELEVER_E6_ARM environment name accepted as a fallback).
type admissionRequest struct {
	Profile, Effort, Role, Purpose, OperatorRequest, RequestedBy, E6Arm string
}

// admissionBucket is one window of a reading. ScopeKind is account, model or
// group; ScopeName names the model or group.
type admissionBucket struct {
	Window, Reset        string
	Used                 *float64
	ScopeKind, ScopeName string
}

// admissionFree is the availability of devin's SWE-2 free tier.
type admissionFree struct {
	State string
	Reset *time.Time
}

// admissionReading is the stored automatic reading of the account a request
// is judged on. FP is that account's fingerprint.
type admissionReading struct {
	Plan    string
	FP      string
	Buckets []admissionBucket
	Free    *admissionFree
}

// admissionManual is one operator-entered window (manual observation).
type admissionManual struct {
	ID, Window, Reset, MeasuredAt, ExpiresAt string
	Used                                     *float64
}

// admissionInputs is everything one verdict reads. Readings and Manual are
// keyed by pool; a pool without a reading is "no observation".
type admissionInputs struct {
	Now      time.Time
	Rows     []admissionRow
	Policy   admissionPolicy
	Readings map[string]admissionReading
	Manual   map[string][]admissionManual
	// Seats is the embedded seat policy, keyed by profile. The gate reads
	// "this model has no thinking levels" from the seat; the catalog row
	// carries only the rung's profile, effort, model id, pool and grade.
	Seats map[string]Seat
	// AccountScoped judges a named account: a free rung needs the account's
	// stored free-bucket state.
	AccountScoped bool
}

// admissionVerdict is the gate's decision, in the fields its receipt carries.
type admissionVerdict struct {
	OK                 bool
	Exit               int
	Profile            string
	Pool               string
	Grade              string
	Class              string
	Reason             string
	UsedPct            *float64
	Basis              string
	E6Arm              string
	Missing            []string
	Alternatives       []string
	EscalationOverride bool
	OperatorRequestRef string
	RequestedBy        string
	Source             string
	ManualIDs          []string
	Inflight           string
	// GateCode names a refusal the request, the catalog or policy decided
	// without any quota reading (gateReasonCode of Reason); empty otherwise.
	GateCode string
}

func (d admissionVerdict) refuse(code int, reason string) admissionVerdict {
	d.OK, d.Exit, d.Reason = false, code, reason
	return d
}

// gateDenial is a refusal raised while the request is resolved.
type gateDenial struct {
	code int
	msg  string
}

// gateCodeMax bounds a gate code so "gate_blocked:"+code fits a 64-byte
// placement skip reason (fleetevent.Code).
const gateCodeMax = 48

// gateReasonCode is the code of a gate refusal reason: its leading word before
// the first colon ("operator_request_not_applicable: ordinary placement"), or
// the whole reason as a code when it has no colon ("rung is retired in the
// catalog" -> "rung_is_retired_in_the_catalog").
func gateReasonCode(reason string) string {
	head, _, _ := strings.Cut(reason, ":")
	return textbound.PrefixWithin(fleetevent.Code(head), gateCodeMax)
}

func admissionNormalize(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

// commandcodePool is the catalog pool of the Command Code account. Every rung
// on it draws on that one account and its 30-day window (docs/quota.md
// §Command Code seats).
const commandcodePool = "commandcode"

// admissionCredit marks a credit rung: a pi- profile, or any rung whose
// catalog pool is commandcode. The pool decides, not the profile name, so
// cmd-dsflash41 and omp-dsflash41 are credit rungs for the same reason.
func admissionCredit(profile, pool string) bool {
	return strings.HasPrefix(profile, "pi-") || pool == commandcodePool
}

// profileCredit answers admissionCredit for a caller that holds only a
// profile: it asks the pool of each of the profile's rungs.
func (in admissionInputs) profileCredit(profile string) bool {
	if admissionCredit(profile, "") {
		return true
	}
	return slices.ContainsFunc(in.profileRows(profile), func(r admissionRow) bool {
		return admissionCredit(profile, r.Pool)
	})
}

func admissionValidPct(p *float64) bool {
	return p != nil && !math.IsNaN(*p) && !math.IsInf(*p, 0) && *p >= 0 && *p <= 100
}

func admissionPct(v float64) *float64 {
	if !admissionValidPct(&v) {
		return nil
	}
	return &v
}

// admissionRoleCheck is pacelever's roleCheck as of v0.3.28: a credit rung
// (pi- profiles and commandcode-pool rungs) seats only an implementer, or a
// builder on a placed default rung, and the consult models keep their seat
// restrictions. cmd-dsflash41 is narrower still: implementer only.
func admissionRoleCheck(req admissionRequest, row admissionRow) *gateDenial {
	role := admissionNormalize(req.Role)
	if admissionCredit(req.Profile, row.Pool) {
		allowed := role == "implementer" || (role == "builder" && row.Placed && row.Gate == defaultGate)
		if !allowed {
			return &gateDenial{gateRoleDenied, "role_restricted: credit rung requires implementer; placed default rungs also allow builder"}
		}
	}
	// Operator 2026-11-07 (hk 3248): cmd-dsflash41 seats only an implementer
	// — stricter than pacelever, which also seats a builder.
	if req.Profile == "cmd-dsflash41" && role != "implementer" {
		return &gateDenial{gateRoleDenied, "role_restricted: cmd-dsflash41 seats only an implementer"}
	}
	if strings.Contains(req.Profile, "astra") && req.Profile != "codex-astra" {
		return &gateDenial{gateRoleDenied, "role_restricted: removed astra seat alias"}
	}
	return nil
}

// admissionValidRef accepts hk:task/N and hk:doc/… references.
func admissionValidRef(s string) bool {
	if strings.TrimSpace(s) != s || strings.ContainsAny(s, "\r\n\t\x00 ") {
		return false
	}
	if rest, ok := strings.CutPrefix(s, "hk:task/"); ok {
		if rest == "" || rest == "0" {
			return false
		}
		for _, r := range rest {
			if r < '0' || r > '9' {
				return false
			}
		}
		return true
	}
	return strings.HasPrefix(s, "hk:doc/") && len(s) > 7
}

// admissionUnratedRole lets the roles that may try an unrated rung skip the
// measurement marker.
func admissionUnratedRole(req admissionRequest) bool {
	switch admissionNormalize(req.Role) {
	case "consult", "builder":
		return true
	}
	return false
}

func admissionMarkerRequired(row admissionRow, allowUnrated bool) bool {
	if row.Grade == gradeUnrated {
		return !allowUnrated
	}
	return !row.Placed && row.Gate != consultOnlyGate && !strings.HasPrefix(row.Profile, "devin-argon")
}

func admissionEffortRank(s string) int {
	for i, k := range []string{"", "off", "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"} {
		if s == k {
			return i
		}
	}
	return 99
}

func admissionGradeRank(g string) int {
	if i := slices.Index(admissionGrades, g); i >= 0 {
		return i
	}
	return len(admissionGrades)
}

func (in admissionInputs) profileRows(profile string) []admissionRow {
	var rows []admissionRow
	for _, r := range in.Rows {
		if r.Profile == profile && !r.Retired {
			rows = append(rows, r)
		}
	}
	return rows
}

// defaultEffort picks the rung an effortless request resolves to: the best
// graded ordinary rung, lowest effort first.
func admissionDefaultEffort(rows []admissionRow) string {
	var ordinary []admissionRow
	for _, r := range rows {
		if r.Gate == defaultGate && (r.Placed || r.Grade == gradeUnrated) {
			ordinary = append(ordinary, r)
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
		return ""
	}
	return ordinary[0].Effort
}

// resolveRow is pacelever's resolveRow: the catalog rung a request names,
// with its resolved effort and the E6 arm it was admitted under.
func (in admissionInputs) resolveRow(req admissionRequest, allowUnrated bool) (admissionRow, string, string, *gateDenial) {
	profile := req.Profile
	requested := admissionNormalize(req.Effort)
	if in.profileCredit(profile) && requested == "" {
		return admissionRow{}, "", "", &gateDenial{gateBlocked, "explicit --effort required for credit profile"}
	}
	if requested != "" && admissionEffortRank(requested) == 99 {
		known := slices.ContainsFunc(in.Rows, func(r admissionRow) bool { return r.Profile == profile && r.Effort == requested })
		if !known {
			return admissionRow{}, "", "", &gateDenial{gateBlocked, "unknown effort rung"}
		}
	}
	if in.profileCredit(profile) {
		allowed := requested == "low" || requested == "high" || requested == "max"
		if requested == "off" {
			allowed = in.Seats[profile].NoThinking && slices.ContainsFunc(in.profileRows(profile), func(r admissionRow) bool { return r.Effort == "off" && r.Pool == commandcodePool })
		}
		if strings.HasPrefix(profile, "pi-") {
			allowed = slices.Contains([]string{"off", "minimal", "low", "medium", "high", "xhigh", "max"}, requested)
		}
		if !allowed {
			return admissionRow{}, "", "", &gateDenial{gateBlocked, "credit effort is not placed"}
		}
	}
	if slices.Contains([]string{"codex-med", "builder-terra-high", "builder-terra-xhigh", "builder-terra-max"}, profile) ||
		slices.ContainsFunc(in.Rows, func(r admissionRow) bool { return r.Profile == profile && r.ModelID == "gpt-5.6-terra" }) {
		return admissionRow{}, "", "", &gateDenial{gateBlocked, "model_refused: retired model identity"}
	}
	rows := in.profileRows(profile)
	if len(rows) == 0 {
		return admissionRow{}, "", "", &gateDenial{gateBlocked, fmt.Sprintf("profile %q is unknown or retired", profile)}
	}
	if requested == "" && strings.HasPrefix(req.E6Arm, profile+"@") && !slices.ContainsFunc(rows, func(r admissionRow) bool { return r.Placed }) {
		requested = strings.TrimPrefix(req.E6Arm, profile+"@")
	}
	var eligible []admissionRow
	for _, r := range rows {
		if requested == r.Effort && r.Grade == gradeUnrated && admissionMarkerRequired(r, allowUnrated) && req.E6Arm != profile+"@"+r.Effort {
			return admissionRow{}, "", "", &gateDenial{gateBlocked, "e6_arm_required: exact measurement marker required"}
		}
		if !admissionMarkerRequired(r, allowUnrated) || req.E6Arm == profile+"@"+r.Effort {
			eligible = append(eligible, r)
		}
	}
	if len(eligible) == 0 {
		return admissionRow{}, "", "", &gateDenial{gateBlocked, "e6_arm_required: unplaced profile requires exact measurement marker"}
	}
	fallback := admissionDefaultEffort(eligible)
	resolved := fallback
	if requested != "" {
		resolved = requested
	}
	if slices.ContainsFunc(in.Rows, func(r admissionRow) bool { return r.Profile == profile && r.Effort == resolved && r.Retired }) {
		return admissionRow{}, "", "", &gateDenial{gateBlocked, "rung is retired in the catalog"}
	}
	i := slices.IndexFunc(eligible, func(r admissionRow) bool { return r.Effort == resolved })
	if i < 0 {
		if in.profileCredit(profile) || profile == "codex-terra61" {
			return admissionRow{}, "", "", &gateDenial{gateBlocked, "e6_arm_required: rung is not placed"}
		}
		i = slices.IndexFunc(eligible, func(r admissionRow) bool { return r.Effort == fallback })
	}
	if i < 0 {
		return admissionRow{}, "", "", &gateDenial{gateBlocked, "rung is not in the catalog"}
	}
	chosen := eligible[i]
	arm := ""
	if admissionMarkerRequired(chosen, allowUnrated) {
		if req.E6Arm != profile+"@"+resolved {
			return admissionRow{}, "", "", &gateDenial{gateBlocked, "e6_arm_required"}
		}
		arm = req.E6Arm
	}
	return chosen, resolved, arm, nil
}

// requiredWindows are the windows a reading of pool must cover. Manual
// evidence for a credit pool or commandcode covers its credit window instead.
// codex's low plans need only the window the plan has. commandcode needs its
// 30-day window (decision/2026-11-06/commandcode-30d-required-in-panewire-gate).
func requiredWindows(pool, plan string, manual bool) []string {
	if slices.Contains(creditAccountPools, pool) || manual && pool == "commandcode" {
		return []string{"credit"}
	}
	switch pool {
	case "codex":
		switch admissionNormalize(plan) {
		case "prolite":
			return []string{"7d"}
		case "free":
			return []string{"30d"}
		}
		return []string{"5h", "7d"}
	case "commandcode":
		return []string{"5h", "7d", "30d"}
	case "claude", "kimi", "agy":
		return []string{"5h", "7d"}
	case "grok", "devin":
		return []string{"7d"}
	case "kiro":
		return []string{"30d"}
	case "clinepass":
		return []string{"5h", "7d", "30d"}
	}
	return nil
}

// missingWindowRefusal names the refusal for required windows a reading
// lacks, or "" when the pool admits without them. codex's low plans refuse on
// any missing window; commandcode refuses only without its 30-day window.
func missingWindowRefusal(pool, plan string, missing []string) string {
	switch {
	case len(missing) == 0:
		return ""
	case pool == "codex" && slices.Contains([]string{"plus", "prolite", "free"}, plan):
		return "codex_required_window_missing:" + strings.Join(missing, ",")
	case pool == "commandcode" && slices.Contains(missing, "30d"):
		return "commandcode_required_window_missing:30d"
	}
	return ""
}

// matchingBuckets keeps the windows that gate a rung: account-wide ones and the
// rung's own group. Model windows never gate.
func matchingBuckets(buckets []admissionBucket, group string) []admissionBucket {
	var out []admissionBucket
	for _, b := range buckets {
		if (b.ScopeKind == "account" || b.ScopeKind == "" || (b.ScopeKind == "group" && b.ScopeName == group)) && admissionValidPct(b.Used) {
			out = append(out, b)
		}
	}
	return out
}

// pace is a window's used share over its elapsed share; nil when the window
// cannot be paced yet.
func admissionPace(b admissionBucket, now time.Time) *float64 {
	if !admissionValidPct(b.Used) {
		return nil
	}
	window, err := windowDuration(b.Window)
	if err != nil || window <= 0 {
		return nil
	}
	reset, err := time.Parse(time.RFC3339Nano, b.Reset)
	if err != nil {
		return nil
	}
	left := reset.Sub(now)
	fraction := 1 - float64(left)/float64(window)
	if left <= 0 || fraction < 0.05 || fraction > 1 {
		return nil
	}
	v := *b.Used / 100 / fraction
	return &v
}

// validManual is pacelever's ManualStore.Valid: the current entry of each
// window that still holds, sorted by window.
func (in admissionInputs) validManual(pool string) []admissionManual {
	latest := map[string]admissionManual{}
	for _, m := range in.Manual[pool] {
		latest[m.Window] = m
	}
	var out []admissionManual
	for _, m := range latest {
		if !admissionValidPct(m.Used) {
			continue
		}
		measured, err := time.Parse(time.RFC3339Nano, m.MeasuredAt)
		if err != nil || measured.After(in.Now.Add(60*time.Second)) {
			continue
		}
		expiry, err := time.Parse(time.RFC3339Nano, m.ExpiresAt)
		if err != nil || !expiry.After(in.Now) {
			continue
		}
		if reset, err := time.Parse(time.RFC3339Nano, m.Reset); err == nil && !reset.After(in.Now) {
			continue
		}
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Window < out[j].Window })
	return out
}

// admit is pacelever's Gate: the verdict, and on a refusal of a graded rung
// the same-grade launch spellings that would be admitted instead.
func (in admissionInputs) admit(req admissionRequest) admissionVerdict {
	d := in.judge(req)
	if !d.OK && d.Grade != "" && d.Exit != gateRoleDenied && d.Exit != gateUnsubscribed {
		for _, r := range in.recommend(d.Grade, req.Role) {
			if r.Profile != req.Profile && r.candidate {
				d.Alternatives = append(d.Alternatives, r.spelling())
			}
		}
	}
	return d
}

func (in admissionInputs) judge(req admissionRequest) admissionVerdict {
	return in.judgeWith(req, admissionUnratedRole(req))
}

// judgeWith is pacelever's gateWithUnrated, rule for rule.
// devinFreeRungs maps each SWE-2 profile to the free bucket it draws on. These
// rungs run on Devin's invisible free-model quota, not the account's paid
// window, so admission judges them on the stored free-bucket state only.
var devinFreeRungs = map[string]string{
	"devin-swe2":              "swe-2-high",
	"devin-swe2-medium":       "swe-2-medium",
	"devin-swe2-max":          "swe-2-max",
	"devin-swe2-flash":        "swe-2-flash-high",
	"devin-swe2-flash-medium": "swe-2-flash-medium",
	"devin-swe2-flash-max":    "swe-2-flash-max",
}

func (in admissionInputs) judgeWith(req admissionRequest, allowUnrated bool) admissionVerdict {
	d := admissionVerdict{Profile: req.Profile, Exit: gateBlocked, Missing: []string{}, Alternatives: []string{}, ManualIDs: []string{}}
	now := in.Now
	profile := req.Profile
	rows := in.profileRows(profile)
	var hint admissionRow
	if len(rows) > 0 {
		hint = rows[0]
	}
	d.Pool = hint.Pool
	// Consult-role checks precede subscription and request-shape checks.
	if !in.profileCredit(profile) {
		if e := admissionRoleCheck(req, hint); e != nil {
			return d.refuse(e.code, e.msg)
		}
	}
	if !in.Policy.subscribed(profile, hint.Pool) {
		return d.refuse(gateUnsubscribed, "unsubscribed: profile or pool subscription disabled")
	}
	if req.RequestedBy != "" && req.OperatorRequest == "" {
		return d.refuse(gateBlocked, "requested_by_requires_operator_request")
	}
	if req.OperatorRequest != "" && !admissionValidRef(req.OperatorRequest) {
		return d.refuse(gateBlocked, "operator_request_ref_invalid")
	}
	if strings.ContainsAny(req.RequestedBy, " \t\n\r\x00") || len(req.RequestedBy) > 80 {
		return d.refuse(gateBlocked, "requested_by_invalid")
	}
	for _, candidate := range rows {
		if req.Effort != "" && candidate.Effort == admissionNormalize(req.Effort) && admissionMarkerRequired(candidate, allowUnrated) && req.E6Arm != profile+"@"+candidate.Effort {
			return d.refuse(gateBlocked, "e6_arm_required: exact measurement marker required")
		}
	}
	row, resolved, arm, e := in.resolveRow(req, allowUnrated)
	if e != nil {
		return d.refuse(e.code, e.msg)
	}
	d.Pool = row.Pool
	if row.Placed && row.Grade != gradeUnrated {
		d.Grade = row.Grade
	}
	if e := admissionRoleCheck(req, row); e != nil {
		return d.refuse(e.code, e.msg)
	}
	if req.OperatorRequest != "" {
		if row.Gate == defaultGate {
			return d.refuse(gateBlocked, "operator_request_not_applicable: ordinary placement")
		}
		d.OperatorRequestRef = req.OperatorRequest
		d.RequestedBy = firstNonempty(req.RequestedBy, "unknown")
	}
	if row.Gate == consultOnlyGate && req.OperatorRequest == "" && profile != "codex-astra" {
		return d.refuse(gateBlocked, "consult_only: pass --operator-request")
	}
	policy := in.Policy.pool(row.Pool, now)
	d.Class = policy.Class
	// The SWE-2 free rungs draw on the free bucket, not the devin pool, so an
	// excluded devin pool does not refuse them: they are judged below on the
	// free bucket exactly as in an unexcluded pool (hk 3926).
	_, freeRung := devinFreeRungs[profile]
	if policy.Class == "exclude" && !freeRung {
		return d.refuse(gateBlocked, "pool excluded by policy")
	}
	reading, observed := in.Readings[row.Pool]
	if strings.HasPrefix(profile, "devin-fusion-sol61") {
		if fp := policy.FusionBillingFP; fp == "" || !observed || reading.FP != fp {
			return d.refuse(gateUnmeasurable, "fusion_billing_account_unavailable")
		}
	}
	if rung, free := devinFreeRungs[profile]; free {
		// The SWE-2 free bucket cannot be measured. A named account is
		// admitted on its stored free-bucket state.
		if in.AccountScoped {
			fb := reading.Free
			if fb == nil || fb.State == "unknown" {
				return d.refuse(gateUnmeasurable, "shared free bucket state unavailable")
			}
			if fb.State == "exhausted" {
				if fb.Reset == nil {
					return d.refuse(gateUnmeasurable, "shared free bucket exhausted without a reset time")
				}
				if fb.Reset.After(now) {
					return d.refuse(gateBlocked, "free limit exceeded until "+fb.Reset.UTC().Format(time.RFC3339))
				}
			}
		}
		d.OK, d.Exit = true, gateAdmitted
		d.UsedPct = admissionPct(0)
		d.Basis = freeBasisPrefix + rung
		d.Reason = freeAvailable + ": SWE-2 free bucket; account credits do not constrain this rung"
		return d
	}
	cutoff := policy.Cutoff
	if in.Policy.Pools[row.Pool].Cutoff == 0 && policy.Class == "preserve" {
		cutoff = 90
	}
	buckets := matchingBuckets(reading.Buckets, row.Group)
	// Confirmed exhaustion refuses even when the reading is otherwise unusable.
	for _, b := range buckets {
		if *b.Used >= cutoff {
			if row.Pool == "commandcode" {
				if reset, err := time.Parse(time.RFC3339Nano, b.Reset); err == nil && !reset.After(now) {
					continue
				}
			}
			d.UsedPct = b.Used
			return d.refuse(gateBlocked, fmt.Sprintf("quota exhausted window=%s cutoff=%g", b.Window, cutoff))
		}
	}
	if !observed || len(buckets) == 0 {
		manual := in.validManual(row.Pool)
		covered := map[string]bool{}
		for _, m := range manual {
			covered[m.Window] = true
		}
		for _, w := range requiredWindows(row.Pool, reading.Plan, true) {
			if !covered[w] {
				return d.refuse(gateUnmeasurable, "measurement unavailable; manual fallback missing required window "+w)
			}
		}
		if len(manual) == 0 {
			return d.refuse(gateUnmeasurable, "measurement unavailable")
		}
		buckets = nil
		for _, m := range manual {
			buckets = append(buckets, admissionBucket{Window: m.Window, Used: m.Used, Reset: m.Reset, ScopeKind: "account"})
			d.ManualIDs = append(d.ManualIDs, m.ID)
		}
		d.Source = sourceOperator
	}
	covered := map[string]bool{}
	maxUsed := -1.0
	for _, b := range buckets {
		if !admissionValidPct(b.Used) {
			continue
		}
		covered[b.Window] = true
		maxUsed = max(maxUsed, *b.Used)
		if *b.Used >= cutoff {
			d.UsedPct = b.Used
			return d.refuse(gateBlocked, fmt.Sprintf("quota exhausted window=%s cutoff=%g", b.Window, cutoff))
		}
	}
	if maxUsed < 0 {
		return d.refuse(gateUnmeasurable, "no measured matching quota windows")
	}
	d.UsedPct = &maxUsed
	for _, w := range requiredWindows(row.Pool, reading.Plan, d.Source == sourceOperator) {
		if !covered[w] {
			d.Missing = append(d.Missing, w)
		}
	}
	if why := missingWindowRefusal(row.Pool, reading.Plan, d.Missing); why != "" {
		return d.refuse(gateUnmeasurable, why)
	}
	if len(policy.InflightReservePct) > 0 {
		// No local inflight count is read, so the reserve cannot refuse.
		d.Inflight = "inflight=unknown(local)"
	}
	if row.Gate == escalationGate && req.Effort == "" && req.OperatorRequest == "" {
		for _, other := range in.Rows {
			if other.Profile == profile || other.Grade != row.Grade || other.Gate != defaultGate || !other.Placed {
				continue
			}
			if in.judge(admissionRequest{Profile: other.Profile, Effort: other.Effort, Role: req.Role}).OK {
				d = d.refuse(gateBlocked, "escalation: same-grade ordinary alternative available")
				d.GateCode = gateReasonCode(d.Reason)
				return d
			}
		}
	}
	if row.Gate == escalationGate && req.OperatorRequest != "" {
		d.EscalationOverride = true
	}
	d.OK, d.Exit, d.E6Arm = true, gateAdmitted, arm
	d.Reason = fmt.Sprintf("%s pool=%s used=%g%% class=%s", d.Profile, row.Pool, maxUsed, policy.Class)
	if resolved != "" {
		d.Reason += " [rung " + profile + "@" + resolved + "]"
	}
	for _, b := range buckets {
		if p := admissionPace(b, now); p != nil && *p > 1 {
			d.Reason += fmt.Sprintf("\npace-warning pool=%s window=%s multiple=%.2f", row.Pool, b.Window, *p)
		}
	}
	return d
}

// admissionAlternative is one ranked same-grade rung.
type admissionAlternative struct {
	Profile, Effort string
	candidate       bool
	priority        int
	score           float64
}

func (r admissionAlternative) spelling() string {
	if r.Effort != "" {
		return r.Profile + " --effort " + r.Effort
	}
	return r.Profile
}

// recommend is pacelever's recommendBase: every placed ordinary rung of the
// grade, judged on the same inputs and ranked free first, then pools promoted
// by another pool's 7-day pace, then by policy score. Pace changes priority,
// never eligibility.
func (in admissionInputs) recommend(grade, role string) []admissionAlternative {
	over := map[string]float64{}
	for pool, reading := range in.Readings {
		for _, b := range reading.Buckets {
			if b.Window == "7d" && b.ScopeKind == "account" {
				if p := admissionPace(b, in.Now); p != nil && *p >= in.Policy.paceThreshold() {
					over[pool] = *p
				}
			}
		}
	}
	var out []admissionAlternative
	seen := map[string]bool{}
	for _, row := range in.Rows {
		key := row.Profile + "@" + row.Effort
		if seen[key] || row.Grade == gradeUnrated || row.Grade != grade || !row.Placed || row.Gate != defaultGate || row.Retired {
			continue
		}
		seen[key] = true
		req := admissionRequest{Profile: row.Profile, Effort: row.Effort, Role: role}
		if req.Role == "" && admissionCredit(row.Profile, row.Pool) {
			req.Role = "implementer"
		}
		d := in.judge(req)
		r := admissionAlternative{Profile: row.Profile, Effort: row.Effort, candidate: d.OK, priority: 2}
		if !d.OK {
			r.priority = 9
		}
		p := in.Policy.pool(row.Pool, in.Now)
		r.score = p.Boost + p.CapacityWeight*100
		if d.UsedPct != nil {
			r.score -= *d.UsedPct
		}
		if admissionCredit(row.Profile, row.Pool) {
			r.priority = 4
		}
		if d.Basis != "" {
			r.priority = 0
		}
		if d.OK && d.Basis == "" {
			for pool := range over {
				if pool != row.Pool {
					r.priority = 1
				}
			}
		}
		out = append(out, r)
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.priority != b.priority {
			return a.priority < b.priority
		}
		if a.score != b.score {
			return a.score > b.score
		}
		if a.Profile != b.Profile {
			return a.Profile < b.Profile
		}
		if (a.Effort == "") != (b.Effort == "") {
			return a.Effort == ""
		}
		return admissionEffortRank(a.Effort) < admissionEffortRank(b.Effort)
	})
	return out
}

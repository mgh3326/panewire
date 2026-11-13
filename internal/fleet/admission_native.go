package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

const gateModeNative = "native"
const gateModeShadow = "shadow" // retired config value retained for migration diagnostics

// Refusals for a named account's stored reading, worded as pacelever words them.
const (
	msgAccountStale     = "fresh automatic shared quota unavailable for account"
	msgAccountMalformed = "shared quota observation unusable for account"
	msgSharedQuotaDown  = "shared quota unavailable for account"
)

func validateGateMode(quota map[string]any) error {
	v, ok := quota["gate"]
	if !ok {
		return nil
	}
	if s, isString := v.(string); !isString || s != gateModeNative {
		return fail(exitConfig, "quota.gate에는 %q만 지정할 수 있습니다", gateModeNative)
	}
	return nil
}

func (a *App) gateMode() string { return gateModeNative }

// admissionQuota is the production QuotaGate. Catalog is handoffkeep's
// (CLIQuota); Gate and Launch each follow their switch (launch_native.go), and
// Pools follows the launch switch, reading the credit pools without pacelever
// only in native mode when the shared store serves them
// (credit_pool_native.go). The native verdict reads the account's stored
// reading from handoffkeep and the bench catalog; it never runs `pacelever
// gate` or `pacelever refresh`.
type admissionQuota struct {
	CLIQuota
	app *App
}

func (q admissionQuota) Gate(ctx context.Context, r QuotaRequest) (string, error) {
	return gateResult(q.app.nativeGate(ctx, r, true), r)
}

// gateReceipt is the decision as `pacelever gate --json` prints it: the
// fields panewire reads (pool, basis, account binding, audit) plus the reason
// and alternatives for the operator.
type gateReceipt struct {
	Schema             string   `json:"schema"`
	GeneratedAt        string   `json:"generated_at"`
	JudgedBy           string   `json:"judged_by"`
	AccountKey         string   `json:"account_key,omitempty"`
	ReadingKey         string   `json:"reading_account_key,omitempty"`
	OK                 bool     `json:"ok"`
	ExitCode           int      `json:"exit_code"`
	Profile            string   `json:"profile"`
	Pool               string   `json:"provider_id"`
	Grade              *string  `json:"grade"`
	Reason             string   `json:"reason"`
	UsedPct            *float64 `json:"used_pct"`
	PoolClass          *string  `json:"pool_class"`
	Unmeasurable       bool     `json:"unmeasurable"`
	RoleDenied         bool     `json:"role_denied"`
	Unsubscribed       bool     `json:"unsubscribed,omitempty"`
	Alternatives       []string `json:"alternatives"`
	MissingWindows     []string `json:"missing_windows"`
	GateBasis          string   `json:"gate_basis,omitempty"`
	E6Arm              string   `json:"e6_arm,omitempty"`
	Inflight           string   `json:"inflight,omitempty"`
	EscalationOverride bool     `json:"escalation_override"`
	OperatorRequestRef *string  `json:"operator_request_ref"`
	RequestedBy        *string  `json:"requested_by"`
	RefResolution      *string  `json:"ref_resolution"`
	Source             *string  `json:"source"`
	ManualIDs          []string `json:"manual_observation_ids"`
	Purpose            *string  `json:"purpose"`
	// GateCode is set only on a refusal no reading decided (admissionVerdict.GateCode).
	GateCode string `json:"gate_code,omitempty"`
}

// nativeGate is panewire's own gate run: the verdict as the exit status and
// JSON receipt `pacelever gate --json` would give, so gateResult reads both
// alike. A live run decides the spawn: like pacelever's gate it measures a
// stale reading of an account this host holds once, and lists alternatives on
// a refusal. The shadow run is not live: it only reads.
func (a *App) nativeGate(ctx context.Context, r QuotaRequest, live bool) Result {
	d, readingKey := a.nativeVerdict(ctx, r, live)
	return verdictResult(a.Now(), r, d, readingKey)
}

// verdictResult is a verdict as the gate's exit status and JSON receipt.
func verdictResult(now time.Time, r QuotaRequest, d admissionVerdict, readingKey string) Result {
	receipt := gateReceipt{Schema: "scopefuel.gate.v1", GeneratedAt: now.UTC().Format(time.RFC3339Nano), JudgedBy: "panewire", AccountKey: r.AccountKey, OK: d.OK, ExitCode: d.Exit, GateCode: d.GateCode,
		Profile: d.Profile, Pool: d.Pool, Reason: d.Reason, UsedPct: d.UsedPct, Unmeasurable: d.Exit == gateUnmeasurable, RoleDenied: d.Exit == gateRoleDenied, Unsubscribed: d.Exit == gateUnsubscribed,
		Alternatives: d.Alternatives, MissingWindows: d.Missing, GateBasis: d.Basis, E6Arm: d.E6Arm, Inflight: d.Inflight, EscalationOverride: d.EscalationOverride, ManualIDs: d.ManualIDs}
	if readingKey != r.AccountKey {
		receipt.ReadingKey = readingKey
	}
	optional := func(s string) *string {
		if s == "" {
			return nil
		}
		return &s
	}
	receipt.Grade, receipt.PoolClass, receipt.Source, receipt.Purpose = optional(d.Grade), optional(d.Class), optional(d.Source), optional(r.Purpose)
	if d.OperatorRequestRef != "" {
		receipt.OperatorRequestRef, receipt.RequestedBy, receipt.RefResolution = optional(d.OperatorRequestRef), optional(d.RequestedBy), optional("unverified")
	}
	out, err := json.Marshal(receipt)
	if err != nil {
		return Result{Code: gateInvalid, Out: `{"reason":"native gate receipt unencodable"}`}
	}
	return Result{Code: d.Exit, Out: string(out)}
}

func invalidVerdict(req admissionRequest) admissionVerdict {
	return admissionVerdict{Profile: req.Profile, Exit: gateInvalid, Missing: []string{}, Alternatives: []string{}, ManualIDs: []string{}}
}

// requestVerdict is the gate's first judgment: the request against the catalog
// and policy with no reading. A refusal it gives other than an unmeasurable one
// is decided by the request, the catalog or policy alone and carries its
// GateCode. invalid is true when d is the final verdict because a gate input
// (the pool policy or the catalog) is unusable.
func (a *App) requestVerdict(ctx context.Context, r QuotaRequest) (req admissionRequest, in admissionInputs, d admissionVerdict, invalid bool) {
	e6Arm := ""
	if !r.E6ArmLocked {
		e6Arm = strings.TrimSpace(os.Getenv("PANEWIRE_E6_ARM"))
		if e6Arm == "" {
			e6Arm = strings.TrimSpace(os.Getenv("PACELEVER_E6_ARM"))
		}
	}
	req = admissionRequest{Profile: firstNonempty(r.GateProfile, r.Profile), Effort: r.Effort, Role: r.Role, Purpose: r.Purpose, OperatorRequest: r.OperatorRequest, RequestedBy: r.RequestedBy, E6Arm: firstNonempty(r.E6Arm, e6Arm)}
	policy, err := loadAdmissionPolicy(os.Getenv, a.Home)
	if err != nil {
		return req, in, invalidVerdict(req).refuse(gateInvalid, err.Error()), true
	}
	rows, err := a.benchCatalog(ctx)
	if err != nil {
		return req, in, invalidVerdict(req).refuse(gateInvalid, "bench catalog unavailable"), true
	}
	in = admissionInputs{Now: a.Now(), Rows: admissionRows(rows), Policy: policy, Seats: a.Policy.Profiles}
	d = in.admit(req)
	if !d.OK && d.Exit != gateUnmeasurable {
		d.GateCode = gateReasonCode(d.Reason)
	}
	return req, in, d, false
}

// RequestRefusal is the gate's refusal of a request no reading can lift (role,
// subscription, request shape, catalog, excluded pool), as the typed refusal
// Gate would give, or nil when the request is admitted or needs a reading. It
// reads no account, so one answer holds for every host.
func (q admissionQuota) RequestRefusal(ctx context.Context, r QuotaRequest) error {
	_, _, d, invalid := q.app.requestVerdict(ctx, r)
	if invalid || d.GateCode == "" {
		return nil
	}
	_, err := gateResult(verdictResult(q.app.Now(), r, d, ""), r)
	return err
}

// nativeVerdict follows `pacelever gate`: a first judgment with no readings
// settles every refusal no reading can lift (role, subscription, request
// shape, catalog, excluded pool); only an admit or an unmeasurable one goes on
// to the account's stored reading. It returns the account key it read.
func (a *App) nativeVerdict(ctx context.Context, r QuotaRequest, live bool) (admissionVerdict, string) {
	req, in, d, invalid := a.requestVerdict(ctx, r)
	if invalid {
		return d, ""
	}
	policy := in.Policy
	key := r.AccountKey
	if key != "" {
		if !validAccountKey(key) {
			return invalidVerdict(req).refuse(gateInvalid, "invalid --account-key"), ""
		}
		if accountPool(key) != d.Pool {
			return invalidVerdict(req).refuse(gateInvalid, "--account-key does not match profile pool"), ""
		}
	}
	if !d.OK && d.Exit != gateUnmeasurable {
		return d, ""
	}
	if key == "" {
		if keys := a.localAccountKeys(ctx, d.Pool); len(keys) > 0 {
			key = keys[0]
		} else if fp := policy.Accounts[d.Pool]; fp != "" {
			key = d.Pool + ":" + fp
		}
	}
	if key == "" {
		return d.refuse(gateUnmeasurable, "measurement unavailable: this host holds no "+d.Pool+" account"), ""
	}
	in.AccountScoped = true
	if why := a.loadAccount(ctx, &in, key, live, r.AccountKey == ""); why != "" {
		return d.refuse(gateUnmeasurable, why), key
	}
	in.Now = a.Now()
	d = in.judge(req)
	if live && !d.OK && d.Grade != "" && d.Exit != gateRoleDenied && d.Exit != gateUnsubscribed {
		// Alternatives read the other same-grade pools' stored readings for
		// the accounts this host holds — no lease, no measurement (hk 3211).
		pools := slices.DeleteFunc(in.gradePools(d.Grade), func(pool string) bool { return pool == d.Pool })
		held := a.heldAccountKeys(ctx, pools)
		for _, pool := range pools {
			if keys := held[pool]; len(keys) > 0 {
				_ = a.loadAccount(ctx, &in, keys[0], false, false)
			}
		}
		for _, alt := range in.recommend(d.Grade, req.Role) {
			if alt.Profile != req.Profile && alt.candidate {
				d.Alternatives = append(d.Alternatives, alt.spelling())
			}
		}
	}
	return d, key
}

// gradePools lists the pools of the placed ordinary rungs of grade.
func (in admissionInputs) gradePools(grade string) []string {
	var pools []string
	for _, row := range in.Rows {
		if row.Grade == grade && row.Placed && row.Gate == defaultGate && !row.Retired && !slices.Contains(pools, row.Pool) {
			pools = append(pools, row.Pool)
		}
	}
	return pools
}

func validAccountKey(key string) bool {
	pool, fp, ok := strings.Cut(key, ":")
	return ok && validID(pool) && accountFP.MatchString(fp)
}

// loadAccount reads one account's stored reading into in: a credit pool's
// operator entries, any other pool's automatic reading. It returns why the
// account cannot be judged, or "". With refresh set, a stale reading of an
// account this host holds in a pool panewire measures is measured once
// (`panewire quota refresh <pool>`) and read again; a dry run (planOnly)
// prints `would measure` instead and judges the stored reading as it is. A
// local request
// (the host's own account, no --account-key) also carries the account's
// operator entries, so like pacelever's local gate it falls back to them when
// the automatic reading is unusable; a named account stays strict.
func (a *App) loadAccount(ctx context.Context, in *admissionInputs, key string, refresh, local bool) string {
	pool := accountPool(key)
	latest, err := a.sharedLatest(ctx, key)
	if err != nil {
		return msgSharedQuotaDown
	}
	if in.Manual == nil {
		in.Manual = map[string][]admissionManual{}
	}
	if slices.Contains(creditAccountPools, pool) {
		in.Manual[pool] = admissionManualFrom(latest.Manual)
		return ""
	}
	if local {
		in.Manual[pool] = admissionManualFrom(latest.Manual)
	}
	reading, why := admissionReadingFrom(latest.Observation, pool, key, a.Now(), a.sharedFreshTTL(pool))
	// Only panewire's own measurement refreshes here: the gate never runs
	// pacelever, so a pool panewire does not measure stays unmeasurable.
	if why == msgAccountStale && refresh && a.nativeQuotaPool(pool) && slices.Contains(a.localAccountKeys(ctx, pool), key) {
		if a.planOnly {
			// A dry run measures nothing: the refresh writes presence, lease
			// and observation to the shared quota store. A spawn measures
			// this account, so the plan may differ from it here.
			a.wouldMeasureLine("", pool)
		} else if checkQuotaPlain(runQuotaCommand(ctx, a.Runner, refreshCommand(pool, "", true), "quota refresh", 60*time.Second), "quota refresh") != nil {
			why = msgAccountStale + "; local refresh failed"
		} else if latest, err = a.sharedLatest(ctx, key); err != nil {
			return msgSharedQuotaDown
		} else {
			reading, why = admissionReadingFrom(latest.Observation, pool, key, a.Now(), a.sharedFreshTTL(pool))
			if local {
				in.Manual[pool] = admissionManualFrom(latest.Manual)
			}
		}
	}
	if why != "" {
		if local && len(in.Manual[pool]) > 0 {
			// No automatic reading: the verdict's manual fallback decides.
			return ""
		}
		return why
	}
	if in.Readings == nil {
		in.Readings = map[string]admissionReading{}
	}
	in.Readings[pool] = reading
	return ""
}

// benchCatalog is the bench catalog the gate judges with: handoffkeep's rows,
// validated and cached by catalog.go. The gate runs no pacelever command.
func (a *App) benchCatalog(ctx context.Context) ([]CatalogRow, error) {
	if a.BenchCatalog != nil {
		return a.BenchCatalog(ctx)
	}
	if a.Quota == nil {
		return nil, fmt.Errorf("bench catalog unavailable")
	}
	return a.Quota.Catalog(ctx)
}

// sharedLatest reads one account's latest stored reading and operator entries.
func (a *App) sharedLatest(ctx context.Context, key string) (shared.Latest, error) {
	if a.SharedLatest != nil {
		return a.SharedLatest(ctx, key)
	}
	c, err := a.quotaClient()
	if err != nil {
		return shared.Latest{}, err
	}
	var latest shared.Latest
	err = c.get(ctx, "/v1/quota/accounts/"+url.PathEscape(key)+"/latest", &latest)
	return latest, err
}

// admissionReadingFrom is pacelever's sharedAuto for a named account: a fresh
// automatic reading whose every window is valid and still running. A reading
// that is missing or too old is stale; one that is unusable at any age is
// malformed.
func admissionReadingFrom(x *shared.Observation, pool, key string, now time.Time, ttl time.Duration) (admissionReading, string) {
	if reading, ok := admissionAuto(x, pool, key, now, ttl, false); ok {
		return reading, ""
	}
	if x != nil {
		if _, wellFormed := admissionAuto(x, pool, key, now, time.Duration(1<<63-1), true); !wellFormed {
			return admissionReading{}, msgAccountMalformed
		}
	}
	return admissionReading{}, msgAccountStale
}

// admissionAuto converts a stored automatic reading. With anyAge set it only
// checks the reading's shape: age and passed resets are ignored.
func admissionAuto(x *shared.Observation, pool, key string, now time.Time, ttl time.Duration, anyAge bool) (admissionReading, bool) {
	if x == nil || x.Kind != shared.KindAuto || x.MeasuredAt.IsZero() {
		return admissionReading{}, false
	}
	age := now.Sub(shared.ValidAt(x))
	var free *shared.FreeBucket
	validFree := true
	if x.FreeBucket != nil {
		free, validFree = shared.NormalizeFree(x.FreeBucket, now)
		validFree = validFree && pool == "devin"
	}
	if age < -time.Minute || age > ttl || !validFree {
		return admissionReading{}, false
	}
	if len(x.Buckets) == 0 && free == nil || pool == "devin" && free == nil && !anyAge {
		return admissionReading{}, false
	}
	_, fp, _ := strings.Cut(key, ":")
	reading := admissionReading{Plan: x.Plan, FP: fp}
	if free != nil {
		reading.Free = &admissionFree{State: free.State, Reset: free.ResetsAt}
	}
	for _, b := range x.Buckets {
		used := b.UsedPct
		if !admissionValidPct(&used) || b.Scope != "account" && b.Scope != "model" && b.Scope != "group" {
			return admissionReading{}, false
		}
		bucket := admissionBucket{Window: b.Window, Used: &used, ScopeKind: b.Scope, ScopeName: b.Model}
		if b.ResetsAt != nil {
			if !b.ResetsAt.After(now) {
				if !anyAge {
					return admissionReading{}, false
				}
				bucket.Used = admissionPct(0)
			} else {
				bucket.Reset = b.ResetsAt.Format(time.RFC3339Nano)
			}
		}
		reading.Buckets = append(reading.Buckets, bucket)
	}
	return reading, true
}

// admissionManualFrom is pacelever's sharedManual: the operator entries that
// carry exactly one account window and an expiry.
func admissionManualFrom(xs []shared.Observation) []admissionManual {
	var out []admissionManual
	for _, x := range xs {
		if x.Kind != "manual" || x.ExpiresAt == nil || len(x.Buckets) != 1 || x.Buckets[0].Scope != "account" || x.Buckets[0].Window != x.Window {
			continue
		}
		b := x.Buckets[0]
		m := admissionManual{ID: fmt.Sprintf("hk-%d", x.ID), Window: x.Window, Used: admissionPct(b.UsedPct), MeasuredAt: x.MeasuredAt.Format(time.RFC3339Nano), ExpiresAt: x.ExpiresAt.Format(time.RFC3339Nano)}
		if b.ResetsAt != nil {
			m.Reset = b.ResetsAt.Format(time.RFC3339Nano)
		}
		out = append(out, m)
	}
	return out
}

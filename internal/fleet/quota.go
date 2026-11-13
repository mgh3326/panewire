package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleetevent"
	"github.com/mgh3326/panewire/internal/quota/agy"
	"github.com/mgh3326/panewire/internal/quota/shared"
	"github.com/mgh3326/panewire/internal/textbound"
)

type Launch struct {
	Catalog struct {
		Stale bool `json:"stale"`
	} `json:"catalog"`
	Profile string `json:"profile"`
	ModelID string `json:"model_id"`
	Effort  string `json:"effort"`
	Grade   string `json:"grade"`
	Gate    string `json:"gate"`
	Pool    string `json:"pool"`
	Family  string `json:"family"`
}

// row is the catalog rung a launch row stands for, for routing checks.
func (l Launch) row() CatalogRow {
	return CatalogRow{Profile: l.Profile, ModelID: l.ModelID, Effort: l.Effort, Gate: l.Gate, Grade: l.Grade, Pool: l.Pool, Family: l.Family}
}

// QuotaRequest asks the quota CLI about one launch. Profile is the launch
// canon; GateProfile, when set, is the name its gate knows the profile by.
// AccountKey scopes only Gate to shared evidence for a remote host's account.
type QuotaRequest struct {
	Profile, Effort, Role, OperatorRequest, RequestedBy, Purpose, GateProfile string
	AccountKey                                                                string
	// E6Arm is the exact <canon>@<effort> experiment marker, when policy
	// requires one. pacelever reads it from its own environment, so Launch and
	// Gate pass it explicitly instead of relying on this process having it.
	E6Arm string
	// E6ArmLocked marks a request whose marker is already authoritative (a
	// hub submission or a node-delivered spawn): the process environment
	// never fills an empty E6Arm.
	E6ArmLocked bool
}

// CatalogRow is one bench catalog rung: profile@effort with its operator-set
// grade, admission gate and quota pool. Retired rows are excluded by the
// selector.
type CatalogRow struct {
	Profile   string   `json:"profile"`
	Effort    string   `json:"effort"`
	Family    string   `json:"family,omitempty"`
	Grade     string   `json:"grade"`
	ModelID   string   `json:"model_id"`
	Pool      string   `json:"pool"`
	Gate      string   `json:"gate"`
	Score     *float64 `json:"score"`
	RetiredAt *string  `json:"retired_at"`
	// Group is the metering group of an agy rung ("gemini", "3p") as
	// pacelever's catalog names it; empty for other pools and for a rung it
	// does not group (meteringGroup).
	Group string `json:"group,omitempty"`
}

// PoolView is one quota pool's status from the quota CLI's status
// status document. Waste carries the subscription use-it-or-lose-it flag when
// the adapter reports verdicts; Blocking is the constraining used percentage
// (verdict.blocking_pct, else the highest account-scope bucket reading).
// Projected is the highest account-scope bucket's used percentage projected
// to its reset at the current pace (pacelever's Pace: used / elapsed window
// fraction); nil when no bucket can be paced yet. Urgency is the pool's
// waste urgency — the unused projected share (100 − projected) of the
// pool's longest account window (7d, monthly) per hour until that window's
// reset. Sub-daily windows (e.g. a 5h rate window) never rank a pool: they
// stay admission gates only. ResetHours is that window's time to reset.
// Both are nil when the longest window cannot be paced.
//
// RankProjected is the longest window projection. WorstLongProjected is the
// highest projection among windows of a day or more, and
// ShortUsed the highest used share among sub-daily windows; a waste-risk pool
// is excluded on those (a sub-daily window only when exhausted) instead of on
// Projected, which also carries the sub-daily windows' pace.
type PoolView struct {
	windowObserved     bool
	Status             string
	Stale              bool
	Class              string
	Waste              *bool
	Blocking           *float64
	Projected          *float64
	Urgency            *float64
	ResetHours         *float64
	RankProjected      *float64
	WorstLongProjected *float64
	ShortUsed          *float64
	// Credit is a prepaid credit pool's hand-entered balance, nil when the
	// manual store records none.
	Credit *CreditBalance
	// CreditUnavailable marks a pool whose balance could not be read or whose
	// shared account binding is missing; selection records the gap as degraded.
	CreditUnavailable bool
	// Free is a pool's free-tier availability (devin's SWE-2 bucket), nil for
	// a pool without one. Selection drops a free rung as free_limit while it
	// reads exhausted with a reset still ahead.
	Free *shared.FreeBucket
	// Groups is the view of each metering group of a reading that has only
	// group-scoped windows (agy), by group name. Each group view ranks on that
	// group's longest window of a day or more. The
	// fields above then carry the strictest group's view whole, so a rung that
	// maps to no group is blocked and paced on the same group. A rung that maps
	// to a group ranks on its own group's view instead (forRung).
	Groups map[string]PoolView
	// Group names the metering group this view is: a group view's own name, or
	// the strictest group's name on the account view of a group-only reading.
	// Empty for an account-scoped reading and for a pool view built elsewhere.
	Group string
	// Shortest is the view's shortest window, the one a job's expected hours
	// must fit inside (headroom key, select.go); nil when no window is known.
	Shortest *windowRoom
}

// forRung is the view a catalog rung ranks and is blocked on. A rung that maps
// to a metering group of a group-scoped reading takes its own group's view, so
// one exhausted group blocks only the rungs that draw from it; false when the
// reading has no window for that group (it stays unknown, never borrowed from
// another group). Every other rung — one that maps to no group, or any rung of
// an account-scoped reading — takes the whole view, the strictest group.
func (v PoolView) forRung(row CatalogRow) (PoolView, bool) {
	if len(v.Groups) == 0 {
		return v, true
	}
	group := row.meteringGroup()
	if group == "" {
		return v, true
	}
	g, ok := v.Groups[group]
	return g, ok
}

// meteringGroup is the quota group an agy rung draws from — "gemini" or "3p" —
// "" when it maps to none. It is the catalog row's own group when pacelever's
// catalog names one, else the group of its model id by pacelever's agy rule
// (agy.Group), so a rung of a new profile still maps.
func (r CatalogRow) meteringGroup() string {
	if r.Pool != "agy" {
		return ""
	}
	for _, group := range []string{r.Group, agy.Group(r.ModelID, "")} {
		if group == agy.GroupGemini || group == agy.Group3P {
			return group
		}
	}
	return ""
}

// QuotaGate is deliberately independent of the executable name; pacelever
// can implement this interface or the same CLI/JSON contract. Pools is read
// for the pool policy's credit pools only (selection and creditPoolStatus).
type QuotaGate interface {
	Launch(context.Context, QuotaRequest) (Launch, error)
	// Raw exit 3 is legacy quota-blocked compatibility at this boundary.
	// Adapters with broader codes must supply a typed gateRefusal.
	Gate(context.Context, QuotaRequest) (string, error)
	Catalog(context.Context) ([]CatalogRow, error)
	Pools(context.Context) (map[string]PoolView, error)
}

// requestGate is the optional half of a QuotaGate that refuses, before any
// host is judged, a request no quota reading can lift: role, subscription,
// request shape, catalog or an excluded pool. It returns the gate's typed
// refusal (gateRefusal.code set), or nil for a request that is admitted or
// needs a reading. A gate without it is judged host by host.
type requestGate interface {
	RequestRefusal(context.Context, QuotaRequest) error
}

type CLIQuota struct {
	Runner Runner
	Binary string
	Now    func() time.Time
	// CreditTotalUSD is each prepaid credit pool's fixed grant total in
	// dollars, from `[quota].credit_total_usd`. A shared manual entry records
	// only its used percentage, so credit_expiry.go derives the dollar balance
	// from this total; an entry that records its own dollars still wins.
	CreditTotalUSD map[string]float64
	// SharedLatest reads one account's stored manual and automatic entries
	// from hk. When set, credit balances are read there directly and the
	// pacelever process is never started. Accounts maps a pool to its
	// configured account fingerprint for that read.
	SharedLatest func(context.Context, string) (shared.Latest, error)
	Accounts     map[string]string
}

// launchResult reads a policy launch run into the launch it resolved; a stale
// catalog is refused.
func launchResult(result Result) (Launch, error) {
	if e := checkQuota(result, "quota catalog"); e != nil {
		return Launch{}, e
	}
	var l Launch
	if e := uniqueJSON([]byte(result.Out)); e != nil {
		return l, fail(exitConfig, "ambiguous quota catalog JSON: %v", e)
	}
	if e := json.Unmarshal([]byte(result.Out), &l); e != nil {
		return l, fail(exitConfig, "invalid quota catalog JSON: %v", e)
	}
	if l.Catalog.Stale {
		return l, fail(exitConfig, "quota catalog is stale")
	}
	return l, nil
}
func quotaArgs(a []string, r QuotaRequest) []string {
	for _, p := range [][2]string{{"--effort", r.Effort}, {"--role", r.Role}, {"--operator-request", r.OperatorRequest}, {"--requested-by", r.RequestedBy}, {"--purpose", r.Purpose}} {
		if p[1] != "" {
			a = append(a, p[0], p[1])
		}
	}
	return a
}

// gateResult reads a gate's exit status and JSON receipt — pacelever's or
// panewire's own (admission_native.go) — into the receipt an admission
// records or the typed refusal it surfaces.
func gateResult(x Result, r QuotaRequest) (string, error) {
	if x.Code == quotaGateRoleDenied {
		var body struct {
			Reason string `json:"reason"`
		}
		_ = json.Unmarshal([]byte(x.Out), &body)
		reason := body.Reason
		if reason == "" {
			reason = "gate denied this role"
		}
		reason = roleRestrictedReason(reason)
		return "", &gateRefusal{cause: fail(exitSeatRefused, "%s", reason), outcome: outcomeNoRoleCandidate, code: receiptGateCode(x.Out)}
	}
	if r.AccountKey != "" && (x.Code == 0 || x.Code == quotaGateBlocked) {
		receipt, err := readAccountGateReceipt(x, r.AccountKey)
		if x.Code == quotaGateBlocked {
			outcome := outcomeQuotaUnknown
			if err == nil {
				outcome, err = receipt.refusalOutcome()
			}
			reason := clipGateReason(receipt.Reason, 200)
			if err != nil {
				// A blocked gate with untrusted evidence cannot establish
				// exhaustion or a policy failure. Keep it typed for refresh.
				outcome, reason = outcomeQuotaUnknown, err.Error()
			}
			code := ""
			if err == nil {
				code = receiptGateCode(x.Out)
			}
			if outcome == outcomeNoRoleCandidate {
				return "", &gateRefusal{cause: fail(exitSeatRefused, "%s", roleRestrictedReason(reason)), outcome: outcome, code: code}
			}
			return "", &gateRefusal{cause: fail(x.Code, "quota gate: %s", reason), outcome: outcome, code: code}
		}
		if err != nil {
			return "", err
		}
	}
	if e := checkQuota(x, "quota gate"); e != nil {
		if code := receiptGateCode(x.Out); code != "" && (x.Code == quotaGateBlocked || x.Code == gateUnsubscribed) {
			// A refusal no reading decided keeps its gate code. Raw exit 3
			// stays the legacy quota block at this boundary, as judgeGate reads it.
			outcome := outcomeQuotaExhausted
			if x.Code != quotaGateBlocked {
				outcome = gateOutcome(e)
			}
			return "", &gateRefusal{cause: e, outcome: outcome, code: code}
		}
		return "", e
	}
	if !json.Valid([]byte(x.Out)) {
		return "", fail(exitConfig, "invalid quota gate JSON")
	}
	return x.Out, nil
}

// receiptGateCode reads the gate_code of a refusal receipt: the code of a
// refusal decided by the request, the catalog or policy alone, never by a
// reading (admissionVerdict.GateCode). It is empty for every other receipt.
func receiptGateCode(out string) string {
	var body struct {
		GateCode string `json:"gate_code"`
	}
	if json.Unmarshal([]byte(out), &body) != nil || fleetevent.Code(body.GateCode) != body.GateCode {
		return ""
	}
	return body.GateCode
}

// gateCodeNote is the host note of a refusal the gate decided from the
// request, the catalog or policy, never from a reading: "gate_blocked:" and
// the gate's code. It is empty for any other error.
func gateCodeNote(err error) string {
	var refusal *gateRefusal
	if errors.As(err, &refusal) && refusal.code != "" {
		return "gate_blocked:" + refusal.code
	}
	return ""
}

// hostGateNote is the note a refused host carries in the placement record: the
// gate's own code for a refusal no reading decided, else quota_<outcome>.
func hostGateNote(j quotaJudgment, err error) string {
	return firstNonempty(gateCodeNote(err), "quota_"+j.Outcome)
}

func roleRestrictedReason(reason string) string {
	if strings.HasPrefix(reason, "role_restricted:") {
		return reason
	}
	return "role_restricted: " + reason
}

func clipGateReason(reason string, max int) string {
	reason = strings.Join(strings.Fields(reason), " ")
	if len(reason) <= max {
		return reason
	}
	const ellipsis = "…"
	if max <= len(ellipsis) {
		return textbound.PrefixWithin(reason, max)
	}
	contentLimit := max - len(ellipsis)
	headLimit := contentLimit / 2
	tailLimit := contentLimit - headLimit
	head := textbound.PrefixWithin(reason, headLimit)
	if i := strings.LastIndexByte(head, ' '); i > 0 {
		head = head[:i]
	}
	tail := textbound.SuffixWithin(reason, tailLimit)
	if i := strings.IndexByte(tail, ' '); i >= 0 && i+1 < len(tail) {
		tail = tail[i+1:]
	}
	return head + ellipsis + tail
}

// gateRefusal carries a gate outcome independently of diagnostic prose;
// untrusted receipts stay unknown. Unwrap preserves the dependency exit code.
type gateRefusal struct {
	cause   error
	outcome string
	// code is the gate's own code for a refusal no quota reading decided (the
	// request, the catalog or policy did); empty when a reading or the
	// evidence behind one did.
	code string
}

func (e *gateRefusal) Error() string { return e.cause.Error() }
func (e *gateRefusal) Unwrap() error { return e.cause }

type accountGateReceipt struct {
	Schema       string `json:"schema"`
	AccountKey   string `json:"account_key"`
	Pool         string `json:"provider_id"`
	OK           *bool  `json:"ok"`
	ExitCode     *int   `json:"exit_code"`
	OutcomeCode  string `json:"outcome_code"`
	ReasonCode   string `json:"reason_code"`
	Reason       string `json:"reason"`
	RoleDenied   bool   `json:"role_denied"`
	Unmeasurable bool   `json:"unmeasurable"`
	Unsubscribed bool   `json:"unsubscribed"`
}

func readAccountGateReceipt(x Result, key string) (accountGateReceipt, error) {
	var receipt accountGateReceipt
	pool, _, _ := strings.Cut(key, ":")
	if !validID(pool) || uniqueJSON([]byte(x.Out)) != nil || json.Unmarshal([]byte(x.Out), &receipt) != nil || receipt.Schema != "scopefuel.gate.v1" || receipt.AccountKey != key || receipt.Pool != "" && receipt.Pool != pool || receipt.OK == nil || *receipt.OK != (x.Code == 0) || receipt.ExitCode == nil || *receipt.ExitCode != x.Code {
		return receipt, fail(exitConfig, "quota gate did not confirm the target account and result")
	}
	return receipt, nil
}

func (r accountGateReceipt) refusalOutcome() (string, error) {
	outcome := ""
	merge := func(next string) error {
		if next == "" {
			return nil
		}
		if outcome != "" && outcome != next {
			return fail(exitConfig, "quota gate has conflicting refusal codes")
		}
		outcome = next
		return nil
	}
	if r.OutcomeCode != "" {
		switch r.OutcomeCode {
		case outcomeQuotaExhausted, outcomeQuotaUnknown, outcomeNoRoleCandidate, outcomeAuthUnavailable, outcomeNoEligibleHost, outcomePolicyUnavailable:
			outcome = r.OutcomeCode
		default:
			return outcomeQuotaUnknown, nil
		}
	}
	reasons := map[string]string{
		"quota_exhausted": outcomeQuotaExhausted, "window_exhausted": outcomeQuotaExhausted, "projected_exhaust": outcomeQuotaExhausted,
		"quota_unknown": outcomeQuotaUnknown, "role_denied": outcomeNoRoleCandidate, "no_role_candidate": outcomeNoRoleCandidate,
		"auth_unavailable": outcomeAuthUnavailable, "unsubscribed": outcomeAuthUnavailable,
		"no_eligible_host": outcomeNoEligibleHost, "policy_unavailable": outcomePolicyUnavailable,
	}
	if err := merge(reasons[r.ReasonCode]); err != nil {
		return "", err
	}
	for _, flag := range []struct {
		set     bool
		outcome string
	}{{r.RoleDenied, outcomeNoRoleCandidate}, {r.Unmeasurable, outcomeQuotaUnknown}, {r.Unsubscribed, outcomeAuthUnavailable}} {
		if flag.set {
			if err := merge(flag.outcome); err != nil {
				return "", err
			}
		}
	}
	if outcome == "" {
		outcome = outcomeQuotaUnknown
	}
	return outcome, nil
}

// sharedReason names why a target account's shared observation is unusable,
// or "" when it is observed, fresh and not degraded.
func sharedReason(key string, acc SharedAccount) string {
	switch {
	case key == "":
		return "no_usable_presence"
	case acc.Key != key:
		return "account_mismatch"
	case acc.Degraded:
		return "degraded"
	case !acc.Observed:
		return "not_observed"
	case !acc.Fresh:
		return "stale"
	}
	return ""
}

// selectedAccountReason names why the executing host cannot confirm that it
// holds the selected account, or "" when it can. held is the account its own
// fresh presence names; outside credit pools the shared reading must also be
// observed and not degraded. Freshness belongs to the account-bound quota gate.
func selectedAccountReason(selected, held string, acc SharedAccount, credit bool) string {
	switch {
	case held == "":
		return "no_usable_presence"
	case held != selected:
		return "account_mismatch (host holds " + held + ")"
	case credit:
		return ""
	}
	why := sharedReason(held, acc)
	if why == "stale" {
		return ""
	}
	return why
}

// quotaJudgment binds a gate attempt to its host and, for shared evidence, the
// exact account passed to pacelever. Unknown evidence never invokes a local gate.
type quotaJudgment struct {
	Host       string `json:"host"`
	AccountKey string `json:"account_key,omitempty"`
	Source     string `json:"source"`
	Outcome    string `json:"outcome"`
	ExitCode   int    `json:"exit_code"`
	Reason     string `json:"reason,omitempty"`
	// Basis is the gate's own admission basis (for example free-bucket:NAME).
	Basis string `json:"basis,omitempty"`
	// Measured records an on-target measurement: "ok" or "failed".
	Measured string `json:"measured,omitempty"`
	// State is where the shared reading came from: the hub's fleet state or
	// a direct read (fleet_state.go).
	State *StateUse `json:"state,omitempty"`
}

// freeBasisPrefix opens the gate_basis of an admit that drew on a free bucket.
const freeBasisPrefix = "free-bucket:"

// gateBasis reads the admission basis a gate reports: the gate_basis field of
// its JSON receipt, or a gate_basis=VALUE word of the text form.
func gateBasis(gate string) string {
	if strings.HasPrefix(strings.TrimSpace(gate), "{") {
		var receipt struct {
			Basis string `json:"gate_basis"`
		}
		if json.Unmarshal([]byte(gate), &receipt) == nil {
			return receipt.Basis
		}
		return ""
	}
	for _, word := range strings.Fields(gate) {
		if w, ok := strings.CutPrefix(word, "gate_basis="); ok {
			return w
		}
	}
	return ""
}

// localAccountKeys lists the pool account keys this host holds: the pinned
// [quota.accounts] fingerprint and every key the local presence records name.
func (a *App) localAccountKeys(ctx context.Context, pool string) []string {
	return a.heldAccountKeys(ctx, []string{pool})[pool]
}

// heldAccountKeys is localAccountKeys for several pools from one presence read.
func (a *App) heldAccountKeys(ctx context.Context, pools []string) map[string][]string {
	held := map[string][]string{}
	accounts, _ := a.Config.Quota["accounts"].(map[string]any)
	for _, pool := range pools {
		if fp := str(accounts, pool); accountFP.MatchString(fp) {
			held[pool] = append(held[pool], pool+":"+fp)
		}
	}
	if a.Source == nil {
		return held
	}
	records, e := a.Source.Presence(ctx)
	if e != nil {
		return held
	}
	id := firstNonempty(str(a.Config.Local, "presence_id"), a.machine())
	for _, rec := range records {
		pool := accountPool(rec.AccountKey)
		if rec.HostID == id && slices.Contains(pools, pool) && !slices.Contains(held[pool], rec.AccountKey) {
			held[pool] = append(held[pool], rec.AccountKey)
		}
	}
	return held
}

func refreshCommand(pool, _ string, native bool) Command {
	if !native {
		return Command{}
	}
	return Command{Name: "panewire", Args: []string{"quota", "refresh", pool}}
}

// remoteRefreshLine is refreshCommand as the one shell line ssh runs on the
// target.
func remoteRefreshLine(pool string, native bool) string {
	if !native {
		return "printf '%s\\n' 'quota pool has no measurement source' >&2; exit 4"
	}
	return "panewire quota refresh " + shellQuote(pool)
}

// measureTarget runs the on-demand refresh for the pool on the target over the
// host's SSH transport so its account's shared observation is current. The
// refresh only measures an account the target holds.
func (a *App) measureTarget(ctx context.Context, host, pool string) error {
	h, ok := a.Config.Hosts[host]
	if !ok || !validID(pool) {
		return fail(exitConfig, "target %s pool %s cannot be measured", host, pool)
	}
	if !a.nativeQuotaPool(pool) {
		return fail(exitQuotaUnknown, "%s 풀에는 native 측정 소스가 없습니다", pool)
	}
	if str(h, "via") == "hub" {
		return a.refreshViaHub(ctx, host, pool)
	}
	target := firstNonempty(str(h, "ssh"), host)
	if strings.HasPrefix(target, "-") || strings.ContainsAny(target, "\r\n") {
		return fail(exitConfig, "invalid SSH target")
	}
	timeout := time.Duration(integer(a.Config.Quota, "measure_timeout_s", 60)) * time.Second
	if timeout <= 0 || timeout > time.Minute {
		timeout = time.Minute
	}
	r := runQuotaCommand(ctx, a.Runner, Command{Name: "ssh", Args: []string{"-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "--", target, remoteRefreshLine(pool, true)}}, "quota refresh", timeout)
	return checkQuotaPlain(r, "quota refresh")
}

func (a *App) judgeHost(ctx context.Context, r QuotaRequest, pool, host string, local bool, key string, acc SharedAccount) (quotaJudgment, error) {
	j := quotaJudgment{Host: host, AccountKey: r.AccountKey, Source: "local", Outcome: "unknown", ExitCode: 4}
	policy, err := a.poolPolicy()
	if err != nil {
		return j, err
	}
	if policy.isCredit(pool) {
		if !local {
			j.Source, j.AccountKey = "shared", key
			if key == "" || r.AccountKey != "" && r.AccountKey != key {
				j.Reason, j.ExitCode = "account_mismatch", exitAuthUnavailable
				return j, fail(exitAuthUnavailable, "AUTH_UNAVAILABLE: target no longer holds the selected account")
			}
			r.AccountKey = key
		}
		if err := a.creditPoolStatus(ctx, pool); err != nil {
			j.Reason, j.ExitCode = strings.ToLower(gateOutcome(err)), exitCode(err)
			return j, err
		}
		return a.judgeGate(ctx, r, pool, j)
	}
	if !local {
		j.Source, j.AccountKey = "shared", key
		j.Reason = sharedReason(key, acc)
		j.State = acc.State
		// Every path that leaves this block with a reason refuses the host
		// and records exactly one quota_observation event, here.
		observed := ""
		defer func() {
			if observed != "" {
				a.quotaObservationEvent(host, pool, observed, acc)
			}
		}()
		// A stale or missing observation of an account this host does not hold
		// is never refreshed by the gate, so measure it on the target first.
		if (j.Reason == "stale" || j.Reason == "not_observed") && a.Source != nil && !slices.Contains(a.localAccountKeys(ctx, pool), key) {
			if err := a.measureTarget(ctx, host, pool); err != nil {
				j.Measured, j.Reason = "failed", "measure_failed"
				if exitCode(err) == exitPlacement && str(a.Config.Hosts[host], "via") == "hub" {
					// The hub is the only way to measure a hub host; its
					// outage is a placement refusal, not an unconfirmed quota,
					// so it records no quota_observation event either.
					j.Reason = hubDown
					return j, err
				}
				observed = j.Reason
				return j, &quotaHostRefusal{fail(exitEvictionUnconfirmed, "target account measurement failed: %v", err), j.Reason}
			}
			j.Measured = "ok"
			fresh, err := a.Source.SharedAccount(ctx, key)
			if err != nil {
				fresh = SharedAccount{Key: key, Degraded: true}
			}
			if j.Reason = sharedReason(key, fresh); j.Reason != "" {
				j.Reason = "measure_" + j.Reason
			}
		}
		if j.Reason != "" {
			observed = j.Reason
			return j, &quotaHostRefusal{fail(exitEvictionUnconfirmed, "target account quota unavailable: %s (host %s, pool %s)", j.Reason, host, pool), j.Reason}
		}
		r.AccountKey = key
	}
	return a.judgeGate(ctx, r, pool, j)
}

// creditPoolStatus checks policy metadata, not a credit balance. A successful
// read can omit a balance-less pool, but a failed read cannot clear exclusions.
func (a *App) creditPoolStatus(ctx context.Context, pool string) error {
	if a.Quota == nil {
		return &gateRefusal{cause: fail(exitQuotaUnknown, "QUOTA_UNKNOWN: credit pool status unavailable"), outcome: outcomeQuotaUnknown}
	}
	views, err := a.Quota.Pools(ctx)
	if err != nil {
		return &gateRefusal{cause: fail(exitQuotaUnknown, "QUOTA_UNKNOWN: credit pool status unavailable"), outcome: outcomeQuotaUnknown}
	}
	if views[pool].Class == "exclude" {
		return &gateRefusal{cause: fail(exitSeatRefused, "NO_ROLE_CANDIDATE: credit pool %s is excluded", pool), outcome: outcomeNoRoleCandidate}
	}
	return nil
}

// judgeGate preserves the quota gate's account, pool and policy authority.
// Credit candidates use it without requiring a nonexistent balance reading.
func (a *App) judgeGate(ctx context.Context, r QuotaRequest, pool string, j quotaJudgment) (quotaJudgment, error) {
	if a.Quota == nil {
		return j, fail(exitEvictionUnconfirmed, "quota gate unavailable")
	}
	gate, err := a.Quota.Gate(ctx, r)
	if err != nil && exitCode(err) == quotaGateBlocked {
		var typed *gateRefusal
		if !errors.As(err, &typed) {
			// The legacy QuotaGate contract uses raw exit 3 for quota blocks.
			// Only this gate boundary grants that meaning, never refresh errors.
			err = &gateRefusal{cause: err, outcome: outcomeQuotaExhausted}
		}
	}
	if err == nil {
		accountPool, _, _ := strings.Cut(r.AccountKey, ":")
		var resolved string
		resolved, err = boundPool(pool, accountPool, gatePool(gate))
		if err == nil && resolved == "" {
			err = fail(exitConfig, "quota gate omitted the launch pool")
		}
	}
	j.Outcome, j.ExitCode = "admitted", 0
	if err == nil {
		j.Basis = gateBasis(gate)
	}
	if err != nil {
		j.Outcome, j.ExitCode = "refused", exitCode(err)
	}
	return j, err
}

// Catalog reads the bench catalog from handoffkeep with pacelever's cache
// rules (one-hour TTL, one-day stale maximum). A stale catalog is refused:
// grades and models must come from fresh evidence. No pacelever process runs.
func (q CLIQuota) Catalog(ctx context.Context) ([]CatalogRow, error) {
	data, e := q.CatalogDetails(ctx)
	if e != nil {
		return nil, e
	}
	return catalogDataRows(data), nil
}

// CatalogDetails returns the rows and decode warnings used by planning.
func (q CLIQuota) CatalogDetails(ctx context.Context) (CatalogData, error) {
	client, e := NewCatalogClient(CatalogClientConfig{})
	if e != nil {
		return CatalogData{}, fail(exitConfig, "%v", e)
	}
	data, e := client.Read(ctx, false)
	if e != nil {
		return CatalogData{}, fail(exitConfig, "%v", e)
	}
	if data.Info.Stale {
		return CatalogData{}, fail(exitConfig, "bench catalog is stale")
	}
	return data, nil
}

func (q CLIQuota) Launch(ctx context.Context, r QuotaRequest) (Launch, error) {
	if q.Binary == "" {
		return Launch{}, fail(exitConfig, "native quota launch adapter required")
	}
	args := quotaArgs([]string{"policy", "launch", r.Profile, "--json"}, r)
	result := runQuotaCommand(ctx, q.Runner, Command{Name: q.Binary, Args: args, Env: quotaE6Env(r.E6Arm)}, "quota catalog", 15*time.Second)
	return launchResult(result)
}

func (q CLIQuota) Gate(ctx context.Context, r QuotaRequest) (string, error) {
	if q.Binary == "" {
		return "", fail(exitConfig, "native quota gate adapter required")
	}
	name := firstNonempty(r.GateProfile, r.Profile)
	args := quotaArgs([]string{"gate", "-m", name}, r)
	if r.AccountKey != "" {
		args = append(args, "--account-key", r.AccountKey)
	}
	args = append(args, "--json")
	return gateResult(runQuotaCommand(ctx, q.Runner, Command{Name: q.Binary, Args: args, Env: quotaE6Env(r.E6Arm)}, "quota gate", 30*time.Second), r)
}

func quotaE6Env(arm string) []string {
	if arm == "" {
		return nil
	}
	return []string{"PANEWIRE_E6_ARM=" + arm}
}

// Pools reads the quota CLI's status document and reduces it to
// the per-pool signals the selection order needs. An absent verdict means the
// adapter does not report waste flags or a blocking percentage; the selector
// degrades those inputs rather than inventing them.
func (q CLIQuota) Pools(ctx context.Context) (map[string]PoolView, error) {
	if q.Binary == "" {
		return nil, fail(exitConfig, "native quota pool adapter required")
	}
	r := runQuotaCommand(ctx, q.Runner, Command{Name: q.Binary, Args: []string{"--json"}}, "quota status", 30*time.Second)
	if e := checkQuota(r, "quota status"); e != nil {
		return nil, e
	}
	var doc struct {
		Schema      string `json:"schema"`
		GeneratedAt string `json:"generated_at"`
		Providers   []struct {
			ID        string `json:"id"`
			Status    string `json:"status"`
			Stale     bool   `json:"stale"`
			PoolClass string `json:"pool_class"`
			Verdict   *struct {
				Waste    *bool    `json:"waste"`
				Blocking *float64 `json:"blocking_pct"`
			} `json:"verdict"`
			Buckets []struct {
				Window   string   `json:"window"`
				UsedPct  *float64 `json:"used_pct"`
				ResetsAt string   `json:"resets_at"`
				Scope    struct {
					Kind string `json:"kind"`
				} `json:"scope"`
			} `json:"buckets"`
		} `json:"providers"`
	}
	if e := json.Unmarshal([]byte(r.Out), &doc); e != nil || doc.Schema == "" {
		return nil, fail(exitConfig, "invalid quota status JSON")
	}
	// Projections are paced at the document's own measurement time.
	now, e := time.Parse(time.RFC3339Nano, doc.GeneratedAt)
	if e != nil {
		now = time.Now()
	}
	// The status document can repeat a provider id (a stale cached row plus a
	// fresh measurement); usable rows win, and among them the strictest
	// readings accumulate.
	groups := map[string][]int{}
	for i, p := range doc.Providers {
		groups[p.ID] = append(groups[p.ID], i)
	}
	pools := map[string]PoolView{}
	for id, idx := range groups {
		var src []int
		for _, i := range idx {
			p := doc.Providers[i]
			if p.Status == "ok" && !p.Stale {
				src = append(src, i)
			}
		}
		if len(src) == 0 {
			src = idx
		}
		v := PoolView{}
		// Ranking windows: only the pool's longest account window of a day or
		// more sets waste urgency; a longer unpaceable window clears any
		// urgency a shorter window produced.
		var longestW time.Duration
		for n, i := range src {
			p := doc.Providers[i]
			if n == 0 {
				v.Status, v.Class = p.Status, p.PoolClass
			}
			v.Stale = v.Stale || p.Stale
			if p.Verdict != nil {
				if p.Verdict.Waste != nil && *p.Verdict.Waste {
					v.Waste = p.Verdict.Waste
				} else if v.Waste == nil {
					v.Waste = p.Verdict.Waste
				}
				if p.Verdict.Blocking != nil && (v.Blocking == nil || *p.Verdict.Blocking > *v.Blocking) {
					v.Blocking = p.Verdict.Blocking
				}
			}
			for _, b := range p.Buckets {
				if b.Scope.Kind != "account" {
					continue
				}
				if b.UsedPct != nil && (v.Blocking == nil || *b.UsedPct > *v.Blocking) {
					v.Blocking = b.UsedPct
				}
				w, werr := windowDuration(b.Window)
				if werr == nil {
					v.windowObserved = true
				}
				ranking := werr == nil && w >= 24*time.Hour
				if ranking && w > longestW {
					longestW = w
					v.Urgency, v.ResetHours, v.RankProjected = nil, nil, nil
				}
				if werr == nil && !ranking && b.UsedPct != nil && (v.ShortUsed == nil || *b.UsedPct > *v.ShortUsed) {
					v.ShortUsed = b.UsedPct
				}
				if x := projectAtReset(b.UsedPct, b.Window, b.ResetsAt, now); x != nil {
					if v.Projected == nil || *x > *v.Projected {
						v.Projected = x
					}
					if ranking && (v.WorstLongProjected == nil || *x > *v.WorstLongProjected) {
						v.WorstLongProjected = x
					}
					if ranking && w == longestW && (v.RankProjected == nil || *x > *v.RankProjected) {
						v.RankProjected = x
					}
					if ranking && w == longestW {
						if reset, e := time.Parse(time.RFC3339Nano, b.ResetsAt); e == nil {
							if hours := reset.Sub(now).Hours(); hours > 0 {
								if u := (100 - *x) / hours; v.Urgency == nil || u > *v.Urgency {
									urg, left := u, hours
									v.Urgency, v.ResetHours = &urg, &left
								}
							}
						}
					}
				}
			}
		}
		pools[id] = v
	}
	// A credit balance is optional: without a readable store every credit
	// pool keeps the credit tier, as before balances were read.
	if credits, e := q.creditBalances(ctx); e == nil {
		for pool, c := range credits {
			v := pools[pool]
			v.Credit = c
			pools[pool] = v
		}
	} else {
		for pool, view := range pools {
			if slices.Contains(creditAccountPools, pool) {
				view.CreditUnavailable = true
				pools[pool] = view
			}
		}
	}
	return pools, nil
}

// projectAtReset is pacelever's Pace expressed as a percentage: the used
// share divided by the elapsed share of the window. A bucket that is past its
// reset or unreadable has no projection. A long window less than 5% elapsed
// projects its used share; a sub-daily one has no projection yet. Unlike
// Pace, an over-consumed reading above 100% still projects, so the pool is
// excluded rather than treated as unpaceable.
func projectAtReset(used *float64, window, resetsAt string, now time.Time) *float64 {
	if used == nil || math.IsNaN(*used) || math.IsInf(*used, 0) || *used < 0 {
		return nil
	}
	w, e := windowDuration(window)
	if e != nil || w <= 0 {
		return nil
	}
	reset, e := time.Parse(time.RFC3339Nano, resetsAt)
	if e != nil {
		return nil
	}
	left := reset.Sub(now)
	fraction := 1 - float64(left)/float64(w)
	if left <= 0 || fraction > 1 {
		return nil
	}
	if fraction < 0.05 {
		if w < 24*time.Hour {
			return nil
		}
		x := *used
		return &x
	}
	x := *used / fraction
	return &x
}

// windowDuration parses a quota window ("5h", "7d", "1w", "1d12h") with the
// same grammar as pacelever's Duration.
func windowDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, fail(exitConfig, "empty duration")
	}
	var total time.Duration
	for len(s) > 0 {
		i := 0
		for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '.') {
			i++
		}
		if i == 0 || i == len(s) {
			return 0, fail(exitConfig, "invalid duration")
		}
		n, e := strconv.ParseFloat(s[:i], 64)
		if e != nil || math.IsNaN(n) || math.IsInf(n, 0) {
			return 0, fail(exitConfig, "invalid duration")
		}
		unit := map[byte]time.Duration{'s': time.Second, 'm': time.Minute, 'h': time.Hour, 'd': 24 * time.Hour, 'w': 168 * time.Hour}[s[i]]
		if unit == 0 || n < 0 || n > 1e6 {
			return 0, fail(exitConfig, "invalid duration")
		}
		total += time.Duration(n * float64(unit))
		s = strings.TrimSpace(s[i+1:])
	}
	return total, nil
}

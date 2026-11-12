package fleet

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

const (
	outcomeAdmitted          = "ADMITTED"
	outcomeQuotaExhausted    = "QUOTA_EXHAUSTED"
	outcomeQuotaUnknown      = "QUOTA_UNKNOWN"
	outcomeAuthUnavailable   = "AUTH_UNAVAILABLE"
	outcomeNoEligibleHost    = "NO_ELIGIBLE_HOST"
	outcomePolicyUnavailable = "POLICY_UNAVAILABLE"
	outcomeNoRoleCandidate   = "NO_ROLE_CANDIDATE"
)

type accountBinding struct {
	key   string
	hosts []selHost
	// holders are every host that may measure the account: hosts plus
	// the holders --host or --exclude-host keep out of placement.
	holders []selHost
}

func (sp *selPresence) hostEligible(h selHost, seat *Seat) bool {
	return !h.excluded && sp.canMeasure(h, seat)
}

// canMeasure is hostEligible without the placement restriction.
func (sp *selPresence) canMeasure(h selHost, seat *Seat) bool {
	return h.eligible && (!h.excluded || h.restricted) && (h.awake || h.wakeable) && (seat.Capability == "standard" || boolean(h.cfg, seat.Capability, false))
}

func (sp *selPresence) hasHost(seat *Seat) bool {
	return slices.ContainsFunc(sp.hosts, func(h selHost) bool { return sp.hostEligible(h, seat) })
}

// bindings groups current logins by account, so two hosts never supply two
// copies of the same quota. Admission ranks hosts across these accounts. An
// account binds only through a placeable host; its restricted holders may
// still measure it.
func (sp *selPresence) bindings(seat *Seat, pool string) []accountBinding {
	var bindings []accountBinding
	if sp.presErr {
		return bindings
	}
	for _, h := range sp.hosts {
		if !sp.canMeasure(h, seat) {
			continue
		}
		key := hostAccount(sp.presence, pool, h.presence, sp.now, sp.presTTL)
		if key == "" {
			continue
		}
		i := slices.IndexFunc(bindings, func(b accountBinding) bool { return b.key == key })
		if i < 0 {
			bindings = append(bindings, accountBinding{key: key})
			i = len(bindings) - 1
		}
		bindings[i].holders = append(bindings[i].holders, h)
		if sp.hostEligible(h, seat) {
			// The readiness filter drops the pair before ranking: the host
			// stays a holder (it can still measure the account) but is not
			// offered this seat.
			if code := sp.readinessDrop(h, seat); code != "" {
				continue
			}
			bindings[i].hosts = append(bindings[i].hosts, h)
		}
	}
	bindings = slices.DeleteFunc(bindings, func(b accountBinding) bool { return len(b.hosts) == 0 })
	for _, b := range bindings {
		sortHolders(b.hosts)
		sortHolders(b.holders)
	}
	return bindings
}

// sortHolders orders an account's hosts: awake first, then by hub score when
// every host has one, else by load.
func sortHolders(hosts []selHost) {
	byScore := !slices.ContainsFunc(hosts, func(h selHost) bool { return h.score == nil })
	slices.SortStableFunc(hosts, func(x, y selHost) int {
		if x.awake != y.awake {
			if x.awake {
				return -1
			}
			return 1
		}
		if byScore {
			return cmp.Compare(*y.score, *x.score)
		}
		return cmp.Compare(loadOrInf(x.load), loadOrInf(y.load))
	})
}

// accountRefresh is one bounded refresh operation. A transport failure can
// move it to another holder; a completed measurement ends the operation.
type accountRefresh struct {
	measured string
	host     string
	err      error
}

// account caches both the reading and the refresh cause per account, so a
// second profile cannot repeat a measurement or disguise an infrastructure
// failure as missing quota evidence.
func (sp *selPresence) account(ctx context.Context, key, pool string, hosts []selHost, force bool) SharedAccount {
	acc, read := sp.accounts[key]
	if !read {
		var err error
		acc, err = sp.a.Source.SharedAccount(ctx, key)
		if err != nil {
			acc = SharedAccount{Key: key, Degraded: true}
		}
		sp.accounts[key] = acc
	}
	if _, refreshed := sp.refreshed[key]; refreshed || !force && sharedReason(key, acc) == "" && acc.Quota != nil {
		return acc
	}
	ordered := slices.Clone(hosts)
	if i := slices.IndexFunc(ordered, func(h selHost) bool { return h.local }); i > 0 {
		local := ordered[i]
		copy(ordered[1:i+1], ordered[:i])
		ordered[0] = local
	}
	refresh, cancel := context.WithTimeout(ctx, sp.a.placementTTL("measure_timeout_s", 60))
	defer cancel()
	result := accountRefresh{measured: "failed"}
	for _, h := range ordered {
		result.host = h.name
		if h.local {
			if !sp.a.nativeQuotaPool(pool) {
				result.err = fail(exitQuotaUnknown, "pool %s has no native measurement source", pool)
			} else {
				result.err = checkQuotaPlain(runQuotaCommand(refresh, sp.a.Runner, refreshCommand(pool, "", true), "quota refresh", 60*time.Second), "quota refresh")
			}
		} else {
			result.err = sp.a.measureTarget(refresh, h.name, pool)
		}
		if refresh.Err() != nil {
			result.err = fail(exitTimeout, "account measurement deadline expired")
			break
		}
		if result.err == nil {
			result.measured = "ok"
			var err error
			acc, err = sp.a.Source.SharedAccount(refresh, key)
			if refresh.Err() != nil {
				result.measured, result.err = "failed", fail(exitTimeout, "account measurement deadline expired during observation read")
				acc = SharedAccount{Key: key, Degraded: true}
				sp.refreshed[key], sp.accounts[key] = result, acc
				return acc
			}
			if err != nil {
				acc = SharedAccount{Key: key, Degraded: true}
			}
			sp.refreshed[key], sp.accounts[key] = result, acc
			return acc
		}
		// Only a transport failure takes the host down for the round:
		// NO_ELIGIBLE_HOST (exit 81) also covers account- and
		// capability-scoped refusals, which fail this account only.
		if code := exitCode(result.err); code == exitUnreachable || code == exitTimeout {
			sp.unreachable[h.name] = "measure_failed"
		}
		if !hostGateFailure(result.err) {
			break
		}
	}
	acc = SharedAccount{Key: key, Degraded: true}
	sp.refreshed[key], sp.accounts[key] = result, acc
	return acc
}

func (sp *selPresence) accountFailureCode(key string) string {
	if err := sp.refreshed[key].err; err != nil {
		return gateOutcome(err)
	}
	return outcomeQuotaUnknown
}

func (sp *selPresence) unknownJudgment(b accountBinding, why string) quotaJudgment {
	if why == "" {
		why = "no_account_reading"
	}
	host, status := b.hosts[0], exitEvictionUnconfirmed
	r := sp.refreshed[b.key]
	if r.measured != "" {
		why = "measure_" + why
		if r.measured == "failed" {
			why = "measure_failed"
		}
		for _, h := range b.hosts {
			if h.name == r.host {
				host = h
				break
			}
		}
		if r.err != nil {
			status = exitCode(r.err)
		}
	}
	source := "shared"
	if host.local {
		source = "local"
	}
	return quotaJudgment{Host: host.name, AccountKey: b.key, Source: source, Outcome: "unknown", ExitCode: status, Reason: why, Measured: r.measured}
}

// classifyRung classifies c on the view of the account's reading that its rung
// draws from (PoolView.forRung). A rung whose group the reading has no window
// for is quota_unknown, so a refresh may still supply it.
func (p *selPlan) classifyRung(c *selCand, account *PoolView) {
	v, ok := account.forRung(c.row)
	if !ok {
		c.reason, c.code, c.outcome = "quota_unknown", outcomeQuotaUnknown, "dropped:quota_unknown"
		return
	}
	p.classify(c, v)
}

func (p *selPlan) classify(c *selCand, v PoolView) {
	c.blocking, c.projected, c.urgency, c.resetHrs = v.Blocking, v.Projected, v.Urgency, v.ResetHours
	c.longProj = longProjection(v)
	c.headroom = p.policy.headroomFor(v.Shortest, p.req.expected)
	c.wasteGroup = v.Group
	if p.policy.isCredit(c.row.Pool) || v.Class == "credit" {
		c.creditPool = true
		c.creditLeft = false
		// Version 3 retains the free index; the comparator places credit after free.
		c.tierName, c.tier = creditTier, slices.Index(p.order, "free")
		if v.Class == "exclude" {
			c.reason, c.code, c.outcome = "pool_excluded", outcomeNoRoleCandidate, "dropped:pool_excluded"
			return
		}
		if v.Credit != nil && v.Credit.LeftPct <= 0 {
			c.reason, c.code, c.outcome = "credit_exhausted", outcomeQuotaExhausted, "dropped:credit_exhausted"
			c.tier, c.creditSince = -1, v.Credit.MeasuredAt
			return
		}
		at := p.req.creditAt
		if at.IsZero() {
			at = p.at
		}
		// An entry whose credit expiry has passed is not a candidate. An
		// entry with no expiry at all stays, as it always has, for the gate
		// to judge.
		if v.Credit != nil && !v.Credit.ExpiresAt.IsZero() && !v.Credit.ExpiresAt.After(at) {
			c.reason, c.code, c.outcome = "credit_expired", outcomeQuotaExhausted, "dropped:credit_expired"
			return
		}
		// A recorded balance competes until its expiry. With dollars, rank by
		// the dollars at risk after the spend projected before expiry; without,
		// keep the percentage rule and tell the operator to record dollars.
		if v.Credit != nil && v.Credit.BalanceUSD == nil && !slices.Contains(p.creditMissing, c.row.Pool) {
			p.creditMissing = append(p.creditMissing, c.row.Pool)
		}
		if ok, hours := v.Credit.expiring(at); ok {
			c.creditLeft = v.Credit.StaleAt.IsZero()
			c.staleCredit = !v.Credit.StaleAt.IsZero()
			c.credit = &creditFinal{balance: v.Credit, hoursLeft: hours}
			if v.Credit.BalanceUSD != nil {
				burn := v.Credit.BurnUSDPerH
				atRisk := math.Max(0, *v.Credit.BalanceUSD-burn*hours)
				usdUrgency := atRisk / hours
				c.credit.burnUSDPerH, c.credit.atRiskUSD, c.credit.usdUrgency = &burn, &atRisk, &usdUrgency
				c.usdUrgency = &usdUrgency
				return
			}
			left, urgency := v.Credit.LeftPct, v.Credit.LeftPct/hours
			c.urgency, c.resetHrs, c.unused = &urgency, &hours, &left
		}
		return
	}
	free := p.policy.isFree(c.row)
	c.risk, c.unused = p.policy.wasteRisk(c.row, free, v, true)
	c.paced = p.policy.paced(c.row, free, c.risk, v)
	if free {
		c.exhausted = v.Projected != nil && *v.Projected >= *p.policy.ExcludeAt
		// The free tier's own reading decides a limit; the shared paid
		// window never does.
		if freeLimitHeld(v.Free, p.at) {
			c.reason, c.code = "free_limit", outcomeQuotaExhausted
		}
	} else {
		c.reason = p.policy.exhaustReason(v, c.risk)
		if c.reason == "" && v.Blocking != nil && *v.Blocking >= 100 {
			c.reason = "window_exhausted"
		}
	}
	if c.reason != "" {
		c.code = outcomeQuotaExhausted
	} else {
		c.tierName, c.tier = p.policy.tierOf(c.row, p.order, v)
		if c.tier < 0 || !free && v.Blocking == nil && v.Projected == nil {
			c.reason, c.code = "quota_unknown", outcomeQuotaUnknown
		}
	}
	if c.reason != "" {
		c.outcome = "dropped:" + c.reason
	}
}

// freeLimitHeld reports a free-tier reading at its limit: the shared bucket is
// exhausted and its reset is still ahead. A reset that has passed reads as
// available (shared.NormalizeFree), so the free rung returns on its own.
func freeLimitHeld(f *shared.FreeBucket, now time.Time) bool {
	n, ok := shared.NormalizeFree(f, now)
	return ok && n != nil && n.State == shared.FreeExhausted
}

func gateOutcome(err error) string {
	var refusal *gateRefusal
	if errors.As(err, &refusal) {
		return refusal.outcome
	}
	switch exitCode(err) {
	case exitEvictionUnconfirmed:
		return outcomeQuotaUnknown
	case 6:
		return outcomeAuthUnavailable
	case exitSeatRefused:
		return outcomeNoRoleCandidate
	case exitUnreachable, exitTimeout, exitPlacement, exitLocalDisabled:
		return outcomeNoEligibleHost
	default:
		return outcomePolicyUnavailable
	}
}

func hostGateFailure(err error) bool {
	code := gateOutcome(err)
	return code == outcomeNoEligibleHost || code == outcomePolicyUnavailable
}

// admitAccount judges one account candidate on the given hosts in order. A
// refresh may still measure the account on any of its holders (c.holders).
func (sp *selPresence) admitAccount(ctx context.Context, qr QuotaRequest, c *selCand, p *selPlan, hosts []selHost) ([]quotaJudgment, error) {
	ordered := slices.Clone(hosts)
	// A holder that just completed the refresh is known reachable.
	if host := sp.refreshed[c.accountKey].host; host != "" {
		if i := slices.IndexFunc(ordered, func(h selHost) bool { return h.name == host }); i > 0 {
			ordered[0], ordered[i] = ordered[i], ordered[0]
		}
	}
	// An earlier rung may have refreshed this account after candidates were
	// ranked. Reuse its current reading before admitting another rung.
	if !c.creditPool {
		acc := sp.accounts[c.accountKey]
		if why := sharedReason(c.accountKey, acc); why != "" || acc.Quota == nil {
			c.code = sp.accountFailureCode(c.accountKey)
			c.reason = strings.ToLower(c.code)
			j := sp.unknownJudgment(accountBinding{key: c.accountKey, hosts: ordered}, why)
			if cause := sp.refreshed[c.accountKey].err; cause != nil {
				return []quotaJudgment{j}, cause
			}
			return []quotaJudgment{j}, fail(exitEvictionUnconfirmed, "account quota remains unknown after refresh")
		}
		p.classifyRung(c, acc.Quota)
		if c.reason != "" {
			j := quotaJudgment{Host: ordered[0].name, AccountKey: c.accountKey, Source: "shared", Outcome: "unknown", ExitCode: exitEvictionUnconfirmed, Reason: c.reason}
			if ordered[0].local {
				j.Source = "local"
			}
			if c.code == outcomeQuotaExhausted {
				j.Outcome, j.ExitCode = "refused", 3
			}
			return []quotaJudgment{j}, fail(j.ExitCode, "%s: %s", c.code, c.reason)
		}
	}
	var judgments []quotaJudgment
	var last error
	for _, h := range ordered {
		acc := sp.accounts[c.accountKey]
		var j quotaJudgment
		var err error
		if c.creditPool {
			source := "shared"
			if h.local {
				source = "local"
			}
			j, err = sp.a.judgeGate(ctx, qr, c.row.Pool, quotaJudgment{Host: h.name, AccountKey: c.accountKey, Source: source})
		} else {
			j, err = sp.a.judgeHost(ctx, qr, c.row.Pool, h.name, h.local, c.accountKey, acc)
		}
		if err != nil && !c.creditPool && gateOutcome(err) == outcomeQuotaUnknown {
			if _, refreshed := sp.refreshed[c.accountKey]; !refreshed {
				acc = sp.account(ctx, c.accountKey, c.row.Pool, c.holders, true)
				if cause := sp.refreshed[c.accountKey].err; cause != nil {
					j.Reason, j.Measured, j.ExitCode = "measure_failed", "failed", exitCode(cause)
					return append(judgments, j), cause
				}
				if sharedReason(c.accountKey, acc) == "" && acc.Quota != nil {
					p.classifyRung(c, acc.Quota)
					if c.reason == "" {
						j, err = sp.a.judgeHost(ctx, qr, c.row.Pool, h.name, h.local, c.accountKey, acc)
					}
				}
			}
		}
		j.Measured = sp.refreshed[c.accountKey].measured
		if err == nil && c.exhausted && !strings.HasPrefix(j.Basis, freeBasisPrefix) {
			c.reason, c.code = "projected_exhaust", outcomeQuotaExhausted
			j.Outcome, j.ExitCode = "refused", exitSeatRefused
			err = fail(exitSeatRefused, "projected_exhaust: gate basis %q is not a free bucket", j.Basis)
		}
		judgments = append(judgments, j)
		if err == nil {
			return judgments, nil
		}
		last = err
		if !hostGateFailure(err) {
			return judgments, err
		}
	}
	return judgments, last
}

func (a *App) selectionRefused(ctx context.Context, s Spawn, req selReq, p *selPlan) (*selPlan, error) {
	// Binding absence describes a profile, not another quota. If accounts
	// were found, their admission outcomes decide; missing logins or hosts
	// only decide when there was no eligible account to judge.
	accounts := make([]*selCand, 0, len(p.cands))
	for _, c := range p.cands {
		if c.accountKey != "" {
			accounts = append(accounts, c)
		}
	}
	if len(accounts) == 0 {
		accounts = p.cands
	}
	roleRestricted := slices.ContainsFunc(accounts, func(c *selCand) bool {
		return c.code == outcomeNoRoleCandidate && strings.HasPrefix(c.reason, "role_restricted:")
	})
	preemptingOutcome := slices.ContainsFunc(accounts, func(c *selCand) bool {
		switch c.code {
		case outcomeQuotaUnknown, outcomeAuthUnavailable, outcomeNoEligibleHost, outcomePolicyUnavailable:
			return true
		default:
			return false
		}
	})
	if roleRestricted && !preemptingOutcome {
		for _, c := range accounts {
			if c.code != outcomeNoRoleCandidate || !strings.HasPrefix(c.reason, "role_restricted:") {
				continue
			}
			p.outcome = outcomeNoRoleCandidate
			note := fmt.Sprintf(": no account admitted routing role %s%s%s%s", routingRole(req), hostScope(s), p.capsGap+p.hostDropNote(), p.poolAtCapNote())
			return p, fail(exitSeatRefused, "%s%s", c.reason, note)
		}
	}
	order := []string{outcomeQuotaUnknown, outcomeAuthUnavailable, outcomeNoEligibleHost, outcomePolicyUnavailable, outcomeNoRoleCandidate}
	if !slices.ContainsFunc(p.cands, func(c *selCand) bool { return c.accountKey != "" }) {
		// No account was bound, so no measured account failure decided this.
		// A host-side drop (repo_unmapped, caps, readiness) names the cause
		// the operator can repair; AUTH_UNAVAILABLE would send them to sign
		// in instead.
		order = []string{outcomeQuotaUnknown, outcomeNoEligibleHost, outcomeAuthUnavailable, outcomePolicyUnavailable, outcomeNoRoleCandidate}
	}
	for _, code := range order {
		if !slices.ContainsFunc(accounts, func(c *selCand) bool { return c.code == code }) {
			continue
		}
		p.outcome = code
		exit := map[string]int{outcomeQuotaUnknown: exitQuotaUnknown, outcomeAuthUnavailable: exitAuthUnavailable, outcomeNoEligibleHost: exitPlacement, outcomePolicyUnavailable: exitConfig, outcomeNoRoleCandidate: exitSeatRefused}[code]
		gap := ""
		if code == outcomeNoEligibleHost {
			gap = p.capsGap + p.hostDropNote()
		}
		return p, fail(exit, "%s: no account admitted routing role %s%s%s%s", code, routingRole(req), hostScope(s), gap, p.poolAtCapNote())
	}
	if slices.ContainsFunc(accounts, func(c *selCand) bool { return c.code == outcomeQuotaExhausted }) {
		p.outcome = outcomeQuotaExhausted
		if req.route != nil && req.route.rule != nil {
			return a.roleExhausted(ctx, s, req, p)
		}
		return p, fail(exitRoleWait, "%s: every eligible account is measured exhausted%s%s", outcomeQuotaExhausted, hostScope(s), p.poolAtCapNote())
	}
	p.outcome, p.routeRefusal = outcomeNoRoleCandidate, reasonRoleNoCandidate
	if note := p.poolAtCapNote(); note != "" {
		return p, fail(exitSeatRefused, "%s: %s: every eligible candidate is in a pool at its concurrency cap%s", reasonRoleNoCandidate, outcomeNoRoleCandidate, note)
	}
	return p, fail(exitSeatRefused, "%s: %s: no profile passes role, retirement, grade, gate and seat rules", reasonRoleNoCandidate, outcomeNoRoleCandidate)
}

func routingRole(req selReq) string {
	if req.route != nil {
		return req.route.role
	}
	return req.role
}

// hostDropNote names the dominant host-side drop among the candidates, with
// the repair that matches it. It answers "" when capsGap already names the
// cause or no host-side drop was recorded.
func (p *selPlan) hostDropNote() string {
	if p.capsGap != "" {
		return ""
	}
	counts := map[string]int{}
	for _, c := range p.cands {
		if c.code == outcomeNoEligibleHost && c.reason != "" {
			counts[c.reason]++
		}
	}
	reason, best := "", 0
	for r, n := range counts {
		if n > best || n == best && (reason == "" || r < reason) {
			reason, best = r, n
		}
	}
	if reason != ReadyRepoUnmapped {
		return ""
	}
	repo := p.repo
	if repo == "" {
		repo = "the repository"
	}
	return fmt.Sprintf("; %s: no node maps repo %s; add it to cwd_map or ensure work_root", reason, repo)
}

func hostScope(s Spawn) string {
	if s.Host != "" && s.Host != "auto" {
		return "; --host " + s.Host + " restricts accounts; other hosts' accounts were not considered (not fleet exhaustion)"
	}
	return ""
}

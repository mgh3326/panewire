package fleet

import (
	"context"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
)

// Pools is the pool view selection ranks a credit pool on and
// creditPoolStatus reads a credit pool's exclusion from; both read only the
// pool policy's credit pools. It follows `[quota].launch` (docs/quota.md
// §Credit pool views):
//
//   - shadow (the default): pacelever's status document (`pacelever --json`)
//     is the view, as before. After it is read, when the shared store serves
//     every credit pool, each pool's class is read offline from pacelever's
//     pool policy and compared with pacelever's (fleet event
//     quota_pools_shadow). The comparison runs in the background after the
//     views are returned (runShadow): it never reads the shared store, never
//     runs on the caller's context or holds up the spawn, and never reaches
//     the caller.
//   - native: when the shared store serves every credit pool — each is one of
//     the hand-recorded API pools (creditAccountPools) and shared reads are
//     wired — the view is built here and `pacelever --json` is not started;
//     otherwise pacelever's status document is read as in shadow.
func (q admissionQuota) Pools(ctx context.Context) (map[string]PoolView, error) {
	if q.app.launchMode() == launchModeNative {
		pools, served := q.nativeCreditPools()
		if !served {
			return q.CLIQuota.Pools(ctx)
		}
		views, err := q.creditStoreViews(ctx, pools)
		if err != nil {
			return nil, err
		}
		q.addCreditBalances(ctx, views)
		return views, nil
	}
	views, err := q.CLIQuota.Pools(ctx)
	q.shadowPools(maps.Clone(views), err)
	return views, err
}

// nativeCreditPools lists the policy's credit pools when the shared store
// serves every one of them.
func (q admissionQuota) nativeCreditPools() ([]string, bool) {
	if q.SharedLatest == nil {
		return nil, false
	}
	policy, err := LoadPoolPolicy(str(q.app.Config.Local, "pool_policy"))
	if err != nil {
		return nil, false
	}
	for _, pool := range policy.CreditPools {
		if !slices.Contains(creditAccountPools, pool) {
			return nil, false
		}
	}
	return policy.CreditPools, true
}

// creditStoreViews is what pacelever's status document says of a credit
// pool, short of the balance: the class from its pool policy (pacelever's
// config.toml, read as the gate reads it) and the used share of the account's
// current operator credit entry. A store that cannot be read leaves the class
// alone, as pacelever's status keeps it for an unmeasured pool; an unreadable
// policy is an error, so a failed read never clears an exclusion.
func (q admissionQuota) creditStoreViews(ctx context.Context, pools []string) (map[string]PoolView, error) {
	views, policy, err := q.creditPolicyViews(pools)
	if err != nil {
		return nil, err
	}
	now := q.app.Now()
	for _, pool := range pools {
		v := views[pool]
		if fp := firstNonempty(q.Accounts[pool], policy.Accounts[pool]); accountFP.MatchString(fp) {
			if latest, err := q.SharedLatest(ctx, pool+":"+fp); err == nil {
				in := admissionInputs{Now: now, Manual: map[string][]admissionManual{pool: admissionManualFrom(latest.Manual)}}
				for _, m := range in.validManual(pool) {
					if m.Window == "credit" {
						v.Status, v.Blocking = "ok", m.Used
					}
				}
			}
		}
		views[pool] = v
	}
	return views, nil
}

// creditPolicyViews is each credit pool's view from pacelever's pool policy
// alone: its class, with no reading. It reads only the local config file.
func (q admissionQuota) creditPolicyViews(pools []string) (map[string]PoolView, admissionPolicy, error) {
	policy, err := loadAdmissionPolicy(os.Getenv, q.app.Home)
	if err != nil {
		return nil, policy, fail(exitConfig, "quota status: %v", err)
	}
	now := q.app.Now()
	views := map[string]PoolView{}
	for _, pool := range pools {
		views[pool] = PoolView{Status: "degraded", Class: policy.pool(pool, now).Class}
	}
	return views, policy, nil
}

// addCreditBalances sets each view's balance (credit_expiry.go), as
// CLIQuota.Pools does; without a readable store the views keep none.
func (q admissionQuota) addCreditBalances(ctx context.Context, views map[string]PoolView) {
	credits, err := q.creditBalances(ctx)
	if err != nil {
		return
	}
	for pool, c := range credits {
		if v, ok := views[pool]; ok {
			v.Credit = c
			views[pool] = v
		}
	}
}

// shadowPools starts the comparison of each credit pool's class from
// pacelever's pool policy, read offline, with pacelever's status document
// (runShadow) and returns at once; pacelever is a copy of the caller's
// views. When the shared store serves every credit pool, the comparison
// records its outcome as fleet event quota_pools_shadow. Only the class is compared: the used
// share is read from the shared store, which the comparison never reads, and
// the balance is the same store's on both paths. Outcome agree: both read and
// every credit pool has the same class; disagree otherwise, with the
// differing pool:field pairs; skipped (reason shadow_timeout) when the local
// reads outlast shadowBound. A comparison skipped because another was running
// records nothing; the next event's detail starts with skipped_busy=N. The
// event is best effort.
func (q admissionQuota) shadowPools(pacelever map[string]PoolView, perr error) {
	type answer struct {
		pools  []string
		served bool
		views  map[string]PoolView
		err    error
	}
	runShadow(shadows, func() answer {
		pools, served := q.nativeCreditPools()
		if !served {
			return answer{}
		}
		views, _, err := q.creditPolicyViews(pools)
		return answer{pools, true, views, err}
	}, func(native answer, inTime bool, skipped func() string) {
		events := q.app.fleetEventWriter()
		switch {
		case !inTime:
			events.Emit("quota_pools_shadow", "", "skipped", "shadow_timeout", strings.TrimSpace(skipped()))
			return
		case !native.served:
			return
		}
		var diff, sides []string
		switch {
		case perr != nil || native.err != nil:
			diff = append(diff, "error")
			sides = append(sides, fmt.Sprintf("pacelever{error=%q} native{error=%q}", errText(perr), errText(native.err)))
		default:
			for _, pool := range native.pools {
				p, n := pacelever[pool], native.views[pool]
				if p.Class != n.Class {
					diff = append(diff, pool+":class")
				}
				sides = append(sides, fmt.Sprintf("%s pacelever{class=%s} native{class=%s}", pool, p.Class, n.Class))
			}
		}
		outcome := "agree"
		if len(diff) > 0 {
			outcome = "disagree"
		}
		events.Emit("quota_pools_shadow", "", outcome, "credit", skipped()+strings.Join(sides, " ")+" diff="+strings.Join(diff, ","))
	})
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

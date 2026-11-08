package fleet

import (
	"context"
	"os"
	"slices"
)

// Pools is the pool view selection ranks a credit pool on and
// creditPoolStatus reads a credit pool's exclusion from; both read only the
// pool policy's credit pools. A credit pool is served natively when it is one
// of the hand-recorded API pools (creditAccountPools) and the operator's
// entries come from the shared store. When every credit pool is, the view is
// built here and `pacelever --json` is not started (docs/quota.md §Launch);
// otherwise pacelever's status document is read as before.
func (q admissionQuota) Pools(ctx context.Context) (map[string]PoolView, error) {
	pools, ok := q.nativeCreditPools()
	if !ok {
		return q.CLIQuota.Pools(ctx)
	}
	return q.creditViews(ctx, pools)
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

// creditViews is what pacelever's status document says of a credit pool: the
// class from its pool policy (pacelever's config.toml, read as the gate reads
// it), the used share of the account's current operator credit entry, and the
// balance (credit_expiry.go). A store that cannot be read leaves the class
// alone, as pacelever's status keeps it for an unmeasured pool; an unreadable
// policy is an error, so a failed read never clears an exclusion.
func (q admissionQuota) creditViews(ctx context.Context, pools []string) (map[string]PoolView, error) {
	policy, err := loadAdmissionPolicy(os.Getenv, q.app.Home)
	if err != nil {
		return nil, fail(exitConfig, "quota status: %v", err)
	}
	now := q.app.Now()
	views := map[string]PoolView{}
	for _, pool := range pools {
		v := PoolView{Status: "degraded", Class: policy.pool(pool, now).Class}
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
	if credits, err := q.creditBalances(ctx); err == nil {
		for pool, c := range credits {
			if v, ok := views[pool]; ok {
				v.Credit = c
				views[pool] = v
			}
		}
	}
	return views, nil
}

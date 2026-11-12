package fleet

import (
	"context"
	"os"
	"slices"
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
	pools, served := q.nativeCreditPools()
	if !served {
		return nil, fail(exitConfig, "quota status: shared manual store unavailable")
	}
	views, policy, err := q.creditStoreViews(ctx, pools)
	if err != nil {
		return nil, err
	}
	accounts := make(map[string]string, len(q.Accounts)+len(policy.Accounts))
	for pool, fp := range policy.Accounts {
		accounts[pool] = fp
	}
	for pool, fp := range q.Accounts {
		accounts[pool] = fp
	}
	q.addCreditBalances(ctx, views, pools, accounts)
	return views, nil
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
func (q admissionQuota) creditStoreViews(ctx context.Context, pools []string) (map[string]PoolView, admissionPolicy, error) {
	views, policy, err := q.creditPolicyViews(pools)
	if err != nil {
		return nil, policy, err
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
	return views, policy, nil
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
func (q admissionQuota) addCreditBalances(ctx context.Context, views map[string]PoolView, pools []string, accounts map[string]string) {
	reader := q.CLIQuota
	if reader.Now == nil && q.app != nil {
		reader.Now = q.app.Now
	}
	reader.Accounts = accounts
	for _, pool := range pools {
		if !accountFP.MatchString(accounts[pool]) {
			v := views[pool]
			v.CreditUnavailable = true
			views[pool] = v
		}
	}
	credits, err := reader.creditBalances(ctx)
	if err != nil {
		for _, pool := range pools {
			v := views[pool]
			v.CreditUnavailable = true
			views[pool] = v
		}
		return
	}
	for pool, c := range credits {
		if v, ok := views[pool]; ok {
			v.Credit = c
			views[pool] = v
		}
	}
}

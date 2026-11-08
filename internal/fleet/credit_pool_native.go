package fleet

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
)

// Pools is the pool view selection ranks a credit pool on and
// creditPoolStatus reads a credit pool's exclusion from; both read only the
// pool policy's credit pools. It follows `[quota].launch` (docs/quota.md
// §Credit pool views):
//
//   - shadow (the default): pacelever's status document (`pacelever --json`)
//     is the view, as before. When the shared store serves every credit pool,
//     the native view is built beside it and the two are compared (fleet event
//     quota_pools_shadow); the native view never reaches the caller.
//   - native: when the shared store serves every credit pool — each is one of
//     the hand-recorded API pools (creditAccountPools) and shared reads are
//     wired — the view is built here and `pacelever --json` is not started;
//     otherwise pacelever's status document is read as in shadow.
func (q admissionQuota) Pools(ctx context.Context) (map[string]PoolView, error) {
	pools, served := q.nativeCreditPools()
	if served && q.app.launchMode() == launchModeNative {
		views, err := q.creditStoreViews(ctx, pools)
		if err != nil {
			return nil, err
		}
		q.addCreditBalances(ctx, views)
		return views, nil
	}
	views, err := q.CLIQuota.Pools(ctx)
	if served {
		shadow, cancel := context.WithTimeout(ctx, shadowLaunchTimeout)
		native, nerr := q.creditStoreViews(shadow, pools)
		cancel()
		q.app.shadowPools(pools, views, err, native, nerr)
	}
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
	return views, nil
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

// shadowPools records how the native credit pool views compare with
// pacelever's (fleet event quota_pools_shadow). Only the class and the used
// share are compared: the balance is read from the same store on both paths.
// Outcome agree: both read and every credit pool has the same class and used
// share; disagree otherwise, with the differing pool:field pairs.
func (a *App) shadowPools(pools []string, pacelever map[string]PoolView, perr error, native map[string]PoolView, nerr error) {
	var diff, sides []string
	switch {
	case perr != nil || nerr != nil:
		diff = append(diff, "error")
		sides = append(sides, fmt.Sprintf("pacelever{error=%q} native{error=%q}", errText(perr), errText(nerr)))
	default:
		for _, pool := range pools {
			p, n := pacelever[pool], native[pool]
			if p.Class != n.Class {
				diff = append(diff, pool+":class")
			}
			if fmtPct(p.Blocking) != fmtPct(n.Blocking) {
				diff = append(diff, pool+":blocking")
			}
			sides = append(sides, fmt.Sprintf("%s pacelever{class=%s blocking=%s} native{class=%s blocking=%s}", pool, p.Class, fmtPct(p.Blocking), n.Class, fmtPct(n.Blocking)))
		}
	}
	outcome := "agree"
	if len(diff) > 0 {
		outcome = "disagree"
	}
	a.emitFleetEvent("quota_pools_shadow", "", outcome, "credit", strings.Join(sides, " ")+" diff="+strings.Join(diff, ","))
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// fmtPct is a used share to two decimals, or "-" when absent.
func fmtPct(v *float64) string {
	if v == nil {
		return "-"
	}
	return strconv.FormatFloat(*v, 'f', 2, 64)
}

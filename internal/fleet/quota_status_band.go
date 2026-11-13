package fleet

import (
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

// StatusWindow is one window of a quota status reading, in the shape the
// status table reads it.
type StatusWindow struct {
	Window   string
	UsedPct  *float64
	ResetsAt string
	Scope    string
	Model    string
}

// StatusBand names the band `quota status` prints for a pool. It applies the
// same rules as selection (PoolPolicy.tierOf) to the reading's own windows, so
// the table cannot disagree with placement on the same reading. free, excluded
// and credit come before the windows, as tierOf orders them; unknown is a stale
// reading or one that gives no tier.
func StatusBand(policy PoolPolicy, pool, class string, stale bool, windows []StatusWindow, now time.Time) string {
	row := CatalogRow{Pool: pool}
	switch {
	case policy.isFree(row):
		return "free"
	case class == "exclude":
		return "excluded"
	case policy.isCredit(pool) || class == "credit":
		return creditTier
	case stale:
		return "unknown"
	}
	buckets := make([]accountBucket, 0, len(windows))
	for _, w := range windows {
		buckets = append(buckets, accountBucket{Window: w.Window, UsedPct: w.UsedPct, ResetsAt: w.ResetsAt, Scope: w.Scope, Model: w.Model})
	}
	v := accountPoolView(buckets, now)
	if v == nil {
		return "unknown"
	}
	tier, _ := policy.tierOf(row, policy.Order, *v)
	if tier == "" {
		return "unknown"
	}
	return tier
}

// CreditWindow is a prepaid credit pool's newest manual credit entry: the used
// share the operator recorded and when that entry expires.
type CreditWindow struct {
	UsedPct   float64
	ExpiresAt time.Time
}

// CreditWindowOf is the newest manual credit entry of an account, by measured
// time. ok is false when the store holds no well-formed credit entry.
func CreditWindowOf(latest shared.Latest) (CreditWindow, bool) {
	var (
		best         CreditWindow
		bestMeasured time.Time
		found        bool
	)
	for _, m := range admissionManualFrom(latest.Manual) {
		if m.Window != "credit" || m.Used == nil {
			continue
		}
		expiry, err := time.Parse(time.RFC3339Nano, m.ExpiresAt)
		if err != nil {
			continue
		}
		measured, err := time.Parse(time.RFC3339Nano, m.MeasuredAt)
		if err != nil {
			continue
		}
		if !found || measured.After(bestMeasured) {
			best = CreditWindow{UsedPct: *m.Used, ExpiresAt: expiry}
			bestMeasured = measured
			found = true
		}
	}
	return best, found
}

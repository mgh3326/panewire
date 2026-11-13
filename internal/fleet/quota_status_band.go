package fleet

import (
	"os"
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

// StatusPoolClass is pool's operator class at now from the admission policy:
// the class the admission verdict refuses an excluded pool on, and the class
// selection ranks it under. An unreadable policy is an error, never a guessed
// class; a missing policy is the default, where every pool is spend.
func StatusPoolClass(pool string, now time.Time) (string, error) {
	home, _ := os.UserHomeDir()
	policy, err := loadAdmissionPolicy(os.Getenv, home)
	if err != nil {
		return "", err
	}
	return policy.pool(pool, now).Class, nil
}

// StatusBand names the band `quota status` prints for a pool. It applies the
// same rules as selection (PoolPolicy.tierOf) to the reading's own windows, with
// class as the operator's admission class for the pool, so the table cannot
// disagree with placement on the same reading. free, excluded and credit come
// before the windows, as tierOf orders them; unknown is a stale reading or one
// that gives no tier.
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
	// accountPoolView reports the spend class for every reading; the operator's
	// class replaces it so tierOf ranks an admission-preserved pool as placement does.
	v.Class = class
	tier, _ := policy.tierOf(row, policy.Order, *v)
	if tier == "" {
		return "unknown"
	}
	return tier
}

// CreditWindow is a prepaid credit pool's credit entry: the used share the
// operator recorded, if any, and when the entry expires.
type CreditWindow struct {
	UsedPct   *float64
	ExpiresAt time.Time
}

// CreditWindowOf is the credit entry CreditEntryOf reads as the account's
// state: the unexpired entry that expires last, so the row agrees with a live
// credit state; when no entry is unexpired, the newest recorded entry, as the
// expired state reports. ok is false when the store holds no well-formed
// credit entry.
func CreditWindowOf(latest shared.Latest, now time.Time) (CreditWindow, bool) {
	var (
		best         CreditWindow
		bestMeasured time.Time
		bestLive     bool
		found        bool
	)
	for _, m := range admissionManualFrom(latest.Manual) {
		if m.Window != "credit" {
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
		live := expiry.After(now)
		var better bool
		switch {
		case !found:
			better = true
		case live != bestLive:
			better = live
		case live:
			better = expiry.After(best.ExpiresAt)
		default:
			better = measured.After(bestMeasured)
		}
		if better {
			best = CreditWindow{ExpiresAt: expiry}
			if m.Used != nil {
				used := *m.Used
				best.UsedPct = &used
			}
			bestMeasured, bestLive, found = measured, live, true
		}
	}
	return best, found
}

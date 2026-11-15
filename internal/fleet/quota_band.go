package fleet

import "time"

// The selection band a pool's reading falls in, as the fleet status snapshot
// (fleet/status/latest) labels it: the four names the pool policy ranks on.
const (
	BandWaste  = "waste"
	BandFree   = "free"
	BandCredit = creditTier
	BandRanked = "ranked"
	// BandExcluded is a pool the policy excludes from selection altogether.
	BandExcluded = "excluded"
)

// QuotaWindow is one window of an account's quota reading: the window name as
// the provider reports it ("5h", "7d"), its used percentage, and the reset
// time as RFC3339 (empty when the provider gives none).
type QuotaWindow struct {
	Window   string
	UsedPct  *float64
	ResetsAt string
}

// QuotaView reduces a reading's windows to the view the selection order ranks
// on. It is the same reduction CLIQuota.Pools applies to the quota status
// document, for a caller that holds only window readings (the hub's fleet
// state); now is the reading's own measurement time, as projections are paced
// at it.
func QuotaView(windows []QuotaWindow, now time.Time) PoolView {
	v := PoolView{}
	var longestW time.Duration
	for _, b := range windows {
		w, err := windowDuration(b.Window)
		if err == nil {
			v.windowObserved = true
		}
		if b.UsedPct != nil && (v.Blocking == nil || *b.UsedPct > *v.Blocking) {
			v.Blocking = b.UsedPct
		}
		ranking := err == nil && w >= 24*time.Hour
		if ranking && w > longestW {
			longestW = w
			v.Urgency, v.ResetHours, v.RankProjected = nil, nil, nil
		}
		if err == nil && !ranking && b.UsedPct != nil && (v.ShortUsed == nil || *b.UsedPct > *v.ShortUsed) {
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
							v.Urgency, v.ResetHours = new(u), new(hours)
						}
					}
				}
			}
		}
	}
	return v
}

// Band names the selection band a pool's reading falls in under p: "free" for
// a pool the policy lists a free set for, "credit" for a credit pool (and a
// reading the adapter classes as credit), "waste" when the reading's longest
// window is projected below the policy's waste threshold, "excluded" for a
// pool the policy excludes, else "ranked".
func (p PoolPolicy) Band(pool string, v PoolView) string {
	for _, free := range p.Free {
		if free.Pool == pool {
			return BandFree
		}
	}
	if p.isCredit(pool) || v.Class == "credit" {
		return BandCredit
	}
	if v.Class == "exclude" {
		return BandExcluded
	}
	below := 100.0
	if p.WasteBelow != nil {
		below = *p.WasteBelow
	}
	if long := longProjection(v); long != nil {
		if *long < below {
			return BandWaste
		}
	} else if !v.windowObserved && v.Waste != nil && *v.Waste {
		return BandWaste
	}
	return BandRanked
}

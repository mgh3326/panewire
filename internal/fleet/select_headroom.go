package fleet

import (
	"time"

	"github.com/mgh3326/handoffkeep/client"
)

// The headroom key prefers an account whose shortest window can carry the
// job's expected hours. A candidate fits when that window keeps at least
// burn x expected hours of its share, or the window resets within the first
// half of the job. Fitting candidates rank ahead of non-fitting ones inside a
// band (compareSelCands); nothing is refused or delayed, and a job without an
// expected time never fits, so its order is unchanged.

// Burn defaults in percent of a window per job-hour, used until a measured
// rate is recorded. Windows up to one day burn at the short rate. The rates
// are documented in docs/config.md under the pool order policy.
const (
	defaultBurnShortPctPerH = 15.0
	defaultBurnLongPctPerH  = 2.0
)

// BurnPolicy holds the burn rates by window length class; an absent rate
// takes its default.
type BurnPolicy struct {
	ShortPctPerH *float64 `json:"short_pct_per_h"`
	LongPctPerH  *float64 `json:"long_pct_per_h"`
}

// headroomFit is one candidate's fit against the job's expected hours.
// remainingPct is nil when the candidate's view has no window to fit.
type headroomFit struct {
	fits          bool
	remainingPct  *float64
	burnPctPerH   float64
	expectedHours float64
	resetHours    *float64
}

// burnPctPerH is the burn rate for a window of the given length. A window of
// unknown length takes the short rate, the larger and so the conservative one.
func (p PoolPolicy) burnPctPerH(window time.Duration) float64 {
	short, long := defaultBurnShortPctPerH, defaultBurnLongPctPerH
	if p.Burn != nil {
		short, long = orDefaultPct(p.Burn.ShortPctPerH, short), orDefaultPct(p.Burn.LongPctPerH, long)
	}
	if window > 24*time.Hour {
		return long
	}
	return short
}

func orDefaultPct(v *float64, dflt float64) float64 {
	if v == nil {
		return dflt
	}
	return *v
}

// headroomFor fits a candidate's shortest window against the job's expected
// time. It returns nil when the job has no expected time.
func (p PoolPolicy) headroomFor(room *windowRoom, expected time.Duration) *headroomFit {
	if expected <= 0 {
		return nil
	}
	hours := expected.Hours()
	if room == nil {
		return &headroomFit{burnPctPerH: p.burnPctPerH(0), expectedHours: hours}
	}
	remaining := 100 - room.usedPct
	f := &headroomFit{
		remainingPct:  &remaining,
		burnPctPerH:   p.burnPctPerH(room.window),
		expectedHours: hours,
		resetHours:    room.resetHours,
	}
	f.fits = remaining >= f.burnPctPerH*hours || room.resetHours != nil && *room.resetHours <= hours/2
	return f
}

// fits reports whether the candidate's shortest window fits the job.
func (c *selCand) fits() bool {
	return c.headroom != nil && c.headroom.fits
}

// taskExpected reads a task's expected duration; an absent value is no expected
// time. unreadable marks a value that did not parse; the key then stays off.
func taskExpected(t client.Task) (time.Duration, bool) {
	if t.Expected == nil {
		return 0, false
	}
	return parseExpected(*t.Expected)
}

// parseExpected reads an expected duration: a Go duration ("6h") or a day/hour
// window ("1d"). An empty value is no expected time.
func parseExpected(s string) (expected time.Duration, unreadable bool) {
	if s == "" {
		return 0, false
	}
	if d, err := time.ParseDuration(s); err == nil && d > 0 {
		return d, false
	}
	if d, err := windowDuration(s); err == nil && d > 0 {
		return d, false
	}
	return 0, true
}

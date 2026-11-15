package fleet

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/mgh3326/handoffkeep/client"

	"github.com/mgh3326/panewire/internal/durable"
)

// The headroom key charges a job's expected hours at a burn rate per pool
// (select_headroom.go). A pool's rate is measured from its accounts' quota
// observations in hk — the used share's rise per hour over the last
// burnHistorySpan, with a drop at a window reset left out rather than counted
// as negative burn. A pool whose history cannot support a rate keeps the
// policy defaults (15 %/h up to a day, 2 %/h longer) and the selection record
// says which rate was used and why (headroom.burn_source, headroom.burn_reason).
//
// Admission reads a pool's history at most once per burnCacheTTL: the measured
// rates live in the state directory, so a spawn reads that cache instead of hk.
// A pool is the unit of measurement: an account key names its pool
// ("codex:<fp>"), so a pool that several harnesses draw on shares its accounts
// and every harness burns it at the one measured rate. A burn rate only orders
// candidates; it never refuses or delays one (decision 172).

const (
	// burnHistorySpan is how far back a rate is measured. It covers a 5h
	// window past its reset and still gives a week-long window enough points.
	burnHistorySpan = 6 * time.Hour
	// burnMinSpan is the shortest coverage a rate may come from: two points
	// closer together measure noise, not a rate.
	burnMinSpan = time.Hour
	// burnCacheTTL is how long a pool's measurement is reused, so admission
	// reads hk history for a pool at most once per TTL, not once per spawn. A
	// failed read is cached for the same TTL: an hk outage must not make every
	// spawn wait on it.
	burnCacheTTL = 15 * time.Minute
	// burnCacheVersion is the cache file's schema version.
	burnCacheVersion = 1
	// burnObservationLimit is the hk observations route's page cap (1 to 5000).
	burnObservationLimit = 5000
)

// burnSourceMeasured and burnSourceDefault are the headroom record's
// burn_source values: a rate measured from observation history, or the pool
// policy's default because history did not supply one.
const (
	burnSourceMeasured = "measured"
	burnSourceDefault  = "default"
)

// Why a default stood in for a measured rate (headroom.burn_reason).
const (
	burnReasonNoHistory       = "no_history"
	burnReasonUnreadable      = "history_unreadable"
	burnReasonTooFewPoints    = "too_few_points"
	burnReasonShortSpan       = "history_span_short"
	burnReasonNoWindowHistory = "no_window_history"
	burnReasonUnknownWindow   = "window_unknown"
)

// burnRates is one pool's measured burn by window label ("5h", "7d"), and
// reason says why the policy defaults stand where no window was measured.
type burnRates struct {
	At      time.Time          `json:"at"`
	Windows map[string]float64 `json:"windows,omitempty"`
	Reason  string             `json:"reason,omitempty"`
}

// burnCache is the state directory's copy of the measured rates.
type burnCache struct {
	Version int                  `json:"version"`
	Pools   map[string]burnRates `json:"pools"`
}

// burnRate is the rate one candidate's window is charged at.
type burnRate struct {
	pctPerH float64
	source  string
	reason  string
	window  string
}

// burnRate resolves the rate for a window of the given length: the measured
// rate of the window whose label has that length, else the policy default with
// the reason the measurement gives. A window of unknown length never matches a
// measurable one (it cannot be paced, so the short default is the safe one).
func (r burnRates) rate(window time.Duration, p PoolPolicy) burnRate {
	if window <= 0 {
		return burnRate{pctPerH: p.burnPctPerH(window), source: burnSourceDefault, reason: burnReasonUnknownWindow}
	}
	labels := make([]string, 0, len(r.Windows))
	for label := range r.Windows {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	for _, label := range labels {
		if w, err := windowDuration(label); err == nil && w == window {
			return burnRate{pctPerH: r.Windows[label], source: burnSourceMeasured, window: label}
		}
	}
	reason := r.Reason
	if reason == "" {
		reason = burnReasonNoWindowHistory
	}
	return burnRate{pctPerH: p.burnPctPerH(window), source: burnSourceDefault, reason: reason}
}

// burnSeries is one window's measured rise over the deltas that counted.
type burnSeries struct {
	rise   float64 // summed positive used-share rise, in percent
	hours  float64 // hours the counted deltas cover
	points int     // observations of this window
}

// burnSeriesFromObservations folds one account's observations into a series
// per window label. A drop from one point to the next is a window reset: the
// new window starts below the old reading, so neither the fall nor the span
// between the two points is burn. Only automatic readings count; a manual
// entry is the operator's record, not a measurement.
func burnSeriesFromObservations(obs []client.QuotaObservation) map[string]burnSeries {
	type point struct {
		at   time.Time
		used float64
	}
	points := map[string][]point{}
	for _, o := range obs {
		if o.Kind != "auto" || o.MeasuredAt.IsZero() {
			continue
		}
		for _, b := range o.Buckets {
			if b.Scope != "account" && b.Scope != "" {
				continue
			}
			if _, err := windowDuration(b.Window); err != nil {
				continue
			}
			points[b.Window] = append(points[b.Window], point{at: o.MeasuredAt, used: b.UsedPct})
		}
	}
	out := map[string]burnSeries{}
	for label, pts := range points {
		sort.SliceStable(pts, func(i, j int) bool { return pts[i].at.Before(pts[j].at) })
		s := burnSeries{points: len(pts)}
		for i := 1; i < len(pts); i++ {
			hours := pts[i].at.Sub(pts[i-1].at).Hours()
			if hours <= 0 {
				// Two readings of one instant measure nothing.
				continue
			}
			if rise := pts[i].used - pts[i-1].used; rise >= 0 {
				s.rise += rise
				s.hours += hours
			}
		}
		out[label] = s
	}
	return out
}

// poolAccounts lists the account keys the presence snapshot binds to pool.
func (sp *selPresence) poolAccounts(pool string) []string {
	var keys []string
	for _, rec := range sp.presence {
		if strings.HasPrefix(rec.AccountKey, pool+":") && !slices.Contains(keys, rec.AccountKey) {
			keys = append(keys, rec.AccountKey)
		}
	}
	slices.Sort(keys)
	return keys
}

// burnRates returns pool's burn rates: the cached measurement while it is
// fresh, else a read of the pool's accounts' observation history. It never
// fails; a pool without a usable measurement names why in Reason. One plan
// reads a pool once however many candidates it has.
func (sp *selPresence) burnRates(ctx context.Context, pool string) burnRates {
	if r, ok := sp.burn[pool]; ok {
		return r
	}
	cache := sp.a.readBurnCache()
	if r, ok := cache.Pools[pool]; ok && sp.now.Sub(r.At) >= 0 && sp.now.Sub(r.At) < burnCacheTTL {
		sp.burn[pool] = r
		return r
	}
	r := sp.a.measureBurn(ctx, pool, sp.poolAccounts(pool), sp.now)
	if cache.Pools == nil {
		cache.Pools = map[string]burnRates{}
	}
	cache.Pools[pool] = r
	if err := sp.a.writeBurnCache(cache); err != nil {
		sp.a.warn("burn history cache not written: %v", err)
	}
	sp.burn[pool] = r
	return r
}

// measureBurn reads the pool's accounts' observations for the last
// burnHistorySpan and reduces them to a rate per window label: the summed rise
// over the summed covered hours across the pool's accounts (a usage-weighted
// mean, so one account gives its own rate). A pool with no account, no
// observation, too few points, too short a coverage or an unreadable answer
// keeps the defaults and names why.
func (a *App) measureBurn(ctx context.Context, pool string, keys []string, now time.Time) burnRates {
	out := burnRates{At: now}
	if len(keys) == 0 {
		out.Reason = burnReasonNoHistory
		return out
	}
	hk, err := newHKClient()
	if err != nil {
		out.Reason = burnReasonUnreadable
		return out
	}
	since, until := now.Add(-burnHistorySpan), now
	rise, hours, points := map[string]float64{}, map[string]float64{}, map[string]int{}
	for _, key := range keys {
		obs, err := hk.ListQuotaObservations(ctx, key, since, until, burnObservationLimit)
		if err != nil {
			return burnRates{At: now, Reason: burnReasonUnreadable}
		}
		for label, s := range burnSeriesFromObservations(obs) {
			rise[label] += s.rise
			hours[label] += s.hours
			points[label] += s.points
		}
	}
	best := 0
	for label, h := range hours {
		best = max(best, points[label])
		if points[label] < 2 || h < burnMinSpan.Hours() {
			continue
		}
		if out.Windows == nil {
			out.Windows = map[string]float64{}
		}
		out.Windows[label] = rise[label] / h
	}
	if len(out.Windows) > 0 {
		return out
	}
	if len(points) == 0 {
		out.Reason = burnReasonNoHistory
	} else if best < 2 {
		out.Reason = burnReasonTooFewPoints
	} else {
		out.Reason = burnReasonShortSpan
	}
	return out
}

// burnCachePath is the state directory's measured burn file.
func (a *App) burnCachePath() string { return filepath.Join(a.stateDir(), "burn-history.json") }

// readBurnCache reads the state directory's copy. An absent or unreadable file
// is an empty cache: a stale measurement is never trusted over a fresh read.
func (a *App) readBurnCache() burnCache {
	var c burnCache
	if b, err := os.ReadFile(a.burnCachePath()); err == nil && json.Unmarshal(b, &c) == nil && c.Version == burnCacheVersion {
		if c.Pools == nil {
			c.Pools = map[string]burnRates{}
		}
		return c
	}
	return burnCache{Version: burnCacheVersion, Pools: map[string]burnRates{}}
}

// writeBurnCache replaces the state directory's copy atomically. A failed
// write costs a later history read, never selection.
func (a *App) writeBurnCache(c burnCache) error {
	c.Version = burnCacheVersion
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(a.burnCachePath()), 0700); err != nil {
		return err
	}
	return durable.ReplaceFileAtomically(a.burnCachePath(), ".burn-history-*.json", append(b, '\n'), 0600)
}

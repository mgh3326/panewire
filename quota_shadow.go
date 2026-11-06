package panewire

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/quota/shared"
)

// Shadow mode (docs/quota.md §Shadow and native): a pool with a switch that the
// host's `[quota].native_pools` does not list is still refreshed by pacelever —
// spawns, the fleet-state loop and `quota.refresh` all run it. Beside that run
// the node measures the pool itself, with no store lease and no publication,
// and logs one line comparing the two readings. The desk reads the lines and
// flips the switch.

const (
	// shadowMeasureTimeout bounds the node's own measurement in shadow, so a
	// slow provider cannot hold up the pacelever refresh it rides on.
	shadowMeasureTimeout = 25 * time.Second
	// shadowUsedTolerance is how far apart two used percentages may be and
	// still agree: the two readings are seconds apart.
	shadowUsedTolerance = 1.0
	// shadowResetTolerance is the same for a window's reset time.
	shadowResetTolerance = time.Minute
)

// isShadowQuotaPool says whether pool is measured beside pacelever: it has a
// switch and the switch is not flipped.
func isShadowQuotaPool(pool string) bool {
	return fleet.QuotaPoolHasSwitch(pool) && !isNativeQuotaPool(pool)
}

// shadowBucket is one window of a reading in the comparison.
type shadowBucket struct {
	window string
	used   float64
	reset  time.Time // zero when the window names none
}

// shadowReading is one side of the comparison: the account key and windows it
// measured, or the error kind it failed with.
type shadowReading struct {
	key     string
	buckets []shadowBucket
	errKind string
}

func (r shadowReading) ok() bool { return r.errKind == "" }

// shadowFromPublish is pacelever's side: the reading its refresh printed, as
// the node would publish it. A refresh that gave none is an error of errKind.
func shadowFromPublish(publish FleetStatePublish, ok bool, errKind string) shadowReading {
	if !ok {
		return shadowReading{errKind: errKind}
	}
	reading := shadowReading{key: publish.Key}
	for _, b := range publish.Quota.Buckets {
		bucket := shadowBucket{window: b.Window}
		if b.UsedPct != nil {
			bucket.used = *b.UsedPct
		}
		if at, err := time.Parse(time.RFC3339, b.ResetsAt); err == nil {
			bucket.reset = at
		}
		reading.buckets = append(reading.buckets, bucket)
	}
	return reading
}

// shadowFromShared is the node's own side.
func shadowFromShared(reading shared.Reading, err error) shadowReading {
	if err != nil {
		return shadowReading{errKind: quotaErrorKind(err)}
	}
	out := shadowReading{key: reading.Key}
	for _, b := range reading.Buckets {
		bucket := shadowBucket{window: b.Window, used: b.UsedPct}
		if b.ResetsAt != nil {
			bucket.reset = *b.ResetsAt
		}
		out.buckets = append(out.buckets, bucket)
	}
	return out
}

func (r shadowReading) String() string {
	if !r.ok() {
		return "error " + r.errKind
	}
	parts := make([]string, 0, len(r.buckets))
	for _, b := range r.buckets {
		reset := "-"
		if !b.reset.IsZero() {
			reset = b.reset.UTC().Format(time.RFC3339)
		}
		parts = append(parts, fmt.Sprintf("%s=%.1f%%@%s", b.window, b.used, reset))
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// compareShadow lists how the node's reading differs from pacelever's, window
// by window, in pacelever's then the node's order. Empty means they agree.
func compareShadow(pacelever, native shadowReading) []string {
	var diffs []string
	switch {
	case !pacelever.ok() && !native.ok():
		if pacelever.errKind != native.errKind {
			diffs = append(diffs, "error kind differs")
		}
		return diffs
	case !pacelever.ok():
		return append(diffs, "pacelever failed ("+pacelever.errKind+"), native measured")
	case !native.ok():
		return append(diffs, "native failed ("+native.errKind+"), pacelever measured")
	}
	if pacelever.key != native.key {
		diffs = append(diffs, "account differs")
	}
	seen := map[string]bool{}
	index := func(buckets []shadowBucket, window string) (shadowBucket, bool) {
		for _, b := range buckets {
			if b.window == window {
				return b, true
			}
		}
		return shadowBucket{}, false
	}
	var windows []string
	for _, b := range append(append([]shadowBucket{}, pacelever.buckets...), native.buckets...) {
		if !seen[b.window] {
			seen[b.window] = true
			windows = append(windows, b.window)
		}
	}
	for _, window := range windows {
		p, inPacelever := index(pacelever.buckets, window)
		n, inNative := index(native.buckets, window)
		switch {
		case !inNative:
			diffs = append(diffs, window+": pacelever only")
		case !inPacelever:
			diffs = append(diffs, window+": native only")
		case math.Abs(p.used-n.used) > shadowUsedTolerance:
			diffs = append(diffs, fmt.Sprintf("%s: used %.1f%% vs %.1f%%", window, p.used, n.used))
		case p.reset.IsZero() != n.reset.IsZero() || !p.reset.IsZero() && p.reset.Sub(n.reset).Abs() > shadowResetTolerance:
			diffs = append(diffs, window+": reset differs")
		}
	}
	return diffs
}

// shadowQuotaLine is the one comparison line of a refresh: pool, whether the
// readings agree, the account match, both readings (windows, used%, resets) or
// error kinds, and the differences. No credential, response body or URL can be
// in it: every part is a window label, a number, a time or an error kind.
func shadowQuotaLine(pool string, pacelever, native shadowReading) string {
	diffs := compareShadow(pacelever, native)
	account := "unknown"
	if pacelever.ok() && native.ok() && pacelever.key != "" && native.key != "" {
		account = "differ"
		if pacelever.key == native.key {
			account = "same"
		}
	}
	diff := "none"
	if len(diffs) > 0 {
		diff = strings.Join(diffs, "; ")
	}
	return fmt.Sprintf("quota shadow %s: match=%t account=%s pacelever=%s native=%s diff=%s", pool, len(diffs) == 0, account, pacelever, native, diff)
}

// logShadowQuota measures pool in this process — directly, never through the
// shared store — and logs the comparison with pacelever's reading. It reports
// nothing back: spawns and the fleet-state publish keep pacelever's result.
func (client *HubClient) logShadowQuota(ctx context.Context, pool string, pacelever shadowReading) {
	if ctx.Err() != nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, shadowMeasureTimeout)
	defer cancel()
	reading, err := client.nativeQuotaSource(pool).measureOnly(ctx)
	client.warnMessage(shadowQuotaLine(pool, pacelever, shadowFromShared(reading, err)))
}

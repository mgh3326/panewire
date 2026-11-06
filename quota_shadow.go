package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
	"github.com/mgh3326/panewire/internal/quota/shared"
)

// Shadow comparison (docs/quota.md §Shadow and native). While a switched pool
// is in shadow the node's own measurement runs beside pacelever's and one log
// line says whether they agree. The comparison never changes what a spawn, the
// fleet-state loop or the quota.refresh command answers.

const (
	// shadowUsedTolerance is how far two used percentages may differ and still
	// match: the two readings are taken seconds to minutes apart.
	shadowUsedTolerance = 1.0
	// shadowResetTolerance is the same for reset times. devin's panel states a
	// reset as "in 6d 2h", so its resolution is the minute and the reading's age.
	shadowResetTolerance = 5 * time.Minute
)

// shadowPacelever is pacelever's reading of the pool, or why it has none.
type shadowPacelever struct {
	ok      bool
	publish FleetStatePublish
	// errKind is pacelever's error kind when !ok; kindFn, when set, computes it
	// lazily (it may ask pacelever for its status), off the caller's path.
	errKind string
	kindFn  func(context.Context) string
}

// paceleverShadow reads one `pacelever refresh` answer for the comparison. It
// is more permissive than the hub publish: a per-model free tag has no window,
// which the publish refuses, but is no reason to ignore the paid buckets next
// to it. errKind is what the caller knows of a failure; empty means ask kindFn.
func paceleverShadow(pool string, output []byte, runErr error, errKind string) shadowPacelever {
	pace := shadowPacelever{errKind: errKind}
	var reading paceleverReading
	if runErr != nil || json.Unmarshal(output, &reading) != nil || reading.ID != pool || reading.Stale || reading.Error != "" {
		return pace
	}
	fp := ""
	switch {
	case reading.PublishFPKind == "account" && reading.PublishFP != "":
		fp = reading.PublishFP
	case reading.AccountFPKind == "account" && reading.AccountFP != "":
		fp = reading.AccountFP
	}
	pace.ok = true
	pace.publish = FleetStatePublish{Quota: &FleetStateQuota{Pool: pool, Source: reading.Source}}
	if fp != "" {
		pace.publish.Key = pool + ":" + fp
	}
	for _, b := range reading.Buckets {
		if b.UsedPct == nil {
			continue
		}
		out := FleetStateBucket{Window: b.Window, UsedPct: b.UsedPct, Scope: b.Scope.Kind, Model: b.Scope.Name}
		if at, err := time.Parse(time.RFC3339Nano, b.ResetsAt); err == nil {
			out.ResetsAt = at.UTC().Format(time.RFC3339)
		}
		pace.publish.Quota.Buckets = append(pace.publish.Quota.Buckets, out)
	}
	return pace
}

// shadowBucket is one window of either reading, keyed by what it limits.
type shadowBucket struct {
	scope, model, window string
	used                 float64
	reset                time.Time // zero when the reading states none
}

func (b shadowBucket) id() string { return b.scope + "/" + b.model + "/" + b.window }

func (b shadowBucket) String() string {
	text := fmt.Sprintf("%s=%.4g", b.window, b.used)
	if b.scope == quota.ScopeModel {
		text = b.model + ":" + text
	}
	if !b.reset.IsZero() {
		text += "@" + b.reset.UTC().Format(time.RFC3339)
	}
	return text
}

func paceleverShadowBuckets(p FleetStatePublish) []shadowBucket {
	var out []shadowBucket
	if p.Quota == nil {
		return out
	}
	for _, b := range p.Quota.Buckets {
		// A bucket with no window is a per-model free tag, not a measurement.
		if b.UsedPct == nil || b.Window == "" {
			continue
		}
		bucket := shadowBucket{scope: b.Scope, model: b.Model, window: b.Window, used: *b.UsedPct}
		if at, err := time.Parse(time.RFC3339, b.ResetsAt); err == nil {
			bucket.reset = at
		}
		out = append(out, bucket)
	}
	return out
}

func nativeShadowBuckets(r shared.Reading) []shadowBucket {
	var out []shadowBucket
	for _, b := range r.Buckets {
		bucket := shadowBucket{scope: b.Scope, model: b.Model, window: b.Window, used: b.UsedPct}
		if b.ResetsAt != nil {
			bucket.reset = *b.ResetsAt
		}
		out = append(out, bucket)
	}
	return out
}

func joinShadowBuckets(buckets []shadowBucket) string {
	parts := make([]string, 0, len(buckets))
	for _, b := range buckets {
		parts = append(parts, b.String())
	}
	slices.Sort(parts)
	return strings.Join(parts, ",")
}

// quotaShadowComparison is the outcome the log line reports.
type quotaShadowComparison struct {
	Verdict string // match, differ, native_error, pacelever_error or both_error
	Account string // same, different or unknown
	Native  string
	Pace    string
	Diffs   []string
}

// compareQuotaShadow judges the node's reading against pacelever's: same
// account, the same buckets, used percentages and resets within tolerance.
func compareQuotaShadow(pace shadowPacelever, native shared.Reading, nativeErr error) quotaShadowComparison {
	c := quotaShadowComparison{Verdict: "match", Account: "unknown"}
	switch {
	case nativeErr != nil && !pace.ok:
		c.Verdict = "both_error"
		return c
	case nativeErr != nil:
		c.Verdict = "native_error"
		c.Pace = joinShadowBuckets(paceleverShadowBuckets(pace.publish))
		return c
	case !pace.ok:
		c.Verdict = "pacelever_error"
		c.Native = joinShadowBuckets(nativeShadowBuckets(native))
		return c
	}
	paceBuckets, nativeBuckets := paceleverShadowBuckets(pace.publish), nativeShadowBuckets(native)
	c.Native, c.Pace = joinShadowBuckets(nativeBuckets), joinShadowBuckets(paceBuckets)
	if native.Key != "" && pace.publish.Key != "" {
		c.Account = "same"
		if native.Key != pace.publish.Key {
			c.Account = "different"
			c.Diffs = append(c.Diffs, "account")
		}
	}
	for _, p := range paceBuckets {
		i := slices.IndexFunc(nativeBuckets, func(n shadowBucket) bool { return n.id() == p.id() })
		if i < 0 {
			c.Diffs = append(c.Diffs, "native_missing:"+p.id())
			continue
		}
		n := nativeBuckets[i]
		if math.Abs(n.used-p.used) > shadowUsedTolerance {
			c.Diffs = append(c.Diffs, "used:"+p.id())
		}
		if !n.reset.IsZero() && !p.reset.IsZero() && n.reset.Sub(p.reset).Abs() > shadowResetTolerance || n.reset.IsZero() != p.reset.IsZero() {
			c.Diffs = append(c.Diffs, "reset:"+p.id())
		}
	}
	for _, n := range nativeBuckets {
		if !slices.ContainsFunc(paceBuckets, func(p shadowBucket) bool { return p.id() == n.id() }) {
			c.Diffs = append(c.Diffs, "pacelever_missing:"+n.id())
		}
	}
	if len(c.Diffs) > 0 {
		c.Verdict = "differ"
	}
	return c
}

// shadowQuota compares the node's own measurement of pool with pacelever's
// reading, in the background so neither the spawn path nor the loop waits for
// it. One comparison per pool runs at a time; a request made while one runs is
// dropped, since the next reading will be compared too.
func (client *HubClient) shadowQuota(pool string, pace shadowPacelever) {
	client.shadowMu.Lock()
	if client.shadowRunning[pool] {
		client.shadowMu.Unlock()
		return
	}
	if client.shadowRunning == nil {
		client.shadowRunning = map[string]bool{}
	}
	client.shadowRunning[pool] = true
	client.shadowMu.Unlock()
	go func() {
		defer func() {
			client.shadowMu.Lock()
			delete(client.shadowRunning, pool)
			client.shadowMu.Unlock()
		}()
		ctx, cancel := context.WithTimeout(context.Background(), fleetQuotaTimeout)
		defer cancel()
		if !pace.ok && pace.kindFn != nil {
			pace.errKind = pace.kindFn(ctx)
		}
		native, err := client.nativeQuotaSource(pool).Shadow(ctx)
		if kind := quota.KindOf(err); kind == quota.ErrKindCredentials {
			// No login here: nothing to compare, and not worth a line per round.
			slog.Debug("quota shadow skipped", "pool", pool, "reason", "no_login")
			return
		}
		c := compareQuotaShadow(pace, native, err)
		if pace.ok {
			pace.errKind = ""
		}
		slog.Info("quota shadow compare", "pool", pool, "verdict", c.Verdict, "account", c.Account,
			"native_source", native.Source, "native_buckets", c.Native, "native_error_kind", shadowErrorKind(err),
			"pacelever_buckets", c.Pace, "pacelever_error_kind", pace.errKind, "diffs", strings.Join(c.Diffs, ","))
	}()
}

func shadowErrorKind(err error) string {
	if err == nil {
		return ""
	}
	return quotaErrorKind(err)
}

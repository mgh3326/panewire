package shared

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
)

const (
	// DefaultLeaseTTL is the measurement lease, in seconds.
	DefaultLeaseTTL = 120
	// DefaultFreshTTL is how long a claude reading stays current.
	DefaultFreshTTL = 15 * time.Minute
	// OtherFreshTTL is how long a reading of any other pool stays current
	// (pacelever's and placement's rule).
	OtherFreshTTL = 5 * time.Minute
	// DefaultRefreshMargin is the time reserved for the publisher's next
	// refresh cycle and measurement before a shared reading expires.
	DefaultRefreshMargin = 2 * time.Minute
	// OtherRefreshTTL is when a five-minute shared reading becomes due for a
	// refresh, leaving one node cycle and one measurement timeout before expiry.
	OtherRefreshTTL = OtherFreshTTL - DefaultRefreshMargin
	// DefaultPresenceEvery bounds how often a host re-announces an account.
	DefaultPresenceEvery = 10 * time.Minute
	defaultWait          = time.Second
	probeMargin          = 200 * time.Millisecond
	presenceTimeout      = 3 * time.Second
	releaseTimeout       = 3 * time.Second
	defaultRateWait      = 300 * time.Second
	// futureSkew is how far ahead of this clock a reading may be stamped.
	futureSkew = time.Minute
)

var accountKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}:[0-9a-f]{16}$`)

// Reading is a current, attributed reading of one account.
type Reading struct {
	Key         string
	Source      string
	MeasuredAt  time.Time
	ConfirmedAt *time.Time
	Plan        string
	Buckets     []Bucket
	// Free is the free-tier state of a pool that has one (Flow.LocalFree set).
	Free *FreeBucket
}

// Measurer takes one fresh measurement. A failure must be a *quota.Error.
type Measurer func(ctx context.Context) (quota.Observation, error)

// Flow keeps accounts' shared readings current. One Flow serves a whole node:
// it remembers the last presence announcement and the provider's rate-limit
// backoff per account, and is safe for concurrent use.
type Flow struct {
	// Now is the clock for freshness and stamps; nil is the wall clock.
	Now func() time.Time
	// FreshTTL is how long a reading is current; zero is DefaultFreshTTL.
	FreshTTL time.Duration
	// LeaseTTL is the lease length in seconds; zero is DefaultLeaseTTL.
	LeaseTTL int
	// Wait is the poll interval while another host measures; zero is 1 s.
	Wait time.Duration
	// HostID is announced as the holder of an account. Empty: no presence.
	HostID string
	// MaxWait is the hard bound on the whole time this host spends waiting for
	// another host's measurement — every poll pause and the last one — so a held
	// lease never holds the caller longer; zero is two lease windows.
	MaxWait time.Duration
	// Wall and Sleep are the wall clock and the pause the wait runs on; nil are
	// time.Now and a context-aware sleep. A fixture replaces both to prove the
	// MaxWait bound without waiting for it.
	Wall  func() time.Time
	Sleep func(context.Context, time.Duration) error
	// LocalFree makes the pool one with a free tier: it reports this host's
	// own free-tier state at a time, which is merged into every publication.
	// Such a reading needs a free bucket, may have no paid window, and a failed
	// paid measurement still publishes the free state (pacelever's rule: the
	// free tier does not depend on the paid probe).
	LocalFree func(now time.Time) *FreeBucket
	// PresenceEvery is the minimum gap between announcements of one account;
	// zero is DefaultPresenceEvery.
	PresenceEvery time.Duration

	mu       sync.Mutex
	presence map[string]time.Time
	backoff  map[string]time.Time
}

func (f *Flow) now() time.Time {
	if f.Now != nil {
		return f.Now().UTC()
	}
	return time.Now().UTC()
}

func (f *Flow) freshTTL() time.Duration {
	if f.FreshTTL > 0 {
		return f.FreshTTL
	}
	return DefaultFreshTTL
}

func (f *Flow) leaseTTL() int {
	if f.LeaseTTL > 0 {
		return f.LeaseTTL
	}
	return DefaultLeaseTTL
}

func (f *Flow) wait() time.Duration {
	if f.Wait > 0 {
		return f.Wait
	}
	return defaultWait
}

func (f *Flow) maxWait() time.Duration {
	if f.MaxWait > 0 {
		return f.MaxWait
	}
	return 2 * time.Duration(f.leaseTTL()) * time.Second
}

func (f *Flow) wall() time.Time {
	if f.Wall != nil {
		return f.Wall()
	}
	return time.Now()
}

func (f *Flow) pause(ctx context.Context, d time.Duration) error {
	if f.Sleep != nil {
		return f.Sleep(ctx, d)
	}
	return sleep(ctx, d)
}

func (f *Flow) presenceEvery() time.Duration {
	if f.PresenceEvery > 0 {
		return f.PresenceEvery
	}
	return DefaultPresenceEvery
}

// ValidAt is when a reading was last known true: the later of its measurement
// and the store's confirmation.
func ValidAt(o *Observation) time.Time {
	if o.ConfirmedAt != nil && o.ConfirmedAt.After(o.MeasuredAt) {
		return *o.ConfirmedAt
	}
	return o.MeasuredAt
}

// current is o as a Reading when it is an automatic reading no older than ttl
// whose every window is still running, else false. A pool with a free tier
// also needs a well-formed free bucket and may have no paid window, and a
// reading this host must still correct is not current.
func (f *Flow) current(o *Observation, key string, now time.Time) (Reading, bool) {
	ttl := f.freshTTL()
	if f.LocalFree == nil {
		return current(o, key, now, ttl)
	}
	free, ok := NormalizeFree(o.freeBucket(), now)
	if !ok || free == nil || freeNeedsPublish(free, f.LocalFree(now), now) {
		return Reading{}, false
	}
	return currentWith(o, key, now, ttl, free, true)
}

func (o *Observation) freeBucket() *FreeBucket {
	if o == nil {
		return nil
	}
	return o.FreeBucket
}

func current(o *Observation, key string, now time.Time, ttl time.Duration) (Reading, bool) {
	return currentWith(o, key, now, ttl, nil, false)
}

func currentWith(o *Observation, key string, now time.Time, ttl time.Duration, free *FreeBucket, hasFree bool) (Reading, bool) {
	if o == nil || o.Kind != KindAuto || o.MeasuredAt.IsZero() || len(o.Buckets) == 0 && !hasFree {
		return Reading{}, false
	}
	age := now.Sub(ValidAt(o))
	if age < -futureSkew || age > ttl {
		return Reading{}, false
	}
	for _, b := range o.Buckets {
		used := b.UsedPct
		if !quota.ValidPct(&used) || b.Scope != quota.ScopeAccount && b.Scope != quota.ScopeModel && b.Scope != quota.ScopeGroup {
			return Reading{}, false
		}
		if b.ResetsAt != nil && !b.ResetsAt.After(now) {
			return Reading{}, false
		}
	}
	return Reading{Key: key, Source: quota.SourceHandoffkeep, MeasuredAt: o.MeasuredAt, ConfirmedAt: o.ConfirmedAt, Plan: o.Plan, Buckets: o.Buckets, Free: free}, true
}

// freeOnlyObservation is the observation to publish when the stored reading is
// still current but this host's free-tier record is not: the stored reading
// with the merged free bucket, published without a provider measurement. It
// answers false when the free bucket needs no publication or the stored paid
// part could not be republished as it stands (too old, or a window past its
// reset), which needs a measurement.
func (f *Flow) freeOnlyObservation(o *Observation, now time.Time) (Observation, bool) {
	if f.LocalFree == nil || o == nil || o.Kind != KindAuto || o.MeasuredAt.IsZero() {
		return Observation{}, false
	}
	prior, ok := NormalizeFree(o.freeBucket(), now)
	if !ok {
		return Observation{}, false
	}
	out := *o
	out.FreeBucket = mergeFree(prior, f.LocalFree(now), now)
	merged, ok := NormalizeFree(out.FreeBucket, now)
	if !ok || merged == nil {
		return Observation{}, false
	}
	out.FreeBucket = merged
	if _, ok := currentWith(&out, "", now, f.freshTTL(), merged, true); !ok {
		return Observation{}, false
	}
	return out, true
}

// Refresh returns key's current reading, measuring only when the store has
// none: a fresh stored reading is returned without measuring; otherwise this
// host takes the lease, measures, publishes and releases. Another host's live
// lease is waited on (bounded by two lease windows), and an expired one is
// taken over. The failure is a *quota.Error.
func (f *Flow) Refresh(ctx context.Context, c Client, key string, measure Measurer) (Reading, error) {
	pool, _, _ := strings.Cut(key, ":")
	if !accountKeyPattern.MatchString(key) {
		return Reading{}, quota.Fail(quota.ErrKindAccountUnknown, "no account key")
	}
	latest, err := c.Latest(ctx, key)
	if err != nil {
		return Reading{}, quota.Fail(quota.ErrKindSharedUnavailable, "shared quota unavailable")
	}
	// Presence says this host holds the account (the node named it from its
	// login and reached the store), not that a reading succeeded: it goes out
	// before the lease and the measurement, so a measurement that fails, a
	// lease held elsewhere or a deadline in the wait cannot leave a host with a
	// working login unknown to placement.
	f.Announce(ctx, c, key)
	fresh := func() (Reading, bool) { return f.current(latest.Observation, key, f.now()) }
	if reading, ok := fresh(); ok {
		return reading, nil
	}
	backingOff := f.backingOff(key)
	// A host that loses the lease waits for the in-flight publisher until that
	// holder's lease expires, then takes over the expired slot once, so a
	// failed or killed publication never strands the account on "pending".
	holder := ""
	publisherGone, waitedOut := false, false
	// Wall-clock pacing: the whole wait for another host's measurement, over
	// both lease attempts, ends at waitDeadline. Every pause is cut to what
	// is left of it, and once it has passed nothing more is slept, so the
	// caller waits at most MaxWait (plus the store calls) however the server
	// keeps reporting contention.
	waitDeadline := f.wall().Add(f.maxWait())
	// A free-tier pool measures through a paid-probe backoff: the free state
	// is published without the probe.
	for attempts := 0; attempts < 2 && holder == "" && (!backingOff || f.LocalFree != nil); attempts++ {
		holder, err = newHolder()
		if err != nil {
			return Reading{}, quota.Fail(quota.ErrKindSharedUnavailable, "shared quota unavailable")
		}
		lease, leaseErr := c.Lease(ctx, key, holder, f.leaseTTL())
		if leaseErr != nil {
			return Reading{}, quota.Fail(quota.ErrKindSharedUnavailable, "shared quota unavailable")
		}
		publisherGone = false
		for !lease.Acquired || lease.Holder != holder {
			if !lease.ExpiresAt.After(f.wall()) {
				publisherGone = true
				break
			}
			remaining := waitDeadline.Sub(f.wall())
			if remaining <= 0 {
				waitedOut = true
				break
			}
			// Each pause ends at the holder's lease expiry, at the next poll,
			// or at the bound, whichever is first.
			if f.pause(ctx, min(f.wait(), lease.ExpiresAt.Sub(f.wall()), remaining)) != nil {
				return Reading{}, quota.Fail(quota.ErrKindSharedWait, "shared quota wait cancelled")
			}
			if latest, err = c.Latest(ctx, key); err != nil {
				return Reading{}, quota.Fail(quota.ErrKindSharedUnavailable, "shared quota unavailable")
			}
			if reading, ok := fresh(); ok {
				return reading, nil
			}
			// A holder that finishes or fails early releases its lease, so
			// retrying each poll ends the wait on release, not on expiry.
			if retry, retryErr := c.Lease(ctx, key, holder, f.leaseTTL()); retryErr == nil {
				lease = retry
			}
		}
		if !lease.Acquired || lease.Holder != holder {
			holder = ""
		}
		if waitedOut {
			break
		}
	}
	if holder == "" {
		// One last poll interval, never past the bound: after waiting out the
		// bound the host reads once more and gives up without sleeping.
		if last := min(f.wait(), max(waitDeadline.Sub(f.wall()), 0)); last > 0 && f.pause(ctx, last) != nil {
			return Reading{}, quota.Fail(quota.ErrKindSharedWait, "shared quota wait cancelled")
		}
		if latest, err = c.Latest(ctx, key); err != nil {
			return Reading{}, quota.Fail(quota.ErrKindSharedUnavailable, "shared quota unavailable")
		}
		if reading, ok := fresh(); ok {
			return reading, nil
		}
		switch {
		case backingOff:
			return Reading{}, quota.Fail(quota.ErrKindRateLimited, "provider backoff active")
		case publisherGone:
			return Reading{}, quota.Fail(quota.ErrKindSharedFailed, "shared measurement failed")
		}
		return Reading{}, quota.Fail(quota.ErrKindSharedPending, "shared measurement pending")
	}
	released := false
	defer func() {
		if !released {
			_ = f.release(c, key, holder)
		}
	}()
	// Re-read after acquiring: another host may have published between the
	// first read and the lease.
	if latest, err = c.Latest(ctx, key); err != nil {
		return Reading{}, quota.Fail(quota.ErrKindSharedUnavailable, "shared quota unavailable")
	}
	if reading, ok := fresh(); ok {
		return reading, nil
	}
	// A local free-tier record changes what the account publishes without
	// changing its paid reading. Publish the merged bucket over the stored
	// reading rather than spending a provider measurement to learn nothing new:
	// publishing a limit recorded on a pane must not wait on (or be lost to) a
	// measurement.
	if stored, ok := f.freeOnlyObservation(latest.Observation, f.now()); ok {
		if c.Publish(ctx, key, holder, stored) != nil {
			return Reading{}, quota.Fail(quota.ErrKindSharedPublishFailed, "shared publish failed")
		}
		if f.release(c, key, holder) != nil {
			return Reading{}, quota.Fail(quota.ErrKindSharedReleaseFailed, "shared release failed")
		}
		released = true
		reading, ok := f.current(&stored, key, f.now())
		if !ok {
			return Reading{}, quota.Fail(quota.ErrKindProbeFailed, "measurement failed")
		}
		return reading, nil
	}
	probeCtx, cancel := context.WithTimeout(ctx, time.Duration(f.leaseTTL())*time.Second-probeMargin)
	defer cancel()
	var observed quota.Observation
	if !backingOff || f.LocalFree == nil {
		var measureErr error
		if observed, measureErr = measure(probeCtx); measureErr != nil {
			failure := asFailure(measureErr)
			if failure.Kind == quota.ErrKindRateLimited {
				wait := failure.RetryAfter
				if wait <= 0 {
					wait = defaultRateWait
				}
				f.setBackoff(key, f.now().Add(wait))
			}
			if f.LocalFree == nil {
				return Reading{}, failure
			}
			observed = quota.Observation{}
		}
	}
	// The account measured must be the account the lease is for. A free-tier
	// pool whose paid measurement produced nothing names no account.
	if f.LocalFree == nil || len(observed.Buckets) > 0 {
		if observed.AccountFP == "" || pool+":"+observed.AccountFP != key {
			return Reading{}, quota.Fail(quota.ErrKindAccountMismatch, "measured account differs from the shared account")
		}
	}
	at := f.now()
	stored, convertErr := convert(observed, at, f.LocalFree != nil)
	if convertErr != nil {
		return Reading{}, convertErr
	}
	if f.LocalFree != nil {
		stored.FreeBucket = mergeFree(latest.Observation.freeBucket(), f.LocalFree(at), at)
	}
	reading, ok := f.current(&stored, key, at)
	if !ok {
		return Reading{}, quota.Fail(quota.ErrKindProbeFailed, "measurement failed")
	}
	if c.Publish(ctx, key, holder, stored) != nil {
		return Reading{}, quota.Fail(quota.ErrKindSharedPublishFailed, "shared publish failed")
	}
	if f.release(c, key, holder) != nil {
		return Reading{}, quota.Fail(quota.ErrKindSharedReleaseFailed, "shared release failed")
	}
	released = true
	return reading, nil
}

// asFailure is err as a *quota.Error; any other failure is probe_failed.
func asFailure(err error) *quota.Error {
	if e, ok := err.(*quota.Error); ok {
		return e
	}
	return quota.Fail(quota.ErrKindProbeFailed, "measurement failed")
}

func (f *Flow) release(c Client, key, holder string) error {
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()
	return c.Release(ctx, key, holder)
}

// Announce tells the store this host holds key, at most once per presence
// interval. Refresh announces through it before the lease and the measurement;
// a pool with no reading of its own to publish (a credit pool, which no host
// measures) announces through it directly. A lost announcement is retried on
// the next call rather than suppressed for the whole interval; it never
// changes a reading's outcome.
func (f *Flow) Announce(ctx context.Context, c Client, key string) {
	if f.HostID == "" || !accountKeyPattern.MatchString(key) {
		return
	}
	now := f.now()
	f.mu.Lock()
	last, announced := f.presence[key]
	if announced && !last.After(now) && now.Sub(last) < f.presenceEvery() {
		f.mu.Unlock()
		return
	}
	if f.presence == nil {
		f.presence = map[string]time.Time{}
	}
	f.presence[key] = now
	f.mu.Unlock()
	pctx, cancel := context.WithTimeout(ctx, presenceTimeout)
	defer cancel()
	if c.PublishPresence(pctx, Presence{HostID: f.HostID, AccountKey: key, LastSeen: now}) != nil {
		f.mu.Lock()
		if f.presence[key].Equal(now) {
			delete(f.presence, key)
		}
		f.mu.Unlock()
	}
}

func (f *Flow) backingOff(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	until, ok := f.backoff[key]
	return ok && f.now().Before(until)
}

func (f *Flow) setBackoff(key string, until time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.backoff == nil {
		f.backoff = map[string]time.Time{}
	}
	f.backoff[key] = until
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func newHolder() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// Convert is a measurement as the observation the store keeps, stamped at.
// A bucket reported without a value is unmeasured, not invalid, and is left
// out; a value outside 0..100, an unparsable reset or no measured bucket at
// all fails as probe_failed.
func Convert(observed quota.Observation, at time.Time) (Observation, error) {
	return convert(observed, at, false)
}

// convert is Convert; allowEmpty accepts a measurement with no paid window, for
// a pool whose reading carries a free-tier state.
func convert(observed quota.Observation, at time.Time, allowEmpty bool) (Observation, error) {
	stored := Observation{Kind: KindAuto, MeasuredAt: at, Plan: observed.Plan, Buckets: []Bucket{}}
	for _, b := range observed.Buckets {
		if b.UsedPct == nil {
			continue
		}
		if !quota.ValidPct(b.UsedPct) {
			return Observation{}, quota.Fail(quota.ErrKindProbeFailed, "measurement failed")
		}
		published := Bucket{Window: b.Window, UsedPct: *b.UsedPct, Scope: b.Scope.Kind, Model: b.Scope.Name}
		if b.ResetsAt != "" {
			reset, err := time.Parse(time.RFC3339Nano, b.ResetsAt)
			if err != nil {
				return Observation{}, quota.Fail(quota.ErrKindProbeFailed, "measurement failed")
			}
			published.ResetsAt = &reset
		}
		stored.Buckets = append(stored.Buckets, published)
	}
	if len(stored.Buckets) == 0 && !allowEmpty {
		return Observation{}, quota.Fail(quota.ErrKindProbeFailed, "measurement failed")
	}
	return stored, nil
}

package shared

import "time"

// A pool with a free tier (devin) publishes the tier's availability next to its
// paid windows. These are pacelever's rules for that state (shared_devin.go),
// so a reading either writer publishes is read the same by both.

// normalizeFree is b as a reader sees it at now, and whether it is well formed.
// An exhausted state needs a reset; one whose reset has passed reads as
// available; available and unknown carry no reset. Nil is valid and absent.
func normalizeFree(b *FreeBucket, now time.Time) (*FreeBucket, bool) {
	if b == nil {
		return nil, true
	}
	switch b.State {
	case FreeAvailable, FreeUnknown:
		return &FreeBucket{State: b.State}, b.ResetsAt == nil
	case FreeExhausted:
		if b.ResetsAt == nil || b.ResetsAt.IsZero() {
			return nil, false
		}
		if !b.ResetsAt.After(now) {
			return &FreeBucket{State: FreeAvailable}, true
		}
		return &FreeBucket{State: b.State, ResetsAt: b.ResetsAt}, true
	}
	return nil, false
}

// freeNeedsPublish reports whether this host's own free state must reach the
// store although the stored reading is fresh: a locally recorded exceed must
// not wait for the reading to age, and a host without that record must not
// clear another host's active hold.
func freeNeedsPublish(stored, local *FreeBucket, now time.Time) bool {
	prior, ok := normalizeFree(stored, now)
	if !ok || prior == nil {
		return true
	}
	if local.State == FreeUnknown {
		return prior.State != FreeUnknown && prior.State != FreeExhausted
	}
	return local.State == FreeExhausted && (prior.State != FreeExhausted || local.ResetsAt.After(*prior.ResetsAt))
}

// mergeFree is the state to publish: another host's active, later hold wins
// over this host's weaker record.
func mergeFree(stored, local *FreeBucket, now time.Time) *FreeBucket {
	prior, ok := normalizeFree(stored, now)
	if ok && prior != nil && prior.State == FreeExhausted {
		if local.State != FreeExhausted || prior.ResetsAt.After(*local.ResetsAt) {
			return prior
		}
	}
	return local
}

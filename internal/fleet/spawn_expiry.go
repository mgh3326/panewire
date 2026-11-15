package fleet

import (
	"fmt"
	"time"
)

// A request's time limit (hk design/2026-11-13/hub-owned-spawn-queue §3 rule
// 2, hk 4660). The per-attempt landing wait (Submission.DeadlineSeconds) ends
// an attempt; the request ends only when its expiry passes.
const (
	// SpawnExpiresMax is the longest limit a request may ask for.
	SpawnExpiresMax = 24 * time.Hour
	// SpawnExpiresMin keeps --expires from being shorter than one attempt's
	// first useful placement pass.
	SpawnExpiresMin = time.Minute
	// SpawnExpiresImplementer is the default for an implementer.
	SpawnExpiresImplementer = 6 * time.Hour
	// SpawnExpiresReviewer is the default for a reviewer or a consult: a
	// review older than an hour is for a stale head anyway.
	SpawnExpiresReviewer = time.Hour
)

// SpawnExpiresDefault is the time limit a request of role gets when it names
// none.
func SpawnExpiresDefault(role string) time.Duration {
	if role == "reviewer" || role == "consult" {
		return SpawnExpiresReviewer
	}
	return SpawnExpiresImplementer
}

// SpawnExpiresFor is the time limit of a request of role that asked for
// seconds (zero: the role's default).
func SpawnExpiresFor(role string, seconds int) time.Duration {
	if seconds == 0 {
		return SpawnExpiresDefault(role)
	}
	return time.Duration(seconds) * time.Second
}

// ValidSpawnExpiresSeconds reports whether seconds is a limit a request may
// carry: zero (the role's default) or between SpawnExpiresMin and
// SpawnExpiresMax.
func ValidSpawnExpiresSeconds(seconds int) bool {
	if seconds == 0 {
		return true
	}
	// Compared in seconds: converting a wire value to a Duration first
	// would wrap an enormous one into range.
	return seconds >= int(SpawnExpiresMin/time.Second) && seconds <= int(SpawnExpiresMax/time.Second)
}

// parseSpawnExpires reads --expires: a Go duration such as 90m or 2h, whole
// seconds, between SpawnExpiresMin and SpawnExpiresMax.
func parseSpawnExpires(value string) (int, error) {
	d, err := time.ParseDuration(value)
	if err != nil || d%time.Second != 0 {
		return 0, fmt.Errorf("invalid --expires %q: expected a whole-second duration such as 90m or 6h", value)
	}
	seconds := int(d / time.Second)
	if seconds == 0 || !ValidSpawnExpiresSeconds(seconds) {
		return 0, fmt.Errorf("invalid --expires %q: expected between %s and %s", value, SpawnExpiresMin, SpawnExpiresMax)
	}
	return seconds, nil
}

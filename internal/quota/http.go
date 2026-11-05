package quota

import (
	"math"
	"net/http"
	"strings"
	"time"
)

// MaxRetryAfter caps a server-declared wait.
const MaxRetryAfter = 86400 * time.Second

// StatusError is the one mapping from a non-200 usage response to a safe
// failure: 401/403 auth, 429 rate_limited, anything else http. A 429 carries
// the Retry-After wait, or rateWait when the header gives none; a zero
// rateWait leaves the backoff to the caller.
func StatusError(status int, header http.Header, now time.Time, rateWait time.Duration) *Error {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return Fail(ErrKindAuth, "usage HTTP "+http.StatusText(status))
	case http.StatusTooManyRequests:
		e := Fail(ErrKindRateLimited, "usage HTTP 429")
		e.RetryAfter = RetryAfter(header.Get("Retry-After"), now)
		if e.RetryAfter <= 0 {
			e.RetryAfter = rateWait
		}
		return e
	}
	return Fail(ErrKindHTTP, "usage HTTP "+http.StatusText(status))
}

// RetryAfter reads a Retry-After header as delay-seconds or an HTTP-date,
// capped at a day. Missing, zero, negative and unparseable values are 0.
func RetryAfter(header string, now time.Time) time.Duration {
	value := strings.TrimSpace(header)
	if value == "" {
		return 0
	}
	if n, ok := ParseFinite(value); ok {
		if n <= 0 {
			return 0
		}
		return time.Duration(math.Min(n, MaxRetryAfter.Seconds()) * float64(time.Second))
	}
	if at, err := http.ParseTime(value); err == nil {
		if d := at.Sub(now); d > 0 {
			return min(d, MaxRetryAfter)
		}
	}
	return 0
}

// Package hubretry repeats one idempotent hub call through a transport error,
// a 502 or a 503 for a bounded budget. The caller decides that its call is
// idempotent; this package decides only how often and how long to wait.
// Operator contract: docs/hub-uninterrupted-calls.md.
package hubretry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
)

// Budget is how long an idempotent call keeps retrying when its Policy names
// none.
const Budget = 60 * time.Second

const (
	defaultInitial = 250 * time.Millisecond
	defaultMax     = 5 * time.Second
)

// Policy bounds one call's retries. The zero value retries for Budget, with
// backoff from 250ms doubling to 5s. A negative Budget disables retries, so a
// fixture that counts exact attempts can keep its count.
type Policy struct {
	Budget  time.Duration
	Initial time.Duration
	Max     time.Duration
}

// Stats is what retries cost: how many times a call was sent again and the
// total time it waited between sends. Add folds another call's stats in.
type Stats struct {
	Retries int
	Waited  time.Duration
}

// Add folds other into s.
func (s *Stats) Add(other Stats) {
	s.Retries += other.Retries
	s.Waited += other.Waited
}

// Line is the one line a CLI prints after retries, e.g.
// "hub: retried 3 times, waited 4.5s". It names the count and the total wait
// so a slow call can be explained.
func (s Stats) Line() string {
	noun := "times"
	if s.Retries == 1 {
		noun = "time"
	}
	return fmt.Sprintf("hub: retried %d %s, waited %s", s.Retries, noun, s.Waited.Round(time.Millisecond))
}

// Retryable reports whether an answer may be sent again: a transport failure
// (err), or else a 502 or 503 status. A rejected request, a redirect the
// caller refused and the caller's own cancellation are not retried.
func Retryable(status int, err error) bool {
	if err != nil {
		return transportFailure(err)
	}
	return status == http.StatusBadGateway || status == http.StatusServiceUnavailable
}

// transportFailure reports whether err is a failure of the connection rather
// than of the request. http.Client wraps every failure in a *url.Error, so the
// wrapped cause decides: a network error (refused, reset, timed out) or a
// connection that ended early is retried; a plain error is not.
func transportFailure(err error) bool {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		err = urlErr.Err
	}
	var netErr net.Error
	return errors.As(err, &netErr) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

// Do runs attempt until its answer is not Retryable, the budget is spent, or
// ctx ends. attempt returns the answer, its HTTP status (0 when it had none)
// and its error; it should bound each attempt itself. discard, if not nil,
// releases an answer that is about to be replaced, such as a response body.
//
// The last answer is returned as attempt gave it, so a caller that gives up
// reports the final 503 or transport error exactly as one unretried call
// would. A budget is wall-clock time, as a deadline is.
func Do[T any](ctx context.Context, policy Policy, attempt func(context.Context) (T, int, error), discard func(T)) (T, Stats, error) {
	var stats Stats
	budget := policy.budget()
	if budget < 0 {
		answer, _, err := attempt(ctx)
		return answer, stats, err
	}
	started := time.Now()
	backoff := policy.initial()
	for {
		answer, status, err := attempt(ctx)
		if !Retryable(status, err) || ctx.Err() != nil {
			return answer, stats, err
		}
		remaining := budget - time.Since(started)
		if remaining <= 0 {
			return answer, stats, err
		}
		wait := min(backoff, remaining)
		if discard != nil {
			discard(answer)
		}
		var zero T
		if waitErr := sleepContext(ctx, wait); waitErr != nil {
			return zero, stats, waitErr
		}
		stats.Retries++
		stats.Waited += wait
		backoff = min(backoff*2, policy.max())
	}
}

// Within returns p with its budget capped at d, so a call whose caller has a
// deadline does not retry past it. A disabled policy stays disabled.
func (p Policy) Within(d time.Duration) Policy {
	if d <= 0 {
		p.Budget = -1
		return p
	}
	if p.budget() >= 0 {
		p.Budget = min(p.budget(), d)
	}
	return p
}

func (p Policy) budget() time.Duration {
	if p.Budget == 0 {
		return Budget
	}
	return p.Budget
}

func (p Policy) initial() time.Duration {
	if p.Initial <= 0 {
		return defaultInitial
	}
	return p.Initial
}

func (p Policy) max() time.Duration {
	if p.Max <= 0 {
		return defaultMax
	}
	return p.Max
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

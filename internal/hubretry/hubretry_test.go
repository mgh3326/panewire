package hubretry

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"testing"
	"time"
)

// fast keeps backoff at a millisecond so the budget, not the sleep, decides
// how many attempts run.
func fast(budget time.Duration) Policy {
	return Policy{Budget: budget, Initial: time.Millisecond, Max: time.Millisecond}
}

func TestRetryableClassifiesAnswers(t *testing.T) {
	refused := &url.Error{Op: "Get", URL: "https://hub", Err: &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}}
	reset := &url.Error{Op: "Get", URL: "https://hub", Err: &net.OpError{Op: "read", Err: syscall.ECONNRESET}}
	early := &url.Error{Op: "Get", URL: "https://hub", Err: io.EOF}
	timedOut := &url.Error{Op: "Get", URL: "https://hub", Err: context.DeadlineExceeded}
	redirect := &url.Error{Op: "Get", URL: "https://hub", Err: errors.New("hub request redirect rejected")}
	for _, tc := range []struct {
		name   string
		status int
		err    error
		want   bool
	}{
		{"bad gateway", http.StatusBadGateway, nil, true},
		{"unavailable", http.StatusServiceUnavailable, nil, true},
		{"refused", 0, refused, true},
		{"reset", 0, reset, true},
		{"ended early", 0, early, true},
		{"attempt timeout", 0, timedOut, true},
		{"wrapped transport", 0, errors.Join(errors.New("hub"), reset), true},
		{"not found", http.StatusNotFound, nil, false},
		{"conflict", http.StatusConflict, nil, false},
		{"ok", http.StatusOK, nil, false},
		{"redirect refused", 0, redirect, false},
		{"own cancellation", 0, context.Canceled, false},
		{"plain error", 0, errors.New("invalid hub API path"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Retryable(tc.status, tc.err); got != tc.want {
				t.Fatalf("Retryable(%d, %v)=%v want %v", tc.status, tc.err, got, tc.want)
			}
		})
	}
}

func TestDoRetriesUntilTheAnswerRecovers(t *testing.T) {
	var sent []int
	attempt := func(context.Context) (string, int, error) {
		sent = append(sent, len(sent))
		if len(sent) <= 3 {
			return "down", http.StatusServiceUnavailable, nil
		}
		return "up", http.StatusOK, nil
	}
	answer, stats, err := Do(context.Background(), fast(time.Second), attempt, nil)
	if err != nil || answer != "up" {
		t.Fatalf("answer=%q err=%v", answer, err)
	}
	if stats.Retries != 3 || len(sent) != 4 {
		t.Fatalf("stats=%+v sent=%d", stats, len(sent))
	}
}

func TestDoGivesUpWithTheLastAnswer(t *testing.T) {
	var calls int
	var discarded []string
	attempt := func(context.Context) (string, int, error) {
		calls++
		return "answer-" + string(rune('a'+calls-1)), http.StatusBadGateway, nil
	}
	discard := func(answer string) { discarded = append(discarded, answer) }
	answer, stats, err := Do(context.Background(), fast(20*time.Millisecond), attempt, discard)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if calls < 2 || stats.Retries != calls-1 {
		t.Fatalf("calls=%d stats=%+v", calls, stats)
	}
	// The final answer is handed back, not discarded; every replaced one is.
	if answer != "answer-"+string(rune('a'+calls-1)) || len(discarded) != calls-1 {
		t.Fatalf("answer=%q discarded=%v", answer, discarded)
	}
	if stats.Waited <= 0 {
		t.Fatalf("waited=%v", stats.Waited)
	}
}

func TestDoBudgetBoundsTheWaitNotTheAttempt(t *testing.T) {
	started := time.Now()
	policy := Policy{Budget: 30 * time.Millisecond, Initial: 10 * time.Millisecond, Max: 10 * time.Millisecond}
	_, stats, _ := Do(context.Background(), policy, func(context.Context) (int, int, error) {
		return 0, http.StatusServiceUnavailable, nil
	}, nil)
	if stats.Waited > policy.Budget {
		t.Fatalf("waited %v beyond budget %v", stats.Waited, policy.Budget)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("budget ran for %v", elapsed)
	}
}

func TestDoNeverRetriesANonRetryableAnswer(t *testing.T) {
	var calls int
	answer, stats, err := Do(context.Background(), fast(time.Second), func(context.Context) (string, int, error) {
		calls++
		return "missing", http.StatusNotFound, nil
	}, nil)
	if answer != "missing" || err != nil || calls != 1 || stats.Retries != 0 {
		t.Fatalf("answer=%q err=%v calls=%d stats=%+v", answer, err, calls, stats)
	}
}

func TestDoNegativeBudgetSendsOnce(t *testing.T) {
	var calls int
	_, stats, _ := Do(context.Background(), Policy{Budget: -1}, func(context.Context) (int, int, error) {
		calls++
		return 0, http.StatusServiceUnavailable, nil
	}, nil)
	if calls != 1 || stats.Retries != 0 {
		t.Fatalf("calls=%d stats=%+v", calls, stats)
	}
}

func TestDoStopsWhenItsContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls int
	policy := Policy{Budget: time.Minute, Initial: time.Hour, Max: time.Hour}
	go func() {
		time.Sleep(10 * time.Millisecond) // simulated latency: lets the first backoff start before the cancel.
		cancel()
	}()
	_, _, err := Do(ctx, policy, func(context.Context) (int, int, error) {
		calls++
		return 0, http.StatusServiceUnavailable, nil
	}, nil)
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestDoStopsAtAContextThatEndedBeforeTheCall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var calls int
	_, _, err := Do(ctx, fast(time.Second), func(ctx context.Context) (int, int, error) {
		calls++
		return 0, 0, ctx.Err()
	}, nil)
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestStatsLineNamesCountAndWait(t *testing.T) {
	if got := (Stats{Retries: 3, Waited: 4500 * time.Millisecond}).Line(); got != "hub: retried 3 times, waited 4.5s" {
		t.Fatalf("line=%q", got)
	}
	if got := (Stats{Retries: 1, Waited: 250 * time.Millisecond}).Line(); !strings.HasPrefix(got, "hub: retried 1 time, waited ") {
		t.Fatalf("singular line=%q", got)
	}
	var total Stats
	total.Add(Stats{Retries: 2, Waited: time.Second})
	total.Add(Stats{Retries: 1, Waited: time.Second})
	if total.Retries != 3 || total.Waited != 2*time.Second {
		t.Fatalf("total=%+v", total)
	}
}

func TestWithinCapsTheBudgetAndKeepsDisabledPolicies(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy Policy
		limit  time.Duration
		want   time.Duration
	}{
		{"zero policy takes the cap", Policy{}, time.Second, time.Second},
		{"larger budget is capped", Policy{Budget: time.Minute}, time.Second, time.Second},
		{"smaller budget is kept", Policy{Budget: time.Second}, time.Minute, time.Second},
		{"disabled stays disabled", Policy{Budget: -1}, time.Second, -1},
		{"no time left disables", Policy{Budget: time.Minute}, 0, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.policy.Within(tc.limit).budget(); got != tc.want {
				t.Fatalf("budget=%v want %v", got, tc.want)
			}
		})
	}
}

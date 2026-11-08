package fleet

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleetevent"
)

// The shadow comparisons never sit on the spawn path (docs/quota.md §Shadow
// and native launch): Launch and Pools return the authoritative answer at
// once, the comparison and its event append run in one background goroutine,
// and at most one comparison runs per process — a hung filesystem holds one
// goroutine, not one per spawn.

// shadowReturnBound is how long a shadow-mode Launch or Pools may take with
// its comparison held. The fake pacelever answers at once, so the call takes
// milliseconds; the bound is far under shadowBound so a call that waits for
// the comparison fails.
const shadowReturnBound = time.Second

// feedEvents makes a's events arrive on the returned channel, from whichever
// goroutine appends them.
func feedEvents(a *App) <-chan fleetevent.Event {
	ch := make(chan fleetevent.Event, 64)
	a.EventSink = func(e fleetevent.Event) { ch <- e }
	return ch
}

// nextEvent is the next event fed to ch, failing the test after 10 s.
func nextEvent(t *testing.T, ch <-chan fleetevent.Event) fleetevent.Event {
	t.Helper()
	select {
	case e := <-ch:
		return e
	case <-time.After(10 * time.Second):
		t.Fatal("no fleet event arrived")
		return fleetevent.Event{}
	}
}

// holdShadowReads makes every shadow comparison wait before its local reads
// until release is called, as reads on a hung filesystem wait. entered
// receives once per comparison started; started counts them.
func holdShadowReads(t *testing.T) (release func(), entered <-chan struct{}, started *atomic.Int64) {
	t.Helper()
	gate := make(chan struct{})
	in := make(chan struct{}, 64)
	var n atomic.Int64
	shadowReadHook = func() {
		n.Add(1)
		in <- struct{}{}
		<-gate
	}
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	t.Cleanup(func() {
		release()
		awaitShadowIdle(t)
		shadowReadHook = nil
	})
	return release, in, &n
}

// awaitEntered waits for one held comparison to reach its reads.
func awaitEntered(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("no shadow comparison started")
	}
}

// returnsWithin runs call in its own goroutine and reports whether it
// returned within bound, so a call held by the comparison fails the test
// instead of hanging it.
func returnsWithin(bound time.Duration, call func()) (time.Duration, bool) {
	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		call()
	}()
	select {
	case <-done:
		return time.Since(start), true
	case <-time.After(bound):
		return bound, false
	}
}

// TestLaunchShadowDoesNotBlockOnHeldReads: with the comparison's local reads
// held (a hung filesystem), a shadow-mode Launch returns pacelever's answer
// within shadowReturnBound and the caller's deadline is untouched — still
// live, unchanged — for the next gate call.
func TestLaunchShadowDoesNotBlockOnHeldReads(t *testing.T) {
	a, _, _ := launchApp(t, "", launchRulesCatalog(), paceleverLaunchJSON(opusLaunch))
	events := feedEvents(a)
	release, entered, _ := holdShadowReads(t)

	ctx, cancel := context.WithTimeout(context.Background(), shadowBound-500*time.Millisecond)
	defer cancel()
	deadline, _ := ctx.Deadline()
	var got Launch
	var err error
	took, ok := returnsWithin(shadowReturnBound, func() {
		got, err = a.Quota.Launch(ctx, QuotaRequest{Profile: "opus", Effort: "high", Role: "worker"})
	})
	if !ok {
		t.Fatalf("Launch waited on the held comparison: no answer within %v", shadowReturnBound)
	}
	if err != nil || got != opusLaunch {
		t.Fatalf("shadow must return pacelever's launch: %+v %v", got, err)
	}
	if ctx.Err() != nil {
		t.Fatalf("the caller's context ended inside Launch (%v): %v", took, ctx.Err())
	}
	if d, _ := ctx.Deadline(); !d.Equal(deadline) {
		t.Fatalf("caller deadline changed: %v -> %v", deadline, d)
	}
	awaitEntered(t, entered)
	release()
	if e := nextEvent(t, events); e.Kind != "quota_launch_shadow" || e.Outcome != "agree" {
		t.Fatalf("the released comparison must still record its answer: %+v", e)
	}
}

// TestCreditPoolShadowDoesNotBlockOnHeldReads: the same for Pools — with the
// pool policy reads held, pacelever's views come back at once and the
// caller's deadline is untouched.
func TestCreditPoolShadowDoesNotBlockOnHeldReads(t *testing.T) {
	a, _ := creditPoolApp(t, excludedAnthropicPolicy, anthropicCreditLatest)
	events := feedEvents(a)
	release, entered, _ := holdShadowReads(t)

	ctx, cancel := context.WithTimeout(context.Background(), shadowBound-500*time.Millisecond)
	defer cancel()
	deadline, _ := ctx.Deadline()
	var views map[string]PoolView
	var err error
	if _, ok := returnsWithin(shadowReturnBound, func() { views, err = a.Quota.Pools(ctx) }); !ok {
		t.Fatalf("Pools waited on the held comparison: no answer within %v", shadowReturnBound)
	}
	if err != nil || views["anthropic-api"].Class != "spend" {
		t.Fatalf("shadow must return pacelever's views: %+v %v", views, err)
	}
	if ctx.Err() != nil {
		t.Fatalf("the caller's context ended inside Pools: %v", ctx.Err())
	}
	if d, _ := ctx.Deadline(); !d.Equal(deadline) {
		t.Fatalf("caller deadline changed: %v -> %v", deadline, d)
	}
	awaitEntered(t, entered)
	release()
	if e := nextEvent(t, events); e.Kind != "quota_pools_shadow" || e.Outcome != "disagree" {
		t.Fatalf("the released comparison must still record its answer: %+v", e)
	}
}

// TestShadowEventAppendOffThePath: the event append is part of the
// background comparison. With the event sink blocked (a full or hung state
// directory), Launch and Pools still return at once.
func TestShadowEventAppendOffThePath(t *testing.T) {
	block := func(t *testing.T, a *App) {
		gate := make(chan struct{})
		t.Cleanup(func() {
			close(gate)
			awaitShadowIdle(t)
		})
		a.EventSink = func(fleetevent.Event) { <-gate }
	}
	t.Run("launch", func(t *testing.T) {
		a, _, _ := launchApp(t, "", launchRulesCatalog(), paceleverLaunchJSON(opusLaunch))
		block(t, a)
		if _, ok := returnsWithin(shadowReturnBound, func() {
			if got, err := a.Quota.Launch(context.Background(), QuotaRequest{Profile: "opus", Effort: "high", Role: "worker"}); err != nil || got != opusLaunch {
				t.Errorf("shadow must return pacelever's launch: %+v %v", got, err)
			}
		}); !ok {
			t.Fatalf("Launch waited on the shadow event append")
		}
	})
	t.Run("pools", func(t *testing.T) {
		a, _ := creditPoolApp(t, excludedAnthropicPolicy, anthropicCreditLatest)
		block(t, a)
		if _, ok := returnsWithin(shadowReturnBound, func() {
			if _, err := a.Quota.Pools(context.Background()); err != nil {
				t.Errorf("shadow must return pacelever's views: %v", err)
			}
		}); !ok {
			t.Fatalf("Pools waited on the shadow event append")
		}
	})
}

// TestShadowSingleFlight: while one comparison is held, further Launch calls
// start no comparison of their own — each is skipped and counted — and the
// held one, once it completes, records how many it skipped
// (skipped_busy=N). After it, the next call compares again.
func TestShadowSingleFlight(t *testing.T) {
	a, _, _ := launchApp(t, "", launchRulesCatalog(), paceleverLaunchJSON(opusLaunch))
	events := feedEvents(a)
	release, entered, started := holdShadowReads(t)
	req := QuotaRequest{Profile: "opus", Effort: "high", Role: "worker"}

	launch := func() {
		t.Helper()
		if _, ok := returnsWithin(shadowReturnBound, func() {
			if got, err := a.Quota.Launch(context.Background(), req); err != nil || got != opusLaunch {
				t.Errorf("shadow must return pacelever's launch: %+v %v", got, err)
			}
		}); !ok {
			t.Fatalf("Launch waited on the held comparison")
		}
	}
	launch()
	awaitEntered(t, entered)
	const more = 3
	for range more {
		launch()
	}
	release()
	e := nextEvent(t, events)
	if e.Kind != "quota_launch_shadow" || !strings.Contains(e.Detail, "skipped_busy=3") {
		t.Fatalf("the completed comparison must record the %d skipped: %+v", more, e)
	}
	if n := started.Load(); n != 1 {
		t.Fatalf("%d comparisons started while one was held, want 1", n)
	}
	select {
	case e := <-events:
		t.Fatalf("a skipped comparison recorded an event: %+v", e)
	default:
	}

	// The slot is free again, and the skip count was spent.
	awaitShadowIdle(t)
	launch()
	awaitEntered(t, entered)
	if e := nextEvent(t, events); e.Kind != "quota_launch_shadow" || strings.Contains(e.Detail, "skipped_busy") {
		t.Fatalf("the next comparison must run and carry no stale skip count: %+v", e)
	}
	if n := started.Load(); n != 2 {
		t.Fatalf("comparisons started = %d, want 2", n)
	}
}

// TestLaunchShadowFollowsCatalogFreshness: the shadow judges the catalog
// cache by pacelever's [bench] catalog_ttl_s, as native does
// (TestLaunchNativeHonoursCatalogFreshness), so the two agree on which cache
// counts as fresh. A 90-minute cache inside a two-hour TTL is compared; a
// 45-minute cache past a 30-minute TTL is skipped. Neither asks handoffkeep
// or touches the cache.
func TestLaunchShadowFollowsCatalogFreshness(t *testing.T) {
	for _, tc := range []struct {
		name, toml string
		age        time.Duration
		outcome    string
		reason     string
	}{
		{"inside a longer ttl", "[bench]\ncatalog_ttl_s = 7200\n", 90 * time.Minute, "agree", "claude"},
		{"past a shorter ttl", "[bench]\ncatalog_ttl_s = 1800\n", 45 * time.Minute, "skipped", "no_fresh_cache"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hk := &countingHandoffkeep{body: newRungCatalog}
			client := shadowCatalogEnv(t, hk)
			a, _, _ := launchApp(t, "", nil, paceleverLaunchJSON(opusLaunch))
			events := feedEvents(a)
			path := filepath.Join(t.TempDir(), "config.toml")
			if err := os.WriteFile(path, []byte(tc.toml), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PACELEVER_CONFIG", path)
			writeCatalogCacheAt(t, client, launchRulesCatalog(), tc.age)
			before := cacheDirState(t, client)
			if got, err := a.Quota.Launch(context.Background(), QuotaRequest{Profile: "opus", Effort: "high", Role: "worker"}); err != nil || got != opusLaunch {
				t.Fatalf("shadow must return pacelever's launch: %+v %v", got, err)
			}
			if e := nextEvent(t, events); e.Kind != "quota_launch_shadow" || e.Outcome != tc.outcome || e.Reason != tc.reason {
				t.Fatalf("shadow event = %+v, want outcome=%s reason=%s", e, tc.outcome, tc.reason)
			}
			awaitShadowIdle(t)
			if n := hk.hits.Load(); n != 0 {
				t.Fatalf("shadow read handoffkeep %d times, want 0", n)
			}
			if after := cacheDirState(t, client); !equalStringMaps(before, after) {
				t.Fatalf("shadow changed the catalog cache")
			}
		})
	}
}

// awaitShadowIdle waits until no shadow comparison runs in this process, so
// its event is recorded and nothing writes into the test's directories after
// it, failing the test after 10 s.
func awaitShadowIdle(t *testing.T) {
	t.Helper()
	select {
	case <-shadows.idle():
	case <-time.After(10 * time.Second):
		t.Fatal("a shadow comparison is still running")
	}
}

package fleet

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// defaultFloor is the submit window of a host with no brief_submit_* settings.
const defaultFloor = defaultBriefSubmitRetryMS*time.Millisecond + briefSubmitWaitSlice

var windowCutRE = regexp.MustCompile(`window cut to (\d+)s`)

// hk 5174: the steps before the brief (fetch, checkout, launch) spent the node
// budget until deliver's window was cut to 0s. A budget that cannot hold the
// submit window now refuses before the brief is sent.
func TestDeliverRefusesBeforeSendingWhenBudgetCannotHoldSubmitWindow(t *testing.T) {
	for _, tc := range []struct {
		name   string
		budget time.Duration
	}{
		{"nothing left", 100 * time.Millisecond},
		{"only the reserve left", briefSubmitReserve},
		{"reserve plus a short window", briefSubmitReserve + defaultFloor - 5*time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, d := claudeDeliveryApp(t, func(*claudeDelivery) string { return claudeScreenEmpty() }, idleStatus)
			ctx, cancel := context.WithTimeout(t.Context(), tc.budget)
			defer cancel()
			err := a.deliver(ctx, "claude", "w:p1", claudeTestBrief)
			if code(err) != exitConfig || !strings.Contains(err.Error(), "SUBMIT_WINDOW_UNAVAILABLE") || !strings.Contains(err.Error(), "hub_wait") {
				t.Fatalf("err=%v", err)
			}
			if d.pastes != 0 || d.returns != 0 {
				t.Fatalf("brief sent into a pane whose submit cannot be confirmed: pastes=%d returns=%d", d.pastes, d.returns)
			}
		})
	}
}

// A budget that holds the minimum window keeps at least that window.
func TestDeliverKeepsMinimumSubmitWindowInsideBudget(t *testing.T) {
	a, d := claudeDeliveryApp(t, func(d *claudeDelivery) string {
		if d.pastes == 0 {
			return claudeScreenEmpty()
		}
		return claudeScreenBriefInComposer()
	}, idleStatus)
	ctx, cancel := context.WithTimeout(t.Context(), briefSubmitReserve+defaultFloor+3*time.Second)
	defer cancel()
	err := a.deliver(ctx, "claude", "w:p1", claudeTestBrief)
	m := windowCutRE.FindStringSubmatch(errText(err))
	if code(err) != exitSubmitUnconfirmed || m == nil {
		t.Fatalf("err=%v", err)
	}
	if secs, _ := strconv.Atoi(m[1]); time.Duration(secs)*time.Second < defaultFloor {
		t.Fatalf("window cut to %ds, below the %v minimum", secs, defaultFloor)
	}
	if d.pastes != 1 {
		t.Fatalf("pastes=%d", d.pastes)
	}
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// The launch ends where the window and the reserve begin; a budget too short
// for them refuses before a pane exists.
func TestReserveSubmitWindowBoundsLaunch(t *testing.T) {
	a := testApp(t)
	tight, cancelTight := context.WithTimeout(t.Context(), briefSubmitReserve+defaultFloor-time.Second)
	defer cancelTight()
	if _, _, err := a.reserveSubmitWindow(tight); code(err) != exitConfig || !strings.Contains(err.Error(), "nothing launched") {
		t.Fatalf("tight budget: %v", err)
	}

	budget := 4 * time.Minute
	ctx, cancel := context.WithTimeout(t.Context(), budget)
	defer cancel()
	launch, endLaunch, err := a.reserveSubmitWindow(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer endLaunch()
	until, _ := ctx.Deadline()
	launchBy, ok := launch.Deadline()
	if !ok || until.Sub(launchBy) != briefSubmitReserve+defaultFloor {
		t.Fatalf("launch ends %v before the node budget, want reserve+window=%v", until.Sub(launchBy), briefSubmitReserve+defaultFloor)
	}

	// A launch cut by its own deadline names the cause; a node-budget expiry or
	// a launch that did not time out keeps its error.
	expired, endExpired := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer endExpired()
	if got := a.launchBudgetSpent(expired, t.Context(), os.ErrDeadlineExceeded); code(got) != exitConfig || !strings.Contains(got.Error(), "SUBMIT_WINDOW_UNAVAILABLE") {
		t.Fatalf("launch overrun: %v", got)
	}
	if got := a.launchBudgetSpent(launch, ctx, os.ErrPermission); got != os.ErrPermission {
		t.Fatalf("unrelated launch error rewritten: %v", got)
	}
}

// Spawn level: a node budget spent by the earlier steps refuses the spawn
// before any pane exists, and the lease is released.
func TestSpawnRefusesBeforeLaunchWhenBudgetCannotHoldSubmitWindow(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), briefSubmitReserve+defaultFloor-time.Second)
	defer cancel()
	e := a.Spawn(ctx, s)
	if code(e) != exitConfig || !strings.Contains(errText(e), "SUBMIT_WINDOW_UNAVAILABLE") {
		t.Fatalf("err=%v", e)
	}
	for _, c := range f.Calls {
		if strings.HasPrefix(strings.Join(c.Args, " "), "tab create") {
			t.Fatal("a pane was launched on a budget that cannot hold the submit window")
		}
	}
	if st, pools := ledgerState(t, a, s.Job); st != "released" || len(pools) != 0 {
		t.Fatalf("lease not released: %s %v", st, pools)
	}
}

// Spawn level: the brief_sent receipt is written after the send, names the
// pane, and digests the brief file the pane was told to read.
func TestSpawnRecordsBriefSentBeforeConfirming(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	original := f.Do
	f.Do = func(c Command) Result {
		r := original(c)
		if strings.HasPrefix(strings.Join(c.Args, " "), "agent list") {
			r.Out = strings.ReplaceAll(r.Out, `"working"`, `"idle"`)
		}
		return r
	}
	if e := a.Spawn(context.Background(), s); code(e) != exitSubmitUnconfirmed {
		t.Fatalf("err=%v", e)
	}
	events := mustEvents(t, a, s.Job)
	var sent *Event
	for i := range events {
		if events[i].Kind == eventBriefSent {
			sent = &events[i]
		}
	}
	if sent == nil {
		t.Fatalf("no %s receipt after the brief was typed into the pane: %v", eventBriefSent, eventKindsOf(t, a, s.Job))
	}
	body, err := os.ReadFile(filepath.Join(a.Jobs, s.Job, "brief-delivered.md"))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	if str(sent.Payload, "pane_id") != "w:p1" || str(sent.Payload, "brief_sha256") != hex.EncodeToString(sum[:]) {
		t.Fatalf("receipt=%v", sent.Payload)
	}
	if receipt := a.briefSentReceipt(events, s.Job); receipt == nil {
		t.Fatal("the node's own receipt is not accepted as spawn evidence")
	}
}

// The floor follows the host's own retry period: with a 25 s retry, a 20 s
// window ends before the first Return retry that the window exists to keep.
func TestSubmitFloorFollowsConfiguredRetry(t *testing.T) {
	a, d := claudeDeliveryApp(t, func(*claudeDelivery) string { return claudeScreenEmpty() }, idleStatus)
	a.Config.Local = map[string]any{"brief_submit_timeout_ms": int64(60000), "brief_submit_retry_ms": int64(25000)}
	if got, want := a.submitFloor(), 25*time.Second+briefSubmitWaitSlice; got != want {
		t.Fatalf("floor=%v want %v", got, want)
	}
	// A default-sized window (20 s) would pass this budget; the configured one must not.
	ctx, cancel := context.WithTimeout(t.Context(), briefSubmitReserve+defaultFloor+2*time.Second)
	defer cancel()
	if err := a.deliver(ctx, "claude", "w:p1", claudeTestBrief); code(err) != exitConfig || d.pastes != 0 {
		t.Fatalf("err=%v pastes=%d", err, d.pastes)
	}
	a.Config.Local = map[string]any{"brief_submit_timeout_ms": int64(30000), "brief_submit_retry_ms": int64(29000)}
	if got := a.submitFloor(); got != 30*time.Second {
		t.Fatalf("floor=%v, want the configured timeout to bound it", got)
	}
}

// The tab create is not run under the launch deadline: a create killed there
// may already have made the pane, which would then never be parsed or closed.
func TestSpawnTabCreateRunsUnderNodeBudget(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	spy := &deadlineSpy{Runner: f}
	a.Runner = spy
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	want, _ := ctx.Deadline()
	_ = a.Spawn(ctx, s)
	if !spy.seen {
		t.Fatal("no tab create observed")
	}
	if !spy.deadline.Equal(want) {
		t.Fatalf("tab create deadline is %v before the node budget's", want.Sub(spy.deadline))
	}
}

type deadlineSpy struct {
	Runner
	seen     bool
	deadline time.Time
}

func (d *deadlineSpy) Run(ctx context.Context, c Command) Result {
	if c.Name == "herdr" && len(c.Args) > 1 && c.Args[0] == "tab" && c.Args[1] == "create" {
		d.seen = true
		d.deadline, _ = ctx.Deadline()
	}
	return d.Runner.Run(ctx, c)
}

// The send can take seconds of the floor. Confirmation then ends before the
// node deadline minus what the capture needs, and refuses with 76 and its
// capture, not with the node's context error.
func TestConfirmBriefEndsBeforeNodeDeadlineWhenSendSpentTheFloor(t *testing.T) {
	a, d := claudeDeliveryApp(t, func(*claudeDelivery) string { return claudeScreenBriefInComposer() }, idleStatus)
	// 14 s remain: the 20 s reserve is gone and the floor no longer fits.
	remaining := briefCaptureNeed + 2*time.Second
	ctx, cancel := context.WithTimeout(t.Context(), remaining)
	defer cancel()
	err := a.confirmBrief(ctx, "claude", "w:p1", claudeTestBrief, claudeScreenEmpty())
	m := windowCutRE.FindStringSubmatch(errText(err))
	if code(err) != exitSubmitUnconfirmed || !strings.Contains(errText(err), "BRIEF_SUBMISSION_UNCONFIRMED") || m == nil {
		t.Fatalf("err=%v", err)
	}
	// The window must end where the capture's time begins, not run to the
	// node deadline: at most remaining minus briefCaptureNeed.
	if secs, _ := strconv.Atoi(m[1]); time.Duration(secs)*time.Second > remaining-briefCaptureNeed {
		t.Fatalf("window of %ds runs into the %v the capture needs (budget %v)", secs, briefCaptureNeed, remaining)
	}
	if ctx.Err() != nil {
		t.Fatal("the node budget expired under the confirmation")
	}
	_ = d
}

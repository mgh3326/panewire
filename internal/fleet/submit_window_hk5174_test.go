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
		{"reserve plus a short window", briefSubmitReserve + briefSubmitMinWindow - 5*time.Second},
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
	ctx, cancel := context.WithTimeout(t.Context(), briefSubmitReserve+briefSubmitMinWindow+3*time.Second)
	defer cancel()
	err := a.deliver(ctx, "claude", "w:p1", claudeTestBrief)
	m := windowCutRE.FindStringSubmatch(errText(err))
	if code(err) != exitSubmitUnconfirmed || m == nil {
		t.Fatalf("err=%v", err)
	}
	if secs, _ := strconv.Atoi(m[1]); time.Duration(secs)*time.Second < briefSubmitMinWindow {
		t.Fatalf("window cut to %ds, below the %v minimum", secs, briefSubmitMinWindow)
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
	tight, cancelTight := context.WithTimeout(t.Context(), briefSubmitReserve+briefSubmitMinWindow-time.Second)
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
	if !ok || until.Sub(launchBy) != briefSubmitReserve+briefSubmitMinWindow {
		t.Fatalf("launch ends %v before the node budget, want reserve+window=%v", until.Sub(launchBy), briefSubmitReserve+briefSubmitMinWindow)
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
	ctx, cancel := context.WithTimeout(t.Context(), briefSubmitReserve+briefSubmitMinWindow-time.Second)
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

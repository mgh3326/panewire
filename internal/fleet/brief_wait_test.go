package fleet

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

type deliveryWaitFake struct {
	now                 time.Time
	waits, lists, reads int
	returns             []time.Duration
	slices              []time.Duration
	submitted           bool
	status, screen      string
	reply               func(time.Duration) Result
}

func waitDeliveryApp(t *testing.T, reply func(time.Duration) Result) (*App, *deliveryWaitFake) {
	t.Helper()
	a := testApp(t)
	f := &deliveryWaitFake{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), status: "idle", screen: claudeScreenBriefInComposer(), reply: reply}
	start := f.now
	a.Now = func() time.Time { return f.now }
	a.Sleep = func(_ context.Context, d time.Duration) error { f.now = f.now.Add(d); return nil }
	a.Config.Local = map[string]any{"brief_submit_timeout_ms": int64(20000), "brief_submit_retry_ms": int64(7000)}
	a.Runner.(*fakeRunner).Do = func(c Command) Result {
		args := c.Args
		switch {
		case len(args) >= 2 && args[0] == "agent" && args[1] == "list":
			if f.submitted {
				f.lists++
			}
			return Result{Out: fmt.Sprintf(`{"agents":[{"pane_id":"w:p1","agent_status":%q}]}`, f.status)}
		case len(args) >= 2 && args[0] == "pane" && args[1] == "read":
			f.reads++
			if !f.submitted {
				return Result{Out: claudeScreenEmpty()}
			}
			return Result{Out: f.screen}
		case len(args) >= 2 && ((args[0] == "agent" && args[1] == "prompt") || (args[0] == "pane" && args[1] == "send-text")):
			f.submitted = true
		case len(args) >= 2 && args[1] == "send-keys":
			f.returns = append(f.returns, f.now.Sub(start))
		case len(args) >= 2 && args[0] == "agent" && args[1] == "wait":
			f.waits++
			if len(args) != 9 || strings.Join(args[3:7], " ") != "--until working --until blocked" || args[7] != "--timeout" {
				t.Fatalf("wait args: %v", args)
			}
			ms, err := strconv.Atoi(args[8])
			if err != nil || ms < 1 {
				t.Fatalf("wait timeout: %v", args)
			}
			d := time.Duration(ms) * time.Millisecond
			f.slices = append(f.slices, d)
			return f.reply(d)
		}
		return Result{}
	}
	return a, f
}

func waitAnswer(status string) Result {
	return Result{Out: fmt.Sprintf(`{"id":"cli:agent:wait","result":{"type":"agent_info","agent":{"agent_status":%q}}}`, status)}
}
func waitTimeout() Result {
	return Result{Code: 1, Out: `{"error":{"code":"timeout","message":"timed out"}}`}
}

func TestDeliverWaitWorkingRequiresScreenProof(t *testing.T) {
	a, f := waitDeliveryApp(t, func(time.Duration) Result { return waitAnswer("working") })
	f.screen = claudeScreenEmpty()
	if err := a.deliver(t.Context(), "claude", "w:p1", claudeTestBrief); err != nil {
		t.Fatal(err)
	}
	if f.waits != 1 || f.lists != 0 || f.reads != 2 {
		t.Fatalf("waits=%d lists=%d reads=%d", f.waits, f.lists, f.reads)
	}
}

func TestDeliverWaitTimeoutRetryCadence(t *testing.T) {
	var f *deliveryWaitFake
	a, fake := waitDeliveryApp(t, func(d time.Duration) Result { f.now = f.now.Add(d); return waitTimeout() })
	f = fake
	err := a.deliver(t.Context(), "claude", "w:p1", claudeTestBrief)
	if code(err) != exitSubmitUnconfirmed || !strings.Contains(err.Error(), "BRIEF_SUBMISSION_UNCONFIRMED") || !strings.Contains(err.Error(), "brief still in composer") {
		t.Fatalf("err=%v", err)
	}
	if f.waits != 6 || f.lists != f.waits || fmt.Sprint(f.returns) != "[7s 14s]" || fmt.Sprint(f.slices) != "[5s 2s 5s 2s 5s 1s]" {
		t.Fatalf("waits=%d lists=%d returns=%v slices=%v", f.waits, f.lists, f.returns, f.slices)
	}
}

func TestDeliverWaitBlockedDoesNotSpinOrSubmit(t *testing.T) {
	a, f := waitDeliveryApp(t, func(time.Duration) Result { return waitAnswer("blocked") })
	f.status = "blocked"
	// The initial status is idle before prompt.
	base := a.Runner.(*fakeRunner).Do
	a.Runner.(*fakeRunner).Do = func(c Command) Result {
		if !f.submitted && len(c.Args) > 1 && c.Args[1] == "list" {
			return Result{Out: `{"agents":[{"pane_id":"w:p1","agent_status":"idle"}]}`}
		}
		return base(c)
	}
	err := a.deliver(t.Context(), "claude", "w:p1", claudeTestBrief)
	if code(err) != exitSubmitUnconfirmed || f.waits < 1 || f.waits > 21 || len(f.returns) != 0 {
		t.Fatalf("err=%v waits=%d returns=%v", err, f.waits, f.returns)
	}
}

func TestDeliverWaitFallbackAndCmd(t *testing.T) {
	for _, tc := range []struct {
		name, kind string
		reply      Result
		waits      int
	}{
		{"empty", "claude", Result{}, 1}, {"garbage", "claude", Result{Out: "garbage"}, 1}, {"missing", "claude", Result{Code: 1, Out: `{"error":{"code":"agent_not_found"}}`}, 1}, {"cmd", "cmd", Result{}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, f := waitDeliveryApp(t, func(time.Duration) Result { return tc.reply })
			err := a.deliver(t.Context(), tc.kind, "w:p1", claudeTestBrief)
			if code(err) != exitSubmitUnconfirmed || f.waits != tc.waits || f.lists != 20 || f.reads != 21 {
				t.Fatalf("err=%v waits=%d lists=%d reads=%d", err, f.waits, f.lists, f.reads)
			}
		})
	}
}

func TestDeliverWaitWorkingWithComposerIsNotProof(t *testing.T) {
	a, f := waitDeliveryApp(t, func(time.Duration) Result { return waitAnswer("working") })
	err := a.deliver(t.Context(), "claude", "w:p1", claudeTestBrief)
	if code(err) != exitSubmitUnconfirmed || f.waits == 0 {
		t.Fatalf("err=%v waits=%d", err, f.waits)
	}
}

func TestDeliverWaitTimeoutChecksStatusBeforeReturn(t *testing.T) {
	var f *deliveryWaitFake
	a, fake := waitDeliveryApp(t, func(d time.Duration) Result { f.now = f.now.Add(d); f.status = "blocked"; return waitTimeout() })
	f = fake
	err := a.deliver(t.Context(), "claude", "w:p1", claudeTestBrief)
	if code(err) != exitSubmitUnconfirmed || f.lists == 0 || len(f.returns) != 0 {
		t.Fatalf("err=%v lists=%d returns=%v", err, f.lists, f.returns)
	}
}

func TestDeliverWaitTimeoutStillAcceptsSubmittedTurn(t *testing.T) {
	var f *deliveryWaitFake
	a, fake := waitDeliveryApp(t, func(d time.Duration) Result { f.now = f.now.Add(d); return waitTimeout() })
	f = fake
	f.screen = claudeScreenSubmitted()
	if err := a.deliver(t.Context(), "claude", "w:p1", claudeTestBrief); err != nil || f.waits != 1 || f.lists != 1 {
		t.Fatalf("err=%v waits=%d lists=%d", err, f.waits, f.lists)
	}
}

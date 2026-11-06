package fleet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// quotaStopFixture is a live job of the given role whose pane reports status
// and shows screen. It records every herdr call so a test can prove the pane
// was only read, never typed into.
func quotaStopFixture(t *testing.T, role, status, screen string) (*App, *fakeRunner, *fakeHK, string) {
	t.Helper()
	a := testApp(t)
	f := ledgerRunner(t, a)
	h := newFakeHK(t)
	h.claimJob("j", float64(42), "test-host")
	h.records["j"]["state"] = "spawned"
	a.JobStore = h.client()
	f.Do = func(c Command) Result {
		if c.Name != "herdr" {
			return Result{}
		}
		switch {
		case strings.Join(c.Args, " ") == "agent list":
			return Result{Out: fmt.Sprintf(`{"result":{"agents":[{"pane_id":"w:p1","agent_status":%q,"state_change_seq":7}]}}`, status)}
		case len(c.Args) > 2 && c.Args[0] == "agent" && c.Args[1] == "read":
			return Result{Out: screen}
		}
		t.Errorf("unexpected herdr call %v", c.Args)
		return Result{}
	}
	report := filepath.Join(a.Jobs, "j", "report.md")
	putEvent(t, a, "j", "job.claim", map[string]any{"owner_lane": "owner", "task_id": "42", "t_level": "T1", "role": role})
	putEvent(t, a, "j", "job.spawned", map[string]any{"pane_id": "w:p1", "label": "agent-a", "report": report})
	seedLedger(t, a, "j")
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "60")
	return a, f, h, report
}

func devinStopScreen(t *testing.T) string {
	t.Helper()
	b, e := os.ReadFile("../../testdata/quota-stop/devin-something-went-wrong.txt")
	if e != nil {
		t.Fatal(e)
	}
	return string(b)
}

func lostPayload(t *testing.T, a *App) map[string]any {
	t.Helper()
	evs, e := a.events("j")
	if e != nil {
		t.Fatal(e)
	}
	for _, ev := range evs {
		if ev.Kind == "job.lost" {
			return ev.Payload
		}
	}
	return nil
}

// An idle worker, tester or reviewer pane on devin's rate-limit screen with no
// report ends the job as lost with a reason, instead of idling to the watch
// deadline (hk 3302).
func TestWatcherQuotaStopEndsIdleJobAsLost(t *testing.T) {
	for _, role := range []string{"worker", "tester", "reviewer"} {
		t.Run(role, func(t *testing.T) {
			a, f, h, _ := quotaStopFixture(t, role, "idle", devinStopScreen(t))
			reset := a.Now().Add(5 * time.Minute).UTC().Format(time.RFC3339)
			if e := a.watch(context.Background(), []string{"j"}); e != nil {
				t.Fatal(e)
			}
			rec := h.record(t, "j")
			wantEnd := "quota_stop pool=devin-free reset_at=" + reset
			if str(rec, "state") != "lost" || str(rec, "end_reason") != wantEnd || len(wantEnd) > 128 {
				t.Fatalf("hk record state=%v end_reason=%q, want lost / %q", rec["state"], rec["end_reason"], wantEnd)
			}
			if _, ok := rec["result"]; ok {
				t.Fatalf("hk record carries a result: %v", rec["result"])
			}
			p := lostPayload(t, a)
			if p == nil || str(p, "reason") != "quota_stop" || str(p, "pool") != "devin-free" || str(p, "reset_at") != reset || str(p, "pane_id") != "w:p1" {
				t.Fatalf("job.lost payload=%v, want reason quota_stop pool devin-free reset_at %s", p, reset)
			}
			if _, ok := p["owner_lane"]; ok {
				t.Fatalf("quota stop unexpectedly routes a lane relay: %v", p)
			}
			if st, pools := ledgerState(t, a, "j"); st != "released" || len(pools) != 0 {
				t.Fatalf("quota not released: %s %v", st, pools)
			}
			for _, c := range f.Calls {
				if c.Name == "herdr" && len(c.Args) > 1 && c.Args[0] == "agent" && c.Args[1] != "list" && c.Args[1] != "read" {
					t.Fatalf("watcher wrote to the pane: %v", c.Args)
				}
			}
		})
	}
}

func TestWatcherQuotaStopLeavesOtherPanesLive(t *testing.T) {
	for name, tc := range map[string]struct{ role, status, screen string }{
		"counsel role":               {"counsel", "idle", ""},
		"working pane":               {"worker", "working", ""},
		"idle, not a quota":          {"worker", "idle", "I finished the task.\n❭ Ask Devin to build features, fix bugs, or work on your code\n"},
		"stop, then output":          {"worker", "idle", "▲ Something went wrong — Reached free model rate limit … reset in 5 minutes\nresumed and wrote the report\n"},
		"working pane, claude limit": {"worker", "working", "You've hit your session limit · resets unknown\n"},
	} {
		t.Run(name, func(t *testing.T) {
			screen := tc.screen
			if screen == "" {
				screen = devinStopScreen(t)
			}
			a, _, h, _ := quotaStopFixture(t, tc.role, tc.status, screen)
			t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "1")
			if e := a.watch(context.Background(), []string{"j"}); e != nil {
				t.Fatal(e)
			}
			if got := str(h.record(t, "j"), "state"); got == "lost" {
				t.Fatal("job was ended as lost")
			}
			if slices.Contains(eventKindsOf(t, a, "j"), "job.lost") {
				t.Fatal("job.lost recorded")
			}
			if st, pools := ledgerState(t, a, "j"); st != "leased" || len(pools) != 1 {
				t.Fatalf("live job lost its lease: %s %v", st, pools)
			}
		})
	}
}

// A report with the end signal is today's done path even when the pane still
// shows the stop screen.
func TestWatcherQuotaStopWithReportKeepsDonePath(t *testing.T) {
	a, f, h, report := quotaStopFixture(t, "reviewer", "idle", devinStopScreen(t))
	writeReport(t, report, "final\n")
	putLaneEvent(t, a, "w:p1", "READY", a.Now().Add(time.Minute))
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if records, relays := completions(t, a, f); records != 1 || relays != 1 {
		t.Fatalf("records=%d relays=%d, want one completion", records, relays)
	}
	if slices.Contains(eventKindsOf(t, a, "j"), "job.lost") {
		t.Fatal("job with a report was ended as lost")
	}
	if got := str(h.record(t, "j"), "state"); got != "completed" {
		t.Fatalf("hk state=%q, want completed", got)
	}
}

// With no parsed reset, reset_at is left out of both the hk end_reason and job.lost.
func TestWatcherQuotaStopUnknownResetOmitsResetAt(t *testing.T) {
	a, _, h, _ := quotaStopFixture(t, "worker", "idle", "▲ Something went wrong — Reached free model rate limit\n❭ Ask Devin to build features, fix bugs, or work on your code\n")
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if got := str(h.record(t, "j"), "end_reason"); got != "quota_stop pool=devin-free" {
		t.Fatalf("end_reason=%q", got)
	}
	if p := lostPayload(t, a); p == nil || str(p, "reason") != "quota_stop" || str(p, "pool") != "devin-free" || p["reset_at"] != nil {
		t.Fatalf("job.lost payload=%v", p)
	}
}

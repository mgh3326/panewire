package panewire

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// hk 5349: an implementer on a free-limit or request-rate-limit stop is waited
// out and resumed once when the reset is inside its budget, and is resumed at
// once when the reset has already passed; only a reset past the budget hands it
// over. The 2026-11-19 loss was pw-terminal-owner-5344: a task with no expected
// time and an 8 second reset was handed over and ended job.lost with no
// replacement.

func implementerResetPolicy(t *testing.T) quotaPolicy {
	t.Helper()
	policy, ok := parseQuotaPolicy("## Values\n- default_expected: 1d\n- urgent_priorities: [1, 2]\n- max_waits: 1\n- max_wait: 1h\n- resume_text: \"continue\"\n")
	if !ok {
		t.Fatal("policy did not parse")
	}
	return policy
}

func TestQuotaDecisionJobShortResetWaitsPastResetResumesLateResetHandsOver(t *testing.T) {
	policy := implementerResetPolicy(t)
	now := time.Date(2026, 11, 19, 2, 19, 51, 0, time.UTC)
	started := now.Add(-5 * time.Minute)
	for _, tc := range []struct {
		name     string
		expected time.Duration
		started  time.Time
		reset    time.Time
		waits    int
		decision string
		reason   string
	}{
		{"reset already past", 4 * time.Hour, started, now.Add(-41 * time.Second), 0, "wait", "reset_passed"},
		{"reset already past, no expected time", 0, started, now.Add(-time.Second), 0, "wait", "reset_passed"},
		{"reset already past, start unknown", 4 * time.Hour, time.Time{}, now.Add(-time.Second), 0, "wait", "reset_passed"},
		{"reset exactly now", 4 * time.Hour, started, now, 0, "wait", "reset_passed"},
		{"reset 10 minutes away", 4 * time.Hour, started, now.Add(10 * time.Minute), 0, "wait", "reset_within_expected"},
		{"short reset, task names no expected time", 0, started, now.Add(8 * time.Second), 0, "wait", "reset_within_default_expected"},
		{"reset past start plus expected", time.Hour, started, started.Add(time.Hour + time.Minute), 0, "handover", "reset_too_late"},
		{"reset 23 hours away, no expected time", 0, started, now.Add(23 * time.Hour), 0, "handover", "reset_beyond_max_wait"},
		{"reset unknown", 4 * time.Hour, started, time.Time{}, 0, "handover", "reset_unknown"},
		{"a past reset after one resume", 4 * time.Hour, started, now.Add(-time.Second), 1, "handover", "resume_limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, reason := quotaDecisionJob(policy, true, 3, tc.expected, tc.started, tc.waits, quotaStop{Pool: "devin-free", Reset: tc.reset}, now)
			if got != tc.decision || reason != tc.reason {
				t.Fatalf("quotaDecisionJob = (%s, %s), want (%s, %s)", got, reason, tc.decision, tc.reason)
			}
		})
	}
}

// A reset past the job's budget is handed over even when it is inside the
// policy's max_wait: the budget is the task's own expected time.
func TestQuotaDecisionJobBudgetIsTheTasksExpectedTime(t *testing.T) {
	policy := implementerResetPolicy(t)
	now := time.Date(2026, 11, 19, 2, 0, 0, 0, time.UTC)
	got, reason := quotaDecisionJob(policy, true, 3, 20*time.Minute, now.Add(-15*time.Minute), 0, quotaStop{Reset: now.Add(10 * time.Minute)}, now)
	if got != "handover" || reason != "reset_too_late" {
		t.Fatalf("quotaDecisionJob = (%s, %s), want (handover, reset_too_late)", got, reason)
	}
}

// pw-terminal-owner-5344: the task names no expected time and the stop's reset
// is seconds away. The implementer is waited out and resumed once, and no
// job.lost is announced.
func TestQuotaStopImplementerWithoutExpectedTimeResumesAfterShortReset(t *testing.T) {
	rig := newRoleStopRigScreen(t, "implementer", time.Minute, `{"tasks":[{"priority":3,"refs":{"job_id":"job-active"}}]}`, "testdata/quota-stop/devin-request-rate-limit.txt", "devin-swe2-flash")
	rig.run()
	if d, reason := rig.decision(t); d != "wait" || reason != "reset_within_default_expected" {
		t.Fatalf("decision=%s reason=%s, want wait reset_within_default_expected", d, reason)
	}
	if rig.sends != 1 || len(rig.announce) != 0 {
		t.Fatalf("sends=%d announced=%d, want one resume and no hand-over (the job is not lost)", rig.sends, len(rig.announce))
	}
}

// A reset 10 minutes away waits first, then resumes: nothing is sent before the
// reset, one resume after it, and the job is never announced lost.
func TestQuotaStopImplementerResetTenMinutesAwayWaitsThenResumes(t *testing.T) {
	screen := filepath.Join(t.TempDir(), "devin-ten-minutes.txt")
	if err := os.WriteFile(screen, []byte("Reached free model rate limit · reset in 10 minutes (at 02:30 UTC)\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	rig := newRoleStopRigScreen(t, "implementer", time.Hour, roleStopTask, screen, "devin-swe2-flash")
	rig.observe("working", 1, rig.now)
	rig.observe("idle", 2, rig.now.Add(time.Second))
	rig.manager.Tick(context.Background(), rig.now.Add(3*time.Second))
	rig.route(0, "owner")
	rig.manager.Tick(context.Background(), rig.now.Add(4*time.Second))
	rig.manager.Tick(context.Background(), rig.now.Add(5*time.Second))
	if d, reason := rig.decision(t); d != "wait" || reason != "reset_within_expected" {
		t.Fatalf("decision=%s reason=%s, want wait reset_within_expected", d, reason)
	}
	rig.manager.Tick(context.Background(), rig.now.Add(5*time.Minute))
	if rig.sends != 0 {
		t.Fatalf("resumed %d time(s) before the reset", rig.sends)
	}
	rig.manager.Tick(context.Background(), rig.now.Add(11*time.Minute))
	if rig.sends != 1 || len(rig.announce) != 0 {
		t.Fatalf("sends=%d announced=%d, want one resume after the reset and no hand-over", rig.sends, len(rig.announce))
	}
}

// A reset already past when the stop is decided (the devin session record's
// reset is the log time plus the span, so it can be past by the time the pane
// settles idle) is resumed at once and the job is not lost.
func TestQuotaStopImplementerResetAlreadyPastResumesNotLost(t *testing.T) {
	rig := newRoleStopRigScreen(t, "implementer", time.Hour, roleStopTask, "testdata/quota-stop/devin.txt", "devin-swe2-flash")
	// The manager's own look finds no stop, which reserves the wake; the stop is
	// then decided directly with a reset that has already passed.
	rig.q.read = func(context.Context, string) (string, error) { return "idle\n", nil }
	rig.observe("working", 1, rig.now)
	rig.observe("idle", 2, rig.now.Add(time.Second))
	rig.manager.Tick(context.Background(), rig.now.Add(3*time.Second))
	rig.route(0, "owner")
	rig.manager.Tick(context.Background(), rig.now.Add(4*time.Second))
	request := rig.requests[0]
	candidate := idleWakeCandidate{EventID: request.EventID, PaneID: request.Pane, WorkspaceID: request.WorkspaceID, Label: request.Label, Status: "idle"}
	job := HubActiveJob{JobID: "job-active", AgentLabel: "worker-synthetic", OwnerLane: "owner", Role: "implementer", StartedAt: rig.now.Add(-time.Hour).UTC().Format(time.RFC3339)}
	stop := quotaStop{Pool: "devin-free", Reset: rig.now.Add(-41 * time.Second)}
	if err := rig.q.actOnQuotaStop(context.Background(), candidate, job, "", "42", stop, "structured", rig.now.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if d, reason := rig.decision(t); d != "wait" || reason != "reset_passed" {
		t.Fatalf("decision=%s reason=%s, want wait reset_passed", d, reason)
	}
	// The pane still shows the stop when the resume is due.
	screen, err := os.ReadFile("testdata/quota-stop/devin.txt")
	if err != nil {
		t.Fatal(err)
	}
	rig.q.read = func(context.Context, string) (string, error) { return string(screen), nil }
	rig.manager.Tick(context.Background(), rig.now.Add(10*time.Second))
	rig.manager.Tick(context.Background(), rig.now.Add(20*time.Second))
	if rig.sends != 1 || len(rig.announce) != 0 {
		t.Fatalf("sends=%d announced=%d, want one resume and no hand-over (the job is not lost)", rig.sends, len(rig.announce))
	}
}

// A reset past the task's budget is handed over, not waited out.
func TestQuotaStopImplementerResetPastBudgetIsHandedOver(t *testing.T) {
	screen := filepath.Join(t.TempDir(), "devin-late.txt")
	if err := os.WriteFile(screen, []byte("Reached free model rate limit · reset in 50 minutes (at 03:10 UTC)\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Started 3h50m ago with 4h expected: 10 minutes of budget are left.
	rig := newRoleStopRigScreen(t, "implementer", 3*time.Hour+50*time.Minute, roleStopTask, screen, "devin-swe2-flash")
	rig.run()
	if d, reason := rig.decision(t); d != "handover" || reason != "reset_too_late" {
		t.Fatalf("decision=%s reason=%s, want handover reset_too_late", d, reason)
	}
	if rig.sends != 0 || len(rig.announce) != 1 {
		t.Fatalf("sends=%d announced=%d, want a hand-over and no resume", rig.sends, len(rig.announce))
	}
}

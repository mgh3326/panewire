package panewire

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A reviewer on a quota stop (hk 5355): the manager waits out a short reset and
// resumes the pane once; any other stop is announced for the hub to carry on at once (hk 5690).
// The rig is the implementer rig with the spawn receipt naming a reviewer. The
// manager runs in shadow mode, and a reviewer is still acted on.

const reviewerStopTask = roleStopTask // priority 3, expected 4h

func newReviewerStopRig(t *testing.T, startedAgo time.Duration, task, screenPath string) *roleStopRig {
	t.Helper()
	return newRoleStopRigScreen(t, "reviewer", startedAgo, task, screenPath, "")
}

// reviewerLedgerHas reports whether the job's ledger holds an event of kind.
func reviewerLedgerHas(t *testing.T, rig *roleStopRig, kind string) bool {
	t.Helper()
	dir := filepath.Join(rig.manager.inboxRoot, "jobs", "job-active", "events")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), `"kind":"`+kind+`"`) {
			return true
		}
	}
	return false
}

// decisionFor returns the quota decision recorded for one stop, by its event id.
func decisionFor(t *testing.T, rig *roleStopRig, eventID string) (decision, reason string) {
	t.Helper()
	if err := rig.store.db.QueryRow(`SELECT decision,reason FROM quota_stop_decisions WHERE event_id=? AND decision<>''`, eventID).Scan(&decision, &reason); err != nil {
		t.Fatalf("no decision for stop %s: %v", eventID, err)
	}
	return
}

// A reset two minutes away: the reviewer waits and is resumed once at the reset.
// The job is not lost and the watcher is not asked to end it (hk 5355).
func TestQuotaReviewerShortResetIsResumedOnce(t *testing.T) {
	rig := newReviewerStopRig(t, time.Hour, reviewerStopTask, "testdata/quota-stop/devin-reset-2min.txt")
	rig.run()
	if d, reason := rig.decision(t); d != "wait" || reason != "reset_within_reviewer_wait" {
		t.Fatalf("decision=%s reason=%s, want wait reset_within_reviewer_wait", d, reason)
	}
	if rig.sends != 1 {
		t.Fatalf("sends=%d, want one resume at the reset", rig.sends)
	}
	if len(rig.announce) != 0 {
		t.Fatalf("a waited reviewer was announced: %+v", rig.announce)
	}
	if reviewerLedgerHas(t, rig, "job.lost") {
		t.Fatal("job.lost recorded for a reviewer that was resumed")
	}
	rig.manager.Tick(context.Background(), rig.now.Add(10*time.Minute))
	if rig.sends != 1 {
		t.Fatalf("resumed more than once: sends=%d", rig.sends)
	}
}

// reviewer_wait is the policy's reviewer bound (hk 5355): default 10 min, never
// above max_wait, and a malformed or repeated line makes the policy unavailable.
func TestQuotaReviewerWaitPolicyLine(t *testing.T) {
	base := quotaMaxWaitDoc()
	for name, tc := range map[string]struct {
		doc   string
		want  time.Duration
		valid bool
	}{
		"absent defaults to ten minutes": {base, 10 * time.Minute, true},
		"explicit value":                 {quotaMaxWaitDoc("reviewer_wait: 3m"), 3 * time.Minute, true},
		"clamped to max_wait":            {quotaMaxWaitDoc("max_wait: 5m", "reviewer_wait: 20m"), 5 * time.Minute, true},
		// The watcher holds a reviewer's stop only within ten minutes, so a larger
		// policy value is clamped to that ceiling (mergewake review, hk 5355).
		"clamped to the watcher ceiling": {quotaMaxWaitDoc("reviewer_wait: 30m"), 10 * time.Minute, true},
		"malformed fails the policy":     {quotaMaxWaitDoc("reviewer_wait: soon"), 0, false},
		"zero fails the policy":          {quotaMaxWaitDoc("reviewer_wait: 0s"), 0, false},
		"repeated line fails the policy": {quotaMaxWaitDoc("reviewer_wait: 2m", "reviewer_wait: 3m"), 0, false},
	} {
		t.Run(name, func(t *testing.T) {
			p, ok := parseQuotaPolicy(tc.doc)
			if ok != tc.valid {
				t.Fatalf("valid=%v, want %v", ok, tc.valid)
			}
			if ok && p.reviewerWaitLimit() != tc.want {
				t.Fatalf("reviewer wait=%v, want %v", p.reviewerWaitLimit(), tc.want)
			}
		})
	}
}

// The policy's reviewer_wait bounds the manager's reviewer decision: a reset
// inside the default ten minutes but beyond a policy of one minute is announced.
func TestQuotaReviewerPolicyWaitBoundsTheDecision(t *testing.T) {
	rig := newReviewerStopRig(t, time.Hour, reviewerStopTask, "testdata/quota-stop/devin-reset-2min.txt")
	doc := quotaMaxWaitDoc("reviewer_wait: 1m")
	rig.q.hk.policy = func(context.Context) ([]byte, error) { return []byte(quotaPolicyAnswer(doc)), nil }
	rig.run()
	if d, reason := rig.decision(t); d != "handover" || reason != "reset_beyond_reviewer_wait" {
		t.Fatalf("decision=%s reason=%s, want handover reset_beyond_reviewer_wait", d, reason)
	}
	if rig.sends != 0 || len(rig.announce) != 1 {
		t.Fatalf("sends=%d ended=%d, want no resume and one end", rig.sends, len(rig.announce))
	}
}

// A reset two hours away is beyond the reviewer wait: the reviewer is announced
// for the hub to carry on at once, with no resume (hk 5690). The same stop at the
// reviewer's first prompt, which is how the 2026-11-19 Devin reviewers stopped,
// is this case: a pane with no built-up work is handed over, never ended.
func TestQuotaReviewerLongResetIsAnnouncedAtOnceForTheHub(t *testing.T) {
	rig := newReviewerStopRig(t, time.Hour, reviewerStopTask, "testdata/quota-stop/devin-reset-2h.txt")
	rig.run()
	if d, reason := rig.decision(t); d != "handover" || reason != "reset_beyond_reviewer_wait" {
		t.Fatalf("decision=%s reason=%s, want handover reset_beyond_reviewer_wait", d, reason)
	}
	if rig.sends != 0 {
		t.Fatalf("resumed a reviewer on a long reset: sends=%d", rig.sends)
	}
	if len(rig.announce) != 1 || rig.announce[0].Pool != "devin-free" {
		t.Fatalf("announced=%+v, want one devin-free announcement", rig.announce)
	}
	rig.manager.Tick(context.Background(), rig.now.Add(10*time.Minute))
	if len(rig.announce) != 1 {
		t.Fatalf("the reviewer was announced again: %d", len(rig.announce))
	}
}

// A second stop after the one resume announces the reviewer at once and sends nothing.
func TestQuotaReviewerSecondStopAfterResumeEndsAtOnce(t *testing.T) {
	rig := newReviewerStopRig(t, time.Hour, reviewerStopTask, "testdata/quota-stop/devin-reset-2min.txt")
	rig.run()
	if rig.sends != 1 {
		t.Fatalf("first stop: sends=%d, want one resume", rig.sends)
	}
	// The second stop is a new limit with its own reset. The ledger drops a
	// repeat of a decided pane and reset, and a real second stop never repeats it.
	secondScreen := []byte("Reached free model rate limit · reset in 3 minutes (at 00:13 UTC)\n")
	rig.q.read = func(context.Context, string) (string, error) { return string(secondScreen), nil }
	again := rig.now.Add(10 * time.Minute)
	rig.observe("working", 3, again)
	rig.observe("idle", 4, again.Add(time.Second))
	rig.manager.Tick(context.Background(), again.Add(5*time.Second))
	rig.route(1, "owner")
	rig.manager.Tick(context.Background(), again.Add(6*time.Second))
	rig.manager.Tick(context.Background(), again.Add(7*time.Second))
	rig.manager.Tick(context.Background(), again.Add(3*time.Minute))
	if len(rig.requests) != 2 {
		t.Fatalf("settled stops=%d, want the second one routed too", len(rig.requests))
	}
	if d, reason := decisionFor(t, rig, rig.requests[1].EventID); d != "handover" || reason != "resume_limit" {
		t.Fatalf("second stop decision=%s reason=%s, want handover resume_limit\nlogs:\n%s", d, reason, rig.logs.String())
	}
	if rig.sends != 1 {
		t.Fatalf("resumed a second time: sends=%d", rig.sends)
	}
	if len(rig.announce) != 1 {
		t.Fatalf("ended=%d, want the second stop ended once", len(rig.announce))
	}
}

// A stop with no readable reset ends the reviewer at once, as before.
func TestQuotaReviewerUnknownResetEndsAtOnce(t *testing.T) {
	screen := filepath.Join(t.TempDir(), "screen.txt")
	if err := os.WriteFile(screen, []byte("▲ Something went wrong — Reached free model rate limit\n❭ Ask Devin to build features, fix bugs, or work on your code\n"), 0600); err != nil {
		t.Fatal(err)
	}
	rig := newReviewerStopRig(t, time.Hour, reviewerStopTask, screen)
	rig.run()
	if d, reason := rig.decision(t); d != "handover" || reason != "reset_unknown" {
		t.Fatalf("decision=%s reason=%s, want handover reset_unknown", d, reason)
	}
	if rig.sends != 0 || len(rig.announce) != 1 {
		t.Fatalf("sends=%d ended=%d, want no resume and one end", rig.sends, len(rig.announce))
	}
}

// A wait that would pass the review deadline (the job's start plus its expected
// time) is ended at once. Here the deadline is a minute away and the reset two.
func TestQuotaReviewerResetPastReviewDeadlineEndsAtOnce(t *testing.T) {
	rig := newReviewerStopRig(t, 29*time.Minute, `{"tasks":[{"priority":3,"expected":"30m","refs":{"job_id":"job-active"}}]}`, "testdata/quota-stop/devin-reset-2min.txt")
	rig.run()
	if d, reason := rig.decision(t); d != "handover" || reason != "reset_past_review_deadline" {
		t.Fatalf("decision=%s reason=%s, want handover reset_past_review_deadline", d, reason)
	}
	if rig.sends != 0 || len(rig.announce) != 1 {
		t.Fatalf("sends=%d ended=%d, want no resume and one end", rig.sends, len(rig.announce))
	}
}

// A reset already past is due at once even when the job's start is missing: the
// stop is over and the pane is resumed, not ended (mergewake review, hk 5355).
func TestQuotaReviewerResetPassedWithoutStartIsResumed(t *testing.T) {
	now := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	policy, ok := parseQuotaPolicy(quotaMaxWaitDoc())
	if !ok {
		t.Fatal("policy did not parse")
	}
	passed := quotaStop{Pool: "devin-free", Reset: now.Add(-time.Minute)}
	if d, reason := quotaDecisionReviewer(policy, true, 3, time.Time{}, 4*time.Hour, 0, passed, now); d != "wait" || reason != "reset_passed" {
		t.Fatalf("passed reset without a start: decision=%s reason=%s, want wait reset_passed", d, reason)
	}
	future := quotaStop{Pool: "devin-free", Reset: now.Add(2 * time.Minute)}
	if d, reason := quotaDecisionReviewer(policy, true, 3, time.Time{}, 4*time.Hour, 0, future, now); d != "handover" || reason != "start_unknown" {
		t.Fatalf("future reset without a start: decision=%s reason=%s, want handover start_unknown", d, reason)
	}
}

// The quota-stop path acts only on a proven stop (mergewake review, hk 5355): a
// reviewer hand-over recorded for a failed screen read (pool unknown) must not
// reach the watcher's intent, even with --quota-stop-act on.
func TestQuotaReviewerUnprovenHandoverIsNotEndedWhenActing(t *testing.T) {
	rig := newReviewerStopRig(t, time.Hour, reviewerStopTask, "testdata/quota-stop/devin-reset-2min.txt")
	rig.q.act = true
	now := rig.now
	if _, err := rig.store.db.Exec(`INSERT INTO quota_stop_decisions(event_id,job,pane,label,pool,reset,decision,reason,role,created) VALUES('evt-unproven','job-active','workspace:pane-1','worker-synthetic','unknown','unknown','handover','screen_read_timeout','reviewer',?)`, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	rig.q.tick(context.Background(), now, false)
	if len(rig.announce) != 0 {
		t.Fatalf("an unproven reviewer hand-over ended the reviewer: %+v", rig.announce)
	}
}

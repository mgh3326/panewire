package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// A reviewer on a short provider-limit reset may wait for the reset and be
// resumed once, but only after the live watcher of the current attempt has
// adopted the wait (hk 5749). The tests drive the manager against a real ledger;
// the watcher is a stand-in that answers the request the way the real one does
// (internal/fleet/watch_quota_wait_t5749_test.go proves the real watcher's side).

// waitRig is a reviewer on a two-minute reset, with a stand-in watcher.
type waitRig struct {
	*roleStopRig
	// live is whether a watcher holds the job's watch lock now.
	live bool
	// kept counts the times the manager asked for a watcher to be running.
	kept int
	// requestErr, when set, fails the manager's wait request.
	requestErr error
}

func newWaitRig(t *testing.T) *waitRig {
	t.Helper()
	rig := &waitRig{roleStopRig: newReviewerStopRig(t, time.Hour, reviewerStopTask, "testdata/quota-stop/devin-reset-2min.txt"), live: true}
	wait := &rig.q.wait
	request, adopted := wait.request, wait.adopted
	wait.request = func(ctx context.Context, job, pane, attempt string, stop quotastop.Stop, until time.Time) (bool, error) {
		if rig.requestErr != nil {
			return false, rig.requestErr
		}
		return request(ctx, job, pane, attempt, stop, until)
	}
	wait.adopted = adopted
	wait.live = func(string) bool { return rig.live }
	wait.keep = func(context.Context, string, string) error { rig.kept++; return nil }
	return rig
}

// decide takes the pane to idle on the stop and ticks until the wait decision is
// recorded and the request written.
func (rig *waitRig) decide() {
	rig.observe("working", 1, rig.now)
	rig.observe("idle", 2, rig.now.Add(time.Second))
	rig.manager.Tick(context.Background(), rig.now.Add(3*time.Second))
	rig.route(0, "owner")
	rig.manager.Tick(context.Background(), rig.now.Add(4*time.Second))
	rig.manager.Tick(context.Background(), rig.now.Add(5*time.Second))
}

func (rig *waitRig) events() string {
	return filepath.Join(rig.manager.inboxRoot, "jobs", "job-active", "events")
}

// nextName is the file name of a ledger event of kind that follows every event
// already there: the ledger's sequence numbers are not contiguous, so the next
// one is the highest prefix plus one.
func (rig *waitRig) nextName(kind string) string {
	entries, err := os.ReadDir(rig.events())
	if err != nil {
		rig.t.Fatal(err)
	}
	next := 1
	for _, e := range entries {
		var n int
		if _, err := fmt.Sscanf(e.Name(), "%d-", &n); err == nil && n >= next {
			next = n + 1
		}
	}
	return fmt.Sprintf("%05d-%s.json", next, kind)
}

// ledgerEvent returns the payload of the newest ledger event of kind.
func (rig *waitRig) ledgerEvent(t *testing.T, kind string) (map[string]any, bool) {
	t.Helper()
	entries, err := os.ReadDir(rig.events())
	if err != nil {
		t.Fatal(err)
	}
	var found map[string]any
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(rig.events(), e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		var ev struct {
			Kind    string         `json:"kind"`
			Payload map[string]any `json:"payload"`
		}
		if json.Unmarshal(b, &ev) == nil && ev.Kind == kind {
			found = ev.Payload
		}
	}
	return found, found != nil
}

// ack answers the manager's request as the job's watcher does: an event naming
// the request's attempt and until, and a deadline that reaches it.
func (rig *waitRig) ack(t *testing.T) {
	t.Helper()
	req, ok := rig.ledgerEvent(t, "job.quota_stop_wait_request")
	if !ok {
		t.Fatal("the manager wrote no wait request")
	}
	until, err := time.Parse(time.RFC3339, req["until"].(string))
	if err != nil {
		t.Fatal(err)
	}
	ack := map[string]any{"kind": "job.quota_stop_wait_adopted", "created_at": time.Now().UTC().Format(time.RFC3339Nano), "payload": map[string]any{"pane_id": req["pane_id"], "attempt": req["attempt"], "pool": req["pool"], "until": req["until"], "deadline": until.Add(time.Minute).Format(time.RFC3339Nano)}}
	b, _ := json.Marshal(ack)
	if err := os.WriteFile(filepath.Join(rig.events(), rig.nextName("job.quota_stop_wait_adopted")), b, 0600); err != nil {
		t.Fatal(err)
	}
}

func (rig *waitRig) wantDecision(t *testing.T, decision, reason string) {
	t.Helper()
	if d, r := rig.decision(t); d != decision || r != reason {
		t.Fatalf("decision=%s reason=%s, want %s %s", d, r, decision, reason)
	}
}

// A short reset with a live watcher is adopted, waited out and resumed once, and
// the review runs on: it is not handed over, and a watcher is kept for it. (Base:
// every reviewer limit stop is a hand-over, so this fails there.)
func TestQuotaReviewerShortResetIsAdoptedWaitedOutAndResumed(t *testing.T) {
	rig := newWaitRig(t)
	rig.decide()
	rig.wantDecision(t, "wait", "reset_within_reviewer_wait")
	req, ok := rig.ledgerEvent(t, "job.quota_stop_wait_request")
	if !ok {
		t.Fatal("no wait was requested of the watcher")
	}
	if req["attempt"] == "" || req["attempt"] == nil || req["until"] == nil {
		t.Fatalf("the request does not name the attempt and the deadline: %v", req)
	}
	if rig.sends != 0 || len(rig.announce) != 0 {
		t.Fatalf("acted before the acknowledgement: sends=%d announced=%d", rig.sends, len(rig.announce))
	}
	rig.ack(t)
	rig.manager.Tick(context.Background(), rig.now.Add(6*time.Second))
	if rig.sends != 0 {
		t.Fatalf("resumed before the reset: sends=%d", rig.sends)
	}
	rig.manager.Tick(context.Background(), rig.now.Add(3*time.Minute))
	if rig.sends != 1 || len(rig.announce) != 0 {
		t.Fatalf("sends=%d announced=%d, want one resume and no hand-over", rig.sends, len(rig.announce))
	}
	if rig.kept == 0 {
		t.Fatal("no watcher was kept for the resumed review")
	}
	if _, lost := rig.ledgerEvent(t, "job.lost"); lost {
		t.Fatal("the waited-out reviewer was recorded lost")
	}
	rig.manager.Tick(context.Background(), rig.now.Add(10*time.Minute))
	if rig.sends != 1 {
		t.Fatalf("resumed more than once: %d", rig.sends)
	}
}

// Without an acknowledgement within the bound the stop is handed over exactly as
// every other reviewer stop, and the pane is never resumed, even at the reset with
// a watcher holding the lock: the ack, not the request and not the lock, adopts a
// wait.
func TestQuotaReviewerWaitNotAcknowledgedIsHandedOver(t *testing.T) {
	for name, setup := range map[string]func(*waitRig){
		"no watcher":             func(rig *waitRig) { rig.live = false },
		"a watcher, no ack":      func(rig *waitRig) {},
		"the request is refused": func(rig *waitRig) { rig.requestErr = errors.New("end lock held") },
	} {
		t.Run(name, func(t *testing.T) {
			rig := newWaitRig(t)
			setup(rig)
			rig.decide()
			rig.wantDecision(t, "wait", "reset_within_reviewer_wait")
			// Inside the bound the wait is only pending.
			rig.manager.Tick(context.Background(), rig.now.Add(20*time.Second))
			if len(rig.announce) != 0 || rig.sends != 0 {
				t.Fatalf("acted inside the bound: sends=%d announced=%d", rig.sends, len(rig.announce))
			}
			rig.live = true
			rig.manager.Tick(context.Background(), rig.now.Add(40*time.Second))
			rig.wantDecision(t, "handover", "wait_not_adopted")
			rig.manager.Tick(context.Background(), rig.now.Add(42*time.Second))
			rig.manager.Tick(context.Background(), rig.now.Add(3*time.Minute))
			if rig.sends != 0 {
				t.Fatalf("a reviewer was resumed without an acknowledged wait: sends=%d", rig.sends)
			}
			if len(rig.announce) != 1 || rig.announce[0].Pool != "devin-free" {
				t.Fatalf("announced=%+v, want one devin-free hand-over", rig.announce)
			}
		})
	}
}

// A watcher that exits after adopting the wait leaves the stop to be handed over
// at the reset, not resumed: the lock is checked again at the reset.
func TestQuotaReviewerWatcherGoneAfterAdoptingIsHandedOverAtTheReset(t *testing.T) {
	rig := newWaitRig(t)
	rig.decide()
	rig.ack(t)
	rig.manager.Tick(context.Background(), rig.now.Add(6*time.Second))
	rig.live = false
	rig.manager.Tick(context.Background(), rig.now.Add(3*time.Minute))
	rig.wantDecision(t, "handover", "watcher_gone")
	rig.manager.Tick(context.Background(), rig.now.Add(3*time.Minute+2*time.Second))
	if rig.sends != 0 {
		t.Fatalf("a reviewer was resumed with no watcher: sends=%d", rig.sends)
	}
	if len(rig.announce) != 1 {
		t.Fatalf("announced=%d, want one hand-over at the reset", len(rig.announce))
	}
}

// An attempt that changed after the wait was adopted (a re-prompt, a respawn) is
// not the one the watcher adopted it for: the stop is handed over, not resumed.
func TestQuotaReviewerAttemptChangedAfterAdoptingIsHandedOver(t *testing.T) {
	rig := newWaitRig(t)
	rig.decide()
	rig.ack(t)
	rig.manager.Tick(context.Background(), rig.now.Add(6*time.Second))
	reprompt := `{"kind":"job.reprompted","created_at":"` + time.Now().UTC().Format(time.RFC3339Nano) + `","payload":{"pane_id":"workspace:pane-1"}}`
	if err := os.WriteFile(filepath.Join(rig.events(), rig.nextName("job.reprompted")), []byte(reprompt), 0600); err != nil {
		t.Fatal(err)
	}
	rig.manager.Tick(context.Background(), rig.now.Add(3*time.Minute))
	rig.wantDecision(t, "handover", "wait_lost")
	if rig.sends != 0 {
		t.Fatalf("resumed into another attempt: sends=%d", rig.sends)
	}
}

// A second stop after the one resume is handed over: a job waits once.
func TestQuotaReviewerSecondStopAfterTheResumeIsHandedOver(t *testing.T) {
	rig := newWaitRig(t)
	rig.decide()
	rig.ack(t)
	rig.manager.Tick(context.Background(), rig.now.Add(6*time.Second))
	rig.manager.Tick(context.Background(), rig.now.Add(3*time.Minute))
	if rig.sends != 1 {
		t.Fatalf("sends=%d, want the one resume", rig.sends)
	}
	var waits int
	if err := rig.store.db.QueryRow(`SELECT count(*) FROM quota_stop_decisions WHERE job='job-active' AND attempted=1`).Scan(&waits); err != nil || waits != 1 {
		t.Fatalf("recorded resumes=%d err=%v", waits, err)
	}
	decision, reason := quotaDecisionReviewer(quotaPolicy{Expected: time.Hour, Urgent: map[int]bool{}, MaxWaits: 1, ReviewerWait: 10 * time.Minute}, true, 3, time.Now().Add(-time.Hour), 4*time.Hour, waits, quotastop.Stop{Pool: "devin-free", Reset: rig.now.Add(time.Minute)}, rig.now)
	if decision != "handover" || reason != "resume_limit" {
		t.Fatalf("a second stop was %s %s, want handover resume_limit", decision, reason)
	}
}

// The reviewer wait bound is the policy's reviewer_wait, never above the ceiling
// or max_wait; every other stop is handed over without a wait.
func TestQuotaDecisionReviewerBounds(t *testing.T) {
	now := time.Now().UTC()
	policy := quotaPolicy{Expected: 2 * time.Hour, Urgent: map[int]bool{1: true}, MaxWaits: 1, ReviewerWait: 30 * time.Minute}
	started := now.Add(-time.Hour)
	for name, tc := range map[string]struct {
		policy  quotaPolicy
		ok      bool
		prio    int
		waits   int
		stop    quotastop.Stop
		started time.Time
		want    string
	}{
		"short reset":               {policy, true, 3, 0, quotastop.Stop{Reset: now.Add(5 * time.Minute)}, started, "wait reset_within_reviewer_wait"},
		"reset passed":              {policy, true, 3, 0, quotastop.Stop{Reset: now.Add(-time.Minute)}, started, "wait reset_passed"},
		"beyond the ceiling":        {policy, true, 3, 0, quotastop.Stop{Reset: now.Add(20 * time.Minute)}, started, "handover reset_beyond_reviewer_wait"},
		"urgent":                    {policy, true, 1, 0, quotastop.Stop{Reset: now.Add(5 * time.Minute)}, started, "handover urgent"},
		"unknown reset":             {policy, true, 3, 0, quotastop.Stop{}, started, "handover reset_unknown"},
		"dialog":                    {policy, true, 3, 0, quotastop.Stop{Reset: now.Add(5 * time.Minute), Choice: true}, started, "handover choice_dialog"},
		"policy unavailable":        {policy, false, 3, 0, quotastop.Stop{Reset: now.Add(5 * time.Minute)}, started, "handover policy_unavailable"},
		"past the review deadline":  {policy, true, 3, 0, quotastop.Stop{Reset: now.Add(5 * time.Minute)}, now.Add(-3 * time.Hour), "handover reset_past_review_deadline"},
		"max_wait below the reset":  {quotaPolicy{Expected: 2 * time.Hour, Urgent: map[int]bool{}, MaxWaits: 1, ReviewerWait: 30 * time.Minute, MaxWait: 2 * time.Minute}, true, 3, 0, quotastop.Stop{Reset: now.Add(5 * time.Minute)}, started, "handover reset_beyond_reviewer_wait"},
		"start unknown":             {policy, true, 3, 0, quotastop.Stop{Reset: now.Add(5 * time.Minute)}, time.Time{}, "handover start_unknown"},
		"a resume already happened": {policy, true, 3, 1, quotastop.Stop{Reset: now.Add(5 * time.Minute)}, started, "handover resume_limit"},
	} {
		t.Run(name, func(t *testing.T) {
			decision, reason := quotaDecisionReviewer(tc.policy, tc.ok, tc.prio, tc.started, 0, tc.waits, tc.stop, now)
			if got := strings.Join([]string{decision, reason}, " "); got != tc.want {
				t.Fatalf("%s, want %s", got, tc.want)
			}
		})
	}
}

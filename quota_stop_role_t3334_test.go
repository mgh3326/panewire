package panewire

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// roleStopRig is a worker or tester job on a quota stop screen, with the
// quota-stop manager in SHADOW mode (--quota-stop-act off): hk 3334 has the
// manager act for these roles regardless.
type roleStopRig struct {
	*idleWakeTestRig
	q         *quotaStopManager
	logs      bytes.Buffer
	sends     int
	announce  []quotastop.Stop
	announced []string
	owner     []hubScannedRelayEvent
}

func newRoleStopRig(t *testing.T, role string, startedAgo time.Duration, task string) *roleStopRig {
	t.Helper()
	rig := &roleStopRig{idleWakeTestRig: newIdleWakeTestRig(t, time.Second)}
	// Jobs older than the active window are not found, so the rig runs on the
	// real clock.
	rig.now = time.Now().UTC().Truncate(time.Second)
	root := rig.manager.inboxRoot
	events := filepath.Join(root, "jobs", "job-active", "events")
	if err := os.MkdirAll(events, 0700); err != nil {
		t.Fatal(err)
	}
	started := rig.now.Add(-startedAgo).UTC().Format(time.RFC3339)
	for name, body := range map[string]string{
		"00001-job.claimed.json": `{"type":"job.claimed","created_at":"` + started + `","agent_label":"worker-synthetic","owner_lane":"owner","role":"worker","task_id":"42","epoch":1}`,
		"00002-job.spawned.json": `{"type":"job.spawned","created_at":"` + started + `","pane_id":"workspace:pane-1","role":"` + role + `","task_id":"42"}`} {
		if err := os.WriteFile(filepath.Join(events, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	q, err := newQuotaStopManager(rig.store, root, false, slog.New(slog.NewTextHandler(&rig.logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	rig.q = q
	rig.manager.quota = q
	q.enqueue = func(e hubScannedRelayEvent) bool {
		rig.owner = append(rig.owner, e)
		return true
	}
	screen, err := os.ReadFile("testdata/quota-stop/devin.txt")
	if err != nil {
		t.Fatal(err)
	}
	q.read = func(context.Context, string) (string, error) { return string(screen), nil }
	policy := "## Values\n- default_expected: 1d\n- urgent_priorities: [1, 2]\n- max_waits: 1\n- resume_text: \"continue\"\n"
	q.hk = quotaStopHKFixture(quotaPolicyAnswer(policy), task, nil)
	q.send = func(_ context.Context, pane, pool, text string) relayInjectResult {
		rig.sends++
		return relayInjectResult{Outcome: relayInjectDelivered, Evidence: "visible:nonce_echo", Proven: relayNoncesIn(text)}
	}
	q.announce = func(_ context.Context, job, pane string, stop quotastop.Stop) error {
		rig.announce = append(rig.announce, stop)
		rig.announced = append(rig.announced, job+" "+pane)
		return nil
	}
	return rig
}

// run takes the pane through working -> idle on the stop screen and ticks past
// its reset.
func (rig *roleStopRig) run() {
	rig.observe("working", 1, rig.now)
	rig.observe("idle", 2, rig.now.Add(time.Second))
	rig.manager.Tick(context.Background(), rig.now.Add(3*time.Second))
	rig.route(0, "owner")
	rig.manager.Tick(context.Background(), rig.now.Add(4*time.Second))
	rig.manager.Tick(context.Background(), rig.now.Add(5*time.Second))
	rig.manager.Tick(context.Background(), rig.now.Add(3*time.Minute))
}

func (rig *roleStopRig) decision(t *testing.T) (decision, reason string) {
	t.Helper()
	if err := rig.store.db.QueryRow(`SELECT decision,reason FROM quota_stop_decisions WHERE decision<>'' ORDER BY created LIMIT 1`).Scan(&decision, &reason); err != nil {
		t.Fatal(err)
	}
	return
}

const roleStopTask = `{"tasks":[{"priority":3,"expected":"4h","refs":{"job_id":"job-active"}}]}`

// (b) hk 3334: a worker or tester whose reset is inside its expected time
// waits, is resumed once at the reset, and the job is not lost, with
// --quota-stop-act off.
func TestQuotaStopWorkerWithinExpectedWaitsAndResumesOnce(t *testing.T) {
	for _, role := range []string{"worker", "tester"} {
		t.Run(role, func(t *testing.T) {
			rig := newRoleStopRig(t, role, time.Hour, roleStopTask)
			rig.run()
			if d, reason := rig.decision(t); d != "wait" || reason != "reset_within_expected" {
				t.Fatalf("decision=%s reason=%s", d, reason)
			}
			if rig.sends != 1 || len(rig.announce) != 0 {
				t.Fatalf("sends=%d announced=%d, want one resume and no hand-over (the job is not lost)", rig.sends, len(rig.announce))
			}
			rig.manager.Tick(context.Background(), rig.now.Add(10*time.Minute))
			if rig.sends != 1 {
				t.Fatalf("resumed more than once: %d", rig.sends)
			}
			var info int
			for _, e := range rig.owner {
				if e.OwnerLane == "owner" && e.NotifyType == "info" && strings.Contains(e.Text, "resumed job-active") {
					info++
				}
			}
			if info != 1 {
				t.Fatalf("owner info events=%d, want one: %+v", info, rig.owner)
			}
		})
	}
}

// (c) A worker whose reset is beyond its expected time, or has no expected
// time, or is urgent, is handed over: job.lost is announced for the hub to
// replace it. The same job is not resumed.
func TestQuotaStopWorkerBeyondExpectedIsHandedOver(t *testing.T) {
	for name, tc := range map[string]struct {
		startedAgo time.Duration
		task       string
		reason     string
	}{
		"reset after start plus expected": {5 * time.Hour, roleStopTask, "reset_too_late"},
		"expected missing":                {time.Hour, `{"tasks":[{"priority":3,"refs":{"job_id":"job-active"}}]}`, "expected_missing"},
		"priority 1":                      {time.Hour, `{"tasks":[{"priority":1,"expected":"4h","refs":{"job_id":"job-active"}}]}`, "urgent"},
	} {
		t.Run(name, func(t *testing.T) {
			rig := newRoleStopRig(t, "worker", tc.startedAgo, tc.task)
			rig.run()
			if d, reason := rig.decision(t); d != "handover" || reason != tc.reason {
				t.Fatalf("decision=%s reason=%s, want handover %s", d, reason, tc.reason)
			}
			if rig.sends != 0 || len(rig.announce) != 1 || rig.announced[0] != "job-active workspace:pane-1" || rig.announce[0].Pool != "devin-free" {
				t.Fatalf("sends=%d announced=%v %+v", rig.sends, rig.announced, rig.announce)
			}
			rig.manager.Tick(context.Background(), rig.now.Add(10*time.Minute))
			if len(rig.announce) != 1 {
				t.Fatalf("hand-over announced again: %d", len(rig.announce))
			}
		})
	}
}

// Other roles stay observe-only in shadow mode: no input, no hand-over. The
// claim says worker for every spawned job; the spawn receipt's role decides
// (a reviewer is not resumed like a worker).
func TestQuotaStopOtherRolesStayShadow(t *testing.T) {
	for _, role := range []string{"reviewer", "counsel"} {
		t.Run("role="+role, func(t *testing.T) {
			rig := newRoleStopRig(t, role, 5*time.Hour, roleStopTask)
			rig.run()
			if rig.sends != 0 || len(rig.announce) != 0 {
				t.Fatalf("shadow mode acted for role %q: sends=%d announced=%d", role, rig.sends, len(rig.announce))
			}
		})
	}
}

// A screen the manager could not read is no proof of a stop: a worker is
// never handed over on it, even though the manager acts for workers.
func TestQuotaStopWorkerReadFailureIsNotHandedOver(t *testing.T) {
	rig := newRoleStopRig(t, "worker", time.Hour, roleStopTask)
	rig.q.read = func(context.Context, string) (string, error) { return "", os.ErrNotExist }
	rig.run()
	if len(rig.announce) != 0 || rig.sends != 0 {
		t.Fatalf("acted without a quota screen: sends=%d announced=%d", rig.sends, len(rig.announce))
	}
}

// A tester inherits its builder's task: the task is bound to the builder's job,
// so the manager finds it through the claim's task_inherited_from and decides
// on its expected time, not task_unavailable (review finding).
func TestQuotaStopTesterFindsItsInheritedTask(t *testing.T) {
	rig := newRoleStopRig(t, "tester", time.Hour, `{"tasks":[{"priority":3,"expected":"4h","refs":{"job_id":"builder-job"}}]}`)
	events := filepath.Join(rig.manager.inboxRoot, "jobs", "job-active", "events")
	started := rig.now.Add(-time.Hour).UTC().Format(time.RFC3339)
	claim := `{"type":"job.claimed","created_at":"` + started + `","agent_label":"worker-synthetic","owner_lane":"owner","role":"worker","task_inherited_from":"builder-job","epoch":1}`
	if err := os.WriteFile(filepath.Join(events, "00001-job.claimed.json"), []byte(claim), 0600); err != nil {
		t.Fatal(err)
	}
	rig.run()
	if d, reason := rig.decision(t); d != "wait" || reason != "reset_within_expected" {
		t.Fatalf("decision=%s reason=%s, want wait reset_within_expected", d, reason)
	}
	if rig.sends != 1 || len(rig.announce) != 0 {
		t.Fatalf("sends=%d announced=%d", rig.sends, len(rig.announce))
	}
}

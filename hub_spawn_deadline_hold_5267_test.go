package panewire

import (
	"context"
	"encoding/json"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// spawnOutboxRow is one outbox row of a request as the lanes would read it.
type spawnOutboxRow struct {
	ID, Text string
}

// spawnOutbox returns every outbox row of request, delivered or not, rendered
// by the same function the flush uses. A test reads what the owner would get.
func spawnOutbox(t *testing.T, s *laneInboxStore, request string) []spawnOutboxRow {
	t.Helper()
	rows, err := s.db.Query(`SELECT event_id,job,owner,result FROM spawn_notifications WHERE request_id=? ORDER BY created_ms,event_id`, request)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []spawnOutboxRow
	for rows.Next() {
		var n spawnNotification
		if err = rows.Scan(&n.id, &n.job, &n.owner, &n.raw); err != nil {
			t.Fatal(err)
		}
		n.request = request
		var outcome hubSpawnOutcome
		if err = json.Unmarshal([]byte(n.raw), &outcome); err != nil {
			t.Fatal(err)
		}
		out = append(out, spawnOutboxRow{ID: n.id, Text: spawnNotificationEvent(n, outcome).Text})
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// spawnOutboxCount counts the outbox rows of request whose event id is id.
func spawnOutboxCount(t *testing.T, s *laneInboxStore, request, id string) int {
	t.Helper()
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM spawn_notifications WHERE request_id=? AND event_id=?`, request, id).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

// A bound deadline is held: reaching it notifies nothing, a late success tells
// only SPAWNED (no correction, nothing was said), and a final failure tells one
// SPAWN_FAILED with that failure (hk 5267 acceptance).
func TestBoundDeadlineHoldsUntilFinal_5267(t *testing.T) {
	t.Run("late success is only SPAWNED", func(t *testing.T) {
		h, _, _ := spawnNotifyHubLanes(t, "", "parent")
		task, _ := bindDispatch(t, h, durableSpawnRequest())
		s := h.inbox.store
		if _, err := s.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Host: "machine-a"}, time.Now()); err != nil {
			t.Fatal(err)
		}
		if rows := spawnOutbox(t, s, task.Request.RequestID); len(rows) != 0 {
			t.Fatalf("a bound deadline wrote notices before it was final: %+v", rows)
		}
		success := hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "w9:p1", Profile: "codex-sol"}
		if changed, err := s.completeSpawn(t.Context(), task, success, time.Now()); err != nil || !changed {
			t.Fatalf("late success changed=%v err=%v", changed, err)
		}
		rows := spawnOutbox(t, s, task.Request.RequestID)
		if len(rows) != 1 || !strings.Contains(rows[0].Text, "SPAWNED") || strings.Contains(rows[0].Text, "unresolved") || strings.Contains(rows[0].Text, "SPAWN_FAILED") {
			t.Fatalf("late success notices = %+v, want one plain SPAWNED", rows)
		}
	})
	t.Run("final failure is one SPAWN_FAILED with that failure", func(t *testing.T) {
		h, _, _ := spawnNotifyHubLanes(t, "", "parent")
		task, _ := bindDispatch(t, h, durableSpawnRequest())
		s := h.inbox.store
		if _, err := s.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Host: "machine-a"}, time.Now()); err != nil {
			t.Fatal(err)
		}
		failed := hubSpawnOutcome{RC: 1, Stage: "landing", Reason: "spawn_failed", Host: "machine-a"}
		if changed, err := s.completeSpawn(t.Context(), task, failed, time.Now()); err != nil || !changed {
			t.Fatalf("final failure changed=%v err=%v", changed, err)
		}
		rows := spawnOutbox(t, s, task.Request.RequestID)
		if len(rows) != 1 || !strings.Contains(rows[0].Text, "SPAWN_FAILED") || !strings.Contains(rows[0].Text, "reason=spawn_failed") {
			t.Fatalf("final failure notices = %+v, want one SPAWN_FAILED with its reason", rows)
		}
	})
}

// An unbound timeout never reaches deadline_exceeded: completeSpawn forces its
// retryable flag, so the hold (which needs a node binding) cannot touch it and
// no unresolved notice is ever queued for it (hk 5267).
func TestUnboundTimeoutIsNeverHeldAsDeadline_5267(t *testing.T) {
	s := openSpawnStore(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	req := durableSpawnRequest()
	task, _, err := s.claimSpawn(t.Context(), req, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	got, err := s.spawnTask(t.Context(), req.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status == "deadline_exceeded" {
		t.Fatalf("an unbound timeout is held as deadline_exceeded: %+v", got)
	}
	if n := spawnOutboxCount(t, s, req.RequestID, spawnUnresolvedEventID(task)); n != 0 {
		t.Fatalf("unbound timeout queued an unresolved notice: %d", n)
	}
}

// The unresolved notice is queued once per attempt at the horizon, and a
// landing that follows is one correcting SPAWNED under the same request id.
func TestUnresolvedThenCorrectedOnceEach_5267(t *testing.T) {
	h, _, _ := spawnNotifyHubLanes(t, "", "parent", "submitter")
	task, _ := bindDispatch(t, h, reconcilingRequest())
	s := h.inbox.store
	request := task.Request.RequestID
	if _, err := s.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if queued, err := s.noteUnresolvedSpawn(t.Context(), task, time.Now()); err != nil || !queued {
			t.Fatalf("unresolved queued=%v err=%v", queued, err)
		}
	}
	unresolved := spawnUnresolvedEventID(task)
	if n := spawnOutboxCount(t, s, request, unresolved); n != 1 {
		t.Fatalf("unresolved notices for the owner = %d, want 1", n)
	}
	if n := spawnOutboxCount(t, s, request, unresolved+":submitter"); n != 1 {
		t.Fatalf("unresolved notices for the submitter = %d, want 1", n)
	}
	rows := spawnOutbox(t, s, request)
	for _, row := range rows {
		for _, want := range []string{"SPAWN_FAILED", "request=" + request, "rc=124", "stage=landing", "reason=deadline_exceeded_unresolved", "replay=no"} {
			if !strings.Contains(row.Text, want) {
				t.Fatalf("unresolved notice %q lacks %q", row.Text, want)
			}
		}
	}
	if got := reconcilingStatus(t, h, task); got != "deadline_exceeded" {
		t.Fatalf("unresolved changed ownership: status %q", got)
	}

	success := hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "w9:p1", Profile: "codex-sol"}
	if changed, err := s.completeSpawn(t.Context(), task, success, time.Now()); err != nil || !changed {
		t.Fatalf("late landing changed=%v err=%v", changed, err)
	}
	// One correction per recipient: the owner's, and the submitter's, which
	// was told the unresolved failure too (hk 5267 review).
	landed := map[string]spawnOutboxRow{}
	for _, row := range spawnOutbox(t, s, request) {
		if strings.HasPrefix(row.Text, "[panewire] SPAWNED ") {
			if _, dup := landed[row.ID]; dup {
				t.Fatalf("more than one SPAWNED row for %s: %+v", row.ID, row)
			}
			landed[row.ID] = row
		}
	}
	if len(landed) != 2 {
		t.Fatalf("correcting SPAWNED rows = %+v, want the owner's and the submitter's", landed)
	}
	for id, row := range landed {
		if !strings.Contains(row.Text, "request="+request) || !strings.Contains(row.Text, "(after an earlier unresolved notice)") {
			t.Fatalf("correcting SPAWNED %s = %+v, want request=%s and the correction marker", id, row, request)
		}
	}
	if changed, err := s.completeSpawn(t.Context(), task, success, time.Now()); err != nil || changed {
		t.Fatalf("duplicate landing changed=%v err=%v", changed, err)
	}
	if queued, err := s.noteUnresolvedSpawn(t.Context(), task, time.Now()); err != nil || queued {
		t.Fatalf("unresolved notice after the landing queued=%v err=%v", queued, err)
	}
	if n := spawnOutboxCount(t, s, request, request+":1:succeeded"); n != 1 {
		t.Fatalf("succeeded rows = %d, want 1", n)
	}
}

// A success that lands with no unresolved notice is plain: only the stale-probe
// horizon can make the correction, so a landing in time never carries it.
func TestLandingBeforeHorizonIsPlainSpawned_5267(t *testing.T) {
	h, _, _ := spawnNotifyHubLanes(t, "", "parent")
	task, _ := bindDispatch(t, h, durableSpawnRequest())
	s := h.inbox.store
	if _, err := s.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "w9:p1", Profile: "codex-sol"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	rows := spawnOutbox(t, s, task.Request.RequestID)
	if len(rows) != 1 || strings.Contains(rows[0].Text, "after an earlier") {
		t.Fatalf("notices = %+v, want one plain SPAWNED", rows)
	}
}

// A late success that commits first leaves no unresolved notice to queue: the
// horizon's claim on the row fails, so the owner is told only SPAWNED.
func TestSuccessFirstSuppressesUnresolved_5267(t *testing.T) {
	h, _, _ := spawnNotifyHubLanes(t, "", "parent")
	task, _ := bindDispatch(t, h, durableSpawnRequest())
	s := h.inbox.store
	if _, err := s.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "w9:p1", Profile: "codex-sol"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if queued, err := s.noteUnresolvedSpawn(t.Context(), task, time.Now()); err != nil || queued {
		t.Fatalf("unresolved after a landing queued=%v err=%v", queued, err)
	}
	if n := spawnOutboxCount(t, s, task.Request.RequestID, spawnUnresolvedEventID(task)); n != 0 {
		t.Fatalf("unresolved rows after a landing = %d", n)
	}
}

// Restart between the steps: the unresolved notice and its correction each
// stay once after the store is reopened and the steps run again.
func TestUnresolvedAndCorrectionSurviveRestart_5267(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	s := openSpawnStore(t, path)
	req := durableSpawnRequest()
	task, _, err := s.claimSpawn(t.Context(), req, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	node := hubSpawnRequest{RequestID: "d676220b-e763-4af1-aefd-000000000011", Machine: "machine-a", Args: []string{"--job", "test-job"}, WaitSeconds: 600}
	bound, err := s.bindSpawn(t.Context(), req.RequestID, task.Attempt, node, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	task = sentSpawnFixture(t, s, bound)
	if _, err = s.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if queued, err := s.noteUnresolvedSpawn(t.Context(), task, time.Now()); err != nil || !queued {
		t.Fatalf("unresolved queued=%v err=%v", queued, err)
	}

	recovered := openSpawnStore(t, path)
	again, err := recovered.spawnTask(t.Context(), req.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if queued, err := recovered.noteUnresolvedSpawn(t.Context(), again, time.Now()); err != nil || !queued {
		t.Fatalf("restarted unresolved queued=%v err=%v", queued, err)
	}
	if n := spawnOutboxCount(t, recovered, req.RequestID, spawnUnresolvedEventID(task)); n != 1 {
		t.Fatalf("unresolved rows after restart = %d, want 1", n)
	}
	success := hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "pane-1"}
	if changed, err := recovered.completeSpawn(t.Context(), again, success, time.Now()); err != nil || !changed {
		t.Fatalf("late landing after restart changed=%v err=%v", changed, err)
	}
	if changed, err := recovered.completeSpawn(t.Context(), again, success, time.Now()); err != nil || changed {
		t.Fatalf("duplicate landing after restart changed=%v err=%v", changed, err)
	}
	corrected := 0
	for _, row := range spawnOutbox(t, recovered, req.RequestID) {
		if strings.HasPrefix(row.Text, "[panewire] SPAWNED ") && strings.Contains(row.Text, "(after an earlier unresolved notice)") {
			corrected++
		}
	}
	if corrected != 1 {
		t.Fatalf("correcting notices = %d, want exactly one", corrected)
	}
}

// The coordinator's stale probe is the horizon: a bound request whose deadline
// passed 31 minutes ago, with its node gone and no pane, gets the unresolved
// notice once across repeated probes, and its late landing one correction.
func TestStrandedSpawnHorizonNoticesOnceThenCorrected_5267(t *testing.T) {
	s := newStrandedSpawnClaimedAgo(t, 41*time.Minute)
	store := s.hub.inbox.store
	request := s.task.Request.RequestID
	if rows := spawnOutbox(t, store, request); len(rows) != 0 {
		t.Fatalf("a bound deadline notified before the horizon: %+v", rows)
	}
	s.run(t)()
	if got := s.status(t); got.Status != "deadline_exceeded" {
		t.Fatalf("horizon probe changed ownership: status=%s", got.Status)
	}
	unresolved := spawnUnresolvedEventID(s.task)
	if n := spawnOutboxCount(t, store, request, unresolved); n != 1 {
		t.Fatalf("unresolved rows after the horizon = %d, want 1", n)
	}
	s.run(t)()
	if n := spawnOutboxCount(t, store, request, unresolved); n != 1 {
		t.Fatalf("unresolved rows after a second probe = %d, want 1", n)
	}
	success := hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "pane-1", Profile: "codex-sol"}
	if changed, err := store.completeSpawn(t.Context(), s.status(t), success, time.Now()); err != nil || !changed {
		t.Fatalf("late landing changed=%v err=%v", changed, err)
	}
	corrected := 0
	for _, row := range spawnOutbox(t, store, request) {
		if strings.HasPrefix(row.Text, "[panewire] SPAWNED ") && strings.Contains(row.Text, "request="+request) && strings.Contains(row.Text, "(after an earlier unresolved notice)") {
			corrected++
		}
	}
	if corrected != 1 {
		t.Fatalf("correcting SPAWNED notices = %d, want 1", corrected)
	}
}

// completedJobStore serves the job record the hub reads for the stranded job.
// The stock roleJobs double returns only the state, and the completed-job
// proof needs the host, pane and task id.
type completedJobStore struct {
	*roleJobs
	full    map[string]any
	parents map[string]map[string]any
}

func (c *completedJobStore) Get(ctx context.Context, id string) (map[string]any, error) {
	if record, ok := c.parents[id]; ok {
		return record, nil
	}
	if id == "test-job" && c.full != nil {
		return c.full, nil
	}
	return c.roleJobs.Get(ctx, id)
}

// completedJobRecord is hk's record of the stranded job once it finished on
// machine-a, pane w9:p4, under the request's task 42.
func completedJobRecord(host string) map[string]any {
	return map[string]any{"state": "completed", "completed_at": time.Now().UTC().Format(time.RFC3339Nano), "host": host, "pane_id": "w9:p4", "label": "test-job", "profile": "codex-sol", "model": "codex-sol-model", "task_id": 42}
}

// useJobStore points the stranded hub's spawn app at store.
func useJobStore(s *strandedSpawn, store fleet.JobStore) {
	s.hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: store, Out: out, Err: errout}, nil
	}
}

// A bound request whose job completed (no receipt reached the hub) settles as
// succeeded at the next probe, before the horizon, with one plain SPAWNED.
func TestStrandedCompletedJobSettlesBeforeHorizon_5267(t *testing.T) {
	s := newStrandedSpawnClaimedAgo(t, 11*time.Minute)
	store := s.hub.inbox.store
	request := s.task.Request.RequestID
	jobs := &completedJobStore{roleJobs: s.jobs, full: completedJobRecord("machine-a")}
	useJobStore(s, jobs)
	s.run(t)()
	if got := s.status(t); got.Status != "succeeded" || got.Outcome.Pane != "w9:p4" {
		t.Fatalf("completed job did not settle the request: status=%s outcome=%+v", got.Status, got.Outcome)
	}
	rows := spawnOutbox(t, store, request)
	if len(rows) != 1 || !strings.HasPrefix(rows[0].Text, "[panewire] SPAWNED ") || strings.Contains(rows[0].Text, "after an earlier") {
		t.Fatalf("notices = %+v, want one plain SPAWNED", rows)
	}
}

// A completed job on another host is not this request's landing.
func TestStrandedCompletedJobOnAnotherHostDoesNotSettle_5267(t *testing.T) {
	s := newStrandedSpawnClaimedAgo(t, 11*time.Minute)
	useJobStore(s, &completedJobStore{roleJobs: s.jobs, full: completedJobRecord("machine-b")})
	s.run(t)()
	if got := s.status(t); got.Status != "deadline_exceeded" {
		t.Fatalf("a foreign completed job settled the request: status=%s", got.Status)
	}
	if rows := spawnOutbox(t, s.hub.inbox.store, s.task.Request.RequestID); len(rows) != 0 {
		t.Fatalf("a foreign completed job wrote notices: %+v", rows)
	}
}

// The second case: the owner was told unresolved at the horizon, then the job
// completed. The next probe settles the request as succeeded and the earlier
// notice is corrected by one SPAWNED under the same request id.
func TestStrandedCompletedJobAfterHorizonCorrectsUnresolved_5267(t *testing.T) {
	s := newStrandedSpawnClaimedAgo(t, 41*time.Minute)
	store := s.hub.inbox.store
	request := s.task.Request.RequestID
	jobs := &completedJobStore{roleJobs: s.jobs}
	useJobStore(s, jobs)
	s.run(t)()
	unresolved := spawnUnresolvedEventID(s.task)
	if n := spawnOutboxCount(t, store, request, unresolved); n != 1 {
		t.Fatalf("unresolved rows at the horizon = %d, want 1", n)
	}
	jobs.full = completedJobRecord("machine-a")
	s.run(t)()
	if got := s.status(t); got.Status != "succeeded" {
		t.Fatalf("completed job after the horizon left status=%s", got.Status)
	}
	corrected := 0
	for _, row := range spawnOutbox(t, store, request) {
		if strings.HasPrefix(row.Text, "[panewire] SPAWNED ") && strings.Contains(row.Text, "request="+request) && strings.Contains(row.Text, "(after an earlier unresolved notice)") {
			corrected++
		}
	}
	if corrected != 1 {
		t.Fatalf("correcting SPAWNED notices = %d, want 1", corrected)
	}
	if n := spawnOutboxCount(t, store, request, unresolved); n != 1 {
		t.Fatalf("unresolved rows after the correction = %d, want 1", n)
	}
}

// A correction waits while the unresolved notice it corrects is undelivered for
// the same recipient, and it reaches the submitter who was told the failure.
// The owner's relay is held for two passes, while the submitter's and the
// success land (hk 5267 review).
func TestCorrectionWaitsForItsUnresolvedAndReachesSubmitter_5267(t *testing.T) {
	h, _, destination := spawnNotifyHubLanes(t, "", "parent", "submitter")
	task, _ := bindDispatch(t, h, reconcilingRequest())
	s := h.inbox.store
	request := task.Request.RequestID
	ownerUnresolved := spawnUnresolvedEventID(task)
	holdOwner := true
	h.spawnNoticeRelayFilter = func(id string) bool { return holdOwner && id == ownerUnresolved }
	if _, err := s.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if queued, err := s.noteUnresolvedSpawn(t.Context(), task, time.Now()); err != nil || !queued {
		t.Fatalf("unresolved queued=%v err=%v", queued, err)
	}
	first := deliveredSpawnTexts(t, h, destination)
	if len(first) != 1 || first[0].Pane != "w1:p2" || !strings.Contains(first[0].Text, "deadline_exceeded_unresolved") {
		t.Fatalf("first pass = %+v, want the submitter's unresolved notice only", first)
	}
	if _, err := s.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "w9:p1", Profile: "codex-sol"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	// The owner's unresolved notice is still held, so its correction must wait.
	second := deliveredSpawnTexts(t, h, destination)
	if len(second) != 1 || second[0].Pane != "w1:p2" || !strings.Contains(second[0].Text, "after an earlier unresolved notice") {
		t.Fatalf("second pass = %+v, want only the submitter's correction", second)
	}
	holdOwner = false
	third := deliveredSpawnTexts(t, h, destination)
	if len(third) != 2 || third[0].Pane != "w1:p1" || third[1].Pane != "w1:p1" ||
		!strings.Contains(third[0].Text, "deadline_exceeded_unresolved") || !strings.Contains(third[1].Text, "after an earlier unresolved notice") ||
		!strings.Contains(third[1].Text, "request="+request) {
		t.Fatalf("third pass = %+v, want the owner's unresolved notice then its correction", third)
	}
}

// A task id from a job record renders as a plain decimal, so a large id
// matches its --task argument (CodeRabbit review on hk 5267).
func TestSpawnRecordTaskIDIsDecimal_5267(t *testing.T) {
	for _, tc := range []struct {
		value any
		want  string
	}{{float64(42), "42"}, {float64(1000000), "1000000"}, {"7", "7"}, {nil, ""}} {
		if got := spawnRecordTaskID(map[string]any{"task_id": tc.value}); got != tc.want {
			t.Fatalf("task_id %v = %q, want %q", tc.value, got, tc.want)
		}
	}
}

// A completed job settles a request whose --task is a large id (float64 in the
// job record), as the earlier completed-job test does for a small one.
func TestStrandedCompletedJobLargeTaskIDSettles_5267(t *testing.T) {
	s := newStrandedSpawnClaimedAgo(t, 11*time.Minute)
	for i, arg := range s.task.Request.Args {
		if arg == "--task" && i+1 < len(s.task.Request.Args) {
			s.task.Request.Args[i+1] = "1000000"
		}
	}
	record := completedJobRecord("machine-a")
	record["task_id"] = float64(1000000)
	useJobStore(s, &completedJobStore{roleJobs: s.jobs, full: record})
	s.run(t)()
	if got := s.status(t); got.Status != "succeeded" {
		t.Fatalf("large task id did not settle: status=%s", got.Status)
	}
}

// Without a --task on the request there is no task identity to check a
// completed job against, so the job does not settle the request (hk 5267 review).
func TestStrandedCompletedJobWithoutTaskArgRefuses_5267(t *testing.T) {
	s := newStrandedSpawnClaimedAgo(t, 11*time.Minute)
	args := []string{}
	for i := 0; i < len(s.task.Request.Args); i++ {
		if s.task.Request.Args[i] == "--task" {
			i++
			continue
		}
		args = append(args, s.task.Request.Args[i])
	}
	s.task.Request.Args = args
	useJobStore(s, &completedJobStore{roleJobs: s.jobs, full: completedJobRecord("machine-a")})
	s.run(t)()
	if got := s.status(t); got.Status != "deadline_exceeded" {
		t.Fatalf("a request without --task settled from a job: status=%s", got.Status)
	}
}

// A --parent-job child settles from the parent it names in its own record,
// even when the parent exists only in the node's local ledger and hk has no
// parent record (mergewake review, hk 5267). A child whose record names a
// different parent does not settle.
func TestStrandedCompletedJobParentJobIdentity_5267(t *testing.T) {
	childArgs := func(s *strandedSpawn) []string {
		var args []string
		for i := 0; i < len(s.task.Request.Args); i++ {
			if s.task.Request.Args[i] == "--task" {
				i++
				continue
			}
			args = append(args, s.task.Request.Args[i])
		}
		return append(args, "--parent-job", "parent-1")
	}
	t.Run("local-only parent settles", func(t *testing.T) {
		s := newStrandedSpawnClaimedAgo(t, 11*time.Minute)
		s.task.Request.Args = childArgs(s)
		record := completedJobRecord("machine-a")
		record["parent_job"] = "parent-1"
		useJobStore(s, &completedJobStore{roleJobs: s.jobs, full: record})
		s.run(t)()
		if got := s.status(t); got.Status != "succeeded" {
			t.Fatalf("local-only parent child did not settle: status=%s", got.Status)
		}
	})
	t.Run("child names another parent", func(t *testing.T) {
		s := newStrandedSpawnClaimedAgo(t, 11*time.Minute)
		s.task.Request.Args = childArgs(s)
		record := completedJobRecord("machine-a")
		record["parent_job"] = "parent-2"
		useJobStore(s, &completedJobStore{roleJobs: s.jobs, full: record})
		s.run(t)()
		if got := s.status(t); got.Status != "deadline_exceeded" {
			t.Fatalf("a child of another parent settled the request: status=%s", got.Status)
		}
	})
}

// A free-text flag value that looks like --task=<id> does not shadow the
// request's real --task (mergewake review, hk 5267): the strict flag reader
// skips the value of --purpose.
func TestStrandedCompletedJobTaskFlagNotShadowed_5267(t *testing.T) {
	s := newStrandedSpawnClaimedAgo(t, 11*time.Minute)
	s.task.Request.Args = append([]string{"--purpose", "--task=999"}, s.task.Request.Args...)
	useJobStore(s, &completedJobStore{roleJobs: s.jobs, full: completedJobRecord("machine-a")})
	s.run(t)()
	if got := s.status(t); got.Status != "succeeded" {
		t.Fatalf("a --task= value in free text refused a valid landing: status=%s", got.Status)
	}
}

// A lookup error holds a correction; it is never released on the error
// (mergewake review, hk 5267). A canceled read stands in for a failed lookup.
func TestCorrectionHoldsOnLookupError_5267(t *testing.T) {
	h, _, _ := spawnNotifyHubLanes(t, "", "parent")
	task, _ := bindDispatch(t, h, durableSpawnRequest())
	s := h.inbox.store
	if _, err := s.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if queued, err := s.noteUnresolvedSpawn(t.Context(), task, time.Now()); err != nil || !queued {
		t.Fatalf("unresolved queued=%v err=%v", queued, err)
	}
	if _, err := s.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "w9:p1", Profile: "codex-sol"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	var raw string
	if err := s.db.QueryRow(`SELECT result FROM spawn_notifications WHERE event_id=?`, task.Request.RequestID+":1:succeeded").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	correction := spawnNotification{id: task.Request.RequestID + ":1:succeeded", request: task.Request.RequestID, raw: raw}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if !h.correctionAwaitsUnresolved(canceled, correction) {
		t.Fatal("a lookup error released the correction before its unresolved notice was delivered")
	}
	if h.correctionAwaitsUnresolved(t.Context(), correction) == false {
		t.Fatal("the correction was released while its unresolved notice is still pending")
	}
}

// A task id at or past 2^53 is refused rather than matched: the job record's
// float64 may have rounded it, so the settle fails closed (mergewake review).
func TestStrandedCompletedJobUnsafeTaskIDRefuses_5267(t *testing.T) {
	s := newStrandedSpawnClaimedAgo(t, 11*time.Minute)
	for i, arg := range s.task.Request.Args {
		if arg == "--task" && i+1 < len(s.task.Request.Args) {
			s.task.Request.Args[i+1] = "9007199254740992"
		}
	}
	record := completedJobRecord("machine-a")
	record["task_id"] = float64(9007199254740992)
	useJobStore(s, &completedJobStore{roleJobs: s.jobs, full: record})
	s.run(t)()
	if got := s.status(t); got.Status != "deadline_exceeded" {
		t.Fatalf("an unsafe task id settled the request: status=%s", got.Status)
	}
}

// A completed record older than the request is a previous incarnation of a
// reused job id, not this landing; a record with no usable completion time is
// refused too (mergewake review, hk 5267).
func TestStrandedCompletedJobCompletionTimeGuards_5267(t *testing.T) {
	cases := map[string]func(map[string]any, time.Time){
		"completed before the request": func(r map[string]any, created time.Time) {
			r["completed_at"] = created.Add(-time.Hour).UTC().Format(time.RFC3339Nano)
		},
		"no completion time":          func(r map[string]any, _ time.Time) { delete(r, "completed_at") },
		"unparseable completion time": func(r map[string]any, _ time.Time) { r["completed_at"] = "yesterday" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s := newStrandedSpawnClaimedAgo(t, 11*time.Minute)
			record := completedJobRecord("machine-a")
			mutate(record, s.task.Created)
			useJobStore(s, &completedJobStore{roleJobs: s.jobs, full: record})
			s.run(t)()
			if got := s.status(t); got.Status != "deadline_exceeded" {
				t.Fatalf("settled from an unusable completion time: status=%s", got.Status)
			}
		})
	}
}

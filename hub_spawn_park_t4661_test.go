package panewire

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 4661: parked requests (reason, next_check_at), the per-request
// re-placement back-off with jitter, exclusions across attempts, and the
// event wake in batches. Design: hk design/2026-11-13/hub-owned-spawn-queue
// §2 parked, §3 rules 3 and 4, §9a; docs/hub-spawn-operations.md#parked.

// withoutSpawnBackoff makes a hub re-place a refused request at once, for
// tests about the next host rather than the back-off before it (hk 4661).
func withoutSpawnBackoff(h *HubServer) {
	h.inbox.store.spawnRetryDelay = func(int) time.Duration { return 0 }
}

// parkRequest is a distinct implementer request (id, job and task) n.
func parkRequest(n int) (fleet.Submission, string) {
	request := durableSpawnRequest()
	request.RequestID = t14RequestID(466100 + n)
	request.Args = append([]string(nil), request.Args...)
	job := fmt.Sprintf("park-%d", n)
	for i, arg := range request.Args {
		switch arg {
		case "test-job":
			request.Args[i] = job
		case "42":
			request.Args[i] = fmt.Sprint(466100 + n)
		}
	}
	return request, job
}

func claimParkRequest(t *testing.T, h *HubServer, n int, now time.Time) hubSpawnTask {
	t.Helper()
	request, job := parkRequest(n)
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, job, "parent", now)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func parkNow(t *testing.T, h *HubServer, task hubSpawnTask, reason string) bool {
	t.Helper()
	parked, err := h.parkSpawnTask(t.Context(), task, reason, "", h.spawnWakeGen.Load())
	if err != nil {
		t.Fatal(err)
	}
	return parked
}

func parkedTask(t *testing.T, h *HubServer, id string) hubSpawnTask {
	t.Helper()
	task, err := h.inbox.store.spawnTask(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func scannedSpawn(t *testing.T, h *HubServer, id string, now time.Time) bool {
	t.Helper()
	page, _, err := h.inbox.store.pendingSpawnPage(t.Context(), spawnScanPosition{}, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range page {
		if task.Request.RequestID == id {
			return true
		}
	}
	return false
}

func TestSpawnParkReasonNamesNoCandidateRefusals_t4661(t *testing.T) {
	for _, tc := range []struct {
		rc         int
		text, want string
	}{
		{86, "quota_exhausted: QUOTA_EXHAUSTED: every eligible worker account (a,b) is measured exhausted; earliest reset 10:00", spawnParkQuota},
		{89, "QUOTA_UNKNOWN: credit pool status unavailable", spawnParkQuota},
		{4, "quota_unconfirmed:no_usable_presence: target account unusable", spawnParkQuota},
		{77, "role_no_candidate: role_no_candidate: every eligible candidate is in a pool at its concurrency cap (p 2/2)", spawnParkPoolCap},
		{77, "role_no_candidate: role_no_candidate: no profile passes role, retirement, grade, gate and seat rules", spawnParkNoEligibleHost},
		{81, "NO_ELIGIBLE_HOST: no host declares tag unity; correct --tags", spawnParkNoEligibleHost},
		{81, "no awake host admitted sonnet; a spawn would wake desktop (asleep)", spawnParkHostFull},
		{81, "no placement candidate accepted spawn; no local fallback", spawnParkHostFull},
		{81, "node desktop did not connect after wake", spawnParkNodeDisconnected},
		{81, "every candidate harness is in launch_backoff", spawnParkLaunchBackoff},
		// Hub placement never spawns locally: rc 80 after every remote tried.
		{80, "local spawns are disabled on this host; no remote candidate completed (tried: desktop task_slots_full)", spawnParkHostFull},
		{80, "local spawns are disabled on this host; no remote candidate completed (tried: desktop disconnected)", spawnParkNodeDisconnected},
		{80, "local spawns are disabled on this host; no remote candidate completed (tried: desktop checkout_refused:dirty)", spawnParkNoEligibleHost},
		// An escalation goes to the parent lane, never a silent park.
		{87, "role_exhausted_escalate: QUOTA_EXHAUSTED: every eligible worker account (a) is measured exhausted; ask the parent lane", ""},
		// Refusals no wait can fix still end the request.
		{2, "invalid_args: unknown flag --frob", ""},
		{81, "branch wk/x is not on origin; push it first", ""},
		{70, "configuration: missing seat policy for worker-x", ""},
	} {
		if got := spawnParkReason(tc.rc, tc.text); got != tc.want {
			t.Errorf("spawnParkReason(%d, %q) = %q, want %q", tc.rc, tc.text, got, tc.want)
		}
	}
}

// The back-off is 30 s × 2^(n-1) capped at 10 min, spread by at most ±20 %
// and never above the cap.
func TestSpawnRetryBackoffGrowsCapsAndJittersWithinBounds_t4661(t *testing.T) {
	base, limit := 30*time.Second, 10*time.Minute
	for failures, want := range map[int]time.Duration{0: 30 * time.Second, 1: 30 * time.Second, 2: time.Minute, 3: 2 * time.Minute, 4: 4 * time.Minute, 5: 8 * time.Minute, 6: limit, 40: limit} {
		if got := spawnRetryBackoff(base, limit, failures, 0.5); got != want {
			t.Errorf("failures=%d without spread: %s, want %s", failures, got, want)
		}
	}
	for _, failures := range []int{1, 2, 3, 4, 5} {
		center := spawnRetryBackoff(base, limit, failures, 0.5)
		low := spawnRetryBackoff(base, limit, failures, 0)
		high := spawnRetryBackoff(base, limit, failures, 0.999999)
		if low != center*8/10 {
			t.Errorf("failures=%d lowest spread %s, want %s", failures, low, center*8/10)
		}
		if high > center*12/10 || high <= center {
			t.Errorf("failures=%d highest spread %s outside (%s, %s]", failures, high, center, center*12/10)
		}
		if high > limit {
			t.Errorf("failures=%d spread %s above the cap", failures, high)
		}
	}
	if got := spawnRetryBackoff(base, limit, 9, 0.999999); got != limit {
		t.Errorf("a capped back-off spread above the cap: %s", got)
	}
	if got := spawnRetryBackoff(base, limit, 9, 0); got != limit*8/10 {
		t.Errorf("a capped back-off spread below: %s, want %s", got, limit*8/10)
	}
}

// No candidate: the request is parked with its reason and a next check one
// spread back-off base ahead (never past expires_at), placement skips it
// until then, and the status API reads parked with both.
func TestNoCandidateParksWithReasonAndNextCheck_t4661(t *testing.T) {
	h := asyncSpawnHub(t)
	clock := expiryClock(h, expiryNow())
	h.spawnRand = func() float64 { return 0 } // the lowest spread: 24 s
	task := claimParkRequest(t, h, 1, *clock)
	if !parkNow(t, h, task, spawnParkPoolCap) {
		t.Fatal("park was not stored")
	}
	saved := parkedTask(t, h, task.Request.RequestID)
	if saved.Status != "accepted" || saved.ParkReason != spawnParkPoolCap || !saved.NextCheck.Equal(clock.Add(24*time.Second)) {
		t.Fatalf("parked row: status=%s reason=%q next=%v, want accepted pool_cap %v", saved.Status, saved.ParkReason, saved.NextCheck, clock.Add(24*time.Second))
	}
	if scannedSpawn(t, h, task.Request.RequestID, *clock) {
		t.Fatal("placement scanned a parked request before its next check")
	}
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	code, _, body := t14HTTP(t, http.MethodGet, server.URL+"/v1/spawns/"+task.Request.RequestID, t14OperatorToken, nil)
	var answer struct {
		Status, Reason string
		NextCheckAt    string `json:"next_check_at"`
	}
	if code != http.StatusOK || json.Unmarshal(body, &answer) != nil || answer.Status != "parked" || answer.Reason != spawnParkPoolCap || answer.NextCheckAt != clock.Add(24*time.Second).UTC().Format(time.RFC3339) {
		t.Fatalf("status read %d %s", code, body)
	}
	*clock = clock.Add(24 * time.Second)
	if !scannedSpawn(t, h, task.Request.RequestID, *clock) {
		t.Fatal("the next check passed and placement still skips the request")
	}
	answer.Reason, answer.NextCheckAt = "", ""
	if _, _, body = t14HTTP(t, http.MethodGet, server.URL+"/v1/spawns/"+task.Request.RequestID, t14OperatorToken, nil); json.Unmarshal(body, &answer) != nil || answer.Status != "accepted" || answer.Reason != "" || answer.NextCheckAt != "" {
		t.Fatalf("a due request still reads parked: %s", body)
	}
	// Close to its limit, the next check is the limit itself.
	short, job := parkRequest(2)
	short.ExpiresSeconds = 60
	near, _, err := h.inbox.store.claimSpawn(t.Context(), short, job, "parent", clock.Add(-50*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	parkNow(t, h, near, spawnParkQuota)
	if got := parkedTask(t, h, near.Request.RequestID); !got.NextCheck.Equal(got.Expires) {
		t.Fatalf("next check %v passes the limit %v", got.NextCheck, got.Expires)
	}
}

// An event wakes at most one batch (8) of the parked requests whose reason
// it names per tick; the rest wake on later ticks, and other reasons wait
// for their own events or timers.
func TestEventWakesParkedInBatchesOfEight_t4661(t *testing.T) {
	h := asyncSpawnHub(t)
	clock := expiryClock(h, expiryNow())
	h.spawnRand = func() float64 { return 0.5 }
	var poolCapped, quota []string
	for n := range 20 {
		task := claimParkRequest(t, h, 10+n, *clock)
		parkNow(t, h, task, spawnParkPoolCap)
		poolCapped = append(poolCapped, task.Request.RequestID)
	}
	for n := range 3 {
		task := claimParkRequest(t, h, 40+n, *clock)
		parkNow(t, h, task, spawnParkQuota)
		quota = append(quota, task.Request.RequestID)
	}
	due := func(ids []string) int {
		n := 0
		for _, id := range ids {
			if !parkedTask(t, h, id).NextCheck.After(*clock) {
				n++
			}
		}
		return n
	}
	h.noteSpawnWake(spawnParkPoolCap)
	for tick, want := range []int{8, 16, 20, 20} {
		h.wakeParkedSpawns(t.Context())
		if got := due(poolCapped); got != want {
			t.Fatalf("tick %d: %d pool_cap requests due, want %d", tick+1, got, want)
		}
		if got := due(quota); got != 0 {
			t.Fatalf("tick %d: a pool slot woke %d quota requests", tick+1, got)
		}
	}
	h.spawnWakeMu.Lock()
	left := len(h.spawnWakes)
	h.spawnWakeMu.Unlock()
	if left != 0 {
		t.Fatalf("a drained wake stays noted: %d reasons", left)
	}
	h.noteSpawnWake(spawnParkQuota)
	h.wakeParkedSpawns(t.Context())
	if got := due(quota); got != 3 {
		t.Fatalf("a quota change woke %d of 3 quota requests", got)
	}
}

// The events that wake parked requests: a node connect, a pool slot freed
// (a job leaving a node's active set), and a finished quota refresh.
func TestEventsNoteTheirParkReasons_t4661(t *testing.T) {
	h := asyncSpawnHub(t)
	noted := func() map[string]bool {
		h.spawnWakeMu.Lock()
		defer h.spawnWakeMu.Unlock()
		got := map[string]bool{}
		for reason := range h.spawnWakes {
			got[reason] = true
		}
		clear(h.spawnWakes)
		return got
	}
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	t14Node(t, h, server, true)
	eventually(t, 3*time.Second, "node connect noted", func() bool {
		h.spawnWakeMu.Lock()
		defer h.spawnWakeMu.Unlock()
		return h.spawnWakes[spawnParkNodeDisconnected]
	})
	if got := noted(); !got[spawnParkNodeDisconnected] || !got[spawnParkHostFull] || !got[spawnParkNoEligibleHost] {
		t.Fatalf("node connect noted %v", got)
	}
	now := time.Now().UTC()
	h.observeActiveJobs("machine-a", []HubActiveJob{{JobID: "job-1", Epoch: 1}}, now)
	if got := noted(); got[spawnParkPoolCap] {
		t.Fatalf("a job starting noted a freed slot: %v", got)
	}
	h.observeActiveJobs("machine-a", nil, now)
	if got := noted(); !got[spawnParkPoolCap] || !got[spawnParkHostFull] {
		t.Fatalf("a job ending noted %v", got)
	}
	// The coordinator turns a finished quota refresh into a quota wake.
	ctx := t.Context()
	h.startSpawnCoordinator(ctx)
	task := claimParkRequest(t, h, 60, time.Now())
	h.inbox.store.parkSpawn(ctx, task.Request.RequestID, task.Attempt, spawnParkQuota, time.Now().Add(time.Hour))
	eventually(t, 5*time.Second, "quota refresh wakes the quota-parked request", func() bool {
		// Each poll finishes another refresh: the coordinator may read the
		// count first after an earlier one.
		h.quotaRefresh.completedCount.Add(1)
		h.kickSpawnPlacement()
		return parkedTask(t, h, task.Request.RequestID).NextCheck.Before(time.Now().Add(time.Minute))
	})
}

// A failed attempt parks the request launch_backoff for 30 s × 2^(n-1),
// capped and spread, on the hub clock; a pass that then finds no candidate
// does not shorten it.
func TestFailedAttemptBacksOffAndGrows_t4661(t *testing.T) {
	h := asyncSpawnHub(t)
	clock := expiryClock(h, expiryNow())
	spread := 1.0 - 1e-9
	h.spawnRand = func() float64 { return spread }
	request, job := parkRequest(70)
	request.ExpiresSeconds = 24 * 3600
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, job, "parent", *clock)
	if err != nil {
		t.Fatal(err)
	}
	for n, base := range []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 10 * time.Minute, 10 * time.Minute} {
		machine := fmt.Sprintf("host-%d", n)
		bound, err := h.inbox.store.bindSpawn(t.Context(), task.Request.RequestID, task.Attempt, boundaryTestNode(task, 466800+n, machine), clock.UTC())
		if err != nil {
			t.Fatal(err)
		}
		bound = sentSpawnFixture(t, h.inbox.store, bound)
		task, err = h.inbox.store.unbindRefusedSpawn(t.Context(), bound, hubSpawnExclusion{Machine: machine, Reason: "cwd_unmapped", RC: 2}, nil, *clock)
		if err != nil {
			t.Fatal(err)
		}
		want := min(time.Duration(float64(base)*(1+0.2*(2*spread-1))), 10*time.Minute)
		if task.ParkReason != spawnParkLaunchBackoff || task.FailedAttempts != n+1 || task.NextCheck.Sub(*clock) != want.Truncate(time.Millisecond) {
			t.Fatalf("failure %d: reason=%q failures=%d wait=%s, want launch_backoff %d %s", n+1, task.ParkReason, task.FailedAttempts, task.NextCheck.Sub(*clock), n+1, want)
		}
		if len(task.Exclusions) != n+1 {
			t.Fatalf("failure %d: %d exclusions", n+1, len(task.Exclusions))
		}
	}
	backoffUntil := task.NextCheck
	h.spawnRand = func() float64 { return 0 }
	parkNow(t, h, task, spawnParkNoEligibleHost)
	if got := parkedTask(t, h, task.Request.RequestID); got.ParkReason != spawnParkLaunchBackoff || !got.NextCheck.Equal(backoffUntil) {
		t.Fatalf("a later park shortened the back-off: %q %v, want %v", got.ParkReason, got.NextCheck, backoffUntil)
	}
}

// A host refusal excludes that host and account for the refused request
// only, in this and later attempts; another request still binds there.
func TestHostRefusalExcludesForThisRequestOnly_t4661(t *testing.T) {
	h := asyncSpawnHub(t)
	withoutSpawnBackoff(h)
	now := time.Now()
	refused := claimParkRequest(t, h, 80, now)
	other := claimParkRequest(t, h, 81, now)
	bound, err := h.inbox.store.bindSpawn(t.Context(), refused.Request.RequestID, refused.Attempt, boundaryTestNode(refused, 466900, "machine-a"), now.UTC())
	if err != nil {
		t.Fatal(err)
	}
	bound = sentSpawnFixture(t, h.inbox.store, bound)
	refused, err = h.inbox.store.unbindRefusedSpawn(t.Context(), bound, hubSpawnExclusion{Machine: "machine-a", Profile: "sonnet", Reason: "spawn_failed:auth_unavailable", RC: 88}, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	// The attempt ends unsent; the exclusion carries into attempt 2.
	if changed, err := h.inbox.store.completeSpawn(t.Context(), refused, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Retryable: true}, now.Add(601*time.Second)); err != nil || !changed {
		t.Fatalf("end attempt 1: %v %v", changed, err)
	}
	refused = parkedTask(t, h, refused.Request.RequestID)
	if refused.Attempt != 2 || len(refused.Exclusions) != 1 {
		t.Fatalf("attempt 2: attempt=%d exclusions=%+v", refused.Attempt, refused.Exclusions)
	}
	request := refusalNodeRequest(refused.Request, 466901, "machine-a")
	request.Args = []string{"--job", refused.Job, "-m", "sonnet"}
	reply, err := (&directSpawnTransport{h: h, task: refused}).Spawn(t.Context(), request)
	if err != nil || !reply.NextHost || reply.Error != "spawn_failed:auth_unavailable" {
		t.Fatalf("the refused request was placed on its excluded host again: %+v %v", reply, err)
	}
	if other = parkedTask(t, h, other.Request.RequestID); len(other.Exclusions) != 0 {
		t.Fatalf("another request inherited the exclusion: %+v", other.Exclusions)
	}
	otherRequest := refusalNodeRequest(other.Request, 466902, "machine-a")
	otherRequest.Args = []string{"--job", other.Job, "-m", "sonnet"}
	_, _ = (&directSpawnTransport{h: h, task: other}).Spawn(t.Context(), otherRequest)
	if got := parkedTask(t, h, other.Request.RequestID); got.Status != "bound" || got.Node == nil || got.Node.Machine != "machine-a" {
		t.Fatalf("another request did not bind on the host: status=%s node=%+v", got.Status, got.Node)
	}
}

// A parked request still gives up at expires_at: its next check is the
// limit, and the pass then ends it given_up.
func TestParkedRequestGivenUpAtItsLimit_t4661(t *testing.T) {
	h := asyncSpawnHub(t)
	clock := expiryClock(h, expiryNow())
	request, job := parkRequest(90)
	request.ExpiresSeconds = 300
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, job, "parent", *clock)
	if err != nil {
		t.Fatal(err)
	}
	*clock = clock.Add(290 * time.Second)
	parkNow(t, h, task, spawnParkQuota)
	parked := parkedTask(t, h, task.Request.RequestID)
	if !parked.NextCheck.Equal(parked.Expires) {
		t.Fatalf("next check %v, want the limit %v", parked.NextCheck, parked.Expires)
	}
	*clock = parked.Expires
	if !scannedSpawn(t, h, task.Request.RequestID, *clock) {
		t.Fatal("a parked request at its limit is not looked at")
	}
	h.executeSpawnTask(t.Context(), parked)
	if got := parkedTask(t, h, task.Request.RequestID); got.Status != "given_up" || got.Outcome.RC != 84 {
		t.Fatalf("parked request at its limit: status=%s outcome=%+v", got.Status, got.Outcome)
	}
}

// A hub restart keeps the park (reason, next check), the failure count and
// the exclusions: they are columns of the row.
func TestRestartKeepsParkedRowsAndExclusions_t4661(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	config := HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken}, InboxStorePath: path}
	h, err := NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	parked := claimParkRequest(t, h, 100, now)
	h.inbox.store.parkSpawn(t.Context(), parked.Request.RequestID, parked.Attempt, spawnParkHostFull, now.Add(time.Minute))
	refused := claimParkRequest(t, h, 101, now)
	bound, err := h.inbox.store.bindSpawn(t.Context(), refused.Request.RequestID, refused.Attempt, boundaryTestNode(refused, 467000, "machine-a"), now)
	if err != nil {
		t.Fatal(err)
	}
	bound = sentSpawnFixture(t, h.inbox.store, bound)
	if _, err = h.inbox.store.unbindRefusedSpawn(t.Context(), bound, hubSpawnExclusion{Machine: "machine-a", Profile: "sonnet", Reason: "cwd_unmapped", RC: 2}, nil, now); err != nil {
		t.Fatal(err)
	}
	want := parkedTask(t, h, refused.Request.RequestID)
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	h, err = NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	got := parkedTask(t, h, parked.Request.RequestID)
	if got.Status != "accepted" || got.ParkReason != spawnParkHostFull || !got.NextCheck.Equal(now.Add(time.Minute)) {
		t.Fatalf("park after restart: status=%s reason=%q next=%v", got.Status, got.ParkReason, got.NextCheck)
	}
	got = parkedTask(t, h, refused.Request.RequestID)
	if got.ParkReason != spawnParkLaunchBackoff || !got.NextCheck.Equal(want.NextCheck) || got.FailedAttempts != 1 || len(got.Exclusions) != 1 || got.Exclusions[0].Machine != "machine-a" {
		t.Fatalf("back-off after restart: %+v, want %+v", got, want)
	}
}

// A rollback: an older hub never reads the park columns. A parked row is an
// accepted row to it, so its coordinator query places it at once (no
// back-off, no wake batch), and its GET shows it as accepted.
func TestOlderHubPlacesParkedRowsAsAccepted_t4661(t *testing.T) {
	h := asyncSpawnHub(t)
	task := claimParkRequest(t, h, 110, time.Now())
	h.inbox.store.parkSpawn(t.Context(), task.Request.RequestID, task.Attempt, spawnParkQuota, time.Now().Add(time.Hour))
	var n int
	// The older hub's coordinator scan (e00bb952 / eefebb12).
	if err := h.inbox.store.db.QueryRow(`SELECT count(*) FROM spawn_operations WHERE status IN ('accepted','bound','dispatching','reconciling','bound_grant','dispatching_grant','reconciling_grant') AND request_id=?`, task.Request.RequestID).Scan(&n); err != nil || n != 1 {
		t.Fatalf("older coordinator scan matches %d parked rows, err=%v", n, err)
	}
	// Its bind CAS (status='accepted' AND node_id IS NULL) also matches.
	if err := h.inbox.store.db.QueryRow(`SELECT count(*) FROM spawn_operations WHERE request_id=? AND status='accepted' AND node_id IS NULL`, task.Request.RequestID).Scan(&n); err != nil || n != 1 {
		t.Fatalf("older bind CAS matches %d parked rows, err=%v", n, err)
	}
}

// A full wake batch leaves its reasons for the next timer tick without
// kicking the coordinator, so one event cannot drain the backlog at once.
func TestFullWakeBatchWaitsForTheNextTick_t4661(t *testing.T) {
	h := asyncSpawnHub(t)
	expiryClock(h, expiryNow())
	for n := range 9 {
		parkNow(t, h, claimParkRequest(t, h, 120+n, h.now()), spawnParkPoolCap)
	}
	h.noteSpawnWake(spawnParkPoolCap)
	<-h.spawnKick
	h.wakeParkedSpawns(t.Context())
	select {
	case <-h.spawnKick:
		t.Fatal("a full batch kicked the coordinator again")
	default:
	}
	h.spawnWakeMu.Lock()
	kept := h.spawnWakes[spawnParkPoolCap]
	h.spawnWakeMu.Unlock()
	if !kept {
		t.Fatal("a full batch dropped its reason")
	}
}

// An event noted while placement ran is noted again once the park lands:
// the wake it raced found no parked row.
func TestWakeDuringPlacementIsKeptForThePark_t4661(t *testing.T) {
	h := asyncSpawnHub(t)
	task := claimParkRequest(t, h, 140, time.Now())
	before := h.spawnWakeGen.Load()
	h.noteSpawnWake(spawnParkHostFull)
	h.wakeParkedSpawns(t.Context()) // drained before the park
	if parked, err := h.parkSpawnTask(t.Context(), task, spawnParkHostFull, "", before); err != nil || !parked {
		t.Fatal(parked, err)
	}
	h.wakeParkedSpawns(t.Context())
	if got := parkedTask(t, h, task.Request.RequestID); got.NextCheck.After(h.now()) {
		t.Fatalf("the raced wake was lost: next check %v", got.NextCheck)
	}
}

// A row with no limit (written before expires_ms) keeps the legacy
// contract: a next-host refusal never parks it past its deadline.
func TestLegacyRowWithoutLimitIsNotBackedOff_t4661(t *testing.T) {
	h := asyncSpawnHub(t)
	task := claimParkRequest(t, h, 150, time.Now())
	if _, err := h.inbox.store.db.Exec(`UPDATE spawn_operations SET expires_ms=0 WHERE request_id=?`, task.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	task = parkedTask(t, h, task.Request.RequestID)
	bound, err := h.inbox.store.bindSpawn(t.Context(), task.Request.RequestID, task.Attempt, boundaryTestNode(task, 467100, "machine-a"), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	bound = sentSpawnFixture(t, h.inbox.store, bound)
	got, err := h.inbox.store.unbindRefusedSpawn(t.Context(), bound, hubSpawnExclusion{Machine: "machine-a", Reason: "cwd_unmapped", RC: 2}, nil, time.Now())
	if err != nil || got.ParkReason != "" || !got.NextCheck.IsZero() {
		t.Fatalf("legacy row parked: %q %v %v", got.ParkReason, got.NextCheck, err)
	}
}

// A parked, unbound request holds no pool slot: liveSpawns leaves it out,
// so it cannot make another request see its pool full.
func TestParkedRequestHoldsNoPoolSlot_t4661(t *testing.T) {
	h := asyncSpawnHub(t)
	parked := claimParkRequest(t, h, 160, time.Now())
	open := claimParkRequest(t, h, 161, time.Now())
	parkNow(t, h, parked, spawnParkQuota)
	live, err := h.inbox.store.liveSpawns(t.Context(), nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, task := range live {
		seen[task.Request.RequestID] = true
	}
	if seen[parked.Request.RequestID] || !seen[open.Request.RequestID] {
		t.Fatalf("live spawns %v: parked counted or accepted missing", seen)
	}
}

// A claimed-harness retry keeps the immediate retry: hk holds the job claim
// on that host, so it is never parked toward expires_at.
func TestClaimedHarnessRetryIsNotBackedOff_t4661(t *testing.T) {
	h := asyncSpawnHub(t)
	task := claimParkRequest(t, h, 170, time.Now())
	bound, err := h.inbox.store.bindSpawn(t.Context(), task.Request.RequestID, task.Attempt, boundaryTestNode(task, 467200, "machine-a"), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	bound = sentSpawnFixture(t, h.inbox.store, bound)
	claimed := hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "launch_proof_failed", NodeResult: true}
	got, err := h.inbox.store.unbindRefusedSpawn(t.Context(), bound, hubSpawnExclusion{Machine: "machine-a", Profile: "codex-sol", Reason: "launch_proof_failed", RC: 70}, &claimed, time.Now())
	if err != nil || got.ParkReason != "" || got.ClaimedHost != "machine-a" {
		t.Fatalf("claimed retry: park=%q claimed=%q err=%v", got.ParkReason, got.ClaimedHost, err)
	}
}

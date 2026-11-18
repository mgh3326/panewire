package panewire

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 4660: a request's time limit (expires_at) and its state given_up. The
// design is hk design/2026-11-13/hub-owned-spawn-queue §2 and §3 rule 2;
// docs/hub-spawn-operations.md (#time-limit) records the behaviour and the
// rollback. Start conditions through the queue are a later part.

// expiryClock gives the hub a clock the test moves.
func expiryClock(h *HubServer, start time.Time) *time.Time {
	clock := start
	h.now = func() time.Time { return clock }
	return &clock
}

func expiryNow() time.Time { return time.Now().UTC().Truncate(time.Second) }

func TestSpawnExpiresDefaultsByRoleAndOverride_t4660(t *testing.T) {
	h := asyncSpawnHub(t)
	now := expiryNow()
	claim := func(t *testing.T, request fleet.Submission, job string) hubSpawnTask {
		t.Helper()
		task, _, err := h.inbox.store.claimSpawn(t.Context(), request, job, "parent", now)
		if err != nil {
			t.Fatal(err)
		}
		saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
		if err != nil || !saved.Expires.Equal(task.Expires) {
			t.Fatalf("expires_at was not stored: claim=%v stored=%v err=%v", task.Expires, saved.Expires, err)
		}
		return saved
	}
	implementer := claim(t, durableSpawnRequest(), "test-job")
	if got := implementer.Expires.Sub(now); got != 6*time.Hour {
		t.Fatalf("implementer expires after %s, want 6h", got)
	}
	// The per-attempt landing wait stays 600 s inside a 6 h limit.
	if got := implementer.Deadline.Sub(now); got != 600*time.Second {
		t.Fatalf("first attempt deadline %s, want 600s", got)
	}
	for _, role := range []string{"reviewer", "consult"} {
		request := reviewerReleaseRequest(role, false)
		request.RequestID = "d676220b-e763-4af1-aefd-0000000007" + map[string]string{"reviewer": "01", "consult": "02"}[role]
		request.Args = replaceSpawnArg(append([]string(nil), request.Args...), "--job", "job-"+role)
		request.Args = replaceSpawnArg(request.Args, "-l", "job-"+role)
		if got := claim(t, request, "job-"+role).Expires.Sub(now); got != time.Hour {
			t.Fatalf("%s expires after %s, want 1h", role, got)
		}
	}
	override := durableSpawnRequest()
	override.RequestID = "d676220b-e763-4af1-aefd-000000000711"
	override.Args = replaceSpawnArg(append([]string(nil), override.Args...), "--task", "43")
	override.Args = replaceSpawnArg(override.Args, "--job", "job-override")
	override.Args = replaceSpawnArg(override.Args, "-l", "job-override")
	override.ExpiresSeconds = 2 * 3600
	if got := claim(t, override, "job-override").Expires.Sub(now); got != 2*time.Hour {
		t.Fatalf("--expires 2h gave %s", got)
	}
	// A limit shorter than one landing wait also ends the first attempt.
	short := durableSpawnRequest()
	short.RequestID = "d676220b-e763-4af1-aefd-000000000712"
	short.Args = replaceSpawnArg(append([]string(nil), short.Args...), "--task", "44")
	short.Args = replaceSpawnArg(short.Args, "--job", "job-short")
	short.Args = replaceSpawnArg(short.Args, "-l", "job-short")
	short.ExpiresSeconds = 300
	got := claim(t, short, "job-short")
	if !got.Deadline.Equal(got.Expires) || got.Expires.Sub(now) != 5*time.Minute {
		t.Fatalf("a 5m limit must cap the 600s attempt: deadline=%v expires=%v", got.Deadline, got.Expires)
	}
}

func TestSpawnSubmitRefusesExpiresOutsideBounds_t4660(t *testing.T) {
	h := asyncSpawnHub(t)
	for _, seconds := range []int{-1, 59, 24*3600 + 1} {
		request := durableSpawnRequest()
		request.ExpiresSeconds = seconds
		raw, _ := json.Marshal(request)
		r := httptest.NewRequest(http.MethodPost, "/v1/spawns", bytes.NewReader(raw))
		r.Header.Set("Authorization", "Bearer "+t14OperatorToken)
		w := httptest.NewRecorder()
		h.Handler().ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("expires_seconds=%d: submit=%d %s, want 400", seconds, w.Code, w.Body)
		}
		if _, err := h.inbox.store.spawnTask(t.Context(), request.RequestID); err == nil {
			t.Fatalf("expires_seconds=%d left an operation behind", seconds)
		}
	}
	request := durableSpawnRequest()
	request.ExpiresSeconds = 24 * 3600
	raw, _ := json.Marshal(request)
	r := httptest.NewRequest(http.MethodPost, "/v1/spawns", bytes.NewReader(raw))
	r.Header.Set("Authorization", "Bearer "+t14OperatorToken)
	w := httptest.NewRecorder()
	h.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusAccepted {
		t.Fatalf("the 24h maximum was refused: %d %s", w.Code, w.Body)
	}
}

// The landing wait ends the attempt; the request goes on until its limit and
// then ends given_up, terminal, with its reason and one notice.
func TestUnstartedRequestGivenUpAtItsLimit_t4660(t *testing.T) {
	h := asyncSpawnHub(t)
	clock := expiryClock(h, expiryNow())
	request := durableSpawnRequest()
	request.ExpiresSeconds = 1800
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", *clock)
	if err != nil {
		t.Fatal(err)
	}
	// Attempt 1's landing wait passes with 20 minutes of the limit left.
	*clock = clock.Add(601 * time.Second)
	h.executeSpawnTask(t.Context(), task)
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "accepted" || saved.Attempt != 2 || !saved.Deadline.Equal(clock.Add(600*time.Second)) {
		t.Fatalf("after the landing wait: %+v %v; want accepted attempt 2 with a fresh 600s wait", saved, err)
	}
	// Attempt 2's wait would end past the limit, so the limit ends it.
	*clock = saved.Deadline
	h.executeSpawnTask(t.Context(), saved)
	saved, err = h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "accepted" || saved.Attempt != 3 || !saved.Deadline.Equal(saved.Expires) {
		t.Fatalf("attempt 3 must end at the limit: %+v %v", saved, err)
	}
	*clock = saved.Expires
	h.executeSpawnTask(t.Context(), saved)
	saved, err = h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "given_up" || saved.Outcome.RC != 84 || saved.Outcome.Stage != "queue" || !strings.HasPrefix(saved.Outcome.Reason, "expired: expires_at "+saved.Expires.UTC().Format(time.RFC3339)+" passed after 3 attempt(s)") || saved.Outcome.Retryable {
		t.Fatalf("at the limit: %+v %v; want given_up with its reason", saved, err)
	}
	// Terminal: no further pass touches it, and the job and role are free.
	page, _, err := h.inbox.store.pendingSpawnPage(t.Context(), spawnScanPosition{}, time.Now())
	if err != nil || len(page) != 0 {
		t.Fatalf("a given_up request was scheduled again: %+v %v", page, err)
	}
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), roleSecondRequest(), "test-job-r2", "parent", *clock); err != nil {
		t.Fatalf("a given_up request kept the task/role reservation: %v", err)
	}
	var notices int
	if err := h.inbox.store.db.QueryRow(`SELECT count(*) FROM spawn_notifications WHERE request_id=?`, request.RequestID).Scan(&notices); err != nil || notices != 1 {
		t.Fatalf("notices=%d err=%v; want one SPAWN_FAILED for the whole request, none per attempt", notices, err)
	}
	var raw string
	if err := h.inbox.store.db.QueryRow(`SELECT result FROM spawn_notifications WHERE event_id=?`, request.RequestID+":3:given_up").Scan(&raw); err != nil {
		t.Fatalf("given_up notice: %v", err)
	}
	var outcome hubSpawnOutcome
	_ = json.Unmarshal([]byte(raw), &outcome)
	if text := spawnNotificationText(task.Job, request.RequestID, outcome); !strings.Contains(text, "SPAWN_FAILED test-job") || !strings.Contains(text, "reason=expired") || !strings.Contains(text, "replay=no") {
		t.Fatalf("notice text %q", text)
	}
	// The status API shows the state and its limit; a replay returns it.
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	status, _, body := t14HTTP(t, http.MethodGet, server.URL+"/v1/spawns/"+request.RequestID, t14OperatorToken, nil)
	var view struct {
		Status    string          `json:"status"`
		ExpiresAt string          `json:"expires_at"`
		Result    hubSpawnOutcome `json:"result"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &view) != nil || view.Status != "given_up" || view.ExpiresAt != saved.Expires.UTC().Format(time.RFC3339) || !strings.HasPrefix(view.Result.Reason, "expired") {
		t.Fatalf("status read %d %s", status, body)
	}
	if !fleet.SpawnRequestStateKnown(view.Status) {
		t.Fatalf("the clients do not know %q", view.Status)
	}
	status, _, body = t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, request)
	if status != http.StatusAccepted || !strings.Contains(string(body), `"status":"given_up"`) || !strings.Contains(string(body), `"expires_at"`) {
		t.Fatalf("replay of a given_up request: %d %s", status, body)
	}
	if again, err := h.inbox.store.spawnTask(t.Context(), request.RequestID); err != nil || again.Status != "given_up" || again.Attempt != 3 {
		t.Fatalf("the replay restarted a given_up request: %+v %v", again, err)
	}
}

// A request whose start is not proven absent is never given up, at its limit
// or after it. Only the node's proof (or the hub never having sent) ends it.
func TestReconcilingRequestIsNeverGivenUp_t4660(t *testing.T) {
	for _, test := range []struct {
		name    string
		outcome hubSpawnOutcome
		want    string
	}{
		{"landing wait ended, node silent", hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Retryable: true}, "deadline_exceeded"},
		{"transport lost", hubSpawnOutcome{RC: 1, Stage: "launch", Reason: "disconnected"}, "reconciling"},
		{"node answer unknown", hubSpawnOutcome{RC: 1, Stage: "launch", Reason: "unknown"}, "reconciling"},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := asyncSpawnHub(t)
			now := expiryNow()
			request := durableSpawnRequest()
			request.ExpiresSeconds = 600
			task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", now)
			if err != nil {
				t.Fatal(err)
			}
			task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, boundTestNode(task), now)
			if err != nil {
				t.Fatal(err)
			}
			task = sentSpawnFixture(t, h.inbox.store, task)
			// Both the attempt's wait and the request's limit are long past.
			after := task.Expires.Add(time.Hour)
			if _, err := h.inbox.store.completeSpawn(t.Context(), task, test.outcome, after); err != nil {
				t.Fatal(err)
			}
			saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
			if err != nil || saved.Status != test.want || saved.Attempt != 1 || saved.Node == nil {
				t.Fatalf("status=%s attempt=%d node=%v err=%v; want %s with ownership kept", saved.Status, saved.Attempt, saved.Node, err, test.want)
			}
			if _, _, err := h.inbox.store.claimSpawn(t.Context(), roleSecondRequest(), "test-job-r2", "parent", after); !errors.Is(err, errSpawnClaimed) {
				t.Fatalf("an unproven start released its reservation: %v", err)
			}
		})
	}
	t.Run("hub pass over a sent request past its limit", func(t *testing.T) {
		h := asyncSpawnHub(t)
		clock := expiryClock(h, expiryNow())
		request := durableSpawnRequest()
		request.ExpiresSeconds = 600
		task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", *clock)
		if err != nil {
			t.Fatal(err)
		}
		if task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, boundTestNode(task), *clock); err != nil {
			t.Fatal(err)
		}
		task = sentSpawnFixture(t, h.inbox.store, task)
		*clock = task.Expires.Add(time.Hour)
		h.executeSpawnTask(t.Context(), task)
		saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
		if err != nil || saved.Status == "given_up" || saved.Attempt != 1 || saved.Node == nil {
			t.Fatalf("a sent request was given up or restarted: %+v %v", saved, err)
		}
	})
	t.Run("node proof after the limit ends it", func(t *testing.T) {
		h := asyncSpawnHub(t)
		now := expiryNow()
		request := durableSpawnRequest()
		request.ExpiresSeconds = 1200
		task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", now)
		if err != nil {
			t.Fatal(err)
		}
		if task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, boundTestNode(task), now); err != nil {
			t.Fatal(err)
		}
		task = sentSpawnFixture(t, h.inbox.store, task)
		// The node's explicit not-started attestation.
		proof := hubSpawnOutcome{RC: 124, Stage: "claim", Reason: "deadline_not_started", Retryable: true, NodeResult: true, Host: "machine-a"}
		// The attestation after the attempt's deadline, inside the limit
		// (20 minutes), renews the attempt.
		if changed, err := h.inbox.store.completeSpawn(t.Context(), task, proof, task.Deadline); err != nil || !changed {
			t.Fatalf("complete: %v %v", changed, err)
		}
		saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
		if err != nil || saved.Status != "accepted" || saved.Attempt != 2 {
			t.Fatalf("proof before the limit: %+v %v; want attempt 2", saved, err)
		}
		// The same proof for an attempt that ends after the limit.
		if changed, err := h.inbox.store.completeSpawn(t.Context(), saved, hubSpawnOutcome{RC: 124, Stage: "placement", Reason: "timeout"}, saved.Expires.Add(time.Second)); err != nil || !changed {
			t.Fatalf("complete: %v %v", changed, err)
		}
		saved, err = h.inbox.store.spawnTask(t.Context(), request.RequestID)
		if err != nil || saved.Status != "given_up" || saved.Attempt != 2 {
			t.Fatalf("proof after the limit: %+v %v; want given_up", saved, err)
		}
	})
}

// A row written before the column (or by an older hub, which leaves it at
// its default) has no limit: its timeout ends it retryable, as it always did.
func TestRequestWithoutExpiryKeepsRetryableTimeout_t4660(t *testing.T) {
	h := asyncSpawnHub(t)
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.inbox.store.db.Exec(`UPDATE spawn_operations SET expires_ms=0`); err != nil {
		t.Fatal(err)
	}
	task, err = h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || !task.Expires.IsZero() {
		t.Fatalf("row without a limit: %+v %v", task, err)
	}
	if changed, err := h.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 124, Stage: "placement", Reason: "timeout", Retryable: true}); err != nil || !changed {
		t.Fatalf("complete: %v %v", changed, err)
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "retryable" || saved.Attempt != 1 {
		t.Fatalf("a request with no limit: %+v %v; want retryable", saved, err)
	}
}

// The renewal runs the same hand-over a client's replay does: the previous
// attempt's deferred placement trace is restored and its hk record sealed.
func TestTimeoutRenewalPreservesDeferredPlacementTrace_t4660(t *testing.T) {
	hk := placementOutboxServer(t)
	h := placementOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(placementTraceSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	h.deferPlacementTraceSave(request.RequestID, task.Attempt, payload, "")
	if changed, err := h.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 124, Stage: "placement", Reason: "timeout", Retryable: true}); err != nil || !changed {
		t.Fatalf("complete: %v %v", changed, err)
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "accepted" || saved.Attempt != 2 {
		t.Fatalf("renewal: %+v %v", saved, err)
	}
	h.flushPlacementRecords(t.Context())
	records := hk.records()
	if len(records) != 1 || records[0]["attempt"] != float64(1) || records[0]["reason"] != "timeout" || records[0]["outcome"] != "refused" {
		t.Fatalf("attempt 1's record: %v", records)
	}
}

// What an older hub does with a given_up row (docs/hub-spawn-operations.md,
// "Rolling back past the time limit"): the status queries it ran list no
// given_up, so it never replays, sweeps, completes or releases such a row. The
// predicates are copied from the code of the commit before this part
// (e00bb952).
func TestOlderHubQueriesIgnoreGivenUpRows_t4660(t *testing.T) {
	h := asyncSpawnHub(t)
	request := durableSpawnRequest()
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", expiryNow()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.inbox.store.db.Exec(`UPDATE spawn_operations SET status='given_up' WHERE request_id=?`, request.RequestID); err != nil {
		t.Fatal(err)
	}
	for name, where := range map[string]string{
		"coordinator replay":           `status IN ('accepted','bound','dispatching','reconciling','bound_grant','dispatching_grant','reconciling_grant')`,
		"stale-deadline replay":        `status IN ('deadline_exceeded','deadline_exceeded_grant')`,
		"outcome write":                `status IN ('accepted','bound','dispatching','reconciling','deadline_exceeded','bound_grant','dispatching_grant','reconciling_grant','deadline_exceeded_grant')`,
		"placement recovery":           `status IN ('bound','dispatching','reconciling','succeeded','bound_grant','dispatching_grant','reconciling_grant') OR status='deadline_exceeded'`,
		"live view":                    `status IN ('accepted','bound','dispatching','bound_grant','dispatching_grant') OR status='succeeded'`,
		"retryable replay":             `status='retryable'`,
		"role release":                 `status IN ('succeeded','failed')`,
		"unplaced-refresh bookkeeping": `status='accepted'`,
	} {
		var n int
		if err := h.inbox.store.db.QueryRow(`SELECT count(*) FROM spawn_operations WHERE ` + where).Scan(&n); err != nil || n != 0 {
			t.Errorf("older-hub query %q matches %d given_up rows, err=%v", name, n, err)
		}
	}
	// An older hub's GET passes the status through unchanged.
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	if code, _, body := t14HTTP(t, http.MethodGet, server.URL+"/v1/spawns/"+request.RequestID, t14OperatorToken, nil); code != http.StatusOK || !strings.Contains(string(body), `"status":"given_up"`) {
		t.Errorf("status read %d %s", code, body)
	}
}

// A staged successor (hub-internal author replacement) inherits the request's
// limit and is activated later: activation never lifts its landing wait past
// the limit, and one activated at or past it is given up unsent (hk 4660).
func TestStagedActivationRespectsTheLimit_t4660(t *testing.T) {
	h := asyncSpawnHub(t)
	clock := expiryClock(h, expiryNow())
	placed := false
	h.spawnNewApp = func(io.Writer, io.Writer) (*fleet.App, error) {
		placed = true
		return nil, errors.New("must not be placed")
	}
	stage := func(n int, expires int) hubSpawnTask {
		request := durableSpawnRequest()
		request.RequestID = t14RequestID(4700 + n)
		request.ExpiresSeconds = expires
		request.Args = replaceSpawnArg(append([]string(nil), request.Args...), "--task", strconv.Itoa(4700+n))
		request.Args = replaceSpawnArg(request.Args, "--job", "staged-"+strconv.Itoa(n))
		request.Args = replaceSpawnArg(request.Args, "-l", "staged-"+strconv.Itoa(n))
		task, _, err := h.inbox.store.claimSpawnReplacing(t.Context(), request, "staged-"+strconv.Itoa(n), "parent", *clock, "", "", "staged", "")
		if err != nil {
			t.Fatal(err)
		}
		return task
	}
	// Activated inside the limit: the 150 s wait is capped at expires_at.
	task := stage(1, 120)
	*clock = clock.Add(10 * time.Second)
	if err := h.inbox.store.activateStagedSpawn(t.Context(), task.Request.RequestID, *clock, 150); err != nil {
		t.Fatal(err)
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil || saved.Status != "accepted" || !saved.Deadline.Equal(saved.Expires) {
		t.Fatalf("activation lifted the deadline past the limit: %+v %v", saved, err)
	}
	// Activated past the limit: given up on the first pass, never placed.
	late := stage(2, 60)
	*clock = clock.Add(2 * time.Minute)
	if err := h.inbox.store.activateStagedSpawn(t.Context(), late.Request.RequestID, *clock, 150); err != nil {
		t.Fatal(err)
	}
	saved, err = h.inbox.store.spawnTask(t.Context(), late.Request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	h.executeSpawnTask(t.Context(), saved)
	saved, err = h.inbox.store.spawnTask(t.Context(), late.Request.RequestID)
	if err != nil || saved.Status != "given_up" || placed {
		t.Fatalf("an expired staged request was not given up unsent: %+v placed=%v err=%v", saved, placed, err)
	}
}

// ---- review round 1 (mergewake findings on head 10548ac) ----

// The node's own not-started answer (and the start proof's fence) carries the
// reason deadline_not_started, not timeout. After the attempt's deadline it
// ends the attempt like a timeout does: renewed before the limit, given_up
// after it. Inside the deadline it is a refusal and ends retryable as before.
func TestNotStartedAnswerRenewsOrGivesUp_t4660(t *testing.T) {
	notStarted := hubSpawnOutcome{RC: 124, Stage: "claim", Reason: "deadline_not_started", Retryable: true, NodeResult: true, Host: "machine-a"}
	sent := func(t *testing.T, h *HubServer, now time.Time) hubSpawnTask {
		t.Helper()
		request := durableSpawnRequest()
		request.ExpiresSeconds = 3600
		task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", now)
		if err != nil {
			t.Fatal(err)
		}
		if task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, boundTestNode(task), now); err != nil {
			t.Fatal(err)
		}
		return sentSpawnFixture(t, h.inbox.store, task)
	}
	for _, test := range []struct {
		name       string
		after      time.Duration // from the claim
		status     string
		attempt    int
		reserved   bool
		notices    int
		wantReason string
	}{
		{"before the attempt's deadline", 5 * time.Minute, "retryable", 1, false, 1, ""},
		{"after the deadline, before the limit", 11 * time.Minute, "accepted", 2, true, 0, ""},
		{"after the limit", 61 * time.Minute, "given_up", 1, false, 1, "expired"},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := asyncSpawnHub(t)
			now := expiryNow()
			clock := expiryClock(h, now)
			task := sent(t, h, now)
			*clock = now.Add(test.after)
			if changed, err := h.completeSpawn(t.Context(), task, notStarted); err != nil || !changed {
				t.Fatalf("complete: %v %v", changed, err)
			}
			saved, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
			if err != nil || saved.Status != test.status || saved.Attempt != test.attempt || (test.wantReason != "" && !strings.HasPrefix(saved.Outcome.Reason, test.wantReason)) {
				t.Fatalf("%+v %v; want %s attempt %d", saved, err, test.status, test.attempt)
			}
			held, _ := h.inbox.store.spawnJobReserved(t.Context(), "test-job")
			var notices int
			if err := h.inbox.store.db.QueryRow(`SELECT count(*) FROM spawn_notifications WHERE request_id=?`, task.Request.RequestID).Scan(&notices); err != nil || held != test.reserved || notices != test.notices {
				t.Fatalf("reserved=%v notices=%d err=%v; want %v %d", held, notices, err, test.reserved, test.notices)
			}
		})
	}
}

// A placement trace the hub cannot restore does not end the request early: the
// attempt's end is held, the request keeps its reservation, and it settles
// once the trace can be restored.
func TestHeldPlacementTraceKeepsRequestPending_t4660(t *testing.T) {
	hub := placementOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	request := durableSpawnRequest()
	task := boundSpawnTask(t, hub, request)
	payload, _ := json.Marshal(placementTraceSnapshot())
	hub.deferPlacementTraceSave(request.RequestID, task.Attempt, payload, "")
	good := hub.inbox.deferredPath
	hub.inbox.deferredPath = nonemptyDeferredDirectory(t)
	timeout := hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Retryable: true, Host: "machine-a"}
	if changed, err := hub.completeSpawn(t.Context(), task, timeout); err == nil || changed {
		t.Fatalf("complete with an unrestorable trace: changed=%v err=%v; want it held", changed, err)
	}
	saved, err := hub.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "bound" || saved.Attempt != 1 {
		t.Fatalf("held end changed the request: %+v %v", saved, err)
	}
	if held, _ := hub.inbox.store.spawnJobReserved(t.Context(), "test-job"); !held {
		t.Fatal("a held end freed the reservation")
	}
	hub.inbox.deferredPath = good
	if changed, err := hub.completeSpawn(t.Context(), task, timeout); err != nil || !changed {
		t.Fatalf("complete after the trace is restorable: %v %v", changed, err)
	}
	if saved, _ := hub.inbox.store.spawnTask(t.Context(), request.RequestID); saved.Status != "accepted" || saved.Attempt != 2 {
		t.Fatalf("after recovery: %+v", saved)
	}
}

// ---- review round 2 (mergewake findings on head ae38439) ----

// ---- hk 4747: the request's absolute expiry reaches the spawn receipt ----

// The hub sends expires_ms only to a node that advertised the capability that
// reads it; an older node's strict decoder never sees the field.
func TestSpawnExpiresFieldOnlyForANodeThatReadsIt_t4747(t *testing.T) {
	expires := time.Date(2026, 11, 15, 6, 0, 0, 0, time.UTC)
	if got := spawnExpiryField(map[string]bool{hubCapabilitySpawnExpires: true}, expires); got != expires.UnixMilli() {
		t.Fatalf("capable node got expires_ms=%d", got)
	}
	if got := spawnExpiryField(map[string]bool{"spawn_ops_v1": true}, expires); got != 0 {
		t.Fatalf("an older node got expires_ms=%d", got)
	}
	if got := spawnExpiryField(map[string]bool{hubCapabilitySpawnExpires: true}, time.Time{}); got != 0 {
		t.Fatalf("a request with no expiry got expires_ms=%d", got)
	}
}

// A node advertises spawn_expires_v1 and reads expires_ms on job.spawn; a frame
// without it keeps today's behavior, and a non-positive value is refused.
func TestNodeSpawnReadsTheRequestExpiry_t4747(t *testing.T) {
	advertised := false
	for _, capability := range hubSpawnCapabilities {
		advertised = advertised || capability == hubCapabilitySpawnExpires
	}
	if !advertised {
		t.Fatalf("hello capabilities %v lack %s", hubSpawnCapabilities, hubCapabilitySpawnExpires)
	}
	frame := `{"type":"job.spawn","request_id":"` + t14RequestID(1) + `","cwd_key":"repo-a","brief":{"inline":"b"},"args":["--job","job-a"],"wait_seconds":10,"deadline_ms":1700000000000,"expires_ms":1700003600000,"operator_authorized":true}`
	message, rejection := parseHubOutboundRejection([]byte(frame))
	if rejection.Class != "" || message.ExpiresMS != 1700003600000 {
		t.Fatalf("job.spawn with expires_ms: class=%q detail=%q expires=%d", rejection.Class, rejection.Detail, message.ExpiresMS)
	}
	old := `{"type":"job.spawn","request_id":"` + t14RequestID(2) + `","cwd_key":"repo-a","brief":{"inline":"b"},"args":["--job","job-a"],"wait_seconds":10,"deadline_ms":1700000000000,"operator_authorized":true}`
	if message, ok := parseHubOutbound([]byte(old)); !ok || message.ExpiresMS != 0 {
		t.Fatalf("a frame without expires_ms: ok=%v expires=%d", ok, message.ExpiresMS)
	}
	for _, field := range []string{`"expires_ms":0`, `"expires_ms":-1`, `"expires_ms":"x"`, `"expires_ms":null`} {
		bad := `{"type":"job.spawn","request_id":"` + t14RequestID(3) + `","cwd_key":"repo-a","brief":{"inline":"b"},"args":["--job","job-a"],"wait_seconds":10,"deadline_ms":1700000000000,"operator_authorized":true,` + field + `}`
		if _, ok := parseHubOutbound([]byte(bad)); ok {
			t.Fatalf("job.spawn with %s was accepted", field)
		}
	}
}

package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 4660: a request's time limit (expires_at), its states waiting and
// given_up, and start conditions through the queue. The design is hk
// design/2026-11-13/hub-owned-spawn-queue §2 to §4; docs/hub-spawn-operations.md
// (#time-limit-and-start-conditions) records the behaviour and the rollback.

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
	page, _, err := h.inbox.store.pendingSpawnPage(t.Context(), spawnScanPosition{})
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
		request.ExpiresSeconds = 600
		task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", now)
		if err != nil {
			t.Fatal(err)
		}
		if task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, boundTestNode(task), now); err != nil {
			t.Fatal(err)
		}
		task = sentSpawnFixture(t, h.inbox.store, task)
		proof := hubSpawnOutcome{RC: 124, Stage: "claim", Reason: "timeout", Retryable: true, NodeResult: true, Host: "machine-a"}
		if changed, err := h.completeSpawn(t.Context(), task, proof); err != nil || changed {
			// h.now is the wall clock here: the limit (10 minutes) has not
			// passed, so the proof renews the attempt instead.
			_ = changed
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

// ---- start conditions through the queue ----

type startConditionHK struct {
	state atomic.Value
	reads atomic.Int32
}

// startConditionServer serves hk tasks 9000 (the state set on the returned
// value) and 9001 (always merged); a task in dropped serves "dropped".
func startConditionServer(t *testing.T, state string) *startConditionHK {
	t.Helper()
	hk := &startConditionHK{}
	hk.state.Store(state)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		hk.reads.Add(1)
		switch r.URL.Path {
		case "/v1/tasks/9000":
			_, _ = w.Write([]byte(`{"id":9000,"state":"` + hk.state.Load().(string) + `","relations":[]}`))
		case "/v1/tasks/9001":
			_, _ = w.Write([]byte(`{"id":9001,"state":"merged","relations":[]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	t.Setenv("HANDOFFKEEP_URL", server.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "start-condition-test")
	return hk
}

func waitingRequest(n int, conditions ...fleet.StartCondition) fleet.Submission {
	request := durableSpawnRequest()
	request.RequestID = t14RequestID(4600 + n)
	request.Args = replaceSpawnArg(append([]string(nil), request.Args...), "--task", strconv.Itoa(5000+n))
	request.Args = replaceSpawnArg(request.Args, "--job", "wait-job")
	request.Args = replaceSpawnArg(request.Args, "-l", "wait-job")
	request.StartConditions = conditions
	return request
}

func TestStartConditionKeepsRequestWaitingThenReleasesIt_t4660(t *testing.T) {
	hk := startConditionServer(t, "ready")
	h := asyncSpawnHub(t)
	clock := expiryClock(h, expiryNow())
	h.startConditionPoll = time.Minute
	request := waitingRequest(1, fleet.StartCondition{Kind: "task", TaskID: "9000", State: "merged"})
	task, fresh, err := h.inbox.store.claimSpawn(t.Context(), request, "wait-job", "parent", *clock)
	if err != nil || !fresh || task.Status != "waiting" {
		t.Fatalf("claim: %+v fresh=%v err=%v; want waiting", task, fresh, err)
	}
	h.checkWaitingSpawns(t.Context())
	// Still waiting: not scheduled, no placement app, and the status API says why.
	if page, _, err := h.inbox.store.pendingSpawnPage(t.Context(), spawnScanPosition{}); err != nil || len(page) != 0 {
		t.Fatalf("a waiting request reached the placement queue: %+v %v", page, err)
	}
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	status, _, body := t14HTTP(t, http.MethodGet, server.URL+"/v1/spawns/"+request.RequestID, t14OperatorToken, nil)
	var view struct {
		Status         string `json:"status"`
		ExpiresAt      string `json:"expires_at"`
		StartCondition struct {
			Status  string   `json:"status"`
			Reasons []string `json:"reasons"`
		} `json:"start_condition"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &view) != nil || view.Status != "waiting" || view.ExpiresAt == "" || view.StartCondition.Status != "waiting" || len(view.StartCondition.Reasons) != 1 || view.StartCondition.Reasons[0] != "task:9000=merged" {
		t.Fatalf("status read %d %s", status, body)
	}
	read := hk.reads.Load()
	if read == 0 {
		t.Fatal("hk was not read for the waiting request")
	}
	// Inside the poll interval hk is not read again, even once the task merged.
	hk.state.Store("merged")
	*clock = clock.Add(30 * time.Second)
	h.checkWaitingSpawns(t.Context())
	if hk.reads.Load() != read {
		t.Fatal("hk was read again inside the poll interval")
	}
	if saved, _ := h.inbox.store.spawnTask(t.Context(), request.RequestID); saved.Status != "waiting" {
		t.Fatalf("released from a stale reading: %s", saved.Status)
	}
	*clock = clock.Add(31 * time.Second)
	h.checkWaitingSpawns(t.Context())
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "accepted" || !saved.Deadline.Equal(clock.Add(600*time.Second)) || saved.Attempt != 1 {
		t.Fatalf("after the condition held: %+v %v; want pending with a fresh landing wait", saved, err)
	}
	if page, _, err := h.inbox.store.pendingSpawnPage(t.Context(), spawnScanPosition{}); err != nil || len(page) != 1 || page[0].Request.RequestID != request.RequestID {
		t.Fatalf("a released request is not in the placement queue: %+v %v", page, err)
	}
	// A released request is not judged again: no start_condition in its status.
	_, _, body = t14HTTP(t, http.MethodGet, server.URL+"/v1/spawns/"+request.RequestID, t14OperatorToken, nil)
	if strings.Contains(string(body), "start_condition") || !strings.Contains(string(body), `"status":"accepted"`) {
		t.Fatalf("released status read: %s", body)
	}
}

func TestTimeConditionReleasesWithoutReadingHK_t4660(t *testing.T) {
	hk := startConditionServer(t, "merged")
	h := asyncSpawnHub(t)
	clock := expiryClock(h, expiryNow())
	at := clock.Add(30 * time.Minute)
	request := waitingRequest(2, fleet.StartCondition{Kind: "time", At: at})
	if task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "wait-job", "parent", *clock); err != nil || task.Status != "waiting" {
		t.Fatalf("claim: %+v %v", task, err)
	}
	*clock = at.Add(-time.Second)
	h.checkWaitingSpawns(t.Context())
	if saved, _ := h.inbox.store.spawnTask(t.Context(), request.RequestID); saved.Status != "waiting" {
		t.Fatalf("released early: %s", saved.Status)
	}
	*clock = at
	h.checkWaitingSpawns(t.Context())
	if saved, _ := h.inbox.store.spawnTask(t.Context(), request.RequestID); saved.Status != "accepted" {
		t.Fatalf("not released at its time: %s", saved.Status)
	}
	if hk.reads.Load() != 0 {
		t.Fatalf("a time condition read hk %d times", hk.reads.Load())
	}
}

// Several --after are ANDed: the time can hold while the task does not.
func TestStartConditionsAreAnded_t4660(t *testing.T) {
	startConditionServer(t, "ready")
	h := asyncSpawnHub(t)
	clock := expiryClock(h, expiryNow())
	request := waitingRequest(3, fleet.StartCondition{Kind: "time", At: clock.Add(time.Minute)}, fleet.StartCondition{Kind: "task", TaskID: "9000", State: "merged"})
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), request, "wait-job", "parent", *clock); err != nil {
		t.Fatal(err)
	}
	*clock = clock.Add(2 * time.Minute)
	h.checkWaitingSpawns(t.Context())
	if saved, _ := h.inbox.store.spawnTask(t.Context(), request.RequestID); saved.Status != "waiting" {
		t.Fatalf("the time alone released an ANDed request: %s", saved.Status)
	}
	if result := h.waitingStartCondition(request.RequestID); result == nil || len(result.Reasons) != 1 || result.Reasons[0] != "task:9000=merged" {
		t.Fatalf("waiting for %+v, want only the task", result)
	}
}

// A request that waits on a task hk dropped can never start: cancelled, with
// that reason, one notice and its reservation freed.
func TestStartConditionOnDroppedTaskCancels_t4660(t *testing.T) {
	startConditionServer(t, "dropped")
	h := asyncSpawnHub(t)
	clock := expiryClock(h, expiryNow())
	request := waitingRequest(4, fleet.StartCondition{Kind: "task", TaskID: "9000", State: "merged"})
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), request, "wait-job", "parent", *clock); err != nil {
		t.Fatal(err)
	}
	h.checkWaitingSpawns(t.Context())
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "cancelled" || !strings.Contains(saved.Outcome.Reason, "task:9000=dropped") || saved.Outcome.RC != 84 {
		t.Fatalf("%+v %v; want cancelled naming the dropped task", saved, err)
	}
	if held, _ := h.inbox.store.spawnJobReserved(t.Context(), "wait-job"); held {
		t.Fatal("a cancelled request kept its reservation")
	}
	var notices int
	if err := h.inbox.store.db.QueryRow(`SELECT count(*) FROM spawn_notifications WHERE request_id=?`, request.RequestID).Scan(&notices); err != nil || notices != 1 {
		t.Fatalf("notices=%d %v", notices, err)
	}
}

func TestWaitingRequestGivenUpAtItsLimit_t4660(t *testing.T) {
	startConditionServer(t, "ready")
	h := asyncSpawnHub(t)
	clock := expiryClock(h, expiryNow())
	request := waitingRequest(5, fleet.StartCondition{Kind: "task", TaskID: "9000", State: "merged"})
	request.ExpiresSeconds = 3600
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), request, "wait-job", "parent", *clock); err != nil {
		t.Fatal(err)
	}
	*clock = clock.Add(3599 * time.Second)
	h.checkWaitingSpawns(t.Context())
	if saved, _ := h.inbox.store.spawnTask(t.Context(), request.RequestID); saved.Status != "waiting" {
		t.Fatalf("given up before its limit: %s", saved.Status)
	}
	*clock = clock.Add(time.Second)
	h.checkWaitingSpawns(t.Context())
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "given_up" || !strings.HasPrefix(saved.Outcome.Reason, "expired") {
		t.Fatalf("%+v %v; want given_up", saved, err)
	}
	if held, _ := h.inbox.store.spawnJobReserved(t.Context(), "wait-job"); held {
		t.Fatal("a given_up request kept its reservation")
	}
}

// A restart keeps the limit and the waiting state (both are columns of the
// row), and the new hub judges the request on its next pass.
func TestRestartKeepsExpiryAndWaitingState_t4660(t *testing.T) {
	hk := startConditionServer(t, "ready")
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	t.Setenv(crashReportDSNEnv, "")
	open := func() *HubServer {
		h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken}, InboxStorePath: path})
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	first := open()
	now := expiryNow()
	request := waitingRequest(6, fleet.StartCondition{Kind: "task", TaskID: "9000", State: "merged"})
	request.ExpiresSeconds = 7200
	claimed, _, err := first.inbox.store.claimSpawn(t.Context(), request, "wait-job", "parent", now)
	if err != nil || claimed.Status != "waiting" {
		t.Fatalf("claim: %+v %v", claimed, err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := open()
	t.Cleanup(func() { second.Close() })
	expiryClock(second, now.Add(time.Hour))
	saved, err := second.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "waiting" || !saved.Expires.Equal(now.Add(2*time.Hour)) {
		t.Fatalf("after the restart: %+v %v; want waiting with expires_at %v", saved, err, now.Add(2*time.Hour))
	}
	second.checkWaitingSpawns(t.Context())
	if saved, _ := second.inbox.store.spawnTask(t.Context(), request.RequestID); saved.Status != "waiting" {
		t.Fatalf("released with the condition unmet: %s", saved.Status)
	}
	hk.state.Store("merged")
	expiryClock(second, now.Add(2*time.Hour))
	// Past the limit at the same pass: the limit wins, as it does for any request.
	second.checkWaitingSpawns(t.Context())
	if saved, _ := second.inbox.store.spawnTask(t.Context(), request.RequestID); saved.Status != "given_up" {
		t.Fatalf("after the limit: %s; want given_up", saved.Status)
	}
}

// A conditioned row an older hub left in accepted (never placed) goes back to
// waiting on start; placed and unconditioned rows are untouched.
func TestLegacyConditionedRowsWaitAgain_t4660(t *testing.T) {
	h := asyncSpawnHub(t)
	now := expiryNow()
	conditioned := waitingRequest(7, fleet.StartCondition{Kind: "task", TaskID: "9000", State: "merged"})
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), conditioned, "wait-job", "parent", now); err != nil {
		t.Fatal(err)
	}
	plain := durableSpawnRequest()
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), plain, "test-job", "parent", now); err != nil {
		t.Fatal(err)
	}
	// The older hub's claim had no waiting state.
	if _, err := h.inbox.store.db.Exec(`UPDATE spawn_operations SET status='accepted',expires_ms=0 WHERE request_id=?`, conditioned.RequestID); err != nil {
		t.Fatal(err)
	}
	if err := h.inbox.store.parkLegacyConditionedSpawns(t.Context()); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string]string{conditioned.RequestID: "waiting", plain.RequestID: "accepted"} {
		if saved, err := h.inbox.store.spawnTask(t.Context(), id); err != nil || saved.Status != want {
			t.Fatalf("%s: %+v %v; want %s", id, saved, err, want)
		}
	}
}

// What an older hub does with the new rows (docs/hub-spawn-operations.md,
// "Rolling back past the time limit"): the status queries it ran at v0.0.4xx
// list none of waiting, given_up or cancelled, so it never replays, sweeps,
// completes or releases them. The predicates are copied from the code of the
// commit before this part (e00bb952).
func TestOlderHubQueriesIgnoreWaitingAndGivenUpRows_t4660(t *testing.T) {
	h := asyncSpawnHub(t)
	now := expiryNow()
	rows := map[string]string{}
	for i, status := range []string{"waiting", "given_up", "cancelled"} {
		request := waitingRequest(10+i, fleet.StartCondition{Kind: "task", TaskID: "9000", State: "merged"})
		request.Args = replaceSpawnArg(append([]string(nil), request.Args...), "--job", "old-"+status)
		request.Args = replaceSpawnArg(request.Args, "-l", "old-"+status)
		if _, _, err := h.inbox.store.claimSpawn(t.Context(), request, "old-"+status, "parent", now); err != nil {
			t.Fatal(err)
		}
		if _, err := h.inbox.store.db.Exec(`UPDATE spawn_operations SET status=? WHERE request_id=?`, status, request.RequestID); err != nil {
			t.Fatal(err)
		}
		rows[status] = request.RequestID
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
			t.Errorf("older-hub query %q matches %d new rows, err=%v", name, n, err)
		}
	}
	// The row keeps its job reservation in a waiting row and frees it in the
	// ended ones; an older hub's GET passes the status through unchanged.
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	for status, id := range rows {
		code, _, body := t14HTTP(t, http.MethodGet, server.URL+"/v1/spawns/"+id, t14OperatorToken, nil)
		if code != http.StatusOK || !strings.Contains(string(body), `"status":"`+status+`"`) {
			t.Errorf("%s: %d %s", status, code, body)
		}
	}
}

// Waiting requests are in no placement query, so any number of them leaves
// the runnable page to the requests that can run (this replaces the old
// separate page for conditioned rows).
func TestWaitingRequestsDoNotStarveRunnableSpawn_t4660(t *testing.T) {
	h := asyncSpawnHub(t)
	now := expiryNow()
	for i := range 40 {
		request := waitingRequest(100+i, fleet.StartCondition{Kind: "task", TaskID: "9000", State: "merged"})
		job := "waiting-" + strconv.Itoa(i)
		request.Args = replaceSpawnArg(append([]string(nil), request.Args...), "--job", job)
		request.Args = replaceSpawnArg(request.Args, "-l", job)
		if _, _, err := h.inbox.store.claimSpawn(t.Context(), request, job, "parent", now.Add(time.Duration(i)*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
	}
	runnable := durableSpawnRequest()
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), runnable, "test-job", "parent", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	page, _, err := h.inbox.store.pendingSpawnPage(t.Context(), spawnScanPosition{})
	if err != nil || len(page) != 1 || page[0].Request.RequestID != runnable.RequestID {
		t.Fatalf("runnable page has %d rows, %v; waiting rows must not consume its slots", len(page), err)
	}
	waiting, err := h.inbox.store.waitingSpawns(t.Context(), spawnScanPosition{}, spawnWaitingScanLimit)
	if err != nil || len(waiting) != 40 {
		t.Fatalf("waiting pass has %d rows, %v; want 40", len(waiting), err)
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

// Rows an older hub left that carry a start condition (no limit) are never
// placed while their condition is unmet: unplaced ones wait for the park, and
// placed ones keep the old bound-row guard.
func TestLegacyConditionedRowsAreNeverStartedEarly_t4660(t *testing.T) {
	startConditionServer(t, "ready")
	h := asyncSpawnHub(t)
	created := false
	h.spawnNewApp = func(io.Writer, io.Writer) (*fleet.App, error) {
		created = true
		return nil, errors.New("must not be reached")
	}
	now := time.Now()
	legacy := func(n int, bind bool) hubSpawnTask {
		t.Helper()
		request := waitingRequest(200+n, fleet.StartCondition{Kind: "task", TaskID: "9000", State: "merged"})
		task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "wait-job-"+strconv.Itoa(n), "parent", now)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.inbox.store.db.Exec(`UPDATE spawn_operations SET status='accepted',expires_ms=0 WHERE request_id=?`, request.RequestID); err != nil {
			t.Fatal(err)
		}
		if bind {
			if task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, 1, boundTestNode(task), now.UTC()); err != nil {
				t.Fatal(err)
			}
			task = sentSpawnFixture(t, h.inbox.store, task)
		} else if task, err = h.inbox.store.spawnTask(t.Context(), request.RequestID); err != nil {
			t.Fatal(err)
		}
		return task
	}
	unplaced, sentRow := legacy(1, false), legacy(2, true)
	// The startup park failed (or has not run): the unplaced row is not placed.
	h.executeSpawnTask(t.Context(), unplaced)
	h.executeSpawnTask(t.Context(), sentRow)
	if created {
		t.Fatal("a legacy conditioned row reached placement or replay with its condition unmet")
	}
	for _, task := range []hubSpawnTask{unplaced, sentRow} {
		if saved, _ := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID); saved.Status == "failed" || saved.Attempt != 1 {
			t.Fatalf("%s was disturbed: %+v", task.Job, saved)
		}
	}
	// The waiting loop parks the unplaced row (retrying until it succeeds).
	if _, err := h.inbox.store.db.Exec(`CREATE TRIGGER fail_park BEFORE UPDATE ON spawn_operations BEGIN SELECT RAISE(FAIL, 'injected park failure'); END`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { h.runWaitingSpawns(ctx); close(done) }()
	time.Sleep(50 * time.Millisecond) // proves nothing more happens while the park keeps failing
	if saved, _ := h.inbox.store.spawnTask(t.Context(), unplaced.Request.RequestID); saved.Status != "accepted" {
		t.Fatalf("parked through a failing write: %s", saved.Status)
	}
	if _, err := h.inbox.store.db.Exec(`DROP TRIGGER fail_park`); err != nil {
		t.Fatal(err)
	}
	eventually(t, 6*time.Second, "the unplaced legacy row to be parked", func() bool {
		saved, err := h.inbox.store.spawnTask(t.Context(), unplaced.Request.RequestID)
		return err == nil && saved.Status == "waiting"
	})
	cancel()
	<-done
	// Once released, a legacy row gets a limit and is an ordinary request.
	waiting, _ := h.inbox.store.spawnTask(t.Context(), unplaced.Request.RequestID)
	if released, err := h.inbox.store.releaseWaitingSpawn(t.Context(), waiting, now.UTC()); err != nil || !released {
		t.Fatalf("release: %v %v", released, err)
	}
	if saved, _ := h.inbox.store.spawnTask(t.Context(), unplaced.Request.RequestID); saved.Status != "accepted" || saved.Expires.IsZero() {
		t.Fatalf("a released legacy row has no limit: %+v", saved)
	}
}

// Passes continue from a cursor: with more waiting rows than one pass reads,
// a later row whose time has come is still released, and a later row past its
// limit is still given up, while older rows keep waiting.
func TestWaitingScanReachesRowsPastThePageLimit_t4660(t *testing.T) {
	h := asyncSpawnHub(t)
	clock := expiryClock(h, expiryNow())
	far := clock.Add(48 * time.Hour)
	total := spawnWaitingScanLimit*2 + 30
	var ready, expiring []string
	for i := 0; i < total; i++ {
		request := waitingRequest(300+i, fleet.StartCondition{Kind: "time", At: far})
		request.Args = replaceSpawnArg(append([]string(nil), request.Args...), "--job", "scan-"+strconv.Itoa(i))
		request.Args = replaceSpawnArg(request.Args, "-l", "scan-"+strconv.Itoa(i))
		switch {
		case i == total-1:
			request.StartConditions = []fleet.StartCondition{{Kind: "time", At: clock.Add(time.Minute)}}
			ready = append(ready, request.RequestID)
		case i == total-2:
			request.ExpiresSeconds = 120
			expiring = append(expiring, request.RequestID)
		}
		if _, _, err := h.inbox.store.claimSpawn(t.Context(), request, "scan-"+strconv.Itoa(i), "parent", clock.Add(time.Duration(i)*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
	}
	*clock = clock.Add(5 * time.Minute)
	for pass := 0; pass < 4; pass++ {
		h.checkWaitingSpawns(t.Context())
	}
	if saved, _ := h.inbox.store.spawnTask(t.Context(), ready[0]); saved.Status != "accepted" {
		t.Fatalf("the last waiting row was never judged: %s", saved.Status)
	}
	if saved, _ := h.inbox.store.spawnTask(t.Context(), expiring[0]); saved.Status != "given_up" {
		t.Fatalf("the last waiting row's limit was never judged: %s", saved.Status)
	}
	if saved, _ := h.inbox.store.spawnTask(t.Context(), t14RequestID(4600+300)); saved.Status != "waiting" {
		t.Fatalf("the oldest row left waiting early: %s", saved.Status)
	}
}

// An hk that answers nothing for one task does not keep the others unread:
// each id is tried at most once per poll interval, answered or not.
func TestUnreachableTaskDoesNotStarveOtherConditions_t4660(t *testing.T) {
	hk := startConditionServer(t, "merged")
	h := asyncSpawnHub(t)
	clock := expiryClock(h, expiryNow())
	h.startConditionPoll = time.Minute
	// Task 9002 is unknown to the server (404); 9000 answers merged.
	missing := waitingRequest(400, fleet.StartCondition{Kind: "task", TaskID: "9002", State: "merged"})
	ok := waitingRequest(401, fleet.StartCondition{Kind: "task", TaskID: "9000", State: "merged"})
	for _, request := range []fleet.Submission{missing, ok} {
		job := "job-" + request.RequestID[len(request.RequestID)-4:]
		request.Args = replaceSpawnArg(append([]string(nil), request.Args...), "--job", job)
		request.Args = replaceSpawnArg(request.Args, "-l", job)
		if _, _, err := h.inbox.store.claimSpawn(t.Context(), request, job, "parent", *clock); err != nil {
			t.Fatal(err)
		}
	}
	h.checkWaitingSpawns(t.Context())
	if saved, _ := h.inbox.store.spawnTask(t.Context(), ok.RequestID); saved.Status != "accepted" {
		t.Fatalf("the readable condition was not released: %s", saved.Status)
	}
	reads := hk.reads.Load()
	h.checkWaitingSpawns(t.Context())
	*clock = clock.Add(10 * time.Second)
	h.checkWaitingSpawns(t.Context())
	if hk.reads.Load() != reads {
		t.Fatalf("an unanswered task was re-read inside the poll interval: %d -> %d", reads, hk.reads.Load())
	}
	if result := h.waitingStartCondition(missing.RequestID); result == nil || result.Reasons[0] != "hk_unavailable" {
		t.Fatalf("an unread task must fail closed: %+v", result)
	}
}

package panewire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// startConditionSnapshot is a fake handoffkeep snapshot route: it answers
// GET /v1/tasks/states/snapshot with one entry per requested id or names it
// missing, counts the calls one check makes, and can fail them.
type startConditionSnapshot struct {
	mu      sync.Mutex
	states  map[string]string
	missing map[string]bool
	calls   int
	fail    bool
	srv     *httptest.Server
}

func newStartConditionSnapshot(t *testing.T) *startConditionSnapshot {
	t.Helper()
	s := &startConditionSnapshot{states: map[string]string{}, missing: map[string]bool{}}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if r.URL.Path != "/v1/tasks/states/snapshot" {
			http.NotFound(w, r)
			return
		}
		s.calls++
		if s.fail {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var tasks, absent []string
		for _, id := range strings.Split(r.URL.Query().Get("ids"), ",") {
			if s.missing[id] {
				absent = append(absent, id)
				continue
			}
			state, ok := s.states[id]
			if !ok {
				state = "ready"
			}
			tasks = append(tasks, fmt.Sprintf(`{"id":%s,"state":%q,"category":"started","blockers":[]}`, id, state))
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"as_of":"2026-11-15T00:00:00Z","tasks":[%s],"missing":[%s]}`, strings.Join(tasks, ","), strings.Join(absent, ","))
	}))
	t.Cleanup(s.srv.Close)
	t.Setenv("HANDOFFKEEP_URL", s.srv.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "start-condition-test")
	return s
}

func (s *startConditionSnapshot) setState(id, state string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[id] = state
}

func (s *startConditionSnapshot) setMissing(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.missing[id] = true
}

func (s *startConditionSnapshot) setFail(fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail = fail
}

func (s *startConditionSnapshot) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// conditionClock is a hub clock a test advances by hand.
type conditionClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *conditionClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *conditionClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func conditionedSpawnHub(t *testing.T) (*HubServer, *conditionClock) {
	t.Helper()
	h := asyncSpawnHub(t)
	clock := &conditionClock{now: time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)}
	h.now = clock.Now
	return h, clock
}

func waitingSpawnFixture(t *testing.T, h *HubServer, seq int, conditions []fleet.StartCondition) hubSpawnTask {
	t.Helper()
	request := durableSpawnRequest()
	request.RequestID = t14RequestID(4700 + seq)
	job := fmt.Sprintf("conditioned-%d", seq)
	request.Args = replaceSpawnArg(request.Args, "--job", job)
	request.Args = replaceSpawnArg(request.Args, "--task", strconv.Itoa(5000+seq))
	request.StartConditions = conditions
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, job, "parent", h.now().UTC())
	if err != nil {
		t.Fatalf("claim conditioned %d: %v", seq, err)
	}
	if task.Status != "waiting" {
		t.Fatalf("conditioned claim %d status = %q, want waiting", seq, task.Status)
	}
	return task
}

func countSpawnStatus(t *testing.T, h *HubServer, status string) int {
	t.Helper()
	var n int
	if err := h.inbox.store.db.QueryRow(`SELECT COUNT(*) FROM spawn_operations WHERE status=?`, status).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func spawnStatusOf(t *testing.T, h *HubServer, requestID string) hubSpawnTask {
	t.Helper()
	task, err := h.inbox.store.spawnTask(t.Context(), requestID)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func TestStartConditionWaitsThenReleasesOnMergedAndDeployed(t *testing.T) {
	hk := newStartConditionSnapshot(t)
	h, clock := conditionedSpawnHub(t)
	merged := waitingSpawnFixture(t, h, 1, []fleet.StartCondition{{Kind: "task", TaskID: "42", State: "merged"}})
	deployed := waitingSpawnFixture(t, h, 2, []fleet.StartCondition{{Kind: "task", TaskID: "43", State: "deployed"}})
	hk.setState("42", "ready")
	hk.setState("43", "merged")

	h.checkWaitingSpawns(t.Context())
	if got := spawnStatusOf(t, h, merged.Request.RequestID); got.Status != "waiting" {
		t.Fatalf("row before its task merged = %q, want waiting", got.Status)
	}
	if got := spawnStatusOf(t, h, deployed.Request.RequestID); got.Status != "waiting" {
		t.Fatalf("row before its task deployed = %q, want waiting", got.Status)
	}

	hk.setState("42", "merged")
	hk.setState("43", "deployed")
	clock.Advance(waitingSpawnPoll)
	h.checkWaitingSpawns(t.Context())
	for _, want := range []string{merged.Request.RequestID, deployed.Request.RequestID} {
		got := spawnStatusOf(t, h, want)
		if got.Status != "accepted" {
			t.Fatalf("released row %s status = %q, want accepted", want, got.Status)
		}
		if !got.Deadline.After(clock.Now()) {
			t.Fatalf("released row %s deadline %v is not a fresh attempt at %v", want, got.Deadline, clock.Now())
		}
	}
	if hk.callCount() != 2 {
		t.Fatalf("snapshot calls = %d, want one per check", hk.callCount())
	}
}

func TestStartConditionTimeReleasesOnTheClock(t *testing.T) {
	hk := newStartConditionSnapshot(t)
	h, clock := conditionedSpawnHub(t)
	at := clock.Now().Add(2 * time.Minute)
	task := waitingSpawnFixture(t, h, 1, []fleet.StartCondition{{Kind: "time", At: at}})

	h.checkWaitingSpawns(t.Context())
	if got := spawnStatusOf(t, h, task.Request.RequestID); got.Status != "waiting" {
		t.Fatalf("time condition before its time = %q, want waiting", got.Status)
	}

	clock.Advance(2 * time.Minute)
	h.checkWaitingSpawns(t.Context())
	if got := spawnStatusOf(t, h, task.Request.RequestID); got.Status != "accepted" {
		t.Fatalf("time condition at its time = %q, want accepted", got.Status)
	}
	if hk.callCount() != 0 {
		t.Fatalf("time-only condition made %d hk calls, want none", hk.callCount())
	}
}

func TestStartConditionMissingTaskIsRefused(t *testing.T) {
	hk := newStartConditionSnapshot(t)
	h, _ := conditionedSpawnHub(t)
	task := waitingSpawnFixture(t, h, 1, []fleet.StartCondition{{Kind: "task", TaskID: "42", State: "merged"}})
	hk.setMissing("42")

	h.checkWaitingSpawns(t.Context())
	got := spawnStatusOf(t, h, task.Request.RequestID)
	if got.Status != "refused" {
		t.Fatalf("missing task id status = %q, want refused", got.Status)
	}
	if got.Outcome.RC != 78 || got.Outcome.Reason != "task:42=missing" {
		t.Fatalf("missing task id outcome = %#v, want rc 78 and the missing reason", got.Outcome)
	}
	var reserved int
	if err := h.inbox.store.db.QueryRow(`SELECT COUNT(*) FROM spawn_reservations WHERE request_id=?`, task.Request.RequestID).Scan(&reserved); err != nil {
		t.Fatal(err)
	}
	if reserved != 0 {
		t.Fatalf("refused row still holds its reservation")
	}
}

func TestStartConditionHKFailureKeepsWaitingAndLogsOnce(t *testing.T) {
	hk := newStartConditionSnapshot(t)
	h, clock := conditionedSpawnHub(t)
	task := waitingSpawnFixture(t, h, 1, []fleet.StartCondition{{Kind: "task", TaskID: "42", State: "merged"}})
	var logs bytes.Buffer
	h.logger = slog.New(slog.NewTextHandler(&logs, nil))
	hk.setFail(true)

	h.checkWaitingSpawns(t.Context())
	if got := spawnStatusOf(t, h, task.Request.RequestID); got.Status != "waiting" {
		t.Fatalf("hk failure changed the row to %q, want waiting", got.Status)
	}
	if hk.callCount() != 1 {
		t.Fatalf("snapshot calls after one check = %d, want 1", hk.callCount())
	}
	if got := strings.Count(logs.String(), "start conditions unavailable"); got != 1 {
		t.Fatalf("failure logged %d times, want once: %s", got, logs.String())
	}

	// The same clock is inside the poll interval: no second read, no second log.
	h.checkWaitingSpawns(t.Context())
	if hk.callCount() != 1 {
		t.Fatalf("a second check inside the poll interval made %d calls, want 1", hk.callCount())
	}
	if got := strings.Count(logs.String(), "start conditions unavailable"); got != 1 {
		t.Fatalf("failure logged %d times after a second check, want once", got)
	}

	clock.Advance(waitingSpawnPoll)
	h.checkWaitingSpawns(t.Context())
	if hk.callCount() != 2 {
		t.Fatalf("snapshot calls after the next poll = %d, want 2", hk.callCount())
	}
	if got := strings.Count(logs.String(), "start conditions unavailable"); got != 1 {
		t.Fatalf("one outage logged %d times, want once", got)
	}

	hk.setFail(false)
	hk.setState("42", "merged")
	clock.Advance(waitingSpawnPoll)
	h.checkWaitingSpawns(t.Context())
	if got := spawnStatusOf(t, h, task.Request.RequestID); got.Status != "accepted" {
		t.Fatalf("recovered hk did not release the row: %q", got.Status)
	}
}

func TestStartConditions250IdsReadInTwoSnapshotCalls(t *testing.T) {
	hk := newStartConditionSnapshot(t)
	h, _ := conditionedSpawnHub(t)
	for i := range 250 {
		id := strconv.Itoa(1000 + i)
		hk.setState(id, "merged")
		waitingSpawnFixture(t, h, i, []fleet.StartCondition{{Kind: "task", TaskID: id, State: "merged"}})
	}

	h.checkWaitingSpawns(t.Context())
	if got := hk.callCount(); got != 2 {
		t.Fatalf("250 waiting ids made %d snapshot calls, want 2", got)
	}
	if got := countSpawnStatus(t, h, "waiting"); got != 0 {
		t.Fatalf("%d rows still waiting after their tasks merged, want 0", got)
	}
	if got := countSpawnStatus(t, h, "accepted"); got != 250 {
		t.Fatalf("%d released rows, want 250", got)
	}
}

func TestWaitingRequestGivenUpAtItsLimit(t *testing.T) {
	hk := newStartConditionSnapshot(t)
	h, clock := conditionedSpawnHub(t)
	request := durableSpawnRequest()
	request.RequestID = t14RequestID(4900)
	request.Args = replaceSpawnArg(request.Args, "--job", "expiring-condition")
	request.StartConditions = []fleet.StartCondition{{Kind: "task", TaskID: "42", State: "merged"}}
	request.ExpiresSeconds = 60
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "expiring-condition", "parent", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	hk.setState("42", "ready")

	h.checkWaitingSpawns(t.Context())
	if got := spawnStatusOf(t, h, task.Request.RequestID); got.Status != "waiting" {
		t.Fatalf("row before its limit = %q, want waiting", got.Status)
	}

	clock.Advance(61 * time.Second)
	h.checkWaitingSpawns(t.Context())
	got := spawnStatusOf(t, h, task.Request.RequestID)
	if got.Status != "given_up" {
		t.Fatalf("row past its limit = %q, want given_up", got.Status)
	}
	if got.Outcome.RC != 84 || !strings.HasPrefix(got.Outcome.Reason, "expired:") {
		t.Fatalf("given_up outcome = %#v, want rc 84 and an expired reason", got.Outcome)
	}
}

func TestWaitingRowsSurviveRestart(t *testing.T) {
	hk := newStartConditionSnapshot(t)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	config := HubServerConfig{
		Tokens:         map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"},
		InboxStorePath: path,
		Logger:         slog.New(slog.NewTextHandler(os.Stderr, nil)),
	}
	t.Setenv(crashReportDSNEnv, "")
	first, err := NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	task := waitingSpawnFixture(t, first, 1, []fleet.StartCondition{{Kind: "task", TaskID: "42", State: "merged"}})
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { second.Close() })
	if got := spawnStatusOf(t, second, task.Request.RequestID); got.Status != "waiting" {
		t.Fatalf("after restart row status = %q, want waiting", got.Status)
	}
	hk.setState("42", "merged")
	second.checkWaitingSpawns(t.Context())
	if got := spawnStatusOf(t, second, task.Request.RequestID); got.Status != "accepted" {
		t.Fatalf("restarted hub did not release the waiting row: %q", got.Status)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	third, err := NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { third.Close() })
	if got := spawnStatusOf(t, third, task.Request.RequestID); got.Status != "accepted" {
		t.Fatalf("a later open moved a released row back to %q", got.Status)
	}
}

func TestWaitingRowsDoNotConsumeTheRunnablePage(t *testing.T) {
	newStartConditionSnapshot(t)
	h, _ := conditionedSpawnHub(t)
	waitingSpawnFixture(t, h, 1, []fleet.StartCondition{{Kind: "task", TaskID: "42", State: "merged"}})
	runnable := durableSpawnRequest()
	runnable.RequestID = t14RequestID(4800)
	runnable.Args = replaceSpawnArg(runnable.Args, "--job", "runnable")
	runnable.Args = replaceSpawnArg(runnable.Args, "--task", "9100")
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), runnable, "runnable", "parent", h.now().UTC()); err != nil {
		t.Fatal(err)
	}
	page, err := h.inbox.store.pendingSpawns(t.Context(), h.now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1 || page[0].Request.RequestID != runnable.RequestID {
		t.Fatalf("runnable page = %#v, want only the unconditioned row", page)
	}
}

func TestStartConditionSubmissionIsAcceptedAsWaiting(t *testing.T) {
	newStartConditionSnapshot(t)
	h, _ := conditionedSpawnHub(t)
	features := httptest.NewRecorder()
	featuresRequest := httptest.NewRequest(http.MethodGet, "/v1/spawns/features", nil)
	featuresRequest.Header.Set("Authorization", "Bearer "+t14OperatorToken)
	h.Handler().ServeHTTP(features, featuresRequest)
	if got := features.Body.String(); !strings.Contains(got, "start_conditions_v1") {
		t.Fatalf("features = %s, want start_conditions_v1", got)
	}
	request := durableSpawnRequest()
	request.RequestID = t14RequestID(4950)
	request.StartConditions = []fleet.StartCondition{{Kind: "task", TaskID: "42", State: "merged"}}
	body, _ := json.Marshal(request)
	r := httptest.NewRequest(http.MethodPost, "/v1/spawns", bytes.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+t14OperatorToken)
	w := httptest.NewRecorder()
	h.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusAccepted || !strings.Contains(w.Body.String(), `"status":"waiting"`) {
		t.Fatalf("conditioned submit = %d %s, want 202 waiting", w.Code, w.Body)
	}
}

func TestWaitingRowGivenUpDuringHKOutage(t *testing.T) {
	hk := newStartConditionSnapshot(t)
	h, clock := conditionedSpawnHub(t)
	request := durableSpawnRequest()
	request.RequestID = t14RequestID(4970)
	request.Args = replaceSpawnArg(request.Args, "--job", "expiring-outage")
	request.StartConditions = []fleet.StartCondition{{Kind: "task", TaskID: "42", State: "merged"}}
	request.ExpiresSeconds = 60
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "expiring-outage", "parent", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	hk.setFail(true)

	h.checkWaitingSpawns(t.Context())
	if got := spawnStatusOf(t, h, task.Request.RequestID); got.Status != "waiting" {
		t.Fatalf("row before its limit = %q, want waiting", got.Status)
	}
	clock.Advance(61 * time.Second)
	h.checkWaitingSpawns(t.Context())
	got := spawnStatusOf(t, h, task.Request.RequestID)
	if got.Status != "given_up" {
		t.Fatalf("expired row during an hk outage = %q, want given_up", got.Status)
	}
	if hk.callCount() != 1 {
		t.Fatalf("snapshot calls = %d: expiry must not wait for hk", hk.callCount())
	}
}

func TestUnboundConditionedRowReturnsToWaiting(t *testing.T) {
	newStartConditionSnapshot(t)
	h, _ := conditionedSpawnHub(t)
	task := waitingSpawnFixture(t, h, 1, []fleet.StartCondition{{Kind: "task", TaskID: "42", State: "merged"}})
	for _, status := range []string{"accepted", "deadline_exceeded"} {
		if _, err := h.inbox.store.db.Exec(`UPDATE spawn_operations SET status=?,conditions_met_ms=0 WHERE request_id=?`, status, task.Request.RequestID); err != nil {
			t.Fatal(err)
		}
		current := spawnStatusOf(t, h, task.Request.RequestID)
		if current.Status != status {
			t.Fatalf("fixture row = %q, want %q", current.Status, status)
		}
		h.spawnNewApp = func(io.Writer, io.Writer) (*fleet.App, error) {
			t.Error("a conditioned row without a met condition reached spawn execution")
			return nil, nil
		}
		h.executeSpawnTask(t.Context(), current)
		if got := spawnStatusOf(t, h, task.Request.RequestID); got.Status != "waiting" {
			t.Fatalf("unbound conditioned row %q stayed %q, want waiting", status, got.Status)
		}
	}
}

func TestWaitingStatusReportsBlockingPredicates(t *testing.T) {
	newStartConditionSnapshot(t)
	h, _ := conditionedSpawnHub(t)
	task := waitingSpawnFixture(t, h, 1, []fleet.StartCondition{{Kind: "task", TaskID: "42", State: "merged"}})
	h.checkWaitingSpawns(t.Context())

	r := httptest.NewRequest(http.MethodGet, "/v1/spawns/"+task.Request.RequestID, nil)
	r.Header.Set("Authorization", "Bearer "+t14OperatorToken)
	w := httptest.NewRecorder()
	h.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"start_condition":{"status":"waiting","reasons":["task:42=merged"]}`) {
		t.Fatalf("waiting status = %d %s, want the blocking predicate", w.Code, w.Body)
	}
}

func TestWaitingSweepLeavesBoundConditionedRowsAlone(t *testing.T) {
	newStartConditionSnapshot(t)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	t.Setenv(crashReportDSNEnv, "")
	config := HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"}, InboxStorePath: path}
	h, err := NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	bound := waitingSpawnFixture(t, h, 1, []fleet.StartCondition{{Kind: "task", TaskID: "42", State: "merged"}})
	unbound := waitingSpawnFixture(t, h, 2, []fleet.StartCondition{{Kind: "task", TaskID: "43", State: "merged"}})
	// A legacy row the old hub bound and let exceed its deadline: its node may
	// already be running the request, so the sweep must not move it.
	if _, err := h.inbox.store.db.Exec(`UPDATE spawn_operations SET status='deadline_exceeded',node_id='node-1',node_payload='{"request_id":"node-1","machine":"machine-a"}' WHERE request_id=?`, bound.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.inbox.store.db.Exec(`UPDATE spawn_operations SET status='accepted' WHERE request_id=?`, unbound.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}

	store := openSpawnStore(t, path)
	reopened, err := store.spawnTask(t.Context(), bound.Request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Status != "deadline_exceeded" || reopened.Node == nil || reopened.Node.RequestID != "node-1" {
		t.Fatalf("bound conditioned row after the sweep = status %q node %+v, want deadline_exceeded still bound", reopened.Status, reopened.Node)
	}
	moved, err := store.spawnTask(t.Context(), unbound.Request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if moved.Status != "waiting" {
		t.Fatalf("unbound conditioned row after the sweep = %q, want waiting", moved.Status)
	}
}

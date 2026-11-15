package panewire

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// hk 4662: one owner notice for an urgent (hk priority 1 or 2) request parked
// past spawn_park_notice_seconds. Design: hk
// design/2026-11-13/hub-owned-spawn-queue §6 notices and §9b Q2;
// docs/hub-spawn-operations.md#parked-notice.

// parkNoticePriority is a fake handoffkeep task route: GET /v1/tasks/{id}
// answers the task with a fixed priority, so a test controls the one hk read
// the parked notice makes, counts it, and can fail it as an outage would.
type parkNoticePriority struct {
	mu         sync.Mutex
	priorities map[string]int
	unreadable map[string]bool
	broken     map[string]bool
	read       map[string]int
	fail       bool
	srv        *httptest.Server
}

func newParkNoticePriority(t *testing.T, priorities map[string]int) *parkNoticePriority {
	t.Helper()
	p := &parkNoticePriority{priorities: priorities, read: map[string]int{}}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/v1/tasks/")
		p.mu.Lock()
		priority, known := p.priorities[id]
		p.read[id]++
		fail := p.fail
		p.mu.Unlock()
		if fail {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		p.mu.Lock()
		broken := p.broken[id]
		p.mu.Unlock()
		if broken {
			// A 5xx for this one record: about the row, not the service.
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		p.mu.Lock()
		unreadable := p.unreadable[id]
		p.mu.Unlock()
		if unreadable {
			// A 2xx answer the task parser cannot read: about this task, not
			// the service.
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"id":`)
			return
		}
		if !known || id == "" || strings.Contains(id, "/") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id":%s,"state":"developing","priority":%d}`, id, priority)
	}))
	t.Cleanup(p.srv.Close)
	t.Setenv("HANDOFFKEEP_URL", p.srv.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "park-notice-test")
	return p
}

func (p *parkNoticePriority) reads(id string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.read[id]
}

func (p *parkNoticePriority) setBroken(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.broken == nil {
		p.broken = map[string]bool{}
	}
	p.broken[id] = true
}

func (p *parkNoticePriority) setUnreadable(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.unreadable == nil {
		p.unreadable = map[string]bool{}
	}
	p.unreadable[id] = true
}

func (p *parkNoticePriority) setFail(fail bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fail = fail
}

// parkNoticeHub is a hub whose owner lane "parent" routes to a connected node,
// publishing through the given store path and reading the given clock, so one
// test can park, notice and restart on the same state.
func parkNoticeHub(t *testing.T, storePath string, client *handoffkeepRelayClient, clock *time.Time) (*HubServer, *hubAgent) {
	t.Helper()
	t.Setenv(crashReportDSNEnv, "")
	config := HubServerConfig{
		Tokens:          map[string]string{"operator": t14OperatorToken, "machine-a": "node-token", "machine-b": "desk-token"},
		InboxStorePath:  storePath,
		ReportRelayPath: r20LanesFile(t, `{"lanes":{"parent":{"machine":"machine-b","pane":"w1:p1"}}}`),
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		handoffkeep:     client,
		Now:             func() time.Time { return *clock },
	}
	h, err := NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 8), persisted: make(chan hubRelayPersistedEvent, 8)}
	h.mu.Lock()
	h.nodes["machine-b"] = &hubNodeRecord{agent: destination}
	h.mu.Unlock()
	return h, destination
}

func checkParkNotices(t *testing.T, h *HubServer) {
	t.Helper()
	h.checkParkedSpawnNotices(t.Context())
}

func TestSpawnParkNoticeTellsUrgentOwnerOnce_t4662(t *testing.T) {
	clock := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	hk := newParkNoticePriority(t, map[string]int{"466101": 1})
	_, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	h, destination := parkNoticeHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), client, &clock)
	task := claimParkRequest(t, h, 1, clock)
	if !parkNow(t, h, task, spawnParkQuota) {
		t.Fatal("the park was refused")
	}
	parked := parkedTask(t, h, task.Request.RequestID)
	// Inside the limit: nothing is sent and hk is not asked.
	checkParkNotices(t, h)
	if got := spawnFailedInjections(destination); len(got) != 0 {
		t.Fatalf("a park inside the limit sent %+v", got)
	}
	if reads := hk.reads("466101"); reads != 0 {
		t.Fatalf("priority read %d times inside the limit", reads)
	}
	// Past the limit: exactly one notice, naming the reason, the wait start,
	// the next look and what would unpark it.
	clock = clock.Add(31 * time.Minute)
	checkParkNotices(t, h)
	injected := spawnFailedInjections(destination)
	if len(injected) != 1 {
		t.Fatalf("owner lane received %d injections, want 1: %+v", len(injected), injected)
	}
	for _, want := range []string{
		"SPAWN_PARKED", task.Job, "request=" + task.Request.RequestID, "reason=" + spawnParkQuota,
		"parked_since=" + parked.ParkSince.UTC().Format(time.RFC3339),
		"next_check_at=" + parked.NextCheck.UTC().Format(time.RFC3339),
		"unpark=" + spawnParkWakes[spawnParkQuota],
	} {
		if !strings.Contains(injected[0].Text, want) {
			t.Fatalf("notice omits %q: %q", want, injected[0].Text)
		}
	}
	if injected[0].Pane != "w1:p1" {
		t.Fatalf("notice went to pane %q", injected[0].Pane)
	}
	// Still parked for the same reason: no repeat, however often the pass runs.
	for range 2 {
		clock = clock.Add(2 * time.Minute)
		checkParkNotices(t, h)
	}
	if got := spawnFailedInjections(destination); len(got) != 0 {
		t.Fatalf("a still-parked request was told again: %+v", got)
	}
	if reads := hk.reads("466101"); reads != 1 {
		t.Fatalf("priority read %d times, want 1", reads)
	}
	// A re-park of the same reason is the same judged reason set.
	if !parkNow(t, h, parkedTask(t, h, task.Request.RequestID), spawnParkQuota) {
		t.Fatal("the same-reason re-park was refused")
	}
	clock = clock.Add(2 * time.Minute)
	checkParkNotices(t, h)
	if got := spawnFailedInjections(destination); len(got) != 0 {
		t.Fatalf("a same-reason re-park was told again: %+v", got)
	}
	// A new reason set may send one more.
	if !parkNow(t, h, parkedTask(t, h, task.Request.RequestID), spawnParkHostFull+","+spawnParkQuota) {
		t.Fatal("the new-reason park was refused")
	}
	clock = clock.Add(2 * time.Minute)
	checkParkNotices(t, h)
	injected = spawnFailedInjections(destination)
	if len(injected) != 1 {
		t.Fatalf("a new reason sent %d injections, want 1: %+v", len(injected), injected)
	}
	if !strings.Contains(injected[0].Text, "reason="+spawnParkHostFull+","+spawnParkQuota) {
		t.Fatalf("the second notice names the wrong reason: %q", injected[0].Text)
	}
	if reads := hk.reads("466101"); reads != 1 {
		t.Fatalf("a re-park re-read the priority: %d reads", reads)
	}
	// And that reason set is settled too.
	clock = clock.Add(2 * time.Minute)
	checkParkNotices(t, h)
	if got := spawnFailedInjections(destination); len(got) != 0 {
		t.Fatalf("the second reason was told twice: %+v", got)
	}
}

func TestSpawnParkNoticeSkipsNormalPriority_t4662(t *testing.T) {
	clock := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	hk := newParkNoticePriority(t, map[string]int{"466102": 3})
	_, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	h, destination := parkNoticeHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), client, &clock)
	task := claimParkRequest(t, h, 2, clock)
	if !parkNow(t, h, task, spawnParkQuota) {
		t.Fatal("the park was refused")
	}
	clock = clock.Add(31 * time.Minute)
	checkParkNotices(t, h)
	if got := spawnFailedInjections(destination); len(got) != 0 {
		t.Fatalf("a P3 park sent %+v", got)
	}
	row := parkedTask(t, h, task.Request.RequestID)
	if row.ParkPriority != 3 || !row.ParkNoticeAt.IsZero() || row.ParkJudgedReason != spawnParkQuota {
		t.Fatalf("P3 park stored %+v, want priority 3, no notice and a judged reason set", row)
	}
	// The priority is read once, and the judged row leaves the candidate page.
	clock = clock.Add(2 * time.Minute)
	checkParkNotices(t, h)
	if reads := hk.reads("466102"); reads != 1 {
		t.Fatalf("priority read %d times, want 1", reads)
	}
	rows, err := h.inbox.store.parkedSpawnNoticeCandidates(t.Context(), clock, spawnParkNoticeCursor{}, spawnParkNoticePage)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("a judged P3 row stays a notice candidate: %+v", rows)
	}
}

// A backlog of noticed-non-urgent rows must not crowd an urgent request parked
// behind them out of the candidate page (hk 4662 review: starvation).
func TestSpawnParkNoticeJudgedRowsMakeProgress_t4662(t *testing.T) {
	clock := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	priorities := map[string]int{}
	for n := 1; n <= spawnParkNoticePage; n++ {
		priorities[fmt.Sprint(466100+n)] = 3
	}
	priorities[fmt.Sprint(466100+spawnParkNoticePage+1)] = 1
	hk := newParkNoticePriority(t, priorities)
	_, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	h, destination := parkNoticeHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), client, &clock)
	for n := 1; n <= spawnParkNoticePage; n++ {
		task := claimParkRequest(t, h, n, clock)
		if !parkNow(t, h, task, spawnParkQuota) {
			t.Fatalf("park %d was refused", n)
		}
	}
	// The urgent request is parked later, so it sorts behind the whole page.
	clock = clock.Add(time.Second)
	urgent := claimParkRequest(t, h, spawnParkNoticePage+1, clock)
	if !parkNow(t, h, urgent, spawnParkQuota) {
		t.Fatal("the urgent park was refused")
	}
	clock = clock.Add(31 * time.Minute)
	// The first page is the whole P3 backlog: judged, told nothing, and each
	// leaves the page, so the next pass reaches the urgent request.
	checkParkNotices(t, h)
	if got := spawnFailedInjections(destination); len(got) != 0 {
		t.Fatalf("the P3 backlog sent %d notices", len(got))
	}
	clock = clock.Add(2 * time.Minute)
	checkParkNotices(t, h)
	injected := spawnFailedInjections(destination)
	if len(injected) != 1 {
		t.Fatalf("the urgent request behind the backlog got %d notices, want 1", len(injected))
	}
	if !strings.Contains(injected[0].Text, "request="+urgent.Request.RequestID) {
		t.Fatalf("the notice names the wrong request: %q", injected[0].Text)
	}
	if reads := hk.reads(fmt.Sprint(466100 + spawnParkNoticePage + 1)); reads != 1 {
		t.Fatalf("the urgent priority was read %d times, want 1", reads)
	}
}

// An hk outage judges nothing and loses nothing: the pass stops on the failed
// read and the next pass tells the owner once hk answers.
func TestSpawnParkNoticeRetriesAnHkOutage_t4662(t *testing.T) {
	clock := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	hk := newParkNoticePriority(t, map[string]int{"466101": 1})
	_, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	h, destination := parkNoticeHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), client, &clock)
	task := claimParkRequest(t, h, 1, clock)
	if !parkNow(t, h, task, spawnParkQuota) {
		t.Fatal("the park was refused")
	}
	clock = clock.Add(31 * time.Minute)
	hk.setFail(true)
	checkParkNotices(t, h)
	if got := spawnFailedInjections(destination); len(got) != 0 {
		t.Fatalf("an hk outage sent %+v", got)
	}
	if row := parkedTask(t, h, task.Request.RequestID); row.ParkJudgedReason != "" || !row.ParkNoticeAt.IsZero() {
		t.Fatalf("an outage judged the row: %+v", row)
	}
	hk.setFail(false)
	clock = clock.Add(2 * time.Minute)
	checkParkNotices(t, h)
	if got := spawnFailedInjections(destination); len(got) != 1 {
		t.Fatalf("the recovered hub sent %d notices, want 1: %+v", len(got), got)
	}
}

// A node that takes a request and refuses it does not start the wait over: the
// whole wait is what the owner is told about.
func TestSpawnParkNoticeCountsTheWaitAcrossBinds_t4662(t *testing.T) {
	clock := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	start := clock
	hk := newParkNoticePriority(t, map[string]int{"466101": 1})
	_, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	h, destination := parkNoticeHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), client, &clock)
	task := claimParkRequest(t, h, 1, clock)
	if !parkNow(t, h, task, spawnParkQuota) {
		t.Fatal("the park was refused")
	}
	// A node takes the request, then refuses it: a new back-off park, in the
	// same wait.
	clock = clock.Add(time.Minute)
	bound, err := h.inbox.store.bindSpawn(t.Context(), task.Request.RequestID, task.Attempt, boundaryTestNode(task, 466230, "machine-a"), clock)
	if err != nil {
		t.Fatal(err)
	}
	bound = sentSpawnFixture(t, h.inbox.store, bound)
	refused, err := h.inbox.store.unbindRefusedSpawn(t.Context(), bound, hubSpawnExclusion{Machine: "machine-a", Reason: "checkout_refused", RC: 81}, nil, clock.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if refused.ParkReason == "" || refused.Node != nil {
		t.Fatalf("the refusal left %+v, want an unbound park", refused)
	}
	if !refused.ParkSince.Equal(start) {
		t.Fatalf("a bind and refusal restarted the wait at %v, want %v", refused.ParkSince, start)
	}
	// 31 minutes after the first park the whole wait is past the limit.
	clock = clock.Add(30 * time.Minute)
	checkParkNotices(t, h)
	injected := spawnFailedInjections(destination)
	if len(injected) != 1 {
		t.Fatalf("the bind-and-refusal path sent %d notices, want 1: %+v", len(injected), injected)
	}
	if !strings.Contains(injected[0].Text, "parked_since="+start.Format(time.RFC3339)) {
		t.Fatalf("the notice does not name the whole wait: %q", injected[0].Text)
	}
	if reads := hk.reads("466101"); reads != 1 {
		t.Fatalf("the priority was read %d times, want 1", reads)
	}
}

func TestSpawnParkNoticeSurvivesRestart_t4662(t *testing.T) {
	clock := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	hk := newParkNoticePriority(t, map[string]int{"466103": 2, "466104": 1})
	_, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	storePath := filepath.Join(t.TempDir(), "inbox.sqlite3")
	first, firstDestination := parkNoticeHub(t, storePath, client, &clock)
	notified := claimParkRequest(t, first, 3, clock)
	if !parkNow(t, first, notified, spawnParkQuota) {
		t.Fatal("the first park was refused")
	}
	clock = clock.Add(31 * time.Minute)
	checkParkNotices(t, first)
	if injected := spawnFailedInjections(firstDestination); len(injected) != 1 {
		t.Fatalf("the first hub sent %d notices, want 1: %+v", len(injected), injected)
	}
	// A second row is parked and never noticed before the restart, so the
	// restart must not lose its notice.
	unnotified := claimParkRequest(t, first, 4, clock)
	if !parkNow(t, first, unnotified, spawnParkQuota) {
		t.Fatal("the second park was refused")
	}
	first.Close()
	second, secondDestination := parkNoticeHub(t, storePath, client, &clock)
	clock = clock.Add(31 * time.Minute)
	checkParkNotices(t, second)
	injected := spawnFailedInjections(secondDestination)
	if len(injected) != 1 {
		t.Fatalf("the restarted hub sent %d notices, want 1: %+v", len(injected), injected)
	}
	if !strings.Contains(injected[0].Text, "request="+unnotified.Request.RequestID) {
		t.Fatalf("the restarted hub told the wrong request: %q", injected[0].Text)
	}
	// The row told before the restart is never told again.
	clock = clock.Add(2 * time.Minute)
	checkParkNotices(t, second)
	if got := spawnFailedInjections(secondDestination); len(got) != 0 {
		t.Fatalf("the restarted hub repeated a notice: %+v", got)
	}
	if reads := hk.reads("466103"); reads != 1 {
		t.Fatalf("the notified row's priority was read %d times, want 1", reads)
	}
	if reads := hk.reads("466104"); reads != 1 {
		t.Fatalf("the unnotified row's priority was read %d times, want 1", reads)
	}
	if row := parkedTask(t, second, notified.Request.RequestID); row.ParkNoticeAt.IsZero() || row.ParkJudgedReason != spawnParkQuota {
		t.Fatalf("the notified marker did not survive the restart: %+v", row)
	}
}

// One task hk answers badly for must not stop the pass, and must not hold the
// candidate page forever: the rows behind it are judged, and after a bounded
// number of passes that task reads as not urgent (hk 4662 review).
func TestSpawnParkNoticeAnswersBadlyForOneTask_t4662(t *testing.T) {
	clock := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	hk := newParkNoticePriority(t, map[string]int{"466101": 1, "466102": 1})
	hk.setUnreadable("466101")
	_, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	h, destination := parkNoticeHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), client, &clock)
	bad := claimParkRequest(t, h, 1, clock)
	if !parkNow(t, h, bad, spawnParkQuota) {
		t.Fatal("the first park was refused")
	}
	clock = clock.Add(time.Second)
	urgent := claimParkRequest(t, h, 2, clock)
	if !parkNow(t, h, urgent, spawnParkQuota) {
		t.Fatal("the urgent park was refused")
	}
	clock = clock.Add(31 * time.Minute)
	checkParkNotices(t, h)
	injected := spawnFailedInjections(destination)
	if len(injected) != 1 || !strings.Contains(injected[0].Text, "request="+urgent.Request.RequestID) {
		t.Fatalf("the unreadable task starved the urgent row: %+v", injected)
	}
	if row := parkedTask(t, h, bad.Request.RequestID); row.ParkPriority != -1 || row.ParkPriorityFailures != 1 || row.ParkJudgedReason != "" {
		t.Fatalf("the unreadable row was judged on its first failure: %+v", row)
	}
	// After spawnParkPriorityFailures passes it is judged not urgent and
	// leaves the page.
	for pass := 2; pass <= spawnParkPriorityFailures; pass++ {
		clock = clock.Add(2 * time.Minute)
		checkParkNotices(t, h)
	}
	if got := spawnFailedInjections(destination); len(got) != 0 {
		t.Fatalf("a later pass sent %+v", got)
	}
	row := parkedTask(t, h, bad.Request.RequestID)
	if row.ParkPriority != 0 || row.ParkJudgedReason != spawnParkQuota || row.ParkPriorityFailures != 0 {
		t.Fatalf("the unreadable row was not judged after %d failures: %+v", spawnParkPriorityFailures, row)
	}
}

// A deliberate replay starts a new attempt with a fresh wait (hk 4662 review):
// the park after it must not inherit the old attempt's wait start.
func TestSpawnParkNoticeReplayStartsAFreshWait_t4662(t *testing.T) {
	clock := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	start := clock
	hk := newParkNoticePriority(t, map[string]int{"466101": 1})
	_, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	h, destination := parkNoticeHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), client, &clock)
	task := claimParkRequest(t, h, 1, clock)
	if !parkNow(t, h, task, spawnParkQuota) {
		t.Fatal("the park was refused")
	}
	// Why the wait is past the limit for the first attempt.
	clock = clock.Add(31 * time.Minute)
	checkParkNotices(t, h)
	if injected := spawnFailedInjections(destination); len(injected) != 1 {
		t.Fatalf("the first attempt sent %d notices, want 1", len(injected))
	}
	// completeSpawn frees the reservation before a request ends retryable; the
	// fixture does the same, so the deliberate replay can claim it again.
	if _, err := h.inbox.store.db.Exec(`DELETE FROM spawn_reservations WHERE job=?`, task.Job); err != nil {
		t.Fatal(err)
	}
	if _, err := h.inbox.store.db.Exec(`UPDATE spawn_operations SET status='retryable' WHERE request_id=?`, task.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	retryable := parkedTask(t, h, task.Request.RequestID)
	replayed, err := h.inbox.store.resumeCleanedSpawn(t.Context(), retryable, clock)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Attempt != retryable.Attempt+1 || !replayed.ParkSince.IsZero() || replayed.ParkPriority != -1 || replayed.ParkPriorityFailures != 0 {
		t.Fatalf("the replay kept the old attempt's wait: %+v", replayed)
	}
	// The new attempt's park starts its wait over, and a new reason set is
	// noticed only once that wait is past the limit.
	if !parkNow(t, h, replayed, spawnParkHostFull) {
		t.Fatal("the new attempt's park was refused")
	}
	row := parkedTask(t, h, replayed.Request.RequestID)
	if !row.ParkSince.Equal(clock) {
		t.Fatalf("the new attempt's park starts at %v, want %v", row.ParkSince, clock)
	}
	clock = clock.Add(29 * time.Minute)
	checkParkNotices(t, h)
	if got := spawnFailedInjections(destination); len(got) != 0 {
		t.Fatalf("the new attempt's %s wait was noticed before the limit: %+v", 29*time.Minute, got)
	}
	clock = clock.Add(2 * time.Minute)
	checkParkNotices(t, h)
	injected := spawnFailedInjections(destination)
	if len(injected) != 1 {
		t.Fatalf("the new attempt sent %d notices, want 1", len(injected))
	}
	if !strings.Contains(injected[0].Text, "parked_since="+start.Add(31*time.Minute).UTC().Format(time.RFC3339)) {
		t.Fatalf("the notice names a stale wait: %q", injected[0].Text)
	}
	if reads := hk.reads("466101"); reads != 2 {
		t.Fatalf("the new attempt read the priority %d times, want 2", reads)
	}
}

// The request's own --urgency decision decides whether its owner is told: the
// flag wins over the task's hk priority, exactly as placement reads it, and a
// flag-decided row needs no hk read (hk 4662 review).
func TestSpawnParkNoticeHonoursTheUrgencyFlag_t4662(t *testing.T) {
	clock := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	hk := newParkNoticePriority(t, map[string]int{"466101": 3, "466102": 1})
	_, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	h, destination := parkNoticeHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), client, &clock)
	claim := func(n int, urgency, reason string) hubSpawnTask {
		t.Helper()
		request, job := parkRequest(n)
		request.Args = append(request.Args, "--urgency", urgency)
		task, _, err := h.inbox.store.claimSpawn(t.Context(), request, job, "parent", clock)
		if err != nil {
			t.Fatal(err)
		}
		if !parkNow(t, h, task, reason) {
			t.Fatalf("park %d was refused", n)
		}
		return task
	}
	// --urgency urgent on a P3 task is urgent for placement, so its owner is
	// told; --urgency normal on a P1 task is normal, so its owner is not.
	told := claim(1, "urgent", spawnParkQuota)
	clock = clock.Add(time.Second)
	quiet := claim(2, "normal", spawnParkQuota)
	clock = clock.Add(31 * time.Minute)
	checkParkNotices(t, h)
	injected := spawnFailedInjections(destination)
	if len(injected) != 1 || !strings.Contains(injected[0].Text, "request="+told.Request.RequestID) {
		t.Fatalf("the urgency flag was ignored: %+v", injected)
	}
	if reads := hk.reads("466101") + hk.reads("466102"); reads != 0 {
		t.Fatalf("a flag-decided row read hk %d times, want 0", reads)
	}
	for _, id := range []string{told.Request.RequestID, quiet.Request.RequestID} {
		if row := parkedTask(t, h, id); row.ParkJudgedReason != spawnParkQuota {
			t.Fatalf("a flag-decided row was not judged: %+v", row)
		}
	}
}

// One record hk answers 5xx for must not hold the sweep: the rows behind it are
// examined in the same pass, and a down service ends the sweep after a bounded
// number of reads, resuming after the cursor (hk 4662 review).
func TestSpawnParkNoticeBadRecordDoesNotHoldTheSweep_t4662(t *testing.T) {
	clock := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	hk := newParkNoticePriority(t, map[string]int{"466101": 1, "466102": 1, "466103": 1})
	hk.setBroken("466101")
	_, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	h, destination := parkNoticeHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), client, &clock)
	bad := claimParkRequest(t, h, 1, clock)
	if !parkNow(t, h, bad, spawnParkQuota) {
		t.Fatal("the first park was refused")
	}
	clock = clock.Add(time.Second)
	urgent := claimParkRequest(t, h, 2, clock)
	if !parkNow(t, h, urgent, spawnParkQuota) {
		t.Fatal("the urgent park was refused")
	}
	clock = clock.Add(31 * time.Minute)
	checkParkNotices(t, h)
	injected := spawnFailedInjections(destination)
	if len(injected) != 1 || !strings.Contains(injected[0].Text, "request="+urgent.Request.RequestID) {
		t.Fatalf("the bad record held the sweep: %+v", injected)
	}
	if row := parkedTask(t, h, bad.Request.RequestID); row.ParkPriority != -1 || row.ParkJudgedReason != "" {
		t.Fatalf("a 5xx row was judged before its read succeeded: %+v", row)
	}
}

// A service that answers nothing costs spawnParkNoticeOutages reads per sweep,
// and the cursor spreads the next sweeps over the remaining rows, so an outage
// cannot pin the sweep to the same head rows and no notice is lost once hk
// answers again (hk 4662 review).
func TestSpawnParkNoticeOutageEndsTheSweep_t4662(t *testing.T) {
	clock := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	hk := newParkNoticePriority(t, map[string]int{"466101": 3, "466102": 3, "466103": 3})
	hk.setFail(true)
	_, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	h, destination := parkNoticeHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), client, &clock)
	for n := 1; n <= 3; n++ {
		task := claimParkRequest(t, h, n, clock)
		if !parkNow(t, h, task, spawnParkQuota) {
			t.Fatalf("park %d was refused", n)
		}
		clock = clock.Add(time.Second)
	}
	total := func() int { return hk.reads("466101") + hk.reads("466102") + hk.reads("466103") }
	clock = clock.Add(31 * time.Minute)
	checkParkNotices(t, h)
	if got := total(); got != spawnParkNoticeOutages {
		t.Fatalf("the outage sweep read %d rows, want %d", got, spawnParkNoticeOutages)
	}
	if got := hk.reads("466103"); got != 0 {
		t.Fatalf("the sweep did not stop after the bounded outage reads: third row read %d", got)
	}
	// The next sweep resumes after the cursor at the row the stop left it on.
	clock = clock.Add(2 * time.Minute)
	checkParkNotices(t, h)
	if got := hk.reads("466103"); got != 1 {
		t.Fatalf("the sweep did not resume after the cursor: third row read %d", got)
	}
	// With hk answering again, the wrapped sweep judges every row; a P3 row is
	// told nothing.
	hk.setFail(false)
	clock = clock.Add(2 * time.Minute)
	checkParkNotices(t, h)
	if got := spawnFailedInjections(destination); len(got) != 0 {
		t.Fatalf("P3 rows sent %+v", got)
	}
	for n := 1; n <= 3; n++ {
		if row := parkedTask(t, h, t14RequestID(466100+n)); row.ParkJudgedReason != spawnParkQuota {
			t.Fatalf("row %d was not judged after recovery: %+v", n, row)
		}
	}
}

// A permanent handoffkeep rejection of one notice judges that row as not told
// instead of retrying it at the head of every sweep, so the rows behind it are
// still judged (hk 4662 review).
func TestSpawnParkNoticePermanentRelayRejection_t4662(t *testing.T) {
	clock := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	hk := newParkNoticePriority(t, map[string]int{"466101": 1, "466102": 1})
	fake, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	fake.status = http.StatusBadRequest
	h, destination := parkNoticeHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), client, &clock)
	first := claimParkRequest(t, h, 1, clock)
	if !parkNow(t, h, first, spawnParkQuota) {
		t.Fatal("the first park was refused")
	}
	clock = clock.Add(time.Second)
	second := claimParkRequest(t, h, 2, clock)
	if !parkNow(t, h, second, spawnParkQuota) {
		t.Fatal("the second park was refused")
	}
	clock = clock.Add(31 * time.Minute)
	checkParkNotices(t, h)
	if got := spawnFailedInjections(destination); len(got) != 0 {
		t.Fatalf("a rejected notice was injected: %+v", got)
	}
	for _, task := range []hubSpawnTask{first, second} {
		row := parkedTask(t, h, task.Request.RequestID)
		if row.ParkJudgedReason != spawnParkQuota || !row.ParkNoticeAt.IsZero() {
			t.Fatalf("a rejected row was not judged as not told: %+v", row)
		}
	}
	if reads := hk.reads("466101") + hk.reads("466102"); reads != 2 {
		t.Fatalf("priorities read %d times, want 2", reads)
	}
}

// A rate-limit or auth status on the notice write is transient: the row stays
// unjudged and is told once hk accepts writes again (hk 4662 review).
func TestSpawnParkNoticeTransientRelayRejection_t4662(t *testing.T) {
	clock := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	hk := newParkNoticePriority(t, map[string]int{"466101": 1, "466102": 1})
	fake, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	for i, status := range []int{http.StatusTooManyRequests, http.StatusUnauthorized} {
		fake.status = status
		h, destination := parkNoticeHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), client, &clock)
		task := claimParkRequest(t, h, i+1, clock)
		if !parkNow(t, h, task, spawnParkQuota) {
			t.Fatal("the park was refused")
		}
		clock = clock.Add(31 * time.Minute)
		checkParkNotices(t, h)
		if got := spawnFailedInjections(destination); len(got) != 0 {
			t.Fatalf("status %d injected %+v", status, got)
		}
		if row := parkedTask(t, h, task.Request.RequestID); row.ParkJudgedReason != "" || !row.ParkNoticeAt.IsZero() {
			t.Fatalf("status %d judged the row not told: %+v", status, row)
		}
		fake.status = http.StatusCreated
		clock = clock.Add(2 * time.Minute)
		checkParkNotices(t, h)
		injected := spawnFailedInjections(destination)
		if len(injected) != 1 {
			t.Fatalf("status %d: the notice did not arrive after recovery: %+v", status, injected)
		}
		if reads := hk.reads(fmt.Sprint(466100 + i + 1)); reads != 1 {
			t.Fatalf("status %d: priority read %d times, want 1", status, reads)
		}
		h.Close()
	}
}

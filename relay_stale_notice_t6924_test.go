package panewire

// hk 6924: the daily stale-replay notice. Each case retires rows through the
// real replay path (replayUndeliveredLaneEvents, with an inbox present), then
// files the day's notice through fileStaleReplayNotices and reads the design
// lane's inbox table. The only double is hk: its GET /v1/tasks/{id} is served
// from a map the test fills, and the spawn-store read is a hook.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	t6924Design = "stale-6924-design"
	// t6924Workers is how many worker lanes a rig routes, one job each.
	t6924Workers = 4
)

// t6924Day is the UTC day the rows are retired on; the notice files on the next
// day.
var (
	t6924Day     = time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	t6924NextDay = time.Date(2026, 11, 27, 0, 30, 0, 0, time.UTC)
)

func t6924Lane(i int) string { return fmt.Sprintf("stale-6924-w%d", i) }
func t6924Job(i int) string  { return fmt.Sprintf("job-6924-w%d", i) }

// t6924Tasks serves GET /v1/tasks/{id} from a map the test fills.
type t6924Tasks struct {
	mu    sync.Mutex
	tasks map[int64]staleReplayTaskRead
}

func (t *t6924Tasks) set(id int64, state, category string, updated time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tasks[id] = staleReplayTaskRead{ID: id, State: state, Category: category, UpdatedAt: updated}
}

func (t *t6924Tasks) serve(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(strings.TrimPrefix(r.URL.Path, "/v1/tasks/"), 10, 64)
	t.mu.Lock()
	task, ok := t.tasks[id]
	t.mu.Unlock()
	if err != nil || !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	_ = json.NewEncoder(w).Encode(task)
}

type t6924Rig struct {
	t     *testing.T
	fake  *fakeHandoffkeep
	tasks *t6924Tasks
	path  string
	now   time.Time
	hub   *HubServer
	// taskOf maps a worker job to the hk task it was spawned for; a job not set
	// has no task.
	taskOf map[string]string
	seq    int64
}

// newT6924Rig starts a fake hk that also serves task reads, and a hub whose
// inbox store lives at path.
func newT6924Rig(t *testing.T, path string) *t6924Rig {
	t.Helper()
	tasks := &t6924Tasks{tasks: map[int64]staleReplayTaskRead{}}
	fake := &fakeHandoffkeep{nextID: 100, status: http.StatusCreated, rows: map[string]*handoffkeepRelayEvent{}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/tasks/") {
			tasks.serve(w, r)
			return
		}
		fake.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	client, err := newHandoffkeepRelayClient(hubHandoffkeepEnv{URL: server.URL, Token: "test-token"}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	rig := &t6924Rig{t: t, fake: fake, tasks: tasks, path: path, now: t6924Day, taskOf: map[string]string{}}
	rig.open(client)
	return rig
}

// open starts a hub on the rig's inbox store. A restart reopens the same path.
func (r *t6924Rig) open(client *handoffkeepRelayClient) {
	r.t.Helper()
	routes := map[string]reportRelayRoute{t6924Design: {Machine: "host-a", Pane: "w1:design"}}
	for i := 1; i <= t6924Workers; i++ {
		routes[t6924Lane(i)] = reportRelayRoute{Machine: "host-a", Pane: fmt.Sprintf("w1:worker%d", i), Job: t6924Job(i), Parent: t6924Design}
	}
	lanes, err := json.Marshal(map[string]any{"lanes": routes})
	if err != nil {
		r.t.Fatal(err)
	}
	hub, err := NewHubServer(HubServerConfig{
		Tokens:          map[string]string{"operator": "op", "host-a": "node"},
		ReportRelayPath: r20LanesFile(r.t, string(lanes)),
		InboxStorePath:  r.path,
		handoffkeep:     client,
		Now:             func() time.Time { return r.now },
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		r.t.Fatal(err)
	}
	r.t.Cleanup(func() { hub.Close() })
	hub.staleReplaySpawn = func(_ context.Context, job string) (staleReplaySpawn, bool) {
		return staleReplaySpawn{owner: t6924Design, task: r.taskOf[job]}, strings.HasPrefix(job, "job-6924-")
	}
	r.hub = hub
}

// retire seeds one lane.event row on worker lane i, received at received, and
// runs the replay that retires it. The clock is at t6924Day, so the row is past
// the 24 h window. It returns the row's hk event id.
func (r *t6924Rig) retire(i int, received time.Time, text string) string {
	r.t.Helper()
	r.seq++
	eventID := "t6924-" + strconv.FormatInt(r.seq, 10)
	r.fake.seedUndelivered(handoffkeepRelayEvent{
		ID: 500 + r.seq, Kind: "lane.event", JobID: laneEventTransportID(t6924Lane(i), eventID), Epoch: 1,
		OwnerLane: t6924Lane(i), EventID: eventID, Text: text, ReceivedAt: received.UTC().Format(time.RFC3339Nano),
	})
	r.hub.replayUndeliveredLaneEvents(context.Background())
	return eventID
}

// before is the instant hours before the retirement.
func (r *t6924Rig) before(hours float64) time.Time {
	return t6924Day.Add(-time.Duration(hours * float64(time.Hour)))
}

// unfiledRows counts the rows whose day's notice for their lane is still owed.
func (r *t6924Rig) unfiledRows() int {
	r.t.Helper()
	var n int
	if err := r.hub.inbox.store.db.QueryRow(`SELECT COUNT(*) FROM relay_stale_notice_rows WHERE filed=0`).Scan(&n); err != nil {
		r.t.Fatal(err)
	}
	return n
}

// noticeRows counts the rows the notice store holds, filed or not.
func (r *t6924Rig) noticeRows() int {
	r.t.Helper()
	var n int
	if err := r.hub.inbox.store.db.QueryRow(`SELECT COUNT(*) FROM relay_stale_notice_rows`).Scan(&n); err != nil {
		r.t.Fatal(err)
	}
	return n
}

// designItems reads the design lane's inbox items as class|kind|payload.
func (r *t6924Rig) designItems() []string {
	r.t.Helper()
	rows, err := r.hub.inbox.store.db.Query(`SELECT class||'|'||kind||'|'||payload FROM lane_inbox WHERE lane=? ORDER BY id`, t6924Design)
	if err != nil {
		r.t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			r.t.Fatal(err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		r.t.Fatal(err)
	}
	return out
}

// fileNextDay runs the filing pass on the next UTC day.
// The clock is put back afterwards, so a later retirement stays on t6924Day.
func (r *t6924Rig) fileNextDay() {
	r.t.Helper()
	day := r.now
	r.now = t6924NextDay
	r.hub.fileStaleReplayNotices(context.Background())
	r.now = day
}

// A task that closed 1 h after its row arrived overtook it: the row is counted
// in the footer, not listed. A task that closed 10 h after, or is still open,
// did not: its row is listed. Killed by: a 12 h window (the 10 h row is then
// overtaken and drops out of the list), or a window that never counts.
func TestT6924OvertakenRowsCountedNotListed(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	rig.taskOf[t6924Job(1)] = "1001"
	rig.taskOf[t6924Job(2)] = "1002"
	rig.taskOf[t6924Job(3)] = "1003"
	received := rig.before(26)
	rig.tasks.set(1001, "merged", "completed", received.Add(time.Hour))
	rig.tasks.set(1002, "merged", "completed", received.Add(10*time.Hour))
	rig.tasks.set(1003, "started", "started", received.Add(30*time.Hour))

	rig.retire(1, received, "[consult-done] advice one")
	rig.retire(2, received, "[consult-done] advice two")
	rig.retire(3, received, "[mergewake] BLOCKED three")
	rig.fileNextDay()

	items := rig.designItems()
	if len(items) != 1 {
		t.Fatalf("design lane items=%d, want one notice: %v", len(items), items)
	}
	if !strings.HasPrefix(items[0], "inbox|lane.event|") {
		t.Fatalf("notice is %q, want an inbox-class lane.event", items[0])
	}
	if !strings.Contains(items[0], "Overtaken by their task within 6 h of receipt, not listed: 1.") {
		t.Fatalf("footer does not count the overtaken row: %s", items[0])
	}
	if strings.Contains(items[0], "hk t6924-1 ") {
		t.Fatalf("the overtaken row (1 h) is listed: %s", items[0])
	}
	if !strings.Contains(items[0], "hk t6924-2 ") || !strings.Contains(items[0], "hk t6924-3") {
		t.Fatalf("the 10 h and the open-task rows are not listed: %s", items[0])
	}
}

// A row whose task cannot be read is listed, never dropped.
func TestT6924UnreadableTaskRowIsListed(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	rig.taskOf[t6924Job(1)] = "9999" // not in hk's task map: the read is a 404
	rig.retire(1, rig.before(26), "[consult-done] task not in hk")
	rig.fileNextDay()
	items := rig.designItems()
	if len(items) != 1 || !strings.Contains(items[0], "hk t6924-1") {
		t.Fatalf("unreadable task must list its row; items=%v", items)
	}
}

// An idle-wake report is retired as before but never kept for the notice.
// Killed by: dropping the idle-wake check in noteStaleReplayRetire.
func TestT6924IdleWakeIsNotNoticed(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	wake, err := json.Marshal(map[string]any{"kind": "idle-wake", "pane": "w1:worker1", "changed_at": rig.before(26).UTC().Format(time.RFC3339Nano)})
	if err != nil {
		t.Fatal(err)
	}
	rig.retire(1, rig.before(26), string(wake))
	if got := rig.noticeRows(); got != 0 {
		t.Fatalf("idle-wake row kept for the notice: rows=%d", got)
	}
	rig.fileNextDay()
	if items := rig.designItems(); len(items) != 0 {
		t.Fatalf("idle-wake produced a notice: %v", items)
	}
}

// A row received after its worker job recorded job.completed is the hk 6900
// case and is excluded; one received before it is kept. Killed by: removing the
// job-completion predicate from noteStaleReplayRetire.
func TestT6924PostCompletionRowIsExcluded(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	rig.hub.mu.Lock()
	rig.hub.jobs[t6924Job(1)] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: t6924Job(1), AgentLabel: t6924Lane(1)}, Completed: true, LastSeen: rig.before(25)}
	rig.hub.mu.Unlock()

	rig.retire(1, rig.before(26), "[consult-done] before completion")
	rig.retire(1, rig.before(24.5), "[consult-done] after completion")
	if got := rig.noticeRows(); got != 1 {
		t.Fatalf("rows kept=%d, want only the row received before the completion", got)
	}
	rig.fileNextDay()
	items := rig.designItems()
	if len(items) != 1 || !strings.Contains(items[0], "hk t6924-1") || strings.Contains(items[0], "hk t6924-2") {
		t.Fatalf("notice=%v, want only t6924-1", items)
	}
}

// A day with no listed row files no event: an empty day, and a day whose rows
// were all overtaken. The overtaken rows stay kept until the day is closed.
// Killed by: filing a notice whenever a day closes.
func TestT6924EmptyDayFilesNoEvent(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	rig.fileNextDay()
	if items := rig.designItems(); len(items) != 0 {
		t.Fatalf("empty day filed %v", items)
	}

	rig.taskOf[t6924Job(1)] = "1001"
	rig.tasks.set(1001, "merged", "completed", rig.before(25))
	rig.retire(1, rig.before(26), "[consult-done] overtaken only")
	rig.fileNextDay()
	if items := rig.designItems(); len(items) != 0 {
		t.Fatalf("a day with only overtaken rows filed %v", items)
	}
	if got := rig.noticeRows(); got != 1 {
		t.Fatalf("overtaken row rows=%d after the day closed; the filed flag should keep it, not delete it", got)
	}
}

// A hub restart in the middle of the day keeps the day's rows, so the notice
// still names them when the day closes. Killed by: keeping rows only in memory.
func TestT6924RestartMidDayKeepsRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	rig := newT6924Rig(t, path)
	rig.retire(1, rig.before(26), "[consult-done] before the restart")
	client := rig.hub.handoffkeep
	if err := rig.hub.Close(); err != nil {
		t.Fatal(err)
	}
	rig.open(client)
	if got := rig.noticeRows(); got != 1 {
		t.Fatalf("rows after restart=%d, want 1", got)
	}
	rig.fileNextDay()
	items := rig.designItems()
	if len(items) != 1 || !strings.Contains(items[0], "hk t6924-1") {
		t.Fatalf("notice after restart=%v, want the pre-restart row", items)
	}
}

// Two filing passes make one notice: a closed day's rows are marked filed once
// its item is in the inbox. Killed by: not marking the lane's rows filed.
// The first pass closes the day's rows (filed=1), so a second pass has nothing
// owed. The inbox's event dedupe would hide a second filing from an item count,
// so the closed rows are what is asserted. Killed by: not marking the lane's
// rows filed, which leaves them owed for every later pass.
func TestT6924FilingIsOncePerDay(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	rig.retire(1, rig.before(26), "[consult-done] once")
	rig.fileNextDay()
	if got := rig.unfiledRows(); got != 0 {
		t.Fatalf("rows still owed after the day was filed: %d", got)
	}
	rig.fileNextDay()
	if items := rig.designItems(); len(items) != 1 {
		t.Fatalf("two filing passes filed %d items, want 1", len(items))
	}
}

// The notice names at most staleReplayListLimit rows, counts the rest and the
// overtaken rows, and stays inside one lane.event.
func TestT6924NoticeTextIsBounded(t *testing.T) {
	var rows []staleReplayRow
	for i := 0; i < staleReplayListLimit+5; i++ {
		rows = append(rows, staleReplayRow{RelayID: int64(i + 1), Kind: "lane.event", Sender: "consult-done", HKEventID: strings.Repeat("e", 400), ReceivedMS: t6924Day.Add(-26 * time.Hour).UnixMilli(), RetiredMS: t6924Day.UnixMilli()})
	}
	text := staleReplayNoticeText("2026-11-26", t6924Design, rows, 2)
	if len(text) > laneEventTextLimit {
		t.Fatalf("notice is %d bytes, over the lane.event limit %d", len(text), laneEventTextLimit)
	}
	if got := strings.Count(text, " | "); got != staleReplayListLimit {
		t.Fatalf("notice lists %d rows, want %d", got, staleReplayListLimit)
	}
	if !strings.Contains(text, "+5 more not listed.") {
		t.Fatalf("notice does not count the rows past the list: %s", text)
	}
	if !strings.Contains(text, "Overtaken by their task within 6 h of receipt, not listed: 2.") {
		t.Fatalf("notice does not count the overtaken rows: %s", text)
	}
}

// The sender prefix is the leading [label] of a relay text, and only that.
func TestT6924SenderPrefix(t *testing.T) {
	cases := map[string]string{
		"[consult-done] advice": "consult-done",
		"no prefix here":        "",
		"[]empty":               "",
		"[" + strings.Repeat("x", 40) + "] too long": "",
	}
	for in, want := range cases {
		if got := staleReplaySender(in); got != want {
			t.Errorf("staleReplaySender(%q)=%q, want %q", in, got, want)
		}
	}
}

// The copy reads relay_stale_notice_rows in relay_id order on every backend. A
// text COLLATE on the bigint key is refused by Postgres with SQLSTATE 42804, the
// CI failure of the older-source copy. Killed by: removing the table from
// storeCopyNumericKeys.
func TestT6924CopyOrdersRelayIDAsNumber(t *testing.T) {
	table, ok := storeCopyTableNamed("relay_stale_notice_rows")
	if !ok {
		t.Fatal("relay_stale_notice_rows is not a portable table")
	}
	for name, d := range map[string]sqlDialect{"postgres": dialectPostgres, "mysql": dialectMySQL, "sqlite": dialectSQLite} {
		if got := storeCopyOrderBy(d, table); got != "relay_id" {
			t.Errorf("%s order by %q, want %q", name, got, "relay_id")
		}
	}
}

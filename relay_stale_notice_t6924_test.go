package panewire

// hk 6924: the daily stale-replay notice. Each case retires rows through the
// real replay path (replayUndeliveredLaneEvents, with an inbox present), then
// files the day's notice through fileStaleReplayNotices and reads the design
// lane's inbox table. Receipts go through relayLaneEventContext, so the lane
// bindings are the ones a live hub writes. The doubles are hk, which serves
// GET /v1/tasks/{id} from a map the test fills, and the spawn-store read, which
// is a hook.

import (
	"context"
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

func t6924Lane(i int) string          { return fmt.Sprintf("stale-6924-w%d", i) }
func t6924Job(i int) string           { return fmt.Sprintf("job-6924-w%d", i) }
func t6924At(at time.Time) *time.Time { return &at }

// t6924HKTask is the part of hk's task answer the cases set: the state, the
// completed_at and canceled_at stamps hk keeps for the end, and updated_at, which
// moves with every edit. Category is served so a case can also cover a reader
// that uses it; the notice does not read it.
type t6924HKTask struct {
	ID          int64      `json:"id"`
	State       string     `json:"state"`
	Category    string     `json:"category,omitempty"`
	CompletedAt *time.Time `json:"completed_at"`
	CanceledAt  *time.Time `json:"canceled_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

// t6924Tasks serves GET /v1/tasks/{id} from a map the test fills.
type t6924Tasks struct {
	mu     sync.Mutex
	tasks  map[int64]t6924HKTask
	onRead func()
}

func (t *t6924Tasks) set(task t6924HKTask) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tasks[task.ID] = task
}

// once runs fn at the next task read, before the read is answered.
func (t *t6924Tasks) once(fn func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.onRead = fn
}

func (t *t6924Tasks) serve(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(strings.TrimPrefix(r.URL.Path, "/v1/tasks/"), 10, 64)
	t.mu.Lock()
	hook := t.onRead
	t.onRead = nil
	t.mu.Unlock()
	if hook != nil {
		hook()
	}
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
	hub   *HubServer
	// routes is the lanes.json the hub reads: the design lane and one worker
	// lane per job.
	routes map[string]reportRelayRoute
	// taskOf maps a worker job to the hk task it was spawned for; a job not set
	// has no task.
	taskOf map[string]string
	seq    int64
	now    time.Time
	// stamp is the received_at handoffkeep gives the rows it creates next; it
	// is the row's receipt time, which the hub's clock (now) need not match.
	stamp time.Time
	path  string
}

// newT6924Rig starts a fake hk that also serves task reads, and a hub whose
// inbox store lives at path.
func newT6924Rig(t *testing.T, path string) *t6924Rig {
	t.Helper()
	tasks := &t6924Tasks{tasks: map[int64]t6924HKTask{}}
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
	routes := map[string]reportRelayRoute{t6924Design: {Machine: "host-a", Pane: "w1:design"}}
	for i := 1; i <= t6924Workers; i++ {
		routes[t6924Lane(i)] = reportRelayRoute{Machine: "host-a", Pane: fmt.Sprintf("w1:worker%d", i), Job: t6924Job(i), Parent: t6924Design}
	}
	rig := &t6924Rig{t: t, fake: fake, tasks: tasks, routes: routes, taskOf: map[string]string{}, now: t6924Day, stamp: t6924Day, path: path}
	fake.receivedAt = func() time.Time { return rig.stamp }
	rig.open(client)
	return rig
}

// open starts a hub on the rig's inbox store. A restart reopens the same path.
func (r *t6924Rig) open(client *handoffkeepRelayClient) {
	r.t.Helper()
	lanes, err := json.Marshal(map[string]any{"lanes": r.routes})
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

// setRoute points worker lane i at job; an empty job removes the lane's route,
// as releaseDeadLanes does when a worker pane has ended. It rewrites the lanes
// file the hub reads.
func (r *t6924Rig) setRoute(i int, job string) {
	r.t.Helper()
	lane := t6924Lane(i)
	if job == "" {
		delete(r.routes, lane)
	} else {
		route := r.routes[lane]
		route.Job = job
		r.routes[lane] = route
	}
	body, err := json.Marshal(map[string]any{"lanes": r.routes})
	if err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(r.hub.reportRelayPath, body, 0600); err != nil {
		r.t.Fatal(err)
	}
}

// receive posts a lane.event on worker lane i at the instant at, through the
// hub's own receipt path, and returns the event id. The receipt time is the row's
// handoffkeep received_at, which the fake does not stamp, so it is set here.
func (r *t6924Rig) receive(i int, at time.Time, text string) string {
	r.t.Helper()
	return r.receiveStamped(i, at, at, text)
}

// receiveStamped is receive with the hub's clock (hubAt) and the row's
// handoffkeep received_at (received) set apart: the two clocks are not one
// clock in production, and a binding must follow the row's own stamp.
func (r *t6924Rig) receiveStamped(i int, hubAt, received time.Time, text string) string {
	r.t.Helper()
	r.seq++
	eventID := "t6924-" + strconv.FormatInt(r.seq, 10)
	savedNow, savedStamp := r.now, r.stamp
	r.now, r.stamp = hubAt, received
	defer func() { r.now, r.stamp = savedNow, savedStamp }()
	event := hubJobEventPayload{JobID: laneEventTransportID(t6924Lane(i), eventID), Epoch: 1, OwnerLane: t6924Lane(i), EventID: eventID, Text: text}
	r.hub.relayLaneEventContext(context.Background(), event, nil)
	r.fake.mu.Lock()
	defer r.fake.mu.Unlock()
	for _, row := range r.fake.rows {
		if row.EventID == eventID {
			return eventID
		}
	}
	r.t.Fatalf("receipt %s was not persisted", eventID)
	return ""
}

// retire seeds one lane.event row on worker lane i, received at received, and
// runs the replay that retires it. The clock is at t6924Day, so the row is past
// the 24 h window. It returns the row's hk event id. The seeded rows are the
// only ones the fake lists, so a case mixing receive and retire must use receive
// alone.
func (r *t6924Rig) retire(i int, received time.Time, text string) string {
	r.t.Helper()
	r.seq++
	eventID := "t6924-" + strconv.FormatInt(r.seq, 10)
	r.fake.seedUndelivered(handoffkeepRelayEvent{
		ID: 500 + r.seq, Kind: "lane.event", JobID: laneEventTransportID(t6924Lane(i), eventID), Epoch: 1,
		OwnerLane: t6924Lane(i), EventID: eventID, Text: text, ReceivedAt: received.UTC().Format(time.RFC3339Nano),
	})
	r.replay()
	return eventID
}

// replay runs the startup replay on the current clock.
func (r *t6924Rig) replay() {
	r.hub.replayUndeliveredLaneEvents(context.Background())
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

// retiredCount counts the handoffkeep rows the replay closed.
func (r *t6924Rig) retiredCount() int {
	r.fake.mu.Lock()
	defer r.fake.mu.Unlock()
	return len(r.fake.deliveredTo)
}

// dropNoticeRows removes the notice table, so the store refuses the row insert.
func (r *t6924Rig) dropNoticeRows() {
	r.t.Helper()
	if _, err := r.hub.inbox.store.db.Exec(`DROP TABLE relay_stale_notice_rows`); err != nil {
		r.t.Fatal(err)
	}
}

// createNoticeRows restores the notice tables the schema defines.
func (r *t6924Rig) createNoticeRows() {
	r.t.Helper()
	for _, stmt := range relayStaleNoticeSchema {
		if _, err := r.hub.inbox.store.db.Exec(stmt); err != nil {
			r.t.Fatal(err)
		}
	}
}

// itemsOf reads one lane's inbox items as class|kind|payload.
func (r *t6924Rig) itemsOf(lane string) []string {
	r.t.Helper()
	rows, err := r.hub.inbox.store.db.Query(`SELECT class||'|'||kind||'|'||payload FROM lane_inbox WHERE lane=? ORDER BY id`, lane)
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

// designItems reads the design lane's inbox items.
func (r *t6924Rig) designItems() []string { return r.itemsOf(t6924Design) }

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
	rig.tasks.set(t6924HKTask{ID: 1001, State: "merged", CompletedAt: t6924At(received.Add(time.Hour)), UpdatedAt: received.Add(time.Hour)})
	rig.tasks.set(t6924HKTask{ID: 1002, State: "merged", CompletedAt: t6924At(received.Add(10 * time.Hour)), UpdatedAt: received.Add(10 * time.Hour)})
	rig.tasks.set(t6924HKTask{ID: 1003, State: "started", UpdatedAt: received.Add(30 * time.Hour)})

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
	// The handoffkeep row ids are 502 and 503: each entry names its row.
	if !strings.Contains(items[0], "row 502 hk t6924-2 ") || !strings.Contains(items[0], "row 503 hk t6924-3") {
		t.Fatalf("the 10 h and the open-task rows are not listed by row id: %s", items[0])
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

// A row received after its worker job's completion was recorded is kept like any
// other: the hk 6900 exclusion was dropped once 6900 merged, and a lost or
// revoked job's rows are real messages. Killed by: restoring a job-completion
// exclusion in noteStaleReplayRetire.
func TestT6924RowAfterJobCompletionIsNoticed(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	rig.hub.mu.Lock()
	rig.hub.jobs[t6924Job(1)] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: t6924Job(1), AgentLabel: t6924Lane(1)}, Completed: true, LastSeen: rig.before(25)}
	rig.hub.mu.Unlock()

	rig.retire(1, rig.before(26), "[consult-done] before completion")
	rig.retire(1, rig.before(24.5), "[consult-done] after completion")
	if got := rig.noticeRows(); got != 2 {
		t.Fatalf("rows kept=%d, want both rows", got)
	}
	rig.fileNextDay()
	items := rig.designItems()
	if len(items) != 1 || !strings.Contains(items[0], "hk t6924-1") || !strings.Contains(items[0], "hk t6924-2") {
		t.Fatalf("notice=%v, want both rows", items)
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
	rig.tasks.set(t6924HKTask{ID: 1001, State: "merged", CompletedAt: t6924At(rig.before(25)), UpdatedAt: rig.before(25)})
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
// its item is in the inbox. The first pass closes the day's rows (filed=1), so a
// second pass has nothing owed. The inbox's event dedupe would hide a second
// filing from an item count, so the closed rows are what is asserted. Killed by:
// not marking the lane's rows filed, which leaves them owed for every later pass.
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
// overtaken rows, and stays inside one lane.event, also when every field is at
// its width cap. Killed by: a footer written after the entries without room
// reserved for it.
func TestT6924NoticeTextIsBounded(t *testing.T) {
	received := t6924Day.Add(-26 * time.Hour)
	cases := map[string]staleReplayRow{
		// Entries at their widest: a 32-byte sender, a 64-byte kind and a long
		// event id (cut to 48 bytes in the notice).
		"wide":  {Kind: strings.Repeat("k", 64), Sender: strings.Repeat("s", 32), HKEventID: strings.Repeat("e", 400)},
		"short": {Kind: "lane.event", Sender: "consult-done", HKEventID: "e"},
	}
	for name, sample := range cases {
		var rows []staleReplayRow
		for i := 0; i < staleReplayListLimit+5; i++ {
			row := sample
			row.RelayID = int64(i + 1)
			row.ReceivedMS = received.UnixMilli()
			row.RetiredMS = t6924Day.UnixMilli()
			rows = append(rows, row)
		}
		text := staleReplayNoticeText("2026-11-26", t6924Design, rows, 2)
		if len(text) > laneEventTextLimit {
			t.Fatalf("%s: notice is %d bytes, over the lane.event limit %d", name, len(text), laneEventTextLimit)
		}
		listed := strings.Count(text, " | ")
		if listed == 0 || listed > staleReplayListLimit {
			t.Fatalf("%s: notice lists %d rows, want between 1 and %d", name, listed, staleReplayListLimit)
		}
		if !strings.Contains(text, fmt.Sprintf(" +%d more not listed.", len(rows)-listed)) {
			t.Fatalf("%s: notice does not count the rows it did not list: %s", name, text)
		}
		if !strings.Contains(text, "Overtaken by their task within 6 h of receipt, not listed: 2.") {
			t.Fatalf("%s: notice does not count the overtaken rows: %s", name, text)
		}
		if name == "short" && listed != staleReplayListLimit {
			t.Fatalf("short entries list %d rows, want the full %d", listed, staleReplayListLimit)
		}
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

// A row retired after its worker's route is gone (the pane ended and
// releaseDeadLanes removed the route) still reaches its design lane: the job it
// was received for was bound at receipt. Killed by: resolving the job from the
// route at retirement only.
func TestT6924RouteRemovedBeforeRetirementIsListed(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	rig.taskOf[t6924Job(1)] = "1001"
	rig.tasks.set(t6924HKTask{ID: 1001, State: "started", UpdatedAt: rig.before(26)})
	eventID := rig.receive(1, rig.before(26), "[consult-done] received while its route stood")
	rig.setRoute(1, "")
	rig.replay()
	rig.fileNextDay()
	items := rig.designItems()
	if len(items) != 1 || !strings.Contains(items[0], "hk "+eventID) {
		t.Fatalf("row of a lane whose route is gone was not listed on its design lane: items=%v", items)
	}
}

// A row with no bound job and no route to hint one is listed on the hub's notice
// lane, not dropped. Killed by: dropping an unresolved row.
func TestT6924UnboundRowIsListedOnTheNoticeLane(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	rig.setRoute(1, "")
	rig.retire(1, rig.before(26), "[consult-done] no route, no binding")
	rig.fileNextDay()
	if items := rig.designItems(); len(items) != 0 {
		t.Fatalf("an unresolved row was filed on the design lane: %v", items)
	}
	items := rig.itemsOf(defaultLoopsNoticeLane)
	if len(items) != 1 || !strings.Contains(items[0], "hk t6924-1") {
		t.Fatalf("unresolved row must be listed on the notice lane %s: items=%v", defaultLoopsNoticeLane, items)
	}
}

// A row is judged against the job its lane had when the row arrived, not the job
// the lane has later: a lane taken over by a new job does not move an older row
// into the new job's task. Killed by: resolving every row's job from the route at
// retirement.
func TestT6924RowKeepsTheJobItWasReceivedFor(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	const laterJob = "job-6924-w1b"
	rig.taskOf[t6924Job(1)] = "1001"
	rig.taskOf[laterJob] = "1002"
	rig.tasks.set(t6924HKTask{ID: 1001, State: "merged", CompletedAt: t6924At(rig.before(39)), UpdatedAt: rig.before(39)})
	rig.tasks.set(t6924HKTask{ID: 1002, State: "started", UpdatedAt: rig.before(30)})

	early := rig.receive(1, rig.before(40), "[consult-done] received under the first job")
	rig.setRoute(1, laterJob)
	late := rig.receive(1, rig.before(30), "[consult-done] received under the later job")
	rig.replay()
	rig.fileNextDay()

	items := rig.designItems()
	if len(items) != 1 {
		t.Fatalf("design lane items=%d, want one notice: %v", len(items), items)
	}
	if !strings.Contains(items[0], "Overtaken by their task within 6 h of receipt, not listed: 1.") {
		t.Fatalf("the row received under the first job is not counted as overtaken: %s", items[0])
	}
	if strings.Contains(items[0], "hk "+early) {
		t.Fatalf("the row received under the first job is listed against the later job: %s", items[0])
	}
	if !strings.Contains(items[0], "hk "+late) {
		t.Fatalf("the row received under the later job is not listed: %s", items[0])
	}
}

// The end of a task is its completed_at, so a merged task that was deployed a
// day after the row arrived still overtook it. Its updated_at moved with the
// deploy and is not the end. Killed by: reading updated_at as the end.
func TestT6924TaskEndIsCompletedAtNotUpdatedAt(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	rig.taskOf[t6924Job(1)] = "1001"
	rig.taskOf[t6924Job(2)] = "1002"
	received := rig.before(26)
	rig.tasks.set(t6924HKTask{ID: 1001, State: "deployed", CompletedAt: t6924At(received.Add(time.Hour)), UpdatedAt: received.Add(20 * time.Hour)})
	rig.tasks.set(t6924HKTask{ID: 1002, State: "started", UpdatedAt: received})
	rig.retire(1, received, "[consult-done] merged an hour after receipt, deployed later")
	rig.retire(2, received, "[mergewake] still open")
	rig.fileNextDay()

	items := rig.designItems()
	if len(items) != 1 {
		t.Fatalf("design lane items=%d, want one notice: %v", len(items), items)
	}
	if !strings.Contains(items[0], "Overtaken by their task within 6 h of receipt, not listed: 1.") {
		t.Fatalf("a task deployed after the window was not counted as overtaken: %s", items[0])
	}
	if strings.Contains(items[0], "hk t6924-1 ") {
		t.Fatalf("the overtaken row is listed: %s", items[0])
	}
}

// A task that ended before the row arrived did not overtake it, even when an
// edit after the receipt moves updated_at into the window. Killed by: reading
// updated_at as the end.
func TestT6924EndBeforeReceiptIsNotOvertaken(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	rig.taskOf[t6924Job(1)] = "1001"
	received := rig.before(26)
	rig.tasks.set(t6924HKTask{ID: 1001, State: "merged", CompletedAt: t6924At(received.Add(-2 * time.Hour)), UpdatedAt: received.Add(time.Hour)})
	rig.retire(1, received, "[consult-done] merged before it arrived, edited after")
	rig.fileNextDay()

	items := rig.designItems()
	if len(items) != 1 || !strings.Contains(items[0], "hk t6924-1") {
		t.Fatalf("a row whose task ended before it arrived must be listed: items=%v", items)
	}
	if strings.Contains(items[0], "Overtaken") {
		t.Fatalf("a row whose task ended before it arrived is counted as overtaken: %s", items[0])
	}
}

// icebox is parked, not ended: a task parked within the window does not overtake
// its rows. Killed by: accepting the canceled category as an end.
func TestT6924IceboxIsNotEnded(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	rig.taskOf[t6924Job(1)] = "1001"
	received := rig.before(26)
	rig.tasks.set(t6924HKTask{ID: 1001, State: "icebox", Category: "canceled", CanceledAt: t6924At(received.Add(time.Hour)), UpdatedAt: received.Add(time.Hour)})
	rig.retire(1, received, "[consult-done] parked in icebox an hour after receipt")
	rig.fileNextDay()

	items := rig.designItems()
	if len(items) != 1 || !strings.Contains(items[0], "hk t6924-1") {
		t.Fatalf("a row whose task is parked in icebox must be listed: items=%v", items)
	}
	if strings.Contains(items[0], "Overtaken") {
		t.Fatalf("an icebox task counted as overtaking its row: %s", items[0])
	}
}

// A row whose notice row cannot be kept is not closed: it stays undelivered, and
// the next replay keeps it and closes it. Killed by: closing the row before its
// notice row is written.
func TestT6924NoticeRowFailureLeavesRowForRetry(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	rig.dropNoticeRows()
	rig.retire(1, rig.before(26), "[consult-done] kept once the store is back")
	if got := rig.retiredCount(); got != 0 {
		t.Fatalf("row closed without its notice row: %d closed", got)
	}
	rig.createNoticeRows()
	rig.replay()
	if got := rig.retiredCount(); got != 1 {
		t.Fatalf("row closed=%d after the retry, want 1", got)
	}
	if got := rig.noticeRows(); got != 1 {
		t.Fatalf("notice rows=%d after the retry, want 1", got)
	}
}

// A row retired while the filing pass reads its task is not closed by that pass:
// only the rows the pass read are marked filed. Killed by: closing every owed row
// of the lane and day.
func TestT6924FilingKeepsRowsReadLater(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	rig.taskOf[t6924Job(1)] = "1001"
	rig.tasks.set(t6924HKTask{ID: 1001, State: "started", UpdatedAt: rig.before(26)})
	rig.retire(1, rig.before(26), "[consult-done] read by the filing pass")
	rig.tasks.once(func() {
		err := rig.hub.inbox.store.insertStaleReplayRow(context.Background(), staleReplayRow{
			RelayID: 900, NoticeDay: staleReplayDay(t6924Day), DesignLane: t6924Design, WorkerLane: t6924Lane(2), Job: t6924Job(2),
			Kind: "lane.event", Sender: "consult-done", HKEventID: "t6924-late", ReceivedMS: rig.before(26).UnixMilli(), RetiredMS: t6924Day.UnixMilli(),
		})
		if err != nil {
			t.Error(err)
		}
	})
	rig.fileNextDay()
	if got := rig.unfiledRows(); got != 1 {
		t.Fatalf("rows still owed after the pass=%d, want the row retired during the read", got)
	}
	items := rig.designItems()
	if len(items) != 1 || !strings.Contains(items[0], "hk t6924-1") || strings.Contains(items[0], "t6924-late") {
		t.Fatalf("notice=%v, want only the row the pass read", items)
	}
}

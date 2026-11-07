package panewire

// hk 3431: an unconfirmed relay row tells the job's owner lane only when
// someone can still act on it, and at most once per (owner lane, worker lane)
// per window. A worker lane whose jobs have all ended, or whose live job's hk
// task has ended, is logged instead; drops inside the window are folded into
// the next notice. Contract: docs/relay-harness-delivery.md.

import (
	"context"
	"encoding/json"
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

	"github.com/mgh3326/panewire/internal/fleet"
)

const (
	t3431Worker = "wk-t3431"
	t3431Design = "design-t3431"
	t3431Job    = "hk3431-fix"
	t3431Task   = "3431"
)

type t3431Clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *t3431Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *t3431Clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *t3431Clock) set(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = at
}

// t3431Jobs is the hk job-record store the hub reads to learn a job's task.
type t3431Jobs struct {
	records map[string]map[string]any
	err     error
}

func (t3431Jobs) Claim(context.Context, string, string, string) error { return nil }
func (t3431Jobs) Put(context.Context, string, map[string]any) error   { return nil }
func (j t3431Jobs) Get(_ context.Context, id string) (map[string]any, error) {
	if j.err != nil {
		return nil, j.err
	}
	if r, ok := j.records[id]; ok {
		return r, nil
	}
	return nil, fleet.ErrJobNotFound
}

// t3431Tasks answers GET /v1/tasks/<id> the way hk does. A task it does not
// hold is a 500: the read fails.
type t3431Tasks struct {
	mu    sync.Mutex
	tasks map[string]fleet.TaskStatus
	reads int
}

func (f *t3431Tasks) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads++
	id := strings.TrimPrefix(r.URL.Path, "/v1/tasks/")
	task, ok := f.tasks[id]
	if r.Method != http.MethodGet || !ok {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	var number int64
	fmt.Sscan(id, &number)
	_ = json.NewEncoder(w).Encode(map[string]any{"id": number, "state": task.State, "category": task.Category, "title": task.Title})
}

type t3431Rig struct {
	t     *testing.T
	hub   *HubServer
	clock *t3431Clock
	logs  *lockedBuffer
	dest  *hubAgent
	src   *hubAgent
	tasks *t3431Tasks
	seq   int
	// pending holds owner-lane notices drop read while it waited for the
	// worker-lane directive.
	pending []hubRelayInjectEvent
}

// t3431Start is a hub whose worker lane reports to the design lane, over a
// fake relay store, a fake hk job-record store and a fake hk task endpoint.
// The task endpoint is reached through the hk settings in the environment,
// pointed at the fake: no real configuration or credential is read.
func t3431Start(t *testing.T, jobs fleet.JobStore) *t3431Rig {
	t.Helper()
	_, client, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	tasks := &t3431Tasks{tasks: map[string]fleet.TaskStatus{}}
	taskServer := httptest.NewServer(tasks)
	t.Cleanup(taskServer.Close)
	t.Setenv("HANDOFFKEEP_CONFIG", filepath.Join(t.TempDir(), "absent.env"))
	t.Setenv("HANDOFFKEEP_URL", taskServer.URL)
	t.Setenv("HANDOFFKEEP_TOKEN", "test-token")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET", "")

	lanes, _ := json.Marshal(map[string]any{"lanes": map[string]reportRelayRoute{
		t3431Worker: {Machine: "host-a", Pane: "w1:pW", Parent: t3431Design},
		t3431Design: {Machine: "host-a", Pane: "w1:pD"},
	}})
	clock := &t3431Clock{now: time.Date(2026, 11, 7, 10, 42, 0, 0, time.UTC)}
	logs := &lockedBuffer{}
	hub, err := NewHubServer(HubServerConfig{
		Tokens:          map[string]string{"operator": "op", "host-a": "node"},
		ReportRelayPath: r20LanesFile(t, string(lanes)),
		Logger:          slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
		handoffkeep:     client,
		Now:             clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hub.Close() })
	hub.relayControl.relayAckTimeout = time.Hour
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: jobs, Out: out, Err: errout}, nil
	}
	rig := &t3431Rig{t: t, hub: hub, clock: clock, logs: logs, tasks: tasks}
	rig.dest = &hubAgent{relays: make(chan hubRelayInjectEvent, 64), persisted: make(chan hubRelayPersistedEvent, 64)}
	rig.src = &hubAgent{relays: make(chan hubRelayInjectEvent, 64), persisted: make(chan hubRelayPersistedEvent, 64)}
	hub.mu.Lock()
	hub.nodes["host-a"] = &hubNodeRecord{agent: rig.dest}
	hub.nodes["host-b"] = &hubNodeRecord{agent: rig.src}
	hub.mu.Unlock()
	return rig
}

// job registers one hub job running in the worker lane.
func (rig *t3431Rig) job(id string, completed, orphaned bool) {
	rig.hub.mu.Lock()
	defer rig.hub.mu.Unlock()
	rig.hub.jobs[id] = &hubJobRecord{
		HubActiveJob: HubActiveJob{JobID: id, AgentLabel: t3431Worker, Epoch: 1, OwnerLane: t3431Design},
		Node:         "host-b", LastSeen: rig.clock.Now(), Completed: completed, Orphaned: orphaned,
		FencedNodes: map[string]uint64{},
	}
}

func (rig *t3431Rig) task(id, state, category string) {
	rig.tasks.mu.Lock()
	defer rig.tasks.mu.Unlock()
	rig.tasks.tasks[id] = fleet.TaskStatus{State: state, Category: category, Title: "hk " + id + " (grade C, T2)"}
}

// drop relays one findings lane.event to the worker lane and has the node drop
// it as maybe_in_pane. It returns the durable row id and the drop.
func (rig *t3431Rig) drop() (int64, relayDroppedPayload) {
	rig.t.Helper()
	rig.seq++
	eventID := fmt.Sprintf("mergewake-findings-%d", rig.seq)
	event := hubJobEventPayload{JobID: laneEventTransportID(t3431Worker, eventID), Epoch: 1, OwnerLane: t3431Worker, EventID: eventID, NotifyType: "findings", Text: "[mergewake] findings for the worker: fix the failing check"}
	rig.hub.relayLaneEvent(event, rig.src)
	var directive hubRelayInjectEvent
	for directive.Lane != t3431Worker {
		select {
		case directive = <-rig.dest.relays:
			if directive.Lane == t3431Design {
				rig.pending = append(rig.pending, directive)
			}
		case <-time.After(2 * time.Second):
			rig.t.Fatal("the hub queued no relay.inject for the worker lane")
		}
	}
	if directive.EventID < 1 {
		rig.t.Fatalf("directive without a durable id: %+v", directive)
	}
	dropped := relayDroppedPayload{JobID: directive.JobID, Pane: directive.Pane, Lane: directive.Lane, OriginalEventID: directive.EventID, Reason: relayDroppedMaybeInPane}
	t650Send(rig.t, rig.hub, "host-a", rig.dest, "relay.dropped", dropped)
	return directive.EventID, dropped
}

// notices returns the owner-lane notices queued since the last call.
func (rig *t3431Rig) notices() []hubRelayInjectEvent {
	out := rig.pending
	rig.pending = nil
	for {
		select {
		case directive := <-rig.dest.relays:
			if directive.Lane == t3431Design {
				out = append(out, directive)
			}
		default:
			return out
		}
	}
}

// infoLines returns the INFO log lines that contain every one of parts.
func (rig *t3431Rig) infoLines(parts ...string) []string {
	var out []string
	for _, line := range strings.Split(rig.logs.String(), "\n") {
		if !strings.Contains(line, "level=INFO") {
			continue
		}
		matched := true
		for _, part := range parts {
			if !strings.Contains(line, part) {
				matched = false
				break
			}
		}
		if matched {
			out = append(out, line)
		}
	}
	return out
}

// t3431TodayText is the hk 3276 notice, byte for byte. The relay puts its own
// lane.event prefix in front of it, so a notice "is" today's text when it ends
// with it.
func t3431TodayText(row int64, eventID string) string {
	return fmt.Sprintf("[delivery unconfirmed] event %d (lane.event, producer id %s) for lane %s was not confirmed on host-a pane w1:pW: maybe_in_pane. The worker may not have it; read the pane or re-send. Reported once.", row, eventID, t3431Worker)
}

func t3431LiveJobWithTask() t3431Jobs {
	return t3431Jobs{records: map[string]map[string]any{t3431Job: {"state": "running", "task_id": float64(3431), "label": t3431Worker}}}
}

// Acceptance 1: the worker lane's only job has ended, so nobody is there to
// read or re-send: no owner-lane notice, one INFO line instead.
func TestUnconfirmedNoticeSkippedWhenWorkerJobEnded_t3431(t *testing.T) {
	for name, state := range map[string][2]bool{"completed": {true, false}, "orphaned": {false, true}} {
		t.Run(name, func(t *testing.T) {
			rig := t3431Start(t, t3431LiveJobWithTask())
			rig.task(t3431Task, "in_progress", "started")
			rig.job(t3431Job, state[0], state[1])

			row, _ := rig.drop()

			if got := rig.notices(); len(got) != 0 {
				t.Fatalf("an ended job's lane told its owner lane: %+v", got)
			}
			lines := rig.infoLines(fmt.Sprintf("event_id=%d", row), "lane="+t3431Worker, "reason=maybe_in_pane", "why=job_ended")
			if len(lines) != 1 {
				t.Fatalf("want exactly one INFO line naming the event, lane, reason and why; got %d in:\n%s", len(lines), rig.logs.String())
			}
		})
	}
}

// Acceptance 2: the live job's hk task has ended (merged, dropped, deployed or
// canceled), judged by hk's category, or by the state when hk sends none.
func TestUnconfirmedNoticeSkippedWhenTaskEnded_t3431(t *testing.T) {
	for _, tc := range []struct{ state, category string }{
		{"merged", "completed"}, {"dropped", "canceled"}, {"deployed", "completed"},
		{"merged", ""}, {"dropped", ""}, {"canceled", ""},
	} {
		t.Run(tc.state+"/"+tc.category, func(t *testing.T) {
			rig := t3431Start(t, t3431LiveJobWithTask())
			rig.task(t3431Task, tc.state, tc.category)
			rig.job(t3431Job, false, false)

			row, _ := rig.drop()

			if got := rig.notices(); len(got) != 0 {
				t.Fatalf("a %s task's lane told its owner lane: %+v", tc.state, got)
			}
			if lines := rig.infoLines(fmt.Sprintf("event_id=%d", row), "lane="+t3431Worker, "reason=maybe_in_pane", "why=task_ended"); len(lines) != 1 {
				t.Fatalf("want exactly one INFO line for the skipped notice; logs:\n%s", rig.logs.String())
			}
		})
	}
}

// Acceptance 3: a live job whose task is in progress gets the hk 3276 notice,
// unchanged.
func TestUnconfirmedNoticeSentForTaskInProgress_t3431(t *testing.T) {
	rig := t3431Start(t, t3431LiveJobWithTask())
	rig.task(t3431Task, "in_progress", "started")
	rig.job(t3431Job, false, false)

	row, _ := rig.drop()

	got := rig.notices()
	if len(got) != 1 {
		t.Fatalf("owner lane got %d notices, want 1: %+v", len(got), got)
	}
	if want := t3431TodayText(row, "mergewake-findings-1"); !strings.HasSuffix(got[0].Text, want) {
		t.Fatalf("notice text changed:\n got %q\nwant suffix %q", got[0].Text, want)
	}
	rig.tasks.mu.Lock()
	reads := rig.tasks.reads
	rig.tasks.mu.Unlock()
	if reads == 0 {
		t.Fatal("the hub never read the job's hk task")
	}
}

// Acceptance 4: seven drops inside the window send one notice; the first drop
// after the window sends the next one and names the six folded into it.
func TestUnconfirmedNoticeOncePerWindow_t3431(t *testing.T) {
	rig := t3431Start(t, t3431LiveJobWithTask())
	rig.task(t3431Task, "in_progress", "started")
	rig.job(t3431Job, false, false)
	first := rig.clock.Now()

	var sent []hubRelayInjectEvent
	var rows []int64
	for i := 0; i < 7; i++ {
		row, _ := rig.drop()
		rows = append(rows, row)
		sent = append(sent, rig.notices()...)
		rig.clock.advance(4 * time.Minute) // seven drops over 24 minutes
	}
	if len(sent) != 1 {
		t.Fatalf("seven drops inside 30 minutes sent %d notices, want 1: %+v", len(sent), sent)
	}
	if want := t3431TodayText(rows[0], "mergewake-findings-1"); !strings.HasSuffix(sent[0].Text, want) {
		t.Fatalf("the first notice is not the hk 3276 notice:\n got %q\nwant suffix %q", sent[0].Text, want)
	}
	for _, row := range rows[1:] {
		if lines := rig.infoLines(fmt.Sprintf("event_id=%d", row), "lane="+t3431Worker, "why=folded"); len(lines) != 1 {
			t.Fatalf("folded drop %d logged %d INFO lines, want 1; logs:\n%s", row, len(lines), rig.logs.String())
		}
	}

	rig.clock.set(first.Add(31 * time.Minute))
	row, _ := rig.drop()
	second := rig.notices()
	if len(second) != 1 {
		t.Fatalf("the drop after the window sent %d notices, want 1", len(second))
	}
	suffix := fmt.Sprintf("(+6 more unconfirmed for this lane since %s UTC)", first.Format("15:04"))
	if !strings.HasSuffix(second[0].Text, t3431TodayText(row, "mergewake-findings-8")+" "+suffix) {
		t.Fatalf("the notice after the window does not name the folded drops (%q):\n%q", suffix, second[0].Text)
	}
}

// Acceptance 5: a job record or task the hub cannot read fails open: the
// notice goes out as before.
func TestUnconfirmedNoticeFailsOpenWhenUnreadable_t3431(t *testing.T) {
	cases := map[string]struct {
		jobs     t3431Jobs
		withTask bool
	}{
		"job record unreadable": {jobs: t3431Jobs{err: fmt.Errorf("hk unavailable")}, withTask: true},
		"job record missing":    {jobs: t3431Jobs{records: map[string]map[string]any{}}, withTask: true},
		"task unreadable":       {jobs: t3431LiveJobWithTask(), withTask: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rig := t3431Start(t, tc.jobs)
			if tc.withTask {
				rig.task(t3431Task, "merged", "completed")
			}
			rig.job(t3431Job, false, false)

			row, _ := rig.drop()

			got := rig.notices()
			if len(got) != 1 || !strings.HasSuffix(got[0].Text, t3431TodayText(row, "mergewake-findings-1")) {
				t.Fatalf("an unreadable record must fail open with the hk 3276 notice; got %+v", got)
			}
		})
	}
}

// A worker lane the hub has no job for at all is not known to have ended (a
// hub restart forgets every ended job; a lane may run work the hub never
// placed), so it keeps the hk 3276 notice.
func TestUnconfirmedNoticeSentWhenHubKnowsNoJob_t3431(t *testing.T) {
	rig := t3431Start(t, t3431LiveJobWithTask())

	row, _ := rig.drop()

	got := rig.notices()
	if len(got) != 1 || !strings.HasSuffix(got[0].Text, t3431TodayText(row, "mergewake-findings-1")) {
		t.Fatalf("want the hk 3276 notice for a lane the hub has no job for; got %+v", got)
	}
}

// The same row dropped again inside the window is the hk 3276 duplicate, not
// another folded drop, and a second worker lane has its own window.
func TestUnconfirmedNoticeRedropIsNotFolded_t3431(t *testing.T) {
	rig := t3431Start(t, t3431LiveJobWithTask())
	rig.task(t3431Task, "in_progress", "started")
	rig.job(t3431Job, false, false)
	first := rig.clock.Now()

	_, dropped := rig.drop()
	if got := rig.notices(); len(got) != 1 {
		t.Fatalf("first drop sent %d notices, want 1", len(got))
	}
	t650Send(t, rig.hub, "host-a", rig.dest, "relay.dropped", dropped)
	if got := rig.notices(); len(got) != 0 {
		t.Fatalf("the same row escalated twice: %+v", got)
	}
	rig.drop()
	rig.clock.set(first.Add(31 * time.Minute))
	row, _ := rig.drop()
	got := rig.notices()
	if len(got) != 1 || !strings.HasSuffix(got[0].Text, fmt.Sprintf("(+1 more unconfirmed for this lane since %s UTC)", first.Format("15:04"))) || !strings.Contains(got[0].Text, fmt.Sprintf("[delivery unconfirmed] event %d ", row)) {
		t.Fatalf("a re-dropped row was counted as folded: %+v", got)
	}
}

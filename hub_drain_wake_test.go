package panewire

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// hk 4473 (operator decision 233): a design lane whose in-flight work drains
// while hk still shows open backlog for its project gets one interrupting
// line, at most once per relay.drain_wake_interval. The lane's project is its
// name without the -design suffix.

const (
	t4473Lane = "handoffkeep-design"
	t4473Pane = "w1:design"
	t4473Line = "2 backlog, 0 in work: dispatch the next batch"
	// t4473PageLimit is the page the hub asks hk for; hk's first page must be
	// full for the cursor to be followed.
	t4473PageLimit = 1000
)

type t4473Task struct {
	ID               int64
	Project          string
	State            string
	AwaitingDecision bool
}

// t4473Handler serves GET /v1/tasks from tasks the way hk does: a project and
// a state filter when given, hk's exclusive after_id cursor, and limit. Every
// other route goes to the relay fake.
func t4473Handler(relay http.Handler, tasks []t4473Task) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/tasks" {
			relay.ServeHTTP(w, r)
			return
		}
		query := r.URL.Query()
		after, _ := strconv.ParseInt(query.Get("after_id"), 10, 64)
		limit, _ := strconv.Atoi(query.Get("limit"))
		page := []map[string]any{}
		for _, task := range tasks {
			if task.ID <= after || len(page) >= limit {
				continue
			}
			if query.Has("project") && task.Project != query.Get("project") {
				continue
			}
			if query.Has("state") && task.State != query.Get("state") {
				continue
			}
			page = append(page, map[string]any{"id": task.ID, "project": task.Project, "state": task.State, "awaiting_decision": task.AwaitingDecision})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"tasks": page})
	})
}

// t4473Rig is the inbox-wake rig with an hk whose task list is tasks.
func t4473Rig(t *testing.T, tasks []t4473Task) (*t3602Rig, *t3602Clock) {
	t.Helper()
	fake := &fakeHandoffkeep{nextID: 100, status: http.StatusCreated, rows: map[string]*handoffkeepRelayEvent{}}
	server := httptest.NewServer(t4473Handler(fake, tasks))
	t.Cleanup(server.Close)
	client, err := newHandoffkeepRelayClient(hubHandoffkeepEnv{URL: server.URL, Token: "test-token"}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	start, _ := time.Parse(time.RFC3339, t3602Start)
	clock := &t3602Clock{now: start}
	return t3602Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", fake, client, clock), clock
}

// t4473Sweep runs one lane-stall sweep and waits for it, which is where the
// drain-wake check runs. It skips Sweep itself: Sweep pings the rig's agent,
// which has no socket.
func t4473Sweep(rig *t3602Rig) {
	rig.h.startLaneStallSweep(rig.h.now().UTC())
	rig.h.laneStallSweeps.Wait()
}

// t4473SetInFlight replaces the host's active jobs; the node record keeps its
// agent, so the lane stays routable.
func t4473SetInFlight(rig *t3602Rig, lane string, count int) {
	active := map[string]HubActiveJob{}
	for i := range count {
		id := "t4473-job-" + strconv.Itoa(i)
		active[id] = HubActiveJob{JobID: id, OwnerLane: lane}
	}
	rig.h.mu.Lock()
	rig.h.nodes["host-a"].activeJobs = active
	rig.h.mu.Unlock()
}

// t4473Lines drains the relay injects queued so far and returns the drain-wake
// lines among them.
func t4473Lines(rig *t3602Rig) []string {
	var out []string
	for {
		select {
		case ev := <-rig.agent.relays:
			if ev.Pane == t4473Pane && len(ev.Text) > 0 && ev.Text[0] >= '0' && ev.Text[0] <= '9' {
				out = append(out, ev.Text)
			}
		default:
			return out
		}
	}
}

// Drain edge: one line when in-flight falls to zero with open backlog; a second
// drain inside the interval waits, and fires once the interval has passed.
func TestDrainWakeOneLinePerIntervalAfterDrain(t *testing.T) {
	rig, clock := t4473Rig(t, []t4473Task{
		{ID: 1, Project: "handoffkeep", State: "backlog"},
		{ID: 2, Project: "handoffkeep", State: "unstarted"},
		{ID: 3, Project: "other", State: "backlog"},
		{ID: 4, Project: "handoffkeep", State: "developing"},
	})
	t4473SetInFlight(rig, t4473Lane, 1)
	t4473Sweep(rig)
	if got := t4473Lines(rig); len(got) != 0 {
		t.Fatalf("a lane with work in flight got a drain line: %q", got)
	}
	t4473SetInFlight(rig, t4473Lane, 0)
	t4473Sweep(rig)
	if got := t4473Lines(rig); len(got) != 1 || got[0] != t4473Line {
		t.Fatalf("drain lines=%q, want exactly [%q]", got, t4473Line)
	}
	t4473SetInFlight(rig, t4473Lane, 1)
	t4473Sweep(rig)
	t4473SetInFlight(rig, t4473Lane, 0)
	clock.advance(10 * time.Minute)
	t4473Sweep(rig)
	if got := t4473Lines(rig); len(got) != 0 {
		t.Fatalf("a second drain inside the interval sent %q", got)
	}
	clock.advance(time.Hour)
	t4473Sweep(rig)
	if got := t4473Lines(rig); len(got) != 1 || got[0] != t4473Line {
		t.Fatalf("the deferred drain after the interval sent %q, want exactly [%q]", got, t4473Line)
	}
}

// The backlog is read page by page: the project's backlog runs past hk's first
// full page of stable ID order, and an unstarted task after it is counted too.
func TestDrainWakeReadsBacklogPastFirstPage(t *testing.T) {
	tasks := make([]t4473Task, 0, t4473PageLimit+2)
	for id := int64(1); id <= t4473PageLimit; id++ {
		tasks = append(tasks, t4473Task{ID: id, Project: "handoffkeep", State: "backlog"})
	}
	tasks = append(tasks,
		t4473Task{ID: t4473PageLimit + 1, Project: "handoffkeep", State: "unstarted"},
		t4473Task{ID: t4473PageLimit + 2, Project: "other", State: "backlog"},
	)
	rig, _ := t4473Rig(t, tasks)
	t4473SetInFlight(rig, t4473Lane, 1)
	t4473Sweep(rig)
	t4473SetInFlight(rig, t4473Lane, 0)
	t4473Sweep(rig)
	want := "1001 backlog, 0 in work: dispatch the next batch"
	if got := t4473Lines(rig); len(got) != 1 || got[0] != want {
		t.Fatalf("drain lines=%q, want exactly [%q]", got, want)
	}
}

// A task waiting on the operator (decision 233) is not open backlog: the drain
// counts only the pre-start tasks nobody is blocked on.
func TestDrainWakeSkipsTasksAwaitingDecision(t *testing.T) {
	rig, _ := t4473Rig(t, []t4473Task{
		{ID: 1, Project: "handoffkeep", State: "backlog", AwaitingDecision: true},
		{ID: 2, Project: "handoffkeep", State: "unstarted", AwaitingDecision: true},
		{ID: 3, Project: "handoffkeep", State: "backlog"},
		{ID: 4, Project: "handoffkeep", State: "unstarted"},
	})
	t4473SetInFlight(rig, t4473Lane, 1)
	t4473Sweep(rig)
	t4473SetInFlight(rig, t4473Lane, 0)
	t4473Sweep(rig)
	want := "2 backlog, 0 in work: dispatch the next batch"
	if got := t4473Lines(rig); len(got) != 1 || got[0] != want {
		t.Fatalf("drain lines=%q, want exactly [%q]", got, want)
	}
}

// Nothing open for the project: a drain sends nothing.
func TestDrainWakeSilentWithoutOpenBacklog(t *testing.T) {
	rig, _ := t4473Rig(t, []t4473Task{
		{ID: 1, Project: "handoffkeep", State: "developing"},
		{ID: 2, Project: "other", State: "backlog"},
	})
	t4473SetInFlight(rig, t4473Lane, 1)
	t4473Sweep(rig)
	t4473SetInFlight(rig, t4473Lane, 0)
	t4473Sweep(rig)
	if got := t4473Lines(rig); len(got) != 0 {
		t.Fatalf("a drain with no open backlog sent %q", got)
	}
}

// relay.drain_wake_interval and relay.drain_wake_threshold parse and refuse as
// documented: the interval defaults to 1h and is 10m..24h; the threshold is 0..10.
func TestDrainWakeConfig(t *testing.T) {
	base := "default: interrupt\n"
	policy, err := parseDeliveryPolicy([]byte(base))
	if err != nil {
		t.Fatalf("default policy refused: %v", err)
	}
	if policy.Relay.drainWakeInterval != defaultDrainWakeInterval || policy.Relay.DrainWakeThreshold != 0 {
		t.Fatalf("defaults interval=%v threshold=%d, want %v and 0", policy.Relay.drainWakeInterval, policy.Relay.DrainWakeThreshold, defaultDrainWakeInterval)
	}
	for _, good := range []string{
		"relay:\n  drain_wake_interval: 30m\n  drain_wake_threshold: 2\n",
		"relay:\n  drain_wake_interval: 10m\n",
		"relay:\n  drain_wake_interval: 24h\n  drain_wake_threshold: 10\n",
	} {
		if _, err := parseDeliveryPolicy([]byte(base + good)); err != nil {
			t.Fatalf("policy %q refused: %v", good, err)
		}
	}
	for _, bad := range []string{
		"relay:\n  drain_wake_interval: 9m\n",
		"relay:\n  drain_wake_interval: 25h\n",
		"relay:\n  drain_wake_interval: soon\n",
		"relay:\n  drain_wake_threshold: -1\n",
		"relay:\n  drain_wake_threshold: 11\n",
	} {
		if _, err := parseDeliveryPolicy([]byte(base + bad)); err == nil {
			t.Fatalf("policy %q was accepted", bad)
		}
	}
}

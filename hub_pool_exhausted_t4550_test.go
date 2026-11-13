package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/mgh3326/panewire/internal/fleet"
)

const (
	sweepDevin = "devin:0123456789abcdef"
	sweepCodex = "codex:fedcba9876543210"
)

// syncBuffer is a log sink the hub's goroutines and the test share.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *syncBuffer) lines(contains string) []string {
	var out []string
	for _, line := range strings.Split(b.String(), "\n") {
		if strings.Contains(line, contains) {
			out = append(out, line)
		}
	}
	return out
}

// sweepNode plays the holder node of every job in the sweep tests: it lists
// the panes it still has, saves and closes a pane, and ends an hk record the
// way the real node does (after its pane is gone), recording each command.
type sweepNode struct {
	mu       sync.Mutex
	conn     *websocket.Conn
	jobs     *quotaJobs
	panes    map[string]nodeAgentRow // by pane id
	commands []string                // "<op> <job or pane>"
	reasons  map[string]string       // job -> end_reason written

}

func (n *sweepNode) serve(ctx context.Context) {
	for {
		_, payload, err := n.conn.Read(ctx)
		if err != nil {
			return
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(payload, &fields) != nil || string(fields["type"]) != `"node.command"` {
			continue
		}
		var requestID, op string
		_ = json.Unmarshal(fields["request_id"], &requestID)
		_ = json.Unmarshal(fields["op"], &op)
		result := n.answer(op, fields["args"])
		_ = wsjson.Write(ctx, n.conn, map[string]any{"type": "node.command.result", "request_id": requestID, "op": op, "result": result})
	}
}

func (n *sweepNode) answer(op string, raw json.RawMessage) map[string]any {
	n.mu.Lock()
	defer n.mu.Unlock()
	switch op {
	case "agents.list":
		rows := make([]nodeAgentRow, 0, len(n.panes))
		for _, row := range n.panes {
			rows = append(rows, row)
		}
		return map[string]any{"agents": []nodeAgentRow{}, "panes": rows, "panes_ok": true}
	case "pane.capture":
		var args nodePaneCaptureArgs
		_ = json.Unmarshal(raw, &args)
		n.commands = append(n.commands, op+" "+args.Name)

		return map[string]any{"outcome": nodeCaptureScreen, "saved": "/jobs/" + args.Name + "/pane-screen.txt"}
	case "pane.close":
		var args nodePaneCloseArgs
		_ = json.Unmarshal(raw, &args)
		n.commands = append(n.commands, op+" "+args.Name)

		delete(n.panes, args.PaneID)
		return map[string]any{"outcome": nodePaneClosed}
	case "job.mark_lost":
		var args nodeJobMarkLostArgs
		_ = json.Unmarshal(raw, &args)
		n.commands = append(n.commands, op+" "+args.Job)

		n.reasons[args.Job] = args.EndReason
		n.jobs.mu.Lock()

		n.jobs.records[args.Job]["state"] = "lost"
		n.jobs.records[args.Job]["end_reason"] = args.EndReason

		n.jobs.mu.Unlock()
		return map[string]any{"outcome": nodeJobMarkLostLost}

	}
	return map[string]any{}
}

func (n *sweepNode) count(command string) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	total := 0
	for _, got := range n.commands {
		if got == command {
			total++
		}
	}
	return total
}

func (n *sweepNode) setStatus(pane, status string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	row := n.panes[pane]
	row.AgentStatus = status
	n.panes[pane] = row
}

func (n *sweepNode) reason(job string) string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.reasons[job]
}

type sweepFixture struct {
	*quotaReplaceFixture
	node2 *sweepNode
	logs  *syncBuffer
	// owner is the lane that owns the jobs addJobOn adds; "parent" routes.
	owner string
	next  atomic.Int32
}

// newSweepFixture is the quota-replace fixture with the holder node playing
// every pane. Its own job "seed" has no placement record and sits on pane
// w2:p3, idle: the sweep must never touch it.
func newSweepFixture(t *testing.T) *sweepFixture {
	t.Helper()
	base := newQuotaReplaceFixture(t, "implementer", "seed")
	logs := &syncBuffer{}
	base.hub.logger = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	base.jobs.records["seed"]["pool"] = "devin"
	node := &sweepNode{conn: base.node, jobs: base.jobs, reasons: map[string]string{}, panes: map[string]nodeAgentRow{
		roleHeldPane: {PaneID: roleHeldPane, Name: "seed", AgentStatus: "idle", Cwd: "/work/project.checkout"},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go node.serve(ctx)
	f := &sweepFixture{quotaReplaceFixture: base, node2: node, logs: logs, owner: "parent"}
	// The hub places a successor as soon as the sweep claims it; the test is
	// the placement.
	go func() {
		for ctx.Err() == nil {
			for i := 1; i <= int(f.next.Load()); i++ {
				successor, err := f.hub.inbox.store.latestSpawnForJob(ctx, "sweep-"+strconv.Itoa(i)+"-q1")
				if err == nil && successor.Status == "accepted" {
					_, _ = f.hub.inbox.store.completeSpawn(ctx, successor, hubSpawnOutcome{RC: 0, Stage: "done", Host: quotaPlaced, Pane: "w9:p1", Profile: "sonnet"}, time.Now())
				}
			}
			time.Sleep(5 * time.Millisecond) // placement latency of the fake hub
		}
	}()
	return f
}

// addJob is addJobOn a free-tier rung, devin-swe2.
func (f *sweepFixture) addJob(t *testing.T, role, pool, account, status string) string {
	t.Helper()
	return f.addJobOn(t, "devin-swe2", role, pool, account, status)
}

// addJobOn records a live job of role on profile, pool/account as a node placed it
// (account "" is an operation with no placement record), with its hk record
// and its pane in status. It returns the job id.
func (f *sweepFixture) addJobOn(t *testing.T, profile, role, pool, account, status string) string {
	t.Helper()
	n := int(f.next.Add(1))
	return f.addNamedJob(t, profile, role, pool, account, status, "sweep-"+strconv.Itoa(n), n)
}

func (f *sweepFixture) addNamedJob(t *testing.T, profile, role, pool, account, status, job string, n int) string {
	t.Helper()
	request := quotaHolderRequest(role, job)
	id, err := fleet.NewRequestID()
	if err != nil {
		t.Fatal(err)
	}
	request.RequestID = id
	for i := 0; i+1 < len(request.Args); i++ {
		switch request.Args[i] {
		case "--task":
			request.Args[i+1] = strconv.Itoa(100 + n)
		case "--owner":
			request.Args[i+1] = f.owner
		}
	}
	held, _, err := f.hub.inbox.store.claimSpawn(t.Context(), request, job, f.owner, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	pane := "w" + strconv.Itoa(10+n) + ":p1"
	outcome := hubSpawnOutcome{RC: 0, Stage: "done", Host: "machine-a", Pane: pane, Role: role, Profile: profile}
	if account != "" {
		outcome.Pool, outcome.AccountKey = pool, account
	}
	if _, err = f.hub.inbox.store.completeSpawn(t.Context(), held, outcome, time.Now()); err != nil {
		t.Fatal(err)
	}
	f.jobs.mu.Lock()
	f.jobs.records[job] = map[string]any{"state": "spawned", "pool": pool}
	f.jobs.mu.Unlock()
	f.node2.mu.Lock()
	f.node2.panes[pane] = nodeAgentRow{PaneID: pane, Name: job, AgentStatus: status, Cwd: "/work/project.checkout"}
	f.node2.mu.Unlock()
	return job
}

func (f *sweepFixture) state(job string) string {
	f.jobs.mu.Lock()
	defer f.jobs.mu.Unlock()
	state, _ := f.jobs.records[job]["state"].(string)
	return state
}

// publishFreeLimit has the node publish devin's free bucket as exhausted until
// reset, as a node does after a free-limit record reaches the shared store.
// measured moves the value on so the hub records it again.
func (f *sweepFixture) publishFreeLimit(t *testing.T, account string, reset, measured time.Time) {
	t.Helper()
	pool, _, _ := strings.Cut(account, ":")
	payload := map[string]any{
		"kind": "quota", "key": account, "measured_at": measured.UTC().Format(time.RFC3339),
		"quota": map[string]any{"account_key": account, "pool": pool, "free_bucket": map[string]any{"state": "exhausted", "resets_at": reset.UTC().Format(time.RFC3339)}},
	}
	if err := wsjson.Write(t.Context(), f.node, map[string]any{"type": "event", "kind": "state", "payload": payload}); err != nil {
		t.Fatal(err)
	}
}

// sweeps counts the passes that listed jobs (their summary lines).
func (f *sweepFixture) sweeps() int { return len(f.logs.lines("pool exhausted sweep")) }

func TestPoolExhaustedNotifiesOwnerWithoutEndingJob(t *testing.T) {
	for _, role := range []string{"implementer", "reviewer", "consult"} {
		t.Run(role, func(t *testing.T) {
			f := newSweepFixture(t)
			job := f.addJob(t, role, "devin", sweepDevin, "idle")
			task, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), job)
			if err != nil {
				t.Fatal(err)
			}
			e := poolExhaustion{Pool: "devin", Account: sweepDevin, Cause: poolCauseFreeLimit, Reset: time.Now().Add(time.Hour)}
			if result := f.hub.sweepPoolJob(t.Context(), e, task); result.action != poolJobNotified {
				t.Fatalf("result=%+v", result)
			}
			if got := f.state(job); got != "spawned" {
				t.Fatalf("state=%s, want spawned", got)
			}
			if got := f.node2.count("pane.close " + job); got != 0 {
				t.Fatalf("pane closed %d times", got)
			}
			if result := f.hub.sweepPoolJob(t.Context(), e, task); result.action != poolJobNotified {
				t.Fatalf("retry result=%+v", result)
			}
			events := spawnFailedInjections(f.destination)
			if len(events) != 1 || events[0].Kind != "lane.event" || !strings.Contains(events[0].Text, job) || !strings.Contains(events[0].Text, "hand it over or end it") {
				t.Fatalf("events=%+v", events)
			}
		})
	}
}

func TestPoolExhaustedRoutesMergewakeReviewerNotice(t *testing.T) {
	f := newSweepFixture(t)
	routes := `{"lanes":{"mergewake":{"machine":"machine-b","pane":"w1:p1"}}}`
	if err := os.WriteFile(f.hub.reportRelayPath, []byte(routes), 0o600); err != nil {
		t.Fatal(err)
	}
	job := f.addNamedJob(t, "devin-swe2", "reviewer", "devin", sweepDevin, "idle", "mwr-99-p1", 99)
	task, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), job)
	if err != nil {
		t.Fatal(err)
	}
	e := poolExhaustion{Pool: "devin", Account: sweepDevin, Cause: poolCauseFreeLimit, Reset: time.Now().Add(time.Hour)}
	if result := f.hub.sweepPoolJob(t.Context(), e, task); result.action != poolJobNotified {
		t.Fatalf("result=%+v", result)
	}
	events := spawnFailedInjections(f.destination)
	if len(events) != 1 || events[0].Lane != "mergewake" || !strings.Contains(events[0].Text, job) {
		t.Fatalf("events=%+v", events)
	}
}

func TestPoolExhaustedRetriesAnUnpersistedOwnerNotice(t *testing.T) {
	f := newSweepFixture(t)
	job := f.addJob(t, "reviewer", "devin", sweepDevin, "idle")
	task, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), job)
	if err != nil {
		t.Fatal(err)
	}
	e := poolExhaustion{Pool: "devin", Account: sweepDevin, Cause: poolCauseFreeLimit, Reset: time.Now().Add(time.Hour)}
	hk := f.hub.handoffkeep
	f.hub.handoffkeep = nil
	if result := f.hub.sweepPoolJob(t.Context(), e, task); result.action != poolJobSkipped {
		t.Fatalf("result=%+v, want retry", result)
	}
	f.hub.handoffkeep = hk
	if result := f.hub.sweepPoolJob(t.Context(), e, task); result.action != poolJobNotified {
		t.Fatalf("retry result=%+v", result)
	}
	if events := spawnFailedInjections(f.destination); len(events) != 1 {
		t.Fatalf("events=%+v, want one notice", events)
	}
}

func TestPoolExhaustedIgnoresExpiredAndSupersededEvents(t *testing.T) {
	f := newSweepFixture(t)
	job := f.addJob(t, "reviewer", "devin", sweepDevin, "idle")
	task, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), job)
	if err != nil {
		t.Fatal(err)
	}
	e := poolExhaustion{Pool: "devin", Account: sweepDevin, Cause: poolCauseFreeLimit, Reset: time.Now().Add(-time.Second)}
	if result := f.hub.sweepPoolJob(t.Context(), e, task); result.action != poolJobSkipped {
		t.Fatalf("expired result=%+v", result)
	}
	if events := spawnFailedInjections(f.destination); len(events) != 0 {
		t.Fatalf("expired event delivered: %+v", events)
	}
	now := time.Now().UTC()
	f.hub.stateStore().publish(FleetStateMessage{Host: "newer-host", Kind: fleetStateKindQuota, Key: sweepDevin, MeasuredAt: now, ReceivedAt: now, Quota: &FleetStateQuota{Pool: "devin", AccountKey: sweepDevin, Free: &FleetStateFree{State: "available"}}}, 0)
	e.Reset = now.Add(time.Hour)
	if result := f.hub.sweepPoolJob(t.Context(), e, task); result.action != poolJobSkipped {
		t.Fatalf("superseded result=%+v", result)
	}
	if events := spawnFailedInjections(f.destination); len(events) != 0 {
		t.Fatalf("superseded event delivered: %+v", events)
	}
}

func TestPoolExhaustedPublishedQuotaNotifiesOnlyMatchingLiveJobs(t *testing.T) {
	f := newSweepFixture(t)
	jobs := []string{
		f.addJob(t, "implementer", "devin", sweepDevin, "idle"),
		f.addJob(t, "reviewer", "devin", sweepDevin, "idle"),
		f.addJob(t, "consult", "devin", sweepDevin, "working"),
	}
	other := f.addJob(t, "reviewer", "devin", "devin:1111111111111111", "idle")
	reset := time.Now().Add(time.Hour)
	f.publishFreeLimit(t, sweepDevin, reset, time.Now())
	eventually(t, 5*time.Second, "the matching notices", func() bool { return f.sweeps() == 1 })
	events := spawnFailedInjections(f.destination)
	if len(events) != len(jobs) {
		t.Fatalf("events=%+v, want %d", events, len(jobs))
	}
	for _, job := range append(jobs, other) {
		if state := f.state(job); state != "spawned" {
			t.Fatalf("job %s state=%s, want spawned", job, state)
		}
		if got := f.node2.count("pane.close " + job); got != 0 {
			t.Fatalf("job %s closed %d times", job, got)
		}
		if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), job+"-q1"); err == nil {
			t.Fatalf("job %s unexpectedly received a successor", job)
		}
	}
	for _, event := range events {
		if event.Kind != "lane.event" || strings.Contains(event.Text, other) {
			t.Fatalf("unexpected notice %+v", event)
		}
	}
	f.publishFreeLimit(t, sweepDevin, reset, time.Now().Add(2*time.Minute))
	eventually(t, 5*time.Second, "the repeated sweep", func() bool { return f.sweeps() == 2 })
	if extra := spawnFailedInjections(f.destination); len(extra) != 0 {
		t.Fatalf("duplicate notices: %+v", extra)
	}
}

func TestPoolExhaustedNoticeUsesOneDurableRecordPerJob(t *testing.T) {
	f := newSweepFixture(t)
	fake, client, stop := newFakeHandoffkeep(t)
	defer stop()
	f.hub.handoffkeep = client
	job := f.addJob(t, "reviewer", "devin", sweepDevin, "idle")
	task, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), job)
	if err != nil {
		t.Fatal(err)
	}
	e := poolExhaustion{Pool: "devin", Account: sweepDevin, Cause: poolCauseFreeLimit, Reset: time.Now().Add(time.Hour)}
	if result := f.hub.sweepPoolJob(t.Context(), e, task); result.action != poolJobNotified {
		t.Fatalf("result=%+v", result)
	}
	// A restarted hub has no in-memory relay dedupe state.
	f.hub.mu.Lock()
	f.hub.relayDedupe = make(map[string]int64)
	f.hub.lanePersisted = make(map[string]int64)
	f.hub.laneEventSHA = make(map[string]string)
	f.hub.mu.Unlock()
	if result := f.hub.sweepPoolJob(t.Context(), e, task); result.action != poolJobNotified {
		t.Fatalf("retry result=%+v", result)
	}
	if got := fake.rowCount(); got != 1 {
		t.Fatalf("durable rows=%d, want one", got)
	}
}

func TestPoolExhaustedOnlyAccountWideWindowsTrigger(t *testing.T) {
	now := time.Now().UTC()
	used := 100.0
	reset := now.Add(time.Hour).Format(time.RFC3339)
	quota := &FleetStateQuota{Pool: "devin", AccountKey: sweepDevin, Buckets: []FleetStateBucket{
		{Window: "model", Scope: "model", UsedPct: &used, ResetsAt: reset},
		{Window: "group", Scope: "group", UsedPct: &used, ResetsAt: reset},
		{Window: "scoped-model", Scope: "account", Model: "x", UsedPct: &used, ResetsAt: reset},
	}}
	if got := poolExhaustionsOf(quota, now); len(got) != 0 {
		t.Fatalf("non-account buckets triggered: %+v", got)
	}
	quota.Buckets = append(quota.Buckets, FleetStateBucket{Window: "weekly", Scope: "account", UsedPct: &used, ResetsAt: reset})
	if got := poolExhaustionsOf(quota, now); len(got) != 1 || got[0].Cause != "window:weekly" {
		t.Fatalf("account window result=%+v", got)
	}
}

func TestPoolExhaustionCoversOnlyBucketProfiles(t *testing.T) {
	free := poolExhaustion{Cause: poolCauseFreeLimit}
	paid := poolExhaustion{Cause: "window:weekly"}
	if !free.covers("devin-swe2") || free.covers("devin-ds41") || paid.covers("devin-swe2") || !paid.covers("devin-ds41") || free.covers("") || paid.covers("") {
		t.Fatal("free and paid quota buckets matched the wrong profiles")
	}
}

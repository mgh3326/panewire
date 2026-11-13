package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
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
	next  atomic.Int32
}

// newSweepFixture is the quota-replace fixture with the holder node playing
// every pane. Its own job "seed" has no placement record and sits on pane
// w2:p3, idle: the sweep must never touch it.
func newSweepFixture(t *testing.T) *sweepFixture {
	t.Helper()
	base := newQuotaReplaceFixture(t, "worker", "seed")
	logs := &syncBuffer{}
	base.hub.logger = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	base.jobs.records["seed"]["pool"] = "devin"
	node := &sweepNode{conn: base.node, jobs: base.jobs, reasons: map[string]string{}, panes: map[string]nodeAgentRow{
		roleHeldPane: {PaneID: roleHeldPane, Name: "seed", AgentStatus: "idle", Cwd: "/work/project.checkout"},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go node.serve(ctx)
	f := &sweepFixture{quotaReplaceFixture: base, node2: node, logs: logs}
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
	job := "sweep-" + strconv.Itoa(n)
	request := quotaHolderRequest(role, job)
	id, err := fleet.NewRequestID()
	if err != nil {
		t.Fatal(err)
	}
	request.RequestID = id
	for i := 0; i+1 < len(request.Args); i++ {
		if request.Args[i] == "--task" {
			request.Args[i+1] = strconv.Itoa(100 + n)
		}
	}
	held, _, err := f.hub.inbox.store.claimSpawn(t.Context(), request, job, "parent", time.Now())
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

func (f *sweepFixture) sweeps() int { return len(f.logs.lines("pool exhausted sweep")) }

// hk 4550: a published free limit for devin hands over its live workers and
// ends its reviewer, in one pass, and leaves every job that is not on that
// pool and account alone; seeing the same exhaustion again acts on nothing.
func TestPoolExhaustedFreeLimitHandsOverWorkersAndEndsReviewer(t *testing.T) {
	f := newSweepFixture(t)
	workerA := f.addJob(t, "worker", "devin", sweepDevin, "idle")
	workerB := f.addJob(t, "worker", "devin", sweepDevin, "idle")
	reviewer := f.addJob(t, "reviewer", "devin", sweepDevin, "idle")
	codex := f.addJob(t, "worker", "codex", sweepCodex, "idle")
	otherAccount := f.addJob(t, "worker", "devin", "devin:1111111111111111", "idle")
	unplaced := f.addJob(t, "worker", "devin", "", "idle")
	reset := time.Now().Add(3 * time.Hour).UTC().Truncate(time.Second)

	f.publishFreeLimit(t, sweepDevin, reset, time.Now())
	eventually(t, 5*time.Second, "the sweep's summary line", func() bool { return f.sweeps() == 1 })

	for _, job := range []string{workerA, workerB} {
		if got := f.state(job); got != "lost" {
			t.Fatalf("worker %s state=%s, want handed over (lost)", job, got)
		}
		if want := "quota_stop pool=devin handed_over_to=" + job + "-q1"; f.node2.reason(job) != want {
			t.Fatalf("worker %s end_reason=%q, want %q", job, f.node2.reason(job), want)
		}
		successor, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), job+"-q1")
		if err != nil {
			t.Fatalf("worker %s has no successor: %v", job, err)
		}
		if got := hubSpawnArg(successor.Request.Args, "--exclude-pool"); got != "devin" {
			t.Fatalf("successor of %s excludes %q, want the exhausted pool devin", job, got)
		}
		if got := hubSpawnArg(successor.Request.Args, "-m"); got != "auto" {
			t.Fatalf("successor of %s uses -m %q, want auto", job, got)
		}
	}
	if got := f.state(reviewer); got != "lost" {
		t.Fatalf("reviewer state=%s, want ended (lost)", got)
	}
	if want := "quota_stop pool=devin reset_at=" + reset.Format(time.RFC3339); f.node2.reason(reviewer) != want {
		t.Fatalf("reviewer end_reason=%q, want %q", f.node2.reason(reviewer), want)
	}
	if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), reviewer+"-q1"); err == nil {
		t.Fatal("a reviewer is ended for its owner to respawn, not replaced by the hub")
	}
	var injected []hubRelayInjectEvent
	eventually(t, 3*time.Second, "the owner's job.lost for the reviewer", func() bool {
		injected = append(injected, spawnFailedInjections(f.destination)...)
		for _, relay := range injected {
			if relay.Kind == "job.lost" && relay.JobID == reviewer && strings.Contains(relay.Text, "quota_stop") && strings.Contains(relay.Text, "pool=devin") && strings.Contains(relay.Text, "reset_at="+reset.Format(time.RFC3339)) {
				return true
			}
		}
		return false
	})
	for _, job := range []string{codex, otherAccount, unplaced, "seed"} {
		if got := f.state(job); got != "spawned" {
			t.Fatalf("job %s off the pool was touched: state=%s", job, got)
		}
		if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), job+"-q1"); err == nil {
			t.Fatalf("job %s off the pool got a successor", job)
		}
	}
	// One hk record per acted job: three jobs ended, nothing else written.
	if got := f.node2.count("job.mark_lost " + workerA); got != 1 {
		t.Fatalf("worker A written %d times", got)
	}
	summary := f.logs.lines("pool exhausted sweep")[0]
	for _, want := range []string{"pool=devin", "handed_over=2", "ended=1"} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary %q lacks %q", summary, want)
		}
	}
	if got := len(f.logs.lines("pool exhausted job")); got != 3 {
		t.Fatalf("%d per-job lines, want one for each of the three jobs acted on:\n%s", got, f.logs.String())
	}

	// The same exhaustion again (the node republishes a moved measurement):
	// the hub looks, and acts on nothing.
	f.publishFreeLimit(t, sweepDevin, reset, time.Now().Add(2*time.Minute))
	eventually(t, 5*time.Second, "the second pass", func() bool { return f.sweeps() == 2 })
	for _, job := range []string{workerA, workerB, reviewer} {
		if got := f.node2.count("job.mark_lost " + job); got != 1 {
			t.Fatalf("job %s was written %d times after the same exhaustion was seen twice", job, got)
		}
	}
	for _, job := range []string{workerA, workerB} {
		if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), job+"-q1-q2"); err == nil {
			t.Fatalf("job %s was handed over twice", job)
		}
	}
	if got := len(f.logs.lines("pool exhausted job")); got != 3 {
		t.Fatalf("the repeated pass acted again: %d per-job lines", got)
	}
}

// A job whose pane is still working is not touched; the next time the pool is
// seen exhausted and the pane has gone idle, it is handed over.
func TestPoolExhaustedLeavesAWorkingPaneForTheNextPass(t *testing.T) {
	f := newSweepFixture(t)
	busy := f.addJob(t, "worker", "devin", sweepDevin, "working")
	reset := time.Now().Add(3 * time.Hour)

	f.publishFreeLimit(t, sweepDevin, reset, time.Now())
	eventually(t, 5*time.Second, "the first pass", func() bool { return f.sweeps() == 1 })
	if got := f.state(busy); got != "spawned" {
		t.Fatalf("a working pane was handed over: state=%s", got)
	}
	if got := f.node2.count("pane.close " + busy); got != 0 {
		t.Fatal("a working pane was closed")
	}

	f.node2.setStatus("w11:p1", "idle")
	f.publishFreeLimit(t, sweepDevin, reset, time.Now().Add(2*time.Minute))
	eventually(t, 5*time.Second, "the busy worker to be handed over", func() bool { return f.state(busy) == "lost" })
}

// A used-up account window of a measurement is an exhausted pool as well; a
// window scoped to one model group is not.
func TestPoolExhaustedByAnAccountWindow(t *testing.T) {
	f := newSweepFixture(t)
	reviewer := f.addJobOn(t, "devin-ds41", "reviewer", "devin", sweepDevin, "idle")
	freeWorker := f.addJob(t, "worker", "devin", sweepDevin, "idle")
	reset := time.Now().Add(2 * time.Hour).UTC().Truncate(time.Second)
	publish := func(measured time.Time, buckets []map[string]any) {
		payload := map[string]any{
			"kind": "quota", "key": sweepDevin, "measured_at": measured.UTC().Format(time.RFC3339),
			"quota": map[string]any{"account_key": sweepDevin, "pool": "devin", "buckets": buckets},
		}
		if err := wsjson.Write(t.Context(), f.node, map[string]any{"type": "event", "kind": "state", "payload": payload}); err != nil {
			t.Fatal(err)
		}
	}
	publish(time.Now(), []map[string]any{
		{"window": "weekly", "used_pct": 100, "resets_at": reset.Format(time.RFC3339), "scope": "group", "model": "opus"},
		{"window": "5h", "used_pct": 99.5, "resets_at": reset.Format(time.RFC3339), "scope": "account"},
	})
	publish(time.Now().Add(time.Minute), []map[string]any{{"window": "weekly", "used_pct": 100, "resets_at": reset.Format(time.RFC3339), "scope": "account"}})
	eventually(t, 5*time.Second, "the reviewer to be ended", func() bool { return f.state(reviewer) == "lost" })
	if want := "quota_stop pool=devin reset_at=" + reset.Format(time.RFC3339); f.node2.reason(reviewer) != want {
		t.Fatalf("end_reason=%q, want %q", f.node2.reason(reviewer), want)
	}
	// The model group's window started no pass: only the account window did.
	eventually(t, 5*time.Second, "the sweep's summary line", func() bool { return f.sweeps() == 1 })
	// A paid window pays for the paid rungs only: the free-tier worker stays.
	if got := f.state(freeWorker); got != "spawned" {
		t.Fatalf("a paid window reached a free-rung job: state=%s", got)
	}
}

// A free limit exhausts the free rungs only: a devin-swe2 worker is handed over
// and a devin-ds41 worker on the same account is left untouched, as is a
// reviewer on a paid rung.
func TestPoolExhaustedFreeLimitLeavesPaidRungsAlone(t *testing.T) {
	f := newSweepFixture(t)
	free := f.addJobOn(t, "devin-swe2", "worker", "devin", sweepDevin, "idle")
	paid := f.addJobOn(t, "devin-ds41", "worker", "devin", sweepDevin, "idle")
	paidReviewer := f.addJobOn(t, "devin-ds41", "reviewer", "devin", sweepDevin, "idle")

	f.publishFreeLimit(t, sweepDevin, time.Now().Add(3*time.Hour), time.Now())
	eventually(t, 5*time.Second, "the sweep's summary line", func() bool { return f.sweeps() == 1 })
	if got := f.state(free); got != "lost" {
		t.Fatalf("the free-rung worker state=%s, want handed over", got)
	}
	for _, job := range []string{paid, paidReviewer} {
		if got := f.state(job); got != "spawned" {
			t.Fatalf("paid-rung job %s was touched by a free limit: state=%s", job, got)
		}
		if got := f.node2.count("pane.close " + job); got != 0 {
			t.Fatalf("paid-rung job %s had its pane closed", job)
		}
	}
	if summary := f.logs.lines("pool exhausted sweep")[0]; !strings.Contains(summary, "jobs=1 ") || !strings.Contains(summary, "handed_over=1") {
		t.Fatalf("summary %q", summary)
	}
}

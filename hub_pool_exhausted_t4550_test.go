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
	mu                sync.Mutex
	conn              *websocket.Conn
	jobs              *quotaJobs
	panes             map[string]nodeAgentRow // by pane id
	commands          []string                // "<op> <job or pane>"
	reasons           map[string]string       // job -> end_reason written
	markers           map[string]string       // job -> registered pool action id
	failReadAfterMark bool
	afterCapture      func()
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
		if n.afterCapture != nil {
			n.afterCapture()
		}
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
		if n.failReadAfterMark {
			n.jobs.failGets = 1
		}
		n.jobs.mu.Unlock()
		return map[string]any{"outcome": nodeJobMarkLostLost}
	case "job.pool_action":
		var args nodeJobPoolActionArgs
		_ = json.Unmarshal(raw, &args)
		n.commands = append(n.commands, op+" "+args.Job+" "+args.Op)
		if args.Op == nodePoolActionRegister {
			n.markers[args.Job] = args.ActionID
		} else if args.Op == nodePoolActionClear {
			delete(n.markers, args.Job)
		}
		return map[string]any{"outcome": map[string]string{nodePoolActionRegister: nodePoolActionRegistered, nodePoolActionClear: nodePoolActionCleared, nodePoolActionFinish: nodePoolActionFinished}[args.Op]}
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
	base := newQuotaReplaceFixture(t, "worker", "seed")
	logs := &syncBuffer{}
	base.hub.logger = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	base.jobs.records["seed"]["pool"] = "devin"
	node := &sweepNode{conn: base.node, jobs: base.jobs, reasons: map[string]string{}, markers: map[string]string{}, panes: map[string]nodeAgentRow{
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
	job := "sweep-" + strconv.Itoa(n)
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
func (f *sweepFixture) sweeps() int { return len(f.logs.lines("handed_over=")) }

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
		{"window": "daily", "used_pct": 100, "resets_at": reset.Format(time.RFC3339), "scope": "model"},
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

// A delayed exhausted reading from one host, recorded after another host has
// published the account as available with a newer measurement, starts nothing.
func TestPoolExhaustedIgnoresAReadingAnotherHostSupersedes(t *testing.T) {
	f := newSweepFixture(t)
	worker := f.addJob(t, "worker", "devin", sweepDevin, "idle")
	now := time.Now()
	f.hub.stateStore().publish(FleetStateMessage{Host: "machine-b", Kind: fleetStateKindQuota, Key: sweepDevin, MeasuredAt: now.Add(time.Minute), ReceivedAt: now,
		Quota: &FleetStateQuota{AccountKey: sweepDevin, Pool: "devin", Free: &FleetStateFree{State: "available"}}}, 0)

	f.publishFreeLimit(t, sweepDevin, now.Add(3*time.Hour), now)
	eventually(t, 5*time.Second, "the superseded pass", func() bool { return len(f.logs.lines("pool exhausted sweep superseded")) == 1 })
	if got := f.state(worker); got != "spawned" || f.sweeps() != 0 {
		t.Fatalf("a superseded exhaustion acted: state=%s sweeps=%d", got, f.sweeps())
	}
}

// A screen-triggered replacement and the sweep end a job one at a time: while
// another operation holds the job, the sweep leaves it alone and acts once the
// guard is released.
func TestPoolExhaustedWaitsForTheJobGuard(t *testing.T) {
	f := newSweepFixture(t)
	reviewer := f.addJob(t, "reviewer", "devin", sweepDevin, "idle")
	release, ok := f.hub.jobGuard.acquire(t.Context(), reviewer)
	if !ok {
		t.Fatal("guard not acquired")
	}
	f.publishFreeLimit(t, sweepDevin, time.Now().Add(3*time.Hour), time.Now())
	time.Sleep(300 * time.Millisecond) // a window that proves nothing happens while the guard is held
	if got := f.state(reviewer); got != "spawned" || f.node2.count("pane.close "+reviewer) != 0 {
		t.Fatalf("the sweep acted under a held guard: state=%s", got)
	}
	release()
	eventually(t, 5*time.Second, "the reviewer to be ended once the guard is free", func() bool { return f.state(reviewer) == "lost" })
}

// An owner notice that cannot be made durable leaves the job's action pending
// after the record has ended (hk never rewrites a terminal record, so the record
// is not held back for the notice); the next drive, once the owner can be
// reached, delivers the one notice and finishes the action.
func TestPoolExhaustedRetriesAJobWhoseOwnerCannotBeNotified(t *testing.T) {
	f := newSweepFixture(t)
	f.owner = "nowhere" // no route in the lanes file yet
	reviewer := f.addJob(t, "reviewer", "devin", sweepDevin, "idle")
	reset := time.Now().Add(3 * time.Hour)

	f.publishFreeLimit(t, sweepDevin, reset, time.Now())
	eventually(t, 5*time.Second, "the pass", func() bool { return f.sweeps() == 1 })
	if got := f.state(reviewer); got != "lost" {
		t.Fatalf("state=%s, want the terminal record while the notice is owed", got)
	}
	if summary := f.logs.lines("handed_over=")[0]; !strings.Contains(summary, "owner_not_notified=1") {
		t.Fatalf("summary %q does not name the skip", summary)
	}
	for _, relay := range spawnFailedInjections(f.destination) {
		if relay.Kind == "job.lost" {
			t.Fatalf("an unroutable owner was sent %+v", relay)
		}
	}
	if got := len(f.pendingActions(t)); got != 1 {
		t.Fatalf("%d pending actions, want the one whose notice is owed", got)
	}

	// The owner becomes reachable; the pending action finishes it.
	routes := `{"lanes":{"parent":{"machine":"machine-b","pane":"w1:p1"},"nowhere":{"machine":"machine-b","pane":"w1:p2"}}}`
	if err := os.WriteFile(f.hub.reportRelayPath, []byte(routes), 0o600); err != nil {
		t.Fatal(err)
	}
	f.hub.reconcilePoolActions(t.Context())
	var lost []hubRelayInjectEvent
	for _, relay := range spawnFailedInjections(f.destination) {
		if relay.Kind == "job.lost" && relay.JobID == reviewer {
			lost = append(lost, relay)
		}
	}
	if len(lost) != 1 {
		t.Fatalf("%d owner notices, want exactly one after the retry", len(lost))
	}
	if got := len(f.pendingActions(t)); got != 0 {
		t.Fatalf("%d actions still pending, want the action done", got)
	}
	if got := f.node2.count("job.mark_lost " + reviewer); got != 1 {
		t.Fatalf("the record was written %d times, want once", got)
	}
}

// An ending found once is finished even after the job has left the heartbeat
// and its operation has left the live-settle window: the durable action, not
// the pane list, carries it. Without the action the next pass could never
// list the job again, and its owner would never be told.
func TestPoolExhaustedRetriesAnOldJobWhosePaneIsGone(t *testing.T) {
	f := newSweepFixture(t)
	f.owner = "nowhere"
	reviewer := f.addJob(t, "reviewer", "devin", sweepDevin, "idle")
	reset := time.Now().Add(3 * time.Hour).UTC().Truncate(time.Second)

	f.publishFreeLimit(t, sweepDevin, reset, time.Now())
	eventually(t, 5*time.Second, "the pass", func() bool { return f.sweeps() == 1 })
	if got := f.state(reviewer); got != "lost" {
		t.Fatalf("state=%s, want the terminal record", got)
	}
	// The pane is closed and the operation is older than the settle window, so
	// neither the heartbeat nor liveSpawnTasks would name the job again.
	f.ageJob(t, reviewer)
	f.hub.poolSweep = poolSweepState{}

	routes := `{"lanes":{"parent":{"machine":"machine-b","pane":"w1:p1"},"nowhere":{"machine":"machine-b","pane":"w1:p2"}}}`
	if err := os.WriteFile(f.hub.reportRelayPath, []byte(routes), 0o600); err != nil {
		t.Fatal(err)
	}
	if finished := f.hub.reconcilePoolActions(t.Context()); finished != 1 {
		t.Fatalf("reconciled %d actions, want the one left owed", finished)
	}
	var lost []hubRelayInjectEvent
	for _, relay := range spawnFailedInjections(f.destination) {
		if relay.Kind == "job.lost" && relay.JobID == reviewer {
			lost = append(lost, relay)
		}
	}
	if len(lost) != 1 {
		t.Fatalf("%d owner notices, want exactly one for the old job", len(lost))
	}
	if want := "quota_stop pool=devin reset_at=" + reset.Format(time.RFC3339); f.node2.reason(reviewer) != want {
		t.Fatalf("end_reason=%q, want %q", f.node2.reason(reviewer), want)
	}
	if got := len(f.pendingActions(t)); got != 0 {
		t.Fatalf("%d actions still pending", got)
	}
	if got := f.node2.count("pane.close " + reviewer); got != 1 {
		t.Fatalf("the pane was closed %d times, want once", got)
	}
}

// An intent whose exhaustion no longer holds before anything was touched is
// cancelled: a reset that passed during a wait must not close a working pane.
func TestPoolExhaustedCancelsAnIntentWhoseResetPassed(t *testing.T) {
	f := newSweepFixture(t)
	busy := f.addJob(t, "reviewer", "devin", sweepDevin, "working")
	reset := time.Now().Add(time.Hour)

	f.publishFreeLimit(t, sweepDevin, reset, time.Now())
	eventually(t, 5*time.Second, "the first pass", func() bool { return f.sweeps() == 1 })
	if got := len(f.pendingActions(t)); got != 1 {
		t.Fatalf("%d pending actions, want the busy pane's intent", got)
	}
	// The reset passes while the pane is still working.
	if _, err := f.hub.inbox.store.db.Exec(`UPDATE pool_exhausted_actions SET reset_ms=?`, time.Now().Add(-time.Minute).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	f.setJobPaneStatus(busy, "idle")
	if finished := f.hub.reconcilePoolActions(t.Context()); finished != 0 {
		t.Fatalf("an expired intent finished %d actions", finished)
	}
	if got := f.node2.count("pane.close " + busy); got != 0 {
		t.Fatal("a pane was closed after the reset passed")
	}
	if got := f.state(busy); got != "spawned" {
		t.Fatalf("state=%s, want the job left alone", got)
	}
	if got := len(f.pendingActions(t)); got != 0 {
		t.Fatalf("%d intents left pending, want the expired one cancelled", got)
	}
	if !strings.Contains(strings.Join(f.logs.lines("pool exhausted action cancelled"), "\n"), "reset_passed") {
		t.Fatalf("no reset_passed cancellation line:\n%s", f.logs.String())
	}
}

func TestPoolExhaustedRechecksResetAfterCapture(t *testing.T) {
	f := newSweepFixture(t)
	start := time.Now().UTC()
	var clock atomic.Int64
	clock.Store(start.UnixNano())
	f.hub.now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	f.node2.afterCapture = func() { clock.Store(start.Add(2 * time.Hour).UnixNano()) }
	reviewer := f.addJob(t, "reviewer", "devin", sweepDevin, "idle")
	f.publishFreeLimit(t, sweepDevin, start.Add(time.Hour), start)
	eventually(t, 5*time.Second, "the pass", func() bool { return f.sweeps() == 1 })
	if got := f.node2.count("pane.close " + reviewer); got != 0 {
		t.Fatalf("pane closed %d times after reset", got)
	}
	if got := f.state(reviewer); got != "spawned" {
		t.Fatalf("state=%s, want spawned", got)
	}
}

// A job that completed on its own is never told it was lost to a quota stop,
// even when the sweep listed it: the record's terminal state wins over the
// notice.
func TestPoolExhaustedLeavesACompletedJobAlone(t *testing.T) {
	f := newSweepFixture(t)
	reviewer := f.addJob(t, "reviewer", "devin", sweepDevin, "idle")
	f.jobs.mu.Lock()
	f.jobs.records[reviewer]["state"] = "completed"
	f.jobs.mu.Unlock()

	f.publishFreeLimit(t, sweepDevin, time.Now().Add(3*time.Hour), time.Now())
	eventually(t, 5*time.Second, "the pass", func() bool { return f.sweeps() == 1 })
	if got := f.node2.count("pane.close " + reviewer); got != 0 {
		t.Fatalf("the pane of a completed job was closed %d times", got)
	}
	for _, relay := range spawnFailedInjections(f.destination) {
		if relay.Kind == "job.lost" && relay.JobID == reviewer {
			t.Fatalf("a completed job was told it was lost: %+v", relay)
		}
	}
	if got := len(f.pendingActions(t)); got != 0 {
		t.Fatalf("%d actions pending, want the completion to cancel the action", got)
	}
	if got := f.state(reviewer); got != "completed" {
		t.Fatalf("state=%s, want the completion kept", got)
	}
}

// A hand-over interrupted between its terminal record and its successor's claim
// is resumed from the saved successor submission, without another publish and
// without generating a new request id; a second drive claims nothing more.
func TestPoolExhaustedResumesAHandOverAfterTheRecordEnded(t *testing.T) {
	f := newSweepFixture(t)
	worker := f.addJob(t, "worker", "devin", sweepDevin, "idle")
	holder, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), worker)
	if err != nil {
		t.Fatal(err)
	}
	prior, err := fleet.ValidateSubmission(holder.Request)
	if err != nil {
		t.Fatal(err)
	}
	successor, _, ok := quotaReplacementJob(worker)
	if !ok {
		t.Fatal("no successor id")
	}
	request, err := quotaReplacementRequest(holder.Request, worker, successor, "devin", prior.Role)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	endReason := fleet.QuotaStopEndReason("devin", time.Time{}, "handed_over_to", successor)
	f.jobs.mu.Lock()
	f.jobs.records[worker]["state"] = "lost"
	f.jobs.records[worker]["end_reason"] = endReason
	f.jobs.mu.Unlock()
	f.dropJobPane(worker)
	action := poolAction{
		Request: holder.Request.RequestID, Attempt: holder.Attempt, Job: worker, Owner: f.owner, Role: prior.Role,
		Kind: poolActionHandOver, Pool: "devin", Account: sweepDevin, Cause: poolCauseFreeLimit, Reset: time.Now().Add(time.Hour),
		EndReason: endReason, EventID: "pool-exhausted:" + worker, Successor: successor, SuccessorRequest: string(raw),
		Stage: poolActionTerminal, Created: time.Now().UTC(), Updated: time.Now().UTC(),
	}
	if err := f.hub.inbox.store.savePoolAction(t.Context(), action); err != nil {
		t.Fatal(err)
	}

	if finished := f.hub.reconcilePoolActions(t.Context()); finished != 1 {
		t.Fatalf("reconciled %d actions, want the claim it owed:\n%s", finished, f.logs.String())
	}
	claimed, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), successor)
	if err != nil {
		t.Fatalf("the successor was not claimed: %v", err)
	}
	if claimed.Request.RequestID != request.RequestID {
		t.Fatalf("successor request id=%s, want the saved %s", claimed.Request.RequestID, request.RequestID)
	}
	if finished := f.hub.reconcilePoolActions(t.Context()); finished != 0 {
		t.Fatalf("a second drive finished %d actions, want none left", finished)
	}
	if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), successor+"-q1"); err == nil {
		t.Fatal("a second successor was claimed")
	}
}

// A failed successor claim must retain the old role reservation for retry.
func TestPoolExhaustedKeepsRoleWhenSuccessorClaimFails(t *testing.T) {
	f := newSweepFixture(t)
	worker := f.addJob(t, "worker", "devin", sweepDevin, "idle")
	holder, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), worker)
	if err != nil {
		t.Fatal(err)
	}
	prior, err := fleet.ValidateSubmission(holder.Request)
	if err != nil {
		t.Fatal(err)
	}
	successor, _, _ := quotaReplacementJob(worker)
	request, err := quotaReplacementRequest(holder.Request, worker, successor, "devin", prior.Role)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	reason := fleet.QuotaStopEndReason("devin", time.Time{}, "handed_over_to", successor)
	f.jobs.mu.Lock()
	f.jobs.records[worker]["state"] = "lost"
	f.jobs.records[worker]["end_reason"] = reason
	f.jobs.mu.Unlock()
	f.dropJobPane(worker)
	now := time.Now().UTC()
	action := poolAction{Request: holder.Request.RequestID, Attempt: holder.Attempt, Job: worker, Owner: f.owner, Role: prior.Role, Kind: poolActionHandOver, Pool: "devin", Account: sweepDevin, Cause: poolCauseFreeLimit, Reset: now.Add(time.Hour), EndReason: reason, Successor: successor, SuccessorRequest: string(raw), Stage: poolActionTerminal, Created: now, Updated: now}
	if err := f.hub.inbox.store.savePoolAction(t.Context(), action); err != nil {
		t.Fatal(err)
	}
	if _, err := f.hub.inbox.store.db.Exec(`CREATE TRIGGER fail_successor BEFORE INSERT ON spawn_operations WHEN NEW.job LIKE '%-q1' BEGIN SELECT RAISE(FAIL, 'claim unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	f.hub.reconcilePoolActions(t.Context())
	owner, _, _, err := f.hub.inbox.store.spawnRoleOwner(t.Context(), prior.RoleKey())
	if err != nil || owner != holder.Request.RequestID {
		t.Fatalf("role owner=%q error=%v, want holder %q", owner, err, holder.Request.RequestID)
	}
}

func TestPoolExhaustedReconcileDoesNotStarveLaterActions(t *testing.T) {
	f := newSweepFixture(t)
	f.owner = "nowhere"
	reviewer := f.addJob(t, "reviewer", "devin", sweepDevin, "idle")
	f.publishFreeLimit(t, sweepDevin, time.Now().Add(time.Hour), time.Now())
	eventually(t, 5*time.Second, "the pass", func() bool { return f.sweeps() == 1 })
	old := time.Now().Add(-time.Hour)
	for i := 0; i < 257; i++ {
		action := poolAction{Request: "blocked-" + strconv.Itoa(i), Attempt: 1, Job: "blocked-" + strconv.Itoa(i), Stage: poolActionIntent, Reset: old, Created: old, Updated: old}
		if err := f.hub.inbox.store.savePoolAction(t.Context(), action); err != nil {
			t.Fatal(err)
		}
	}
	routes := `{"lanes":{"nowhere":{"machine":"machine-b","pane":"w1:p2"}}}`
	if err := os.WriteFile(f.hub.reportRelayPath, []byte(routes), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := f.hub.reconcilePoolActions(t.Context()); got != 1 {
		t.Fatalf("finished=%d, want the later action", got)
	}
	var notices int
	for _, relay := range spawnFailedInjections(f.destination) {
		if relay.Kind == "job.lost" && relay.JobID == reviewer {
			notices++
		}
	}
	if notices != 1 {
		t.Fatalf("notices=%d, want 1", notices)
	}
}

// pendingActions lists the actions that still owe a step.
func (f *sweepFixture) pendingActions(t *testing.T) []poolAction {
	t.Helper()
	actions, err := f.hub.inbox.store.pendingPoolActions(t.Context(), 100)
	if err != nil {
		t.Fatal(err)
	}
	return actions
}

// ageJob moves the job's operation out of the live-settle window and drops its
// pane, the state a job is in after an interrupted ending.
func (f *sweepFixture) ageJob(t *testing.T, job string) {
	t.Helper()
	old := time.Now().Add(-10 * time.Minute).UnixMilli()
	if _, err := f.hub.inbox.store.db.Exec(`UPDATE spawn_operations SET created_ms=?,last_run_ms=? WHERE job=?`, old, old, job); err != nil {
		t.Fatal(err)
	}
	f.dropJobPane(job)
}

// dropJobPane removes the job's pane from the node, as a close does.
func (f *sweepFixture) dropJobPane(job string) {
	f.node2.mu.Lock()
	defer f.node2.mu.Unlock()
	for pane, row := range f.node2.panes {
		if row.Name == job {
			delete(f.node2.panes, pane)
		}
	}
}

// setJobPaneStatus changes the status of the pane the job occupies.
func (f *sweepFixture) setJobPaneStatus(job, status string) {
	f.node2.mu.Lock()
	defer f.node2.mu.Unlock()
	for pane, row := range f.node2.panes {
		if row.Name == job {
			row.AgentStatus = status
			f.node2.panes[pane] = row
		}
	}
}

// A record that ended for another reason is not this action's ending: it is
// neither handed over again nor announced as a quota stop, and the action leaves
// nothing behind.
func TestPoolExhaustedLeavesAJobLostForAnotherReasonAlone(t *testing.T) {
	f := newSweepFixture(t)
	reviewer := f.addJob(t, "reviewer", "devin", sweepDevin, "idle")
	f.jobs.mu.Lock()
	f.jobs.records[reviewer]["state"] = "lost"
	f.jobs.records[reviewer]["end_reason"] = "reaped"
	f.jobs.mu.Unlock()

	f.publishFreeLimit(t, sweepDevin, time.Now().Add(3*time.Hour), time.Now())
	eventually(t, 5*time.Second, "the pass", func() bool { return f.sweeps() == 1 })
	if got := f.node2.count("pane.close " + reviewer); got != 0 {
		t.Fatalf("the pane of an already lost job was closed %d times", got)
	}
	for _, relay := range spawnFailedInjections(f.destination) {
		if relay.Kind == "job.lost" && relay.JobID == reviewer {
			t.Fatalf("an unrelated loss was announced as a quota stop: %+v", relay)
		}
	}
	if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), reviewer+"-q1"); err == nil {
		t.Fatal("an already lost job was handed over")
	}
	if got := len(f.pendingActions(t)); got != 0 {
		t.Fatalf("%d actions left behind for an unrelated loss", got)
	}
	if got := f.state(reviewer); got != "lost" {
		t.Fatalf("state=%s, want the original loss kept", got)
	}
}

// A failed terminal readback cannot authorize a notice or successor.
func TestPoolExhaustedWaitsForTerminalReadback(t *testing.T) {
	f := newSweepFixture(t)
	f.node2.failReadAfterMark = true
	reviewer := f.addJob(t, "reviewer", "devin", sweepDevin, "idle")
	f.publishFreeLimit(t, sweepDevin, time.Now().Add(time.Hour), time.Now())
	eventually(t, 5*time.Second, "the pass", func() bool { return f.sweeps() == 1 })
	for _, relay := range spawnFailedInjections(f.destination) {
		if relay.Kind == "job.lost" && relay.JobID == reviewer {
			t.Fatalf("notice before terminal readback: %+v", relay)
		}
	}
	if got := len(f.pendingActions(t)); got != 1 {
		t.Fatalf("pending actions=%d, want retry", got)
	}
}

// An unregistered intent must not claim a pane that disappeared independently.
func TestPoolExhaustedDoesNotClaimAnUnregisteredAbsentPane(t *testing.T) {
	f := newSweepFixture(t)
	reviewer := f.addJob(t, "reviewer", "devin", sweepDevin, "idle")
	holder, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), reviewer)
	if err != nil {
		t.Fatal(err)
	}
	f.dropJobPane(reviewer)
	now := time.Now().UTC()
	action := poolAction{Request: holder.Request.RequestID, Attempt: holder.Attempt, Job: reviewer, Owner: f.owner, Role: "reviewer", Kind: poolActionEnd, Pool: "devin", Account: sweepDevin, Cause: poolCauseFreeLimit, Reset: now.Add(-time.Hour), EndReason: "quota_stop pool=devin", EventID: "pool-exhausted:" + reviewer, Stage: poolActionIntent, Created: now, Updated: now}
	if err := f.hub.inbox.store.savePoolAction(t.Context(), action); err != nil {
		t.Fatal(err)
	}
	f.hub.reconcilePoolActions(t.Context())
	if got := f.state(reviewer); got != "spawned" {
		t.Fatalf("state=%s, want spawned for independently absent pane", got)
	}
}

// A failed closed-stage write must not allow the terminal record to advance.
func TestPoolExhaustedWaitsForClosedStagePersistence(t *testing.T) {
	f := newSweepFixture(t)
	reviewer := f.addJob(t, "reviewer", "devin", sweepDevin, "idle")
	_, err := f.hub.inbox.store.db.Exec(`CREATE TRIGGER fail_pool_closed BEFORE UPDATE ON pool_exhausted_actions WHEN NEW.stage = 'closed' BEGIN SELECT RAISE(FAIL, 'closed unavailable'); END`)
	if err != nil {
		t.Fatal(err)
	}
	f.publishFreeLimit(t, sweepDevin, time.Now().Add(time.Hour), time.Now())
	eventually(t, 5*time.Second, "the pass", func() bool { return f.sweeps() == 1 })
	if got := f.state(reviewer); got != "spawned" {
		t.Fatalf("state=%s, want spawned until closed stage persists", got)
	}
}

// An unrelated terminal write after closure cannot satisfy this action's quota ending.
func TestPoolExhaustedDoesNotAnnounceAnUnrelatedLossAfterClose(t *testing.T) {
	f := newSweepFixture(t)
	reviewer := f.addJob(t, "reviewer", "devin", sweepDevin, "idle")
	holder, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), reviewer)
	if err != nil {
		t.Fatal(err)
	}
	f.jobs.mu.Lock()
	f.jobs.records[reviewer]["state"] = "lost"
	f.jobs.records[reviewer]["end_reason"] = "reaped"
	f.jobs.mu.Unlock()
	f.dropJobPane(reviewer)
	now := time.Now().UTC()
	action := poolAction{Request: holder.Request.RequestID, Attempt: holder.Attempt, Job: reviewer, Owner: f.owner, Role: "reviewer", Kind: poolActionEnd, Pool: "devin", Account: sweepDevin, Cause: poolCauseFreeLimit, Reset: now.Add(time.Hour), EndReason: "quota_stop pool=devin", EventID: "pool-exhausted:" + reviewer, Stage: poolActionClosed, Created: now, Updated: now}
	if err := f.hub.inbox.store.savePoolAction(t.Context(), action); err != nil {
		t.Fatal(err)
	}
	f.hub.reconcilePoolActions(t.Context())
	for _, relay := range spawnFailedInjections(f.destination) {
		if relay.Kind == "job.lost" && relay.JobID == reviewer {
			t.Fatalf("unrelated loss announced as quota stop: %+v", relay)
		}
	}
}

// A close whose stage write was lost is not reconsidered against an expired
// exhaustion: the pane is gone, so the ending is finished (record and notice)
// even though the reset has passed.
func TestPoolExhaustedFinishesACloseWhoseStageWasLost(t *testing.T) {
	f := newSweepFixture(t)
	reviewer := f.addJob(t, "reviewer", "devin", sweepDevin, "idle")
	f.ageJob(t, reviewer)
	holder, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), reviewer)
	if err != nil {
		t.Fatal(err)
	}
	prior, err := fleet.ValidateSubmission(holder.Request)
	if err != nil {
		t.Fatal(err)
	}
	reset := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	action := poolAction{
		Request: holder.Request.RequestID, Attempt: holder.Attempt, Job: reviewer, Owner: f.owner, Role: prior.Role,
		Kind: poolActionEnd, Pool: "devin", Account: sweepDevin, Cause: poolCauseFreeLimit, Reset: reset,
		EndReason: fleet.QuotaStopEndReason("devin", reset, "", ""), EventID: "pool-exhausted:" + reviewer,
		Stage: poolActionClosing, Registered: time.Now().UTC(), Created: time.Now().UTC(), Updated: time.Now().UTC(),
	}
	if err := f.hub.inbox.store.savePoolAction(t.Context(), action); err != nil {
		t.Fatal(err)
	}
	if finished := f.hub.reconcilePoolActions(t.Context()); finished != 1 {
		t.Fatalf("reconciled %d actions, want the interrupted ending finished:\n%s", finished, f.logs.String())
	}
	if got := f.state(reviewer); got != "lost" {
		t.Fatalf("state=%s, want the terminal record", got)
	}
	if want := "quota_stop pool=devin reset_at=" + reset.Format(time.RFC3339); f.node2.reason(reviewer) != want {
		t.Fatalf("end_reason=%q, want %q", f.node2.reason(reviewer), want)
	}
	var lost int
	for _, relay := range spawnFailedInjections(f.destination) {
		if relay.Kind == "job.lost" && relay.JobID == reviewer {
			lost++
		}
	}
	if lost != 1 {
		t.Fatalf("%d owner notices, want one", lost)
	}
}

// An intent cancelled before it touched anything does not block a later
// exhaustion of the same live attempt.
func TestPoolExhaustedActsOnANewExhaustionAfterACancelledIntent(t *testing.T) {
	f := newSweepFixture(t)
	busy := f.addJob(t, "reviewer", "devin", sweepDevin, "working")
	reset := time.Now().Add(time.Hour)

	f.publishFreeLimit(t, sweepDevin, reset, time.Now())
	eventually(t, 5*time.Second, "the first pass", func() bool { return f.sweeps() == 1 })
	// The reset passes while the pane is still working; when it goes idle the
	// intent is cancelled before any effect, and the row goes with it.
	if _, err := f.hub.inbox.store.db.Exec(`UPDATE pool_exhausted_actions SET reset_ms=?`, time.Now().Add(-time.Minute).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	f.setJobPaneStatus(busy, "idle")
	if finished := f.hub.reconcilePoolActions(t.Context()); finished != 0 {
		t.Fatalf("an expired intent finished %d actions", finished)
	}
	if got := len(f.pendingActions(t)); got != 0 {
		t.Fatalf("%d cancelled intents left, want the row gone", got)
	}
	if got := f.node2.count("pane.close " + busy); got != 0 {
		t.Fatal("a pane was closed under an expired exhaustion")
	}

	// A new exhaustion of the same account decides again.
	f.publishFreeLimit(t, sweepDevin, time.Now().Add(2*time.Hour), time.Now().Add(time.Minute))
	eventually(t, 5*time.Second, "the job to be ended by the new exhaustion", func() bool { return f.state(busy) == "lost" })
	eventually(t, 5*time.Second, "the new action to finish", func() bool { return len(f.pendingActions(t)) == 0 })
}

// The store lookback bounds history, not liveness: a job started longer ago is
// still a candidate while a node reports it active.
func TestPoolExhaustedIncludesAnOldJobAHeartbeatReports(t *testing.T) {
	f := newSweepFixture(t)
	worker := f.addJob(t, "worker", "devin", sweepDevin, "idle")
	old := time.Now().Add(-8 * 24 * time.Hour).UnixMilli()
	if _, err := f.hub.inbox.store.db.Exec(`UPDATE spawn_operations SET created_ms=?,last_run_ms=? WHERE job=?`, old, old, worker); err != nil {
		t.Fatal(err)
	}
	f.hub.mu.Lock()
	if record := f.hub.nodes["machine-a"]; record != nil {
		record.activeJobs[worker] = HubActiveJob{JobID: worker, Epoch: 1}
	} else {
		f.hub.mu.Unlock()
		t.Fatal("the fixture node is not connected")
	}
	f.hub.mu.Unlock()

	f.publishFreeLimit(t, sweepDevin, time.Now().Add(3*time.Hour), time.Now())
	eventually(t, 5*time.Second, "the old live job to be handed over", func() bool { return f.state(worker) == "lost" })
	eventually(t, 5*time.Second, "the successor to be claimed", func() bool {
		_, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), worker+"-q1")
		return err == nil
	})
}

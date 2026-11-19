package panewire

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// readGateHub is a hub on a file store with a fake clock, whose handoffkeep
// points at the fake, so every coordinator read is live: the notice flush and
// the task-comment sweep return early without an hk client.
func readGateHub(t *testing.T, path string, now *time.Time, logs io.Writer) (*HubServer, *fakeHandoffkeep) {
	t.Helper()
	t.Setenv(crashReportDSNEnv, "")
	fake, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	if logs == nil {
		logs = io.Discard
	}
	h, err := NewHubServer(HubServerConfig{
		Tokens:          map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"},
		InboxStorePath:  path,
		ReportRelayPath: r20LanesFile(t, `{"lanes":{"parent":{"machine":"machine-b","pane":"w1:p1"}}}`),
		Logger:          slog.New(slog.NewTextHandler(logs, nil)),
		Now:             func() time.Time { return *now },
		handoffkeep:     client,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	return h, fake
}

// gateClaim claims a spawn for its own hk task, so claims in one test do not
// hold the same task role.
func gateClaim(t *testing.T, h *HubServer, seq int) hubSpawnTask {
	t.Helper()
	return taskFailClaimTask(t, h, seq, fmt.Sprint(9000+seq))
}

// readGateSteps runs one iteration of every gated loop, as the loops' own
// goroutines do each tick, and returns the requests the coordinator's
// scheduler was handed.
type readGateSteps struct {
	h      *HubServer
	ctx    context.Context
	handed []string
}

func newReadGateSteps(t *testing.T, h *HubServer) *readGateSteps {
	t.Helper()
	return &readGateSteps{h: h, ctx: t.Context()}
}

func (s *readGateSteps) all() {
	s.h.stepSpawnCoordinator(s.ctx, func(task hubSpawnTask) bool {
		s.handed = append(s.handed, task.Request.RequestID)
		return false // the row stays runnable, as a busy node leaves it
	})
	s.h.stepPlacementRecords(s.ctx)
	s.h.stepConsults(s.ctx)
}

func (s *readGateSteps) takeHanded() []string {
	out := s.handed
	s.handed = nil
	return out
}

// TestReadGateReasons guards the gate itself (hk 5302): a pass runs at start,
// on a noted write, at the due time, and at the backstop, and never otherwise.
func TestReadGateReasons(t *testing.T) {
	now := time.Now().UTC()
	var g readGate
	g.init("test")
	ticket, run := g.begin(now)
	if !run || ticket.why != readWhyStart {
		t.Fatalf("first pass = %v %q, want start", run, ticket.why)
	}
	g.end(ticket, now, readResult{due: now.Add(10 * time.Second).UnixMilli()}, nil)
	if _, run := g.begin(now.Add(9 * time.Second)); run {
		t.Fatal("a pass ran before its due time with nothing noted")
	}
	if ticket, run := g.begin(now.Add(10 * time.Second)); !run || ticket.why != readWhyDue {
		t.Fatalf("pass at the due time = %v %q, want due", run, ticket.why)
	}
	g.note()
	ticket, run = g.begin(now.Add(time.Second))
	if !run || ticket.why != readWhyKick {
		t.Fatalf("pass after a noted write = %v %q, want kick", run, ticket.why)
	}
	g.end(ticket, now, readResult{}, nil)
	if _, run := g.begin(now.Add(readBackstop - time.Second)); run {
		t.Fatal("an idle gate ran before the backstop")
	}
	if ticket, run := g.begin(now.Add(readBackstop)); !run || ticket.why != readWhyBackstop {
		t.Fatalf("pass at the backstop = %v %q, want backstop", run, ticket.why)
	}
	// A write that lands while a pass is reading is not lost: the pass keeps
	// the counter it began at.
	ticket, _ = g.begin(now.Add(readBackstop))
	g.note()
	g.end(ticket, now.Add(readBackstop), readResult{}, nil)
	if _, run := g.begin(now.Add(readBackstop)); !run {
		t.Fatal("a write noted during a pass was spent by it")
	}
	// A failed pass leaves the gate unprimed, so the next tick reads again.
	ticket, _ = g.begin(now.Add(readBackstop))
	g.end(ticket, now.Add(readBackstop), readResult{failed: true}, nil)
	if ticket, run := g.begin(now.Add(readBackstop)); !run || ticket.why != readWhyStart {
		t.Fatalf("pass after a failed one = %v %q, want start", run, ticket.why)
	}
}

// TestReadGatesIdle guards the acceptance of hk 5302: over a fake-clock minute
// with no writes, no loop sends a query to the store, and the backstop is what
// reads next. The hub's loops tick every 2 s (the coordinator, the probe) and
// 5 s (consults); the test drives each tick.
func TestReadGatesIdle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	now := time.Now().UTC()
	h, _ := readGateHub(t, path, &now, nil)
	steps := newReadGateSteps(t, h)
	steps.all() // every gate is unprimed: this pass reads once and rebuilds the due times
	calls := countStoreCalls(t, h, path)
	start := now
	for now.Sub(start) < readBackstop-2*time.Second {
		now = now.Add(2 * time.Second)
		steps.all()
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("idle loops sent %d store calls in %s, want 0", got, now.Sub(start))
	}
	// Control: the counting wrapper sees the backstop's read.
	now = start.Add(readBackstop)
	steps.all()
	if calls.Load() == 0 {
		t.Fatal("the backstop did not read the store")
	}
}

// TestReadGatesReachTheirReaderOnOneKick guards the write to read path of each
// loop: the write's own mark opens the gate, so the reader runs on the next
// step with the clock where it was, and no tick or timer is waited for.
func TestReadGatesReachTheirReaderOnOneKick(t *testing.T) {
	t.Run("a spawn submission reaches the coordinator", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "inbox.sqlite3")
		now := time.Now().UTC()
		h, _ := readGateHub(t, path, &now, nil)
		steps := newReadGateSteps(t, h)
		steps.all()
		task := gateClaim(t, h, 31)
		steps.all()
		if got := steps.takeHanded(); len(got) == 0 || got[0] != task.Request.RequestID {
			t.Fatalf("coordinator was handed %v, want %s", got, task.Request.RequestID)
		}
	})

	t.Run("a spawn notice reaches the flush", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "inbox.sqlite3")
		now := time.Now().UTC()
		h, fake := readGateHub(t, path, &now, nil)
		steps := newReadGateSteps(t, h)
		steps.all()
		task := gateClaim(t, h, 32)
		steps.all() // the claim's own kick; the row is handed and stays runnable
		outcome := hubSpawnOutcome{RC: 78, Stage: "queue", Reason: "task:42=missing"}
		if _, err := h.inbox.store.completeSpawn(t.Context(), task, outcome, now); err != nil {
			t.Fatal(err)
		}
		steps.all()
		var delivered int
		if err := h.inbox.store.db.QueryRow(`SELECT count(*) FROM spawn_notifications WHERE delivered=1`).Scan(&delivered); err != nil {
			t.Fatal(err)
		}
		if delivered == 0 {
			t.Fatal("the terminal write's notice was not routed on the next step")
		}
		// The same write owes its hk task comment, and the sweep posts it.
		if got := taskFailComments(t, fake); len(got) != 1 {
			t.Fatalf("task comments = %d, want 1", len(got))
		}
	})

	t.Run("a placement snapshot reaches the probe", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "inbox.sqlite3")
		now := time.Now().UTC()
		h, _ := readGateHub(t, path, &now, nil)
		steps := newReadGateSteps(t, h)
		steps.all()
		calls := countStoreCalls(t, h, path)
		steps.all()
		if calls.Load() != 0 {
			t.Fatal("the probe read the store while idle")
		}
		task := gateClaim(t, h, 33)
		if err := h.inbox.store.saveSpawnPlacementSnapshot(t.Context(), task.Request.RequestID, task.Attempt, []byte(`{}`), ""); err != nil {
			t.Fatal(err)
		}
		before := calls.Load()
		h.stepPlacementRecords(t.Context())
		if calls.Load() == before {
			t.Fatal("a snapshot written to the outbox did not open the probe")
		}
	})

	t.Run("a consult write reaches the consult loop", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "inbox.sqlite3")
		now := time.Now().UTC()
		h, _ := readGateHub(t, path, &now, nil)
		steps := newReadGateSteps(t, h)
		steps.all()
		// A queued consult past its time limit: the loop fails it on its read.
		c := hubConsult{ID: "consult-1", IdemKey: "key-1", Requester: "parent", State: consultQueued, RequestID: "missing", Created: now, Deadline: now.Add(-time.Second)}
		if _, fresh, err := h.inbox.store.insertConsult(t.Context(), c); err != nil || !fresh {
			t.Fatalf("insert = %v %v", fresh, err)
		}
		steps.all()
		got, err := h.inbox.store.consultByID(t.Context(), "consult-1")
		if err != nil || got.State != consultFailed {
			t.Fatalf("consult = %+v %v, want failed by the loop's read", got, err)
		}
	})
}

// TestSpawnScanDueTime guards the due-time rule: a parked operation is handed
// to the coordinator when its next_check_ms passes, with no write to announce
// it, and not a tick sooner.
func TestSpawnScanDueTime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	now := time.Now().UTC()
	h, _ := readGateHub(t, path, &now, nil)
	steps := newReadGateSteps(t, h)
	task := gateClaim(t, h, 41)
	steps.all()
	steps.takeHanded()
	parkedUntil := now.Add(45 * time.Second)
	if parked, err := h.inbox.store.parkSpawn(t.Context(), task.Request.RequestID, task.Attempt, spawnParkHostFull, nil, parkedUntil, now); err != nil || !parked {
		t.Fatalf("park = %v %v", parked, err)
	}
	// The next pass is the retry the last scan left; it finds the row parked
	// and records its next_check_ms as the due time.
	now = now.Add(spawnScanRetry)
	steps.all()
	if got := steps.takeHanded(); len(got) != 0 {
		t.Fatalf("a parked row was handed early: %v", got)
	}
	calls := countStoreCalls(t, h, path)
	for now.Add(2 * time.Second).Before(parkedUntil) {
		now = now.Add(2 * time.Second)
		steps.all()
	}
	if calls.Load() != 0 {
		t.Fatalf("the coordinator sent %d store calls while the row was parked", calls.Load())
	}
	now = parkedUntil
	steps.all()
	if got := steps.takeHanded(); len(got) != 1 || got[0] != task.Request.RequestID {
		t.Fatalf("at next_check_ms the coordinator was handed %v, want %s", got, task.Request.RequestID)
	}
}

// TestTaskFailSweepDueTime guards the sweep's due time: a comment hk refused is
// retried when its recorded delay ends, with no write and no query before.
func TestTaskFailSweepDueTime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	now := time.Now().UTC()
	h, fake := readGateHub(t, path, &now, nil)
	steps := newReadGateSteps(t, h)
	steps.all()
	fake.mu.Lock()
	fake.commentStatus = http.StatusServiceUnavailable
	fake.mu.Unlock()
	task := gateClaim(t, h, 42)
	if _, err := h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "spawn_failed: boom", Host: "machine-a"}, now); err != nil {
		t.Fatal(err)
	}
	steps.all() // hk refuses: the row is deferred by the base delay
	fake.mu.Lock()
	fake.commentStatus = 0
	fake.mu.Unlock()
	retryAt := now.Add(spawnTaskFailBackoffDefault)
	// The other loops' retry cadence (the notice read-again) ends within a few
	// ticks; after it only the sweep's own due time is left.
	for range 4 {
		now = now.Add(2 * time.Second)
		steps.all()
	}
	calls := countStoreCalls(t, h, path)
	for now.Add(2 * time.Second).Before(retryAt) {
		now = now.Add(2 * time.Second)
		steps.all()
	}
	if calls.Load() != 0 {
		t.Fatalf("loops sent %d store calls inside the row's retry delay", calls.Load())
	}
	if got := taskFailComments(t, fake); len(got) != 0 {
		t.Fatalf("comment posted %d times before its retry time", len(got))
	}
	now = retryAt
	steps.all()
	if got := taskFailComments(t, fake); len(got) != 1 {
		t.Fatalf("comments at the retry time = %d, want 1", len(got))
	}
}

// TestReadGatesRebuildAfterRestart guards the start of every loop: pending
// work the previous hub left in the store is found by the first pass, with no
// write to kick it and no fixed wait, and a parked row's due time is rebuilt
// from the queue.
func TestReadGatesRebuildAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	now := time.Now().UTC()
	first, _ := readGateHub(t, path, &now, nil)
	runnable := gateClaim(t, first, 51)
	parked := gateClaim(t, first, 52)
	parkedUntil := now.Add(30 * time.Second)
	if ok, err := first.inbox.store.parkSpawn(t.Context(), parked.Request.RequestID, parked.Attempt, spawnParkHostFull, nil, parkedUntil, now); err != nil || !ok {
		t.Fatalf("park = %v %v", ok, err)
	}
	failed := gateClaim(t, first, 53)
	if _, err := first.inbox.store.completeSpawn(t.Context(), failed, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "spawn_failed: boom", Host: "machine-a"}, now); err != nil {
		t.Fatal(err)
	}
	consult := hubConsult{ID: "consult-r", IdemKey: "key-r", Requester: "parent", State: consultQueued, RequestID: "missing", Created: now, Deadline: now.Add(-time.Second)}
	if _, _, err := first.inbox.store.insertConsult(t.Context(), consult); err != nil {
		t.Fatal(err)
	}
	first.Close()

	second, fake := readGateHub(t, path, &now, nil)
	steps := newReadGateSteps(t, second)
	steps.all() // the first pass of a new hub: no write was noted in this process
	handed := steps.takeHanded()
	if len(handed) != 1 || handed[0] != runnable.Request.RequestID {
		t.Fatalf("restarted coordinator was handed %v, want only %s", handed, runnable.Request.RequestID)
	}
	if got := taskFailComments(t, fake); len(got) != 1 {
		t.Fatalf("restarted sweep posted %d task comments, want the 1 the old hub owed", len(got))
	}
	if got, err := second.inbox.store.consultByID(t.Context(), "consult-r"); err != nil || got.State != consultFailed {
		t.Fatalf("restarted consult loop left %+v %v", got, err)
	}
	// The parked row comes due at its next_check_ms: the due time was rebuilt
	// from the queue, so no write is needed to see it.
	for now.Add(2 * time.Second).Before(parkedUntil) {
		now = now.Add(2 * time.Second)
		steps.all()
		steps.takeHanded()
	}
	now = parkedUntil
	steps.all()
	got := steps.takeHanded()
	if !strings.Contains(strings.Join(got, ","), parked.Request.RequestID) {
		t.Fatalf("at the parked row's next_check_ms the coordinator was handed %v, want %s", got, parked.Request.RequestID)
	}
}

// TestReadBackstopHealsAndNamesAMissingKick guards the safety tick: a row
// written without the kick every real writer gives is read at the backstop and
// no later, and the hub logs that the backstop found work nothing announced.
func TestReadBackstopHealsAndNamesAMissingKick(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	now := time.Now().UTC()
	var logs bytes.Buffer
	h, _ := readGateHub(t, path, &now, &logs)
	steps := newReadGateSteps(t, h)
	steps.all()
	// A notice written straight to the tables, as a writer that forgot to
	// call noteWork would.
	task := gateClaim(t, h, 61)
	steps.all()
	steps.takeHanded()
	raw := `{"rc":0,"stage":"landing"}`
	if _, err := h.inbox.store.db.Exec(`INSERT INTO spawn_notifications(event_id,request_id,job,owner,result,created_ms) VALUES(?,?,?,?,?,?)`, "orphan-event", task.Request.RequestID, task.Job, "parent", raw, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.inbox.store.db.Exec(`INSERT INTO spawn_notice_outbox(event_id) VALUES('orphan-event')`); err != nil {
		t.Fatal(err)
	}
	start := now
	for now.Sub(start) < readBackstop-2*time.Second {
		now = now.Add(2 * time.Second)
		steps.all()
	}
	if strings.Contains(logs.String(), "backstop found work") {
		t.Fatalf("the backstop was logged before it lapsed:\n%s", logs.String())
	}
	now = start.Add(readBackstop)
	steps.all()
	if !strings.Contains(logs.String(), "backstop found work that no kick announced") || !strings.Contains(logs.String(), "loop=spawn_notice") {
		t.Fatalf("the backstop's find was not logged:\n%s", logs.String())
	}
	var delivered int
	if err := h.inbox.store.db.QueryRow(`SELECT delivered FROM spawn_notifications WHERE event_id='orphan-event'`).Scan(&delivered); err != nil {
		t.Fatal(err)
	}
	if delivered != 1 {
		t.Fatalf("the backstop left the notice undelivered (delivered=%d)", delivered)
	}
}

// TestReadGateWritersNoteTheirLoops lists the store writes that give a loop
// work and the loop each one wakes. A writer that stops calling noteWork turns
// its row red here, naming the loop that would sleep through it.
func TestReadGateWritersNoteTheirLoops(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	now := time.Now().UTC()
	h, _ := readGateHub(t, path, &now, nil)
	store := h.inbox.store
	gens := func() [readKinds]uint64 {
		var out [readKinds]uint64
		for k := range out {
			out[k] = store.reads[k].gen.Load()
		}
		return out
	}
	expect := func(t *testing.T, name string, write func(), want ...readKind) {
		t.Helper()
		before := gens()
		write()
		after := gens()
		for _, k := range want {
			if after[k] == before[k] {
				t.Errorf("%s did not note %s", name, readKindNames[k])
			}
		}
	}
	task := gateClaim(t, h, 71)
	expect(t, "claimSpawn", func() { gateClaim(t, h, 72) }, readSpawnScan)
	expect(t, "completeSpawn (failed)", func() {
		if _, err := store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 1, Stage: "placement", Reason: "boom", Host: "machine-a"}, now); err != nil {
			t.Fatal(err)
		}
	}, readSpawnNotice, readSpawnTaskFail, readPlacementRecord)
	expect(t, "saveSpawnNotice", func() {
		other := gateClaim(t, h, 73)
		if err := store.saveSpawnNotice(t.Context(), fmt.Sprintf("%s:1:note", other.Request.RequestID), other, `{}`, now); err != nil {
			t.Fatal(err)
		}
	}, readSpawnNotice)
	expect(t, "saveSpawnPlacementSnapshot", func() {
		other := gateClaim(t, h, 74)
		if err := store.saveSpawnPlacementSnapshot(t.Context(), other.Request.RequestID, other.Attempt, []byte(`{}`), ""); err != nil {
			t.Fatal(err)
		}
	}, readPlacementRecord)
	expect(t, "insertConsult", func() {
		if _, _, err := store.insertConsult(t.Context(), hubConsult{ID: "c-w", IdemKey: "k-w", Requester: "parent", State: consultQueued, Created: now, Deadline: now.Add(time.Hour)}); err != nil {
			t.Fatal(err)
		}
	}, readConsult)
	expect(t, "saveConsult", func() {
		if err := store.saveConsult(t.Context(), hubConsult{ID: "c-w", IdemKey: "k-w", Requester: "parent", State: consultRunning, Created: now, Deadline: now.Add(time.Hour)}); err != nil {
			t.Fatal(err)
		}
	}, readConsult)
	expect(t, "wakeParkedSpawns", func() {
		parked := gateClaim(t, h, 75)
		if ok, err := store.parkSpawn(t.Context(), parked.Request.RequestID, parked.Attempt, spawnParkHostFull, nil, now.Add(time.Hour), now); err != nil || !ok {
			t.Fatalf("park = %v %v", ok, err)
		}
		if n, err := store.wakeParkedSpawns(t.Context(), []string{spawnParkHostFull}, 8, now); err != nil || n == 0 {
			t.Fatalf("wake = %d %v", n, err)
		}
	}, readSpawnScan)
}

// TestSpawnScanStaleProbeDueTime guards the stale probe's due time: a request
// past its deadline is replayed on a back-off, so the coordinator reads again
// when that back-off ends, and sends nothing before.
func TestSpawnScanStaleProbeDueTime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	now := time.Now().UTC()
	h, _ := readGateHub(t, path, &now, nil)
	steps := newReadGateSteps(t, h)
	task := gateClaim(t, h, 81)
	node := hubSpawnRequest{RequestID: t14RequestID(51981), Machine: "machine-a", Args: task.Request.Args, WaitSeconds: 600}
	bound, err := h.inbox.store.bindSpawn(t.Context(), task.Request.RequestID, task.Attempt, node, now)
	if err != nil {
		t.Fatal(err)
	}
	bound = sentSpawnFixture(t, h.inbox.store, bound)
	if _, err := h.inbox.store.completeSpawn(t.Context(), bound, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Host: "machine-a"}, now); err != nil {
		t.Fatal(err)
	}
	if saved, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID); err != nil || saved.Status != "deadline_exceeded" {
		t.Fatalf("row = %+v %v, want deadline_exceeded", saved, err)
	}
	steps.all()
	if got := steps.takeHanded(); len(got) != 1 {
		t.Fatalf("first probe handed %v, want the stale row once", got)
	}
	// The worker's probe records its run; the next one is due a back-off later.
	h.inbox.store.touchSpawn(t.Context(), task.Request.RequestID, now)
	probeAt := now.Add(spawnStaleProbeMin)
	// One retry pass finds the row not yet due and records the probe time.
	now = now.Add(spawnScanRetry)
	steps.all()
	steps.takeHanded()
	calls := countStoreCalls(t, h, path)
	for now.Add(2 * time.Second).Before(probeAt) {
		now = now.Add(2 * time.Second)
		steps.all()
	}
	if calls.Load() != 0 {
		t.Fatalf("the coordinator sent %d store calls inside the probe back-off", calls.Load())
	}
	now = probeAt
	steps.all()
	if got := steps.takeHanded(); len(got) != 1 || got[0] != task.Request.RequestID {
		t.Fatalf("at the back-off's end the coordinator was handed %v, want %s", got, task.Request.RequestID)
	}
}

// TestRetryIsCountedFromThePassStart guards the retry cadence: the loops wake
// on tickers anchored before their pass, so a retry counted from the pass's end
// would skip the next tick and run at twice its period.
func TestRetryIsCountedFromThePassStart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	now := time.Now().UTC()
	h, _ := readGateHub(t, path, &now, nil)
	start := now
	ran := 0
	pass := func() readResult {
		ran++
		now = now.Add(50 * time.Millisecond) // the pass itself takes time
		return readResult{found: true, retry: 2 * time.Second}
	}
	h.gatedRead(readConsult, "", pass)
	now = start.Add(2 * time.Second) // the next tick of a 2 s ticker
	h.gatedRead(readConsult, "", pass)
	if ran != 2 {
		t.Fatalf("the retry ran %d times by the next 2 s tick, want 2", ran)
	}
	now = start.Add(2*time.Second + time.Second)
	h.gatedRead(readConsult, "", func() readResult { ran++; return readResult{} })
	if ran != 2 {
		t.Fatal("a retry ran a second early")
	}
}

// TestPlacementProbeRetriesAFailedRead guards the probe against a store error
// on its outbox read: it is a failed pass, not an empty one, so the next tick
// reads again instead of waiting for the backstop.
func TestPlacementProbeRetriesAFailedRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	now := time.Now().UTC()
	h, _ := readGateHub(t, path, &now, nil)
	steps := newReadGateSteps(t, h)
	steps.all()
	store := h.inbox.store
	if _, err := store.db.Exec(`ALTER TABLE placement_record_outbox RENAME TO placement_record_outbox_away`); err != nil {
		t.Fatal(err)
	}
	store.noteWork(readPlacementRecord) // a committed snapshot kicks the probe
	h.stepPlacementRecords(t.Context()) // the read fails
	if _, err := store.db.Exec(`ALTER TABLE placement_record_outbox_away RENAME TO placement_record_outbox`); err != nil {
		t.Fatal(err)
	}
	calls := countStoreCalls(t, h, path)
	h.stepPlacementRecords(t.Context()) // same clock, no new write
	if calls.Load() == 0 {
		t.Fatal("a failed outbox read was recorded as an empty pass")
	}
}

// TestHeldNoticePageIsReadAgain guards the held-notice page: a full page that
// is all superseded leaves later rows unexamined, so the flush reads again at
// its cadence instead of waiting for a kick.
func TestHeldNoticePageIsReadAgain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	now := time.Now().UTC()
	h, _ := readGateHub(t, path, &now, nil)
	steps := newReadGateSteps(t, h)
	steps.all()
	store := h.inbox.store
	total := spawnNoticePage + 1
	for i := range total {
		task := gateClaim(t, h, 100+i)
		id := fmt.Sprintf("%s:1:retryable", task.Request.RequestID)
		// The operation is accepted, not retryable: the held notice is superseded.
		if _, err := store.db.Exec(`INSERT INTO spawn_notifications(event_id,request_id,job,owner,result,created_ms,delivered) VALUES(?,?,?,?,?,?,2)`, id, task.Request.RequestID, task.Job, "parent", `{"rc":1}`, now.UnixMilli()+int64(i)); err != nil {
			t.Fatal(err)
		}
		if _, err := store.db.Exec(`INSERT INTO spawn_notice_outbox(event_id) VALUES(?)`, id); err != nil {
			t.Fatal(err)
		}
	}
	store.noteWork(readSpawnNotice)
	steps.all()
	open := func() int {
		var n int
		if err := store.db.QueryRow(`SELECT count(*) FROM spawn_notice_outbox`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if got := open(); got != 1 {
		t.Fatalf("outbox rows after the first page = %d, want the 1 past the page", got)
	}
	now = now.Add(spawnNoticeRetry)
	steps.all()
	if got := open(); got != 0 {
		t.Fatalf("outbox rows after the retry = %d, want 0: the row past the page was never examined", got)
	}
}

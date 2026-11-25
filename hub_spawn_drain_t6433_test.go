package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 6433: the drain for a spawn without a checkout is read once per scan from
// the bound_machine and exclusive columns. The hub tests run the real
// coordinator against a connected node and read the frames it is sent.

// drainRow is one request bound to a machine. An empty dir binds it without a
// checkout: an exclusive.
type drainRow struct {
	n       int // the request id's number, so n orders request ids
	machine string
	dir     string
	profile string // "sonnet" when empty
	grant   bool
	// created is created_ms; lastRun is last_run_ms, zero for never run.
	created, lastRun time.Time
}

// bindDrainRow claims and binds row at now, then writes its queue position.
func bindDrainRow(t *testing.T, s *laneInboxStore, row drainRow, now time.Time) hubSpawnTask {
	t.Helper()
	if row.machine == "" {
		row.machine = "machine-a"
	}
	if row.profile == "" {
		row.profile = "sonnet"
	}
	job := fmt.Sprintf("drain-%d", row.n)
	request := durableSpawnRequest()
	request.RequestID = t14RequestID(643300 + row.n)
	request.Args = replaceSpawnArg(replaceSpawnArg(replaceSpawnArg(request.Args, "--job", job), "-l", job), "--task", fmt.Sprint(643300+row.n))
	task, _, err := s.claimSpawn(t.Context(), request, job, "parent", now)
	if err != nil {
		t.Fatal(err)
	}
	node := boundaryTestNode(task, 653300+row.n, row.machine)
	node.CWDKey = "repo-a"
	node.Args = []string{"--job", job, "-l", job, "-m", row.profile}
	node.GrantRequired = row.grant
	if row.dir != "" {
		node.Checkout = t3482Checkout()
		node.Checkout.Dir = row.dir
	}
	if _, err := s.bindSpawn(t.Context(), request.RequestID, task.Attempt, node, now.UTC()); err != nil {
		t.Fatal(err)
	}
	lastRun := int64(0)
	if !row.lastRun.IsZero() {
		lastRun = row.lastRun.UnixMilli()
	}
	if !row.created.IsZero() {
		if _, err := s.db.Exec(s.q(`UPDATE spawn_operations SET created_ms=?,last_run_ms=? WHERE request_id=?`), row.created.UnixMilli(), lastRun, request.RequestID); err != nil {
			t.Fatal(err)
		}
	}
	bound, err := s.spawnTask(t.Context(), request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	return bound
}

// drainNode is a connected node and the node request ids of the spawn frames it
// has been sent, in order.
type drainNode struct{ frames chan string }

// connectDrainNode connects machine-a with the extra capabilities and starts
// reading its frames.
func connectDrainNode(t *testing.T, h *HubServer, capabilities ...string) drainNode {
	t.Helper()
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	connection := t14Node(t, h, server, true)
	h.mu.Lock()
	for _, capability := range capabilities {
		h.nodes["machine-a"].capabilities[capability] = true
	}
	h.mu.Unlock()
	node := drainNode{frames: make(chan string, 128)}
	go readDrainFrames(t.Context(), connection, node.frames)
	return node
}

func readDrainFrames(ctx context.Context, connection *websocket.Conn, frames chan<- string) {
	for {
		_, payload, err := connection.Read(ctx)
		if err != nil {
			return
		}
		if message, valid := parseHubOutbound(payload); valid && message.Type == "job.spawn" {
			frames <- message.RequestID
		}
	}
}

// expect fails unless the node's next spawn frame is task's.
func (n drainNode) expect(t *testing.T, task hubSpawnTask, label string) {
	t.Helper()
	select {
	case got := <-n.frames:
		if got != task.Node.RequestID {
			t.Fatalf("%s: node was sent %s, want %s (%s)", label, got, task.Node.RequestID, task.Job)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: node was sent no spawn, want %s (%s)", label, task.Node.RequestID, task.Job)
	}
}

// expectNone runs several scans and fails if any of them sends a frame.
func (n drainNode) expectNone(t *testing.T, h *HubServer, label string) {
	t.Helper()
	for range 4 {
		kickSpawnCoordinator(h)
		// A window that proves no further frame is sent.
		select {
		case got := <-n.frames:
			t.Fatalf("%s: node was sent %s", label, got)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// The exclusive pages after more than a page of newer checkouts. They wait all
// the same: the first frame the node gets is the exclusive's.
func TestDrainHoldsAcrossPages_t6367(t *testing.T) {
	h := asyncSpawnHub(t)
	node := connectDrainNode(t, h)
	base := time.Now()
	const checkouts = 40
	for i := range checkouts {
		bindDrainRow(t, h.inbox.store, drainRow{n: 100 + i, dir: fmt.Sprintf("dir-%d", i), created: base.Add(time.Duration(i) * time.Millisecond)}, base)
	}
	exclusive := bindDrainRow(t, h.inbox.store, drainRow{n: 1, created: base.Add(-time.Hour), lastRun: base}, base)
	first, _, err := h.inbox.store.pendingSpawnPage(t.Context(), spawnScanPosition{}, base)
	if err != nil || len(first) != 32 || slices.ContainsFunc(first, func(task hubSpawnTask) bool { return task.Request.RequestID == exclusive.Request.RequestID }) {
		t.Fatalf("the exclusive must be past the first page: %d rows, %v", len(first), err)
	}
	h.startSpawnCoordinator(t.Context())
	node.expect(t, exclusive, "first dispatch")
	node.expectNone(t, h, "while the exclusive is open")
}

// A drain mark lasts one scan. Once the hub's clock passes the exclusive's
// deadline, the next scan sends the checkout that waited, though the wall clock
// has barely moved.
func TestDrainEndsOnExpiryWithFrozenClock_t6367(t *testing.T) {
	t.Setenv(crashReportDSNEnv, "")
	base := time.Now().UTC().Truncate(time.Millisecond)
	var offset atomic.Int64
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3"),
		Now: func() time.Time { return base.Add(time.Duration(offset.Load())) }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	// Two slots: only the drain keeps the newer checkout behind the open one.
	node := connectDrainNode(t, h, hubCapabilitySpawnSlotsPrefix+"2")
	open := bindDrainRow(t, h.inbox.store, drainRow{n: 1, dir: "older", created: base.Add(-3 * time.Second)}, base)
	exclusive := bindDrainRow(t, h.inbox.store, drainRow{n: 2, created: base.Add(-2 * time.Second)}, base)
	waiting := bindDrainRow(t, h.inbox.store, drainRow{n: 3, dir: "newer", created: base.Add(-time.Second)}, base)
	// The exclusive reaches its attempt's deadline and its request's limit in
	// five seconds, so it ends there and is not placed again.
	if _, err := h.inbox.store.db.Exec(`UPDATE spawn_operations SET deadline_ms=?,expires_ms=? WHERE request_id=?`, base.Add(5*time.Second).UnixMilli(), base.Add(5*time.Second).UnixMilli(), exclusive.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	// The deadline is read on the hub's clock, not the store's or the wall's.
	for _, at := range []struct {
		offset time.Duration
		held   bool
	}{{6 * time.Second, false}, {0, true}} {
		offset.Store(int64(at.offset))
		marks, err := h.readSpawnDrainMarks(t.Context())
		if err != nil || marks.holds(waiting) != at.held {
			t.Fatalf("at +%v: held=%v err=%v, want %v", at.offset, marks.holds(waiting), err, at.held)
		}
	}
	h.startSpawnCoordinator(t.Context())
	node.expect(t, open, "older checkout")
	node.expectNone(t, h, "while the exclusive waits")
	offset.Store(int64(6 * time.Second))
	kickSpawnCoordinator(h)
	node.expect(t, waiting, "after the exclusive's deadline")
}

// Equal created_ms: the request id decides which of the two goes first.
func TestDrainTieBreaksByRequestID_t6367(t *testing.T) {
	for _, test := range []struct {
		name          string
		checkout      int
		checkoutFirst bool
	}{
		{"a later request id waits", 30, false},
		{"an earlier request id goes first", 10, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := asyncSpawnHub(t)
			node := connectDrainNode(t, h)
			base := time.Now()
			created := base.Add(-time.Minute)
			// The checkout pages first, so only the drain can hold it.
			checkout := bindDrainRow(t, h.inbox.store, drainRow{n: test.checkout, dir: "tie", created: created}, base)
			exclusive := bindDrainRow(t, h.inbox.store, drainRow{n: 20, created: created, lastRun: base}, base)
			h.startSpawnCoordinator(t.Context())
			want := exclusive
			if test.checkoutFirst {
				want = checkout
			}
			node.expect(t, want, "first dispatch")
		})
	}
}

// A grant-required exclusive is stored as bound_grant and holds checkouts back
// like any other.
func TestDrainReadsGrantStatus_t6367(t *testing.T) {
	h := asyncSpawnHub(t)
	node := connectDrainNode(t, h, hubCapabilitySpawnGrant)
	base := time.Now()
	bindDrainRow(t, h.inbox.store, drainRow{n: 2, dir: "newer", created: base.Add(-time.Second)}, base)
	exclusive := bindDrainRow(t, h.inbox.store, drainRow{n: 1, grant: true, created: base.Add(-time.Minute), lastRun: base}, base)
	var stored string
	if err := h.inbox.store.db.QueryRow(`SELECT status FROM spawn_operations WHERE request_id=?`, exclusive.Request.RequestID).Scan(&stored); err != nil || stored != "bound_grant" {
		t.Fatalf("fixture status %q %v, want bound_grant", stored, err)
	}
	h.startSpawnCoordinator(t.Context())
	node.expect(t, exclusive, "first dispatch")
}

// An exclusive holds back only what queued after it.
func TestNewerExclusiveDoesNotBlockOlderCheckout_t6367(t *testing.T) {
	h := asyncSpawnHub(t)
	node := connectDrainNode(t, h)
	base := time.Now()
	older := bindDrainRow(t, h.inbox.store, drainRow{n: 1, dir: "older", created: base.Add(-time.Minute)}, base)
	bindDrainRow(t, h.inbox.store, drainRow{n: 2, created: base.Add(-time.Second), lastRun: base}, base)
	h.startSpawnCoordinator(t.Context())
	node.expect(t, older, "first dispatch")
}

// An exclusive this scan will unbind marks nothing, so a newer checkout on its
// machine is not held back by it in the same scan.
func TestExclusiveWaitingOnBindingDoesNotStarveMachine_t6404(t *testing.T) {
	for _, test := range []struct {
		name string
		// stop makes the exclusive undispatchable.
		stop func(h *HubServer)
		held bool
	}{
		{"dispatchable", func(*HubServer) {}, true},
		{"readiness drop", func(h *HubServer) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.launchBackoffs = map[launchKey]*launchBackoff{{"machine-a", "codex"}: {until: time.Now().Add(time.Hour)}}
		}, false},
		{"node not accepting", func(h *HubServer) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.relayControl.acceptingOverride = map[string]acceptingOverride{"machine-a": {Mode: "off"}}
		}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := asyncSpawnHub(t)
			connectDrainNode(t, h)
			base := time.Now()
			// The oldest exclusive needs codex; a newer one on the same machine
			// does not stand in for it.
			exclusive := bindDrainRow(t, h.inbox.store, drainRow{n: 1, profile: "codex-sol", created: base.Add(-time.Minute)}, base)
			checkout := bindDrainRow(t, h.inbox.store, drainRow{n: 2, dir: "newer", created: base.Add(-time.Second)}, base)
			if kind := fleet.ProfileKind("codex-sol"); kind != "codex" {
				t.Fatalf("fixture profile kind %q, want codex", kind)
			}
			test.stop(h)
			if got := h.boundSpawnDispatchable(exclusive); got != test.held {
				t.Fatalf("fixture exclusive dispatchable=%v, want %v", got, test.held)
			}
			offered := false
			// One scan, with nothing dispatched or unbound under it.
			h.spawnCoordinatorScan(t.Context(), func(task hubSpawnTask, drain spawnDrainMarks) bool {
				if task.Request.RequestID != checkout.Request.RequestID {
					return false
				}
				offered = true
				if got := drain.holds(task); got != test.held {
					t.Errorf("checkout held=%v, want %v", got, test.held)
				}
				return false
			})
			if !offered {
				t.Fatal("the scan never offered the checkout")
			}
		})
	}
}

// The postgres and mysql CI jobs select store tests by name (ci.yml), so the
// dialect cases also run under one they match.
func TestLaneInboxStoreSpawnDrain_t6433(t *testing.T) {
	t.Run("bind", TestBindWritesBoundMachineAndExclusive_t6404)
	t.Run("oldest per machine", TestBoundExclusiveSpawnsReturnsOldestPerMachine_t6433)
	t.Run("backfill", TestBoundMachineBackfillBoundedBatches_t6404)
	t.Run("repair blank", TestSpawnBoundRepairFillsBlankInFlightRow_t6433)
	t.Run("repair stale", TestSpawnBoundRepairCorrectsStaleInFlightRow_t6433)
	t.Run("repair leaves finished", TestSpawnBoundRepairLeavesTerminalRow_t6433)
	t.Run("repair second start", TestSpawnBoundRepairSecondStartWritesNothing_t6433)
}

// drainColumns reads the two columns of one operation.
func drainColumns(t *testing.T, s *laneInboxStore, requestID string) string {
	t.Helper()
	var machine string
	var exclusive int
	if err := s.db.QueryRow(s.q(`SELECT bound_machine,exclusive FROM spawn_operations WHERE request_id=?`), requestID).Scan(&machine, &exclusive); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%q,%d", machine, exclusive)
}

// The bind writes both columns, and each of the four statements that unbind
// clears them.
func TestBindWritesBoundMachineAndExclusive_t6404(t *testing.T) {
	const marked, checkoutMarked, cleared = `"machine-a",1`, `"machine-a",0`, `"",0`
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := queueTestStore(t, open)
		now := time.Now().UTC()
		want := func(task hubSpawnTask, label, columns string) {
			t.Helper()
			if got := drainColumns(t, s, task.Request.RequestID); got != columns {
				t.Errorf("%s: bound_machine,exclusive = %s, want %s", label, got, columns)
			}
		}
		unbound := func(task hubSpawnTask, label string) {
			t.Helper()
			current, err := s.spawnTask(t.Context(), task.Request.RequestID)
			if err != nil || current.Node != nil {
				t.Fatalf("%s: still bound: %+v %v", label, current.Node, err)
			}
			want(task, label, cleared)
		}

		checkout := bindDrainRow(t, s, drainRow{n: 1, dir: "checkout"}, now)
		want(checkout, "checkout bind", checkoutMarked)

		unsent := bindDrainRow(t, s, drainRow{n: 2}, now)
		want(unsent, "exclusive bind", marked)
		if _, err := s.unbindUnsentSpawn(t.Context(), unsent.Request.RequestID, unsent.Attempt, unsent.Node.RequestID); err != nil {
			t.Fatal(err)
		}
		unbound(unsent, "unbind of an unsent binding")

		refused := sentSpawnFixture(t, s, bindDrainRow(t, s, drainRow{n: 3}, now))
		want(refused, "sent exclusive", marked)
		if _, err := s.unbindRefusedSpawn(t.Context(), refused, hubSpawnExclusion{Machine: "machine-a", Reason: "cwd_unmapped", RC: 2}, nil, now); err != nil {
			t.Fatal(err)
		}
		unbound(refused, "unbind of a refusal")

		starved := bindDrainRow(t, s, drainRow{n: 4, grant: true}, now)
		want(starved, "grant exclusive bind", marked)
		if changed, err := s.completeSpawn(t.Context(), starved, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Retryable: true}, now); err != nil || !changed {
			t.Fatalf("unstarted timeout: %v %v", changed, err)
		}
		unbound(starved, "unstarted attempt's end")

		failed := sentSpawnFixture(t, s, bindDrainRow(t, s, drainRow{n: 5}, now))
		if changed, err := s.completeSpawn(t.Context(), failed, hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed", Host: "machine-a", Retryable: true}, now); err != nil || !changed {
			t.Fatalf("retryable outcome: %v %v", changed, err)
		}
		finished, err := s.spawnTask(t.Context(), failed.Request.RequestID)
		if err != nil || finished.Status != "retryable" {
			t.Fatalf("fixture status %q %v, want retryable", finished.Status, err)
		}
		want(finished, "finished row keeps its values", marked)
		if _, err := s.resumeCleanedSpawn(t.Context(), finished, now); err != nil {
			t.Fatal(err)
		}
		unbound(failed, "resume of a cleaned attempt")
	})
}

// The prepass returns one row per machine: its oldest unsent exclusive that is
// still inside its deadline, ties broken by request id.
func TestBoundExclusiveSpawnsReturnsOldestPerMachine_t6433(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := queueTestStore(t, open)
		now := time.Now().UTC()
		at := func(ago time.Duration) time.Time { return now.Add(-ago) }
		// Older than every answer, and none of them one: a checkout, a sent
		// exclusive and an expired one.
		bindDrainRow(t, s, drainRow{n: 1, dir: "checkout", created: at(time.Hour)}, now)
		sentSpawnFixture(t, s, bindDrainRow(t, s, drainRow{n: 2, created: at(time.Hour)}, now))
		expired := bindDrainRow(t, s, drainRow{n: 3, created: at(time.Hour)}, now)
		if _, err := s.db.Exec(s.q(`UPDATE spawn_operations SET deadline_ms=? WHERE request_id=?`), now.UnixMilli(), expired.Request.RequestID); err != nil {
			t.Fatal(err)
		}
		bindDrainRow(t, s, drainRow{n: 4, created: at(time.Second)}, now)
		second := bindDrainRow(t, s, drainRow{n: 6, created: at(time.Minute)}, now)
		first := bindDrainRow(t, s, drainRow{n: 5, created: at(time.Minute)}, now)
		other := bindDrainRow(t, s, drainRow{n: 7, machine: "machine-b", grant: true, created: at(time.Second)}, now)
		got, err := s.boundExclusiveSpawns(t.Context(), now)
		if err != nil {
			t.Fatal(err)
		}
		byMachine := map[string]boundExclusiveSpawn{}
		for _, row := range got {
			byMachine[row.machine] = row
		}
		if len(got) != 2 || byMachine["machine-a"].requestID != first.Request.RequestID || byMachine["machine-b"].requestID != other.Request.RequestID {
			t.Fatalf("got %+v, want %s (not %s) and %s", got, first.Request.RequestID, second.Request.RequestID, other.Request.RequestID)
		}
		if row := byMachine["machine-a"]; row.createdMS != first.Created.UnixMilli() {
			t.Fatalf("created_ms %d, want %d", row.createdMS, first.Created.UnixMilli())
		}
	})
}

// recordSpawnBoundBackfill is the source hub's start for a store-copy test.
func recordSpawnBoundBackfill(t *testing.T, s *laneInboxStore) {
	t.Helper()
	if err := s.backfillSpawnBoundMachines(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// forgetSpawnBoundBackfill makes the store look as it did before the backfill
// ran, so the next start runs it.
func forgetSpawnBoundBackfill(t *testing.T, s *laneInboxStore) {
	t.Helper()
	statement := `DELETE FROM panewire_schema_migrations WHERE version='` + spawnBoundBackfillVersion + `'`
	if s.dialect.ownsSchema() {
		statement = `PRAGMA user_version=1`
	}
	if _, err := s.db.Exec(statement); err != nil {
		t.Fatal(err)
	}
}

// seedPreColumnRows writes operations as a hub without the columns left them.
func seedPreColumnRows(t *testing.T, s *laneInboxStore, rows []preColumnRow) {
	t.Helper()
	tx, err := s.beginTx(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, row := range rows {
		node := hubSpawnRequest{RequestID: "node-" + row.id, Machine: row.machine, Args: []string{"--job", "job-" + row.id}}
		if row.dir != "" {
			node.Checkout = t3482Checkout()
			node.Checkout.Dir = row.dir
		}
		raw, err := json.Marshal(node)
		if err != nil {
			t.Fatal(err)
		}
		var nodeID any = node.RequestID
		payload := string(raw)
		if row.status == "accepted" {
			nodeID, payload = nil, ""
		}
		if _, err := tx.ExecContext(t.Context(), s.q(`INSERT INTO spawn_operations(request_id,job,owner,fingerprint,payload,status,created_ms,deadline_ms,node_id,node_payload) VALUES(?,?,?,?,?,?,?,?,?,?)`),
			row.id, "job-"+row.id, "parent", "fingerprint", `{}`, row.status, 1, time.Now().Add(time.Hour).UnixMilli(), nodeID, payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

type preColumnRow struct{ id, status, machine, dir string }

// 1200 bound rows from before the columns are filled in three batches at one
// start, and a later start is a no-op.
func TestBoundMachineBackfillBoundedBatches_t6404(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		first := queueTestStore(t, open)
		statuses := []string{"bound", "dispatching", "reconciling", "bound_grant", "dispatching_grant", "reconciling_grant"}
		var rows []preColumnRow
		want := map[string]string{}
		for i := range 1200 {
			row := preColumnRow{id: fmt.Sprintf("bound-%04d", i), status: statuses[i%len(statuses)], machine: fmt.Sprintf("machine-%d", i/len(statuses)%3)}
			exclusive := 1
			if i%4 == 0 {
				row.dir, exclusive = fmt.Sprintf("dir-%d", i), 0
			}
			rows = append(rows, row)
			want[row.id] = fmt.Sprintf("%q,%d", row.machine, exclusive)
		}
		// Not bound or finished: left as they are.
		for _, status := range []string{"accepted", "succeeded", "failed"} {
			row := preColumnRow{id: "other-" + status, status: status, machine: "machine-0"}
			rows = append(rows, row)
			want[row.id] = `"",0`
		}
		// In flight but past the backfill's states: the same start's repair
		// fills it.
		rows = append(rows, preColumnRow{id: "other-deadline_exceeded", status: "deadline_exceeded", machine: "machine-0"})
		want["other-deadline_exceeded"] = `"machine-0",1`
		seedPreColumnRows(t, first, rows)
		forgetSpawnBoundBackfill(t, first)
		if err := first.close(); err != nil {
			t.Fatal(err)
		}

		second := queueTestStore(t, open)
		if !slices.Equal(second.spawnBoundBackfilled, []int{500, 500, 200}) {
			t.Fatalf("backfill batches = %v, want [500 500 200]", second.spawnBoundBackfilled)
		}
		if second.spawnBoundRepaired != 1 {
			t.Fatalf("the repair after the backfill wrote %d rows, want the one past its states", second.spawnBoundRepaired)
		}
		wrong := 0
		for id, columns := range want {
			if got := drainColumns(t, second, id); got != columns {
				if wrong++; wrong <= 5 {
					t.Errorf("%s: bound_machine,exclusive = %s, want %s", id, got, columns)
				}
			}
		}
		if wrong > 0 {
			t.Fatalf("%d rows hold the wrong columns", wrong)
		}

		// A later start is a no-op: the backfill does not run and nothing is
		// rewritten.
		if err := second.close(); err != nil {
			t.Fatal(err)
		}
		third := queueTestStore(t, open)
		if third.spawnBoundBackfilled != nil || third.spawnBoundRepaired != 0 {
			t.Fatalf("a second start wrote: backfill %v, repaired %d", third.spawnBoundBackfilled, third.spawnBoundRepaired)
		}
		// A scan reads the columns alone: a row without them is not decoded.
		late := preColumnRow{id: "after-start", status: "bound", machine: "machine-late"}
		seedPreColumnRows(t, third, []preColumnRow{late})
		marks, err := third.boundExclusiveSpawns(t.Context(), time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if slices.ContainsFunc(marks, func(row boundExclusiveSpawn) bool { return row.machine == late.machine }) || len(marks) != 3 {
			t.Fatalf("marks = %+v, want the three backfilled machines only", marks)
		}
	})
}

// A source that has not run the backfill is refused; its own start records it.
func TestStoreCopySourceSpawnBoundBackfilledSQLite_t6433(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source.sqlite")
	s := openSpawnStore(t, path)
	src, err := openStoreCopyDB(t.Context(), "sqlite://"+path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer src.db.Close()
	check := func(label string, want bool) {
		t.Helper()
		got, err := storeCopySourceSpawnBoundBackfilled(t.Context(), src)
		if err != nil || got != want {
			t.Fatalf("%s: backfilled=%v err=%v, want %v", label, got, err, want)
		}
	}
	check("a store started on this build", true)
	forgetSpawnBoundBackfill(t, s)
	check("a store without the backfill", false)
	if notices, err := storeCopySourceBackfilled(t.Context(), src); err != nil || !notices {
		t.Fatalf("the notice backfill is its own record: %v %v", notices, err)
	}
	recordSpawnBoundBackfill(t, s)
	check("after the source's start", true)
}

// The copier refuses such a source before it touches the target. Its name puts
// it in the CI job that has a MySQL server.
func TestStoreCopyIntoMySQLRefusesUnbackfilledSource_t6433(t *testing.T) {
	_, targetURL := hubMySQLTestDatabase(t)
	path := filepath.Join(t.TempDir(), "source.sqlite")
	forgetSpawnBoundBackfill(t, openSpawnStore(t, path))
	err := copyHubStore(t.Context(), "sqlite://"+path, targetURL, "", false, false, io.Discard)
	if !errors.Is(err, errStoreCopySourceSpawnBoundNotBackfilled) {
		t.Fatalf("source without the backfill: %v", err)
	}
	target, err := openStoreCopyDB(t.Context(), targetURL, false)
	if err != nil {
		t.Fatal(err)
	}
	defer target.db.Close()
	var tables int
	if err := target.db.QueryRow(`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE()`).Scan(&tables); err != nil || tables != 0 {
		t.Fatalf("refused copy touched the target: %d tables, %v", tables, err)
	}
}

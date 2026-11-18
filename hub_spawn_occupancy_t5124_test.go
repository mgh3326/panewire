package panewire

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// The placed columns migrate with the store on both dialects: a fresh store
// applies them, and a store from before them gains them on upgrade (hk 5124).
// Postgres runs in CI only; locally its half skips without a database.
func TestPlacedColumnsMigrationAppliesBothDialects_t5124(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		if err := s.ensureSpawnTasks(); err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		now := time.Now().UTC()
		claim := func(suffix int, job string) string {
			t.Helper()
			req := durableSpawnRequest()
			req.RequestID = fmt.Sprintf("d676220b-e763-4af1-aefd-0000000051%02d", suffix)
			for i := range req.Args {
				if req.Args[i] == "--task" {
					req.Args[i+1] = fmt.Sprint(5100 + suffix)
				}
			}
			if _, _, err := s.claimSpawn(ctx, req, job, "parent", now); err != nil {
				t.Fatal(err)
			}
			return req.RequestID
		}
		bind := func(requestID string, suffix int, account string) hubSpawnTask {
			t.Helper()
			node := hubSpawnRequest{RequestID: fmt.Sprintf("d676220b-e763-4af1-aefd-0000000052%02d", suffix), Machine: "machine-a"}
			task, err := s.bindSpawnPlaced(ctx, requestID, 1, node, account, now)
			if err != nil {
				t.Fatal(err)
			}
			return task
		}
		// Fresh stores already carry the columns: the bind round-trips them.
		first := claim(1, "job-placed-fresh")
		if bound := bind(first, 1, "commandcode:fp1"); bound.PlacedAccountKey != "commandcode:fp1" || bound.PlacedAttempt != 1 {
			t.Fatalf("fresh placed = %q/%d, want commandcode:fp1/1", bound.PlacedAccountKey, bound.PlacedAttempt)
		}
		// Roll back to a pre-column store, then upgrade: dropping the columns
		// and re-running the migration path must bring them back.
		for _, statement := range []string{
			`ALTER TABLE spawn_operations DROP COLUMN placed_account_key`,
			`ALTER TABLE spawn_operations DROP COLUMN placed_attempt`,
		} {
			if _, err := s.db.Exec(statement); err != nil {
				t.Fatal(err)
			}
		}
		if s.dialect == laneInboxPostgres {
			if _, err := s.db.Exec(`DELETE FROM panewire_schema_migrations WHERE version='0022_spawn_placed_account'`); err != nil {
				t.Fatal(err)
			}
			_ = s.close()
			s = open()
		}
		if err := s.ensureSpawnTasks(); err != nil {
			t.Fatal(err)
		}
		second := claim(2, "job-placed-upgraded")
		if bound := bind(second, 2, "commandcode:fp2"); bound.PlacedAccountKey != "commandcode:fp2" || bound.PlacedAttempt != 1 {
			t.Fatalf("upgraded placed = %q/%d, want commandcode:fp2/1", bound.PlacedAccountKey, bound.PlacedAttempt)
		}
	})
}

// Two concurrent commandcode admissions on one nearly spent account admit
// exactly one seat; below the weekly gate both are admitted (hk 5124).
func TestConcurrentCommandcodeAdmissionsAdmitOne_t5124(t *testing.T) {
	h := asyncSpawnHub(t)
	agent := &hubAgent{}
	h.connect("machine-a", "test", "fixture", agent, true, []string{"spawn_ops_v1"})
	now := time.Now()
	const acct = "commandcode:5c7a09bd00000099"

	spawn := func(op hubSpawnTask, n int, weekly float64) error {
		transport := &directSpawnTransport{
			h: h, task: op,
			snapshotPool:       commandcodePoolName,
			snapshotAccountKey: acct,
			snapshotWeeklyUsed: &weekly,
		}
		req := fleet.HubSpawnRequest{
			RequestID: fmt.Sprintf("d676220b-e763-4af1-aefd-0000000053%02d", n),
			Machine:   "machine-a", CWDKey: "project",
			Brief: fleet.HubBrief{Doc: "brief/test"},
			Args:  op.Request.Args, WaitSeconds: 600,
			AccountWeeklyUsed: &weekly,
		}
		_, err := transport.Spawn(t.Context(), req)
		return err
	}
	race := func(n1, n2 int, job1, job2 string, weekly float64) []error {
		op1 := liveOp(t, h, n1, job1, "auto", now)
		op2 := liveOp(t, h, n2, job2, "auto", now)
		start := make(chan struct{})
		errs := make([]error, 2)
		var wg sync.WaitGroup
		for i, tc := range []struct {
			op hubSpawnTask
			n  int
		}{{op1, n1}, {op2, n2}} {
			wg.Add(1)
			go func(i int, op hubSpawnTask, n int) {
				defer wg.Done()
				<-start
				errs[i] = spawn(op, n, weekly)
			}(i, tc.op, tc.n)
		}
		close(start)
		wg.Wait()
		return errs
	}

	// At 7d = 80% the second admission waits for the first bind, then sees it.
	high := 80.0
	errs := race(11, 12, "job-race-high-1", "job-race-high-2", high)
	bound := 0
	for _, err := range errs {
		if errors.Is(err, fleet.ErrHubPlacementBound) {
			bound++
		}
	}
	if bound != 1 {
		t.Fatalf("nearly spent race admitted %d seats, want exactly 1 (errs %v)", bound, errs)
	}
	var busy *fleet.HubError
	for _, err := range errs {
		if errors.As(err, &busy) && strings.HasPrefix(busy.Kind, accountBusyNearWeeklyLimit) {
			break
		}
		busy = nil
	}
	if busy == nil {
		t.Fatalf("loser refusal = %v, want %s", errs, accountBusyNearWeeklyLimit)
	}
	jobs, err := h.liveAccountJobs(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[acct] == "" {
		t.Fatalf("occupancy after race = %v, want one seat on %s", jobs, acct)
	}

	// At 7d = 70% the gate allows a second seat: both binds proceed.
	low := 70.0
	errs = race(13, 14, "job-race-low-1", "job-race-low-2", low)
	for _, err := range errs {
		if !errors.Is(err, fleet.ErrHubPlacementBound) {
			t.Fatalf("below-gate admission refused: %v", err)
		}
	}
}

// The placed columns clear in the same UPDATE that changes the status: a
// trigger forbidding a seated row outside bound/dispatching/reconciling turns
// a split write red (hk 5124).
func TestPlacedColumnsClearInSameWrite_t5124(t *testing.T) {
	h := asyncSpawnHub(t)
	now := time.Now()
	const acct = "commandcode:5c7a09bd00000098"
	const singleWriteTrigger = `CREATE TEMP TRIGGER placed_columns_single_write
		BEFORE UPDATE ON spawn_operations
		FOR EACH ROW WHEN NEW.placed_account_key<>'' AND NEW.status NOT IN ('bound','dispatching','reconciling','bound_grant','dispatching_grant','reconciling_grant')
		BEGIN SELECT RAISE(ABORT, 'placed columns outlive their status'); END`
	if _, err := h.inbox.store.db.Exec(singleWriteTrigger); err != nil {
		t.Fatal(err)
	}
	// staleSeat stages a row the old model left behind: a seat on a status
	// that must clear it. The trigger forbids staging it directly, so the
	// trigger drops while the stale row is written, then goes back up before
	// the write under test. A nonzero expires pins the row's limit.
	staleSeat := func(n int, job, status string, expires time.Time) hubSpawnTask {
		t.Helper()
		if _, err := h.inbox.store.db.Exec(`DROP TRIGGER placed_columns_single_write`); err != nil {
			t.Fatal(err)
		}
		task := liveOp(t, h, n, job, "auto", now)
		if !expires.IsZero() {
			if _, err := h.inbox.store.db.Exec(`UPDATE spawn_operations SET expires_ms=? WHERE request_id=?`, expires.UnixMilli(), task.Request.RequestID); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := h.inbox.store.db.Exec(`UPDATE spawn_operations SET status=?,placed_account_key=?,placed_attempt=? WHERE request_id=?`, status, acct, task.Attempt, task.Request.RequestID); err != nil {
			t.Fatal(err)
		}
		if _, err := h.inbox.store.db.Exec(singleWriteTrigger); err != nil {
			t.Fatal(err)
		}
		staged, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
		if err != nil {
			t.Fatal(err)
		}
		return staged
	}
	bind := func(n int, job, machine string) hubSpawnTask {
		t.Helper()
		task := liveOp(t, h, n, job, "auto", now)
		node := hubSpawnRequest{RequestID: fmt.Sprintf("d676220b-e763-4af1-aefd-0000000054%02d", n), Machine: machine}
		bound, err := h.inbox.store.bindSpawnPlaced(t.Context(), task.Request.RequestID, task.Attempt, node, acct, now)
		if err != nil {
			t.Fatal(err)
		}
		return bound
	}
	// dispatching -> failed clears in the completion write.
	dispatched, err := h.inbox.store.spawnTask(t.Context(), bind(21, "job-atomic-fail", "machine-a").Request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := h.inbox.store.beginSpawnSend(t.Context(), dispatched); err != nil || !ok {
		t.Fatalf("begin send: %v %v", ok, err)
	}
	sent, err := h.inbox.store.spawnTask(t.Context(), dispatched.Request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.inbox.store.completeSpawn(t.Context(), sent, hubSpawnOutcome{RC: 1, Stage: "landing", Reason: "spawn_failed", Host: "machine-a"}, now); err != nil {
		t.Fatal(err)
	}
	// bound -> retryable -> accepted clears in each write.
	retryable := bind(22, "job-atomic-retry", "machine-a")
	if _, err := h.inbox.store.completeSpawn(t.Context(), retryable, hubSpawnOutcome{RC: 1, Stage: "landing", Reason: "spawn_failed", Retryable: true}, now); err != nil {
		t.Fatal(err)
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), retryable.Request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.inbox.store.resumeCleanedSpawn(t.Context(), saved, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	// dispatching -> accepted clears in the refusal write.
	refused, err := h.inbox.store.spawnTask(t.Context(), bind(23, "job-atomic-refuse", "machine-a").Request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := h.inbox.store.beginSpawnSend(t.Context(), refused); err != nil || !ok {
		t.Fatalf("begin send: %v %v", ok, err)
	}
	sentRefused, err := h.inbox.store.spawnTask(t.Context(), refused.Request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.inbox.store.unbindRefusedSpawn(t.Context(), sentRefused, hubSpawnExclusion{Machine: "machine-a", Reason: "spawn_failed"}, nil, now); err != nil {
		t.Fatal(err)
	}
	// bound -> accepted clears in the unbind write.
	held := bind(24, "job-atomic-unbind", "machine-a")
	heldNode := held.Node.RequestID
	if _, err := h.inbox.store.unbindSpawn(t.Context(), held.Request.RequestID, held.Attempt, heldNode, "bound"); err != nil {
		t.Fatal(err)
	}
	// reconciling without a node -> accepted clears in the resume write.
	reconciling := staleSeat(25, "job-atomic-reconcile", "reconciling", time.Time{})
	if _, err := h.inbox.store.resumeUndispatchedSpawn(t.Context(), reconciling); err != nil {
		t.Fatal(err)
	}
	// retryable -> accepted clears in the expiry handover write.
	expiringTask := staleSeat(26, "job-atomic-expiry", "retryable", time.Time{})
	expiryTx, err := h.inbox.store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := h.inbox.store.endUnstartedAttempt(t.Context(), expiryTx, expiringTask, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Host: "machine-a", Retryable: true}, now); err != nil || !changed {
		t.Fatalf("expiry handover: %v %v", changed, err)
	}
	// retryable at the limit -> given_up clears in the same write.
	givenUpTask := staleSeat(27, "job-atomic-givenup", "retryable", now.Add(-time.Hour))
	givenUpTx, err := h.inbox.store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := h.inbox.store.endUnstartedAttempt(t.Context(), givenUpTx, givenUpTask, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Host: "machine-a", Retryable: true}, now); err != nil || !changed {
		t.Fatalf("expiry give-up: %v %v", changed, err)
	}
}

// A failed occupancy lookup refuses fail-closed with account_busy_unknown
// and logs one warn per failure streak (hk 5124).
func TestAccountJobsLookupFailureRefusesUnknown_t5124(t *testing.T) {
	var warnings atomic.Int64
	h, err := NewHubServer(HubServerConfig{
		Tokens:         map[string]string{"operator": t14OperatorToken},
		InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3"),
		Logger:         slog.New(warnCounterHandler{count: &warnings}),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	if _, err := h.liveAccountJobs(t.Context(), ""); err != nil {
		t.Fatal(err)
	}
	if n := warnings.Load(); n != 0 {
		t.Fatalf("warnings after success = %d, want 0", n)
	}
	// Break the store: every lookup now fails.
	if err := h.inbox.store.db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := h.liveAccountJobs(t.Context(), ""); err == nil {
		t.Fatal("liveAccountJobs on a closed store succeeded, want an error")
	}
	if _, err := h.liveAccountJobs(t.Context(), ""); err == nil {
		t.Fatal("liveAccountJobs on a closed store succeeded, want an error")
	}
	if n := warnings.Load(); n != 1 {
		t.Fatalf("warnings after a failure streak = %d, want exactly 1", n)
	}
	h.noteAccountJobsLookup(nil)
	h.mu.Lock()
	streak := h.accountJobsLookupFails
	h.mu.Unlock()
	if streak != 0 {
		t.Fatalf("streak after success = %d, want 0", streak)
	}
}

// warnCounterHandler counts Warn records for the fail-closed streak test.
type warnCounterHandler struct {
	count *atomic.Int64
}

func (w warnCounterHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelWarn
}

func (w warnCounterHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Level == slog.LevelWarn {
		w.count.Add(1)
	}
	return nil
}

func (w warnCounterHandler) WithAttrs(_ []slog.Attr) slog.Handler { return w }
func (w warnCounterHandler) WithGroup(_ string) slog.Handler      { return w }

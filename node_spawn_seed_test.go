package panewire

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

type spawnRestartFixture struct {
	client      *HubClient
	peer        *hubClientConnection
	results     <-chan hubSpawnResult
	old         hubOutboundMessage
	nowMS       atomic.Int64
	start       time.Time
	herdrCalled chan struct{}
}

func newSpawnRestartFixture(t *testing.T, slots int, old hubOutboundMessage) *spawnRestartFixture {
	t.Helper()
	rig := newConcurrencyRig(t, slots)
	peer, results := admissionPeer(t)
	f := &spawnRestartFixture{
		client: rig.client, peer: peer, results: results, old: old,
		start: time.Now().UTC().Truncate(time.Millisecond), herdrCalled: make(chan struct{}),
	}
	f.nowMS.Store(f.start.UnixMilli())
	f.client.now = func() time.Time { return time.UnixMilli(f.nowMS.Load()) }
	f.client.spawnEngine = func(context.Context, fleet.LaunchDir, string, string, string, string, time.Time, []string) (*hubSpawnOutput, fleet.Execution) {
		return t14EngineOK()
	}
	var once sync.Once
	f.client.nodeHerdr = func(context.Context, ...string) ([]byte, error) {
		once.Do(func() { close(f.herdrCalled) })
		return nil, errors.New("herdr unreachable")
	}
	return f
}

func (f *spawnRestartFixture) startRecovery(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() {
		cancel()
		eventually(t, 3*time.Second, "restart journal closed", func() bool {
			f.client.spawnJournalMu.Lock()
			defer f.client.spawnJournalMu.Unlock()
			return f.client.spawnJournalUsers == 0
		})
	})
	f.client.startSpawnRecovery(ctx, f.start)
	admissionWaitSignal(t, f.herdrCalled, "startup settle tried herdr")
}

func (f *spawnRestartFixture) legacyClaim(t *testing.T) {
	t.Helper()
	journal, err := openNodeSpawnJournal(f.client.spawnJournalDir)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.db.Close()
	if claimed, _, err := journal.claim(t.Context(), f.old, spawnJournalHold{}); err != nil || !claimed {
		t.Fatalf("legacy claim: claimed=%v err=%v", claimed, err)
	}
}

func (f *spawnRestartFixture) spawn(t *testing.T, message hubOutboundMessage, want string) {
	t.Helper()
	f.client.handleHubSpawn(t.Context(), f.peer, message)
	got := admissionResult(t, f.results, 3*time.Second)
	if got.RequestID != message.RequestID || got.Error != want || (want == "" && got.RC != 0) {
		t.Fatalf("spawn %s: got %+v, want error %q", message.RequestID, got, want)
	}
}

func (f *spawnRestartFixture) requireExecuting(t *testing.T, requestID string) executingSpawn {
	t.Helper()
	journal, err := f.client.acquireSpawnJournal()
	if err != nil {
		t.Fatal(err)
	}
	defer f.client.releaseSpawnJournal()
	rows, err := journal.executing(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.RequestID == requestID {
			return row
		}
	}
	t.Fatalf("request %s is no longer executing", requestID)
	return executingSpawn{}
}

func recordInterruptedSpawn(t *testing.T, previous *HubClient, peer *hubClientConnection, old hubOutboundMessage) {
	t.Helper()
	ctx, abort := context.WithCancel(t.Context())
	defer abort()
	previous.spawnEngine = func(context.Context, fleet.LaunchDir, string, string, string, string, time.Time, []string) (*hubSpawnOutput, fleet.Execution) {
		abort()
		return &hubSpawnOutput{}, fleet.Execution{RC: 1}
	}
	previous.handleHubSpawn(ctx, peer, old)
	if ctx.Err() == nil {
		t.Fatal("fixture did not reach the engine")
	}
}

func (f *spawnRestartFixture) interrupt(t *testing.T) executingSpawn {
	t.Helper()
	previous := &HubClient{
		spawnConfigPath:    f.client.spawnConfigPath,
		spawnJournalDir:    f.client.spawnJournalDir,
		spawnPrepare:       f.client.spawnPrepare,
		spawnSlotsOverride: 1,
		now:                f.client.now,
		fleetEvents:        (&fleetEventRecorder{}).write,
	}
	recordInterruptedSpawn(t, previous, f.peer, f.old)
	row := f.requireExecuting(t, f.old.RequestID)
	if row.CreatedMS != f.start.UnixMilli() || row.DeadlineMS != f.old.DeadlineMS || row.StartByMS != f.old.StartByMS || row.HasCheckout != (f.old.Checkout != nil) {
		t.Fatalf("journal claim metadata = %+v", row)
	}
	if row.HasCheckout && (row.Key != hubSpawnOverlapKey(&hubSpawnRequest{Checkout: f.old.Checkout, Args: f.old.Args}) || filepath.Base(row.FinalCWD) != f.old.Checkout.Dir || row.RootReal == "") {
		t.Fatalf("journal checkout metadata = %+v", row)
	}
	return row
}

func TestRestartKeepsExecutingHold_t6367(t *testing.T) {
	f := newSpawnRestartFixture(t, 2, concurrencyMessage(1, "job-a"))
	f.interrupt(t)
	f.startRecovery(t)
	a := concurrencyMessage(2, "job-a")
	a.DeadlineMS = time.Now().Add(350 * time.Millisecond).UnixMilli()
	b := concurrencyMessage(3, "job-b")
	go f.client.handleHubSpawn(t.Context(), f.peer, a)
	f.client.handleHubSpawn(t.Context(), f.peer, b)
	for range 2 {
		got := admissionResult(t, f.results, 3*time.Second)
		switch got.RequestID {
		case b.RequestID:
			if got.RC != 0 {
				t.Fatalf("unrelated checkout failed: %+v", got)
			}
		case a.RequestID:
			if got.Error != "deadline_not_started" {
				t.Fatalf("same checkout bypassed restored hold: %+v", got)
			}
		default:
			t.Fatalf("unexpected result: %+v", got)
		}
	}
	f.requireExecuting(t, f.old.RequestID)
}

func TestUnprovableExecutingRowReleasesAtDeadline_t6367(t *testing.T) {
	f := newSpawnRestartFixture(t, 2, concurrencyMessage(1, "job-a"))
	row := f.interrupt(t)
	f.startRecovery(t)
	f.nowMS.Store(time.UnixMilli(row.DeadlineMS).Add(2 * time.Minute).UnixMilli())
	f.spawn(t, concurrencyMessage(2, "job-a"), "")
	f.requireExecuting(t, f.old.RequestID)
}

func TestLegacyRowDoesNotWedgeN1Node_t6367(t *testing.T) {
	f := newSpawnRestartFixture(t, 1, admissionMessage(1, true))
	f.legacyClaim(t)
	f.startRecovery(t)
	f.spawn(t, concurrencyMessage(2, "job-a"), "")
	f.requireExecuting(t, f.old.RequestID)
}

func TestLegacyRowReleasesAfterLegacyHold_t6404(t *testing.T) {
	f := newSpawnRestartFixture(t, 2, admissionMessage(1, true))
	f.legacyClaim(t)
	f.startRecovery(t)
	b := concurrencyMessage(2, "job-b")
	b.DeadlineMS = time.Now().Add(350 * time.Millisecond).UnixMilli()
	f.spawn(t, b, "deadline_not_started")
	f.nowMS.Store(f.start.Add(10 * time.Minute).UnixMilli())
	f.spawn(t, concurrencyMessage(3, "job-b"), "")
	f.requireExecuting(t, f.old.RequestID)
}

func TestNoCheckoutRowHoldsNothing_t6404(t *testing.T) {
	f := newSpawnRestartFixture(t, 2, admissionMessage(1, true))
	f.interrupt(t)
	f.startRecovery(t)
	f.spawn(t, concurrencyMessage(2, "job-b"), "")
	f.requireExecuting(t, f.old.RequestID)
}

func TestJournalReadErrorRetriesThenOpens_t6404(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	start := time.Now().UTC().Truncate(time.Millisecond)
	var now atomic.Int64
	now.Store(start.UnixMilli())
	client := &HubClient{now: func() time.Time { return time.UnixMilli(now.Load()) }}
	a := client.nodeAdmission()
	a.seeding = true
	journal, err := openNodeSpawnJournal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.db.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		admissionWaitSignal(t, done, "seed retry stopped")
	})
	var attempts atomic.Int32
	go func() {
		defer close(done)
		client.seedSpawnAdmission(ctx, start, func(ctx context.Context) ([]executingSpawn, error) {
			attempts.Add(1)
			return journal.executing(ctx)
		})
	}()
	assertClosed := func() {
		t.Helper()
		if release, _ := a.tryAdmit("dir:b", 2); release != nil {
			release()
			t.Fatal("admitted during the read-error hold")
		}
	}
	for i := int32(1); i <= 4; i++ {
		eventually(t, 5*time.Second, "journal read attempted", func() bool { return attempts.Load() >= i })
		assertClosed()
	}
	admissionWaitSignal(t, done, "seed retries exhausted")
	if attempts.Load() != 4 || strings.Count(logs.String(), "spawn admission seed failed") != 1 {
		t.Fatalf("attempts=%d logs=%s", attempts.Load(), logs.String())
	}
	if stats := client.nodeLoopSnapshot()[nodeSpawnSettle]; stats.SilentErrors != 1 {
		t.Fatalf("silent error count = %d", stats.SilentErrors)
	}
	assertClosed()
	now.Store(start.Add(2 * time.Minute).UnixMilli())
	release, _ := a.tryAdmit("dir:b", 2)
	if release == nil {
		t.Fatal("read-error hold did not expire")
	}
	release()
}

func TestRestartRootPinFromJournal_t6367(t *testing.T) {
	fixture, restarted := rootChangeSpawnFixture(t, 2)
	f := newSpawnRestartFixture(t, 2, rootChangeMessage(fixture, 636711, "job-a"))
	previous := fixture.client()
	previous.spawnJournalDir = restarted.spawnJournalDir
	previous.now = f.client.now
	restarted.now = f.client.now
	restarted.nodeHerdr = f.client.nodeHerdr
	f.client = restarted
	recordInterruptedSpawn(t, previous, f.peer, f.old)
	row := f.requireExecuting(t, f.old.RequestID)
	if row.RootReal != fixture.workRoot || row.FinalCWD != filepath.Join(fixture.workRoot, "job-a") {
		t.Fatalf("prepared target was not journaled: %+v", row)
	}
	newRoot := filepath.Join(fixture.root, "new-root")
	if err := os.Mkdir(newRoot, 0700); err != nil {
		t.Fatal(err)
	}
	writeSpawnRoot(t, fixture.config, newRoot)
	restarted.spawnEngine = func(_ context.Context, cwd fleet.LaunchDir, _, _, _, _ string, _ time.Time, _ []string) (*hubSpawnOutput, fleet.Execution) {
		if want := filepath.Join(newRoot, "job-b"); cwd.Path != want {
			t.Errorf("launch cwd = %s, want %s", cwd.Path, want)
		}
		return t14EngineOK()
	}
	f.startRecovery(t)
	b := rootChangeMessage(fixture, 636712, "job-b")
	f.spawn(t, b, "checkout_refused:work_root_changed")
	f.nowMS.Store(time.UnixMilli(row.DeadlineMS).Add(2 * time.Minute).UnixMilli())
	f.spawn(t, b, "")
}

func TestNodeSpawnJournalMigratesRestartHoldColumns(t *testing.T) {
	root := t.TempDir()
	old, err := sql.Open("sqlite", filepath.Join(root, "spawn-inbox.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = old.ExecContext(t.Context(), `CREATE TABLE spawn_inbox (request_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, status TEXT NOT NULL,
		result TEXT NOT NULL DEFAULT '', job_id TEXT NOT NULL DEFAULT '', label TEXT NOT NULL DEFAULT '',
		strict INTEGER NOT NULL DEFAULT 0, sparse_hash TEXT NOT NULL DEFAULT '');
		INSERT INTO spawn_inbox (request_id,fingerprint,status,job_id,label,strict) VALUES ('old','old','executing','job-a','label-a',1)`)
	closeErr := old.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("write old journal: %v / %v", err, closeErr)
	}
	for range 2 {
		journal, err := openNodeSpawnJournal(root)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := journal.executing(t.Context())
		closeErr := journal.db.Close()
		want := executingSpawn{RequestID: "old", Job: "job-a", Label: "label-a", Strict: true}
		if err != nil || closeErr != nil || len(rows) != 1 || rows[0] != want {
			t.Fatalf("migrated rows=%+v readErr=%v closeErr=%v", rows, err, closeErr)
		}
	}
}

func TestRestartHoldWakesWaitingAdmissionAtExpiry(t *testing.T) {
	f := newSpawnRestartFixture(t, 2, concurrencyMessage(1, "job-a"))
	f.client.now = time.Now
	journal, err := openNodeSpawnJournal(f.client.spawnJournalDir)
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(2 * time.Second)
	hold := spawnJournalHold{
		Key:        f.client.spawnTargetFor(f.old).Key,
		CreatedMS:  expires.Add(-5 * time.Minute).UnixMilli(),
		DeadlineMS: expires.Add(-4 * time.Minute).UnixMilli(),
	}
	// The grant's start-by is later than the local admission deadline.
	f.old.StartByMS = expires.Add(-2 * time.Minute).UnixMilli()
	claimed, _, claimErr := journal.claim(t.Context(), f.old, hold)
	closeErr := journal.db.Close()
	if !claimed || claimErr != nil || closeErr != nil {
		t.Fatalf("seed row: claimed=%v err=%v close=%v", claimed, claimErr, closeErr)
	}
	f.startRecovery(t)
	a := concurrencyMessage(2, "job-a")
	a.DeadlineMS = time.Now().Add(4 * time.Second).UnixMilli()
	f.spawn(t, a, "")
	if time.Now().UnixMilli() < expires.UnixMilli() {
		t.Fatal("hold released before the grant's start-by plus two minutes")
	}
	f.requireExecuting(t, f.old.RequestID)
}

func TestRestartHoldReleasesOnSettle(t *testing.T) {
	f := newSpawnRestartFixture(t, 2, concurrencyMessage(1, "job-a"))
	// At startup herdr is unreachable. Once it recovers, replay can prove
	// that the interrupted request never launched.
	var reachable atomic.Bool
	emptyHerdr := &nodeFakeHerdr{agents: []string{nodeAgentList()}, panes: settlePanes()}
	unreachableHerdr := f.client.nodeHerdr
	f.client.nodeHerdr = func(ctx context.Context, args ...string) ([]byte, error) {
		if reachable.Load() {
			return emptyHerdr.run(ctx, args...)
		}
		return unreachableHerdr(ctx, args...)
	}
	f.interrupt(t)
	f.startRecovery(t)
	reachable.Store(true)
	f.spawn(t, f.old, "spawn_failed")
	f.spawn(t, concurrencyMessage(2, "job-a"), "")
}

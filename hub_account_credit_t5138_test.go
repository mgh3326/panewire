package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// hk 5138: a Command Code credit rejection on one job's pane marks that job's
// own account, never the pool, and the job gets the pool-exhausted treatment
// (owner notice, and for an implementer the quota-stop successor). The mark is
// durable and lifts at the first reading of the account after it or in 24 h.
const (
	ccMarkAccountA = "commandcode:7d279b7dcc317cda"
	ccMarkAccountB = "commandcode:a1fd7fc5ae227218"
)

func commandcodeCreditScreen(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("testdata/quota-stop/omp-commandcode-credit-2026-11-18.txt")
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// The exact screen text marks only the job's account: the other account's job
// is not notified, and the snapshot placement reads carries one mark.
func TestCommandcodeCreditRejectionMarksOnlyThatAccount(t *testing.T) {
	f := newSweepFixture(t)
	marked := f.addNamedJob(t, "cmd-dsflash41", "reviewer", "commandcode", ccMarkAccountA, "idle", "sweep-1", 1)
	other := f.addNamedJob(t, "cmd-dsflash41", "implementer", "commandcode", ccMarkAccountB, "idle", "sweep-2", 2)
	f.setScreen(t, marked, commandcodeCreditScreen(t))

	f.hub.checkPoolTriggers(t.Context())
	eventually(t, 5*time.Second, "the commandcode account sweep", func() bool { return f.sweeps() >= 1 })

	counts := noticeJobs(spawnFailedInjections(f.destination))
	if counts[marked] != 1 {
		t.Fatalf("job on the rejected account notices=%d, want one: %+v", counts[marked], spawnFailedInjections(f.destination))
	}
	if counts[other] != 0 {
		t.Fatalf("another account's job was notified: %+v", counts)
	}
	for _, event := range spawnFailedInjections(f.destination) {
		if !strings.Contains(event.Text, "no provider credit left") || !strings.Contains(event.Text, ccMarkAccountA) {
			t.Fatalf("notice does not name the account: %+v", event)
		}
	}

	marks := f.hub.fleetSnapshot(f.hub.now()).AccountCreditExhausted
	if _, ok := marks[ccMarkAccountA]; !ok {
		t.Fatalf("snapshot marks=%+v, want %s", marks, ccMarkAccountA)
	}
	if _, ok := marks[ccMarkAccountB]; ok {
		t.Fatalf("snapshot marked the other account: %+v", marks)
	}

	// A second pass repeats neither the mark nor the notice.
	before := marks[ccMarkAccountA]
	f.hub.checkPoolTriggers(t.Context())
	time.Sleep(100 * time.Millisecond) // the window proves no further notice
	if extra := spawnFailedInjections(f.destination); len(extra) != 0 {
		t.Fatalf("duplicate notices: %+v", extra)
	}
	if after := f.hub.fleetSnapshot(f.hub.now()).AccountCreditExhausted[ccMarkAccountA]; !after.Equal(before) {
		t.Fatalf("repeated pass moved the mark from %v to %v", before, after)
	}
}

// An implementer on the marked account is handed over through the quota-stop
// flow's successor, and the successor excludes no pool: the account mark keeps
// it off the spent account while the pool's other accounts stay placeable.
func TestCommandcodeCreditRejectionHandsOverImplementer(t *testing.T) {
	f := newSweepFixture(t)
	job := f.addNamedJob(t, "cmd-dsflash41", "implementer", "commandcode", ccMarkAccountA, "idle", "sweep-1", 1)
	f.setScreen(t, job, commandcodeCreditScreen(t))

	f.hub.checkPoolTriggers(t.Context())
	var successor hubSpawnTask
	eventually(t, 10*time.Second, "the successor to be claimed", func() bool {
		var err error
		successor, err = f.hub.inbox.store.latestSpawnForJob(t.Context(), "sweep-1-q1")
		return err == nil && f.state(job) == "lost"
	})
	if slices.Contains(successor.Request.Args, "--exclude-pool") {
		t.Fatalf("successor excludes a pool, dropping the account's healthy siblings: %v", successor.Request.Args)
	}
	if reason := f.node2.reason(job); !strings.Contains(reason, "handed_over_to=sweep-1-q1") {
		t.Fatalf("end_reason = %q, want the successor pointer", reason)
	}
	if counts := noticeJobs(spawnFailedInjections(f.destination)); counts[job] != 1 {
		t.Fatalf("owner notices=%d, want one: %+v", counts[job], counts)
	}
}

// The mark is served from the hub store after a restart, lifts at the first
// reading of the account observed after it, and lifts after 24 h otherwise.
func TestCommandcodeCreditMarkSurvivesRestartAndLifts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	at := time.Date(2026, 11, 18, 12, 40, 0, 0, time.UTC)
	now := at
	newHub := func() *HubServer {
		h, err := NewHubServer(HubServerConfig{
			Tokens:         map[string]string{"operator": t14OperatorToken},
			InboxStorePath: path,
			Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
			Now:            func() time.Time { return now },
		})
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	first := newHub()
	if err := first.markAccountCreditExhausted("commandcode", ccMarkAccountA, at); err != nil {
		t.Fatal(err)
	}
	first.Close()

	second := newHub()
	defer second.Close()
	now = at.Add(time.Hour)
	marks := marksAt(second, now)
	if got, ok := marks[ccMarkAccountA]; !ok || !got.Equal(at) {
		t.Fatalf("marks after restart = %+v, want %s at %v", marks, ccMarkAccountA, at)
	}

	publish := func(balance *float64, measured time.Time) {
		second.stateStore().publish(FleetStateMessage{
			Host: "mac-personal", Kind: fleetStateKindQuota, Key: ccMarkAccountA,
			MeasuredAt: measured, ReceivedAt: measured,
			Quota: &FleetStateQuota{AccountKey: ccMarkAccountA, Pool: "commandcode", Balance: balance},
		}, 0)
	}
	balance := func(n float64) *float64 { return &n }

	// A reading with no balance says nothing about the credits: the mark holds.
	publish(nil, at.Add(30*time.Minute))
	if _, held := marksAt(second, now)[ccMarkAccountA]; !held {
		t.Fatal("a reading with no balance lifted the mark")
	}
	// Below one credit is still an exhausted account: the mark holds.
	publish(balance(0.5), at.Add(40*time.Minute))
	if _, held := marksAt(second, now)[ccMarkAccountA]; !held {
		t.Fatal("a reading below one credit lifted the mark")
	}
	// At least one credit is the fresh report that lifts it.
	publish(balance(1), at.Add(50*time.Minute))
	if marks := marksAt(second, now); len(marks) != 0 {
		t.Fatalf("a reading with credits did not lift the mark: %+v", marks)
	}

	// A new episode holds until the 24 h bound.
	now = at.Add(2 * time.Hour)
	if err := second.markAccountCreditExhausted("commandcode", ccMarkAccountB, now); err != nil {
		t.Fatal(err)
	}
	if _, held := marksAt(second, now)[ccMarkAccountB]; !held {
		t.Fatal("the new mark does not hold")
	}
	if marks := marksAt(second, now.Add(24*time.Hour)); len(marks) != 0 {
		t.Fatalf("the mark outlived 24 h: %+v", marks)
	}
}

// An ordinary 400 is not a credit rejection: nothing is marked and no sweep
// runs.
func TestCommandcodeGeneric400DoesNotMark(t *testing.T) {
	f := newSweepFixture(t)
	job := f.addNamedJob(t, "cmd-dsflash41", "implementer", "commandcode", ccMarkAccountA, "idle", "sweep-1", 1)
	f.setScreen(t, job, "⏺ API Error: 400 {\"error\":{\"type\":\"invalid_request_error\",\"message\":\"bad request\"}}\n\n❯ \n")

	f.hub.checkPoolTriggers(t.Context())
	time.Sleep(200 * time.Millisecond) // the window proves nothing more happens
	if marks := f.hub.fleetSnapshot(f.hub.now()).AccountCreditExhausted; len(marks) != 0 {
		t.Fatalf("a generic 400 marked an account: %+v", marks)
	}
	if counts := noticeJobs(spawnFailedInjections(f.destination)); len(counts) != 0 {
		t.Fatalf("a generic 400 notified owners: %+v", counts)
	}
	if sweeps := f.sweeps(); sweeps != 0 {
		t.Fatalf("a generic 400 ran %d sweeps", sweeps)
	}
}

// hk 5138 review: when the successor cannot be placed (here the pane is still
// working), the sweep reports the job skipped so the caller releases it and the
// next pass retries while the account mark holds.
func TestCommandcodeAccountHandoverRetriesAfterFailure(t *testing.T) {
	f := newSweepFixture(t)
	job := f.addNamedJob(t, "cmd-dsflash41", "implementer", "commandcode", ccMarkAccountA, "idle", "sweep-1", 1)
	f.node2.setStatus("w11:p1", "working")
	if err := f.hub.markAccountCreditExhausted("commandcode", ccMarkAccountA, f.hub.now()); err != nil {
		t.Fatal(err)
	}
	task, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), job)
	if err != nil {
		t.Fatal(err)
	}
	e := poolExhaustion{Pool: "commandcode", Account: ccMarkAccountA, Cause: poolCauseCreditExhausted}
	for pass := 0; pass < 2; pass++ {
		if result := f.hub.sweepPoolJob(t.Context(), e, task); result.action != poolJobSkipped || result.why != "handover_failed" {
			t.Fatalf("pass %d = %+v, want skipped/handover_failed so the job is released and retried", pass, result)
		}
	}
}

// hk 5138 review: a failed first read of the durable marks is retried, so a
// busy store at hub start cannot leave every mark unserved for the process.
func TestAccountCreditMarkLoadRetriesAfterFailure(t *testing.T) {
	f := newSweepFixture(t)
	at := f.hub.now()
	mark := accountCreditMark{Account: ccMarkAccountA, Pool: "commandcode", Marked: at}
	if err := f.hub.inbox.store.putAccountCreditMark(t.Context(), mark.Account, mark); err != nil {
		t.Fatal(err)
	}
	calls := 0
	f.hub.accountCreditRead = func(ctx context.Context) (map[string]accountCreditMark, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("store busy")
		}
		return f.hub.inbox.store.accountCreditMarks(ctx)
	}
	if marks, err := f.hub.snapshotAccountCreditMarks(at); err == nil || len(marks) != 0 {
		t.Fatalf("a failed load = %+v, %v; want an error and no marks", marks, err)
	}
	marks := marksAt(f.hub, at)
	if _, ok := marks[ccMarkAccountA]; !ok || calls != 2 {
		t.Fatalf("retry marks=%+v calls=%d, want %s after a second read", marks, calls, ccMarkAccountA)
	}
}

// hk 5138: the durable marks are served on their own route, so a node reads
// them even when the fleet snapshot is off or unavailable.
func TestAccountCreditMarksEndpoint_t5138(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	hub, err := NewHubServer(HubServerConfig{
		Tokens:         map[string]string{"operator": fsOperatorToken},
		InboxStorePath: path,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hub.Close() })
	at := hub.now().UTC()
	if err := hub.markAccountCreditExhausted("commandcode", ccMarkAccountA, at); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(hub.Handler())
	defer server.Close()
	status, body := fsGet(t, server, "/v1/state/account-credit", fsOperator())
	if status != 200 {
		t.Fatalf("GET account-credit = %d %s", status, body)
	}
	var out struct {
		V     int                  `json:"v"`
		Marks map[string]time.Time `json:"account_credit_exhausted"`
	}
	if err := json.Unmarshal(body, &out); err != nil || out.V != 1 {
		t.Fatalf("account-credit response = %s, %v", body, err)
	}
	if got, ok := out.Marks[ccMarkAccountA]; !ok || !got.Equal(at) {
		t.Fatalf("marks = %+v, want %s at %v", out.Marks, ccMarkAccountA, at)
	}
}

// hk 5138: a failed delete of a lifted mark stays pending and is retried, so a
// restart cannot reload a mark whose reason is gone.
func TestAccountCreditMarkDeleteRetries_t5138(t *testing.T) {
	f := newSweepFixture(t)
	at := f.hub.now()
	mark := accountCreditMark{Account: ccMarkAccountA, Pool: "commandcode", Marked: at}
	if err := f.hub.inbox.store.putAccountCreditMark(t.Context(), mark.Account, mark); err != nil {
		t.Fatal(err)
	}
	balance := 2.0
	f.hub.stateStore().publish(FleetStateMessage{
		Host: "mac-personal", Kind: fleetStateKindQuota, Key: ccMarkAccountA,
		MeasuredAt: at.Add(time.Minute), ReceivedAt: at.Add(time.Minute),
		Quota: &FleetStateQuota{AccountKey: ccMarkAccountA, Pool: "commandcode", Balance: &balance},
	}, 0)
	attempts := 0
	f.hub.accountCreditDelete = func(ctx context.Context, account string, marked time.Time) error {
		attempts++
		if attempts == 1 {
			return errors.New("store busy")
		}
		return f.hub.inbox.store.deleteAccountCreditMark(ctx, account, marked)
	}
	if marks := marksAt(f.hub, at.Add(2*time.Minute)); len(marks) != 0 {
		t.Fatalf("a lifted mark was served: %+v", marks)
	}
	if rows, err := f.hub.inbox.store.accountCreditMarks(t.Context()); err != nil || len(rows) != 1 {
		t.Fatalf("rows after failed delete = %+v, %v; want the row kept", rows, err)
	}
	// A later reading with no balance must not revive the lift: the pending
	// delete is tracked apart from the active marks.
	f.hub.stateStore().publish(FleetStateMessage{
		Host: "mac-personal", Kind: fleetStateKindQuota, Key: ccMarkAccountA,
		MeasuredAt: at.Add(3 * time.Minute), ReceivedAt: at.Add(3 * time.Minute),
		Quota: &FleetStateQuota{AccountKey: ccMarkAccountA, Pool: "commandcode"},
	}, 0)
	if f.hub.accountCreditMarkHeld(ccMarkAccountA, at.Add(4*time.Minute)) {
		t.Fatal("a balance-less reading revived a lifted mark")
	}
	if marks := marksAt(f.hub, at.Add(4*time.Minute)); len(marks) != 0 {
		t.Fatalf("a balance-less reading served a lifted mark: %+v", marks)
	}
	if rows, err := f.hub.inbox.store.accountCreditMarks(t.Context()); err != nil || len(rows) != 0 {
		t.Fatalf("rows after retried delete = %+v, %v; want none", rows, err)
	}
	if attempts != 2 {
		t.Fatalf("delete attempts = %d, want 2", attempts)
	}
}

// marksAt reads the hub's active marks or fails the test.
func marksAt(h *HubServer, at time.Time) map[string]time.Time {
	marks, err := h.snapshotAccountCreditMarks(at)
	if err != nil {
		panic(err)
	}
	return marks
}

// hk 5138: a configured mark read that fails is an error on the marks route,
// so selection fails closed instead of reading an empty map as "no marks".
func TestAccountCreditMarksEndpointFailsClosed_t5138(t *testing.T) {
	hub, err := NewHubServer(HubServerConfig{
		Tokens:         map[string]string{"operator": fsOperatorToken},
		InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3"),
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hub.Close() })
	hub.accountCreditRead = func(context.Context) (map[string]accountCreditMark, error) {
		return nil, errors.New("store busy")
	}
	if _, err := hub.snapshotAccountCreditMarks(hub.now()); err == nil {
		t.Fatal("a failed load reported success")
	}
	server := httptest.NewServer(hub.Handler())
	defer server.Close()
	if status, body := fsGet(t, server, "/v1/state/account-credit", fsOperator()); status != 503 {
		t.Fatalf("GET account-credit = %d %s, want 503", status, body)
	}
}

package panewire

import (
	"io"
	"log/slog"
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
	marks := second.snapshotAccountCreditMarks(now)
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
	if _, held := second.snapshotAccountCreditMarks(now)[ccMarkAccountA]; !held {
		t.Fatal("a reading with no balance lifted the mark")
	}
	// Below one credit is still an exhausted account: the mark holds.
	publish(balance(0.5), at.Add(40*time.Minute))
	if _, held := second.snapshotAccountCreditMarks(now)[ccMarkAccountA]; !held {
		t.Fatal("a reading below one credit lifted the mark")
	}
	// At least one credit is the fresh report that lifts it.
	publish(balance(1), at.Add(50*time.Minute))
	if marks := second.snapshotAccountCreditMarks(now); len(marks) != 0 {
		t.Fatalf("a reading with credits did not lift the mark: %+v", marks)
	}

	// A new episode holds until the 24 h bound.
	now = at.Add(2 * time.Hour)
	if err := second.markAccountCreditExhausted("commandcode", ccMarkAccountB, now); err != nil {
		t.Fatal(err)
	}
	if _, held := second.snapshotAccountCreditMarks(now)[ccMarkAccountB]; !held {
		t.Fatal("the new mark does not hold")
	}
	if marks := second.snapshotAccountCreditMarks(now.Add(24 * time.Hour)); len(marks) != 0 {
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

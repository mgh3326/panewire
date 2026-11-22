package panewire

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 6082: one live-spawn read per placement selection, and one served by
// indexes. liveSpawnsLiveBranch is the text only a liveSpawns call sends, so a
// statement log counts calls whatever the split into parts.
const liveSpawnsLiveBranch = `status IN ('accepted','bound','dispatching','bound_grant','dispatching_grant') AND NOT (status='accepted'`

func liveSpawnReads(log *stmtLog) int {
	n := 0
	for _, statement := range log.snapshot() {
		if strings.Contains(statement, liveSpawnsLiveBranch) {
			n++
		}
	}
	return n
}

// hubSpawnAppEnv keeps fleet.New off the operator's config, the way the other
// real-App tests do.
func hubSpawnAppEnv(t *testing.T) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("FLEET_HOSTS_CONFIG", "")
	t.Setenv("WRK_HOSTS_CONFIG", "")
	t.Setenv("PANEWIRE_POLICY", "")
}

// A selection asks for the live spawns once per candidate host through three
// readers (pool cap, free-band launches, account occupancy). The App the hub
// builds must send the store one query for all of them.
func TestLiveSpawnsReadOncePerSelection_t6082(t *testing.T) {
	hubSpawnAppEnv(t)
	store, log := recordedStore(t, dialectSQLite)
	h := &HubServer{
		inbox:  &hubInbox{store: store},
		now:    time.Now,
		nodes:  map[string]*hubNodeRecord{},
		jobs:   map[string]*hubJobRecord{},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	const hosts = 5
	for range hosts {
		if _, err := app.LiveSpawnProfiles(t.Context(), "req"); err != nil {
			t.Fatal(err)
		}
		if _, err := app.LivePlacedByAccount(t.Context(), "req"); err != nil {
			t.Fatal(err)
		}
		if _, err := app.LiveAccountJobs(t.Context(), "req"); err != nil {
			t.Fatal(err)
		}
	}
	if got := liveSpawnReads(log); got != 1 {
		t.Fatalf("liveSpawns ran %d times for one selection over %d hosts, want 1", got, hosts)
	}

	// The next selection builds a new App and reads again.
	next, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := next.LiveAccountJobs(t.Context(), "req"); err != nil {
		t.Fatal(err)
	}
	if got := liveSpawnReads(log); got != 2 {
		t.Fatalf("liveSpawns ran %d times for two selections, want 2", got)
	}
}

// A failed read is not kept: the next reader asks again and fails closed.
func TestLiveSnapshotMemoKeepsNoFailure_t6082(t *testing.T) {
	h := asyncSpawnHub(t)
	snapshot := h.memoLiveSnapshot()
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := snapshot(canceled); err == nil {
		t.Fatal("a canceled read succeeded")
	}
	if _, _, err := snapshot(t.Context()); err != nil {
		t.Fatalf("read after a failed one = %v, want it asked again", err)
	}
}

// The seat re-check after the account lock must see what changed since the
// selection read, so it never takes the selection's memo.
func TestRefuseBusyCommandcodeSeatRereads_t6082(t *testing.T) {
	hubSpawnAppEnv(t)
	h := asyncSpawnHub(t)
	h.spawnNewApp = nil
	now := time.Now()
	const acct = "commandcode:5c7a09bd00000098"
	mine := liveOp(t, h, 61, "job-mine", "auto", now)
	weekly := 80.0
	transport := &directSpawnTransport{h: h, task: mine, snapshotPool: commandcodePoolName, snapshotAccountKey: acct, snapshotWeeklyUsed: &weekly}

	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if jobs, err := app.LiveAccountJobs(t.Context(), mine.Request.RequestID); err != nil || len(jobs) != 0 {
		t.Fatalf("selection occupancy = %v %v, want none", jobs, err)
	}
	if err := transport.refuseBusyCommandcodeSeat(t.Context()); err != nil {
		t.Fatalf("refused an idle account: %v", err)
	}

	// Another seat binds to the account while this one waits for its lock.
	other := liveOp(t, h, 62, "job-other", "auto", now)
	node := hubSpawnRequest{RequestID: "d676220b-e763-4af1-aefd-000000000062", Machine: "machine-a"}
	if _, err := h.inbox.store.bindSpawnPlaced(t.Context(), other.Request.RequestID, 1, node, acct, now); err != nil {
		t.Fatal(err)
	}

	// The selection's memo is stale by design; the re-check is not.
	if jobs, _ := app.LiveAccountJobs(t.Context(), mine.Request.RequestID); len(jobs) != 0 {
		t.Fatalf("selection occupancy after the bind = %v, want the memoized none", jobs)
	}
	err = transport.refuseBusyCommandcodeSeat(t.Context())
	var busy *fleet.HubError
	if !errors.As(err, &busy) || !strings.HasPrefix(busy.Kind, accountBusyNearWeeklyLimit) {
		t.Fatalf("re-check after the bind = %v, want %s", err, accountBusyNearWeeklyLimit)
	}
}

// liveSpawnsOracle is liveSpawns as it was before the split: one OR over the
// table. The split must return the same rows.
func liveSpawnsOracle(t *testing.T, s *laneInboxStore, activeJobs []string, settled, now time.Time) []string {
	t.Helper()
	query := `SELECT ` + spawnTaskColumns + ` FROM spawn_operations WHERE (status IN ('accepted','bound','dispatching','bound_grant','dispatching_grant') AND NOT (status='accepted' AND node_id IS NULL AND park_reason<>'' AND next_check_ms>?)) OR status='succeeded' AND (created_ms>=? OR last_run_ms>=?`
	args := []any{now.UnixMilli(), settled.UnixMilli(), settled.UnixMilli()}
	if len(activeJobs) > 0 {
		query += ` OR job IN (` + strings.TrimSuffix(strings.Repeat("?,", len(activeJobs)), ",") + `)`
		for _, id := range activeJobs {
			args = append(args, id)
		}
	}
	rows, err := s.db.QueryContext(t.Context(), s.q(query+`)`), args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		task, err := scanSpawnTask(rows)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, task.Job)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	slices.Sort(ids)
	return ids
}

// Every branch of the live rule, each with a row that the branch alone keeps
// and a near miss it leaves out.
func TestLiveSpawnsSplitMatchesSingleQuery_t6082(t *testing.T) {
	h := asyncSpawnHub(t)
	s := h.inbox.store
	now := time.Now().UTC().Truncate(time.Millisecond)
	settled := now.Add(-spawnLiveSettle)
	old := now.Add(-3 * time.Hour)
	fresh := now.Add(-30 * time.Second)
	type fixture struct {
		job     string
		status  string
		created time.Time
		lastRun time.Time
		nodeID  string // "" leaves node_id NULL
		park    string
		next    time.Time
		want    bool
	}
	fixtures := []fixture{
		{job: "accepted-open", status: "accepted", created: old, want: true},
		{job: "accepted-parked-future", status: "accepted", created: old, park: "quota", next: now.Add(time.Hour)},
		{job: "accepted-parked-due", status: "accepted", created: old, park: "quota", next: now.Add(-time.Minute), want: true},
		{job: "accepted-parked-bound", status: "accepted", created: old, park: "quota", next: now.Add(time.Hour), nodeID: "n-parked", want: true},
		{job: "bound", status: "bound", created: old, nodeID: "n-bound", want: true},
		{job: "dispatching", status: "dispatching", created: old, nodeID: "n-dispatching", want: true},
		{job: "bound-grant", status: "bound_grant", created: old, nodeID: "n-bound-grant", want: true},
		{job: "dispatching-grant", status: "dispatching_grant", created: old, nodeID: "n-dispatching-grant", want: true},
		{job: "reconciling", status: "reconciling", created: old},
		{job: "succeeded-created", status: "succeeded", created: fresh, lastRun: old, want: true},
		{job: "succeeded-last-run", status: "succeeded", created: old, lastRun: fresh, want: true},
		{job: "succeeded-job-listed", status: "succeeded", created: old, lastRun: old, want: true},
		{job: "succeeded-both", status: "succeeded", created: fresh, lastRun: fresh, want: true},
		{job: "succeeded-old", status: "succeeded", created: old, lastRun: old},
		{job: "failed-fresh", status: "failed", created: fresh, lastRun: fresh},
		{job: "cancelled-fresh", status: "cancelled", created: fresh, lastRun: fresh},
		{job: "given-up-listed", status: "given_up", created: old, lastRun: old},
	}
	var want []string
	for i, f := range fixtures {
		task := liveOp(t, h, 700+i, f.job, "auto", f.created)
		var nodeID any
		if f.nodeID != "" {
			nodeID = f.nodeID
		}
		if _, err := s.db.ExecContext(t.Context(), s.q(`UPDATE spawn_operations SET status=?, created_ms=?, last_run_ms=?, node_id=?, park_reason=?, next_check_ms=? WHERE request_id=?`),
			f.status, f.created.UnixMilli(), f.lastRun.UnixMilli(), nodeID, f.park, f.next.UnixMilli(), task.Request.RequestID); err != nil {
			t.Fatal(err)
		}
		if f.want {
			want = append(want, f.job)
		}
	}
	slices.Sort(want)

	for _, active := range [][]string{nil, {"succeeded-job-listed", "failed-fresh", "given-up-listed", "succeeded-both", "no-such-job"}} {
		// Only a succeeded row is kept for its job: the failed and given-up
		// rows the list names stay out.
		wantNow := want
		if active == nil {
			wantNow = slices.DeleteFunc(slices.Clone(want), func(job string) bool { return job == "succeeded-job-listed" })
		}
		oracle := liveSpawnsOracle(t, s, active, settled, now)
		tasks, err := s.liveSpawns(t.Context(), active, settled, now)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, task := range tasks {
			got = append(got, task.Job)
		}
		if len(got) != len(slices.Compact(slices.Sorted(slices.Values(got)))) {
			t.Fatalf("active %v: a row came back twice: %v", active, got)
		}
		slices.Sort(got)
		if !slices.Equal(got, oracle) {
			t.Fatalf("active %v: split = %v, single query = %v", active, got, oracle)
		}
		if !slices.Equal(got, wantNow) {
			t.Fatalf("active %v: live = %v, want %v", active, got, wantNow)
		}
	}
}

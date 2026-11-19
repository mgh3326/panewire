package panewire

import (
	"context"
	"maps"
	"testing"
	"time"

	"github.com/google/uuid"
)

// hk 5300: the quota reconcile reads spawn_operations.replaced_from, not a
// "%-q%" match on the job id, and the consult active set is two indexed reads
// that agree with the old OR query on every state.

// claimSpawnForTest claims one spawn with its own request id and task, so
// rows never share a task role. The job and replacedFrom are the arguments
// under test.
func claimSpawnForTest(t *testing.T, s *laneInboxStore, task, job, replacedFrom string) {
	t.Helper()
	request := durableSpawnRequest()
	request.RequestID = uuid.NewString()
	for i := range request.Args {
		if request.Args[i] == "--task" {
			request.Args[i+1] = task
		}
	}
	if _, _, err := s.claimSpawnFrom(t.Context(), request, job, "parent", time.Now(), replacedFrom); err != nil {
		t.Fatalf("claim %s: %v", job, err)
	}
}

// successorParents maps each listed job to the job it replaces, so a failure
// names jobs, not whole rows.
func successorParents(tasks []hubSpawnTask) map[string]string {
	parents := map[string]string{}
	for _, task := range tasks {
		parents[task.Job] = task.ReplacedFrom
	}
	return parents
}

func TestQuotaSuccessorSpawnsAreSelectedByTheColumn(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		if err := s.ensureSpawnTasks(); err != nil {
			t.Fatal(err)
		}
		claimSpawnForTest(t, s, "101", "mwr-1", "")
		claimSpawnForTest(t, s, "102", "mwr-1-q1", "mwr-1")
		// "-quality" contains "-q", the text the old LIKE matched; it is no
		// replacement, so it must not come back.
		claimSpawnForTest(t, s, "103", "nightly-quality-gate", "")

		got, err := s.quotaSuccessorSpawns(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if parents := successorParents(got); len(parents) != 1 || parents["mwr-1-q1"] != "mwr-1" {
			t.Fatalf("quota successors = %v, want only mwr-1-q1 replacing mwr-1", parents)
		}
		for _, job := range []string{"mwr-1", "nightly-quality-gate"} {
			task, err := s.latestSpawnForJob(t.Context(), job)
			if err != nil || task.ReplacedFrom != "" {
				t.Fatalf("%s: replaced_from = %q, err %v; want empty", job, task.ReplacedFrom, err)
			}
		}
	})
}

// A row an older hub wrote has NULL replaced_from. The start that follows the
// upgrade derives it from the "-q<n>" id, once; a later start leaves it.
func TestSpawnReplacedFromBackfillsOnStartSQLite(t *testing.T) {
	path := freshLaneInboxFile(t)
	first, err := openLaneInboxStoreTimed(t, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.ensureSpawnTasks(); err != nil {
		t.Fatal(err)
	}
	claimSpawnForTest(t, first, "201", "mwr-2-q1", "")
	claimSpawnForTest(t, first, "202", "mwr-2-q1-q2", "")
	claimSpawnForTest(t, first, "203", "nightly-quality-gate", "")
	claimSpawnForTest(t, first, "204", "-q9", "")
	if err := first.close(); err != nil {
		t.Fatal(err)
	}

	second, err := openLaneInboxStoreTimed(t, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.close() })
	if err := second.ensureSpawnTasks(); err != nil {
		t.Fatal(err)
	}
	got, err := second.quotaSuccessorSpawns(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"mwr-2-q1": "mwr-2", "mwr-2-q1-q2": "mwr-2-q1"}
	if parents := successorParents(got); !maps.Equal(parents, want) {
		t.Fatalf("backfilled successors = %v, want %v", parents, want)
	}
	// "-q9" has no parent before its suffix, so it is not a replacement.
	if task, err := second.latestSpawnForJob(t.Context(), "-q9"); err != nil || task.ReplacedFrom != "" {
		t.Fatalf("-q9: replaced_from = %q, err %v; want empty", task.ReplacedFrom, err)
	}
}

// hk 5300 (postgres 0030): the same backfill, run by the migration on a
// database that recorded the file before its rows were set. CI only.
func TestLaneInboxStorePostgresUpgradeBackfillsReplacedFrom_t5300(t *testing.T) {
	config, _ := hubPostgresTestSchema(t)
	first, err := openLaneInboxStorePostgresConfig(context.Background(), config.Copy())
	if err != nil {
		t.Fatal(err)
	}
	claimSpawnForTest(t, first, "301", "mwr-3-q1", "")
	// Roll the schema back to what the hub before 0030 left: the column is
	// NULL and the migration is not recorded.
	for _, statement := range []string{
		`UPDATE spawn_operations SET replaced_from=NULL`,
		`DELETE FROM panewire_schema_migrations WHERE version='0030_explicit_columns'`,
	} {
		if _, err := first.db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	_ = first.close()
	second, err := openLaneInboxStorePostgresConfig(context.Background(), config.Copy())
	if err != nil {
		t.Fatal(err)
	}
	defer second.close()
	got, err := second.quotaSuccessorSpawns(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Job != "mwr-3-q1" || got[0].ReplacedFrom != "mwr-3" {
		t.Fatalf("after the 0030 backfill: %+v, want mwr-3-q1 replacing mwr-3", got)
	}
}

// The consult active set is the old OR query's rows, in the same created
// order, on a fixture with every state, including answered rows both notified
// and not.
func TestActiveConsultsMatchTheOldORQueryOnEveryState(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		if err := s.ensureConsults(); err != nil {
			t.Fatal(err)
		}
		// The created order interleaves the two halves, so the merge is
		// exercised, not just concatenated.
		fixture := []struct {
			state    string
			notified bool
		}{
			{consultQueued, false},
			{consultAnswered, false},
			{consultRunning, false},
			{consultAnswered, true},
			{consultAnswered, false},
			{consultRefused, false},
			{consultQueued, false},
			{consultFailed, false},
			{consultCancelled, false},
			{consultAnswered, true},
			{consultRunning, false},
			{consultAnswered, false},
		}
		base := time.Now().Add(-time.Hour).UTC().Truncate(time.Millisecond)
		for i, row := range fixture {
			c := hubConsult{ID: uuid.NewString(), IdemKey: uuid.NewString(), Task: "42", PR: "r#1", Head: "abc", Requester: "parent", QuestionDoc: "hk:doc/q", State: row.state, Notified: row.notified, Created: base.Add(time.Duration(i) * time.Second), Deadline: base.Add(time.Hour)}
			if _, _, err := s.insertConsult(t.Context(), c); err != nil {
				t.Fatal(err)
			}
		}

		got, err := s.activeConsults(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		want := oldActiveConsultIDs(t, s)
		// Seven fixture rows are active: queued, running, or answered and not
		// notified. Equal empty lists would prove nothing.
		if len(want) != 7 {
			t.Fatalf("the OR query gives %d active rows, the fixture has 7", len(want))
		}
		if len(got) != len(want) {
			t.Fatalf("active consults = %d rows, the OR query gives %d", len(got), len(want))
		}
		for i := range want {
			if got[i].ID != want[i] {
				t.Fatalf("row %d = %s (%s), the OR query gives %s", i, got[i].ID, got[i].State, want[i])
			}
		}
	})
}

// oldActiveConsultIDs runs the statement activeConsults replaced, as the
// oracle for the rows and their order.
func oldActiveConsultIDs(t *testing.T, s *laneInboxStore) []string {
	t.Helper()
	rows, err := s.db.QueryContext(t.Context(), s.q(`SELECT id FROM consult_requests WHERE state IN ('queued','running') OR (state='answered' AND notified=0) ORDER BY created_ms`))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}

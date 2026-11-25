package panewire

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

// hk 6433, PR 888 review: a hub from before the drain columns, run again after
// a rollback, binds rows with blank columns and leaves stale ones behind. The
// backfill runs once, so every start repairs the rows in flight on a node.

// spawnInFlightStatuses are the stored statuses of an attempt in flight on a
// node, named here and not read from the repair so that dropping one from it
// fails a test.
var spawnInFlightStatuses = []string{"bound", "dispatching", "reconciling", "deadline_exceeded", "bound_grant", "dispatching_grant", "reconciling_grant", "deadline_exceeded_grant"}

// setDrainColumns writes the columns as an older hub leaves them.
func setDrainColumns(t *testing.T, s *laneInboxStore, requestID, machine string, exclusive int) {
	t.Helper()
	if _, err := s.db.Exec(s.q(`UPDATE spawn_operations SET bound_machine=?,exclusive=? WHERE request_id=?`), machine, exclusive, requestID); err != nil {
		t.Fatal(err)
	}
}

// nextStart closes s and opens the same store again, after its backfill is
// already recorded.
func nextStart(t *testing.T, open func() *laneInboxStore, s *laneInboxStore) *laneInboxStore {
	t.Helper()
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	next := queueTestStore(t, open)
	if next.spawnBoundBackfilled != nil {
		t.Fatalf("the one-time backfill ran again: %v", next.spawnBoundBackfilled)
	}
	return next
}

func TestSpawnBoundRepairFillsBlankInFlightRow_t6433(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		first := queueTestStore(t, open)
		var rows []preColumnRow
		want := map[string]string{}
		for _, status := range spawnInFlightStatuses {
			rows = append(rows,
				preColumnRow{id: "exclusive-" + status, status: status, machine: "machine-a"},
				preColumnRow{id: "checkout-" + status, status: status, machine: "machine-a", dir: "dir-" + status})
			want["exclusive-"+status], want["checkout-"+status] = `"machine-a",1`, `"machine-a",0`
		}
		seedPreColumnRows(t, first, rows)
		second := nextStart(t, open, first)
		for id, columns := range want {
			if got := drainColumns(t, second, id); got != columns {
				t.Errorf("%s: bound_machine,exclusive = %s, want %s", id, got, columns)
			}
		}
	})
}

func TestSpawnBoundRepairCorrectsStaleInFlightRow_t6433(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		first := queueTestStore(t, open)
		seedPreColumnRows(t, first, []preColumnRow{
			// Bound to machine-a, unbound and bound to machine-b by the older hub.
			{id: "moved", status: "bound", machine: "machine-b"},
			{id: "marked-checkout", status: "bound", machine: "machine-a", dir: "dir"},
			{id: "unmarked-exclusive", status: "bound_grant", machine: "machine-c"},
		})
		setDrainColumns(t, first, "moved", "machine-a", 1)
		setDrainColumns(t, first, "marked-checkout", "machine-a", 1)
		setDrainColumns(t, first, "unmarked-exclusive", "machine-c", 0)
		second := nextStart(t, open, first)
		for id, columns := range map[string]string{"moved": `"machine-b",1`, "marked-checkout": `"machine-a",0`, "unmarked-exclusive": `"machine-c",1`} {
			if got := drainColumns(t, second, id); got != columns {
				t.Errorf("%s: bound_machine,exclusive = %s, want %s", id, got, columns)
			}
		}
		marks, err := second.boundExclusiveSpawns(t.Context(), time.Now())
		if err != nil {
			t.Fatal(err)
		}
		var marked []string
		for _, row := range marks {
			marked = append(marked, row.machine+"="+row.requestID)
		}
		slices.Sort(marked)
		if !slices.Equal(marked, []string{"machine-b=moved", "machine-c=unmarked-exclusive"}) {
			t.Fatalf("marks after the repair = %v", marked)
		}
	})
}

// A finished operation, an ended attempt waiting for a replay and an unbound
// row are not in flight: the repair leaves them, blank or stale.
func TestSpawnBoundRepairLeavesTerminalRow_t6433(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		first := queueTestStore(t, open)
		statuses := []string{"succeeded", "failed", "refused", "given_up", "cancelled", "retryable", "accepted"}
		var rows []preColumnRow
		for _, status := range statuses {
			rows = append(rows,
				preColumnRow{id: "blank-" + status, status: status, machine: "machine-a"},
				preColumnRow{id: "stale-" + status, status: status, machine: "machine-a"})
		}
		seedPreColumnRows(t, first, rows)
		for _, status := range statuses {
			setDrainColumns(t, first, "stale-"+status, "machine-old", 0)
		}
		second := nextStart(t, open, first)
		for _, status := range statuses {
			for id, columns := range map[string]string{"blank-" + status: `"",0`, "stale-" + status: `"machine-old",0`} {
				if got := drainColumns(t, second, id); got != columns {
					t.Errorf("%s: bound_machine,exclusive = %s, want %s", id, got, columns)
				}
			}
		}
	})
}

// The repair reads in batches, rewrites each differing row once, and a later
// start with nothing differing writes no row.
func TestSpawnBoundRepairSecondStartWritesNothing_t6433(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		first := queueTestStore(t, open)
		var rows []preColumnRow
		for i := range 1200 {
			row := preColumnRow{id: fmt.Sprintf("flight-%04d", i), status: spawnInFlightStatuses[i%len(spawnInFlightStatuses)], machine: "machine-a"}
			if i%4 == 0 {
				row.dir = fmt.Sprintf("dir-%d", i)
			}
			rows = append(rows, row)
		}
		seedPreColumnRows(t, first, rows)
		second := nextStart(t, open, first)
		if !slices.Equal(second.spawnBoundRepairBatches, []int{500, 500, 200}) || second.spawnBoundRepaired != 1200 {
			t.Fatalf("first repair read %v and wrote %d, want [500 500 200] and 1200", second.spawnBoundRepairBatches, second.spawnBoundRepaired)
		}
		third := nextStart(t, open, second)
		if !slices.Equal(third.spawnBoundRepairBatches, []int{500, 500, 200}) || third.spawnBoundRepaired != 0 {
			t.Fatalf("second repair read %v and wrote %d, want [500 500 200] and 0", third.spawnBoundRepairBatches, third.spawnBoundRepaired)
		}
	})
}

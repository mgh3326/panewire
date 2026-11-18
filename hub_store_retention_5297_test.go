package panewire

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"
)

// hk 5297: the hub store's retention pass (hub_store_retention.go). Each case
// runs on SQLite and, in CI, on Postgres through the backend harness.

const retentionDay = 24 * time.Hour

// retentionOp inserts a spawn operation row in the given status, terminal at
// terminalAt (zero for a live row). A zero notice makes a failed row unposted.
func retentionOp(t *testing.T, s *laneInboxStore, id, status string, terminalAt time.Time, notice int64) {
	t.Helper()
	var terminalMS int64
	if !terminalAt.IsZero() {
		terminalMS = terminalAt.UnixMilli()
	}
	if _, err := s.db.Exec(s.q(`INSERT INTO spawn_operations(request_id,job,owner,fingerprint,payload,status,created_ms,deadline_ms,terminal_ms,task_fail_notice_ms) VALUES(?,?,?,?,?,?,?,?,?,?)`), id, "job-"+id, "parent", "fp-"+id, "{}", status, 1, 1, terminalMS, notice); err != nil {
		t.Fatal(err)
	}
}

// retentionCount counts rows of table whose key column equals key.
func retentionCount(t *testing.T, s *laneInboxStore, table, column, key string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(s.q(`SELECT count(*) FROM `+table+` WHERE `+column+`=?`), key).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestHubStoreRetentionDeletesOnlyTerminalPastWindow_t5297(t *testing.T) {
	for _, backend := range laneInboxBackends() {
		t.Run(backend.name, func(t *testing.T) {
			s := retentionStore(t, backend)
			now := time.Now().UTC()
			old := now.Add(-31 * retentionDay)
			retentionOp(t, s, "old-succeeded", "succeeded", old, 1)
			retentionOp(t, s, "recent-succeeded", "succeeded", now.Add(-2*retentionDay), 1)
			retentionOp(t, s, "old-accepted", "accepted", time.Time{}, 1)
			// A live status with a stale terminal stamp is still live.
			retentionOp(t, s, "old-dispatching", "dispatching", old, 1)
			retentionOp(t, s, "old-failed-unposted", "failed", old, 0)
			retentionOp(t, s, "old-failed-posted", "failed", old, 1)

			counts, err := s.pruneHubStore(now, 100)
			if err != nil {
				t.Fatal(err)
			}
			if counts.SpawnOperations != 2 {
				t.Fatalf("deleted %d operations, want 2 (old-succeeded, old-failed-posted)", counts.SpawnOperations)
			}
			for id, want := range map[string]int{"old-succeeded": 0, "old-failed-posted": 0, "recent-succeeded": 1, "old-accepted": 1, "old-dispatching": 1, "old-failed-unposted": 1} {
				if got := retentionCount(t, s, "spawn_operations", "request_id", id); got != want {
					t.Fatalf("%s: rows=%d want %d", id, got, want)
				}
			}
		})
	}
}

func TestHubStoreRetentionFencesFollowTheirOperation_t5297(t *testing.T) {
	for _, backend := range laneInboxBackends() {
		t.Run(backend.name, func(t *testing.T) {
			s := retentionStore(t, backend)
			now := time.Now().UTC()
			for _, row := range []struct {
				request, node, status string
				terminalAt            time.Time
			}{
				{"f-old", "node-old", "given_up", now.Add(-2 * retentionDay)},
				{"f-recent", "node-recent", "given_up", now.Add(-time.Hour)},
				{"f-live", "node-live", "dispatching_grant", time.Time{}},
			} {
				retentionOp(t, s, row.request, row.status, row.terminalAt, 1)
				if _, err := s.db.Exec(s.q(`UPDATE spawn_operations SET node_id=? WHERE request_id=?`), row.node, row.request); err != nil {
					t.Fatal(err)
				}
				if _, err := s.db.Exec(s.q(`INSERT INTO spawn_attempt_grants(node_id,request_id,state,decided_ms) VALUES(?,?,'granted',1)`), row.node, row.request); err != nil {
					t.Fatal(err)
				}
				if _, err := s.db.Exec(s.q(`INSERT INTO spawn_attempt_start_by(node_id,start_by_ms) VALUES(?,1)`), row.node); err != nil {
					t.Fatal(err)
				}
			}
			counts, err := s.pruneHubStore(now, 100)
			if err != nil {
				t.Fatal(err)
			}
			if counts.AttemptGrants != 1 || counts.AttemptStartBy != 1 {
				t.Fatalf("grants=%d start_by=%d, want 1 and 1 (only node-old)", counts.AttemptGrants, counts.AttemptStartBy)
			}
			if retentionCount(t, s, "spawn_attempt_grants", "node_id", "node-old") != 0 || retentionCount(t, s, "spawn_attempt_start_by", "node_id", "node-old") != 0 {
				t.Fatal("a fence of a terminal-for-a-day operation survived")
			}
			for _, node := range []string{"node-recent", "node-live"} {
				if retentionCount(t, s, "spawn_attempt_grants", "node_id", node) != 1 || retentionCount(t, s, "spawn_attempt_start_by", "node_id", node) != 1 {
					t.Fatalf("%s: a fence of a recent or live attempt was deleted", node)
				}
			}
		})
	}
}

func TestHubStoreRetentionKeepsRowsStillPointedAt_t5297(t *testing.T) {
	for _, backend := range laneInboxBackends() {
		t.Run(backend.name, func(t *testing.T) {
			s := retentionStore(t, backend)
			now := time.Now().UTC()
			old := now.Add(-31 * retentionDay)
			// Each operation is past its window and terminal, but one row
			// still names it live: an undelivered notification, an
			// unrecorded placement snapshot, a queued consult, a live pool
			// action of its job. Only the unreferenced one goes.
			for _, id := range []string{"op-notice", "op-snapshot", "op-consult", "op-pool", "op-free"} {
				retentionOp(t, s, id, "succeeded", old, 1)
			}
			exec := func(query string, args ...any) {
				t.Helper()
				if _, err := s.db.Exec(s.q(query), args...); err != nil {
					t.Fatal(err)
				}
			}
			exec(`INSERT INTO spawn_notifications(event_id,request_id,job,owner,result,created_ms,delivered,telemetry_recorded) VALUES(?,?,?,?,?,?,0,0)`, "ev-1", "op-notice", "job-op-notice", "parent", "{}", 1)
			exec(`INSERT INTO spawn_placement_snapshots(request_id,attempt,payload,recorded) VALUES(?,?,?,0)`, "op-snapshot", 1, "{}")
			exec(`INSERT INTO consult_requests(id,idem_key,task,requester,question_doc,state,request_id,created_ms,updated_ms,deadline_ms) VALUES(?,?,?,?,?,'queued',?,1,1,1)`, "c-live", "k-live", "t", "r", "q", "op-consult")
			exec(`INSERT INTO pool_actions(id,job,node,epoch,phase,data,final_ms) VALUES(?,?,?,?,'pending',?,0)`, "pa-live", "job-op-pool", "node-a", 1, "{}")
			// A consult final long ago goes with no operation.
			exec(`INSERT INTO consult_requests(id,idem_key,task,requester,question_doc,state,created_ms,updated_ms,deadline_ms,notified) VALUES(?,?,?,?,?,'answered',1,1,1,1)`, "c-final", "k-final", "t", "r", "q")

			counts, err := s.pruneHubStore(now, 100)
			if err != nil {
				t.Fatal(err)
			}
			if counts.SpawnOperations != 1 || counts.Consults != 1 {
				t.Fatalf("operations=%d consults=%d, want 1 and 1", counts.SpawnOperations, counts.Consults)
			}
			for _, id := range []string{"op-notice", "op-snapshot", "op-consult", "op-pool"} {
				if retentionCount(t, s, "spawn_operations", "request_id", id) != 1 {
					t.Fatalf("%s was deleted while a row still points at it", id)
				}
			}
			if retentionCount(t, s, "spawn_operations", "request_id", "op-free") != 0 {
				t.Fatal("unreferenced terminal operation past its window survived")
			}
			if retentionCount(t, s, "consult_requests", "id", "c-live") != 1 || retentionCount(t, s, "consult_requests", "id", "c-final") != 0 {
				t.Fatal("consult retention ignored its state")
			}
		})
	}
}

// The dedupe fence: a final pool action is kept while its job has a spawn row
// still in progress, however old it is (hasPoolAction and supersedeActionsOf
// read it). Only a final row past its window, with no live spawn row of its
// job, goes.
func TestHubStoreRetentionPoolActionDedupeFence_t5297(t *testing.T) {
	for _, backend := range laneInboxBackends() {
		t.Run(backend.name, func(t *testing.T) {
			s := retentionStore(t, backend)
			now := time.Now().UTC()
			old := now.Add(-31 * retentionDay)
			pool := func(id, job string, finalAt time.Time) {
				t.Helper()
				if _, err := s.db.Exec(s.q(`INSERT INTO pool_actions(id,job,node,epoch,phase,data,final_ms) VALUES(?,?,?,?,'done',?,?)`), id, job, "node-a", 1, "{}", finalAt.UnixMilli()); err != nil {
					t.Fatal(err)
				}
			}
			pool("pa-fenced", "job-live", old)
			pool("pa-free", "job-done", old)
			pool("pa-recent", "job-done", now.Add(-2*retentionDay))
			// retentionOp names the job "job-"+id, so these ids give the jobs above.
			retentionOp(t, s, "live", "accepted", time.Time{}, 1)
			retentionOp(t, s, "done", "succeeded", old, 1)

			counts, err := s.pruneHubStore(now, 100)
			if err != nil {
				t.Fatal(err)
			}
			if counts.PoolActions != 1 {
				t.Fatalf("deleted %d pool actions, want 1 (pa-free)", counts.PoolActions)
			}
			if retentionCount(t, s, "pool_actions", "id", "pa-fenced") != 1 {
				t.Fatal("final pool action of a job with a live spawn row was deleted; the dedupe fence is gone")
			}
			if retentionCount(t, s, "pool_actions", "id", "pa-free") != 0 || retentionCount(t, s, "pool_actions", "id", "pa-recent") != 1 {
				t.Fatal("pool action retention ignored its window")
			}
		})
	}
}

func TestHubStoreRetentionSentBatchesFollowTheirItems_t5297(t *testing.T) {
	for _, backend := range laneInboxBackends() {
		t.Run(backend.name, func(t *testing.T) {
			s := retentionStore(t, backend)
			now := time.Now().UTC()
			exec := func(query string, args ...any) {
				t.Helper()
				if _, err := s.db.Exec(s.q(query), args...); err != nil {
					t.Fatal(err)
				}
			}
			exec(`INSERT INTO lane_inbox_batches(id,lane,text,sent) VALUES('b-free','lane-a','x',1)`)
			exec(`INSERT INTO lane_inbox_batch_quarantine(batch_id,reason) VALUES('b-free','external scheduler owns lane digest')`)
			exec(`INSERT INTO lane_inbox_batches(id,lane,text,sent) VALUES('b-named','lane-a','x',1)`)
			exec(`INSERT INTO lane_inbox_batches(id,lane,text,sent) VALUES('b-open','lane-a','x',0)`)
			exec(`INSERT INTO lane_inbox_batch_quarantine(batch_id,reason) VALUES('b-open','inbox-only lane: items filed unread')`)
			item, err := s.put("item-named", "lane-a", "lane.event", "inbox", quietEvent("item-named", "named"), now.Add(-31*retentionDay), 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			exec(`UPDATE lane_inbox SET escalation_batch='b-named' WHERE id=?`, item.ID)

			counts, err := s.pruneHubStore(now, 100)
			if err != nil {
				t.Fatal(err)
			}
			if counts.SentBatches != 1 || counts.Quarantine != 1 {
				t.Fatalf("batches=%d quarantine=%d, want 1 and 1 (b-free)", counts.SentBatches, counts.Quarantine)
			}
			if retentionCount(t, s, "lane_inbox_batches", "id", "b-free") != 0 || retentionCount(t, s, "lane_inbox_batch_quarantine", "batch_id", "b-free") != 0 {
				t.Fatal("unreferenced sent batch and its quarantine row should be gone")
			}
			if retentionCount(t, s, "lane_inbox_batches", "id", "b-named") != 1 {
				t.Fatal("a sent batch an inbox item still names was pruned")
			}
			if retentionCount(t, s, "lane_inbox_batches", "id", "b-open") != 1 || retentionCount(t, s, "lane_inbox_batch_quarantine", "batch_id", "b-open") != 1 {
				t.Fatal("an unsent batch or its quarantine row was pruned")
			}
		})
	}
}

// Each table deletes at most the bound in one pass; repeated passes drain it.
func TestHubStoreRetentionBoundsEachPass_t5297(t *testing.T) {
	for _, backend := range laneInboxBackends() {
		t.Run(backend.name, func(t *testing.T) {
			s := retentionStore(t, backend)
			now := time.Now().UTC()
			for _, id := range []string{"b1", "b2", "b3", "b4", "b5"} {
				if _, err := s.db.Exec(s.q(`INSERT INTO lane_inbox_batches(id,lane,text,sent) VALUES(?,'lane-a','x',1)`), id); err != nil {
					t.Fatal(err)
				}
				retentionOp(t, s, "op-"+id, "succeeded", now.Add(-31*retentionDay), 1)
			}
			for pass, want := range []int64{2, 2, 1, 0} {
				counts, err := s.pruneHubStore(now, 2)
				if err != nil {
					t.Fatal(err)
				}
				if counts.SentBatches != want || counts.SpawnOperations != want {
					t.Fatalf("pass %d: batches=%d operations=%d, want %d each", pass, counts.SentBatches, counts.SpawnOperations, want)
				}
			}
		})
	}
}

// A spawn operation that reaches a terminal status stamps its retention clock
// through the real write path; a retryable one does not.
func TestSpawnTerminalStampsRetentionClock_t5297(t *testing.T) {
	h := asyncSpawnHub(t)
	now := time.Now().UTC().Truncate(time.Second)
	task, _, err := h.inbox.store.claimSpawn(t.Context(), durableSpawnRequest(), "test-job", "parent", now)
	if err != nil {
		t.Fatal(err)
	}
	var terminalMS int64
	if err := h.inbox.store.db.QueryRow(`SELECT terminal_ms FROM spawn_operations WHERE request_id=?`, task.Request.RequestID).Scan(&terminalMS); err != nil || terminalMS != 0 {
		t.Fatalf("a queued operation is stamped terminal: %d, %v", terminalMS, err)
	}
	if _, err := h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "landing"}, now); err != nil {
		t.Fatal(err)
	}
	if err := h.inbox.store.db.QueryRow(`SELECT terminal_ms FROM spawn_operations WHERE request_id=?`, task.Request.RequestID).Scan(&terminalMS); err != nil || terminalMS != now.UnixMilli() {
		t.Fatalf("terminal_ms=%d want %d (%v)", terminalMS, now.UnixMilli(), err)
	}
}

// The Postgres migration drops quota_refresh_failures, adds the two clocks and
// keeps every existing final row. It runs in CI with the Postgres backend.
func TestHubStoreRetentionPostgresMigrationAddsClocks_t5297(t *testing.T) {
	config, _ := hubPostgresTestSchema(t)
	s, err := openLaneInboxStorePostgresConfig(context.Background(), config.Copy())
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	state := hubPostgresSchemaState(t, s)
	if slices.ContainsFunc(state, func(line string) bool { return strings.Contains(line, "quota_refresh_failures") }) {
		t.Fatalf("quota_refresh_failures survived the migration: %v", state)
	}
	for _, want := range []string{"column spawn_operations.terminal_ms ", "column pool_actions.final_ms "} {
		if !slices.ContainsFunc(state, func(line string) bool { return strings.HasPrefix(line, want) }) {
			t.Fatalf("schema lacks %q: %v", want, state)
		}
	}
}

// retentionStore is a fresh backend store with the spawn tables the hub creates
// at start (ensureSpawnTasks is a no-op on Postgres, whose migrations create them).
func retentionStore(t *testing.T, backend laneInboxBackend) *laneInboxStore {
	t.Helper()
	s := backend.fresh(t)()
	if err := s.ensureSpawnTasks(); err != nil {
		t.Fatal(err)
	}
	if err := s.ensureConsults(); err != nil {
		t.Fatal(err)
	}
	return s
}

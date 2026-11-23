package panewire

import (
	"fmt"
	"testing"
	"time"
)

// hk 6084 acceptance tests:
//  1. An operation that turns retryable leaves no spawn_queue row; a pre-existing
//     retryable row is removed by the maintenance pass; accepted, waiting, and
//     deadline_exceeded* rows are untouched.
//  2. 1000+ undeletable terminal grant rows older than a deletable one -> the
//     deletable one is pruned in a bounded number of passes.
func TestSpawnQueueRetryable_t6084(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := queueTestStore(t, open)
		now := time.Now().UTC()

		// 1. An operation that turns retryable leaves no spawn_queue row.
		task := queueTestClaim(t, s, 101, now)
		if _, ok := queueRows(t, s)[task.Request.RequestID]; !ok {
			t.Fatalf("expected claimed request %s in spawn_queue", task.Request.RequestID)
		}
		refusal := hubSpawnOutcome{
			RC:        81,
			Stage:     "checkout",
			Reason:    "checkout_refused:dirty",
			Retryable: true,
			Host:      "machine-a",
		}
		if ok, err := s.completeSpawn(t.Context(), task, refusal, now); err != nil || !ok {
			t.Fatalf("completeSpawn: ok=%v err=%v", ok, err)
		}
		saved, err := s.spawnTask(t.Context(), task.Request.RequestID)
		if err != nil || saved.Status != "retryable" {
			t.Fatalf("saved status = %q, want retryable", saved.Status)
		}
		if _, ok := queueRows(t, s)[task.Request.RequestID]; ok {
			t.Fatalf("retryable operation %s must not remain in spawn_queue", task.Request.RequestID)
		}

		// Replaying the retryable operation via resumeCleanedSpawn re-enqueues it.
		replayed, err := s.resumeCleanedSpawn(t.Context(), saved, now.Add(time.Minute))
		if err != nil || replayed.Status != "accepted" {
			t.Fatalf("resumeCleanedSpawn: %v %v", replayed.Status, err)
		}
		if _, ok := queueRows(t, s)[task.Request.RequestID]; !ok {
			t.Fatalf("replayed operation %s must be re-enqueued into spawn_queue", task.Request.RequestID)
		}

		// 2. Pre-existing retryable row is removed by the maintenance pass;
		// accepted/waiting/deadline_exceeded* rows are untouched.
		reqRetryable := "req-preexisting-retryable"
		retentionOp(t, s, reqRetryable, "retryable", time.Time{}, 0)
		if _, err := s.db.Exec(s.q(s.dialect.insertIgnore(`INSERT INTO spawn_queue(request_id,next_check_ms) VALUES(?,0)`, "")), reqRetryable); err != nil {
			t.Fatal(err)
		}

		// Insert active operations that MUST remain in spawn_queue
		reqAccepted := queueTestClaim(t, s, 102, now).Request.RequestID
		reqWaiting := queueTestWaiting(t, s, 103, now).Request.RequestID

		reqDeadline := "req-active-deadline-exceeded"
		retentionOp(t, s, reqDeadline, "deadline_exceeded", time.Time{}, 0)
		if _, err := s.db.Exec(s.q(s.dialect.insertIgnore(`INSERT INTO spawn_queue(request_id,next_check_ms) VALUES(?,0)`, "")), reqDeadline); err != nil {
			t.Fatal(err)
		}

		reqDeadlineGrant := "req-active-deadline-exceeded-grant"
		retentionOp(t, s, reqDeadlineGrant, "deadline_exceeded_grant", time.Time{}, 0)
		if _, err := s.db.Exec(s.q(s.dialect.insertIgnore(`INSERT INTO spawn_queue(request_id,next_check_ms) VALUES(?,0)`, "")), reqDeadlineGrant); err != nil {
			t.Fatal(err)
		}

		// Run the existing queue maintenance pass
		if err := s.reconcileSpawnQueue(t.Context()); err != nil {
			t.Fatalf("reconcileSpawnQueue: %v", err)
		}

		rows := queueRows(t, s)
		if _, ok := rows[reqRetryable]; ok {
			t.Fatalf("pre-existing retryable row %s should have been removed from spawn_queue", reqRetryable)
		}
		for _, kept := range []string{reqAccepted, reqWaiting, reqDeadline, reqDeadlineGrant} {
			if _, ok := rows[kept]; !ok {
				t.Fatalf("active row %s should have been kept in spawn_queue", kept)
			}
		}
	})
}

func TestSpawnGrantRetentionAdvances_t6084(t *testing.T) {
	for _, backend := range laneInboxBackends() {
		t.Run(backend.name, func(t *testing.T) {
			s := retentionStore(t, backend)
			now := time.Now().UTC()
			old := now.Add(-3 * retentionDay)
			deletableAt := now.Add(-2 * retentionDay)

			// 1000+ older terminal rows whose grants cannot be deleted
			// (e.g. they don't exist in spawn_attempt_grants / already deleted).
			const olderUndeletableCount = 1005
			for i := 0; i < olderUndeletableCount; i++ {
				reqID := fmt.Sprintf("req-old-terminal-%d", i)
				nodeID := fmt.Sprintf("node-old-terminal-%d", i)
				retentionOp(t, s, reqID, "succeeded", old, 1)
				if _, err := s.db.Exec(s.q(`UPDATE spawn_operations SET node_id=? WHERE request_id=?`), nodeID, reqID); err != nil {
					t.Fatal(err)
				}
			}

			// One newer deletable row that DOES have a grant row.
			deletableReq := "req-deletable"
			deletableNode := "node-deletable"
			retentionOp(t, s, deletableReq, "succeeded", deletableAt, 1)
			if _, err := s.db.Exec(s.q(`UPDATE spawn_operations SET node_id=? WHERE request_id=?`), deletableNode, deletableReq); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec(s.q(`INSERT INTO spawn_attempt_grants(node_id,request_id,state,decided_ms) VALUES(?,?,'granted',1)`), deletableNode, deletableReq); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec(s.q(`INSERT INTO spawn_attempt_start_by(node_id,start_by_ms) VALUES(?,1)`), deletableNode); err != nil {
				t.Fatal(err)
			}

			// In a bounded number of passes (at most 2), the deletable grant must be pruned.
			// On base, it would never be pruned because the 1000 older rows block it forever.
			pruned := false
			for pass := 0; pass < 2; pass++ {
				counts, err := s.pruneHubStore(now, 1000)
				if err != nil {
					t.Fatal(err)
				}
				if counts.AttemptGrants > 0 && retentionCount(t, s, "spawn_attempt_grants", "node_id", deletableNode) == 0 {
					pruned = true
					break
				}
			}

			if !pruned {
				t.Fatalf("deletable grant %s was not pruned because older undeletable rows blocked retention", deletableNode)
			}
			if retentionCount(t, s, "spawn_attempt_grants", "node_id", deletableNode) != 0 {
				t.Fatalf("deletable grant %s still exists in spawn_attempt_grants", deletableNode)
			}
			if retentionCount(t, s, "spawn_attempt_start_by", "node_id", deletableNode) != 0 {
				t.Fatalf("deletable start_by %s still exists in spawn_attempt_start_by", deletableNode)
			}
		})
	}
}

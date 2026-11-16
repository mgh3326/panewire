-- Indexes for the hub's periodic scans (hk 4889). Each statement in
-- docs/hub-postgres.md, "Scan cost", reads a predicate no index
-- matched, so Postgres read the whole table instead: 942,868 full scans of
-- spawn_operations and 834,310 of spawn_notifications over 12 days live, and a
-- full lane_inbox pass per pending batch, every 100 ms.
--
-- These are plain CREATE INDEX statements, not CONCURRENTLY: the migration
-- applies inside one transaction under the advisory lock. spawn_operations and
-- spawn_notifications hold thousands of rows and lane_inbox the retained
-- week's items, so each build is well under a second of work; the lock is
-- held no longer than the hub's ordinary start.
--
-- Every index is additive. An older hub neither reads nor writes them and its
-- statements keep returning the same rows. IF NOT EXISTS makes a re-run, or a
-- start racing another hub's migration, a no-op.

-- The batch page (lane_inbox_store.go, pendingBatchesForContext) counts the
-- items of each pending batch by digest_batch and escalation_batch. Both
-- columns are empty on every item no live batch claims, so the indexes stay
-- partial; the statements repeat the <>'' predicate so the planner may use
-- them.
CREATE INDEX IF NOT EXISTS lane_inbox_digest_batch ON lane_inbox(digest_batch) WHERE digest_batch<>'';
CREATE INDEX IF NOT EXISTS lane_inbox_escalation_batch ON lane_inbox(escalation_batch) WHERE escalation_batch<>'';

-- The notification flush reads one delivery state's rows, oldest first, every
-- coordinator tick (spawnNoticePending, spawnNoticeHeld, spawnNoticePublishing).
CREATE INDEX IF NOT EXISTS spawn_notifications_delivered ON spawn_notifications(delivered,created_ms);

-- The plain status index serves the small sets the coordinator asks for by
-- status alone: the waiting page, the stale page, both with a sort of the rows
-- that status holds, and the rollback drain's nonterminal count (index-only, so
-- it reads no heap rows).
CREATE INDEX IF NOT EXISTS spawn_operations_status ON spawn_operations(status);

-- The coordinator's queue scan reads the seven nonterminal statuses in
-- (last_run_ms,created_ms,request_id) order, 32 rows at a time; the partial
-- index is exactly that set, so the scan stops at the LIMIT.
CREATE INDEX IF NOT EXISTS spawn_operations_queue ON spawn_operations(last_run_ms,created_ms,request_id) WHERE status IN ('accepted','bound','dispatching','reconciling','bound_grant','dispatching_grant','reconciling_grant');

-- Parked requests: the wake pass walks due next checks in order, and the
-- owner-notice sweep walks waiting park_since_ms in order.
CREATE INDEX IF NOT EXISTS spawn_operations_parked_wake ON spawn_operations(next_check_ms,request_id) WHERE status='accepted' AND node_id IS NULL;
CREATE INDEX IF NOT EXISTS spawn_operations_park_notice ON spawn_operations(park_since_ms,request_id) WHERE status='accepted' AND node_id IS NULL AND park_reason<>'';

-- Restoring the placement counts at start reads bound, unclaimed operations in
-- placement order.
CREATE INDEX IF NOT EXISTS spawn_operations_bound_unclaimed ON spawn_operations(placed_ms) WHERE node_id IS NOT NULL AND claimed_ms=0;

-- The reviewer-end pass pages succeeded operations newest first, and the live
-- spawn count reads succeeded ones by creation, by last run, or by job id: one
-- index per disjunct, so the planner builds a bitmap OR over them.
CREATE INDEX IF NOT EXISTS spawn_operations_succeeded ON spawn_operations(created_ms DESC,request_id DESC) WHERE status='succeeded';
CREATE INDEX IF NOT EXISTS spawn_operations_succeeded_run ON spawn_operations(last_run_ms) WHERE status='succeeded';

-- A job's operation rows are read newest first by job id (the claim marker and
-- the status API), and the live count names active jobs by id.
CREATE INDEX IF NOT EXISTS spawn_operations_job ON spawn_operations(job);

-- The spawn queue and the task-failure notice outbox (hk 5299, phase 0 of
-- hk 4898). spawn_queue holds one row per non-terminal spawn operation, so
-- the coordinator's scans read the work still to do and not the whole of
-- spawn_operations; the row is inserted with the operation and deleted in the
-- transaction that ends it. next_check_ms mirrors the operation's own parked
-- wake time. task_fail_notice_outbox holds one row per failed or refused
-- operation whose hk task comment (hk 5137) is still owed; retry_ms is its
-- next attempt. spawn_operations keeps every row and column, and
-- task_fail_notice_ms stays on it as the record that the sweep judged it.
-- Additive only: an older hub neither reads nor writes these tables.
CREATE TABLE IF NOT EXISTS spawn_queue (request_id TEXT PRIMARY KEY, next_check_ms BIGINT NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS spawn_queue_next_check ON spawn_queue(next_check_ms, request_id);
CREATE TABLE IF NOT EXISTS task_fail_notice_outbox (request_id TEXT PRIMARY KEY, retry_ms BIGINT NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS task_fail_notice_outbox_retry ON task_fail_notice_outbox(retry_ms, request_id);
-- Backfill, once per database (migrations are versioned; each copy is also
-- idempotent). Every operation that is not terminal goes in the queue.
INSERT INTO spawn_queue(request_id, next_check_ms) SELECT request_id, next_check_ms FROM spawn_operations WHERE status NOT IN ('succeeded','failed','refused','given_up','cancelled') ON CONFLICT DO NOTHING;
-- Every failed or refused operation still owed its comment goes in the
-- outbox, keeping its retry delay. Migration 0023 stamped the failures that
-- predate the mark as judged, so their mark is set and they are not put back.
INSERT INTO task_fail_notice_outbox(request_id, retry_ms) SELECT request_id, task_fail_retry_ms FROM spawn_operations WHERE status IN ('failed','refused') AND task_fail_notice_ms = 0 ON CONFLICT DO NOTHING;

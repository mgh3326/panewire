-- hk 5250: the durable handoffkeep operator wait behind a told one-time
-- action need (hub_one_time_wait.go). wait_task_id and wait_id name the
-- intake anchor task and the open operator wait; both stay 0 until the wait
-- write is confirmed. The row is deleted only after the wait is closed, so
-- a restart finishes either direction. confirmed_ms marks the moment
-- anti-flap clearing confirmed the need gone: the row then only carries the
-- wait-close obligation, and a sighting of the need again starts a new
-- episode. The SQLite twin is oneTimeActionSchema plus the addTableColumn
-- calls in openLaneInboxStore.
ALTER TABLE one_time_action_notices
    ADD COLUMN wait_task_id BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN wait_id BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN confirmed_ms BIGINT NOT NULL DEFAULT 0;

-- hk 5250: the handoffkeep operator wait behind a one-time action need
-- (hub_one_time_wait.go). wait_task_id is the anchor written ahead of the wait
-- write; wait_id is that wait once hk answers; anchor_ref is where the anchor
-- walk last reached; confirmed_ms marks the moment anti-flap clearing confirmed
-- the need gone.
-- applied-if: SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='one_time_action_notices' AND column_name='wait_id'
ALTER TABLE one_time_action_notices
 ADD COLUMN wait_task_id BIGINT NOT NULL DEFAULT 0,
 ADD COLUMN wait_id BIGINT NOT NULL DEFAULT 0,
 ADD COLUMN confirmed_ms BIGINT NOT NULL DEFAULT 0,
 ADD COLUMN anchor_ref TEXT NOT NULL DEFAULT ('');

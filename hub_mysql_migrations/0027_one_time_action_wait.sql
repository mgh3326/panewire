-- hk 5250: the handoffkeep operator wait behind a told one-time action need
-- (hub_one_time_wait.go). wait_task_id and wait_id stay 0 until the wait write
-- is confirmed; confirmed_ms marks the moment anti-flap clearing confirmed the
-- need gone. The row is deleted only after the wait is closed, so a restart
-- finishes either direction.
-- applied-if: SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='one_time_action_notices' AND column_name='wait_id'
ALTER TABLE one_time_action_notices
 ADD COLUMN wait_task_id BIGINT NOT NULL DEFAULT 0,
 ADD COLUMN wait_id BIGINT NOT NULL DEFAULT 0,
 ADD COLUMN confirmed_ms BIGINT NOT NULL DEFAULT 0,
 ADD COLUMN anchor_ref TEXT NOT NULL DEFAULT ('');

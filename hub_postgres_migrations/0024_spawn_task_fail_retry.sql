-- Next attempt for a spawn-failure hk task comment hk would not take
-- (hk 5137). The sweep lists only rows whose delay has passed, so backed-off
-- rows never monopolize the page and starve newer failures; storing it on the
-- operation (rather than hub memory) makes a restart respect the delay
-- instead of retrying at once. 0 is due at once. Additive only: an older hub
-- neither reads nor writes this column and its rows stay exactly as they are.
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS task_fail_retry_ms BIGINT NOT NULL DEFAULT 0;

-- One hk task comment for a spawn that failed with --task (hk 5137).
-- task_fail_notice_ms is when the sweep posted (or judged not owed) the
-- request's spawn-failed comment, so a restart never reposts one and never
-- loses one. Additive only: an older hub neither reads nor writes this column
-- and its terminal rows stay exactly as they are.
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS task_fail_notice_ms BIGINT NOT NULL DEFAULT 0;

-- One hk task comment for a spawn that failed with --task (hk 5137).
-- task_fail_notice_ms is when the sweep judged the request's terminal
-- failure: posted its hk task comment, or found nothing owed. It is the
-- idempotency proof across hub restarts: a failed post keeps it unset and
-- retries, and a hub that stops between the post and the mark reposts once.
-- Additive only: an older hub neither reads nor writes this column and its
-- terminal rows stay exactly as they are.
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS task_fail_notice_ms BIGINT NOT NULL DEFAULT 0;
-- Terminal rows that predate the column never owed a comment: stamp them
-- judged once, so the first sweep after deploy does not repost history. This
-- runs once per database (migrations are versioned). Rows that turn terminal
-- later keep mark 0 and post normally.
UPDATE spawn_operations SET task_fail_notice_ms = (FLOOR(EXTRACT(EPOCH FROM NOW()) * 1000))::BIGINT WHERE task_fail_notice_ms = 0 AND status IN ('failed','refused','deadline_exceeded');

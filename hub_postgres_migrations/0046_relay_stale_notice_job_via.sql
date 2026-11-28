-- hk 7098: the job source of a stale-replay notice row. job_via is
-- 'lane_name' when the row's job is its worker lane name, a spawn of that name
-- having been created before the row arrived (no binding or route named a job);
-- empty otherwise. The SQLite twin is addStaleReplayJobVia; MySQL's twin is
-- migration 0043.
-- Rollback: a hub binary from before this migration neither reads nor writes
-- job_via. A row the newer hub resolved by lane name is then noticed as a plain
-- job, without the marker; no row is lost.
ALTER TABLE relay_stale_notice_rows ADD COLUMN IF NOT EXISTS job_via VARCHAR(16) NOT NULL DEFAULT '';

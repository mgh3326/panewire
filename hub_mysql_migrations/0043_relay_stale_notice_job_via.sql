-- hk 7098: the job source of a stale-replay notice row, the twin of Postgres
-- migration 0046. job_via is 'lane_name' when the row's job is its worker lane
-- name (no binding or route named a job; a spawn of that name created before the
-- row arrived), empty otherwise.
-- Rollback: a hub binary from before this migration neither reads nor writes
-- job_via; its rows read as a plain job, as described in migration 0046.
-- applied-if: SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='relay_stale_notice_rows' AND column_name='job_via'
ALTER TABLE relay_stale_notice_rows ADD COLUMN job_via VARCHAR(16) NOT NULL DEFAULT '';

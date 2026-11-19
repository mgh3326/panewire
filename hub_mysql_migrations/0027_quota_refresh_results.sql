-- One row per finished shared quota refresh attempt, keyed by the scheduler's
-- opaque attempt ID (hk 5456). A request stores the ID it waited on and reads
-- this row, so a completion can only ever answer for its own attempt.
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='quota_refresh_results'
CREATE TABLE quota_refresh_results (
 attempt_id VARCHAR(255) NOT NULL,
 machine VARCHAR(255) NOT NULL,
 pool VARCHAR(255) NOT NULL,
 account_key VARCHAR(255) NOT NULL,
 outcome TEXT NOT NULL,
 seen_ms BIGINT NOT NULL,
 PRIMARY KEY (attempt_id),
 KEY quota_refresh_results_seen (seen_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

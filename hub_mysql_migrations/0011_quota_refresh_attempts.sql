-- Every finished quota refresh attempt, success included (outcome ''), so a
-- timeout that expires while a request waits can tell a failed measurement from
-- one that never ran (hk 4600). quota_refresh_failures is not carried over: no
-- code reads it (phase 0, hk 5297).
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='quota_refresh_attempts'
CREATE TABLE quota_refresh_attempts (
 machine VARCHAR(255) NOT NULL,
 pool VARCHAR(255) NOT NULL,
 outcome TEXT NOT NULL,
 seen_ms BIGINT NOT NULL,
 PRIMARY KEY (machine, pool)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- hk 6924: the daily stale-replay notice, the twin of Postgres migration 0043.
-- One row per real message the replay gate retired as stale; filed=1 once its
-- design lane's notice for the UTC day is in the inbox.
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='relay_stale_notice_rows'
CREATE TABLE relay_stale_notice_rows (
 relay_id BIGINT NOT NULL,
 notice_day VARCHAR(10) NOT NULL,
 design_lane VARCHAR(255) NOT NULL,
 worker_lane VARCHAR(255) NOT NULL,
 job VARCHAR(255) NOT NULL DEFAULT '',
 task_id VARCHAR(32) NOT NULL DEFAULT '',
 kind VARCHAR(64) NOT NULL,
 sender VARCHAR(32) NOT NULL DEFAULT '',
 hk_event_id VARCHAR(255) NOT NULL DEFAULT '',
 received_ms BIGINT NOT NULL,
 retired_ms BIGINT NOT NULL,
 filed INT NOT NULL DEFAULT 0,
 PRIMARY KEY (relay_id),
 KEY relay_stale_notice_rows_day (notice_day, design_lane, filed)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

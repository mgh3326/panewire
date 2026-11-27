-- hk 6924: the lane bindings of the daily stale-replay notice, the twin of the
-- relay_stale_notice_lanes table in Postgres migration 0043. A worker lane is
-- bound to the job its route named at a lane.event receipt, so a row retired
-- after its route is gone still names its job.
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='relay_stale_notice_lanes'
CREATE TABLE relay_stale_notice_lanes (
 worker_lane VARCHAR(255) NOT NULL,
 job VARCHAR(255) NOT NULL,
 first_ms BIGINT NOT NULL,
 PRIMARY KEY (worker_lane, job),
 KEY relay_stale_notice_lanes_first (worker_lane, first_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- The hub's told one-time actions: one row per host, harness kind and need, so a
-- hub restart does not tell the operator again.
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='one_time_action_notices'
CREATE TABLE one_time_action_notices (
 machine VARCHAR(255) NOT NULL,
 kind VARCHAR(64) NOT NULL,
 need VARCHAR(255) NOT NULL,
 told_ms BIGINT NOT NULL,
 gone_ms BIGINT NOT NULL DEFAULT 0,
 PRIMARY KEY (machine, kind, need)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

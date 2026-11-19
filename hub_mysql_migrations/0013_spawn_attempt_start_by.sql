-- The latest start_by sent for each grant-required spawn attempt (hk 4612).
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='spawn_attempt_start_by'
CREATE TABLE spawn_attempt_start_by (
 node_id VARCHAR(255) NOT NULL,
 start_by_ms BIGINT NOT NULL,
 PRIMARY KEY (node_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- The start proof of a grant-required spawn attempt (hk 4612): one row per node
-- request id, state 'granted' or 'fenced', whichever was written first.
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='spawn_attempt_grants'
CREATE TABLE spawn_attempt_grants (
 node_id VARCHAR(255) NOT NULL,
 request_id VARCHAR(255) NOT NULL,
 state VARCHAR(64) NOT NULL,
 decided_ms BIGINT NOT NULL,
 PRIMARY KEY (node_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

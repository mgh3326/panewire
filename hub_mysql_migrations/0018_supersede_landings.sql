-- Durable landing intents of successors that may owe their parent's pane a close
-- (hk 5195).
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='supersede_landings'
CREATE TABLE supersede_landings (
 request_id VARCHAR(255) NOT NULL,
 parent VARCHAR(255) NOT NULL,
 successor VARCHAR(255) NOT NULL,
 owner VARCHAR(255) NOT NULL,
 created_ms BIGINT NOT NULL,
 tried_ms BIGINT NOT NULL DEFAULT 0,
 generation BIGINT NOT NULL,
 state VARCHAR(16) NOT NULL,
 PRIMARY KEY (request_id),
 KEY supersede_landings_pending (state, tried_ms, created_ms),
 CHECK (state IN ('armed','pending','done'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

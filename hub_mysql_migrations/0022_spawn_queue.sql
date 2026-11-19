-- One row per non-terminal spawn operation, so the coordinator's scans read the
-- work still to do (hk 5299). next_check_ms mirrors the operation's parked wake time.
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='spawn_queue'
CREATE TABLE spawn_queue (
 request_id VARCHAR(255) NOT NULL,
 next_check_ms BIGINT NOT NULL DEFAULT 0,
 PRIMARY KEY (request_id),
 KEY spawn_queue_next_check (next_check_ms, request_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

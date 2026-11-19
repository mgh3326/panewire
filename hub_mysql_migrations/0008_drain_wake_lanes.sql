-- Each design lane's owed drain line and last send (hk 4567).
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='drain_wake_lanes'
CREATE TABLE drain_wake_lanes (
 lane VARCHAR(255) NOT NULL,
 pending INT NOT NULL DEFAULT 0,
 last_sent BIGINT NOT NULL DEFAULT 0,
 PRIMARY KEY (lane)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

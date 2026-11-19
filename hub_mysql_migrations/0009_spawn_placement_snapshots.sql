-- The durable placement snapshot each bound attempt leaves so the hk placement
-- record can be built after the placing App is gone (hk 4615). recorded marks the
-- attempt whose record was already produced; delivery holds the settings its
-- record posts and spools through.
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='spawn_placement_snapshots'
CREATE TABLE spawn_placement_snapshots (
 request_id VARCHAR(255) NOT NULL,
 attempt INT NOT NULL,
 payload MEDIUMTEXT NOT NULL,
 recorded INT NOT NULL DEFAULT 0,
 delivery TEXT NOT NULL DEFAULT (''),
 PRIMARY KEY (request_id, attempt),
 KEY spawn_placement_snapshots_unsealed (recorded)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

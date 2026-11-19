-- Digest batches, sent in creation order: seq is the AUTO_INCREMENT column that
-- takes the place of SQLite's rowid.
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='lane_inbox_batches'
CREATE TABLE lane_inbox_batches (
 seq BIGINT NOT NULL AUTO_INCREMENT,
 id VARCHAR(255) NOT NULL,
 lane VARCHAR(255) NOT NULL,
 text MEDIUMTEXT NOT NULL,
 sent TINYINT NOT NULL DEFAULT 0,
 PRIMARY KEY (id),
 UNIQUE KEY lane_inbox_batches_seq (seq),
 KEY lane_inbox_batches_pending (sent, seq)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

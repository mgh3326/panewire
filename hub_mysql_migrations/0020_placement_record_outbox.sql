-- Placement records still owed: written with the snapshot, deleted when it is
-- recorded (hk 5298).
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='placement_record_outbox'
CREATE TABLE placement_record_outbox (
 request_id VARCHAR(255) NOT NULL,
 attempt INT NOT NULL,
 PRIMARY KEY (request_id, attempt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

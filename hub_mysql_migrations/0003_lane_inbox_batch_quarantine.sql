-- A batch the hub refused to deliver, and why.
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='lane_inbox_batch_quarantine'
CREATE TABLE lane_inbox_batch_quarantine (
 batch_id VARCHAR(255) NOT NULL,
 reason TEXT NOT NULL,
 PRIMARY KEY (batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

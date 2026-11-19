-- Spawn notices still owed: written with the notification, deleted when delivered (hk 5298).
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='spawn_notice_outbox'
CREATE TABLE spawn_notice_outbox (
 event_id VARCHAR(768) NOT NULL,
 PRIMARY KEY (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

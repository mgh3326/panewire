-- Spawn notices; spawn_notice_outbox names the ones still owed.
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='spawn_notifications'
CREATE TABLE spawn_notifications (
 event_id VARCHAR(768) NOT NULL,
 request_id VARCHAR(255) NOT NULL,
 job VARCHAR(255) NOT NULL,
 owner VARCHAR(255) NOT NULL,
 result MEDIUMTEXT NOT NULL,
 created_ms BIGINT NOT NULL,
 delivered INT NOT NULL DEFAULT 0,
 telemetry_recorded INT NOT NULL DEFAULT 0,
 PRIMARY KEY (event_id),
 KEY spawn_notifications_delivered (delivered, created_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

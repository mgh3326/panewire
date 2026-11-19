-- The durable owner-notice decision table (hk 5360): one row per decided notice,
-- keyed by its event key; the first decision is the only one. The count rows are
-- the daily spawn_notices group and the repeat window.
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='owner_notice_decisions'
CREATE TABLE owner_notice_decisions (
 event_key VARCHAR(768) NOT NULL,
 lane VARCHAR(255) NOT NULL,
 cause VARCHAR(255) NOT NULL,
 decision VARCHAR(64) NOT NULL,
 day VARCHAR(16) NOT NULL,
 at_ms BIGINT NOT NULL,
 PRIMARY KEY (event_key),
 KEY owner_notice_decisions_window (lane, cause, decision, at_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

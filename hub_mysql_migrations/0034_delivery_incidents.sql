-- hk 6384 (hk 4289 T1): the incident table of the delivery obligations, the
-- twin of Postgres migration 0040. open_slot holds the incident's key while it
-- is open and NULL once closed, so a plain UNIQUE key allows one open incident
-- per key.
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='delivery_incidents'
CREATE TABLE delivery_incidents (
 id BIGINT NOT NULL AUTO_INCREMENT,
 design_lane VARCHAR(255) NOT NULL,
 machine VARCHAR(255) NOT NULL,
 fault VARCHAR(64) NOT NULL,
 harness VARCHAR(64) NOT NULL DEFAULT '',
 state VARCHAR(16) NOT NULL,
 open_slot VARCHAR(512) NULL,
 members INTEGER NOT NULL DEFAULT 0,
 announced INTEGER NOT NULL DEFAULT 0,
 notice_relay_id BIGINT NOT NULL DEFAULT 0,
 notice_event_id VARCHAR(255) NOT NULL DEFAULT '',
 opened_at BIGINT NOT NULL,
 notice_at BIGINT NOT NULL DEFAULT 0,
 last_member_at BIGINT NOT NULL DEFAULT 0,
 closed_at BIGINT NOT NULL DEFAULT 0,
 close_reason VARCHAR(32) NOT NULL DEFAULT '',
 PRIMARY KEY (id),
 UNIQUE KEY delivery_incidents_open_slot (open_slot)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

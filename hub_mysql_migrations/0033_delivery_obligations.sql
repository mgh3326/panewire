-- hk 6384 (hk 4289 T1): durable delivery obligations
-- (docs/r20-relay-persistence.md, "Delivery obligations"). The twin of Postgres
-- migration 0040. result_id is nullable with a plain UNIQUE key because MySQL
-- has no partial indexes and keeps NULLs distinct. event_key is raw bytes, as
-- in lane_inbox.
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='delivery_obligations'
CREATE TABLE delivery_obligations (
 id BIGINT NOT NULL AUTO_INCREMENT,
 event_key VARBINARY(2048) NOT NULL,
 result_id VARCHAR(255) NULL,
 relay_id BIGINT NOT NULL DEFAULT 0,
 kind VARCHAR(64) NOT NULL,
 lane VARCHAR(255) NOT NULL,
 machine VARCHAR(255) NOT NULL DEFAULT '',
 pane VARCHAR(255) NOT NULL DEFAULT '',
 state VARCHAR(16) NOT NULL,
 fault VARCHAR(64) NOT NULL DEFAULT '',
 fault_reason VARCHAR(128) NOT NULL DEFAULT '',
 spent INTEGER NOT NULL DEFAULT 0,
 labelled INTEGER NOT NULL DEFAULT 0,
 revived INTEGER NOT NULL DEFAULT 0,
 revive_requested BIGINT NOT NULL DEFAULT 0,
 attempts_seen INTEGER NOT NULL DEFAULT 0,
 next_at BIGINT NOT NULL DEFAULT 0,
 incident_id BIGINT NOT NULL DEFAULT 0,
 marker_event_id VARCHAR(255) NOT NULL DEFAULT '',
 settle_reason VARCHAR(64) NOT NULL DEFAULT '',
 last_fault_at BIGINT NOT NULL DEFAULT 0,
 created_at BIGINT NOT NULL,
 updated_at BIGINT NOT NULL,
 PRIMARY KEY (id),
 UNIQUE KEY delivery_obligations_event_key (event_key),
 UNIQUE KEY delivery_obligations_result (result_id),
 KEY delivery_obligations_due (state, next_at),
 KEY delivery_obligations_relay (relay_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

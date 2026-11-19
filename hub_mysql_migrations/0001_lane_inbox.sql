-- The hub lane inbox on MySQL (hk 4898 phase 1b). Millisecond timestamps are
-- BIGINT. event_key is raw bytes (the producer's NUL-joined key), so no hex
-- encoding; the MySQL dialect binds it as bytes. must_ack stays an integer 0/1
-- so every dialect runs the same statements. MySQL has no partial indexes: the
-- indexes below lead with the columns the old partial predicates filtered on.
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='lane_inbox'
CREATE TABLE lane_inbox (
 id BIGINT NOT NULL AUTO_INCREMENT,
 event_key VARBINARY(2048) NOT NULL,
 lane VARCHAR(255) NOT NULL,
 kind VARCHAR(255) NOT NULL,
 class VARCHAR(16) NOT NULL,
 payload MEDIUMTEXT NOT NULL,
 created_at BIGINT NOT NULL,
 read_at BIGINT NOT NULL DEFAULT 0,
 acked_at BIGINT NOT NULL DEFAULT 0,
 due_at BIGINT NOT NULL DEFAULT 0,
 digest_at BIGINT NOT NULL DEFAULT 0,
 digest_batch VARCHAR(512) NOT NULL DEFAULT '',
 escalation_batch VARCHAR(512) NOT NULL DEFAULT '',
 must_ack TINYINT NOT NULL DEFAULT 0,
 hold_until BIGINT NOT NULL DEFAULT 0,
 delivered_at BIGINT NOT NULL DEFAULT 0,
 sender VARCHAR(255) NOT NULL DEFAULT '',
 event_id VARCHAR(512) NOT NULL DEFAULT '',
 cause VARCHAR(255) NULL,
 superseded_at BIGINT NOT NULL DEFAULT 0,
 superseded_by BIGINT NOT NULL DEFAULT 0,
 delivery_stage VARCHAR(64) NOT NULL DEFAULT '',
 delivery_reason TEXT NOT NULL DEFAULT (''),
 delivery_machine VARCHAR(255) NOT NULL DEFAULT '',
 delivery_pane VARCHAR(255) NOT NULL DEFAULT '',
 delivery_at BIGINT NOT NULL DEFAULT 0,
 relay_id BIGINT NOT NULL DEFAULT 0,
 PRIMARY KEY (id),
 UNIQUE KEY lane_inbox_event_key (event_key),
 KEY lane_inbox_unread (lane, class, read_at),
 KEY lane_inbox_due (class, read_at, due_at),
 KEY lane_inbox_digest (lane, digest_at),
 KEY lane_inbox_must_ack (lane, must_ack, acked_at),
 KEY lane_inbox_hold (lane, hold_until),
 KEY lane_inbox_event_id (event_id),
 KEY lane_inbox_relay_id (relay_id),
 KEY lane_inbox_digest_batch (digest_batch),
 KEY lane_inbox_escalation_batch (escalation_batch),
 CHECK (class IN ('inbox','interrupt')),
 CHECK (must_ack IN (0,1))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

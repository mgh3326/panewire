-- hk 6384 (hk 4289 T1): durable delivery obligations, one per relay row the hub
-- owes a delivery until a stored fact closes it (docs/r20-relay-persistence.md,
-- "Delivery obligations"). SQLite's twin is deliveryObligationSchema; MySQL's
-- are migrations 0033-0036. The tables hold the same columns on all three.
--
-- result_id and open_slot are nullable with a plain UNIQUE: all three backends
-- keep NULLs distinct, and MySQL has no partial indexes. event_key is the
-- hex form of the hub key, as in lane_inbox. The hub seeds the one
-- delivery_reconciler_state row when it opens the store.
--
-- Additive: an older hub neither reads nor writes these tables.
CREATE TABLE IF NOT EXISTS delivery_obligations (
 id BIGSERIAL PRIMARY KEY,
 event_key TEXT NOT NULL,
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
 UNIQUE (event_key),
 UNIQUE (result_id)
);
CREATE INDEX IF NOT EXISTS delivery_obligations_due ON delivery_obligations(state, next_at);
CREATE INDEX IF NOT EXISTS delivery_obligations_relay ON delivery_obligations(relay_id);
CREATE TABLE IF NOT EXISTS delivery_incidents (
 id BIGSERIAL PRIMARY KEY,
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
 UNIQUE (open_slot)
);
CREATE TABLE IF NOT EXISTS delivery_notice_slots (
 design_lane VARCHAR(255) NOT NULL,
 machine VARCHAR(255) NOT NULL,
 fault VARCHAR(64) NOT NULL,
 notice_at BIGINT NOT NULL DEFAULT 0,
 notices INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY (design_lane, machine, fault)
);
CREATE TABLE IF NOT EXISTS delivery_reconciler_state (
 singleton INTEGER PRIMARY KEY CHECK (singleton=1),
 adopt_after_id BIGINT NOT NULL DEFAULT 0,
 adopt_wrapped_at BIGINT NOT NULL DEFAULT 0
);

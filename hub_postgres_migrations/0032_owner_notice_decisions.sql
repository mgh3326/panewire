-- The durable owner-notice decision table (hk 5360; packet
-- handoff/2026-11-19/panewire-design-hk5360-root-cause). One row per decided
-- notice, keyed by its event key; the first decision is the only one, and
-- every routing applies it. The count rows are the daily spawn_notices group
-- and the repeat window, so counts and the window survive a hub restart.
-- Additive only: an older hub neither reads nor writes this table.
CREATE TABLE IF NOT EXISTS owner_notice_decisions (event_key TEXT PRIMARY KEY, lane TEXT NOT NULL, cause TEXT NOT NULL, decision TEXT NOT NULL, day TEXT NOT NULL, at_ms BIGINT NOT NULL);
CREATE INDEX IF NOT EXISTS owner_notice_decisions_window ON owner_notice_decisions(lane, cause, decision, at_ms);

-- A request's time limit (hk 4660): when it passes, a request whose start is
-- not unproven ends given_up. Zero is none: a row an older hub inserted never
-- names a limit, and the hub never gives it up. Additive only, so a rollback
-- past this migration leaves the column unread.
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS expires_ms BIGINT NOT NULL DEFAULT 0;

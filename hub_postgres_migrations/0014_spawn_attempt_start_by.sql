-- The latest start_by sent for each grant-required spawn attempt (hk 4612).
-- Its own migration, so a database that already applied
-- 0011_spawn_attempt_grants gets it too.
CREATE TABLE IF NOT EXISTS spawn_attempt_start_by (node_id TEXT PRIMARY KEY,start_by_ms BIGINT NOT NULL);

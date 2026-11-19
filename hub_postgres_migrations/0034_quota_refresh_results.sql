-- One row per finished shared quota refresh attempt, keyed by the scheduler's
-- opaque attempt ID (hk 5456). A request stores the ID it waited on and reads
-- this row, so a completion can only ever answer for its own attempt — never
-- for an earlier or later one for the same machine and pool. Additive only: an
-- older hub neither reads nor writes the table.
CREATE TABLE IF NOT EXISTS quota_refresh_results (attempt_id TEXT PRIMARY KEY,machine TEXT NOT NULL,pool TEXT NOT NULL,account_key TEXT NOT NULL,outcome TEXT NOT NULL,seen_ms BIGINT NOT NULL);
CREATE INDEX IF NOT EXISTS quota_refresh_results_seen ON quota_refresh_results(seen_ms);

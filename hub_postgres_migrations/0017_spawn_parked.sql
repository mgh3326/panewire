-- A parked request (hk 4661): an accepted row no candidate can take now
-- carries why (park_reason) and when the hub looks again (next_check_ms).
-- failed_attempts counts the request's failed launches for its re-placement
-- back-off. Additive only: an older hub neither reads nor writes these
-- columns and places a parked row as the accepted row it is.
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS park_reason TEXT NOT NULL DEFAULT '';
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS next_check_ms BIGINT NOT NULL DEFAULT 0;
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS failed_attempts INTEGER NOT NULL DEFAULT 0;
-- claim_result is the cleanup-confirmed node refusal that proved an hk job
-- claim on claimed_host ('' none): the request's end at its limit.
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS claim_result TEXT NOT NULL DEFAULT '';

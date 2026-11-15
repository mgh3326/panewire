-- A consult row carries the failed seat profiles a respawn must keep excluding,
-- and the pre-claim lease that lets recovery wait without a fixed delay
-- (hk 4552).
ALTER TABLE consult_requests ADD COLUMN exclude_profiles TEXT NOT NULL DEFAULT '';
ALTER TABLE consult_requests ADD COLUMN claim_until_ms BIGINT NOT NULL DEFAULT 0;

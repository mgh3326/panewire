-- The start proof of a grant-required spawn attempt (hk 4612): one row per
-- node request id, state 'granted' or 'fenced', whichever was written first.
CREATE TABLE IF NOT EXISTS spawn_attempt_grants (node_id TEXT PRIMARY KEY,request_id TEXT NOT NULL,state TEXT NOT NULL,decided_ms BIGINT NOT NULL);

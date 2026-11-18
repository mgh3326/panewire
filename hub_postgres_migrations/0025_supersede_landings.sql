-- Durable landing intents of successors that may owe their parent's pane a
-- close (hk 5195). A row is written when an explicit implementer successor
-- lands while --supersede-close is on, and ends when the close row exists or
-- the parent is ineligible, so a retry works from exactly those intents.
-- Additive only: an older hub neither reads nor writes this table.
CREATE TABLE supersede_landings (
    request_id TEXT PRIMARY KEY,
    parent TEXT NOT NULL,
    successor TEXT NOT NULL,
    owner TEXT NOT NULL,
    created_ms BIGINT NOT NULL,
    tried_ms BIGINT NOT NULL DEFAULT 0,
    state TEXT NOT NULL CHECK(state IN ('pending','done'))
);
CREATE INDEX supersede_landings_pending ON supersede_landings(state, tried_ms, created_ms);

-- Durable landing intents of successors that may owe their parent's pane a
-- close, and the generation fence that revokes them (hk 5195). A landing row
-- is written in the same transaction as the successor's succeeded outcome,
-- while --supersede-close is on, and ends when the close row exists or the
-- parent is ineligible. A hub started with the switch off advances the fence,
-- so every landing and close row of an earlier generation is cancelled for any
-- later hub, whatever the node or the store did meanwhile.
-- Additive only: an older hub neither reads nor writes these tables.
CREATE TABLE supersede_fence (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    generation BIGINT NOT NULL
);
INSERT INTO supersede_fence(singleton, generation) VALUES(1, 1) ON CONFLICT DO NOTHING;
CREATE TABLE supersede_landings (
    request_id TEXT PRIMARY KEY,
    parent TEXT NOT NULL,
    successor TEXT NOT NULL,
    owner TEXT NOT NULL,
    created_ms BIGINT NOT NULL,
    tried_ms BIGINT NOT NULL DEFAULT 0,
    generation BIGINT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending','done'))
);
CREATE INDEX supersede_landings_pending ON supersede_landings(state, tried_ms, created_ms);

-- The hub's told one-time actions (hub_one_time_action.go, hub_one_time_action_store.go):
-- one row per host, harness kind and need, so a hub restart does not tell the
-- operator again. told_ms is when the notice was sent; gone_ms is when a
-- readiness report first showed the need gone (0 while it shows). The SQLite
-- twin is oneTimeActionSchema.
CREATE TABLE IF NOT EXISTS one_time_action_notices (
    machine TEXT NOT NULL,
    kind TEXT NOT NULL,
    need TEXT NOT NULL,
    told_ms BIGINT NOT NULL,
    gone_ms BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (machine, kind, need)
);

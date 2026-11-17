CREATE TABLE pool_actions (
    id TEXT PRIMARY KEY,
    job TEXT NOT NULL,
    node TEXT NOT NULL,
    epoch BIGINT NOT NULL CHECK(epoch>0),
    phase TEXT NOT NULL CHECK(phase IN ('pending','settled','done','refused')),
    data TEXT NOT NULL
);
CREATE INDEX pool_actions_job ON pool_actions(job, phase);
CREATE INDEX pool_actions_phase ON pool_actions(phase, id);

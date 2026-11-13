-- A job's durable quota action on an exhausted pool: written before the first
-- effect, and driven to done by the sweep or the reconciler after a restart.
-- registered_ms is when the holder node acknowledged the action marker, so the
-- node watcher leaves the terminal record to the hub.
CREATE TABLE pool_exhausted_actions (request_id TEXT NOT NULL,attempt INTEGER NOT NULL,job TEXT NOT NULL,owner TEXT NOT NULL,role TEXT NOT NULL,kind TEXT NOT NULL,pool TEXT NOT NULL,account TEXT NOT NULL,cause TEXT NOT NULL,reset_ms BIGINT NOT NULL,end_reason TEXT NOT NULL,event_id TEXT NOT NULL,successor TEXT NOT NULL DEFAULT '',successor_request TEXT NOT NULL DEFAULT '',stage TEXT NOT NULL,registered_ms BIGINT NOT NULL DEFAULT 0,created_ms BIGINT NOT NULL,updated_ms BIGINT NOT NULL,PRIMARY KEY(request_id,attempt));
CREATE INDEX pool_exhausted_actions_pending ON pool_exhausted_actions(stage,created_ms);

-- hk 6994: the lane bindings of the daily stale-replay notice are keyed by the
-- receipt time, one binding per change of the lane's job, so a lane that returns
-- to a job keeps its later stint's binding (hk 6976). The SQLite twin is
-- migrateStaleReplayBindingKey; MySQL's twin is migration 0042.
-- Rollback: a hub binary from before this migration writes bindings under the
-- (worker_lane, job) key, which has no unique index on this table any more, so
-- each lane.event receipt's binding write fails (a warning, no binding). Stale rows
-- received in that window fall back to the lanes.json hint or the notice lane. A
-- rollback past this migration is a known degradation, not a silent one.
ALTER TABLE relay_stale_notice_lanes DROP CONSTRAINT IF EXISTS relay_stale_notice_lanes_pkey;
ALTER TABLE relay_stale_notice_lanes ADD PRIMARY KEY (worker_lane, first_ms);

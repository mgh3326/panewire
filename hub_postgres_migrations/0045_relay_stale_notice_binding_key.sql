-- hk 6994: the lane bindings of the daily stale-replay notice are keyed by the
-- receipt time, one binding per change of the lane's job, so a lane that returns
-- to a job keeps its later stint's binding (hk 6976). The SQLite twin is
-- migrateStaleReplayBindingKey; MySQL's twin is migration 0042.
ALTER TABLE relay_stale_notice_lanes DROP CONSTRAINT IF EXISTS relay_stale_notice_lanes_pkey;
ALTER TABLE relay_stale_notice_lanes ADD PRIMARY KEY (worker_lane, first_ms);

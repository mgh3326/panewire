-- hk 6994: the twin of Postgres migration 0045. The lane bindings of the
-- stale-replay notice are keyed by the receipt time, one binding per change of
-- the lane's job (hk 6976).
-- Rollback: a hub binary from before this migration writes bindings under the
-- (worker_lane, job) key, which has no unique index on this table any more, so
-- each lane.event receipt's binding write fails (a warning, no binding). Stale rows
-- received in that window fall back to the lanes.json hint or the notice lane. A
-- rollback past this migration is a known degradation, not a silent one.
-- applied-if: SELECT 1 - COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='relay_stale_notice_lanes' AND index_name='PRIMARY' AND column_name='job'
ALTER TABLE relay_stale_notice_lanes DROP PRIMARY KEY, ADD PRIMARY KEY (worker_lane, first_ms);

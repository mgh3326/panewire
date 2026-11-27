-- hk 6994: the twin of Postgres migration 0045. The lane bindings of the
-- stale-replay notice are keyed by the receipt time, one binding per change of
-- the lane's job (hk 6976).
-- applied-if: SELECT 1 - COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='relay_stale_notice_lanes' AND index_name='PRIMARY' AND column_name='job'
ALTER TABLE relay_stale_notice_lanes DROP PRIMARY KEY, ADD PRIMARY KEY (worker_lane, first_ms);

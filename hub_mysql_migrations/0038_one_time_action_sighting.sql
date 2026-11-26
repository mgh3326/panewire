-- hk 6815: retain the latest login-need sighting across hub restarts so an
-- older readiness report cannot clear a standing need.
-- applied-if: SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='one_time_action_notices' AND column_name='sighted_ms'
ALTER TABLE one_time_action_notices
 ADD COLUMN sighted_ms BIGINT NOT NULL DEFAULT 0;

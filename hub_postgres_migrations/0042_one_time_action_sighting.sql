-- hk 6815: retain the latest login-need sighting across hub restarts so an
-- older readiness report cannot clear a standing need.
ALTER TABLE one_time_action_notices
    ADD COLUMN sighted_ms BIGINT NOT NULL DEFAULT 0;

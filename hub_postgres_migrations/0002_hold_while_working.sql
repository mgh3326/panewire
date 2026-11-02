ALTER TABLE lane_inbox ADD COLUMN hold_until BIGINT NOT NULL DEFAULT 0;
CREATE INDEX lane_inbox_hold ON lane_inbox(lane,hold_until) WHERE hold_until>0 AND digest_batch='' AND acked_at=0;

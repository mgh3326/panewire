-- The delivery stage a sender reads through ack-state (hk 3467): where its
-- lane.event is, and the reason when it is unconfirmed or dropped. relay_id
-- ties the row to the durable relay record so a node report that names only
-- that record can update the stage.
ALTER TABLE lane_inbox ADD COLUMN IF NOT EXISTS delivery_stage TEXT NOT NULL DEFAULT '';
ALTER TABLE lane_inbox ADD COLUMN IF NOT EXISTS delivery_reason TEXT NOT NULL DEFAULT '';
ALTER TABLE lane_inbox ADD COLUMN IF NOT EXISTS delivery_machine TEXT NOT NULL DEFAULT '';
ALTER TABLE lane_inbox ADD COLUMN IF NOT EXISTS delivery_pane TEXT NOT NULL DEFAULT '';
ALTER TABLE lane_inbox ADD COLUMN IF NOT EXISTS delivery_at BIGINT NOT NULL DEFAULT 0;
ALTER TABLE lane_inbox ADD COLUMN IF NOT EXISTS relay_id BIGINT NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS lane_inbox_relay_id ON lane_inbox(relay_id) WHERE relay_id<>0;
-- Rows from before the columns: a confirmed delivery is proven by delivered_at,
-- and an inbox-class row was delivered through the lane's inbox. Everything
-- else stays empty and reads as queued.
UPDATE lane_inbox SET delivery_stage='delivered',delivery_at=delivered_at WHERE delivered_at>0 AND delivery_stage='';
UPDATE lane_inbox SET delivery_stage='inbox_only' WHERE delivery_stage='' AND class='inbox';

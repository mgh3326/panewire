-- Delivery confirmation surfaces summaries without reading or acknowledging items.
CREATE TABLE IF NOT EXISTS lane_inbox_surfaces (
 delivery_key TEXT PRIMARY KEY, lane TEXT NOT NULL, through_id BIGINT NOT NULL,
 summary TEXT NOT NULL, delivered_at BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS lane_inbox_surfaces_lane ON lane_inbox_surfaces(lane,delivered_at);

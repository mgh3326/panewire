-- Delivery confirmation surfaces summaries without reading or acknowledging items.
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='lane_inbox_surfaces'
CREATE TABLE lane_inbox_surfaces (
 delivery_key VARCHAR(768) NOT NULL,
 lane VARCHAR(255) NOT NULL,
 through_id BIGINT NOT NULL,
 summary MEDIUMTEXT NOT NULL,
 delivered_at BIGINT NOT NULL DEFAULT 0,
 PRIMARY KEY (delivery_key),
 KEY lane_inbox_surfaces_lane (lane, delivered_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

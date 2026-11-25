-- hk 6384 (hk 4289 T1): the notice spacing of each incident key, the twin of
-- Postgres migration 0040. The limit belongs to the key, so closing and
-- reopening an incident cannot shorten it.
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='delivery_notice_slots'
CREATE TABLE delivery_notice_slots (
 design_lane VARCHAR(255) NOT NULL,
 machine VARCHAR(255) NOT NULL,
 fault VARCHAR(64) NOT NULL,
 notice_at BIGINT NOT NULL DEFAULT 0,
 notices INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY (design_lane, machine, fault)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

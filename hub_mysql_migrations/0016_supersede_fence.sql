-- The generation fence that revokes supersede landings (hk 5195). One row, seeded
-- by the next migration.
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='supersede_fence'
CREATE TABLE supersede_fence (
 singleton INT NOT NULL,
 generation BIGINT NOT NULL,
 PRIMARY KEY (singleton),
 CHECK (singleton=1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

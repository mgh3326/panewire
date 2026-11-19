-- The fence's one row, generation 1.
-- applied-if: SELECT COUNT(*) FROM supersede_fence WHERE singleton=1
INSERT INTO supersede_fence(singleton,generation) VALUES(1,1) AS incoming ON DUPLICATE KEY UPDATE singleton=supersede_fence.singleton;

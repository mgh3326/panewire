-- hk 6384 (hk 4289 T1): where the adopt walk resumes, the twin of Postgres
-- migration 0040. One row, which the hub seeds when it opens the store.
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='delivery_reconciler_state'
CREATE TABLE delivery_reconciler_state (
 singleton INT NOT NULL,
 adopt_after_id BIGINT NOT NULL DEFAULT 0,
 adopt_wrapped_at BIGINT NOT NULL DEFAULT 0,
 PRIMARY KEY (singleton),
 CHECK (singleton=1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

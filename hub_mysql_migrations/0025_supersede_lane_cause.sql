-- The row lock of one (lane, cause) supersede (mysqlDialect.lockLaneCause):
-- Postgres takes an advisory lock for the life of the transaction, which MySQL has
-- no form of, and GET_LOCK outlives the transaction. lock_key is the SHA-256 of
-- the pair, so the table stays small and its key is fixed-size.
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='supersede_lane_cause'
CREATE TABLE supersede_lane_cause (
 lock_key CHAR(64) NOT NULL,
 PRIMARY KEY (lock_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

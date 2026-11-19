-- The commandcode account credit marks.
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='account_credit_marks'
CREATE TABLE account_credit_marks (
 account_key VARCHAR(255) NOT NULL,
 pool VARCHAR(255) NOT NULL,
 marked_ms BIGINT NOT NULL,
 PRIMARY KEY (account_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

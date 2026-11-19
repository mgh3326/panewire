-- One row per failed or refused operation whose hk task comment is still owed
-- (hk 5299); retry_ms is its next attempt.
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='task_fail_notice_outbox'
CREATE TABLE task_fail_notice_outbox (
 request_id VARCHAR(255) NOT NULL,
 retry_ms BIGINT NOT NULL DEFAULT 0,
 PRIMARY KEY (request_id),
 KEY task_fail_notice_outbox_retry (retry_ms, request_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

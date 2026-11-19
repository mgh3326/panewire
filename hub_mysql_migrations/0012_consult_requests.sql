-- Consult-on-request rows (hk 4552). The row is the durable consult: idempotency
-- key (task, repo#pr, head), the resolved seat, the spawn operation and the
-- stored answer document. The active set is two indexed reads: the queued and
-- running rows by state, the answered-but-unnotified rows by state and notified.
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='consult_requests'
CREATE TABLE consult_requests (
 id VARCHAR(255) NOT NULL,
 idem_key VARCHAR(768) NOT NULL,
 task VARCHAR(255) NOT NULL,
 pr VARCHAR(255) NOT NULL DEFAULT '',
 head VARCHAR(255) NOT NULL DEFAULT '',
 requester VARCHAR(255) NOT NULL,
 question_doc VARCHAR(512) NOT NULL,
 author_job VARCHAR(255) NOT NULL DEFAULT '',
 author_family VARCHAR(255) NOT NULL DEFAULT '',
 state VARCHAR(64) NOT NULL,
 seat_profile VARCHAR(255) NOT NULL DEFAULT '',
 seat_family VARCHAR(255) NOT NULL DEFAULT '',
 seat_grade VARCHAR(255) NOT NULL DEFAULT '',
 job VARCHAR(255) NOT NULL DEFAULT '',
 request_id VARCHAR(255) NOT NULL DEFAULT '',
 reason TEXT NOT NULL DEFAULT (''),
 answer_doc VARCHAR(512) NOT NULL DEFAULT '',
 created_ms BIGINT NOT NULL,
 answered_ms BIGINT NOT NULL DEFAULT 0,
 deadline_ms BIGINT NOT NULL,
 respawns INT NOT NULL DEFAULT 0,
 notified INT NOT NULL DEFAULT 0,
 updated_ms BIGINT NOT NULL DEFAULT 0,
 exclude_profiles TEXT NOT NULL DEFAULT (''),
 claim_until_ms BIGINT NOT NULL DEFAULT 0,
 machine VARCHAR(255) NOT NULL DEFAULT '',
 caller_pane VARCHAR(255) NOT NULL DEFAULT '',
 owner VARCHAR(255) NOT NULL DEFAULT '',
 PRIMARY KEY (id),
 UNIQUE KEY consult_requests_idem_key (idem_key),
 KEY consult_requests_active (state, notified, created_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

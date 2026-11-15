-- Consult-on-request rows (hk 4552 phase 1, operator decision 250). The row
-- is the durable consult: idempotency key (task, repo#pr, head), the resolved
-- seat, the spawn operation and the stored answer document. The hub keeps
-- working on queued/running rows after a restart.
CREATE TABLE IF NOT EXISTS consult_requests (
  id TEXT PRIMARY KEY,
  idem_key TEXT NOT NULL UNIQUE,
  task TEXT NOT NULL,
  pr TEXT NOT NULL DEFAULT '',
  head TEXT NOT NULL DEFAULT '',
  requester TEXT NOT NULL,
  question_doc TEXT NOT NULL,
  author_job TEXT NOT NULL DEFAULT '',
  author_family TEXT NOT NULL DEFAULT '',
  state TEXT NOT NULL,
  seat_profile TEXT NOT NULL DEFAULT '',
  seat_family TEXT NOT NULL DEFAULT '',
  seat_grade TEXT NOT NULL DEFAULT '',
  job TEXT NOT NULL DEFAULT '',
  request_id TEXT NOT NULL DEFAULT '',
  reason TEXT NOT NULL DEFAULT '',
  answer_doc TEXT NOT NULL DEFAULT '',
  created_ms BIGINT NOT NULL,
  answered_ms BIGINT NOT NULL DEFAULT 0,
  deadline_ms BIGINT NOT NULL,
  respawns INTEGER NOT NULL DEFAULT 0,
  notified INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS consult_requests_active ON consult_requests(state) WHERE state IN ('queued','running');

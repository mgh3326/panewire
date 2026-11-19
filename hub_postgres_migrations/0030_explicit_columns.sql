-- hk 5300: a quota-stop successor names the job it replaces in an indexed,
-- nullable column, so the quota reconcile selects replacements by the column
-- instead of matching "%-q%" in the job id. NULL is every other spawn.
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS replaced_from TEXT;

-- Backfill the existing successors: "<old>-q<n>" replaced "<old>". A job id
-- with no parent before its "-q<n>" suffix is not a replacement.
UPDATE spawn_operations
   SET replaced_from = regexp_replace(job, '-q[0-9]+$', '')
 WHERE replaced_from IS NULL AND job ~ '^.+-q[0-9]+$';

CREATE INDEX IF NOT EXISTS spawn_operations_replaced_from ON spawn_operations(replaced_from);

-- The consult active set reads answered-but-unnotified rows by state and
-- notified (the queued/running half is served by consult_requests_active,
-- migration 0013).
CREATE INDEX IF NOT EXISTS consult_requests_answered_unnotified ON consult_requests(state, notified, created_ms);

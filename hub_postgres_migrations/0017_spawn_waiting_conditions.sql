-- Start conditions through the spawn queue (hk 4736). A request submitted with
-- --after waits as status waiting until its predicates hold, then the hub
-- releases it to accepted. Both changes are additive: conditions_met_ms and
-- condition_result are new columns an older hub neither reads nor writes, and
-- waiting/refused/cancelled are new values of status it lists in no query.
--
-- Moving a row an older hub left executable with a start condition into the
-- queue is the only data change. This is a fast path for rows written before
-- the executeSpawnTask guard, not the gate itself. It never takes:
--   * staged rows — the ladder owns them (activate/cancel/discard CAS on that
--     status), and a swept successor could launch after a discard;
--   * retryable rows — completeSpawn deleted their reservation, so the queue
--     could release one without a reservation; the replay path re-creates it
--     and the guard waits the row then;
--   * any bound row (node_id IS NULL) — a bound deadline_exceeded row may
--     still be running on its node and keeps its own guard.
-- conditions_met_ms is written on release, so a row the queue already released
-- is not moved back by a later open.
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS conditions_met_ms BIGINT NOT NULL DEFAULT 0;
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS condition_result TEXT NOT NULL DEFAULT '';

UPDATE spawn_operations SET status='waiting'
 WHERE status IN ('accepted','deadline_exceeded','deadline_exceeded_grant')
   AND node_id IS NULL AND conditions_met_ms=0 AND payload LIKE '%"start_conditions":%';

-- A database that applied an earlier build of this branch's 0017 already moved
-- its conditioned staged rows; that build never shipped. Return such a row by
-- hand while the ladder still owns it (status 'staged' cannot be inferred from
-- the row alone, so this is not automatic):
--   UPDATE spawn_operations SET status='staged'
--    WHERE request_id='<request>' AND status='waiting' AND conditions_met_ms=0;

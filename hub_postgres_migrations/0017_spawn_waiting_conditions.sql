-- Start conditions through the spawn queue (hk 4736). A request submitted with
-- --after waits as status waiting until its predicates hold, then the hub
-- releases it to accepted. Both changes are additive: conditions_met_ms and
-- condition_result are new columns an older hub neither reads nor writes, and
-- waiting/refused/cancelled are new values of status it lists in no query.
--
-- Moving a conditioned row that never passed the queue into it is the only
-- data change. Unbound statuses are covered: accepted, deadline_exceeded,
-- retryable and staged rows all become waiting. The column is written on
-- release, so a row the queue already released (status accepted, conditions
-- met) is not moved back by a later open. A bound legacy row keeps its
-- in-code guard and is not moved.
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS conditions_met_ms BIGINT NOT NULL DEFAULT 0;
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS condition_result TEXT NOT NULL DEFAULT '';

UPDATE spawn_operations SET status='waiting'
 WHERE status IN ('accepted','deadline_exceeded','retryable','staged','deadline_exceeded_grant')
   AND conditions_met_ms=0 AND payload LIKE '%"start_conditions":%';

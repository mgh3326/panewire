-- Start conditions through the spawn queue (hk 4736). A request submitted with
-- --after waits as status waiting until its predicates hold, then the hub
-- releases it to accepted. Both changes are additive: conditions_met_ms is a
-- new column an older hub neither reads nor writes, and waiting/refused/
-- cancelled are new values of status it lists in no query.
--
-- Moving an unbound row an older hub accepted with a start condition into the
-- queue is the only data change. The column is written on release, so a row
-- the queue already released (status accepted, conditions met) is not moved
-- back by a later open. A bound legacy row keeps its in-code guard and is not
-- moved.
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS conditions_met_ms BIGINT NOT NULL DEFAULT 0;

UPDATE spawn_operations SET status='waiting'
 WHERE status='accepted' AND conditions_met_ms=0 AND payload LIKE '%"start_conditions":%';

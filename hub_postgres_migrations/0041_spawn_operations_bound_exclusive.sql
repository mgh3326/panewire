-- hk 6433: bound_machine is the machine a bound operation's node request names,
-- and exclusive is 1 when that request has no checkout overlap key, so the node
-- admits it alone. The bind writes both and every statement that unbinds clears
-- them; a terminal row keeps its values, and readers filter by status. The
-- coordinator reads each machine's oldest waiting exclusive in one query
-- (boundExclusiveSpawns) instead of decoding node_payload for every bound row.
-- Rows bound before this file are filled once by the hub at start
-- (backfillSpawnBoundMachines). MySQL's twin is migration 0037.
--
-- Additive only: an older hub neither reads nor writes the columns.
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS bound_machine TEXT NOT NULL DEFAULT '';
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS exclusive INTEGER NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS spawn_operations_bound_exclusive ON spawn_operations(bound_machine, exclusive, status, created_ms, request_id);

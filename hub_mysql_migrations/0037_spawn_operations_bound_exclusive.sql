-- hk 6433: bound_machine is the machine a bound operation's node request names,
-- and exclusive is 1 when that request has no checkout overlap key, so the node
-- admits it alone. The bind writes both and every statement that unbinds clears
-- them; a terminal row keeps its values, and readers filter by status. Rows
-- bound before this file are filled once by the hub at start
-- (backfillSpawnBoundMachines). Postgres' twin is migration 0041.
-- One ALTER adds both columns and the key, so the statement is atomic and the
-- guard below sees all of it or none.
-- applied-if: SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='spawn_operations' AND column_name='bound_machine'
ALTER TABLE spawn_operations ADD COLUMN bound_machine VARCHAR(255) NOT NULL DEFAULT '', ADD COLUMN exclusive INT NOT NULL DEFAULT 0, ADD KEY spawn_operations_bound_exclusive (bound_machine, exclusive, status, created_ms, request_id);

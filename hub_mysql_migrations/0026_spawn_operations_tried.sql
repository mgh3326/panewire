-- What a parked attempt asked and why each candidate was dropped (hk 5456).
-- A request that reaches its limit while parked names them in its given_up
-- outcome instead of saying only that time ran out.
-- applied-if: SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='spawn_operations' AND column_name='tried'
ALTER TABLE spawn_operations ADD COLUMN tried TEXT NOT NULL DEFAULT ('[]');

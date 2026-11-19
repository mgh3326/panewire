-- What a parked attempt asked and why each candidate was dropped (hk 5456).
-- A request that reaches its limit while parked names them in its given_up
-- outcome instead of saying only that time ran out. Additive only: an older
-- hub neither reads nor writes the column.
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS tried TEXT NOT NULL DEFAULT '[]';

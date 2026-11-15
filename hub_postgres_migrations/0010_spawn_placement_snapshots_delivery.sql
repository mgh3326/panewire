-- The delivery settings each attempt's placement record posts and spools
-- through (hk 4615): the placing App's state dir, home and shared-quota
-- override, persisted so the outbox delivers the attempt's record the way
-- its own App would have, even beside a concurrent spawn with different
-- settings.
ALTER TABLE spawn_placement_snapshots ADD COLUMN IF NOT EXISTS delivery TEXT NOT NULL DEFAULT '';

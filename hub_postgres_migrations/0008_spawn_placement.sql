-- The placement stamps a hub restart reads to restore decided-but-unclaimed
-- counts (hk 4460): placed_ms is when a spawn was bound to its node, and
-- claimed_ms is when its claim ended that count. Zero means not stamped.
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS placed_ms BIGINT NOT NULL DEFAULT 0;
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS claimed_ms BIGINT NOT NULL DEFAULT 0;

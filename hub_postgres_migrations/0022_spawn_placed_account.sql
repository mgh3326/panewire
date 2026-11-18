-- The attempt's in-flight seat (hk 5124): the commandcode account the bind
-- chose, cleared by every status change that leaves the attempt. Additive
-- only: an older hub neither reads nor writes these columns and its rows
-- stay exactly as they are.
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS placed_account_key TEXT NOT NULL DEFAULT '';
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS placed_attempt INTEGER NOT NULL DEFAULT 0;

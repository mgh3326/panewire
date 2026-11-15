-- A handoffkeep job claim binds the host across retries and hub restarts.
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS claimed_host TEXT NOT NULL DEFAULT '';

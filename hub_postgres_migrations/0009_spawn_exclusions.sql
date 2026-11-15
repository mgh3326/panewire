-- A definitive node refusal and its unbinding commit together, per attempt.
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS exclusions TEXT NOT NULL DEFAULT '[]';

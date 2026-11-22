-- hk 6074: trace_history keeps each attempt that ended unstarted and was renewed
-- (its end time, bounded reason and stage durations), so the spawn trace of the
-- landed attempt starts at acceptance and shows the earlier attempts and their
-- queue waits. A JSON array; '[]' means no renewed attempt. Additive only: an
-- older hub neither reads nor writes the column.
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS trace_history TEXT NOT NULL DEFAULT '[]';

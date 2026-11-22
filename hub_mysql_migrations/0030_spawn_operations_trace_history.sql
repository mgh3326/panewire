-- hk 6074: trace_history keeps each attempt that ended unstarted and was renewed
-- (its end time, bounded reason and stage durations), so the spawn trace of the
-- landed attempt starts at acceptance and shows the earlier attempts and their
-- queue waits. A JSON array; '[]' means no renewed attempt.
-- applied-if: SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='spawn_operations' AND column_name='trace_history'
ALTER TABLE spawn_operations ADD COLUMN trace_history TEXT NOT NULL DEFAULT ('[]');

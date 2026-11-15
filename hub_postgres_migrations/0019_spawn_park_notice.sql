-- A long-parked request's one owner notice (hk 4662). park_since_ms is when
-- the request's current wait began, so the notice can say since when; it is
-- kept across re-parks and binds and restarted with a new attempt.
-- park_priority is the bound hk task's priority, read once per wait (-1: not
-- read yet), because hk's batched states route carries no priority;
-- park_priority_failures counts the passes that could not read it, so one task
-- hk will not answer for cannot hold the candidate page.
-- park_notice_ms and park_judged_reason are the durable judgement marker: when
-- the owner was told, and the park reason set the pass last judged (told or
-- not, so a judged row leaves the candidate page). Additive only: an older hub
-- neither reads nor writes these columns and its parked rows stay exactly as
-- they are.
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS park_since_ms BIGINT NOT NULL DEFAULT 0;
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS park_priority INTEGER NOT NULL DEFAULT -1;
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS park_notice_ms BIGINT NOT NULL DEFAULT 0;
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS park_judged_reason TEXT NOT NULL DEFAULT '';
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS park_priority_failures INTEGER NOT NULL DEFAULT 0;

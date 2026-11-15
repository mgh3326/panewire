-- A long-parked request's one owner notice (hk 4662). park_since_ms is when
-- the current unbroken parking stretch began, so the notice can say since
-- when; park_priority is the bound hk task's priority, read once per stretch
-- (-1: not read yet), because hk's batched states route carries no priority;
-- park_notice_ms and park_notice_reason are the durable notified marker: when
-- the owner was told and the reason set it named. Additive only: an older hub
-- neither reads nor writes these columns and its parked rows stay exactly as
-- they are.
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS park_since_ms BIGINT NOT NULL DEFAULT 0;
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS park_priority INTEGER NOT NULL DEFAULT -1;
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS park_notice_ms BIGINT NOT NULL DEFAULT 0;
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS park_notice_reason TEXT NOT NULL DEFAULT '';

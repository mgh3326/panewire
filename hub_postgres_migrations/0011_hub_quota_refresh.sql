-- A request parked on a shared quota refresh keeps that link across a hub
-- restart, and the last finished attempt per machine/pool (success included,
-- outcome '') outlives the scheduler's memory, so a timeout that expires while
-- waiting names the attempt instead of the queue-starved cause (hk 4600).
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS refresh_waits TEXT NOT NULL DEFAULT '[]';
CREATE TABLE IF NOT EXISTS quota_refresh_attempts (machine TEXT NOT NULL,pool TEXT NOT NULL,outcome TEXT NOT NULL,seen_ms BIGINT NOT NULL,PRIMARY KEY(machine,pool));

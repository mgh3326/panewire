-- The spawn queue (hk 4388): host+account pairs that refused a request, so
-- placement skips them, and the rank that orders the queue (task priority,
-- then age). 5 is the rank of a request without a task.
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS excluded TEXT NOT NULL DEFAULT '[]';
ALTER TABLE spawn_operations ADD COLUMN IF NOT EXISTS priority_rank INTEGER NOT NULL DEFAULT 5;

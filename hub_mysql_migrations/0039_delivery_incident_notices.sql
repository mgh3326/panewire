-- hk 6821 (hk 4289 T4): which obligation rows a design lane is still to be told
-- about, the twin of Postgres migration 0043. notice_due_at is when a row got a
-- fault no notice has covered, 0 once there is nothing to tell; told_at is when
-- a stored or withheld notice covered the row, which never becomes due again.
-- fault_reason, from migration 0033, holds the fault a due notice reports.
-- One ALTER adds both columns and the key, so the statement is atomic and the
-- guard below sees all of it or none. An older hub neither reads nor writes
-- the columns.
-- applied-if: SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='delivery_obligations' AND column_name='notice_due_at'
ALTER TABLE delivery_obligations ADD COLUMN notice_due_at BIGINT NOT NULL DEFAULT 0, ADD COLUMN told_at BIGINT NOT NULL DEFAULT 0, ADD KEY delivery_obligations_incident (incident_id, notice_due_at);

-- hk 6821 (hk 4289 T4): which obligation rows a design lane is still to be told
-- about (docs/r20-relay-persistence.md, "Incidents and notices").
-- notice_due_at is when a row got a fault no notice has covered, 0 once there
-- is nothing to tell; told_at is when a stored or withheld notice covered the
-- row, which never becomes due again. fault_reason, from migration 0040, holds
-- the fault a due notice reports. MySQL's twin is migration 0039.
--
-- Additive: an older hub neither reads nor writes the columns.
ALTER TABLE delivery_obligations ADD COLUMN IF NOT EXISTS notice_due_at BIGINT NOT NULL DEFAULT 0;
ALTER TABLE delivery_obligations ADD COLUMN IF NOT EXISTS told_at BIGINT NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS delivery_obligations_incident ON delivery_obligations(incident_id, notice_due_at);

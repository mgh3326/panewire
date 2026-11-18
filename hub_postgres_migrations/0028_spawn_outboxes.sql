-- Placement-record and spawn-notice outboxes (hk 5298; design hk:doc
-- design/2026-11-18/pw-4898-hub-store-mysql §2-§3). The recorder and the
-- notice router read these small tables instead of scanning
-- spawn_placement_snapshots (recorded<>1) and spawn_notifications
-- (delivered<>1), which keep every row they ever held. A row is written with
-- the snapshot or notification it names and deleted in the transaction that
-- marks it recorded or delivered. The history tables are unchanged and still
-- carry the recorded and delivered marks. Additive only: an older hub neither
-- reads nor writes these tables.
CREATE TABLE IF NOT EXISTS placement_record_outbox (request_id TEXT NOT NULL,attempt INTEGER NOT NULL,PRIMARY KEY(request_id,attempt));
CREATE TABLE IF NOT EXISTS spawn_notice_outbox (event_id TEXT PRIMARY KEY);
-- The hub also reconciles the outboxes with these two tables at every start
-- (a rollback to an older binary leaves rows behind), reading only the rows
-- still owed: these partial indexes keep that read bounded by them, not by the
-- retained history.
CREATE INDEX IF NOT EXISTS spawn_placement_snapshots_unsealed ON spawn_placement_snapshots(request_id,attempt) WHERE recorded<>1;
CREATE INDEX IF NOT EXISTS spawn_notifications_undelivered ON spawn_notifications(event_id) WHERE delivered<>1;
-- Carry over what is pending now, so nothing owed is lost at the upgrade. This
-- runs once per database (migrations are versioned).
INSERT INTO placement_record_outbox(request_id,attempt) SELECT request_id,attempt FROM spawn_placement_snapshots WHERE recorded<>1 ON CONFLICT DO NOTHING;
INSERT INTO spawn_notice_outbox(event_id) SELECT event_id FROM spawn_notifications WHERE delivered<>1 ON CONFLICT DO NOTHING;

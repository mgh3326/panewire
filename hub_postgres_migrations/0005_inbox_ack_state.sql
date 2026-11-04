-- The machine that relayed a row and the producer's event id, for the
-- ack-state lookup. Rows from before keep an empty sender; their event id is
-- read back from the stored payload.
ALTER TABLE lane_inbox ADD COLUMN sender TEXT NOT NULL DEFAULT '';
ALTER TABLE lane_inbox ADD COLUMN event_id TEXT NOT NULL DEFAULT '';
-- A payload escaping U+0000 is skipped: Postgres text cannot hold it.
UPDATE lane_inbox SET event_id=COALESCE(payload::json->>'event_id','') WHERE payload NOT LIKE '%\\u0000%';
CREATE INDEX lane_inbox_event_id ON lane_inbox(event_id) WHERE event_id<>'';

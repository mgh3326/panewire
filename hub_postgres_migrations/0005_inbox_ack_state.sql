-- The machine that relayed a row and the producer's event id, for the
-- ack-state lookup. Rows from before keep an empty sender; their event id is
-- read back from the stored payload.
ALTER TABLE lane_inbox ADD COLUMN sender TEXT NOT NULL DEFAULT '';
ALTER TABLE lane_inbox ADD COLUMN event_id TEXT NOT NULL DEFAULT '';
-- Only an event_id that itself escapes U+0000 is skipped (Postgres text cannot
-- hold it). -> reads the raw JSON value without decoding it, so a NUL in the
-- body or another field does not matter; ->> runs only for rows that pass.
-- CASE keeps the cast from running on a payload that is not a JSON object (the
-- hub only writes objects, so this guards hand-edited rows). The patterns use
-- bracket classes and chr(92), the backslash, so no string-literal escaping
-- decides what they match.
UPDATE lane_inbox SET event_id=COALESCE(payload::json->>'event_id','') WHERE CASE WHEN payload ~ '^[[:space:]]*[{]' THEN position(chr(92) || 'u0000' IN (payload::json->'event_id')::text) = 0 ELSE false END;
CREATE INDEX lane_inbox_event_id ON lane_inbox(event_id) WHERE event_id<>'';

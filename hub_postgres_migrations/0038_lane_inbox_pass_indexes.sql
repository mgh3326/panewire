-- hk 6081: the held-batch pass (makeHeldBatches, hub_inbox_hold.go) and the
-- digest pass (makeBatchesDue, lane_inbox_store.go) run at least every 60 s and
-- after every write. Each scans lane_inbox by a predicate no index matched: the
-- hold and digest indexes in 0001 and 0002 lead with lane, which neither pass
-- binds, so each pass read most of the table.
--
-- Each index is partial on exactly the pass's own predicate, so it holds only
-- the rows the pass can claim, in id order (the statements' ORDER BY). The
-- statements' text is unchanged; the planner picks these because each WHERE
-- conjunct appears in the index predicate. SQLite's twin is in
-- openLaneInboxStore; MySQL's is migration 0031, which cannot use partial
-- indexes.
--
-- Additive: an older hub never names these indexes, and IF NOT EXISTS makes a
-- re-run or a racing start a no-op.
CREATE INDEX IF NOT EXISTS lane_inbox_hold_pending ON lane_inbox(id) WHERE hold_until>0 AND digest_batch='' AND acked_at=0 AND superseded_at=0;
CREATE INDEX IF NOT EXISTS lane_inbox_digest_pending ON lane_inbox(id) WHERE class='interrupt' AND acked_at=0 AND superseded_at=0 AND digest_at>0 AND digest_batch='' AND hold_until=0;

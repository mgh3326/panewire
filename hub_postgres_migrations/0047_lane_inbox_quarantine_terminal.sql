-- hk 7117: a quarantined batch is terminal (sent=2), so the pending batch page's
-- sent=0 partial index (lane_inbox_batches_pending) never reaches it. Quarantine
-- now sets sent=2 in the same transaction as its quarantine row (quarantineBatch);
-- this moves the batches quarantined before that. The MySQL twin is migration 0044.
-- Only unsent batches move: a batch already sent keeps sent=1, and no row is
-- deleted, so the quarantine rows and their inbox references stay as they are.
-- Rerunning changes nothing: the WHERE matches only sent=0 rows.
-- Rollback: an older hub reads sent=2 as neither pending nor sent; a quarantined
-- batch it leaves at sent=0 is still excluded by its quarantine anti-join.
UPDATE lane_inbox_batches SET sent=2 WHERE sent=0 AND id IN (SELECT batch_id FROM lane_inbox_batch_quarantine);

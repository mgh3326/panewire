-- hk 7117: the MySQL twin of Postgres migration 0047. A quarantined batch is
-- terminal (sent=2) so the pending page's sent=0 key never reaches it. Only
-- unsent quarantined batches move; sent batches and quarantine rows are kept.
-- The guard reads sent=2, which no batch has before this migration; a guard
-- above zero skips the run, and the statement is idempotent if it runs again.
-- applied-if: SELECT COUNT(*) FROM lane_inbox_batches WHERE sent=2
UPDATE lane_inbox_batches b JOIN lane_inbox_batch_quarantine q ON q.batch_id=b.id SET b.sent=2 WHERE b.sent=0;
